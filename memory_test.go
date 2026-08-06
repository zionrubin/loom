package loom_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/memory"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/security"
)

// memStore opens an ephemeral knowledge base and seeds it with committed
// facts, the way an application's accumulated history would arrive.
func memStore(t *testing.T, space string, facts ...string) *memory.InMemory {
	t.Helper()
	s, err := memory.NewInMemory("")
	if err != nil {
		t.Fatalf("open memory: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if len(facts) > 0 {
		seed(t, s, space, facts...)
	}
	return s
}

// seed stages and commits facts, as an earlier run would have.
func seed(t *testing.T, s memory.Store, space string, facts ...string) {
	t.Helper()
	ctx := context.Background()
	e := memory.NewHashEmbedder(0)
	vecs, _, err := e.Embed(ctx, memory.Call{}, facts)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	items := make([]memory.Item, 0, len(facts))
	for i, f := range facts {
		it := memory.NewItem(space, f, nil)
		it.Vector = vecs[i]
		items = append(items, it)
	}
	if _, err := s.Upsert(ctx, items); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if _, err := s.Commit(ctx, space); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// echoRegistry registers a mock that echoes the prompt, so a test can assert
// on exactly what reached the model.
func echoRegistry(t *testing.T, calls *atomic.Int64) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	_, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			if calls != nil {
				calls.Add(1)
			}
			return req.Prompt, nil
		}))
	if err != nil {
		t.Fatalf("register mock: %v", err)
	}
	return reg
}

// TestRecallReachesThePrompt is the base case: a fact an earlier run committed
// is retrieved and lands in a later run's prompt.
func TestRecallReachesThePrompt(t *testing.T) {
	store := memStore(t, "kb",
		"deploys to eu-west are frozen during the quarterly close",
		"the invoice exporter writes csv to the billing bucket")

	p := pipeline.New("answer")
	p.FromRecords("questions", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "are deploys frozen during quarterly close"}),
	}).
		Recall("history", pipeline.RecallSpec{
			Space: "kb", Query: "{{.q}}", K: 1,
		}).
		Infer("answer", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prompt:  "context:\n{{.memory}}\n\nquestion: {{.q}}",
		})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Output) != 1 {
		t.Fatalf("got %d output record(s), want 1", len(res.Output))
	}
	if got := res.Output[0].String("output"); !strings.Contains(got, "quarterly close") {
		t.Fatalf("the recalled fact did not reach the prompt:\n%s", got)
	}
	if res.Memory["kb"] != 1 {
		t.Fatalf("run pinned memory epoch %d, want 1", res.Memory["kb"])
	}
}

// TestCommitIsInvisibleToTheRunThatMadeIt covers the rule that keeps
// content-addressed replay honest: a Remember stage stages for the next epoch,
// so a Recall stage in the same run cannot see it whatever order they ran in.
func TestCommitIsInvisibleToTheRunThatMadeIt(t *testing.T) {
	store := memStore(t, "kb")

	p := pipeline.New("learn-then-read")
	written := p.FromRecords("facts", []core.Record{
		core.NewRecord("f1", map[string]any{"text": "the nightly reconciliation job runs at 02:00 utc"}),
	}).
		Remember("write", pipeline.RememberSpec{Space: "kb", Text: "{{.text}}"})
	written.Recall("read-back", pipeline.RecallSpec{
		Space: "kb", Query: "{{.text}}", K: 5,
	})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	readBack := res.StageOutputs["read-back"]
	if len(readBack) != 1 {
		t.Fatalf("got %d record(s) from the read-back stage", len(readBack))
	}
	if got := readBack[0].String("memory"); got != "" {
		t.Fatalf("a task recalled what its own run wrote:\n%s", got)
	}
	// And the write did happen — it is simply in the next epoch.
	if res.Committed["kb"] != 1 {
		t.Fatalf("run committed epoch %d, want 1", res.Committed["kb"])
	}
	committed, _ := store.Len("kb")
	if committed != 1 {
		t.Fatalf("knowledge base holds %d item(s) after the run, want 1", committed)
	}
}

// TestKnowledgeAccumulatesAcrossRuns is the point of the whole mechanism: what
// one run concludes, a later run recalls.
func TestKnowledgeAccumulatesAcrossRuns(t *testing.T) {
	store := memStore(t, "kb")
	ctx := context.Background()

	learn := pipeline.New("learn")
	learn.FromRecords("notes", []core.Record{
		core.NewRecord("n1", map[string]any{"text": "the payments service owner is the treasury team"}),
	}).
		Remember("write", pipeline.RememberSpec{
			Space: "kb", Text: "{{.text}}", Meta: map[string]string{"kind": "ownership"},
		})

	if _, err := loom.Run(ctx, learn,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil)); err != nil {
		t.Fatalf("learning run: %v", err)
	}

	ask := pipeline.New("ask")
	ask.FromRecords("questions", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "who owns the payments service"}),
	}).
		Recall("history", pipeline.RecallSpec{
			Space: "kb", Query: "{{.q}}", K: 1,
			Filter: map[string]string{"kind": "ownership"},
		})

	res, err := loom.Run(ctx, ask,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil))
	if err != nil {
		t.Fatalf("asking run: %v", err)
	}
	got := res.Output[0].String("memory")
	if !strings.Contains(got, "treasury team") {
		t.Fatalf("the second run did not recall what the first learned:\n%s", got)
	}
	if res.Memory["kb"] != 1 {
		t.Fatalf("second run pinned epoch %d, want 1", res.Memory["kb"])
	}
}

// TestRecallKeyedCacheInvalidation is the design's central claim, and the
// reason recall is a stage of its own rather than a lookup inside the
// inference.
//
// Committing to a knowledge base invalidates every recall stage that reads it,
// because the pinned epoch is in their fingerprints. It must NOT invalidate
// the inference below them wholesale: what reaches that stage's cache key is
// the record, and the record names the items that were retrieved. So a commit
// that does not change what a record recalls leaves that record's expensive
// call cached, and a commit that does change it pays for exactly that record.
func TestRecallKeyedCacheInvalidation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := memStore(t, "kb",
		"the checkout service is owned by the payments team",
		"the search service is owned by the discovery team")

	var calls atomic.Int64
	build := func() *pipeline.Pipeline {
		p := pipeline.New("ask")
		p.FromRecords("questions", []core.Record{
			core.NewRecord("q1", map[string]any{"q": "who owns the checkout service"}),
			core.NewRecord("q2", map[string]any{"q": "who owns the search service"}),
		}).
			Recall("history", pipeline.RecallSpec{Space: "kb", Query: "{{.q}}", K: 1}).
			Infer("answer", pipeline.InferSpec{
				Binding: model.Binding{Tier: model.TierFast},
				Prompt:  "context:\n{{.memory}}\n\nquestion: {{.q}}",
			})
		return p
	}
	// recalled returns each question's retrieved item IDs, so the test can
	// assert its own premise — which record's neighbourhood moved — before
	// drawing a conclusion from the call count.
	recalled := func(res *loom.RunResult) map[string]string {
		t.Helper()
		out := map[string]string{}
		for _, r := range res.StageOutputs["history"] {
			out[r.ID] = fmt.Sprint(r.Data["memory_ids"])
		}
		return out
	}
	run := func() map[string]string {
		t.Helper()
		res, err := loom.Run(ctx, build(),
			loom.WithRegistry(echoRegistry(t, &calls)),
			loom.WithMemory(store, nil),
			loom.WithStateDir(dir))
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return recalled(res)
	}

	first := run()
	if got := calls.Load(); got != 2 {
		t.Fatalf("first run made %d model call(s), want 2", got)
	}

	// A rerun against an unchanged knowledge base is free.
	calls.Store(0)
	run()
	if got := calls.Load(); got != 0 {
		t.Fatalf("rerun against an unchanged store made %d model call(s), want 0", got)
	}

	// Now the knowledge base grows by a fact that outranks what q1 retrieved
	// and does not outrank what q2 retrieved. The epoch moves, so both recall
	// stages recompute — but only the record whose top-1 actually changed
	// should pay for a model call.
	seed(t, store, "kb",
		"who owns the checkout service now: the commerce team owns the checkout service")
	calls.Store(0)
	moved := run()
	if moved["q1"] == first["q1"] {
		t.Fatalf("premise failed: the committed fact did not change q1's recall (%s)", moved["q1"])
	}
	if moved["q2"] != first["q2"] {
		t.Fatalf("premise failed: the committed fact changed q2's recall (%s → %s)",
			first["q2"], moved["q2"])
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("after a commit that moved one record's recall, the run made %d model "+
			"call(s), want 1 — recall-keyed invalidation is not holding", got)
	}

	// And a commit that changes nothing either record retrieves is free again,
	// even though it moved the epoch and invalidated every recall stage.
	seed(t, store, "kb", "sourdough starters need feeding every twelve hours")
	calls.Store(0)
	unmoved := run()
	if unmoved["q1"] != moved["q1"] || unmoved["q2"] != moved["q2"] {
		t.Fatalf("premise failed: an unrelated fact changed a recall")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("after a commit unrelated to either record, the run made %d model "+
			"call(s), want 0", got)
	}
}

// TestMemoryLeastPrivilege: a stage reaches only the spaces it declared, and a
// pipeline's other stages cannot reach the knowledge base at all.
func TestMemoryLeastPrivilege(t *testing.T) {
	store := memStore(t, "public", "a fact anyone may read")
	seed(t, store, "private", "a fact this pipeline may not read")

	p := pipeline.New("sneak")
	p.FromRecords("questions", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "a fact"}),
	}).
		Recall("allowed", pipeline.RecallSpec{Space: "public", Query: "{{.q}}", K: 1}).
		MapTools("peek", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
			_, err := s.Recall(ctx, "private", "a fact", 1)
			if err == nil {
				return r, fmt.Errorf("read an undeclared memory space")
			}
			r.Data["denied"] = err.Error()
			return r, nil
		})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	denied := res.Output[0].String("denied")
	if !strings.Contains(denied, "capability not granted") {
		t.Fatalf("undeclared read failed with %q, want a capability denial", denied)
	}

	var found bool
	for _, e := range res.Audit {
		if e.Action == "memory.read" && e.Subject == "private" && !e.Allowed {
			found = true
		}
	}
	if !found {
		t.Fatalf("the denied read was not audited; audit trail: %+v", res.Audit)
	}
}

// TestMemoryWriteRequiresItsOwnGrant: reading the knowledge base does not make
// a stage an author of it.
func TestMemoryWriteRequiresItsOwnGrant(t *testing.T) {
	store := memStore(t, "kb", "a fact")

	p := pipeline.New("write-without-grant")
	p.FromRecords("questions", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "a fact"}),
	}).
		MapTools("smuggle", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
			_, err := s.Remember(ctx, "kb", "a fact this stage invented", nil)
			if err == nil {
				return r, fmt.Errorf("wrote to memory with only a read grant")
			}
			r.Data["denied"] = err.Error()
			return r, nil
		}, pipeline.WithMemory("kb"))

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if denied := res.Output[0].String("denied"); !strings.Contains(denied, "write capability not granted") {
		t.Fatalf("unauthorized write failed with %q, want a write-capability denial", denied)
	}
	if committed, staged := store.Len("kb"); committed != 1 || staged != 0 {
		t.Fatalf("knowledge base holds %d committed / %d staged, want 1 / 0", committed, staged)
	}
}

// TestPipelineWithoutMemoryStoreFailsToCompile: naming a space with no store
// behind it is an authoring error, and should surface before any money is
// spent rather than on the first record.
func TestPipelineWithoutMemoryStoreFailsToCompile(t *testing.T) {
	p := pipeline.New("no-store")
	p.FromRecords("questions", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "anything"}),
	}).
		Recall("history", pipeline.RecallSpec{Space: "kb", Query: "{{.q}}", K: 1})

	_, err := loom.Run(context.Background(), p, loom.WithRegistry(echoRegistry(t, nil)))
	if err == nil {
		t.Fatal("a pipeline naming a memory space ran without a store")
	}
	if !strings.Contains(err.Error(), "loom.WithMemory") {
		t.Fatalf("error %q does not say how to fix it", err)
	}
}

// TestRequireFailsRecordsThatRecallNothing: a prompt meaningless without
// context should fail loudly rather than ask the model to reason from nothing.
func TestRequireFailsRecordsThatRecallNothing(t *testing.T) {
	store := memStore(t, "kb", "an entirely unrelated fact about glaciers")

	p := pipeline.New("strict")
	p.FromRecords("questions", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "quarterly revenue recognition policy"}),
	}).
		Recall("history", pipeline.RecallSpec{
			Space: "kb", Query: "{{.q}}", K: 3, MinScore: 0.5, Require: true,
		})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil),
		loom.WithContinueOnError(),
		loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Failures) != 1 {
		t.Fatalf("got %d dead letter(s), want 1", len(res.Failures))
	}
	if !strings.Contains(res.Failures[0].Err.Error(), "nothing recalled") {
		t.Fatalf("dead letter reason is %q", res.Failures[0].Err)
	}
}

// TestWithoutMemoryCommitLeavesWritesStaged: the run that may read the
// knowledge base without being trusted to extend it.
func TestWithoutMemoryCommitLeavesWritesStaged(t *testing.T) {
	store := memStore(t, "kb")

	p := pipeline.New("dry-run")
	p.FromRecords("facts", []core.Record{
		core.NewRecord("f1", map[string]any{"text": "a conclusion pending review"}),
	}).
		Remember("write", pipeline.RememberSpec{Space: "kb", Text: "{{.text}}"})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil),
		loom.WithoutMemoryCommit())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Committed) != 0 {
		t.Fatalf("a run with WithoutMemoryCommit committed %v", res.Committed)
	}
	committed, staged := store.Len("kb")
	if committed != 0 || staged != 1 {
		t.Fatalf("knowledge base holds %d committed / %d staged, want 0 / 1", committed, staged)
	}
	if epoch, _ := store.Epoch(context.Background(), "kb"); epoch != 0 {
		t.Fatalf("epoch moved to %d without a commit", epoch)
	}
}

// TestMemoryEventsDescribeTheRun: an observer must be able to say which
// version of the knowledge base a run's results were computed against.
func TestMemoryEventsDescribeTheRun(t *testing.T) {
	store := memStore(t, "kb", "an already-known fact")

	p := pipeline.New("observed")
	p.FromRecords("facts", []core.Record{
		core.NewRecord("f1", map[string]any{"text": "a newly learned fact"}),
	}).
		Recall("history", pipeline.RecallSpec{Space: "kb", Query: "{{.text}}", K: 1}).
		Remember("write", pipeline.RememberSpec{Space: "kb", Text: "{{.text}}"})

	seen := map[observe.EventType][]observe.Event{}
	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil),
		loom.WithEventHandler(func(e observe.Event) {
			seen[e.Type] = append(seen[e.Type], e)
		}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	pinned := seen[observe.MemoryPinned]
	if len(pinned) != 1 || pinned[0].Space != "kb" || pinned[0].Epoch != 1 {
		t.Fatalf("memory.pinned events: %+v", pinned)
	}
	if recalled := seen[observe.MemoryRecalled]; len(recalled) != 1 || recalled[0].Epoch != 1 {
		t.Fatalf("memory.recalled events: %+v", recalled)
	}
	if written := seen[observe.MemoryWritten]; len(written) != 1 || written[0].Items != 1 {
		t.Fatalf("memory.written events: %+v", written)
	}
	committed := seen[observe.MemoryCommitted]
	if len(committed) != 1 || committed[0].Epoch != 2 {
		t.Fatalf("memory.committed events: %+v", committed)
	}
	if res.Committed["kb"] != 2 {
		t.Fatalf("run committed epoch %d, want 2", res.Committed["kb"])
	}
}

// TestFleetSharesOneKnowledgeBase: agents on a fleet read one store, and the
// commit is the fleet owner's to make — an agent finishing must not publish
// another agent's work in progress.
func TestFleetSharesOneKnowledgeBase(t *testing.T) {
	ctx := context.Background()
	store := memStore(t, "kb")

	fleet, err := loom.NewFleet(
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil),
		loom.WithWorkers(4))
	if err != nil {
		t.Fatalf("new fleet: %v", err)
	}
	defer fleet.Close()

	for i, fact := range []string{
		"the release train departs on thursdays",
		"schema migrations need a two-phase rollout",
	} {
		p := pipeline.New(fmt.Sprintf("learn-%d", i))
		p.FromRecords("facts", []core.Record{
			core.NewRecord(fmt.Sprintf("f%d", i), map[string]any{"text": fact}),
		}).
			Remember("write", pipeline.RememberSpec{Space: "kb", Text: "{{.text}}"})
		if _, err := fleet.Run(ctx, p); err != nil {
			t.Fatalf("agent %d: %v", i, err)
		}
	}

	// No agent committed on its own.
	if epoch, _ := store.Epoch(ctx, "kb"); epoch != 0 {
		t.Fatalf("an agent committed on its own: epoch is %d, want 0", epoch)
	}
	if committed, staged := store.Len("kb"); committed != 0 || staged != 2 {
		t.Fatalf("knowledge base holds %d committed / %d staged, want 0 / 2", committed, staged)
	}

	reached, err := fleet.CommitMemory(ctx)
	if err != nil {
		t.Fatalf("commit memory: %v", err)
	}
	if reached["kb"] != 1 {
		t.Fatalf("fleet commit reached epoch %d, want 1", reached["kb"])
	}
	if committed, _ := store.Len("kb"); committed != 2 {
		t.Fatalf("both agents' facts should be in one epoch, got %d item(s)", committed)
	}
}

// TestFleetRejectsPerAgentMemory: one knowledge base serves the fleet, and an
// option that would silently be ignored should fail instead.
func TestFleetRejectsPerAgentMemory(t *testing.T) {
	fleet, err := loom.NewFleet(loom.WithRegistry(echoRegistry(t, nil)))
	if err != nil {
		t.Fatalf("new fleet: %v", err)
	}
	defer fleet.Close()

	p := pipeline.New("agent")
	p.FromRecords("in", []core.Record{core.NewRecord("r1", nil)})
	_, err = fleet.Run(context.Background(), p, loom.WithMemory(memStore(t, "kb"), nil))
	if err == nil || !strings.Contains(err.Error(), "fleet-wide") {
		t.Fatalf("per-agent WithMemory failed with %v, want a fleet-wide rejection", err)
	}
}

// TestEmbedderEgressIsEnforced: an embedder that reaches the network is
// governed by the same deny-by-default allowlist a model provider is, and only
// the stages that touch memory get the host at all.
func TestEmbedderEgressIsEnforced(t *testing.T) {
	store := memStore(t, "kb", "a fact")

	p := pipeline.New("egress")
	p.FromRecords("questions", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "a fact"}),
	}).
		Recall("history", pipeline.RecallSpec{Space: "kb", Query: "{{.q}}", K: 1})

	// An embedder declaring a host that the planner will therefore allow.
	allowed := &fakeEmbedder{host: "embeddings.example", secret: "embed_key"}
	if _, err := loom.Run(context.Background(), p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithSecrets(map[security.SecretRef]string{"embed_key": "sk-test"}),
		loom.WithMemory(store, allowed)); err != nil {
		t.Fatalf("run with a declared embedder host: %v", err)
	}
	if allowed.calls.Load() == 0 {
		t.Fatal("the embedder was never called")
	}
	if allowed.resolved.Load() == 0 {
		t.Fatal("the embedder never resolved its secret through the broker")
	}

	// An embedder whose Endpoint lies about where it goes is denied, because
	// the allowlist was assembled from what it declared.
	liar := &fakeEmbedder{host: "embeddings.example", reach: "exfil.example"}
	_, err := loom.Run(context.Background(), p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, liar),
		loom.WithRetry(quickRetry()))
	if err == nil {
		t.Fatal("an embedder reached a host outside the task's egress allowlist")
	}
}

// fakeEmbedder is a deterministic embedder that reports a network host and a
// secret, so egress and broker scoping can be exercised without a network.
type fakeEmbedder struct {
	host   string
	reach  string // host it actually contacts, when it differs from host
	secret security.SecretRef

	calls    atomic.Int64
	resolved atomic.Int64
}

func (f *fakeEmbedder) Name() string               { return "fake" }
func (f *fakeEmbedder) Dims() int                  { return 64 }
func (f *fakeEmbedder) Endpoint() string           { return f.host }
func (f *fakeEmbedder) Secret() security.SecretRef { return f.secret }

func (f *fakeEmbedder) Embed(ctx context.Context, call memory.Call, texts []string) ([][]float32, core.Usage, error) {
	f.calls.Add(1)
	if f.secret != "" && call.ResolveSecret != nil {
		if _, err := call.ResolveSecret(f.secret); err != nil {
			return nil, core.Usage{}, core.Permanent(err)
		}
		f.resolved.Add(1)
	}
	if f.reach != "" {
		return nil, core.Usage{}, core.Permanent(
			fmt.Errorf("egress to %q denied", f.reach))
	}
	inner := memory.NewHashEmbedder(f.Dims())
	return inner.Embed(ctx, call, texts)
}

// TestExplainReportsWhatItCannotPrice: a projection that silently omitted the
// retrieved context would understate the run, which is the one direction a
// cost projection must never be quietly wrong in.
func TestExplainReportsWhatItCannotPrice(t *testing.T) {
	store := memStore(t, "kb", "a fact worth recalling")

	p := pipeline.New("projected")
	p.FromRecords("questions", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "a fact"}),
		core.NewRecord("q2", map[string]any{"q": "another fact"}),
	}).
		Recall("history", pipeline.RecallSpec{Space: "kb", Query: "{{.q}}", K: 3}).
		Infer("answer", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prompt:  "context:\n{{.memory}}\n\nquestion: {{.q}}",
		})

	proj, err := loom.Explain(p,
		loom.WithRegistry(echoRegistry(t, nil)),
		loom.WithMemory(store, nil))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}

	// The record count downstream of a recall is exact — retrieval is 1:1 —
	// even though what it retrieved is not.
	var recall, infer *loom.StageProjection
	for i := range proj.Stages {
		switch proj.Stages[i].Stage {
		case "history":
			recall = &proj.Stages[i]
		case "answer":
			infer = &proj.Stages[i]
		}
	}
	if recall == nil || infer == nil {
		t.Fatalf("projection is missing a stage: %+v", proj.Stages)
	}
	if recall.Calls != 2 {
		t.Errorf("recall stage projected %d embedding call(s), want 2", recall.Calls)
	}
	if infer.Records != 2 {
		t.Errorf("stage below a recall projected %d record(s), want 2", infer.Records)
	}
	// The recall stage's own counts are exact — one embedding per record — so
	// it is the stages *below* it whose prompt sizes are guessed, exactly as
	// with a ParseJSON stage.
	if !infer.Estimated {
		t.Error("the stage below a recall must be marked estimated: the context " +
			"the recall would have supplied is not knowable from the plan")
	}
	if !proj.Partial() {
		t.Error("a projection downstream of an unpriced recall must report itself partial")
	}

	var sawContext, sawPricing bool
	for _, w := range proj.Warnings {
		if strings.Contains(w, "understated") {
			sawContext = true
		}
		if strings.Contains(w, "not priced") {
			sawPricing = true
		}
	}
	if !sawContext {
		t.Errorf("no warning that the retrieved context is missing from the "+
			"prompts below: %v", proj.Warnings)
	}
	if !sawPricing {
		t.Errorf("no warning that embedding calls are unpriced: %v", proj.Warnings)
	}
}
