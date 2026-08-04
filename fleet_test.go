package loom_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/security"
)

// notePipeline is a minimal one-call-per-record agent: n records, one infer
// stage, nothing clever. It is the unit the fleet tests schedule.
//
// Records are keyed by prefix rather than by pipeline name, so two agents given
// the same prefix produce byte-identical records and therefore identical cache
// keys — which is what the shared-cache tests need, and what agents given
// different prefixes must not accidentally get.
func notePipeline(name string, n int, prefix string) *pipeline.Pipeline {
	recs := make([]core.Record, n)
	for i := range recs {
		recs[i] = core.NewRecord(fmt.Sprintf("%s-%d", prefix, i),
			map[string]any{"text": fmt.Sprintf("%s item %d", prefix, i)})
	}
	p := pipeline.New(name)
	p.FromRecords("src", recs).Infer("note", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast},
		Prompt:  "Note: {{.text}}",
	})
	return p
}

// fleetRegistry returns a registry whose only model is a mock with the given
// latency, plus a counter of how many calls it has served.
func fleetRegistry(t *testing.T, latency time.Duration) (*model.Registry, *model.Mock) {
	t.Helper()
	reg := model.NewRegistry()
	mock, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithLatency(latency),
		model.WithHandler(func(req model.Request) (string, error) {
			return "ok: " + strings.TrimPrefix(req.Prompt, "Note: "), nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	return reg, mock
}

// The headline property: agents on one fleet occupy one bounded set of slots.
// Two separate Runs of the same shape would each take the full concurrency
// they asked for, so the observed peak would be double.
func TestFleetAgentsShareOneSlotPool(t *testing.T) {
	reg := model.NewRegistry()
	var inFlight, peak int64
	_, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			now := atomic.AddInt64(&inFlight, 1)
			for {
				old := atomic.LoadInt64(&peak)
				if now <= old || atomic.CompareAndSwapInt64(&peak, old, now) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt64(&inFlight, -1)
			return "ok", nil
		}))
	if err != nil {
		t.Fatal(err)
	}

	const slots = 3
	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithWorkers(slots),
		loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()
	for _, name := range []string{"alpha", "beta", "gamma"} {
		f.Go(ctx, notePipeline(name, 12, name))
	}
	if err := f.Wait(); err != nil {
		t.Fatalf("fleet: %v", err)
	}

	if got := atomic.LoadInt64(&peak); got > slots {
		t.Errorf("peak concurrent model calls = %d across the fleet, want <= %d slots: "+
			"the agents are not sharing one pool", got, slots)
	}
	if got := atomic.LoadInt64(&peak); got < 2 {
		t.Errorf("peak concurrency = %d: the fleet never overlapped its agents at all", got)
	}
}

// One governor covers the fleet. With a budget below what three agents would
// cost, the fleet must stop — which it can only do if the ceiling is shared.
func TestFleetBudgetIsSharedAcrossAgents(t *testing.T) {
	// Priced so each call costs real money against the budget below.
	reg := model.NewRegistry()
	if err := reg.Register(model.Info{
		ID: "mock-fast", Tier: model.TierFast,
		Pricing:  model.Pricing{InputPerMTok: 1000, OutputPerMTok: 1000},
		Provider: model.NewMock("mock-fast"),
	}); err != nil {
		t.Fatal(err)
	}

	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithWorkers(2),
		loom.WithRetry(quickRetry()), loom.WithContinueOnError(),
		loom.WithFleetBudget(core.Budget{MaxCostUSD: 0.02}))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()
	var agents []*loom.Agent
	for _, name := range []string{"a", "b", "c"} {
		agents = append(agents, f.Go(ctx, notePipeline(name, 40, name)))
	}
	_ = f.Wait()

	spent := f.Spent().CostUSD
	if spent < 0.02 {
		t.Fatalf("fleet spent $%.4f without reaching its $0.0200 ceiling; "+
			"the test never exercised the governor", spent)
	}
	// Post-hoc charging means overrun is bounded by the tasks in flight, not
	// unbounded — the ceiling holds to within a couple of calls, and crucially
	// it is one ceiling rather than one per agent.
	if spent > 0.06 {
		t.Errorf("fleet spent $%.4f against a $0.0200 ceiling: that is ~one budget "+
			"per agent, so the governor is not shared", spent)
	}

	stopped := 0
	for _, a := range agents {
		if _, err := a.Wait(); err != nil {
			stopped++
		}
	}
	if stopped == 0 {
		t.Error("no agent reported the budget stopping it")
	}
}

// Work one agent paid for is free to the next. This is the shared cache, and
// it is the reason a fleet beats a sequence of Runs even when nothing overlaps.
func TestFleetAgentsShareTheResultCache(t *testing.T) {
	reg, mock := fleetRegistry(t, 0)
	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()
	// Two agents over identical records with an identically specified stage:
	// same fingerprint, same inputs, same cache keys.
	if _, err := f.Run(ctx, notePipeline("first", 6, "shared")); err != nil {
		t.Fatalf("first agent: %v", err)
	}
	after := mock.Calls()
	if after == 0 {
		t.Fatal("first agent made no model calls")
	}

	if _, err := f.Run(ctx, notePipeline("second", 6, "shared")); err != nil {
		t.Fatalf("second agent: %v", err)
	}
	if mock.Calls() != after {
		t.Errorf("second agent made %d new model calls; it should have replayed "+
			"the first agent's results from the shared cache", mock.Calls()-after)
	}
}

// Each agent's report, lineage, and audit trail must describe that agent, not
// the whole fleet — stages of the same name in different pipelines especially
// must not fold into one row.
func TestFleetAgentResultsAreNotMixed(t *testing.T) {
	reg, _ := fleetRegistry(t, 2*time.Millisecond)
	// A shared value both agents read, so there is audit activity to attribute:
	// a broadcast read is checked and recorded per task.
	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithWorkers(4),
		loom.WithRetry(quickRetry()), loom.WithBroadcast("style", "terse"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	reading := func(name string, n int) *pipeline.Pipeline {
		recs := make([]core.Record, n)
		for i := range recs {
			recs[i] = core.NewRecord(fmt.Sprintf("%s-%d", name, i),
				map[string]any{"text": fmt.Sprintf("%s item %d", name, i)})
		}
		p := pipeline.New(name)
		p.FromRecords("src", recs).Infer("note", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prompt:  `Style {{broadcast "style"}}. Note: {{.text}}`,
		}, pipeline.WithBroadcast("style"))
		return p
	}

	ctx := context.Background()
	a := f.Go(ctx, reading("left", 5))
	b := f.Go(ctx, reading("right", 9))

	resA, err := a.Wait()
	if err != nil {
		t.Fatalf("left: %v", err)
	}
	resB, err := b.Wait()
	if err != nil {
		t.Fatalf("right: %v", err)
	}

	if resA.RunID == resB.RunID {
		t.Fatal("both agents got the same run ID")
	}
	if got := len(resA.StageOutputs["note"]); got != 5 {
		t.Errorf("left produced %d records, want 5", got)
	}
	if got := len(resB.StageOutputs["note"]); got != 9 {
		t.Errorf("right produced %d records, want 9", got)
	}

	// Both pipelines name their infer stage "note". The reports must still
	// count only their own tasks.
	tasks := func(res *loom.RunResult) int {
		for _, s := range res.Report.Stages {
			if s.Stage == "note" {
				return s.Tasks
			}
		}
		return -1
	}
	if got := tasks(resA); got != 5 {
		t.Errorf("left report counted %d tasks for stage %q, want 5 — the collectors "+
			"are shared across agents", got, "note")
	}
	if got := tasks(resB); got != 9 {
		t.Errorf("right report counted %d tasks for stage %q, want 9", got, "note")
	}

	if got := len(resA.Lineage); got != 5 {
		t.Errorf("left lineage has %d entries, want 5", got)
	}
	for _, e := range resA.Lineage {
		if e.RunID != resA.RunID {
			t.Fatalf("left lineage contains an entry from run %s", e.RunID)
		}
	}

	// Each agent read the shared value once per task, and each audit trail must
	// carry exactly its own agent's reads — the log is fleet-wide, and only the
	// agent's task IDs can attribute it.
	reads := func(res *loom.RunResult) int {
		n := 0
		for _, e := range res.Audit {
			if e.Action == "broadcast.read" {
				n++
			}
		}
		return n
	}
	if got := reads(resA); got != 5 {
		t.Errorf("left audit trail has %d broadcast reads, want 5 (one per task): "+
			"the fleet-wide log is not being attributed per agent", got)
	}
	if got := reads(resB); got != 9 {
		t.Errorf("right audit trail has %d broadcast reads, want 9", got)
	}
}

// The blackboard: what one agent posts, the next agent reads — pinned to the
// snapshot that existed when it launched.
func TestFleetBlackboardCarriesFindingsBetweenAgents(t *testing.T) {
	reg := model.NewRegistry()
	var prompts []string
	var mu sync.Mutex
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			mu.Lock()
			prompts = append(prompts, req.Prompt)
			mu.Unlock()
			return "ok", nil
		})); err != nil {
		t.Fatal(err)
	}

	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithTopic("findings"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// A reader stage that renders the board into its prompt.
	reader := func(name string) *pipeline.Pipeline {
		p := pipeline.New(name)
		p.FromRecords("src", []core.Record{core.NewRecord("r1", map[string]any{"q": "why"})}).
			Infer("read", pipeline.InferSpec{
				Binding: model.Binding{Tier: model.TierFast},
				Prompt:  `Board: {{broadcastJSON "findings"}}` + "\nQuestion: {{.q}}",
			}, pipeline.WithBroadcast("findings"))
		return p
	}

	ctx := context.Background()

	// The agent that runs before anything is posted sees an empty board rather
	// than failing to compile.
	if _, err := f.Run(ctx, reader("early")); err != nil {
		t.Fatalf("early agent: %v", err)
	}

	v, err := f.PostFrom("scout", "findings", map[string]any{"claim": "the cache is warm"})
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if v.Posts != 1 || v.Topic != "findings" || v.Hash == "" {
		t.Fatalf("unexpected version %+v", v)
	}
	if got := v.String(); got != "findings@1" {
		t.Errorf("Version.String() = %q, want %q", got, "findings@1")
	}

	if _, err := f.Run(ctx, reader("late")); err != nil {
		t.Fatalf("late agent: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(prompts) != 2 {
		t.Fatalf("expected 2 prompts, got %d", len(prompts))
	}
	if strings.Contains(prompts[0], "the cache is warm") {
		t.Error("the early agent saw a finding posted after it launched")
	}
	if !strings.Contains(prompts[1], "the cache is warm") {
		t.Errorf("the late agent did not see the posted finding; prompt was:\n%s", prompts[1])
	}
	if !strings.Contains(prompts[1], `"scout"`) {
		t.Errorf("the post lost its attribution; prompt was:\n%s", prompts[1])
	}

	if posts := f.Posts("findings"); len(posts) != 1 || posts[0].Agent != "scout" || posts[0].Seq != 0 {
		t.Errorf("Posts() = %+v", posts)
	}
	if vals := f.Values("findings"); len(vals) != 1 {
		t.Errorf("Values() = %+v", vals)
	}
	if topics := f.Topics(); len(topics) != 1 || topics[0] != "findings" {
		t.Errorf("Topics() = %v", topics)
	}
}

// A post must invalidate exactly the cached results that could have seen the
// topic, and leave everything else warm. That is the property that makes a
// mutable shared log safe to put in front of a content-addressed cache.
func TestFleetBlackboardKeepsTheCacheHonest(t *testing.T) {
	reg, mock := fleetRegistry(t, 0)
	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithTopic("rubric"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	board := func(name string) *pipeline.Pipeline {
		p := pipeline.New(name)
		p.FromRecords("src", []core.Record{core.NewRecord("r1", map[string]any{"text": "x"})}).
			Infer("note", pipeline.InferSpec{
				Binding: model.Binding{Tier: model.TierFast},
				Prefix:  `Rubric: {{broadcastJSON "rubric"}}`,
				Prompt:  "Note: {{.text}}",
			}, pipeline.WithBroadcast("rubric"))
		return p
	}
	// An agent that reads nothing from the board, for contrast.
	plain := func(name string) *pipeline.Pipeline { return notePipeline(name, 1, "plain") }

	ctx := context.Background()
	if _, err := f.Run(ctx, board("reader-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Run(ctx, plain("plain-1")); err != nil {
		t.Fatal(err)
	}
	baseline := mock.Calls()

	// Identical agents, unchanged board: everything replays.
	if _, err := f.Run(ctx, board("reader-2")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Run(ctx, plain("plain-2")); err != nil {
		t.Fatal(err)
	}
	if mock.Calls() != baseline {
		t.Fatalf("%d calls on an unchanged board: cached results were not reused",
			mock.Calls()-baseline)
	}

	// Post to the topic, and only its readers recompute.
	if _, err := f.Post("rubric", "prefer short answers"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Run(ctx, board("reader-3")); err != nil {
		t.Fatal(err)
	}
	afterReader := mock.Calls()
	if afterReader != baseline+1 {
		t.Errorf("board reader made %d calls after a post, want 1: editing the "+
			"board must invalidate its readers", afterReader-baseline)
	}
	if _, err := f.Run(ctx, plain("plain-3")); err != nil {
		t.Fatal(err)
	}
	if mock.Calls() != afterReader {
		t.Errorf("a stage that never declared the topic recomputed after a post: "+
			"%d extra calls", mock.Calls()-afterReader)
	}
}

// Await is the fan-in: launch producers, block until the board reaches a
// count, then launch the consumer that reads the snapshot.
func TestFleetAwaitBlocksUntilTopicFills(t *testing.T) {
	f, err := loom.NewFleet(loom.WithTopic("notes"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	go func() {
		for i := 0; i < 3; i++ {
			time.Sleep(2 * time.Millisecond)
			if _, err := f.Post("notes", i); err != nil {
				t.Errorf("post: %v", err)
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	posts, err := f.Await(ctx, "notes", 3)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if len(posts) != 3 {
		t.Errorf("Await returned %d posts, want 3", len(posts))
	}

	// A count that never arrives must respect the context rather than hang.
	short, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	if _, err := f.Await(short, "notes", 99); err == nil {
		t.Error("Await returned nil error for a count that never arrives")
	}
}

// Fleet-wide settings passed to an agent must fail loudly. Silently ignoring
// WithStateDir on an agent would mean a caller believing they had persistence.
func TestFleetRejectsFleetWideOptionsPerAgent(t *testing.T) {
	reg, _ := fleetRegistry(t, 0)
	f, err := loom.NewFleet(loom.WithRegistry(reg))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	for name, opt := range map[string]loom.Option{
		"WithStateDir":    loom.WithStateDir(t.TempDir()),
		"WithWorkers":     loom.WithWorkers(4),
		"WithFleetBudget": loom.WithFleetBudget(core.Budget{MaxCostUSD: 1}),
		"WithBroadcast":   loom.WithBroadcast("x", 1),
		"WithTopic":       loom.WithTopic("x"),
		"WithSecrets":     loom.WithSecrets(map[security.SecretRef]string{"k": "v"}),
	} {
		_, err := f.Run(context.Background(), notePipeline("a", 1, "a"), opt)
		if err == nil {
			t.Errorf("%s was accepted per agent; it is fleet-wide", name)
			continue
		}
		if !strings.Contains(err.Error(), "fleet-wide") {
			t.Errorf("%s: error does not explain the problem: %v", name, err)
		}
	}
}

// The report is the fleet's deliverable: per-agent completion time, slot-time,
// and queueing, plus the pool and budget totals.
func TestFleetReport(t *testing.T) {
	reg, _ := fleetRegistry(t, 3*time.Millisecond)
	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithWorkers(2),
		loom.WithRetry(quickRetry()), loom.WithTopic("notes"),
		loom.WithFleetBudget(core.Budget{MaxCostUSD: 100}))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()
	f.Go(ctx, notePipeline("sweep", 16, "sweep"))
	f.Go(ctx, notePipeline("summary", 2, "summary"))
	if err := f.Wait(); err != nil {
		t.Fatalf("fleet: %v", err)
	}
	if _, err := f.Post("notes", "done"); err != nil {
		t.Fatal(err)
	}

	rep := f.Report()
	if len(rep.Agents) != 2 {
		t.Fatalf("report has %d agents, want 2", len(rep.Agents))
	}
	if rep.Slots != 2 {
		t.Errorf("Slots = %d, want 2", rep.Slots)
	}
	if rep.Duration() <= 0 {
		t.Error("report has no duration")
	}
	if rep.Pool.Admitted != 18 {
		t.Errorf("pool admitted %d tasks, want 18", rep.Pool.Admitted)
	}
	if rep.Topics != 1 || rep.Posts != 1 {
		t.Errorf("board reported as %d topics / %d posts, want 1/1", rep.Topics, rep.Posts)
	}

	byName := map[string]loom.AgentReport{}
	for _, a := range rep.Agents {
		byName[a.Name] = a
		if a.Tasks == 0 {
			t.Errorf("agent %q reported no tasks", a.Name)
		}
		if a.JCT <= 0 {
			t.Errorf("agent %q reported no completion time", a.Name)
		}
		if a.Service <= 0 {
			t.Errorf("agent %q was charged no slot-time", a.Name)
		}
	}
	if byName["sweep"].Service <= byName["summary"].Service {
		t.Errorf("the 16-record agent attained %s of service and the 2-record agent %s: "+
			"service is not attributed per agent",
			byName["sweep"].Service, byName["summary"].Service)
	}
	// The point of the policy: the short agent finishes well before the long one.
	if byName["summary"].JCT >= byName["sweep"].JCT {
		t.Errorf("the 2-record agent took %s and the 16-record agent %s: the short "+
			"agent did not overtake", byName["summary"].JCT, byName["sweep"].JCT)
	}

	if occ := rep.Occupancy(); occ <= 0 || occ > 1.5 {
		t.Errorf("Occupancy() = %.2f, want a plausible fraction", occ)
	}

	out := rep.String()
	for _, want := range []string{"fleet", "sweep", "summary", "TOTAL", "slots", "blackboard", "fleet budget"} {
		if !strings.Contains(out, want) {
			t.Errorf("report is missing %q:\n%s", want, out)
		}
	}
}

// A fleet publishes onto one bus, so one observer holds every agent — which is
// what lets the constellation view show them side by side.
func TestFleetPublishesEveryAgentOnOneBus(t *testing.T) {
	reg, _ := fleetRegistry(t, 0)

	var mu sync.Mutex
	runs := map[string]string{}
	posts := 0
	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithEventHandler(func(e observe.Event) {
			mu.Lock()
			defer mu.Unlock()
			switch e.Type {
			case observe.RunStarted:
				runs[e.RunID] = e.Pipeline
				if e.Kind != "fleet" {
					t.Errorf("run %s reported driver %q, want %q", e.RunID, e.Kind, "fleet")
				}
			case observe.BlackboardPosted:
				posts++
				if e.Topic != "notes" || e.Posts != 1 || e.Artifact == "" {
					t.Errorf("unexpected blackboard event: %+v", e)
				}
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()
	f.Go(ctx, notePipeline("one", 3, "one"))
	f.Go(ctx, notePipeline("two", 3, "two"))
	if err := f.Wait(); err != nil {
		t.Fatalf("fleet: %v", err)
	}
	if _, err := f.Post("notes", "hello"); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(runs) != 2 {
		t.Errorf("observer saw %d runs, want 2: %v", len(runs), runs)
	}
	if posts != 1 {
		t.Errorf("observer saw %d blackboard posts, want 1", posts)
	}
}

// Explain must work on a fleet with the board in scope, so an agent can be
// priced before it is launched.
func TestFleetExplain(t *testing.T) {
	reg, mock := fleetRegistry(t, 0)
	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithTopic("rubric"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if _, err := f.Post("rubric", strings.Repeat("a long shared rubric. ", 50)); err != nil {
		t.Fatal(err)
	}

	p := pipeline.New("priced")
	p.FromRecords("src", []core.Record{core.NewRecord("r1", map[string]any{"text": "x"})}).
		Infer("note", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prefix:  `Rubric: {{broadcastJSON "rubric"}}`,
			Prompt:  "Note: {{.text}}",
		}, pipeline.WithBroadcast("rubric"))

	proj, err := f.Explain(p)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if mock.Calls() != 0 {
		t.Errorf("Explain made %d model calls", mock.Calls())
	}
	var calls int
	for _, s := range proj.Stages {
		calls += s.Calls
	}
	if calls != 1 {
		t.Errorf("projected %d calls, want 1", calls)
	}
	// The prefix reads the board, so the projection must have priced its bytes.
	if got := proj.Expected().PromptTokens(); got < 100 {
		t.Errorf("projected %d prompt tokens: the board snapshot was not in scope", got)
	}
}

// Report and Post must both be safe against agents that are still running and
// against each other. Two bugs lived here: Report read an agent's error without
// waiting for the edge that publishes it, and two concurrent posts could append
// in order but register out of order, leaving the topic's name pointing at the
// shorter snapshot and losing the later post for every agent started afterwards.
func TestFleetConcurrentPostsAndLiveReport(t *testing.T) {
	reg := model.NewRegistry()
	var seen []string
	var mu sync.Mutex
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithLatency(time.Millisecond),
		model.WithHandler(func(req model.Request) (string, error) {
			mu.Lock()
			seen = append(seen, req.Prompt)
			mu.Unlock()
			return "ok", nil
		})); err != nil {
		t.Fatal(err)
	}

	// A state dir puts a disk write inside each snapshot registration, which is
	// what makes the ordering window wide enough for this test to be a test
	// rather than a hope.
	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithWorkers(4),
		loom.WithRetry(quickRetry()), loom.WithTopic("notes"),
		loom.WithStateDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()
	for _, name := range []string{"w", "x", "y", "z"} {
		f.Go(ctx, notePipeline(name, 10, name))
	}

	var wg sync.WaitGroup
	// Report repeatedly while the agents run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = f.Report().String()
		}
	}()
	// Post from several goroutines at once.
	const posters, each = 16, 8
	for p := 0; p < posters; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if _, err := f.Post("notes", fmt.Sprintf("p%d-%d", p, i)); err != nil {
					t.Errorf("post: %v", err)
				}
			}
		}(p)
	}
	wg.Wait()
	if err := f.Wait(); err != nil {
		t.Fatalf("fleet: %v", err)
	}

	posts := f.Posts("notes")
	if len(posts) != posters*each {
		t.Fatalf("board holds %d posts, want %d", len(posts), posters*each)
	}
	for i, p := range posts {
		if p.Seq != i {
			t.Fatalf("post %d carries Seq %d: sequence numbers are not unique", i, p.Seq)
		}
	}

	// The decisive check: the topic's *registered* snapshot must be the longest
	// one, so an agent launched now reads every post rather than a prefix.
	mu.Lock()
	seen = nil
	mu.Unlock()

	p := pipeline.New("reader")
	p.FromRecords("src", []core.Record{core.NewRecord("r1", map[string]any{"q": "?"})}).
		Infer("read", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prompt:  `{{range broadcast "notes"}}[{{.value}}]{{end}} {{.q}}`,
		}, pipeline.WithBroadcast("notes"))
	if _, err := f.Run(ctx, p); err != nil {
		t.Fatalf("reader: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 {
		t.Fatalf("reader made %d calls, want 1", len(seen))
	}
	for p := 0; p < posters; p++ {
		for i := 0; i < each; i++ {
			want := fmt.Sprintf("[p%d-%d]", p, i)
			if !strings.Contains(seen[0], want) {
				t.Errorf("the registered snapshot is missing %s — a concurrent post "+
					"was lost:\n%s", want, seen[0])
			}
		}
	}
}

// A closed fleet must refuse new work rather than run it against released
// state.
func TestFleetClosed(t *testing.T) {
	reg, _ := fleetRegistry(t, 0)
	f, err := loom.NewFleet(loom.WithRegistry(reg))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
	if _, err := f.Run(context.Background(), notePipeline("a", 1, "a")); err == nil {
		t.Error("a closed fleet accepted an agent")
	}
	if _, err := f.Post("t", 1); err == nil {
		t.Error("a closed fleet accepted a post")
	}
}
