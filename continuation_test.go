package loom_test

// What a continuation has to be worth, tested at the level a user sees.
//
// The delta package proves the splice is exact against a full render. These
// tests ask the questions one level up, where the answer is a pipeline's output
// rather than a byte range: does a stage reading an evolving context see the
// whole context, does it see the same thing whichever way the context reached
// it, does the envelope stay small while the context does not, and does the
// cache invalidate the round that changed and nothing else.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/delta"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/task"
)

// echoRegistry records every prompt it is sent and answers deterministically,
// so a test can compare what two runs actually put in front of a model rather
// than comparing what they claimed to.
type echoRegistry struct {
	mu      sync.Mutex
	prompts []string
	stable  []int
}

func echoModels(t *testing.T) (*model.Registry, *echoRegistry) {
	t.Helper()
	reg := model.NewRegistry()
	rec := &echoRegistry{}
	_, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			rec.mu.Lock()
			rec.prompts = append(rec.prompts, req.FullPrompt())
			rec.stable = append(rec.stable, req.Continuation.Stable)
			rec.mu.Unlock()
			return fmt.Sprintf("seen %d bytes", len(req.FullPrompt())), nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	return reg, rec
}

func (e *echoRegistry) last() (string, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.prompts) == 0 {
		return "", 0
	}
	return e.prompts[len(e.prompts)-1], e.stable[len(e.stable)-1]
}

func (e *echoRegistry) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.prompts)
}

// turn is one appended segment, big enough that the difference between shipping
// it and shipping the whole transcript is a real number.
func turn(i int) delta.Segment {
	return delta.Segment{
		Name: fmt.Sprintf("turn%03d", i),
		Body: fmt.Sprintf("turn %d — %s", i, strings.Repeat("context. ", 400)),
	}
}

// sessionPipeline reads the continuation and reports what it saw.
func sessionPipeline(key string) *pipeline.Pipeline {
	p := pipeline.New("session")
	src := p.FromRecords("ask", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "what happened?"}),
	})
	src.Infer("answer", pipeline.InferSpec{
		Prompt:      "Question: {{.q}}",
		Binding:     model.Binding{Tier: model.TierFast},
		OutputField: "answer",
	}, pipeline.WithContinuation(key))
	return p
}

// inlinePipeline is the same stage with the same context carried the old way:
// copied into the envelope as ordinary fragments. It is the control, and the
// only thing that makes "identical bytes" a measurement rather than a claim.
func inlinePipeline(segs []delta.Segment) *pipeline.Pipeline {
	frags := make([]task.Fragment, 0, len(segs))
	for _, s := range segs {
		frags = append(frags, task.Fragment{Name: s.Name, Content: s.Body})
	}
	p := pipeline.New("session")
	src := p.FromRecords("ask", []core.Record{
		core.NewRecord("q1", map[string]any{"q": "what happened?"}),
	})
	src.Infer("answer", pipeline.InferSpec{
		Prompt:      "Question: {{.q}}",
		Binding:     model.Binding{Tier: model.TierFast},
		OutputField: "answer",
		Context:     frags,
	})
	return p
}

// writeSession writes a chain whose first revision holds root turns and which
// then grows by one turn per round, returning every revision.
//
// The root size is a parameter because it decides which regime the router puts
// the workload in, and both regimes are worth testing. A session that starts
// empty spends its first rounds appending as much as it already holds, and the
// router is right to rebuild those: there is nothing worth retaining. A session
// carrying a real context and adding a turn is the shape this is for.
func writeSession(t *testing.T, dir, key string, root, rounds int) []delta.Ref {
	t.Helper()
	cas, err := store.NewCAS(dir + "/cas")
	if err != nil {
		t.Fatal(err)
	}
	chain, err := delta.NewChain(cas, delta.Tags{}, key)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]delta.Segment, root)
	for i := range first {
		first[i] = turn(i)
	}
	ref, err := chain.Root(first...)
	if err != nil {
		t.Fatal(err)
	}
	refs := []delta.Ref{ref}
	for i := range rounds {
		if ref, err = chain.Append(ref, turn(root+i)); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	return refs
}

// TestContinuationDeliversTheWholeContextEveryRound is the baseline promise: a
// stage reading a continuation sees all of it, however little of it was new.
func TestContinuationDeliversTheWholeContextEveryRound(t *testing.T) {
	dir := t.TempDir()
	refs := writeSession(t, dir, "session/a", 1, 5)
	reg, rec := echoModels(t)

	var routes []observe.EventType
	for i, ref := range refs {
		_, err := loom.Run(context.Background(), sessionPipeline("session"),
			loom.WithRegistry(reg), loom.WithStateDir(dir),
			loom.WithContinuation("session", ref),
			loom.WithEventHandler(func(e observe.Event) {
				if e.Type == observe.DeltaSpliced || e.Type == observe.DeltaRebuilt {
					routes = append(routes, e.Type)
				}
			}))
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		prompt, _ := rec.last()
		for j := 0; j <= i; j++ {
			if !strings.Contains(prompt, fmt.Sprintf("turn %d —", j)) {
				t.Fatalf("round %d: the prompt is missing turn %d", i, j)
			}
		}
		if strings.Contains(prompt, fmt.Sprintf("turn %d —", i+1)) {
			t.Fatalf("round %d: the prompt contains a turn that had not happened", i)
		}
	}

	// Each Run is its own process-lifetime here — one host, one state store —
	// so the first round of each run rebuilds and there is nothing to splice
	// onto. What matters is that every round produced a context at all; the
	// splicing is measured where a process lives across rounds, below.
	if len(routes) != len(refs) {
		t.Fatalf("%d materializations across %d rounds", len(routes), len(refs))
	}
}

// TestContinuationMatchesInlineContext is the correctness claim at the level a
// user cares about: the model sees the same bytes whether the context arrived
// as fragments in the envelope or as a chain in shared storage.
func TestContinuationMatchesInlineContext(t *testing.T) {
	dir := t.TempDir()
	rounds := 5
	refs := writeSession(t, dir, "session/a", 1, rounds-1)

	regA, viaChain := echoModels(t)
	regB, viaFragments := echoModels(t)
	for i := range rounds {
		if _, err := loom.Run(context.Background(), sessionPipeline("session"),
			loom.WithRegistry(regA), loom.WithStateDir(dir),
			loom.WithContinuation("session", refs[i])); err != nil {
			t.Fatalf("chain round %d: %v", i, err)
		}
		var frags []delta.Segment
		for j := 0; j <= i; j++ {
			frags = append(frags, turn(j))
		}
		if _, err := loom.Run(context.Background(), inlinePipeline(frags),
			loom.WithRegistry(regB), loom.WithStateDir(t.TempDir())); err != nil {
			t.Fatalf("inline round %d: %v", i, err)
		}
		chained, _ := viaChain.last()
		inline, _ := viaFragments.last()
		if chained != inline {
			t.Fatalf("round %d: the two ways of carrying a context produced different prompts\n"+
				"chain:  %d bytes\ninline: %d bytes", i, len(chained), len(inline))
		}
	}
}

// TestContinuationKeepsTheEnvelopeSmall is the transport claim, measured on the
// thing that actually travels.
func TestContinuationKeepsTheEnvelopeSmall(t *testing.T) {
	dir := t.TempDir()
	refs := writeSession(t, dir, "session/a", 1, 39)
	last := refs[len(refs)-1]

	reg, _ := echoModels(t)
	pl, err := plan.Compile(sessionPipeline("session"), reg,
		plan.WithContinuations(map[string]delta.Ref{"session": last}))
	if err != nil {
		t.Fatal(err)
	}
	env := pl.ByID["answer"].Envelope("run_1", nil)
	blob, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(blob); got > 1024 {
		t.Fatalf("the envelope is %d bytes; a reference should not grow with the session", got)
	}
	if last.Bytes < 100_000 {
		t.Fatalf("the session is only %d bytes; this test proves nothing that small", last.Bytes)
	}
	if env.Context.Chain.Hash != last.Hash {
		t.Fatal("the envelope does not carry the revision it was compiled against")
	}
}

// TestContinuationInvalidatesOnlyTheRoundThatChanged: a revision joins the
// fingerprint, so re-running a round is free and moving to the next one is not.
func TestContinuationInvalidatesOnlyTheRoundThatChanged(t *testing.T) {
	dir := t.TempDir()
	refs := writeSession(t, dir, "session/a", 1, 2)
	reg, rec := echoModels(t)

	run := func(ref delta.Ref) {
		t.Helper()
		if _, err := loom.Run(context.Background(), sessionPipeline("session"),
			loom.WithRegistry(reg), loom.WithStateDir(dir),
			loom.WithContinuation("session", ref)); err != nil {
			t.Fatal(err)
		}
	}

	run(refs[0])
	if rec.count() != 1 {
		t.Fatalf("%d model calls for the first round", rec.count())
	}
	run(refs[0]) // the same revision: nothing to recompute
	if rec.count() != 1 {
		t.Fatalf("%d model calls after replaying an identical round", rec.count())
	}
	run(refs[1]) // one turn later: exactly this round recomputes
	if rec.count() != 2 {
		t.Fatalf("%d model calls after appending a turn", rec.count())
	}
	run(refs[0]) // and the earlier round is still cached
	if rec.count() != 2 {
		t.Fatalf("%d model calls after going back to a round already answered", rec.count())
	}
}

// TestContinuationSplicesWithinOneProcess is the performance claim where it can
// be made: a host that stays up across rounds materializes each revision from
// the one before, and the bytes it does not re-render are counted.
func TestContinuationSplicesWithinOneProcess(t *testing.T) {
	dir := t.TempDir()
	// A real session: a context already worth keeping, growing by a turn.
	refs := writeSession(t, dir, "session/a", 30, 7)
	reg, rec := echoModels(t)

	var spliced, rebuilt int
	var retained, repaired int64
	handler := func(e observe.Event) {
		switch e.Type {
		case observe.DeltaSpliced:
			spliced++
			retained += int64(e.Retained)
			repaired += int64(e.Repaired)
		case observe.DeltaRebuilt:
			rebuilt++
			repaired += int64(e.Repaired)
			t.Logf("round rebuilt: %s", e.Note)
		case observe.DeltaDiverged:
			t.Errorf("a splice diverged from a full render: %s", e.Err)
		}
	}

	// One fleet, therefore one host, therefore one state store across rounds:
	// the shape a service has, as opposed to a batch job that starts fresh.
	fleet, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithStateDir(dir),
		loom.WithEventHandler(handler),
		loom.WithDeltaPolicy(delta.Policy{Verify: 1}))
	if err != nil {
		t.Fatal(err)
	}
	defer fleet.Close()

	for i, ref := range refs {
		if _, err := fleet.Run(context.Background(), sessionPipeline("session"),
			loom.WithContinuation("session", ref)); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
	}

	if rebuilt != 1 {
		t.Fatalf("%d rebuilds, want one — only the first round has nothing to build on", rebuilt)
	}
	if spliced != len(refs)-1 {
		t.Fatalf("%d splices across %d later rounds", spliced, len(refs)-1)
	}
	if retained < 4*repaired {
		t.Fatalf("retained %d B against %d B re-rendered: the fast path is not paying",
			retained, repaired)
	}

	// And what the last request told its provider: a certified count of leading
	// bytes identical to the previous revision's rendering. It is the retained
	// region and nothing else, so it is a number a KV cache could act on rather
	// than a guess it would have to check.
	prompt, stable := rec.last()
	if stable == 0 {
		t.Fatal("a spliced round told its provider nothing was stable")
	}
	if stable > len(prompt) {
		t.Fatalf("stable prefix of %d bytes in a %d byte prompt", stable, len(prompt))
	}
	if float64(stable)/float64(len(prompt)) < 0.8 {
		t.Fatalf("only %d of %d bytes certified stable after appending one turn",
			stable, len(prompt))
	}
}

// TestUnboundContinuationFailsLoudly: a stage that names a continuation the run
// never bound must not quietly run without one.
func TestUnboundContinuationFailsLoudly(t *testing.T) {
	reg, _ := echoModels(t)
	_, err := loom.Run(context.Background(), sessionPipeline("session"),
		loom.WithRegistry(reg))
	if err == nil {
		t.Fatal("a stage reading an unregistered continuation ran anyway")
	}
	if !strings.Contains(err.Error(), "WithContinuation") {
		t.Fatalf("error %q does not say how to fix it", err)
	}
}
