package loom_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/algo"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
)

// --- fixtures -----------------------------------------------------------

// papers is a citation chain p1 → p2 → p3 → p4: the smallest graph on which
// multi-hop propagation is distinguishable from one pass.
func papers() []core.Record {
	return []core.Record{
		core.NewRecord("p1", map[string]any{"title": "p1", "cites": []any{"p2"}}),
		core.NewRecord("p2", map[string]any{"title": "p2", "cites": []any{"p3"}}),
		core.NewRecord("p3", map[string]any{"title": "p3", "cites": []any{"p4"}}),
		core.NewRecord("p4", map[string]any{"title": "p4", "cites": []any{}}),
	}
}

// hopPrompt renders a vertex and its inbox; hopMock answers with one more hop
// than the deepest thing it was told. Together they make the model's output a
// function of (state, inbox) — the contract an iterative stage assumes — while
// still being a real model call through the whole executor path.
const hopPrompt = `{{.title}}|{{range .Inbox}}{{.}};{{end}}`

func hopMock(req model.Request) (string, error) {
	_, inbox, _ := strings.Cut(req.Prompt, "|")
	best := -1
	for _, m := range strings.Split(inbox, ";") {
		if h, ok := strings.CutPrefix(strings.TrimSpace(m), "hop:"); ok {
			if n, err := strconv.Atoi(h); err == nil && n > best {
				best = n
			}
		}
	}
	return fmt.Sprintf("hop:%d", best+1), nil
}

func hopRegistry(t *testing.T) (*model.Registry, *model.Mock) {
	t.Helper()
	reg := model.NewRegistry()
	m, err := model.RegisterMock(reg, "m", model.TierFast, model.WithHandler(hopMock))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return reg, m
}

func hopStep() pipeline.InferSpec {
	return pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast},
		Prompt:  hopPrompt,
	}
}

func mustBSP(t *testing.T, cfg algo.BSPConfig) algo.Algorithm {
	t.Helper()
	a, err := algo.NewBSP(cfg)
	if err != nil {
		t.Fatalf("NewBSP: %v", err)
	}
	return a
}

func outputs(recs []core.Record) map[string]string {
	out := map[string]string{}
	for _, r := range recs {
		out[r.ID] = r.String("output")
	}
	return out
}

// --- convergence --------------------------------------------------------

// A message entering one end of a chain should walk it hop by hop and then
// stop of its own accord: one round per edge, and quiet when the last vertex
// has nowhere left to send.
func TestIterateBSPPropagatesAlongChain(t *testing.T) {
	reg, mock := hopRegistry(t)
	p := pipeline.New("citations")
	p.FromRecords("papers", papers()).Iterate("propagate", pipeline.IterateSpec{
		Step: hopStep(),
		Algorithm: mustBSP(t, algo.BSPConfig{
			Edges:    algo.EdgesFromField("cites"),
			Emit:     algo.MessagesFromField("output"),
			Seeds:    func(r core.Record) bool { return r.ID == "p1" },
			Directed: true,
		}),
		Halt: pipeline.HaltWhen{MaxRounds: 10},
	})

	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	got := outputs(res.Output)
	want := map[string]string{"p1": "hop:0", "p2": "hop:1", "p3": "hop:2", "p4": "hop:3"}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("vertex %s = %q, want %q", id, got[id], w)
		}
	}

	it, ok := res.Iteration("propagate")
	if !ok {
		t.Fatal("no iteration report for stage propagate")
	}
	if it.Halt != loom.HaltQuiet {
		t.Errorf("halt = %q, want %q", it.Halt, loom.HaltQuiet)
	}
	if !it.Halt.Converged() {
		t.Error("quiet halt should report as converged")
	}
	if it.Rounds != 4 {
		t.Errorf("rounds = %d, want 4 (one per vertex in the chain)", it.Rounds)
	}
	if want := []int{1, 1, 1, 1}; !equalInts(it.Active, want) {
		t.Errorf("active per round = %v, want %v", it.Active, want)
	}
	// One call per active vertex per round, and no vertex ran twice.
	if mock.Calls() != 4 {
		t.Errorf("model calls = %d, want 4", mock.Calls())
	}
	if it.Algorithm != "bsp" {
		t.Errorf("algorithm = %q, want %q", it.Algorithm, "bsp")
	}
}

// Ordering is a correctness property here, not a nicety: an inbox is part of a
// cache key, so the same messages arriving in a different order must produce
// the same key. Two neighbours reporting into one vertex is the smallest case
// that can get it wrong.
func TestIterateInboxIsCanonicallyOrdered(t *testing.T) {
	reg := model.NewRegistry()
	var seen []string
	if _, err := model.RegisterMock(reg, "m", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			if _, inbox, _ := strings.Cut(req.Prompt, "|"); inbox != "" {
				seen = append(seen, inbox)
			}
			return "ok", nil
		})); err != nil {
		t.Fatalf("register: %v", err)
	}

	// b and c both cite d; their messages land in d's inbox together.
	recs := []core.Record{
		core.NewRecord("b", map[string]any{"title": "b", "cites": []any{"d"}, "note": "zebra"}),
		core.NewRecord("c", map[string]any{"title": "c", "cites": []any{"d"}, "note": "apple"}),
		core.NewRecord("d", map[string]any{"title": "d", "cites": []any{}, "note": ""}),
	}
	p := pipeline.New("fan-in")
	p.FromRecords("src", recs).Iterate("gather", pipeline.IterateSpec{
		Step: hopStep(),
		Algorithm: mustBSP(t, algo.BSPConfig{
			Edges:    algo.EdgesFromField("cites"),
			Emit:     algo.MessagesFromField("note"),
			Seeds:    func(r core.Record) bool { return r.ID != "d" },
			Directed: true,
		}),
		Halt: pipeline.HaltWhen{MaxRounds: 5},
	})
	if _, err := loom.Run(context.Background(), p, loom.WithRegistry(reg)); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(seen) != 1 {
		t.Fatalf("expected exactly one vertex with a non-empty inbox, got %d: %v", len(seen), seen)
	}
	// Equal ranks fall back to sender, then body: b's "zebra" precedes c's
	// "apple" because the sender decides, not the text.
	if want := "zebra;apple;"; seen[0] != want {
		t.Errorf("inbox order = %q, want %q (sender, then body)", seen[0], want)
	}
}

// --- halting ------------------------------------------------------------

// alwaysAlgo keeps every vertex active forever. Body controls whether the
// messages it sends are new information (novel) or a repeat.
type alwaysAlgo struct{ novel bool }

func (alwaysAlgo) Name() string { return "always" }

func (a alwaysAlgo) Seed(g algo.Graph) ([]algo.Message, error) {
	var out []algo.Message
	for _, id := range g.IDs() {
		out = append(out, algo.Message{To: id})
	}
	return out, nil
}

func (a alwaysAlgo) Route(r algo.Round) ([]algo.Message, error) {
	var out []algo.Message
	for _, st := range r.Steps {
		body := "same"
		if a.novel {
			body = fmt.Sprintf("round-%d", r.N)
		}
		out = append(out, algo.Message{To: st.ID(), From: st.ID(), Body: body})
	}
	return out, nil
}

// A vertex that receives an inbox it has already run on cannot produce
// anything new, so the loop should stop there rather than paying out the round
// cap discovering it. This is the case a superstep cap alone handles badly:
// the answer arrives in round 2 and the cap is 10.
func TestIterateHaltsOnFixpointBeforeRoundCap(t *testing.T) {
	reg, mock := hopRegistry(t)
	p := pipeline.New("stuck")
	p.FromRecords("src", papers()[:1]).Iterate("spin", pipeline.IterateSpec{
		Step:      hopStep(),
		Algorithm: alwaysAlgo{},
		Halt:      pipeline.HaltWhen{MaxRounds: 10},
	})

	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	it, _ := res.Iteration("spin")
	if it.Halt != loom.HaltFixpoint {
		t.Errorf("halt = %q, want %q", it.Halt, loom.HaltFixpoint)
	}
	if it.Rounds != 2 {
		t.Errorf("rounds = %d, want 2 (round 3's input repeats round 2's)", it.Rounds)
	}
	if it.Quiesced != 1 {
		t.Errorf("quiesced = %d, want 1", it.Quiesced)
	}
	if mock.Calls() != 2 {
		t.Errorf("model calls = %d, want 2: the repeat must not be paid for", mock.Calls())
	}
}

// When every round genuinely brings new information the loop cannot converge,
// and the cap is the only thing that stops it. It must stop exactly on the
// cap: an off-by-one here is a round of real money.
func TestIterateHaltsOnRoundCap(t *testing.T) {
	reg, mock := hopRegistry(t)
	p := pipeline.New("endless")
	p.FromRecords("src", papers()[:1]).Iterate("spin", pipeline.IterateSpec{
		Step:      hopStep(),
		Algorithm: alwaysAlgo{novel: true},
		Halt:      pipeline.HaltWhen{MaxRounds: 3},
	})

	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	it, _ := res.Iteration("spin")
	if it.Halt != loom.HaltRounds {
		t.Errorf("halt = %q, want %q", it.Halt, loom.HaltRounds)
	}
	if it.Halt.Converged() {
		t.Error("a round-capped loop must not report as converged")
	}
	if it.Rounds != 3 || mock.Calls() != 3 {
		t.Errorf("rounds = %d, calls = %d, want 3 and 3", it.Rounds, mock.Calls())
	}
}

// The stage budget stops the loop and lets the pipeline continue, which is the
// distinction from the run governor: an unconverged loop is not a failed run.
func TestIterateHaltsOnStageBudget(t *testing.T) {
	reg, _ := hopRegistry(t)
	p := pipeline.New("pricey")
	src := p.FromRecords("src", papers()[:1])
	iter := src.Iterate("spin", pipeline.IterateSpec{
		Step:      hopStep(),
		Algorithm: alwaysAlgo{novel: true},
		Halt: pipeline.HaltWhen{
			MaxRounds: 50,
			Budget:    core.Budget{MaxTokens: 20},
		},
	})
	iter.Map("after", func(r core.Record) (core.Record, error) {
		r.Data["downstream"] = true
		return r, nil
	})

	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	it, _ := res.Iteration("spin")
	if it.Halt != loom.HaltBudget {
		t.Errorf("halt = %q, want %q", it.Halt, loom.HaltBudget)
	}
	if it.Rounds >= 50 {
		t.Errorf("rounds = %d, want the budget to stop it well short of the cap", it.Rounds)
	}
	if it.Usage.TotalTokens() < 20 {
		t.Errorf("stopped at %d tokens, before the budget it was given", it.Usage.TotalTokens())
	}
	// The pipeline continued past the unconverged loop with what it reached.
	if got := res.StageOutputs["after"]; len(got) != 1 || got[0].Data["downstream"] != true {
		t.Errorf("downstream stage did not run on the loop's partial result: %v", got)
	}
}

// --- growth and pruning -------------------------------------------------

// searchAlgo proposes two successors per candidate for one level, then stops.
// It is the smallest algorithm that grows its own graph.
type searchAlgo struct{ depth int }

func (searchAlgo) Name() string { return "search" }

func (searchAlgo) Seed(g algo.Graph) ([]algo.Message, error) {
	var out []algo.Message
	for _, id := range g.IDs() {
		out = append(out, algo.Message{To: id})
	}
	return out, nil
}

func (s searchAlgo) Route(r algo.Round) ([]algo.Message, error) {
	if r.N >= s.depth {
		return nil, nil
	}
	var out []algo.Message
	for _, st := range r.Steps {
		for i := 0; i < 2; i++ {
			out = append(out, algo.Message{
				To:   fmt.Sprintf("%s.%d", st.ID(), i),
				From: st.ID(),
				Body: fmt.Sprintf("branch %d", i),
				Rank: float64(i),
			})
		}
	}
	return out, nil
}

// A message to a vertex nobody declared is the open-world case: with Grow it
// creates the vertex, without it the message is dropped and counted. Both
// behaviours have to be visible, because "found nothing" and "was not allowed
// to look" produce identical output.
func TestIterateGrowsGraphOnDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name          string
		grow          bool
		wantVertices  int
		wantGrown     int
		wantDropped   int
		wantMinRounds int
	}{
		{name: "open world", grow: true, wantVertices: 7, wantGrown: 6, wantMinRounds: 3},
		{name: "closed world", grow: false, wantVertices: 1, wantDropped: 2, wantMinRounds: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg, _ := hopRegistry(t)
			spec := pipeline.IterateSpec{
				Step:      hopStep(),
				Algorithm: searchAlgo{depth: 2},
				Halt:      pipeline.HaltWhen{MaxRounds: 5},
			}
			if tc.grow {
				spec.Grow = func(id string, msgs []algo.Message) (core.Record, error) {
					return core.NewRecord(id, map[string]any{"title": id}), nil
				}
			}
			p := pipeline.New("search")
			p.FromRecords("root", []core.Record{
				core.NewRecord("r", map[string]any{"title": "r"}),
			}).Iterate("expand", spec)

			res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			it, _ := res.Iteration("expand")
			if it.Vertices != tc.wantVertices {
				t.Errorf("vertices = %d, want %d", it.Vertices, tc.wantVertices)
			}
			if it.Grown != tc.wantGrown {
				t.Errorf("grown = %d, want %d", it.Grown, tc.wantGrown)
			}
			if it.Dropped != tc.wantDropped {
				t.Errorf("dropped = %d, want %d", it.Dropped, tc.wantDropped)
			}
			if it.Rounds < tc.wantMinRounds {
				t.Errorf("rounds = %d, want at least %d", it.Rounds, tc.wantMinRounds)
			}
			if len(res.Output) != tc.wantVertices {
				t.Errorf("output records = %d, want %d", len(res.Output), tc.wantVertices)
			}
		})
	}
}

// MaxFrontier is the cap that bounds the bill: per-vertex message limits do
// not, because a thousand vertices sending a legal two messages each is still
// a two-thousand-call round.
func TestIterateMaxFrontierBoundsTheRound(t *testing.T) {
	reg, mock := hopRegistry(t)
	p := pipeline.New("bounded")
	p.FromRecords("root", []core.Record{
		core.NewRecord("r", map[string]any{"title": "r"}),
	}).Iterate("expand", pipeline.IterateSpec{
		Step:      hopStep(),
		Algorithm: searchAlgo{depth: 3},
		Grow: func(id string, msgs []algo.Message) (core.Record, error) {
			return core.NewRecord(id, map[string]any{"title": id}), nil
		},
		MaxFrontier: 2,
		Halt:        pipeline.HaltWhen{MaxRounds: 4},
	})

	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	it, _ := res.Iteration("expand")
	for i, n := range it.Active {
		if n > 2 {
			t.Errorf("round %d ran %d vertices, above the frontier cap of 2", i, n)
		}
	}
	// 1 + 2 + 2 + 2 with the cap; 1 + 2 + 4 + 8 without it.
	if mock.Calls() > 7 {
		t.Errorf("model calls = %d, want at most 7 under a frontier cap of 2", mock.Calls())
	}
	if it.Truncated == 0 {
		t.Error("truncated = 0, but the frontier cap must have discarded messages")
	}
}

// MaxInbox keeps a high-degree vertex's prompt from growing with its degree.
func TestIterateMaxInboxTruncates(t *testing.T) {
	reg := model.NewRegistry()
	var widest int
	if _, err := model.RegisterMock(reg, "m", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			_, inbox, _ := strings.Cut(req.Prompt, "|")
			if n := strings.Count(inbox, ";"); n > widest {
				widest = n
			}
			return "ok", nil
		})); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Five senders, all pointing at one hub.
	var recs []core.Record
	for i := 0; i < 5; i++ {
		recs = append(recs, core.NewRecord(fmt.Sprintf("s%d", i), map[string]any{
			"title": fmt.Sprintf("s%d", i), "cites": []any{"hub"}, "note": fmt.Sprintf("n%d", i),
		}))
	}
	recs = append(recs, core.NewRecord("hub", map[string]any{"title": "hub", "cites": []any{}, "note": ""}))

	p := pipeline.New("hub")
	p.FromRecords("src", recs).Iterate("gather", pipeline.IterateSpec{
		Step: hopStep(),
		Algorithm: mustBSP(t, algo.BSPConfig{
			Edges:    algo.EdgesFromField("cites"),
			Emit:     algo.MessagesFromField("note"),
			Seeds:    func(r core.Record) bool { return r.ID != "hub" },
			Directed: true,
		}),
		MaxInbox: 2,
		Halt:     pipeline.HaltWhen{MaxRounds: 4},
	})

	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if widest > 2 {
		t.Errorf("hub read %d messages, above the inbox cap of 2", widest)
	}
	it, _ := res.Iteration("gather")
	if it.Truncated != 3 {
		t.Errorf("truncated = %d, want 3 (5 senders capped to 2)", it.Truncated)
	}
}

// --- caching ------------------------------------------------------------

// A vertex's cache key is its state and its inbox — deliberately not the round
// it is in — and this is the test that says what that buys. Rerunning an
// iterative stage replays it for nothing, and editing one vertex recomputes
// that vertex plus whatever its change actually reaches, which is what a
// purpose-built graph engine needs a subsystem for.
//
// The third case is the interesting one: p1 changes, but p1's *message* does
// not, so p2 downstream stays cached. Incremental recomputation follows what
// moved, not what was touched.
func TestIterateRecomputesIncrementally(t *testing.T) {
	dir := t.TempDir()

	// run executes the chain against a fresh mock and reports the calls it
	// made, so each run's cost is measured rather than accumulated.
	run := func(recs []core.Record) (int, map[string]string) {
		t.Helper()
		reg, mock := hopRegistry(t)
		p := pipeline.New("citations")
		p.FromRecords("papers", recs).Iterate("propagate", pipeline.IterateSpec{
			Step: hopStep(),
			Algorithm: mustBSP(t, algo.BSPConfig{
				Edges:    algo.EdgesFromField("cites"),
				Emit:     algo.MessagesFromField("output"),
				Seeds:    func(r core.Record) bool { return r.ID == "p1" },
				Directed: true,
			}),
			Halt: pipeline.HaltWhen{MaxRounds: 10},
		})
		res, err := loom.Run(context.Background(), p,
			loom.WithRegistry(reg), loom.WithStateDir(dir))
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return mock.Calls(), outputs(res.Output)
	}

	calls, first := run(papers())
	if calls != 4 {
		t.Fatalf("first run made %d calls, want 4", calls)
	}

	calls, again := run(papers())
	if calls != 0 {
		t.Errorf("rerun made %d calls, want 0: every round should replay from cache", calls)
	}
	for id, want := range first {
		if again[id] != want {
			t.Errorf("replayed %s = %q, want %q", id, again[id], want)
		}
	}

	// Edit the last vertex in the chain: three rounds replay, one recomputes.
	edited := papers()
	edited[3].Data["title"] = "p4 revised"
	if calls, _ = run(edited); calls != 1 {
		t.Errorf("editing the chain's tail made %d calls, want 1", calls)
	}

	// Edit the head. p1 recomputes, but hop:0 is what it said before, so p2's
	// inbox is unchanged and the rest of the chain stays warm.
	edited = papers()
	edited[0].Data["title"] = "p1 revised"
	if calls, _ = run(edited); calls != 1 {
		t.Errorf("editing the chain's head made %d calls, want 1: an unchanged "+
			"message must leave downstream vertices cached", calls)
	}
}

// --- refine -------------------------------------------------------------

// The fourth workload one pass cannot express, and the shape that shows the
// interface is about control flow rather than graphs: a vertex looping on
// itself, halting when its own output passes a Go predicate.
func TestIterateRefineUntilAccepted(t *testing.T) {
	reg := model.NewRegistry()
	// Each pass appends a "+"; three are needed to be accepted.
	if _, err := model.RegisterMock(reg, "m", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			return strings.Repeat("+", strings.Count(req.Prompt, "revise")+1), nil
		})); err != nil {
		t.Fatalf("register: %v", err)
	}

	refine, err := algo.NewRefine(algo.RefineConfig{
		Accept: func(r core.Record) bool { return len(r.String("output")) >= 3 },
		Note:   func(r core.Record) string { return "revise " + r.String("output") },
		Carry:  true,
	})
	if err != nil {
		t.Fatalf("NewRefine: %v", err)
	}

	p := pipeline.New("drafting")
	p.FromRecords("drafts", []core.Record{
		core.NewRecord("d1", map[string]any{"title": "draft"}),
	}).Iterate("refine", pipeline.IterateSpec{
		Step:      hopStep(),
		Algorithm: refine,
		Halt:      pipeline.HaltWhen{MaxRounds: 8},
	})

	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	it, _ := res.Iteration("refine")
	if it.Halt != loom.HaltQuiet {
		t.Errorf("halt = %q, want %q (Accept passed)", it.Halt, loom.HaltQuiet)
	}
	if it.Rounds != 3 {
		t.Errorf("rounds = %d, want 3", it.Rounds)
	}
	if got := res.Output[0].String("output"); got != "+++" {
		t.Errorf("output = %q, want %q", got, "+++")
	}
	// The inbox fields are the stage's, not the record's: they must not
	// survive into what downstream stages see.
	for _, f := range algo.Reserved() {
		if _, leaked := res.Output[0].Data[f]; leaked {
			t.Errorf("reserved field %q leaked into the stage output", f)
		}
	}
}

// --- driver parity and validation ---------------------------------------

// A round hands its tasks to whichever runner the driver supplies, so the two
// drivers must produce the same computation. This is the same guarantee the
// barrier and streaming drivers already give each other for ReduceAI.
func TestIterateSameUnderBothDrivers(t *testing.T) {
	run := func(opts ...loom.Option) map[string]string {
		t.Helper()
		reg, _ := hopRegistry(t)
		p := pipeline.New("citations")
		p.FromRecords("papers", papers()).Iterate("propagate", pipeline.IterateSpec{
			Step: hopStep(),
			Algorithm: mustBSP(t, algo.BSPConfig{
				Edges:    algo.EdgesFromField("cites"),
				Emit:     algo.MessagesFromField("output"),
				Seeds:    func(r core.Record) bool { return r.ID == "p1" },
				Directed: true,
			}),
			Halt: pipeline.HaltWhen{MaxRounds: 10},
		})
		res, err := loom.Run(context.Background(), p, append(opts, loom.WithRegistry(reg))...)
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return outputs(res.Output)
	}

	barrier := run()
	streaming := run(loom.WithStreaming())
	if len(barrier) != len(streaming) {
		t.Fatalf("barrier produced %d vertices, streaming %d", len(barrier), len(streaming))
	}
	for id, want := range barrier {
		if streaming[id] != want {
			t.Errorf("vertex %s: barrier %q, streaming %q", id, want, streaming[id])
		}
	}
}

// Rounds are observable: a viewer that cannot see the frontier shrink cannot
// tell a converging computation from a stuck one.
func TestIteratePublishesRoundEvents(t *testing.T) {
	reg, _ := hopRegistry(t)
	var rounds []int
	var converged observe.Event
	p := pipeline.New("citations")
	p.FromRecords("papers", papers()).Iterate("propagate", pipeline.IterateSpec{
		Step: hopStep(),
		Algorithm: mustBSP(t, algo.BSPConfig{
			Edges:    algo.EdgesFromField("cites"),
			Emit:     algo.MessagesFromField("output"),
			Seeds:    func(r core.Record) bool { return r.ID == "p1" },
			Directed: true,
		}),
		Halt: pipeline.HaltWhen{MaxRounds: 10},
	})

	var mu sync.Mutex
	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg),
		loom.WithEventHandler(func(e observe.Event) {
			mu.Lock()
			defer mu.Unlock()
			switch e.Type {
			case observe.RoundStarted:
				rounds = append(rounds, e.Round)
			case observe.StageConverged:
				converged = e
			}
		}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if want := []int{1, 2, 3, 4}; !equalInts(rounds, want) {
		t.Errorf("round.started rounds = %v, want %v", rounds, want)
	}
	if converged.Note != string(loom.HaltQuiet) {
		t.Errorf("stage.converged note = %q, want %q", converged.Note, loom.HaltQuiet)
	}
	if converged.Round != 4 {
		t.Errorf("stage.converged round = %d, want 4", converged.Round)
	}
	for _, s := range res.Report.Stages {
		if s.Stage == "propagate" && s.Rounds != 4 {
			t.Errorf("report rounds = %d, want 4", s.Rounds)
		}
	}
}

// A loop with no bound on it is the one authoring mistake whose cost is
// unbounded, so it must not compile.
func TestIterateRejectsUnboundedAndMalformedStages(t *testing.T) {
	base := func(spec pipeline.IterateSpec) *pipeline.Pipeline {
		p := pipeline.New("bad")
		p.FromRecords("src", papers()).Iterate("loop", spec)
		return p
	}
	good := mustBSP(t, algo.BSPConfig{Edges: algo.EdgesFromField("cites")})

	for _, tc := range []struct {
		name string
		spec pipeline.IterateSpec
		want string
	}{
		{
			name: "no round cap",
			spec: pipeline.IterateSpec{Step: hopStep(), Algorithm: good},
			want: "MaxRounds",
		},
		{
			name: "no algorithm",
			spec: pipeline.IterateSpec{Step: hopStep(), Halt: pipeline.HaltWhen{MaxRounds: 3}},
			want: "algorithm",
		},
		{
			name: "no prompt",
			spec: pipeline.IterateSpec{
				Step:      pipeline.InferSpec{Binding: model.Binding{Tier: model.TierFast}},
				Algorithm: good, Halt: pipeline.HaltWhen{MaxRounds: 3},
			},
			want: "prompt",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg, _ := hopRegistry(t)
			_, err := loom.Run(context.Background(), base(tc.spec), loom.WithRegistry(reg))
			if err == nil {
				t.Fatal("expected compilation to fail")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// Vertices are addressed by ID, which makes duplicate IDs ambiguous rather
// than merely untidy, and makes the reserved inbox fields unavailable.
func TestIterateRejectsAmbiguousVertices(t *testing.T) {
	for _, tc := range []struct {
		name string
		recs []core.Record
		want string
	}{
		{
			name: "duplicate IDs",
			recs: []core.Record{
				core.NewRecord("x", map[string]any{"title": "a"}),
				core.NewRecord("x", map[string]any{"title": "b"}),
			},
			want: "duplicate",
		},
		{
			name: "reserved field",
			recs: []core.Record{
				core.NewRecord("x", map[string]any{"title": "a", algo.FieldInbox: "mine"}),
			},
			want: algo.FieldInbox,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg, _ := hopRegistry(t)
			p := pipeline.New("bad")
			p.FromRecords("src", tc.recs).Iterate("loop", pipeline.IterateSpec{
				Step:      hopStep(),
				Algorithm: alwaysAlgo{},
				Halt:      pipeline.HaltWhen{MaxRounds: 2},
			})
			_, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// An iterative stage must sit in a pipeline like any other: fed by upstream
// stages, feeding downstream ones, without the loop leaking out.
func TestIterateComposesWithOtherStages(t *testing.T) {
	reg, _ := hopRegistry(t)
	p := pipeline.New("composed")
	p.FromRecords("papers", papers()).
		Map("mark", func(r core.Record) (core.Record, error) {
			r.Data["title"] = strings.ToUpper(r.String("title"))
			return r, nil
		}).
		Iterate("propagate", pipeline.IterateSpec{
			Step: hopStep(),
			Algorithm: mustBSP(t, algo.BSPConfig{
				Edges:    algo.EdgesFromField("cites"),
				Emit:     algo.MessagesFromField("output"),
				Seeds:    func(r core.Record) bool { return r.ID == "p1" },
				Directed: true,
			}),
			Halt: pipeline.HaltWhen{MaxRounds: 10},
		}).
		Filter("reached", func(r core.Record) (bool, error) {
			return r.String("output") != "", nil
		}).
		Combine("count", func(a, b core.Record) (core.Record, error) {
			n, _ := a.Data["n"].(int)
			if n == 0 {
				n = 1
			}
			a.Data["n"] = n + 1
			return a, nil
		})

	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := res.Output[0].Data["n"]; got != 4 {
		t.Errorf("records reaching the fold = %v, want 4", got)
	}
	if got := res.Output[0].String("title"); got != "P1" {
		t.Errorf("upstream transform lost: title = %q, want %q", got, "P1")
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- projection ---------------------------------------------------------

// A loop over paid calls cannot ship without pre-flight pricing, and the
// number it has to produce is the one HaltWhen.Budget needs: what the stage
// costs if it never converges.
func TestExplainPricesIterativeStages(t *testing.T) {
	reg, mock := hopRegistry(t)
	p := pipeline.New("citations")
	p.FromRecords("papers", papers()).Iterate("propagate", pipeline.IterateSpec{
		Step: hopStep(),
		Algorithm: mustBSP(t, algo.BSPConfig{
			Edges:    algo.EdgesFromField("cites"),
			Emit:     algo.MessagesFromField("output"),
			Seeds:    func(r core.Record) bool { return r.ID == "p1" },
			Directed: true,
		}),
		Halt: pipeline.HaltWhen{MaxRounds: 6},
	})

	proj, err := loom.Explain(p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if mock.Calls() != 0 {
		t.Errorf("Explain issued %d model calls; it must issue none", mock.Calls())
	}

	var stage loom.StageProjection
	for _, s := range proj.Stages {
		if s.Stage == "propagate" {
			stage = s
		}
	}
	// Round 0's frontier is exact — Seed is a pure function, so the projection
	// asks it rather than assuming all four records start active. Later rounds
	// are bounded by the vertex count, because a closed graph cannot grow one.
	if want := 1 + 5*4; stage.Calls != want {
		t.Errorf("projected calls = %d, want %d (1 seeded + 5 rounds × 4 vertices)",
			stage.Calls, want)
	}
	if stage.Ceiling.CostUSD < stage.Usage.CostUSD {
		t.Error("ceiling must not be below the expected case")
	}
	if proj.Partial() {
		t.Error("a closed-world iterative stage is bounded and should not be partial")
	}
	// The real run converges in four rounds, so the projection must be an
	// over-estimate — the only safe direction for a number a budget is set from.
	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if actual := res.Spent.TotalTokens(); actual > stage.Ceiling.TotalTokens() {
		t.Errorf("run spent %d tokens, above the projected ceiling of %d",
			actual, stage.Ceiling.TotalTokens())
	}
}

// A stage that can create vertices and caps neither the frontier nor the graph
// has no bound the plan can compute. Reporting a confident number there is the
// one failure this tool exists to prevent.
func TestExplainMarksUnboundedIterativeStages(t *testing.T) {
	reg, _ := hopRegistry(t)
	p := pipeline.New("open")
	p.FromRecords("root", []core.Record{
		core.NewRecord("r", map[string]any{"title": "r"}),
	}).Iterate("expand", pipeline.IterateSpec{
		Step:      hopStep(),
		Algorithm: searchAlgo{depth: 3},
		Grow: func(id string, msgs []algo.Message) (core.Record, error) {
			return core.NewRecord(id, map[string]any{"title": id}), nil
		},
		Halt: pipeline.HaltWhen{MaxRounds: 4},
	})

	proj, err := loom.Explain(p, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !proj.Partial() {
		t.Error("an unbounded frontier must make the projection partial")
	}
	var mentioned bool
	for _, w := range proj.Warnings {
		if strings.Contains(w, "MaxFrontier") {
			mentioned = true
		}
	}
	if !mentioned {
		t.Errorf("warnings do not name the missing bound: %v", proj.Warnings)
	}

	// With the cap in place the stage is bounded again, and the bound is exact.
	p2 := pipeline.New("bounded")
	p2.FromRecords("root", []core.Record{
		core.NewRecord("r", map[string]any{"title": "r"}),
	}).Iterate("expand", pipeline.IterateSpec{
		Step:      hopStep(),
		Algorithm: searchAlgo{depth: 3},
		Grow: func(id string, msgs []algo.Message) (core.Record, error) {
			return core.NewRecord(id, map[string]any{"title": id}), nil
		},
		MaxFrontier: 2,
		Halt:        pipeline.HaltWhen{MaxRounds: 4},
	})
	proj2, err := loom.Explain(p2, loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if proj2.Partial() {
		t.Error("a frontier-capped stage is bounded and should not be partial")
	}
	for _, s := range proj2.Stages {
		if s.Stage == "expand" && s.Calls != 1+3*2 {
			t.Errorf("projected calls = %d, want 7 (1 seeded + 3 rounds × 2)", s.Calls)
		}
	}
}

// --- failure inside a round ---------------------------------------------

// A vertex whose task dead-letters keeps the state it had and emits nothing,
// so it goes quiet while the rest of the graph carries on. That is the only
// coherent answer: the loop cannot invent the output the model did not return,
// and stopping the whole computation over one vertex would throw away every
// round already paid for.
func TestIterateContinuesPastAFailedVertex(t *testing.T) {
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "m", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			if strings.Contains(req.Prompt, "p2") {
				return "", core.Permanent(errors.New("scripted failure on p2"))
			}
			return hopMock(req)
		})); err != nil {
		t.Fatalf("register: %v", err)
	}

	p := pipeline.New("citations")
	p.FromRecords("papers", papers()).Iterate("propagate", pipeline.IterateSpec{
		Step: hopStep(),
		Algorithm: mustBSP(t, algo.BSPConfig{
			Edges:    algo.EdgesFromField("cites"),
			Emit:     algo.MessagesFromField("output"),
			Seeds:    func(r core.Record) bool { return r.ID == "p1" },
			Directed: true,
		}),
		Halt: pipeline.HaltWhen{MaxRounds: 10},
	})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithContinueOnError(), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	it, _ := res.Iteration("propagate")
	if it.Halt != loom.HaltQuiet {
		t.Errorf("halt = %q, want %q: one dead vertex is not a failed loop", it.Halt, loom.HaltQuiet)
	}
	if len(res.Failures) == 0 {
		t.Error("the failing vertex was not dead-lettered")
	}
	got := outputs(res.Output)
	if got["p1"] != "hop:0" {
		t.Errorf("p1 = %q, want hop:0", got["p1"])
	}
	// p2 never produced output, so the wave stops there: p3 and p4 are never
	// activated rather than activated with a hole in their inbox.
	if got["p2"] != "" {
		t.Errorf("p2 = %q, want empty: its task failed", got["p2"])
	}
	if got["p3"] != "" || got["p4"] != "" {
		t.Errorf("p3 = %q, p4 = %q, want both unreached behind the failure", got["p3"], got["p4"])
	}
	// Every vertex is still returned, so downstream stages see the whole graph.
	if len(res.Output) != 4 {
		t.Errorf("output records = %d, want all 4 vertices", len(res.Output))
	}
}

// Without ContinueOnError a failing vertex aborts the run, and the partial
// state is still returned alongside the error.
func TestIterateAbortsWithoutContinueOnError(t *testing.T) {
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "m", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			return "", core.Permanent(errors.New("scripted failure"))
		})); err != nil {
		t.Fatalf("register: %v", err)
	}

	p := pipeline.New("citations")
	p.FromRecords("papers", papers()).Iterate("propagate", pipeline.IterateSpec{
		Step:      hopStep(),
		Algorithm: alwaysAlgo{novel: true},
		Halt:      pipeline.HaltWhen{MaxRounds: 5},
	})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err == nil {
		t.Fatal("expected the run to fail")
	}
	it, ok := res.Iteration("propagate")
	if !ok {
		t.Fatal("a failed loop must still report how far it got")
	}
	if it.Halt != loom.HaltFailed {
		t.Errorf("halt = %q, want %q", it.Halt, loom.HaltFailed)
	}
}
