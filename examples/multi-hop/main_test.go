package main

import (
	"context"
	"strings"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
)

func runExample(t *testing.T, opts ...loom.Option) *loom.RunResult {
	t.Helper()
	return runRounds(t, 5, opts...)
}

func runRounds(t *testing.T, rounds int, opts ...loom.Option) *loom.RunResult {
	t.Helper()
	p, err := build(rounds, 2.00)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	base := []loom.Option{
		loom.WithRegistry(registry()),
		loom.WithBroadcast("question", question),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 5}),
	}
	res, err := loom.Run(context.Background(), p, append(base, opts...)...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return res
}

// The claim the example exists to demonstrate: the walk reaches a paper that
// no seed cites directly, so the conclusion is one no single pass could have
// produced. p7 is three hops from the seeds, through p3 → p4 → p7.
func TestWalkReachesPapersNoSeedCites(t *testing.T) {
	res := runExample(t)

	reached := map[string]string{}
	for _, r := range res.StageOutputs["explore"] {
		reached[r.ID] = r.String("finding")
	}
	for _, id := range []string{"p1", "p3", "p4", "p7"} {
		if reached[id] == "" {
			t.Errorf("paper %s was never activated; the walk did not reach it", id)
		}
	}
	// p6 and p8 are in the corpus but nothing cites them and they are not
	// seeds: a walk that reads them is not selecting, it is scanning.
	for _, id := range []string{"p6", "p8"} {
		if reached[id] != "" {
			t.Errorf("paper %s was activated, but nothing points at it", id)
		}
	}
	if len(res.Output) == 0 || !strings.Contains(res.Output[0].String("output"), "three hops") {
		t.Errorf("synthesis missed the conclusion that needed the extra hops: %v", res.Output)
	}
}

// The depth has to be load-bearing, or this is a fan-out with extra steps. Cut
// the walk off before it reaches p7 and the same pipeline over the same corpus
// cannot answer the question — which is the whole argument for the operator.
func TestTruncatedWalkCannotAnswer(t *testing.T) {
	res := runRounds(t, 2)

	it, _ := res.Iteration("explore")
	if it.Halt != loom.HaltRounds {
		t.Errorf("halt = %q, want %q: two rounds must not be enough", it.Halt, loom.HaltRounds)
	}
	for _, r := range res.StageOutputs["explore"] {
		if r.ID == "p7" && r.String("finding") != "" {
			t.Error("p7 was reached in two rounds; it should be three hops out")
		}
	}
	if len(res.Output) == 0 {
		t.Fatal("no synthesis")
	}
	if got := res.Output[0].String("output"); strings.Contains(got, "three hops") {
		t.Errorf("a truncated walk still produced the conclusion: %q", got)
	}
	// It still produces an answer, and the answer says what it does not know.
	// A loop that halts on its round cap returns real partial state, not an
	// error — the halt reason is what tells you it is partial.
	if !strings.Contains(res.Output[0].String("output"), "not settled") {
		t.Errorf("truncated synthesis = %q, want it to report the gap",
			res.Output[0].String("output"))
	}
}

// The frontier has to shrink, or the design's economic claim is false: a walk
// whose rounds do not get cheaper is a walk that never converges.
func TestFrontierConvergesAndGraphGrows(t *testing.T) {
	res := runExample(t)
	it, ok := res.Iteration("explore")
	if !ok {
		t.Fatal("no iteration report")
	}
	if it.Halt != loom.HaltQuiet {
		t.Errorf("halt = %q, want the walk to go quiet on its own", it.Halt)
	}
	if len(it.Active) < 3 {
		t.Fatalf("only %d rounds ran; the walk is not multi-hop", len(it.Active))
	}
	peak := 0
	for _, n := range it.Active {
		peak = max(peak, n)
	}
	if last := it.Active[len(it.Active)-1]; last >= peak {
		t.Errorf("frontier per round = %v: it never narrowed, so nothing converged", it.Active)
	}
	// p7 cites p9, which the corpus does not contain. Grow creates it.
	if it.Grown != 1 {
		t.Errorf("grown = %d, want 1 (p9 is cited but not in the corpus)", it.Grown)
	}
	if it.Dropped != 0 {
		t.Errorf("dropped = %d, want 0: with Grow set no citation is lost", it.Dropped)
	}
}

// A loop's projection has to be an over-estimate of a converging run, because
// it prices the round cap. Under-counting is the one direction that costs
// money: a budget set from it would stop the first unconverged run short.
func TestProjectionBoundsTheRun(t *testing.T) {
	p, err := build(5, 2.00)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	opts := []loom.Option{
		loom.WithRegistry(registry()),
		loom.WithBroadcast("question", question),
		loom.WithStageSample("explore", map[string]any{
			"finding": strings.Repeat("x", 120), "follow": []any{"p4"},
		}),
	}
	proj, err := loom.Explain(p, opts...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if proj.Partial() {
		t.Errorf("projection is partial with the stage sample supplied: %v", proj.Warnings)
	}

	res := runExample(t)
	if spent, ceiling := res.Spent.CostUSD, proj.Ceiling().CostUSD; spent > ceiling {
		t.Errorf("run spent $%.4f, above the projected ceiling of $%.4f", spent, ceiling)
	}
}

// Rerunning a converged loop must cost nothing: a vertex's cache key is its
// state and its inbox, so every round replays.
func TestRerunReplaysEveryRound(t *testing.T) {
	dir := t.TempDir()
	first := runExample(t, loom.WithStateDir(dir))
	if first.Spent.TotalTokens() == 0 {
		t.Fatal("first run spent nothing; the fixture is not exercising the models")
	}

	second := runExample(t, loom.WithStateDir(dir))
	if got := second.Spent.TotalTokens(); got != 0 {
		t.Errorf("rerun spent %d tokens, want 0", got)
	}
	if len(second.Output) == 0 ||
		second.Output[0].String("output") != first.Output[0].String("output") {
		t.Error("the replayed run produced a different answer")
	}
}
