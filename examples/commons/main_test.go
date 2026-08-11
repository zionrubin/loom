package main

import (
	"context"
	"testing"
	"time"
)

// The example's headline claim, checked rather than printed: the same fleet,
// the same pipelines, one option — and the source is called once per subject
// instead of once per desk.
func TestCommonsCallsTheSourceOncePerSubject(t *testing.T) {
	ctx := context.Background()
	latency := 5 * time.Millisecond

	plain, err := run(ctx, latency, false, nil)
	if err != nil {
		t.Fatalf("without the commons: %v", err)
	}
	shared, err := run(ctx, latency, true, nil)
	if err != nil {
		t.Fatalf("with the commons: %v", err)
	}

	if want := len(desks) * len(companies); plain.calls != want {
		t.Fatalf("without the commons, calls = %d, want %d (every desk asks)", plain.calls, want)
	}
	if shared.calls != len(companies) {
		t.Fatalf("with the commons, calls = %d, want %d (one per subject)", shared.calls, len(companies))
	}
}

// The safety property everything else rests on. If a served finding were not
// substitutable for the call it replaced, the layer would be changing answers
// to save money, which is not a trade anyone agreed to.
func TestAnswersAreIdenticalWithAndWithoutTheCommons(t *testing.T) {
	ctx := context.Background()
	plain, err := run(ctx, time.Millisecond, false, nil)
	if err != nil {
		t.Fatalf("without: %v", err)
	}
	shared, err := run(ctx, time.Millisecond, true, nil)
	if err != nil {
		t.Fatalf("with: %v", err)
	}
	if diff := firstDifference(plain.briefs, shared.briefs); diff != "" {
		t.Fatalf("a reused answer differs from a researched one: %s", diff)
	}
	if len(shared.briefs) != len(desks)*len(companies) {
		t.Fatalf("briefs = %d, want %d", len(shared.briefs), len(desks)*len(companies))
	}
}

// The overhead claim: the gate must be cheap enough to sit in front of every
// task, not just the ones someone guessed would collide.
func TestGateOverheadStaysWellUnderTheCallItReplaces(t *testing.T) {
	shared, err := run(context.Background(), 20*time.Millisecond, true, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	s := shared.report.Findings
	if s.Asked != len(desks)*len(companies) {
		t.Fatalf("asked = %d, want %d", s.Asked, len(desks)*len(companies))
	}
	// A very loose bound on a map lookup, deliberately: the test should fail
	// when the design regresses, not when the machine is busy.
	if per := s.Overshoot(); per > time.Millisecond {
		t.Fatalf("gate overhead %s per question is too high to gate every task", per)
	}
	if s.Reused() != s.Asked-len(companies) {
		t.Fatalf("reused = %d, want %d", s.Reused(), s.Asked-len(companies))
	}
	if s.AvoidedTime <= 0 || s.Avoided.CostUSD <= 0 {
		t.Fatalf("reuse should credit both the money and the wall clock it avoided")
	}
}

// Retraction reaches what was built on the claim.
func TestRetractionNamesTheTasksThatRestOnTheClaim(t *testing.T) {
	shared, err := run(context.Background(), time.Millisecond, true, nil)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	out := retractionDemo(shared)
	if out == "" {
		t.Fatalf("retraction demo produced nothing")
	}
	if shared.gate == nil {
		t.Fatalf("the fleet should expose its gate")
	}
	// Six subjects, one withdrawn: five claims remain servable.
	live := 0
	for _, tstat := range shared.gate.Ledger.Topics() {
		live += tstat.Live
	}
	if live != len(companies)-1 {
		t.Fatalf("live findings after one retraction = %d, want %d", live, len(companies)-1)
	}
}
