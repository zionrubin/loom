package route

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/security"
)

// ladder is a three-rung ladder priced an order of magnitude apart, which is
// roughly how a real fast/balanced/deep ladder is shaped.
func ladder() []Rung {
	return []Rung{
		{Model: "fast", PriceUSD: 0.001},
		{Model: "mid", PriceUSD: 0.010},
		{Model: "deep", PriceUSD: 0.100},
	}
}

func req(key string) Request {
	return Request{Stage: "classify", Key: key, Rungs: ladder(), EstTokens: 1000}
}

// feed records n verdicts at a rung, valid of them passing.
func feed(a *Adaptive, bucket string, rung, n, valid int) {
	for i := 0; i < n; i++ {
		a.Observe(Outcome{Stage: "classify", Bucket: bucket, Rung: rung, Valid: i < valid})
	}
}

// fixedBucket routes everything into one named bucket, so a test can control
// what the router is generalizing over.
func fixedBucket(name string) Featurizer { return func(Request) string { return name } }

// TestColdRouterIsTodaysBehaviour: the guarantee that makes this safe to
// switch on. With no evidence the router must put every task on the bottom
// rung, because that is what Loom does without one — turning routing on can
// begin to save and must not begin to cost.
func TestColdRouterIsTodaysBehaviour(t *testing.T) {
	a := New(Config{Features: fixedBucket("b")})
	for i := 0; i < 100; i++ {
		d := a.Route(req(fmt.Sprintf("k%d", i)))
		if d.Rung != 0 {
			t.Fatalf("cold router moved task %d to rung %d (%s)", i, d.Rung, d.Reason)
		}
	}
	if s := a.Stats(); s.Cold != 100 || s.Moved != 0 {
		t.Fatalf("stats = %+v, want 100 cold and nothing moved", s)
	}
}

// TestEvidenceBelowMinSamplesDoesNotMove: a handful of failures is not
// evidence, and a router that acted on it would be routing by coin flip with
// the user's money. The gate counts bottom-rung verdicts only, so evidence
// about the rung above does not buy the right to skip the one below.
func TestEvidenceBelowMinSamplesDoesNotMove(t *testing.T) {
	a := New(Config{Features: fixedBucket("b"), MinSamples: 25, NoProbe: true})
	feed(a, "b", 1, 200, 190) // the rung above is well understood
	feed(a, "b", 0, 24, 0)    // 24 straight failures at the bottom, one short

	if d := a.Route(req("k")); d.Rung != 0 {
		t.Fatalf("moved on 24 samples with a minimum of 25: rung %d (%s)", d.Rung, d.Reason)
	}
	feed(a, "b", 0, 1, 0)
	if d := a.Route(req("k")); d.Rung == 0 {
		t.Fatalf("still at the bottom after 25 failures there: %s", d.Reason)
	}
}

// TestACheapRungIsWorthAGambleOnAWeakPrior: the gate is not the only thing
// holding a cold router down, and the arithmetic under it has to be right too.
// A bottom rung ten times cheaper than the next is worth paying for a 4% shot
// at avoiding it, so 25 failures against an *unmeasured* rung above is
// correctly not enough to skip — only evidence that the rung above works
// makes the skip pay.
func TestACheapRungIsWorthAGambleOnAWeakPrior(t *testing.T) {
	a := New(Config{Features: fixedBucket("b"), MinSamples: 25, NoProbe: true})
	feed(a, "b", 0, 25, 0) // hopeless, but nothing is known about the rung above
	if d := a.Route(req("k")); d.Rung != 0 {
		t.Fatalf("skipped a rung 10x cheaper than the next on a coin-flip prior: %s", d.Reason)
	}
	feed(a, "b", 1, 50, 48) // now the rung above is known to work
	if d := a.Route(req("k")); d.Rung != 1 {
		t.Fatalf("rung = %d, want 1 once the rung above is known to answer (%s)",
			d.Rung, d.Reason)
	}
}

// TestHopelessBottomRungIsSkipped: the whole point. A rung that never produces
// valid output for a bucket is a toll rather than a saving, and the router
// must stop paying it.
func TestHopelessBottomRungIsSkipped(t *testing.T) {
	a := New(Config{Features: fixedBucket("hard"), NoProbe: true})
	feed(a, "hard", 0, 60, 0) // the fast model never gets this bucket right
	feed(a, "hard", 1, 60, 57)

	d := a.Route(req("k"))
	if d.Rung != 1 {
		t.Fatalf("rung = %d, want 1 (%s)", d.Rung, d.Reason)
	}
	if d.Reason == "" {
		t.Fatal("a router that cannot say why it moved a record is one nobody will leave on")
	}
}

// TestWorkingBottomRungIsKept: the other half. A cheap rung that answers most
// of a bucket is exactly what the ladder is for, and must not be skipped
// because the rung above it is more reliable.
func TestWorkingBottomRungIsKept(t *testing.T) {
	a := New(Config{Features: fixedBucket("easy")})
	feed(a, "easy", 0, 100, 95)
	feed(a, "easy", 1, 5, 5)

	if d := a.Route(req("k")); d.Rung != 0 {
		t.Fatalf("rung = %d, want 0 — the cheap model answers 95%% of this bucket (%s)",
			d.Rung, d.Reason)
	}
}

// TestBucketsAreRoutedIndependently: routing is worth having only if it can
// move part of a stage. A stage where every record goes the same way could be
// fixed by editing the binding.
func TestBucketsAreRoutedIndependently(t *testing.T) {
	a := New(Config{Features: ByField("tier"), NoProbe: true})
	feed(a, "tier=hard", 0, 60, 0)
	feed(a, "tier=hard", 1, 60, 55)
	feed(a, "tier=easy", 0, 60, 58)

	rec := func(tier string) Request {
		r := req("k-" + tier)
		r.Records = []core.Record{{ID: "r", Data: map[string]any{"tier": tier}}}
		return r
	}
	if d := a.Route(rec("hard")); d.Rung != 1 {
		t.Errorf("hard bucket: rung %d, want 1 (%s)", d.Rung, d.Reason)
	}
	if d := a.Route(rec("easy")); d.Rung != 0 {
		t.Errorf("easy bucket: rung %d, want 0 (%s)", d.Rung, d.Reason)
	}
}

// TestExpectedCostBeatsPositionalHeuristics: the decision is arithmetic over
// prices, not a rule about failure rates. A bottom rung that fails most of the
// time is still the right place to start when the rung above it costs a
// hundred times more.
func TestExpectedCostBeatsPositionalHeuristics(t *testing.T) {
	a := New(Config{Features: fixedBucket("b"), NoProbe: true})
	feed(a, "b", 0, 100, 30) // fails 70% of the time
	feed(a, "b", 1, 100, 99)

	cheap := []Rung{{Model: "fast", PriceUSD: 0.0001}, {Model: "deep", PriceUSD: 0.100}}
	r := req("k")
	r.Rungs = cheap
	if d := a.Route(r); d.Rung != 0 {
		t.Fatalf("rung = %d: a 70%% failure rate on a rung costing 1/1000th of the "+
			"next is still the cheapest way to an answer (%s)", d.Rung, d.Reason)
	}

	// Narrow the price gap and the same failure rate flips the decision.
	r.Rungs = []Rung{{Model: "fast", PriceUSD: 0.09}, {Model: "deep", PriceUSD: 0.100}}
	if d := a.Route(r); d.Rung != 1 {
		t.Fatalf("rung = %d, want 1: at these prices the cheap call is a toll (%s)",
			d.Rung, d.Reason)
	}
}

// TestProbesHoldSomeTasksAtTheBottom: the measurement. Without probes the
// estimate that skips a rung can never be contradicted, and the saving is a
// claim rather than a number.
func TestProbesHoldSomeTasksAtTheBottom(t *testing.T) {
	a := New(Config{Features: fixedBucket("hard"), ProbeRate: 0.2})
	feed(a, "hard", 0, 60, 0)
	feed(a, "hard", 1, 60, 57)

	var probes, moved int
	const n = 2000
	for i := 0; i < n; i++ {
		d := a.Route(req(fmt.Sprintf("k%d", i)))
		switch {
		case d.Probe:
			probes++
			if d.Rung != 0 {
				t.Fatalf("a probe must run at the bottom rung, got %d", d.Rung)
			}
		case d.Rung == 1:
			moved++
		default:
			t.Fatalf("task %d neither probed nor routed: rung %d", i, d.Rung)
		}
	}
	if got := float64(probes) / n; math.Abs(got-0.2) > 0.03 {
		t.Errorf("probe rate = %.3f, want ~0.20", got)
	}
	if moved+probes != n {
		t.Errorf("moved+probes = %d, want %d", moved+probes, n)
	}
}

// TestDecisionsAreDeterministic: two workers holding the same profile must
// agree, and a report regenerated must say the same thing. Nothing here may
// read a clock or a global random source.
func TestDecisionsAreDeterministic(t *testing.T) {
	build := func() *Adaptive {
		a := New(Config{Features: fixedBucket("hard"), ProbeRate: 0.3})
		feed(a, "hard", 0, 60, 0)
		feed(a, "hard", 1, 60, 57)
		return a
	}
	a, b := build(), build()
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("k%d", i)
		da, db := a.Route(req(key)), b.Route(req(key))
		if da.Rung != db.Rung || da.Probe != db.Probe {
			t.Fatalf("key %s: %+v vs %+v", key, da, db)
		}
	}
}

// TestProbeHitsAreCounted: a probe the bottom rung answers is a skip that
// would have been wrong, and it is the only figure that corrects the gross
// saving.
func TestProbeHitsAreCounted(t *testing.T) {
	a := New(Config{Features: fixedBucket("b")})
	a.Observe(Outcome{Stage: "classify", Bucket: "b", Rung: 0, Valid: true, Probe: true})
	a.Observe(Outcome{Stage: "classify", Bucket: "b", Rung: 0, Valid: false, Probe: true})
	a.Observe(Outcome{Stage: "classify", Bucket: "b", Rung: 1, Valid: true, Probe: true})
	a.Observe(Outcome{Stage: "classify", Bucket: "b", Rung: 0, Valid: true})
	if got := a.Stats().ProbeHits; got != 1 {
		t.Fatalf("ProbeHits = %d, want 1 — only a probe answered at the bottom rung counts", got)
	}
}

// TestSingleRungLadderIsLeftAlone: a stage with no escalation has nothing to
// decide, and asking is worse than not asking.
func TestSingleRungLadderIsLeftAlone(t *testing.T) {
	a := New(Config{Features: fixedBucket("b")})
	feed(a, "b", 0, 100, 0)
	r := req("k")
	r.Rungs = []Rung{{Model: "only", PriceUSD: 0.01}}
	if d := a.Route(r); d.Rung != 0 {
		t.Fatalf("rung = %d on a one-rung ladder", d.Rung)
	}
}

// TestProfileRoundTrips: a profile is the artifact that carries calibration
// between runs, so it has to survive the trip byte for byte.
func TestProfileRoundTrips(t *testing.T) {
	a := New(Config{Features: fixedBucket("b")})
	feed(a, "b", 0, 40, 7)
	feed(a, "b", 1, 33, 30)

	blob, err := json.Marshal(a.Profile())
	if err != nil {
		t.Fatal(err)
	}
	var back Profile
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		rung    int
		rate    float64
		samples int
	}{{0, 7.0 / 40, 40}, {1, 30.0 / 33, 33}} {
		rate, n := back.Rate("classify", "b", tc.rung)
		if n != tc.samples || math.Abs(rate-tc.rate) > 1e-9 {
			t.Errorf("rung %d: rate %.4f over %d, want %.4f over %d",
				tc.rung, rate, n, tc.rate, tc.samples)
		}
	}
	// Sorted output, so two profiles holding the same counts are the same bytes.
	again, err := json.Marshal(&back)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(blob) {
		t.Errorf("re-marshal differs:\n%s\n%s", blob, again)
	}
}

// TestLearnedIsAContributionNotATotal: persistence appends, so a run seeded
// from disk must not write its seed back out and count it twice.
func TestLearnedIsAContributionNotATotal(t *testing.T) {
	seed := New(Config{Features: fixedBucket("b")})
	feed(seed, "b", 0, 30, 3)

	second := New(Config{Features: fixedBucket("b"), Profile: seed.Profile()})
	feed(second, "b", 0, 10, 1)

	if _, n := second.Profile().Rate("classify", "b", 0); n != 40 {
		t.Errorf("decisions rest on %d samples, want 40 (seed + this run)", n)
	}
	if _, n := second.Learned().Rate("classify", "b", 0); n != 10 {
		t.Errorf("contribution holds %d samples, want 10 (this run only)", n)
	}
}

// TestMergeIsAdditiveAndOrderFree: several workers calibrate one pipeline at
// once, so folding their contributions together must not depend on who
// finished first.
func TestMergeIsAdditiveAndOrderFree(t *testing.T) {
	mk := func(valid, n int) *Profile {
		a := New(Config{Features: fixedBucket("b")})
		feed(a, "b", 0, n, valid)
		return a.Learned()
	}
	x, y := mk(3, 10), mk(11, 20)

	ab, ba := NewProfile(), NewProfile()
	ab.Merge(x)
	ab.Merge(y)
	ba.Merge(y)
	ba.Merge(x)

	for _, p := range []*Profile{ab, ba} {
		rate, n := p.Rate("classify", "b", 0)
		if n != 30 || math.Abs(rate-14.0/30) > 1e-9 {
			t.Errorf("merged = %.4f over %d, want %.4f over 30", rate, n, 14.0/30)
		}
	}
}

// TestForecastPricesTheEscalations: the number a projection cannot compute
// today. A stage whose bottom rung fails a third of the time costs more than
// one call per record, and the forecast has to say so.
func TestForecastPricesTheEscalations(t *testing.T) {
	a := New(Config{Features: fixedBucket("b"), NoProbe: true})
	feed(a, "b", 0, 300, 200) // a third escalate
	feed(a, "b", 1, 100, 100)

	f := a.Forecast("classify", ladder()[:2])
	if f.Samples != 300 {
		t.Fatalf("Samples = %d, want 300", f.Samples)
	}
	// ~2/3 answered by the cheap rung, ~1/3 escalating to the second.
	if got := f.Rungs[0].FlatCalls; math.Abs(got-1) > 1e-9 {
		t.Errorf("bottom rung flat calls = %.4f, want 1 per record", got)
	}
	if got := f.Rungs[1].FlatCalls; math.Abs(got-0.332) > 0.01 {
		t.Errorf("escalations = %.4f per record, want ~0.33", got)
	}
	// Which is a third more than pricing the base model alone, the gap this
	// exists to expose.
	if f.FlatUSD <= ladder()[0].PriceUSD*1.2 {
		t.Errorf("FlatUSD = %.6f, barely above the base model's %.6f: "+
			"the escalations are not being priced", f.FlatUSD, ladder()[0].PriceUSD)
	}
}

// TestForecastRoutedBeatsFlatWhenEvidenceSaysSo, and does not otherwise.
func TestForecastRoutedBeatsFlat(t *testing.T) {
	hopeless := New(Config{Features: fixedBucket("b"), NoProbe: true})
	feed(hopeless, "b", 0, 100, 0)
	feed(hopeless, "b", 1, 100, 95)
	f := hopeless.Forecast("classify", ladder()[:2])
	if f.RoutedUSD >= f.FlatUSD {
		t.Errorf("routed $%.6f is not below flat $%.6f on a bucket the bottom rung never answers",
			f.RoutedUSD, f.FlatUSD)
	}

	fine := New(Config{Features: fixedBucket("b"), NoProbe: true})
	feed(fine, "b", 0, 100, 98)
	g := fine.Forecast("classify", ladder()[:2])
	if math.Abs(g.RoutedUSD-g.FlatUSD) > 1e-12 {
		t.Errorf("routed $%.6f differs from flat $%.6f on a bucket the bottom rung answers: "+
			"there is nothing to route", g.RoutedUSD, g.FlatUSD)
	}
}

// TestForecastWithoutEvidenceIsTodaysProjection: one call per record at the
// base model, which is what Explain assumes with no profile to read.
func TestForecastWithoutEvidenceIsTodaysProjection(t *testing.T) {
	a := New(Config{Features: fixedBucket("b")})
	f := a.Forecast("never-run", ladder())
	if f.Samples != 0 {
		t.Fatalf("Samples = %d on an unseen stage", f.Samples)
	}
	if f.FlatUSD != ladder()[0].PriceUSD || f.RoutedUSD != ladder()[0].PriceUSD {
		t.Errorf("flat $%.6f routed $%.6f, want the base model's $%.6f",
			f.FlatUSD, f.RoutedUSD, ladder()[0].PriceUSD)
	}
	if f.Rungs[0].FlatCalls != 1 {
		t.Errorf("base rung calls = %.2f, want 1 per record", f.Rungs[0].FlatCalls)
	}
}

// TestPriceLadderOrdersByCost: a router compares rungs in dollars, so the
// prices have to come from the registry rather than from the ladder's order.
func TestPriceLadderOrdersByCost(t *testing.T) {
	infos := []model.Info{
		{ID: "fast", Pricing: model.Pricing{InputPerMTok: 1, OutputPerMTok: 5},
			SecretRef: security.SecretRef("k")},
		{ID: "deep", Pricing: model.Pricing{InputPerMTok: 15, OutputPerMTok: 75}},
	}
	rungs := PriceLadder(infos, 1000, 250)
	if len(rungs) != 2 {
		t.Fatalf("rungs = %d", len(rungs))
	}
	if rungs[0].PriceUSD >= rungs[1].PriceUSD {
		t.Fatalf("fast $%.6f is not below deep $%.6f", rungs[0].PriceUSD, rungs[1].PriceUSD)
	}
	if got := rungs[1].PriceUSD / rungs[0].PriceUSD; math.Abs(got-15) > 0.01 {
		t.Errorf("price ratio = %.2f, want 15 — the ratio is what a decision turns on", got)
	}
}

// TestSizeBucketSeparatesByOrderOfMagnitude, which is the point of bucketing
// by powers of two rather than by exact count: a bucket has to be coarse
// enough that verdicts about one record are evidence about the next.
func TestSizeBucketSeparatesByOrderOfMagnitude(t *testing.T) {
	small := SizeBucket(Request{EstTokens: 1100})
	similar := SizeBucket(Request{EstTokens: 1900})
	large := SizeBucket(Request{EstTokens: 90000})
	if small != similar {
		t.Errorf("1100 and 1900 tokens bucketed apart: %q vs %q", small, similar)
	}
	if small == large {
		t.Errorf("1100 and 90000 tokens bucketed together as %q", small)
	}
	if SizeBucket(Request{}) != "n/a" {
		t.Errorf("a task with no estimate should bucket as n/a")
	}
}

// TestOffRouterIsInert, so a caller can leave the option in place and turn the
// behaviour off without a nil check.
func TestOffRouterIsInert(t *testing.T) {
	var r Router = Off{}
	r.Observe(Outcome{Stage: "classify", Rung: 0, Valid: false})
	if d := r.Route(req("k")); d.Rung != 0 || d.Probe {
		t.Fatalf("Off routed: %+v", d)
	}
}

// TestForecastWeightsBucketsByRecordsNotVerdicts: the bias that made an early
// version of this understate a stage by two thirds.
//
// A bucket the router has moved off the bottom rung stops producing
// bottom-rung verdicts — that is the whole point of moving it — so weighting
// buckets by those verdicts shrinks exactly the buckets routing is working on,
// which are the expensive ones. Starts are recorded so the weight is the
// record count instead.
func TestForecastWeightsBucketsByRecordsNotVerdicts(t *testing.T) {
	// MinSamples matters here: once a bucket is routed off the bottom rung its
	// bottom-rung evidence only grows through probes, so the gate is set below
	// what the probes below supply. A forecast applies the same gate the
	// scheduler does, which is the point of sharing the code.
	a := New(Config{Features: fixedBucket("x"), MinSamples: 10, NoProbe: true})

	// Two thirds of the stage is a bucket the bottom rung never answers, and
	// which the router has long since moved: 20 records probed the bottom
	// before it was moved, 180 went straight to the rung above.
	for i := 0; i < 20; i++ {
		a.Observe(Outcome{Stage: "s", Bucket: "hard", Rung: 0, Valid: false, Start: true})
		a.Observe(Outcome{Stage: "s", Bucket: "hard", Rung: 1, Valid: true})
	}
	for i := 0; i < 180; i++ {
		a.Observe(Outcome{Stage: "s", Bucket: "hard", Rung: 1, Valid: true, Start: true})
	}
	// The other third is answered by the bottom rung every time.
	for i := 0; i < 100; i++ {
		a.Observe(Outcome{Stage: "s", Bucket: "easy", Rung: 0, Valid: true, Start: true})
	}

	f := a.Forecast("s", ladder()[:2])
	// Two thirds hard: nearly every record reaches the second rung, whether it
	// is routed there or escalates into it.
	if got := f.Rungs[1].FlatCalls; math.Abs(got-2.0/3) > 0.05 {
		t.Errorf("second-rung calls = %.3f per record, want ~0.67 — bucket weights "+
			"are being taken from bottom-rung verdicts rather than record counts", got)
	}
	if got := f.Rungs[0].RoutedCalls; math.Abs(got-1.0/3) > 0.05 {
		t.Errorf("routed bottom-rung calls = %.3f per record, want ~0.33 "+
			"(only the easy third should still start there)", got)
	}
}

// TestForecastFallsBackWithoutStarts: a router driven programmatically, with
// no start marked, still forecasts rather than dividing by zero.
func TestForecastFallsBackWithoutStarts(t *testing.T) {
	a := New(Config{Features: fixedBucket("b"), NoProbe: true})
	feed(a, "b", 0, 50, 25) // feed() marks no starts
	f := a.Forecast("classify", ladder()[:2])
	if f.Samples != 50 || f.FlatUSD <= 0 {
		t.Fatalf("forecast = %+v", f)
	}
}
