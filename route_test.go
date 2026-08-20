package loom_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/route"
	"github.com/zionrubin/loom/security"
)

// A stage whose difficulty is carried by a field: "long" documents are beyond
// the fast model and always will be, "short" ones are not. That split is the
// thing a router exists to find, and the thing a flat ladder pays to
// rediscover on every record.
func routingRegistry(t *testing.T) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()

	summarize := func(req model.Request) (string, error) {
		if strings.Contains(req.Prompt, "kind: long") {
			return `{"summary": ""}`, nil // too little to work with: fails validation
		}
		return `{"summary": "ok"}`, nil
	}
	fast := model.NewMock("fast", model.WithHandler(summarize))
	if err := reg.Register(model.Info{ID: "fast", Provider: fast, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 1, OutputPerMTok: 5}}); err != nil {
		t.Fatal(err)
	}
	deep := model.NewMock("deep", model.WithHandler(func(model.Request) (string, error) {
		return `{"summary": "thorough"}`, nil
	}))
	if err := reg.Register(model.Info{ID: "deep", Provider: deep, Tier: model.TierDeep,
		Pricing: model.Pricing{InputPerMTok: 15, OutputPerMTok: 75}}); err != nil {
		t.Fatal(err)
	}
	return reg
}

func routingPipeline(n int) *pipeline.Pipeline {
	recs := make([]core.Record, n)
	for i := range recs {
		kind := "short"
		if i%2 == 0 {
			kind = "long"
		}
		recs[i] = core.NewRecord(fmt.Sprintf("d%d", i), map[string]any{"kind": kind})
	}
	p := pipeline.New("summarize")
	p.FromRecords("docs", recs).Infer("write", pipeline.InferSpec{
		Binding:   model.Binding{Tier: model.TierFast, Escalation: []string{"deep"}},
		Prompt:    "Summarize this document. kind: {{.kind}}",
		ParseJSON: true,
		Validate: func(r core.Record) error {
			if r.String("summary") == "" {
				return fmt.Errorf("empty summary")
			}
			return nil
		},
	})
	return p
}

func runRouted(t *testing.T, n int, opts ...loom.Option) *loom.RunResult {
	t.Helper()
	base := []loom.Option{
		loom.WithRegistry(routingRegistry(t)),
		loom.WithRetry(quickRetry()),
		loom.WithSecrets(map[security.SecretRef]string{}),
		loom.WithWorkers(1),
	}
	res, err := loom.Run(context.Background(), routingPipeline(n), append(base, opts...)...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return res
}

// TestRoutingCutsTheWastedCallsWithoutChangingTheOutput: the feature, through
// the public API. Half these documents are beyond the fast model, and a flat
// ladder pays for a fast call on every one of them before finding out.
func TestRoutingCutsTheWastedCallsWithoutChangingTheOutput(t *testing.T) {
	const n = 80
	flatRes := runRouted(t, n)
	routed := runRouted(t, n, loom.WithRouting(route.Config{
		Features: route.ByField("kind"), MinSamples: 10, NoProbe: true}))

	if len(flatRes.Output) != n || len(routed.Output) != n {
		t.Fatalf("flat produced %d records, routed %d, want %d",
			len(flatRes.Output), len(routed.Output), n)
	}
	// Same answers. Routing moves where a record starts, never what it says.
	for i := range flatRes.Output {
		if a, b := flatRes.Output[i].String("summary"), routed.Output[i].String("summary"); a != b {
			t.Fatalf("record %d: flat %q routed %q", i, a, b)
		}
	}

	// Measured on the report rather than on RunResult.Spent, and the
	// difference matters here more than anywhere else in Loom: the governor is
	// charged from a task's result, and a task whose call failed validation
	// returns no result, so the money spent on exactly the calls this feature
	// eliminates is money Spent never counted. The model.called events do
	// carry it, because they are published when the call is made.
	flat, routedT := flatRes.Report.Totals(), routed.Report.Totals()
	if routedT.Requests >= flat.Requests {
		t.Fatalf("routed made %d calls against the flat ladder's %d", routedT.Requests, flat.Requests)
	}
	if routedT.CostUSD >= flat.CostUSD {
		t.Errorf("routed spent $%.6f against flat's $%.6f", routedT.CostUSD, flat.CostUSD)
	}
	if routed.Routing.Moved == 0 {
		t.Errorf("routing stats show nothing moved: %+v", routed.Routing)
	}
	// The reported saving is priced from calls that actually ran, so it has to
	// land near the difference the two runs actually show. A figure derived
	// from the planner's max-output estimate would be several times this, and
	// would be contradicted by the cost column beside it.
	_, skipped, saved, _, _ := routed.Report.Routing()
	if skipped != flat.Requests-routedT.Requests {
		t.Errorf("report claims %d skipped calls; the runs differ by %d",
			skipped, flat.Requests-routedT.Requests)
	}
	measured := flat.CostUSD - routedT.CostUSD
	if saved <= 0 || saved > 2*measured {
		t.Errorf("reported saving $%.6f against a measured difference of $%.6f",
			saved, measured)
	}
	t.Logf("flat %d calls $%.6f → routed %d calls $%.6f (reported saving $%.6f) %+v",
		flat.Requests, flat.CostUSD, routedT.Requests, routedT.CostUSD, saved, routed.Routing)
}

// TestRoutingNeverExceedsTheProjectedCeiling: starting at rung k walks a
// subset of the rungs a flat ladder walks, so the budget a caller set on the
// strength of loom.Explain still holds.
func TestRoutingNeverExceedsTheProjectedCeiling(t *testing.T) {
	const n = 40
	routed := runRouted(t, n, loom.WithRouting(route.Config{
		Features: route.ByField("kind"), MinSamples: 5}))
	flat := runRouted(t, n)
	if a, b := routed.Report.Totals().Requests, flat.Report.Totals().Requests; a > b {
		t.Fatalf("routed issued %d calls, more than the flat ladder's %d", a, b)
	}
}

// TestAColdRunRoutesNothing: switching routing on with nothing learned must
// reproduce the run Loom does without it, to the call.
func TestAColdRunRoutesNothing(t *testing.T) {
	const n = 8 // fewer records than the router's minimum evidence
	flat := runRouted(t, n)
	cold := runRouted(t, n, loom.WithRouting(route.Config{
		Features: route.ByField("kind"), MinSamples: 100}))

	if a, b := flat.Report.Totals().Requests, cold.Report.Totals().Requests; a != b {
		t.Fatalf("cold router made %d calls where the flat ladder made %d", b, a)
	}
	if cold.Routing.Moved != 0 {
		t.Errorf("a cold router moved %d task(s)", cold.Routing.Moved)
	}
}

// TestCalibrationOutlivesTheRun: the second run over similar input starts
// knowing what the first one paid to find out.
func TestCalibrationOutlivesTheRun(t *testing.T) {
	dir := t.TempDir()
	opts := func() loom.Option {
		return loom.WithRouting(route.Config{
			Features: route.ByField("kind"), MinSamples: 10, NoProbe: true})
	}

	// A first run in its own state dir, learning as it goes. Its cache would
	// make the second run free, so each run gets a fresh one for results and
	// they share only the profile.
	first := runRouted(t, 60, opts(), loom.WithStateDir(dir))
	if first.Routing.Observations == 0 {
		t.Fatal("first run recorded no verdicts")
	}

	prof, err := route.LoadProfile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, n := prof.Rate("write", "kind=long", 0); n == 0 {
		t.Fatal("nothing persisted for the bucket the fast model cannot handle")
	}

	// A second run seeded with that profile but no result cache: it must route
	// from the first task rather than paying to learn again.
	second := runRouted(t, 20, loom.WithRouting(route.Config{
		Features: route.ByField("kind"), MinSamples: 10, NoProbe: true, Profile: prof}))
	cold := runRouted(t, 20, opts())

	if second.Routing.Cold != 0 {
		t.Errorf("a seeded run declined %d decisions for want of evidence", second.Routing.Cold)
	}
	if a, b := second.Report.Totals().Requests, cold.Report.Totals().Requests; a >= b {
		t.Errorf("seeded run made %d calls, cold run %d: the profile bought nothing", a, b)
	}
}

// TestExplainPricesTheEscalations: the number a projection cannot compute
// without a profile. The columns price one call per record at the base model;
// a stage that escalates half its records costs considerably more, and once
// verdicts exist the projection has to say so.
func TestExplainPricesTheEscalations(t *testing.T) {
	dir := t.TempDir()
	runRouted(t, 60, loom.WithStateDir(dir), loom.WithRouting(route.Config{
		Features: route.ByField("kind"), MinSamples: 10, NoProbe: true}))

	proj, err := loom.Explain(routingPipeline(100),
		loom.WithRegistry(routingRegistry(t)),
		loom.WithStateDir(dir),
		loom.WithRouting(route.Config{Features: route.ByField("kind"), MinSamples: 10}))
	if err != nil {
		t.Fatal(err)
	}

	var stage *loom.StageProjection
	for i := range proj.Stages {
		if proj.Stages[i].Stage == "write" {
			stage = &proj.Stages[i]
		}
	}
	if stage == nil || stage.Ladder == nil {
		t.Fatalf("no ladder projection on the infer stage: %+v", proj.Stages)
	}
	l := stage.Ladder
	if l.Samples == 0 {
		t.Error("ladder projected from no verdicts")
	}
	if len(l.Rungs) != 2 {
		t.Fatalf("rungs = %d, want 2", len(l.Rungs))
	}
	// Half the records escalate, so the flat ladder costs materially more than
	// the base-model figure in the Usage column.
	if l.FlatUSD <= stage.Usage.CostUSD {
		t.Errorf("flat ladder $%.6f is not above the base-model projection $%.6f: "+
			"the escalations are not priced", l.FlatUSD, stage.Usage.CostUSD)
	}
	if l.Saved() <= 0 {
		t.Errorf("routing saves $%.6f on a stage where half the cheap calls are doomed",
			l.Saved())
	}
	if s := proj.String(); !strings.Contains(s, "write ladder") {
		t.Errorf("projection does not report the ladder:\n%s", s)
	}
}

// TestExplainWithoutAProfileIsUnchanged: a projection with nothing learned
// must look exactly as it did before this existed.
func TestExplainWithoutAProfileIsUnchanged(t *testing.T) {
	proj, err := loom.Explain(routingPipeline(20),
		loom.WithRegistry(routingRegistry(t)),
		loom.WithRouting())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range proj.Stages {
		if s.Ladder != nil {
			t.Errorf("stage %q carries a ladder projection with no verdicts behind it", s.Stage)
		}
	}
}

// TestWithRouterInstallsAPolicy: a caller who wants a rule rather than a
// learner gets exactly the rule they wrote, and nothing seeds or persists it.
func TestWithRouterInstallsAPolicy(t *testing.T) {
	res := runRouted(t, 20, loom.WithRouter(alwaysTop{}))
	if res.Report.Totals().Requests != 20 {
		t.Errorf("calls = %d, want 20: a router pinning every record to the top "+
			"rung should produce exactly one call each", res.Report.Totals().Requests)
	}
	for _, r := range res.Output {
		if got := r.String("summary"); got != "thorough" {
			t.Errorf("record answered by the wrong rung: %q", got)
		}
	}
}

// alwaysTop pins every task to the top of its ladder.
type alwaysTop struct{}

func (alwaysTop) Route(r route.Request) route.Decision {
	return route.Decision{Rung: len(r.Rungs) - 1, Reason: "policy: always the strongest model"}
}
func (alwaysTop) Observe(route.Outcome) {}
