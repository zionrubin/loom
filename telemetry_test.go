package loom_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/telemetry"
)

// The telemetry package's own tests drive it from synthetic events, which is
// how they can assert the exposition byte for byte. These drive it from a
// real run instead, and assert the one thing synthetic events cannot: that
// the numbers a scrape reports are the same numbers the run's own report
// does. An exporter that disagrees with RunResult is worse than no exporter,
// because two dashboards will be built on it.

// telemetryRegistry registers the mock with real pricing. RegisterMock leaves
// the rates at zero, and a cost assertion against a free model is an assertion
// that 0 equals 0.
func telemetryRegistry(t *testing.T) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	err := reg.Register(model.Info{
		ID:       "mock-fast",
		Provider: model.NewMock("mock-fast", model.WithHandler(classifyMock)),
		Tier:     model.TierFast,
		Pricing:  model.Pricing{InputPerMTok: 0.80, OutputPerMTok: 4},
		Limits:   model.Limits{RequestsPerMinute: 100000},
	})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func series(t *testing.T, scrape, name string) float64 {
	t.Helper()
	var total float64
	var seen bool
	for _, line := range strings.Split(scrape, "\n") {
		head, rest, ok := strings.Cut(line, " ")
		if !ok || !strings.HasPrefix(head, name) {
			continue
		}
		// Sum every labelled member of the family, so a test can ask what a
		// run spent without enumerating its stages and models.
		if head != name && !strings.HasPrefix(head, name+"{") {
			continue
		}
		var f float64
		if _, err := fmt.Sscanf(rest, "%g", &f); err != nil {
			continue
		}
		total, seen = total+f, true
	}
	if !seen {
		t.Fatalf("no series of family %q in the exposition:\n%s", name, scrape)
	}
	return total
}

// TestTheScrapeAgreesWithTheRunReport.
func TestTheScrapeAgreesWithTheRunReport(t *testing.T) {
	tel := telemetry.New(telemetry.Options{Service: "triage-test"})
	reg := telemetryRegistry(t)

	p := pipeline.New("triage")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			Prompt:    "Classify this ticket: {{.subject}}",
			ParseJSON: true,
		})

	// The wallet is a gauge about work in flight, so it has to be read while
	// there is some. This watches it fall from inside the run.
	var mu sync.Mutex
	lowest := math.Inf(1)
	watch := func(observe.Event) {
		s := tel.Scrape()
		mu.Lock()
		defer mu.Unlock()
		for _, line := range strings.Split(s, "\n") {
			name, rest, ok := strings.Cut(line, " ")
			if !ok || name != `loom_budget_headroom_usd{pipeline="triage"}` {
				continue
			}
			var f float64
			if _, err := fmt.Sscanf(rest, "%g", &f); err == nil && f < lowest {
				lowest = f
			}
		}
	}

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1}),
		loom.WithEventHandler(watch),
		loom.WithTelemetry(tel))
	if err != nil {
		t.Fatal(err)
	}
	scrape := tel.Scrape()

	if got, want := series(t, scrape, "loom_cost_usd_total"), res.Spent.CostUSD; !near(got, want) {
		t.Errorf("the exposition says $%.6f, the report says $%.6f", got, want)
	}
	if got, want := series(t, scrape, "loom_model_calls_total"), float64(res.Spent.Requests); got != want {
		t.Errorf("the exposition counts %v calls, the report counts %v", got, want)
	}
	tokens := series(t, scrape, "loom_tokens_total")
	if want := float64(res.Spent.TotalTokens()); tokens != want {
		t.Errorf("the exposition counts %v tokens, the report counts %v", tokens, want)
	}
	if got := series(t, scrape, "loom_tasks_total"); got != float64(len(tickets())) {
		t.Errorf("want one settled task per record, got %v", got)
	}
	// The wallet is reported against the ceiling the run was given, without
	// anybody having to call Explain: it falls while the run spends, and never
	// below what the run actually spent.
	if lowest >= 1 {
		t.Errorf("the wallet never fell while $%.6f was being spent", res.Spent.CostUSD)
	}
	if floor := 1 - res.Spent.CostUSD; lowest < floor-1e-9 {
		t.Errorf("headroom fell to $%.6f, below the $%.6f the run left", lowest, floor)
	}
	// And it is whole again afterwards, because an idle pipeline has spent
	// nothing — a stale "almost empty" would page somebody about a run that
	// ended hours ago.
	if got := series(t, scrape, "loom_budget_headroom_usd"); got != 1 {
		t.Errorf("with nothing in flight the wallet is whole, got $%.6f", got)
	}
}

func near(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }

// TestASecondRunReplaysAndTheScrapeSaysSo: the cache is the feature an
// operator most wants a number for, because it is the one that shows up as
// money not spent.
func TestASecondRunReplaysAndTheScrapeSaysSo(t *testing.T) {
	dir := t.TempDir()
	tel := telemetry.New(telemetry.Options{})
	reg := telemetryRegistry(t)

	build := func() *pipeline.Pipeline {
		p := pipeline.New("triage")
		p.FromRecords("tickets", tickets()).
			Infer("classify", pipeline.InferSpec{
				Binding:   model.Binding{Tier: model.TierFast},
				Prompt:    "Classify this ticket: {{.subject}}",
				ParseJSON: true,
			})
		return p
	}
	opts := []loom.Option{loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithStateDir(dir), loom.WithTelemetry(tel)}

	if _, err := loom.Run(context.Background(), build(), opts...); err != nil {
		t.Fatal(err)
	}
	first := series(t, tel.Scrape(), "loom_cost_usd_total")
	if _, err := loom.Run(context.Background(), build(), opts...); err != nil {
		t.Fatal(err)
	}
	scrape := tel.Scrape()

	if second := series(t, scrape, "loom_cost_usd_total"); !near(second, first) {
		t.Errorf("a replayed run should cost nothing: $%.6f became $%.6f", first, second)
	}
	if hits := series(t, scrape, "loom_cache_hits_total"); hits != float64(len(tickets())) {
		t.Errorf("want a cache hit per record on the second run, got %v", hits)
	}
	// Counters are cumulative for the life of the process, which is what a
	// scrape expects: two runs of four records are eight settled tasks.
	if tasks := series(t, scrape, "loom_tasks_total"); tasks != 2*float64(len(tickets())) {
		t.Errorf("want eight settled tasks across two runs, got %v", tasks)
	}
}

// TestTheTraceOfARunIsFoundByItsRunID closes the loop the derived identifiers
// exist for: RunResult carries a run ID, and that string alone locates the
// trace.
func TestTheTraceOfARunIsFoundByItsRunID(t *testing.T) {
	spans := &telemetry.Capture{}
	tel := telemetry.New(telemetry.Options{Traces: spans,
		Sample: telemetry.Sampling{Ratio: 1}})
	reg := telemetryRegistry(t)

	p := pipeline.New("triage")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prompt:  "Classify this ticket: {{.subject}}",
		})
	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()), loom.WithTelemetry(tel))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tel.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	want := telemetry.TraceIDFor(res.RunID)
	var run, stages, tasks, calls int
	for _, s := range spans.Spans() {
		if s.TraceID != want {
			t.Fatalf("span %q is outside the run's trace", s.Name)
		}
		switch {
		case strings.HasPrefix(s.Name, "loom.run"):
			run++
		case strings.HasPrefix(s.Name, "loom.stage"):
			stages++
		case strings.HasPrefix(s.Name, "loom.task"):
			tasks++
		case strings.HasPrefix(s.Name, "model.call"):
			calls++
		}
	}
	if run != 1 {
		t.Errorf("want exactly one root span, got %d", run)
	}
	if stages != 2 {
		t.Errorf("want a span for the source and the inference stage, got %d", stages)
	}
	if tasks != len(tickets()) || calls != len(tickets()) {
		t.Errorf("want a task and a call per record, got %d tasks and %d calls", tasks, calls)
	}
}

// TestOneExporterCoversAFleet: a fleet holds one limiter, one governor, one
// cache and one set of slots, and the exporter belongs to the same list. An
// exporter per agent would report a fleet as many small deployments.
func TestOneExporterCoversAFleet(t *testing.T) {
	tel := telemetry.New(telemetry.Options{})
	reg := telemetryRegistry(t)

	fleet, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithWorkers(4), loom.WithTelemetry(tel))
	if err != nil {
		t.Fatal(err)
	}
	defer fleet.Close()

	for _, name := range []string{"desk-a", "desk-b"} {
		p := pipeline.New(name)
		p.FromRecords("tickets", tickets()).
			Infer("classify", pipeline.InferSpec{
				Binding: model.Binding{Tier: model.TierFast},
				Prompt:  "Classify this ticket: {{.subject}}",
			})
		if _, err := fleet.Run(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}
	scrape := tel.Scrape()

	// One exporter, two pipelines, told apart by their own label.
	for _, name := range []string{"desk-a", "desk-b"} {
		want := fmt.Sprintf(`loom_runs_total{pipeline=%q}`, name)
		if !strings.Contains(scrape, want+" 1") {
			t.Errorf("want one run counted for %s:\n%s", name, scrape)
		}
	}
	if got, want := series(t, scrape, "loom_tasks_total"), float64(2*len(tickets())); got != want {
		t.Errorf("want %v settled tasks across the fleet, got %v", want, got)
	}
}

// TestAnAgentCannotHoldTheFleetsExporter: the same refusal every other
// fleet-wide option gets, for the same reason — an option that silently
// applied to one agent would produce a fleet nobody could read.
func TestAnAgentCannotHoldTheFleetsExporter(t *testing.T) {
	reg := telemetryRegistry(t)
	fleet, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithWorkers(2))
	if err != nil {
		t.Fatal(err)
	}
	defer fleet.Close()

	p := pipeline.New("desk")
	p.FromRecords("tickets", tickets())
	_, err = fleet.Run(context.Background(), p,
		loom.WithTelemetry(telemetry.New(telemetry.Options{})))
	if err == nil || !strings.Contains(err.Error(), "WithTelemetry") {
		t.Fatalf("want a refusal naming the option, got %v", err)
	}
}

// TestTheProjectionAndTheRunLandOnOneDashboard: point Explain and Run at the
// same exporter and the forecast is a gauge beside the counter it predicts.
func TestTheProjectionAndTheRunLandOnOneDashboard(t *testing.T) {
	tel := telemetry.New(telemetry.Options{})
	reg := telemetryRegistry(t)

	build := func() *pipeline.Pipeline {
		p := pipeline.New("triage")
		p.FromRecords("tickets", tickets()).
			Infer("classify", pipeline.InferSpec{
				Binding: model.Binding{Tier: model.TierFast},
				Prompt:  "Classify this ticket: {{.subject}}",
			})
		return p
	}
	opts := []loom.Option{loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithTelemetry(tel)}

	proj, err := loom.Explain(build(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	scrape := tel.Scrape()
	if got := series(t, scrape, "loom_projected_ceiling_usd"); got <= 0 {
		t.Errorf("the ceiling should reach the exposition, got %v", got)
	}
	if got, want := series(t, scrape, "loom_projected_cost_usd"), proj.Expected().CostUSD; !near(got, want) {
		t.Errorf("the exposition forecasts $%.6f, Explain says $%.6f", got, want)
	}

	res, err := loom.Run(context.Background(), build(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	scrape = tel.Scrape()
	spent := series(t, scrape, "loom_cost_usd_total")
	if !near(spent, res.Spent.CostUSD) {
		t.Errorf("actual spend disagrees with the report: %v vs %v", spent, res.Spent.CostUSD)
	}
	// The forecast is still there afterwards, which is the point: both halves
	// of the comparison sit in one scrape.
	if got := series(t, scrape, "loom_projected_ceiling_usd"); got <= 0 {
		t.Error("the projection should survive the run it predicted")
	}
	if spent > series(t, scrape, "loom_projected_ceiling_usd") {
		t.Error("a run that stayed inside its ceiling should scrape that way")
	}
}
