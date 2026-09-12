package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/telemetry"
)

// run drives the example's pipeline through an exporter and hands back both
// halves of what a deployment would see.
func run(t *testing.T, opts telemetry.Options) (*loom.RunResult, *telemetry.Telemetry) {
	t.Helper()
	reg, err := registry()
	if err != nil {
		t.Fatal(err)
	}
	tel := telemetry.New(opts)
	res, err := loom.Run(context.Background(), triage(),
		loom.WithRegistry(reg),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 0.50}),
		loom.WithTelemetry(tel))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tel.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	return res, tel
}

func value(t *testing.T, scrape, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(scrape, "\n") {
		name, rest, ok := strings.Cut(line, " ")
		if !ok || name != series {
			continue
		}
		var f float64
		if _, err := fmt.Sscanf(rest, "%g", &f); err != nil {
			t.Fatalf("unparseable value for %s: %q", series, rest)
		}
		return f
	}
	t.Fatalf("series %q not exposed:\n%s", series, scrape)
	return 0
}

// TestTheEscalationIsVisibleInTheCounters is the example's central claim. Two
// of the four tickets are rejected by the validator and answered on the rung
// above, and the provider billed for all six calls. A cost metric fed from
// task completions would have reported four.
func TestTheEscalationIsVisibleInTheCounters(t *testing.T) {
	res, tel := run(t, telemetry.Options{})
	s := tel.Scrape()

	fast := value(t, s, `loom_model_calls_total{model="mock-fast",outcome="ok",pipeline="beacon",stage="classify"}`)
	deep := value(t, s, `loom_model_calls_total{model="mock-deep",outcome="ok",pipeline="beacon",stage="classify"}`)
	if fast != 4 || deep != 2 {
		t.Errorf("want four calls on the cheap model and two on the rung above, got %v and %v", fast, deep)
	}
	if got := value(t, s, `loom_escalations_total{pipeline="beacon",stage="classify"}`); got != 2 {
		t.Errorf("want two escalations, got %v", got)
	}
	if got := float64(res.Spent.Requests); got != fast+deep {
		t.Errorf("the report counts %v calls, the counters %v", got, fast+deep)
	}

	var counted float64
	for _, m := range []string{"mock-fast", "mock-deep"} {
		counted += value(t, s, fmt.Sprintf(
			`loom_cost_usd_total{model=%q,pipeline="beacon",stage="classify"}`, m))
	}
	if diff := counted - res.Spent.CostUSD; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("the counters say $%.6f, the run report says $%.6f", counted, res.Spent.CostUSD)
	}
}

// TestSamplingKeepsTheTwoTasksAnybodyWouldOpen: the ratio is zero and the
// escalated tasks are traced anyway, because the decision is made when a task
// settles rather than when it starts.
func TestSamplingKeepsTheTwoTasksAnybodyWouldOpen(t *testing.T) {
	spans := &telemetry.Capture{}
	res, _ := run(t, telemetry.Options{Traces: spans, Sample: telemetry.Sampling{Ratio: 0}})

	trace := telemetry.TraceIDFor(res.RunID)
	var tasks, calls, roots int
	for _, s := range spans.Spans() {
		if s.TraceID != trace {
			t.Fatalf("span %q is outside the run's trace", s.Name)
		}
		switch {
		case strings.HasPrefix(s.Name, "loom.task"):
			tasks++
		case strings.HasPrefix(s.Name, "model.call"):
			calls++
		}
		if s.ParentID == "" {
			roots++
		}
	}
	if roots != 1 {
		t.Errorf("want one root span for the run, got %d", roots)
	}
	if tasks != 2 {
		t.Errorf("want the two escalated tasks and nothing else, got %d task spans", tasks)
	}
	if calls != 4 {
		t.Errorf("a kept task keeps its calls: want 4, got %d", calls)
	}
}

// TestTheOpsSurfaceIsWhatAnOrchestratorExpects: three endpoints, and the
// scrape parses as an exposition rather than as prose.
func TestTheOpsSurfaceIsWhatAnOrchestratorExpects(t *testing.T) {
	_, tel := run(t, telemetry.Options{})
	s := tel.Scrape()

	types, samples := 0, 0
	for _, line := range strings.Split(s, "\n") {
		switch {
		case line == "":
		case strings.HasPrefix(line, "# TYPE "):
			types++
		case strings.HasPrefix(line, "# HELP "):
		case strings.HasPrefix(line, "loom_"):
			samples++
			if !strings.Contains(line, " ") {
				t.Fatalf("a sample needs a value: %q", line)
			}
		default:
			t.Fatalf("unexpected line in the exposition: %q", line)
		}
	}
	if types < 10 || samples < 20 {
		t.Errorf("thin exposition: %d families, %d samples", types, samples)
	}
	if printed := families(s); printed != types {
		t.Errorf("the example's own family count disagrees: %d vs %d", printed, types)
	}
}
