package telemetry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
)

var t0 = time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)

func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

// feed drives one small run through an exporter: two stages, a task that
// answers on the cheap model, and a task that fails validation, escalates and
// answers on the second rung.
func feed(tel *Telemetry) {
	tel.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Pipeline: "triage",
		Kind: "barrier", Budget: core.Budget{MaxCostUSD: 5}, Time: at(0)})
	tel.Handle(observe.Event{Type: observe.StageStarted, RunID: "run_1", Stage: "classify",
		Kind: "infer", Time: at(1)})

	tel.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "run_1", Stage: "classify",
		TaskID: "t1", Records: 1, Time: at(1)})
	tel.Handle(observe.Event{Type: observe.TaskStarted, RunID: "run_1", Stage: "classify",
		TaskID: "t1", Time: at(2)})
	tel.Handle(observe.Event{Type: observe.ModelCalled, RunID: "run_1", Stage: "classify",
		TaskID: "t1", Model: "fast", Latency: time.Second, Time: at(3),
		Usage: core.Usage{InputTokens: 100, OutputTokens: 20, Requests: 1, CostUSD: 0.01}})
	tel.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "run_1", Stage: "classify",
		TaskID: "t1", Attempt: 1, Model: "fast", Time: at(3),
		Usage: core.Usage{InputTokens: 100, OutputTokens: 20, Requests: 1, CostUSD: 0.01}})

	// The interesting one: a rejected answer, an escalation, a second call.
	tel.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "run_1", Stage: "classify",
		TaskID: "t2", Records: 1, Time: at(1)})
	tel.Handle(observe.Event{Type: observe.TaskStarted, RunID: "run_1", Stage: "classify",
		TaskID: "t2", Time: at(2)})
	tel.Handle(observe.Event{Type: observe.ModelCalled, RunID: "run_1", Stage: "classify",
		TaskID: "t2", Model: "fast", Latency: time.Second, Time: at(3),
		Usage: core.Usage{InputTokens: 100, OutputTokens: 20, Requests: 1, CostUSD: 0.01}})
	tel.Handle(observe.Event{Type: observe.TaskRetried, RunID: "run_1", Stage: "classify",
		TaskID: "t2", Attempt: 1, Err: "schema violation",
		Note: "semantic: escalating to ladder level 1", Time: at(4)})
	tel.Handle(observe.Event{Type: observe.ModelCalled, RunID: "run_1", Stage: "classify",
		TaskID: "t2", Model: "deep", Latency: 2 * time.Second, Time: at(6),
		Usage: core.Usage{InputTokens: 100, OutputTokens: 40, Requests: 1, CostUSD: 0.25}})
	tel.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "run_1", Stage: "classify",
		TaskID: "t2", Attempt: 2, Model: "deep", Rung: 1, Time: at(6),
		Usage: core.Usage{InputTokens: 100, OutputTokens: 40, Requests: 1, CostUSD: 0.25}})

	tel.Handle(observe.Event{Type: observe.StageFinished, RunID: "run_1", Stage: "classify",
		Time: at(7)})
	tel.Handle(observe.Event{Type: observe.RunFinished, RunID: "run_1", Pipeline: "triage",
		Time: at(8)})
}

// value reads one series out of a rendered exposition, so the assertions
// below go through the same bytes a scraper would.
func value(t *testing.T, scrape, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(scrape, "\n") {
		name, rest, ok := strings.Cut(line, " ")
		if !ok || name != series {
			continue
		}
		var f float64
		if _, err := fmt.Sscanf(rest, "%g", &f); err != nil {
			t.Fatalf("series %q has an unparseable value %q", series, rest)
		}
		return f
	}
	t.Fatalf("series %q not in the exposition:\n%s", series, scrape)
	return 0
}

func absent(t *testing.T, scrape, series string) {
	t.Helper()
	for _, line := range strings.Split(scrape, "\n") {
		if name, _, ok := strings.Cut(line, " "); ok && name == series {
			t.Fatalf("series %q should not be exposed:\n%s", series, scrape)
		}
	}
}

// TestCostIsCountedAtTheCallNotTheTask is the property the whole metric
// surface rests on. A task that climbed an escalation ladder made two calls
// and was billed for both; its completion event carries only the usage of the
// one that answered. A cost counter fed from completions would under-report a
// run by exactly the amount the escalations cost — which is the money an
// operator most wants to see.
func TestCostIsCountedAtTheCallNotTheTask(t *testing.T) {
	tel := New(Options{})
	feed(tel)
	s := tel.Scrape()

	fast := value(t, s, `loom_cost_usd_total{model="fast",pipeline="triage",stage="classify"}`)
	deep := value(t, s, `loom_cost_usd_total{model="deep",pipeline="triage",stage="classify"}`)
	if fast != 0.02 {
		t.Errorf("two calls on the cheap model cost 0.02, counter says %v", fast)
	}
	if deep != 0.25 {
		t.Errorf("one call on the deep model cost 0.25, counter says %v", deep)
	}
	// What a completion-fed counter would have said: 0.26.
	if total := fast + deep; total != 0.27 {
		t.Errorf("the run spent 0.27 including the rejected call, counters total %v", total)
	}
	if got := value(t, s, `loom_escalations_total{pipeline="triage",stage="classify"}`); got != 1 {
		t.Errorf("want one escalation, got %v", got)
	}
	if got := value(t, s, `loom_task_retries_total{class="semantic",pipeline="triage",stage="classify"}`); got != 1 {
		t.Errorf("want one semantic retry, got %v", got)
	}
}

// TestBudgetHeadroomFollowsSpendAndReturns covers the number an operator
// alerts on. It falls as a run spends, and it is restored when the run ends,
// because an idle pipeline has spent nothing.
func TestBudgetHeadroomFollowsSpendAndReturns(t *testing.T) {
	tel := New(Options{})
	tel.Handle(observe.Event{Type: observe.RunStarted, RunID: "r", Pipeline: "triage",
		Budget: core.Budget{MaxCostUSD: 5}, Time: at(0)})
	if got := value(t, tel.Scrape(), `loom_budget_headroom_usd{pipeline="triage"}`); got != 5 {
		t.Fatalf("a run that has spent nothing has its whole wallet, got %v", got)
	}
	tel.Handle(observe.Event{Type: observe.ModelCalled, RunID: "r", Stage: "s", Model: "m",
		Usage: core.Usage{CostUSD: 1.5}, Time: at(1)})
	if got := value(t, tel.Scrape(), `loom_budget_headroom_usd{pipeline="triage"}`); got != 3.5 {
		t.Fatalf("after spending 1.50 of 5.00 the headroom is 3.50, got %v", got)
	}
	tel.Handle(observe.Event{Type: observe.RunFinished, RunID: "r", Pipeline: "triage", Time: at(2)})
	if got := value(t, tel.Scrape(), `loom_budget_headroom_usd{pipeline="triage"}`); got != 5 {
		t.Fatalf("with nothing in flight the wallet is whole again, got %v", got)
	}
}

// TestAnUnboundedRunReportsNoHeadroom: there is no honest number for how much
// is left of a ceiling nobody set, and a zero would read as an emergency.
func TestAnUnboundedRunReportsNoHeadroom(t *testing.T) {
	tel := New(Options{})
	tel.Handle(observe.Event{Type: observe.RunStarted, RunID: "r", Pipeline: "triage", Time: at(0)})
	absent(t, tel.Scrape(), `loom_budget_headroom_usd{pipeline="triage"}`)
}

// TestCardinalityIsBoundedAndTheDollarsSurvive is the property that keeps a
// generated pipeline from taking the monitoring system down with it. Past the
// ceiling the attribution collapses onto one overflow member — and the total
// is still the total, because the alternative, dropping the series, would
// have silently stopped counting money.
func TestCardinalityIsBoundedAndTheDollarsSurvive(t *testing.T) {
	tel := New(Options{MaxSeries: 4})
	for i := range 200 {
		tel.Handle(observe.Event{Type: observe.ModelCalled, RunID: "r",
			Stage: fmt.Sprintf("stage-%03d", i), Model: "m",
			Usage: core.Usage{CostUSD: 0.01}, Time: at(i)})
	}
	s := tel.Scrape()

	var lines, total float64
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(line, "loom_cost_usd_total{") {
			continue
		}
		lines++
		_, rest, _ := strings.Cut(line, " ")
		var f float64
		fmt.Sscanf(rest, "%g", &f)
		total += f
	}
	if lines > 5 { // the ceiling, plus the one overflow member
		t.Errorf("200 stages produced %v series against a ceiling of 4", lines)
	}
	if diff := total - 2.0; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("200 calls at $0.01 total $2.00; the exposition says %v", total)
	}
	if !strings.Contains(s, overflowValue) {
		t.Error("the overflow member should be visible, so an operator can see the attribution was lost")
	}
	if got := value(t, s, "loom_telemetry_overflowed_metrics"); got < 1 {
		t.Error("an exporter that has stopped attributing should say so")
	}
}

// TestPrefixSavingSplitsIntoTwoMonotonicHalves: the prefix cache's worth
// arrives as one signed number, and a counter that can fall is a series every
// rate() over it misreads.
func TestPrefixSavingSplitsIntoTwoMonotonicHalves(t *testing.T) {
	tel := New(Options{})
	base := observe.Event{Type: observe.ModelCalled, RunID: "r", Stage: "s", Model: "m"}
	write := base
	write.Saved, write.Time = -0.004, at(1) // an entry written, not yet reused
	read := base
	read.Saved, read.Time = 0.030, at(2) // and then reused
	tel.Handle(write)
	tel.Handle(read)

	s := tel.Scrape()
	if got := value(t, s, `loom_prefix_cache_saved_usd_total{model="m",pipeline="unknown",stage="s"}`); got != 0.03 {
		t.Errorf("the read saved 0.030, counter says %v", got)
	}
	if got := value(t, s, `loom_prefix_cache_premium_usd_total{model="m",pipeline="unknown",stage="s"}`); got != 0.004 {
		t.Errorf("the write cost 0.004, counter says %v", got)
	}
}

// TestExpositionIsOrderedAndTyped: two processes that saw the same events must
// produce the same bytes, or a diff between them is unreadable.
func TestExpositionIsOrderedAndTyped(t *testing.T) {
	a, b := New(Options{}), New(Options{})
	feed(a)
	feed(b)
	if a.Scrape() != b.Scrape() {
		t.Fatal("two exporters fed the same events disagree on their exposition")
	}

	s := a.Scrape()
	if !strings.Contains(s, "# TYPE loom_cost_usd_total counter") ||
		!strings.Contains(s, "# HELP loom_cost_usd_total ") {
		t.Errorf("every family needs a HELP and a TYPE:\n%s", s)
	}
	if !strings.Contains(s, `loom_model_call_duration_seconds_bucket{model="deep",pipeline="triage",stage="classify",le="2.5"} 1`) {
		t.Errorf("histogram buckets should be cumulative and labelled:\n%s", s)
	}
	if !strings.Contains(s, `loom_model_call_duration_seconds_count{model="deep",pipeline="triage",stage="classify"} 1`) {
		t.Errorf("histograms need a _count:\n%s", s)
	}
	// A family nothing touched is not printed at all: this process ran no
	// stream job, and a scraper should not be told about panes.
	absent(t, s, "loom_panes_total")
}

// TestUndeclaredNamesAreCountedRatherThanCreated: a registry that creates a
// family on first use cannot tell a new metric from a typo.
func TestUndeclaredNamesAreCountedRatherThanCreated(t *testing.T) {
	tel := New(Options{})
	tel.reg.add("not_a_declared_metric_total", 1)
	s := tel.Scrape()
	if strings.Contains(s, "not_a_declared_metric_total") {
		t.Error("an undeclared name should not become a series")
	}
	if got := value(t, s, "loom_telemetry_undeclared_writes_total"); got != 1 {
		t.Errorf("want one undeclared write counted, got %v", got)
	}
}

// TestAGaugeIsNotACounter: counters refuse to go backwards, because the
// consumer of a counter is rate().
func TestACounterRefusesToGoBackwards(t *testing.T) {
	tel := New(Options{})
	tel.reg.add(mCostUSD, 1, L(lModel, "m"))
	tel.reg.add(mCostUSD, -10, L(lModel, "m"))
	if got := tel.reg.value(mCostUSD, L(lModel, "m")); got != 1 {
		t.Errorf("a negative delta must not reach a counter, got %v", got)
	}
}

// --- Traces -------------------------------------------------------------

func spansByName(spans []Span) map[string]Span {
	out := map[string]Span{}
	for _, s := range spans {
		out[s.Name] = s
	}
	return out
}

func keepAll() Sampling { return Sampling{Ratio: 1} }

// TestTheRunIDIsTheTraceID is why the identifiers are derived rather than
// generated: a run ID from a report, a log line or the constellation view is
// enough to find the trace, with no correlation field anybody had to remember
// to log.
func TestTheRunIDIsTheTraceID(t *testing.T) {
	cap := &Capture{}
	tel := New(Options{Traces: cap, Sample: keepAll()})
	feed(tel)
	flush(t, tel)

	want := TraceIDFor("run_1")
	if len(want) != 32 {
		t.Fatalf("a trace ID is 32 hex characters, got %q", want)
	}
	spans := cap.Spans()
	if len(spans) == 0 {
		t.Fatal("no spans exported")
	}
	for _, s := range spans {
		if s.TraceID != want {
			t.Fatalf("span %q is in trace %s, want %s", s.Name, s.TraceID, want)
		}
	}
}

// TestTheSpanTreeMirrorsTheRun: run over stage over task over call, so a
// tracing backend's flame graph is the run's own shape.
func TestTheSpanTreeMirrorsTheRun(t *testing.T) {
	cap := &Capture{}
	tel := New(Options{Traces: cap, Sample: keepAll()})
	feed(tel)
	flush(t, tel)

	by := spansByName(cap.Spans())
	run, ok := by["loom.run triage"]
	if !ok {
		t.Fatal("no run span")
	}
	if run.ParentID != "" {
		t.Error("the run span is the root")
	}
	stage := by["loom.stage classify"]
	if stage.ParentID != run.SpanID {
		t.Errorf("the stage hangs off the run, got parent %q", stage.ParentID)
	}
	task := by["loom.task classify"]
	if task.ParentID != stage.SpanID {
		t.Errorf("a task hangs off its stage, got parent %q", task.ParentID)
	}
	call := by["model.call deep"]
	if call.Kind != SpanClient {
		t.Error("a model call is a client span")
	}
	var cost float64
	for _, a := range call.Attrs {
		if a.Key == "loom.cost_usd" {
			cost, _ = a.Value.(float64)
		}
	}
	if cost != 0.25 {
		t.Errorf("a call span should carry what it cost, got %v", cost)
	}
	if call.Start.After(call.End) {
		t.Error("a call span is built backwards from its latency and must not invert")
	}
}

// TestSamplingNeverDropsWhatSomebodyWouldOpen: the sampler decides at the end
// of a task, so it can see the failure a head sampler could not.
func TestSamplingNeverDropsWhatSomebodyWouldOpen(t *testing.T) {
	cap := &Capture{}
	tel := New(Options{Traces: cap, Sample: Sampling{Ratio: 0}})
	feed(tel)
	// A task that simply failed.
	tel.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_2", Pipeline: "triage", Time: at(10)})
	tel.Handle(observe.Event{Type: observe.StageStarted, RunID: "run_2", Stage: "classify", Time: at(10)})
	tel.Handle(observe.Event{Type: observe.TaskStarted, RunID: "run_2", Stage: "classify",
		TaskID: "bad", Time: at(11)})
	tel.Handle(observe.Event{Type: observe.TaskFailed, RunID: "run_2", Stage: "classify",
		TaskID: "bad", Err: "boom", Time: at(12)})
	flush(t, tel)

	var tasks, failed int
	for _, s := range cap.Spans() {
		if !strings.HasPrefix(s.Name, "loom.task") {
			continue
		}
		tasks++
		if s.Status == StatusError {
			failed++
		}
	}
	if failed != 1 {
		t.Errorf("a failure must survive any sampling ratio, got %d", failed)
	}
	// t2 retried and escalated, so it is interesting too; t1 is ordinary and
	// a ratio of zero drops it.
	if tasks != 2 {
		t.Errorf("want the failure and the escalation, got %d task spans", tasks)
	}
}

// TestSlowTasksSurviveSampling: the other rule a head sampler cannot apply.
func TestSlowTasksSurviveSampling(t *testing.T) {
	cap := &Capture{}
	tel := New(Options{Traces: cap, Sample: Sampling{Ratio: 0, Slow: 3 * time.Second}})
	tel.Handle(observe.Event{Type: observe.RunStarted, RunID: "r", Pipeline: "p", Time: at(0)})
	tel.Handle(observe.Event{Type: observe.StageStarted, RunID: "r", Stage: "s", Time: at(0)})
	tel.Handle(observe.Event{Type: observe.TaskStarted, RunID: "r", Stage: "s", TaskID: "quick", Time: at(1)})
	tel.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "r", Stage: "s", TaskID: "quick", Time: at(2)})
	tel.Handle(observe.Event{Type: observe.TaskStarted, RunID: "r", Stage: "s", TaskID: "slow", Time: at(1)})
	tel.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "r", Stage: "s", TaskID: "slow", Time: at(9)})
	flush(t, tel)

	names := map[string]bool{}
	for _, s := range cap.Spans() {
		for _, a := range s.Attrs {
			if a.Key == "loom.task_id" {
				names[fmt.Sprint(a.Value)] = true
			}
		}
	}
	if !names["slow"] {
		t.Error("an eight-second task is exactly what a trace is opened for")
	}
	if names["quick"] {
		t.Error("a one-second task at ratio zero should have been sampled away")
	}
}

// TestAWorkersSpansJoinTheDriversTrace: no context propagation anywhere, and
// yet two processes assemble one trace — because the trace and the stage
// spans are derived from what the envelope already carries, and only the
// per-process view of a task is disambiguated.
func TestAWorkersSpansJoinTheDriversTrace(t *testing.T) {
	driverCap, workerCap := &Capture{}, &Capture{}
	driver := New(Options{Instance: "driver-1", Traces: driverCap, Sample: keepAll()})
	worker := New(Options{Instance: "worker-7", Traces: workerCap, Sample: keepAll(),
		Pipeline: "triage"})

	driver.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Pipeline: "triage", Time: at(0)})
	driver.Handle(observe.Event{Type: observe.StageStarted, RunID: "run_1", Stage: "classify", Time: at(0)})
	driver.Handle(observe.Event{Type: observe.TaskStarted, RunID: "run_1", Stage: "classify",
		TaskID: "t1", Time: at(1)})
	driver.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "run_1", Stage: "classify",
		TaskID: "t1", Time: at(5)})

	// The worker never saw the run header; all it has is the envelope.
	worker.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Pipeline: "triage", Time: at(1)})
	worker.Handle(observe.Event{Type: observe.StageStarted, RunID: "run_1", Stage: "classify", Time: at(1)})
	worker.Handle(observe.Event{Type: observe.TaskStarted, RunID: "run_1", Stage: "classify",
		TaskID: "t1", Time: at(2)})
	worker.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "run_1", Stage: "classify",
		TaskID: "t1", Time: at(4)})

	flush(t, driver)
	flush(t, worker)

	d, w := spansByName(driverCap.Spans()), spansByName(workerCap.Spans())
	dt, wt := d["loom.task classify"], w["loom.task classify"]
	if dt.SpanID == "" || wt.SpanID == "" {
		t.Fatal("both processes should have exported their task span")
	}
	if dt.TraceID != wt.TraceID {
		t.Error("both processes must derive the same trace")
	}
	if dt.SpanID == wt.SpanID {
		t.Error("two processes' views of one task are two spans, not one ID written twice")
	}
	// Neither stage span has been closed yet, so neither has been exported —
	// and the parentage still agrees, because the ID was derived rather than
	// handed over.
	stage := spanIDFor("run_1", "stage:classify")
	if dt.ParentID != stage || wt.ParentID != stage {
		t.Errorf("both tasks should hang off the same derived stage span: driver=%q worker=%q want %q",
			dt.ParentID, wt.ParentID, stage)
	}
}

func flush(t *testing.T, tel *Telemetry) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tel.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// --- The ops surface ----------------------------------------------------

// TestReadinessIsSeparateFromLiveness: a process that is alive but cannot
// take work should be left running and sent nothing, which is a distinction
// only two endpoints can make.
func TestReadinessIsSeparateFromLiveness(t *testing.T) {
	tel := New(Options{})
	broken := true
	tel.Ready("queue", func(context.Context) error {
		if broken {
			return fmt.Errorf("no connection")
		}
		return nil
	})
	srv := httptest.NewServer(tel.Handler())
	defer srv.Close()

	get := func(path string) (int, string) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	if code, _ := get("/healthz"); code != http.StatusOK {
		t.Errorf("a live process answers /healthz 200, got %d", code)
	}
	code, body := get("/readyz")
	if code != http.StatusServiceUnavailable {
		t.Errorf("a process whose dependency is down is not ready, got %d", code)
	}
	if !strings.Contains(body, "queue: no connection") {
		t.Errorf("/readyz should name what failed, got %q", body)
	}
	broken = false
	if code, _ := get("/readyz"); code != http.StatusOK {
		t.Errorf("want 200 once the dependency recovered, got %d", code)
	}

	feed(tel)
	code, body = get("/metrics")
	if code != http.StatusOK || !strings.Contains(body, "loom_cost_usd_total") {
		t.Errorf("/metrics should serve the exposition, got %d:\n%s", code, body)
	}
}

// --- Stream mode --------------------------------------------------------

// TestAStreamJobIsWatchedByItsWatermark. A stream job has no end, so every
// question about it is a question about a gauge. The watermark is exported as
// a timestamp rather than as a lag, because a lag computed in the process is
// as stale as the last event — which for a stream that has stopped moving is
// exactly when the number matters.
func TestAStreamJobIsWatchedByItsWatermark(t *testing.T) {
	tel := New(Options{})
	eventTime := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)

	tel.Handle(observe.Event{Type: observe.RunStarted, RunID: "job", Pipeline: "watchtower",
		Kind: "stream", Time: at(0)})
	tel.Handle(observe.Event{Type: observe.SplitOpened, RunID: "job", Split: "part-0", Time: at(1)})
	tel.Handle(observe.Event{Type: observe.SplitOpened, RunID: "job", Split: "part-1", Time: at(1)})
	tel.Handle(observe.Event{Type: observe.SplitRetired, RunID: "job", Split: "part-1", Time: at(2)})
	tel.Handle(observe.Event{Type: observe.WatermarkAdvanced, RunID: "job",
		Watermark: eventTime, Lag: 9 * time.Second, Time: at(3)})
	tel.Handle(observe.Event{Type: observe.PaneFired, RunID: "job", Stage: "per-minute",
		Records: 12, Note: "final", Time: at(4)})
	tel.Handle(observe.Event{Type: observe.PaneFired, RunID: "job", Stage: "per-minute",
		Records: 3, Note: "early", Time: at(4)})
	tel.Handle(observe.Event{Type: observe.RecordsLate, RunID: "job", Stage: "per-minute",
		Records: 2, Time: at(5)})
	tel.Handle(observe.Event{Type: observe.SinkWrote, RunID: "job", Stage: "digest",
		Records: 12, Time: at(6)})
	tel.Handle(observe.Event{Type: observe.CheckpointCommitted, RunID: "job",
		Latency: 120 * time.Millisecond, Time: at(7)})
	tel.Handle(observe.Event{Type: observe.CheckpointSkipped, RunID: "job", Time: at(8)})

	s := tel.Scrape()
	if got, want := value(t, s, `loom_watermark_seconds{pipeline="watchtower"}`),
		float64(eventTime.Unix()); got != want {
		t.Errorf("the watermark should be exported as a Unix timestamp: got %v want %v", got, want)
	}
	if got := value(t, s, `loom_splits_open{pipeline="watchtower"}`); got != 1 {
		t.Errorf("two opened and one retired leaves one: got %v", got)
	}
	// The windower's own vocabulary, not a paraphrase of it.
	for kind, want := range map[string]float64{"final": 1, "early": 1} {
		series := `loom_panes_total{kind="` + kind + `",pipeline="watchtower",stage="per-minute"}`
		if got := value(t, s, series); got != want {
			t.Errorf("%s panes: got %v want %v", kind, got, want)
		}
	}
	if got := value(t, s, `loom_records_late_total{pipeline="watchtower",stage="per-minute"}`); got != 2 {
		t.Errorf("late records: got %v", got)
	}
	if got := value(t, s, `loom_checkpoints_total{outcome="committed",pipeline="watchtower"}`); got != 1 {
		t.Errorf("committed checkpoints: got %v", got)
	}
	if got := value(t, s, `loom_checkpoints_total{outcome="skipped",pipeline="watchtower"}`); got != 1 {
		t.Errorf("skipped checkpoints: got %v", got)
	}
	// A stream job's events carry a run ID and no pipeline name; the label
	// still has to be right, which is what the run header is held for.
	if strings.Contains(s, `pipeline="unknown"`) {
		t.Errorf("stream events should be attributed to the job's pipeline:\n%s", s)
	}
}

// TestWorkWhoseRunWasNeverSeenIsStillLabelled: a worker process gets
// envelopes, not run headers, and Options.Pipeline is how a deployment names
// what it is running.
func TestWorkWhoseRunWasNeverSeenIsStillLabelled(t *testing.T) {
	tel := New(Options{Pipeline: "ticket-triage"})
	tel.Handle(observe.Event{Type: observe.ModelCalled, RunID: "run_elsewhere",
		Stage: "classify", Model: "m", Latency: time.Second, Time: at(1),
		Usage: core.Usage{CostUSD: 0.02, Requests: 1}})

	s := tel.Scrape()
	if got := value(t, s, `loom_cost_usd_total{model="m",pipeline="ticket-triage",stage="classify"}`); got != 0.02 {
		t.Errorf("a worker's spend should be labelled by the pipeline it serves, got %v", got)
	}
}
