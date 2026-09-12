// Package telemetry exports a Loom run to the monitoring stack a deployment
// already runs: Prometheus metrics over an HTTP endpoint, and OpenTelemetry
// spans over OTLP.
//
// Loom observes itself thoroughly. There is a typed event bus behind every
// lifecycle transition, a run report with per-stage cost and latency
// percentiles, and a constellation view in which every task is a star. All of
// it is in-process, and all of it ends when the process does. That is the
// right shape for developing a pipeline and the wrong shape for operating
// one: a deployment does not ask what a run cost, it asks what is being spent
// right now, whether the wallet lasts the hour, which stage is throttled, and
// whether the escalation ladder started climbing while nobody was looking.
// Those are questions for the system a team already pages off, and this
// package is the seam onto it.
//
//	tel := telemetry.New(telemetry.Options{Service: "ticket-triage"})
//	go tel.Serve(ctx, ":9090")            // /metrics, /healthz, /readyz
//	res, err := loom.Run(ctx, p, loom.WithTelemetry(tel), ...)
//
// What is exported is chosen by what is scarce. A classic data framework
// exports throughput and latency because cores and seconds are what it runs
// out of; Loom exports dollars, tokens, rate-limit headroom and wallet
// headroom beside them, because those are what a run actually runs out of.
// Cost is counted at the call rather than at the task, so a record that
// climbed an escalation ladder is counted as having paid for every rung —
// which is what the provider charged.
//
// The full metric surface, the trace shape, the cardinality bound and what to
// alert on are in docs/TELEMETRY.md.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
)

// --- The metric surface -------------------------------------------------
//
// Every family this package can write is declared here, and the constants
// below are the only names the folding code can use. The table is meant to be
// read: it is the contract a deployment's dashboards and alerts are built
// against, and it should be possible to review a change to it the way one
// reviews a change to a schema.

const (
	mRuns          metric = "runs_total"
	mRunsActive    metric = "runs_active"
	mRunDuration   metric = "run_duration_seconds"
	mStageDuration metric = "stage_duration_seconds"

	mCostUSD       metric = "cost_usd_total"
	mTokens        metric = "tokens_total"
	mModelCalls    metric = "model_calls_total"
	mCallDuration  metric = "model_call_duration_seconds"
	mPrefixSaved   metric = "prefix_cache_saved_usd_total"
	mPrefixPremium metric = "prefix_cache_premium_usd_total"

	mBudgetHeadroom metric = "budget_headroom_usd"
	mBudgetExceeded metric = "budget_exceeded_total"
	mProjectedCost  metric = "projected_cost_usd"
	mProjectedCeil  metric = "projected_ceiling_usd"

	mTasks        metric = "tasks_total"
	mTaskDuration metric = "task_duration_seconds"
	mTaskQueue    metric = "task_queue_seconds"
	mRetries      metric = "task_retries_total"
	mEscalations  metric = "escalations_total"
	mCacheHits    metric = "cache_hits_total"

	mRouted       metric = "routing_decisions_total"
	mRoutedSkips  metric = "routing_skipped_calls_total"
	mRounds       metric = "iteration_rounds_total"
	mContextBuild metric = "context_materializations_total"
	mContextBytes metric = "context_retained_bytes_total"

	mToolCalls    metric = "tool_calls_total"
	mToolDuration metric = "tool_call_duration_seconds"
	mToolQueue    metric = "tool_queue_seconds"
	mToolInFlight metric = "tool_calls_in_flight"
	mToolSlots    metric = "tool_call_slots"

	mFindings      metric = "findings_total"
	mFindingsSaved metric = "findings_saved_usd_total"
	mBlackboard    metric = "blackboard_posts_total"

	mWatermark   metric = "watermark_seconds"
	mSplitLag    metric = "split_lag_seconds"
	mSplitsOpen  metric = "splits_open"
	mPanes       metric = "panes_total"
	mPaneRecords metric = "pane_records_total"
	mLate        metric = "records_late_total"
	mSinkRecords metric = "sink_records_total"
	mCheckpoints metric = "checkpoints_total"
	mCheckpointD metric = "checkpoint_duration_seconds"

	mEvents      metric = "telemetry_events_total"
	mOverflowed  metric = "telemetry_overflowed_metrics"
	mUndeclared  metric = "telemetry_undeclared_writes_total"
	mSpansDrop   metric = "telemetry_spans_dropped_total"
	mExportFails metric = "telemetry_export_failures_total"
	mTracked     metric = "telemetry_tracked_runs"
)

var surface = []decl{
	{mRuns, "Runs started, by pipeline.", kindCounter, nil},
	{mRunsActive, "Runs currently executing, by pipeline.", kindGauge, nil},
	{mRunDuration, "Wall-clock duration of a run.", kindHistogram, durationBuckets},
	{mStageDuration, "Wall-clock duration of a stage.", kindHistogram, durationBuckets},

	{mCostUSD, "Dollars charged by the provider, counted at the call.", kindCounter, nil},
	{mTokens, "Tokens billed, by kind (input, output, cache_read, cache_write).", kindCounter, nil},
	{mModelCalls, "Model calls made, by outcome.", kindCounter, nil},
	{mCallDuration, "Latency of one model call.", kindHistogram, durationBuckets},
	{mPrefixSaved, "Dollars the provider's prompt-prefix cache saved on reads.", kindCounter, nil},
	{mPrefixPremium, "Dollars paid to write the provider's prompt-prefix cache.", kindCounter, nil},

	{mBudgetHeadroom, "Dollars left in the tightest wallet in flight for this pipeline.", kindGauge, nil},
	{mBudgetExceeded, "Times a run budget stopped further work.", kindCounter, nil},
	{mProjectedCost, "Expected cost of the last projection (loom.Explain).", kindGauge, nil},
	{mProjectedCeil, "Ceiling of the last projection: what it cannot exceed.", kindGauge, nil},

	{mTasks, "Tasks settled, by outcome.", kindCounter, nil},
	{mTaskDuration, "Wall-clock duration of a task, admission to result.", kindHistogram, durationBuckets},
	{mTaskQueue, "Time a task waited between being scheduled and starting.", kindHistogram, durationBuckets},
	{mRetries, "Task retries, by failure class.", kindCounter, nil},
	{mEscalations, "Retries that climbed a stage's model escalation ladder.", kindCounter, nil},
	{mCacheHits, "Tasks settled without a model call, by how (stored, coalesced); a subset of tasks_total.", kindCounter, nil},

	{mRouted, "Router decisions, by kind (routed, probe).", kindCounter, nil},
	{mRoutedSkips, "Ladder rungs the router did not call.", kindCounter, nil},
	{mRounds, "Supersteps run by iterative stages.", kindCounter, nil},
	{mContextBuild, "Evolving contexts materialized, by mode (spliced, rebuilt, diverged).", kindCounter, nil},
	{mContextBytes, "Bytes a splice reused instead of rendering again.", kindCounter, nil},

	{mToolCalls, "MCP tool calls, by outcome.", kindCounter, nil},
	{mToolDuration, "Latency of one MCP tool call.", kindHistogram, durationBuckets},
	{mToolQueue, "Time a tool call waited for one of its server's call slots.", kindHistogram, durationBuckets},
	{mToolInFlight, "Calls in flight against an MCP server.", kindGauge, nil},
	{mToolSlots, "Concurrent call ceiling of an MCP server.", kindGauge, nil},

	{mFindings, "Commons activity, by kind (served, learned, coalesced, published).", kindCounter, nil},
	{mFindingsSaved, "Dollars of research the commons served instead of repeating.", kindCounter, nil},
	{mBlackboard, "Entries appended to a fleet's blackboard, by topic.", kindCounter, nil},

	{mWatermark, "Event time the job has declared complete, as a Unix timestamp.", kindGauge, nil},
	{mSplitLag, "How far behind the job's watermark a split was holding.", kindHistogram, durationBuckets},
	{mSplitsOpen, "Source partitions currently being read.", kindGauge, nil},
	{mPanes, "Window firings, by kind (final, early, late).", kindCounter, nil},
	{mPaneRecords, "Records carried by window firings.", kindCounter, nil},
	{mLate, "Records that arrived for a window already gone.", kindCounter, nil},
	{mSinkRecords, "Records made durable by a sink.", kindCounter, nil},
	{mCheckpoints, "Checkpoint attempts, by outcome (committed, skipped).", kindCounter, nil},
	{mCheckpointD, "How long the job was held still to take a checkpoint.", kindHistogram, durationBuckets},

	{mEvents, "Events folded into this registry.", kindCounter, nil},
	{mOverflowed, "Metric families that have reached their series ceiling.", kindGauge, nil},
	{mUndeclared, "Writes to metric names that were never declared (a bug).", kindCounter, nil},
	{mSpansDrop, "Spans dropped because the export queue was full.", kindCounter, nil},
	{mExportFails, "Failed span export attempts.", kindCounter, nil},
	{mTracked, "Runs this exporter is currently holding state for.", kindGauge, nil},
}

// Label names. They are constants for the same reason the metric names are:
// a dashboard is written against them.
const (
	lPipeline = "pipeline"
	lStage    = "stage"
	lModel    = "model"
	lOutcome  = "outcome"
	lKind     = "kind"
	lClass    = "class"
	lServer   = "server"
	lTool     = "tool"
	lTopic    = "topic"
)

// unknownPipeline labels work whose run this process never saw start. A
// worker in a fleet is the ordinary case: its envelopes carry a run ID and a
// stage, and the pipeline's name stayed with the process that compiled it.
// Options.Pipeline names it when the deployment knows.
const unknownPipeline = "unknown"

// --- Options ------------------------------------------------------------

// Options configures an exporter. The zero value is usable: it exports
// metrics under the "loom" namespace and no traces.
type Options struct {
	// Namespace prefixes every metric name. Default "loom".
	Namespace string
	// Service is the deployment's name for this process — service.name on
	// every span, and the value most tracing backends group by. Default
	// "loom".
	Service string
	// Instance distinguishes this process from its replicas
	// (service.instance.id). It also disambiguates task spans, so two
	// processes tracing the same run produce two views of a task rather than
	// two writers of one span. Default: hostname and pid.
	Instance string
	// Pipeline is the name to label work whose run this process did not see
	// start, which in practice means a worker process. Default "unknown".
	Pipeline string
	// Attrs are extra resource attributes on every span — region, cluster,
	// deployment environment, whatever the deployment groups by.
	Attrs map[string]string

	// MaxSeries bounds how many label combinations each metric family holds
	// before the rest fold into one overflow member. Default 500.
	MaxSeries int
	// MaxRuns bounds how many runs this exporter holds per-run state for at
	// once. Beyond it, new runs are counted but not traced. Default 256.
	MaxRuns int
	// MaxTasks bounds the in-flight task state held per run. Default 4096.
	MaxTasks int

	// Endpoint is an OTLP/HTTP collector to send spans to, such as
	// "http://localhost:4318". Setting it is the ordinary way to turn
	// tracing on: New builds an exporter against it with this process's
	// resource attributes already attached.
	//
	// Left empty, the two environment variables every OTLP exporter reads
	// are consulted — OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, then
	// OTEL_EXPORTER_OTLP_ENDPOINT — so a deployment that already sets them
	// for its other services gets Loom's traces without a code change, and a
	// developer's laptop that sets neither gets no exporter and no attempt to
	// reach one.
	Endpoint string
	// OTLP tunes the exporter Endpoint builds: headers, timeout, retries,
	// compression.
	OTLP OTLPOptions
	// Traces receives spans, overriding Endpoint. Nil, with no endpoint,
	// disables tracing entirely — the metric side costs a map lookup and an
	// add, and a deployment that only wants /metrics should not pay for
	// anything else.
	Traces Exporter
	// Sample decides which task spans survive. The zero value keeps only
	// failures, retries and escalations, which is the right default for a
	// large run: those are the spans anybody opens.
	Sample Sampling
	// SpanQueue bounds the export queue. Default 4096.
	SpanQueue int
	// SpanBatch is how many spans are exported at once. Default 256.
	SpanBatch int
	// SpanInterval is how long a partial batch waits. Default 5s.
	SpanInterval time.Duration
}

func (o *Options) defaults() {
	if o.Namespace == "" {
		o.Namespace = "loom"
	}
	if o.Service == "" {
		// OTEL_SERVICE_NAME is what the rest of a deployment's services are
		// named by; a Loom process in the same deployment should answer to
		// the same convention.
		o.Service = firstEnv("OTEL_SERVICE_NAME")
	}
	if o.Service == "" {
		o.Service = "loom"
	}
	if o.Instance == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		o.Instance = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if o.Pipeline == "" {
		o.Pipeline = unknownPipeline
	}
	if o.MaxSeries <= 0 {
		o.MaxSeries = 500
	}
	if o.MaxRuns <= 0 {
		o.MaxRuns = 256
	}
	if o.MaxTasks <= 0 {
		o.MaxTasks = 4096
	}
	if o.SpanQueue <= 0 {
		o.SpanQueue = 4096
	}
	if o.SpanBatch <= 0 {
		o.SpanBatch = 256
	}
	if o.SpanInterval <= 0 {
		o.SpanInterval = 5 * time.Second
	}
	if o.Traces == nil && o.Endpoint == "" {
		o.Endpoint = firstEnv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT")
	}
}

func firstEnv(names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	return ""
}

// Resource returns the attributes every span from this process carries. It is
// exported for a deployment writing its own Exporter, which needs the same
// identity the built-in one attaches.
func (o Options) Resource() []Attr {
	o.defaults()
	attrs := []Attr{
		{Key: "service.name", Value: o.Service},
		{Key: "service.instance.id", Value: o.Instance},
		{Key: "telemetry.sdk.name", Value: "loom"},
		{Key: "telemetry.sdk.language", Value: "go"},
	}
	for _, k := range sortedKeys(o.Attrs) {
		attrs = append(attrs, Attr{Key: k, Value: o.Attrs[k]})
	}
	return attrs
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// --- Telemetry ----------------------------------------------------------

// Telemetry folds a run's events into metrics and spans. It is safe for
// concurrent use, and every method on the event path is non-blocking: a
// collector that has gone away costs a counter, never a stalled run.
type Telemetry struct {
	opts Options
	reg  *registry
	tr   *tracer

	spansDropped atomic.Int64
	exportFails  atomic.Int64

	mu    sync.Mutex
	runs  map[string]*runState
	seen  map[string]core.Budget // last budget seen per pipeline
	ready []check
}

type check struct {
	name string
	fn   func(context.Context) error
}

type runState struct {
	pipeline string
	budget   core.Budget
	spent    float64
	started  time.Time
	span     *openSpan
	stages   map[string]*openSpan
	tasks    map[string]*taskState
	dropped  int
}

type taskState struct {
	scheduled time.Time
	started   time.Time
	span      *openSpan
	calls     int
	// interesting marks a task no sampler may drop: it retried, climbed the
	// ladder, or failed.
	interesting bool
}

// New returns an exporter. Close it when the process is shutting down so that
// the last batch of spans is flushed.
func New(opts Options) *Telemetry {
	opts.defaults()
	t := &Telemetry{
		opts: opts,
		reg:  newRegistry(opts.Namespace, opts.MaxSeries, surface),
		runs: map[string]*runState{},
		seen: map[string]core.Budget{},
	}
	exp := opts.Traces
	if exp == nil && opts.Endpoint != "" {
		exp = OTLP(opts.Endpoint, opts.Resource(), opts.OTLP)
	}
	if exp != nil {
		t.tr = newTracer(exp, opts.SpanQueue, opts.SpanBatch, opts.SpanInterval,
			func() { t.spansDropped.Add(1) },
			func() { t.exportFails.Add(1) })
	}
	return t
}

// Handle folds one event. It is the function loom.WithTelemetry attaches to
// the bus, and it is safe to attach to several buses at once.
func (t *Telemetry) Handle(e observe.Event) {
	t.reg.add(mEvents, 1)

	t.mu.Lock()
	defer t.mu.Unlock()

	switch e.Type {
	case observe.RunStarted:
		t.runStarted(e)
	case observe.RunFinished:
		t.runFinished(e)
	case observe.StageStarted:
		t.stageStarted(e)
	case observe.StageFinished:
		t.stageFinished(e)
	case observe.TaskScheduled:
		t.taskScheduled(e)
	case observe.TaskStarted:
		t.taskStarted(e)
	case observe.TaskCompleted:
		t.taskSettled(e, false)
	case observe.TaskFailed:
		t.taskSettled(e, true)
	case observe.TaskRetried:
		t.taskRetried(e)
	case observe.ModelCalled:
		t.modelCalled(e)
	case observe.CacheHit:
		t.cacheHit(e)
	case observe.BudgetExceeded:
		t.budgetExceeded(e)
	case observe.TaskRouted:
		t.routed(e)
	case observe.RoundFinished:
		t.reg.add(mRounds, 1, L(lPipeline, t.pipelineOf(e)), L(lStage, e.Stage))
	case observe.MCPCalled:
		t.toolCalled(e)
	case observe.MCPConnected:
		t.reg.set(mToolSlots, float64(e.Slots), L(lServer, e.Server))
	case observe.FindingServed:
		t.reg.add(mFindings, 1, L(lKind, "served"))
		t.reg.add(mFindingsSaved, e.Saved)
	case observe.FindingLearned:
		t.reg.add(mFindings, 1, L(lKind, "learned"))
	case observe.FindingCoalesced:
		t.reg.add(mFindings, 1, L(lKind, "coalesced"))
	case observe.FindingPublished:
		t.reg.add(mFindings, 1, L(lKind, "published"))
	case observe.BlackboardPosted:
		t.reg.add(mBlackboard, 1, L(lTopic, e.Topic))
	case observe.DeltaSpliced:
		t.reg.add(mContextBuild, 1, L(lPipeline, t.pipelineOf(e)), L(lStage, e.Stage), L(lKind, "spliced"))
		t.reg.add(mContextBytes, float64(e.Retained), L(lPipeline, t.pipelineOf(e)), L(lStage, e.Stage))
	case observe.DeltaRebuilt:
		t.reg.add(mContextBuild, 1, L(lPipeline, t.pipelineOf(e)), L(lStage, e.Stage), L(lKind, "rebuilt"))
	case observe.DeltaDiverged:
		t.reg.add(mContextBuild, 1, L(lPipeline, t.pipelineOf(e)), L(lStage, e.Stage), L(lKind, "diverged"))
		t.runEvent(e, "delta.diverged", Attr{Key: "loom.note", Value: e.Note})
	case observe.RunProjected:
		t.projected(e)
	case observe.SplitOpened:
		t.reg.addGauge(mSplitsOpen, 1, L(lPipeline, t.pipelineOf(e)))
	case observe.SplitRetired:
		t.reg.addGauge(mSplitsOpen, -1, L(lPipeline, t.pipelineOf(e)))
	case observe.WatermarkAdvanced:
		t.watermark(e)
	case observe.PaneFired:
		p := t.pipelineOf(e)
		t.reg.add(mPanes, 1, L(lPipeline, p), L(lStage, e.Stage), L(lKind, paneKind(e.Note)))
		t.reg.add(mPaneRecords, float64(e.Records), L(lPipeline, p), L(lStage, e.Stage))
	case observe.RecordsLate:
		t.reg.add(mLate, float64(e.Records), L(lPipeline, t.pipelineOf(e)), L(lStage, e.Stage))
	case observe.SinkWrote:
		t.reg.add(mSinkRecords, float64(e.Records), L(lPipeline, t.pipelineOf(e)), L(lStage, e.Stage))
	case observe.CheckpointCommitted:
		t.reg.add(mCheckpoints, 1, L(lPipeline, t.pipelineOf(e)), L(lOutcome, "committed"))
		t.reg.observe(mCheckpointD, e.Latency.Seconds(), L(lPipeline, t.pipelineOf(e)))
	case observe.CheckpointSkipped:
		t.reg.add(mCheckpoints, 1, L(lPipeline, t.pipelineOf(e)), L(lOutcome, "skipped"))
	}
}

// pipelineOf resolves the label for an event. Only the run header carries the
// pipeline's name, so everything else is resolved through the run state — and
// through Options.Pipeline when this process never saw the run start.
func (t *Telemetry) pipelineOf(e observe.Event) string {
	if e.Pipeline != "" {
		return e.Pipeline
	}
	if r := t.runs[e.RunID]; r != nil {
		return r.pipeline
	}
	return t.opts.Pipeline
}

// paneKind narrows the windower's note to the label vocabulary. It is an
// allowlist rather than a pass-through because a label value is part of a
// dashboard's query: a note that gained a word would silently become a
// different series.
func paneKind(note string) string {
	switch note {
	case "final", "early", "late":
		return note
	default:
		return "final"
	}
}

// --- Run and stage ------------------------------------------------------

func (t *Telemetry) runStarted(e observe.Event) {
	name := e.Pipeline
	if name == "" {
		name = t.opts.Pipeline
	}
	t.reg.add(mRuns, 1, L(lPipeline, name))
	t.reg.addGauge(mRunsActive, 1, L(lPipeline, name))

	if len(t.runs) >= t.opts.MaxRuns {
		// State is refused rather than evicted: a run whose state was dropped
		// halfway through would report half a trace and half its latencies,
		// which is worse than reporting none. Its calls and dollars are still
		// counted — those need no per-run state.
		return
	}
	r := &runState{pipeline: name, budget: e.Budget, started: e.Time,
		stages: map[string]*openSpan{}, tasks: map[string]*taskState{}}
	t.runs[e.RunID] = r
	if e.Budget != (core.Budget{}) {
		t.seen[name] = e.Budget
	}
	t.headroom(name)

	if t.tr != nil {
		trace := TraceIDFor(e.RunID)
		r.span = &openSpan{Span: Span{
			TraceID: trace, SpanID: spanIDFor(e.RunID, "run"),
			Name: "loom.run " + name, Kind: SpanInternal, Start: e.Time,
		}}
		r.span.attr("loom.run_id", e.RunID)
		r.span.attr("loom.pipeline", name)
		r.span.attr("loom.driver", e.Kind)
		if e.Budget.MaxCostUSD > 0 {
			r.span.attr("loom.budget_usd", e.Budget.MaxCostUSD)
		}
	}
}

func (t *Telemetry) runFinished(e observe.Event) {
	r := t.runs[e.RunID]
	name := e.Pipeline
	if name == "" && r != nil {
		name = r.pipeline
	}
	if name == "" {
		name = t.opts.Pipeline
	}
	t.reg.addGauge(mRunsActive, -1, L(lPipeline, name))
	if r == nil {
		return
	}
	if !r.started.IsZero() && !e.Time.IsZero() {
		t.reg.observe(mRunDuration, e.Time.Sub(r.started).Seconds(), L(lPipeline, name))
	}
	if t.tr != nil && r.span != nil {
		// Stages that never published a finish — a run cut short by a budget
		// trip or a cancelled context — are closed here rather than leaked.
		for _, sp := range r.stages {
			t.tr.emit(sp.finish(e.Time))
		}
		r.span.attr("loom.cost_usd", round(r.spent))
		t.tr.emit(r.span.finish(e.Time))
	}
	delete(t.runs, e.RunID)
	t.headroom(name)
}

func (t *Telemetry) stageStarted(e observe.Event) {
	r := t.runs[e.RunID]
	if r == nil || t.tr == nil {
		return
	}
	parent := ""
	if r.span != nil {
		parent = r.span.SpanID
	}
	sp := &openSpan{Span: Span{
		TraceID: TraceIDFor(e.RunID), SpanID: spanIDFor(e.RunID, "stage:"+e.Stage),
		ParentID: parent, Name: "loom.stage " + e.Stage, Kind: SpanInternal, Start: e.Time,
	}}
	sp.attr("loom.stage", e.Stage)
	sp.attr("loom.stage_kind", e.Kind)
	sp.attr("loom.pipeline", r.pipeline)
	r.stages[e.Stage] = sp
}

func (t *Telemetry) stageFinished(e observe.Event) {
	r := t.runs[e.RunID]
	if r == nil {
		return
	}
	sp := r.stages[e.Stage]
	if sp == nil {
		return
	}
	delete(r.stages, e.Stage)
	if !sp.Start.IsZero() && !e.Time.IsZero() {
		t.reg.observe(mStageDuration, e.Time.Sub(sp.Start).Seconds(),
			L(lPipeline, r.pipeline), L(lStage, e.Stage))
	}
	if t.tr != nil {
		t.tr.emit(sp.finish(e.Time))
	}
}

// --- Tasks --------------------------------------------------------------

func (t *Telemetry) task(e observe.Event, create bool) (*runState, *taskState) {
	r := t.runs[e.RunID]
	if r == nil || e.TaskID == "" {
		return r, nil
	}
	if ts := r.tasks[e.TaskID]; ts != nil {
		return r, ts
	}
	if !create {
		return r, nil
	}
	if len(r.tasks) >= t.opts.MaxTasks {
		r.dropped++
		return r, nil
	}
	ts := &taskState{}
	r.tasks[e.TaskID] = ts
	return r, ts
}

func (t *Telemetry) taskScheduled(e observe.Event) {
	_, ts := t.task(e, true)
	if ts != nil && ts.scheduled.IsZero() {
		ts.scheduled = e.Time
	}
}

func (t *Telemetry) taskStarted(e observe.Event) {
	r, ts := t.task(e, true)
	if r == nil || ts == nil {
		return
	}
	if ts.started.IsZero() {
		ts.started = e.Time
	}
	if !ts.scheduled.IsZero() {
		t.reg.observe(mTaskQueue, e.Time.Sub(ts.scheduled).Seconds(),
			L(lPipeline, r.pipeline), L(lStage, e.Stage))
	}
	if t.tr == nil || ts.span != nil {
		return
	}
	parent := ""
	if sp := r.stages[e.Stage]; sp != nil {
		parent = sp.SpanID
	} else if r.span != nil {
		parent = r.span.SpanID
	}
	ts.span = &openSpan{Span: Span{
		TraceID: TraceIDFor(e.RunID),
		// The instance joins a task span's identity so that the process that
		// dispatched a task and the worker that ran it are two spans under one
		// stage, rather than two processes writing one ID.
		SpanID:   spanIDFor(e.RunID, "task:"+t.opts.Instance+":"+e.TaskID),
		ParentID: parent, Name: "loom.task " + e.Stage, Kind: SpanInternal, Start: e.Time,
	}}
	ts.span.attr("loom.task_id", e.TaskID)
	ts.span.attr("loom.stage", e.Stage)
	ts.span.attr("loom.pipeline", r.pipeline)
	if e.Records > 0 {
		ts.span.attr("loom.records", int64(e.Records))
	}
}

func (t *Telemetry) taskSettled(e observe.Event, failed bool) {
	r, ts := t.task(e, false)
	pipeline := t.pipelineOf(e)
	outcome := "completed"
	if failed {
		outcome = "failed"
	}
	t.reg.add(mTasks, 1, L(lPipeline, pipeline), L(lStage, e.Stage), L(lOutcome, outcome))

	var dur time.Duration
	if ts != nil && !ts.started.IsZero() && !e.Time.IsZero() {
		dur = e.Time.Sub(ts.started)
		t.reg.observe(mTaskDuration, dur.Seconds(), L(lPipeline, pipeline), L(lStage, e.Stage))
	}
	if r == nil || ts == nil {
		return
	}
	delete(r.tasks, e.TaskID)

	sp := ts.span
	if sp == nil {
		return
	}
	sp.attr("loom.attempts", int64(max(e.Attempt, 1)))
	if e.Model != "" {
		sp.attr("loom.model", e.Model)
	}
	if e.Rung > 0 {
		sp.attr("loom.ladder_rung", int64(e.Rung))
	}
	if failed {
		sp.Status, sp.Message = StatusError, e.Err
	} else {
		sp.Status = StatusOK
	}
	if t.opts.Sample.keep(sp.SpanID, failed || ts.interesting, dur) {
		t.tr.emit(sp.finish(e.Time))
	}
}

func (t *Telemetry) taskRetried(e observe.Event) {
	pipeline := t.pipelineOf(e)
	class := retryClass(e.Note)
	t.reg.add(mRetries, 1, L(lPipeline, pipeline), L(lStage, e.Stage), L(lClass, class))
	escalated := strings.Contains(e.Note, "escalating")
	if escalated {
		t.reg.add(mEscalations, 1, L(lPipeline, pipeline), L(lStage, e.Stage))
	}
	_, ts := t.task(e, false)
	if ts == nil {
		return
	}
	ts.interesting = true
	if ts.span != nil {
		ts.span.event("retry", e.Time,
			Attr{Key: "loom.failure_class", Value: class},
			Attr{Key: "loom.attempt", Value: int64(e.Attempt)},
			Attr{Key: "loom.escalated", Value: escalated},
			Attr{Key: "exception.message", Value: e.Err})
	}
}

// retryClass reads the failure class out of the note the scheduler writes,
// which is either the class alone or the class followed by what it decided.
func retryClass(note string) string {
	if i := strings.IndexByte(note, ':'); i > 0 {
		return note[:i]
	}
	if note == "" {
		return "unknown"
	}
	return note
}

func (t *Telemetry) modelCalled(e observe.Event) {
	pipeline := t.pipelineOf(e)
	outcome := "ok"
	if e.Err != "" {
		outcome = "error"
	}
	pl, st, md := L(lPipeline, pipeline), L(lStage, e.Stage), L(lModel, e.Model)
	t.reg.add(mModelCalls, 1, pl, st, md, L(lOutcome, outcome))
	t.reg.observe(mCallDuration, e.Latency.Seconds(), pl, st, md)

	// Cost is counted here rather than on the task, because this is where the
	// provider counts it. A task that climbed two rungs of an escalation
	// ladder made three calls and was billed for three; its completion event
	// carries only the usage of the one that answered.
	t.reg.add(mCostUSD, e.Usage.CostUSD, pl, st, md)
	t.reg.add(mTokens, float64(e.Usage.InputTokens), pl, st, md, L(lKind, "input"))
	t.reg.add(mTokens, float64(e.Usage.OutputTokens), pl, st, md, L(lKind, "output"))
	t.reg.add(mTokens, float64(e.Usage.CacheReadTokens), pl, st, md, L(lKind, "cache_read"))
	t.reg.add(mTokens, float64(e.Usage.CacheWriteTokens), pl, st, md, L(lKind, "cache_write"))

	// The prefix cache's worth arrives as one signed number, and a signed
	// counter is a series every rate() misreads. It is split into the two
	// monotonic halves it is made of: what a read saved, and what a write
	// cost. The net is their difference, and a dashboard can take it.
	if e.Saved >= 0 {
		t.reg.add(mPrefixSaved, e.Saved, pl, st, md)
	} else {
		t.reg.add(mPrefixPremium, -e.Saved, pl, st, md)
	}

	r := t.runs[e.RunID]
	if r != nil {
		r.spent += e.Usage.CostUSD
		t.headroom(r.pipeline)
	}
	if t.tr == nil || r == nil {
		return
	}
	ts := r.tasks[e.TaskID]
	if ts == nil || ts.span == nil {
		return
	}
	ts.calls++
	end := e.Time
	start := end.Add(-e.Latency)
	call := openSpan{Span: Span{
		TraceID:  ts.span.TraceID,
		SpanID:   spanIDFor(e.RunID, fmt.Sprintf("call:%s:%s#%d", t.opts.Instance, e.TaskID, ts.calls)),
		ParentID: ts.span.SpanID, Name: "model.call " + e.Model, Kind: SpanClient,
		Start: start, End: end,
	}}
	call.attr("loom.model", e.Model)
	call.attr("loom.stage", e.Stage)
	call.attr("loom.cost_usd", round(e.Usage.CostUSD))
	call.attr("loom.tokens.input", int64(e.Usage.InputTokens))
	call.attr("loom.tokens.output", int64(e.Usage.OutputTokens))
	if e.Usage.CacheReadTokens > 0 {
		call.attr("loom.tokens.cache_read", int64(e.Usage.CacheReadTokens))
	}
	if e.Err != "" {
		call.Status, call.Message = StatusError, e.Err
		ts.interesting = true
	} else {
		call.Status = StatusOK
	}
	sort.SliceStable(call.Attrs, func(i, j int) bool { return call.Attrs[i].Key < call.Attrs[j].Key })
	ts.span.children = append(ts.span.children, call.Span)
}

func (t *Telemetry) cacheHit(e observe.Event) {
	kind := "stored"
	if e.Coalesced {
		kind = "coalesced"
	}
	t.reg.add(mCacheHits, 1, L(lPipeline, t.pipelineOf(e)), L(lStage, e.Stage), L(lKind, kind))
	_, ts := t.task(e, false)
	if ts != nil && ts.span != nil {
		ts.span.event("cache.hit", e.Time, Attr{Key: "loom.cache_kind", Value: kind})
	}
}

func (t *Telemetry) routed(e observe.Event) {
	pipeline := t.pipelineOf(e)
	kind := "routed"
	if e.Probe {
		kind = "probe"
	}
	t.reg.add(mRouted, 1, L(lPipeline, pipeline), L(lStage, e.Stage), L(lKind, kind))
	if !e.Probe {
		t.reg.add(mRoutedSkips, float64(len(e.Skipped)), L(lPipeline, pipeline), L(lStage, e.Stage))
	}
}

func (t *Telemetry) toolCalled(e observe.Event) {
	outcome := "ok"
	if e.Err != "" {
		outcome = "error"
	}
	t.reg.add(mToolCalls, 1, L(lServer, e.Server), L(lTool, e.Tool), L(lOutcome, outcome))
	t.reg.observe(mToolDuration, e.Latency.Seconds(), L(lServer, e.Server), L(lTool, e.Tool))
	t.reg.observe(mToolQueue, e.Queued.Seconds(), L(lServer, e.Server))
	t.reg.set(mToolInFlight, float64(e.InFlight), L(lServer, e.Server))
	if e.Slots > 0 {
		t.reg.set(mToolSlots, float64(e.Slots), L(lServer, e.Server))
	}
}

func (t *Telemetry) budgetExceeded(e observe.Event) {
	t.reg.add(mBudgetExceeded, 1, L(lPipeline, t.pipelineOf(e)))
	t.runEvent(e, "budget.exceeded", Attr{Key: "loom.note", Value: e.Note})
}

func (t *Telemetry) projected(e observe.Event) {
	name := e.Pipeline
	if name == "" {
		name = t.opts.Pipeline
	}
	t.reg.set(mProjectedCost, round(e.Usage.CostUSD), L(lPipeline, name))
	t.reg.set(mProjectedCeil, round(e.Ceiling.CostUSD), L(lPipeline, name))
	if e.Budget != (core.Budget{}) {
		t.seen[name] = e.Budget
		t.headroom(name)
	}
}

func (t *Telemetry) watermark(e observe.Event) {
	pipeline := t.pipelineOf(e)
	if !e.Watermark.IsZero() {
		// A timestamp rather than a lag, deliberately. The lag is
		// `time() - loom_watermark_seconds` in the query language, computed
		// against the scrape's own clock; a lag computed here would be as
		// stale as the last event, which for an idle stream is exactly when
		// the number matters most.
		t.reg.set(mWatermark, float64(e.Watermark.UnixNano())/1e9, L(lPipeline, pipeline))
	}
	if e.Lag > 0 {
		t.reg.observe(mSplitLag, e.Lag.Seconds(), L(lPipeline, pipeline))
	}
}

// runEvent annotates a run's span with something that happened to the run
// rather than to a task.
func (t *Telemetry) runEvent(e observe.Event, name string, attrs ...Attr) {
	if t.tr == nil {
		return
	}
	if r := t.runs[e.RunID]; r != nil && r.span != nil {
		r.span.event(name, e.Time, attrs...)
	}
}

// headroom republishes what is left of a pipeline's wallet: the smallest
// remaining balance across its runs in flight, or — when none are — the whole
// of the last ceiling it was given, because an idle pipeline has spent
// nothing. A pipeline with no ceiling reports no series at all; there is no
// honest number for the headroom of an unbounded run.
func (t *Telemetry) headroom(pipeline string) {
	budget, ok := t.seen[pipeline]
	if !ok || budget.MaxCostUSD <= 0 {
		return
	}
	remaining := budget.MaxCostUSD
	for _, r := range t.runs {
		if r.pipeline != pipeline || r.budget.MaxCostUSD <= 0 {
			continue
		}
		if left := r.budget.MaxCostUSD - r.spent; left < remaining {
			remaining = left
		}
	}
	t.reg.set(mBudgetHeadroom, round(remaining), L(lPipeline, pipeline))
}

func round(v float64) float64 { return float64(int64(v*1e8+0.5)) / 1e8 }

// --- Reading it back ----------------------------------------------------

// Ready registers a readiness check. /readyz runs all of them and answers 503
// with the names that failed, which is what a load balancer and an
// orchestrator both want: a process that is alive but cannot yet take work
// should not be sent any.
func (t *Telemetry) Ready(name string, fn func(context.Context) error) {
	t.mu.Lock()
	t.ready = append(t.ready, check{name: name, fn: fn})
	t.mu.Unlock()
}

// Metrics writes the Prometheus exposition for everything folded so far.
func (t *Telemetry) Metrics(w http.ResponseWriter, r *http.Request) {
	t.refresh()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_ = t.reg.writeTo(w)
}

// Scrape returns the same bytes Metrics writes. It exists so a process can
// log or test its own exposition without standing up a server.
func (t *Telemetry) Scrape() string {
	t.refresh()
	var b strings.Builder
	_ = t.reg.writeTo(&b)
	return b.String()
}

// refresh writes the exporter's own state into the registry, just before it
// is read. These are the numbers that say whether the rest can be believed.
func (t *Telemetry) refresh() {
	t.mu.Lock()
	tracked := len(t.runs)
	t.mu.Unlock()

	t.reg.set(mTracked, float64(tracked))
	t.reg.set(mOverflowed, float64(t.reg.overflowed()))
	t.reg.set(mUndeclared, float64(t.reg.unknown.Load()))
	t.reg.set(mSpansDrop, float64(t.spansDropped.Load()))
	t.reg.set(mExportFails, float64(t.exportFails.Load()))
}

// Handler serves the ops surface: /metrics, /healthz and /readyz.
func (t *Telemetry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", t.Metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", t.readyz)
	return mux
}

func (t *Telemetry) readyz(w http.ResponseWriter, r *http.Request) {
	t.mu.Lock()
	checks := append([]check(nil), t.ready...)
	t.mu.Unlock()

	var failed []string
	for _, c := range checks {
		if err := c.fn(r.Context()); err != nil {
			failed = append(failed, c.name+": "+err.Error())
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if len(failed) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(strings.Join(failed, "\n") + "\n"))
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}

// Serve runs the ops surface on addr until ctx is cancelled, then shuts the
// listener down and flushes whatever spans are still queued. It is the one
// line a long-lived process — a worker, a stream job, a serving desk — needs
// in order to be operable.
func (t *Telemetry) Serve(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: t.Handler(), ReadHeaderTimeout: 10 * time.Second}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
		return t.Close(shutdown)
	}
}

// Flush exports every span queued so far and returns once the exporter has
// taken them. Tests and one-shot programs want it; a long-lived process does
// not need to call it.
func (t *Telemetry) Flush(ctx context.Context) error {
	if t.tr == nil {
		return nil
	}
	return t.tr.waitFlush(ctx)
}

// Close flushes and shuts the span exporter down. Metrics remain readable
// afterwards: a scrape that arrives during shutdown should get the totals,
// not an error.
func (t *Telemetry) Close(ctx context.Context) error {
	if t.tr == nil {
		return nil
	}
	return t.tr.shutdown(ctx)
}

// overflowed counts the families that have reached their series ceiling.
func (r *registry) overflowed() int {
	n := 0
	for _, f := range r.fams {
		f.mu.Lock()
		full := len(f.series) > f.cap
		f.mu.Unlock()
		if full {
			n++
		}
	}
	return n
}
