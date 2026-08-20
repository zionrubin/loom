package loom

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/ops"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/stream"
)

// Defaults for a stream job. They are chosen for a workload whose unit of work
// is a model call: a checkpoint interval measured in seconds rather than
// milliseconds, and a poll that waits long enough to gather a batch without
// spinning.
const (
	DefaultCheckpointEvery = 30 * time.Second
	DefaultPollWait        = 250 * time.Millisecond
	DefaultPollRecords     = 256
	DefaultIdleTimeout     = 30 * time.Second
	// DefaultQuiesceTimeout bounds how long a checkpoint waits for the job to
	// come to rest. It is generous because the thing it is waiting for is a
	// model call, and a checkpoint that gave up after a second would give up
	// every time.
	DefaultQuiesceTimeout = 2 * time.Minute
)

// Stream runs a pipeline against an unbounded source until it is stopped.
//
// It is the same pipeline, the same planner, the same envelopes, the same
// scheduler, the same cache and the same budget as Run. What changes is that
// the input never ends, and therefore that the aggregates need something other
// than the end of the input to fold to. That something is a Window, and the
// three things this entry point adds over Run are the three things a window
// needs to exist: a source that reports event time and can be resumed, a
// checkpoint that ties window state to source positions, and a sink whose
// writes those positions are safe against.
//
//	p := pipeline.New("incident-desk")
//	events := p.FromStream("incidents")
//
//	events.
//	    Infer("grade", ...).                        // per record, as they arrive
//	    Window("per-minute", stream.WindowSpec{
//	        Assigner: stream.Tumbling(time.Minute),
//	        Lateness: 30 * time.Second,
//	    }).
//	    ReduceAI("digest", ...)                     // once per pane
//
//	res, err := loom.Stream(ctx, p,
//	    loom.WithSource("incidents", src),
//	    loom.WithSink("digest", sink),
//	    loom.WithStateDir("./state"),               // cache + checkpoints
//	    loom.WithJobID("incident-desk"),            // what a restart resumes
//	)
//
// Stopping it is ordinary context cancellation, and returns the report rather
// than an error: a stream job that was asked to stop has succeeded. Work done
// since the last checkpoint is replayed on the next start, and because the
// result cache is keyed on content rather than on time, replayed model calls
// are served rather than re-billed.
func Stream(ctx context.Context, p *pipeline.Pipeline, opts ...Option) (*StreamResult, error) {
	cfg := Config{Workers: 8, Retry: runtime.DefaultRetry}
	for _, o := range opts {
		o(&cfg)
	}
	h, err := newHost(cfg)
	if err != nil {
		return nil, err
	}
	defer h.close()
	return h.launchStream(ctx, p, h.cfg)
}

// StreamResult is the outcome of a stream job.
type StreamResult struct {
	JobID string
	// Report is the per-stage execution report: tasks, model calls, tokens,
	// cost, retries, latency. It covers the job's whole life, not one pane.
	Report observe.RunReport
	// Stream is what a batch run has no equivalent of: watermarks, panes,
	// lateness, checkpoints, and per-split lag.
	Stream StreamReport
	// StageOutputs holds each stage's most recent output — the last pane for a
	// windowed stage, the last batch for a per-record one. A stream job cannot
	// return every record it ever produced, and the thing worth having in a
	// result is the most recent one.
	StageOutputs map[string][]core.Record
	Failures     []runtime.Failure
	Spent        core.Usage
	// Iterations reports each iterative stage's rounds, as for a run.
	Iterations []IterationReport
}

// StreamReport is a stream job's own accounting.
type StreamReport struct {
	Started time.Time
	Stopped time.Time
	// StopReason says why the job ended: the context was cancelled, a limit was
	// reached, or every source ran out.
	StopReason string
	// Records is how many events were ingested, Panes how many window firings
	// were produced, and Batches how many were written to sinks.
	Records int64
	Panes   int64
	// Late counts records that arrived after their window was gone, and
	// Undecodable those a source could not parse at all. Both being zero is the
	// happy case; either one growing is a fact about the source rather than
	// about Loom.
	Late        int64
	Undecodable int64
	Batches     int64
	// Checkpoints is how many recoverable points were recorded, Epoch the last
	// one's number, and Skipped how many were abandoned because the job would
	// not come to rest.
	Checkpoints int64
	Epoch       int64
	Skipped     int64
	// ResumedFrom is the epoch this job restarted from, zero for a cold start.
	ResumedFrom int64
	// Watermark is how far event time had advanced when the job stopped.
	Watermark time.Time
	// Splits reports each source partition's progress and lag.
	Splits []stream.SplitLag
	// Windows reports each window stage's assignment and firing counts.
	Windows map[string]stream.WindowStats
}

// Duration is how long the job ran.
func (r StreamReport) Duration() time.Duration {
	if r.Started.IsZero() || r.Stopped.IsZero() {
		return 0
	}
	return r.Stopped.Sub(r.Started)
}

// String renders the report for a terminal.
func (r StreamReport) String() string {
	var b []byte
	add := func(format string, args ...any) { b = append(b, fmt.Sprintf(format, args...)...) }

	add("stream: %s after %s\n", r.StopReason, r.Duration().Round(time.Millisecond))
	add("  ingested   %d records", r.Records)
	if r.Late > 0 {
		add(", %d late", r.Late)
	}
	if r.Undecodable > 0 {
		add(", %d undecodable", r.Undecodable)
	}
	add("\n  panes      %d fired, %d written to sinks\n", r.Panes, r.Batches)
	add("  checkpoint %d taken", r.Checkpoints)
	if r.Skipped > 0 {
		add(", %d skipped", r.Skipped)
	}
	if r.ResumedFrom > 0 {
		add(" (resumed from epoch %d)", r.ResumedFrom)
	}
	add("\n")
	if !r.Watermark.IsZero() {
		add("  watermark  %s\n", r.Watermark.UTC().Format(time.RFC3339))
	}
	for _, s := range r.Splits {
		add("  split      %-28s %6d events", s.Split, s.Events)
		if s.Lag > 0 {
			add("  lag %s", s.Lag.Round(time.Second))
		}
		switch {
		case s.Retired:
			add("  (ended)")
		case s.Idle:
			add("  (idle)")
		}
		add("\n")
	}
	names := make([]string, 0, len(r.Windows))
	for name := range r.Windows {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		w := r.Windows[name]
		add("  window     %-28s %6d panes from %d records", name, w.Panes, w.Records)
		if w.Early > 0 {
			add(", %d early", w.Early)
		}
		if w.Evicted > 0 {
			add(", %d evicted", w.Evicted)
		}
		if w.LiveWindows > 0 {
			add(", %d still open", w.LiveWindows)
		}
		add("\n")
	}
	return string(b)
}

// --- Options -------------------------------------------------------------

// WithSource binds a stream source to a source stage declared with
// pipeline.FromStream.
//
// A job may have several: two topics feeding two branches of one graph is an
// ordinary shape, and each gets its own splits, its own positions in the
// checkpoint, and its own contribution to the watermark.
func WithSource(stage string, src stream.Source) Option {
	return func(c *Config) {
		if c.Sources == nil {
			c.Sources = map[string]stream.Source{}
		}
		c.Sources[stage] = src
	}
}

// WithSink sends a stage's output to a destination, one batch per pane.
//
// An empty stage name binds the sink to every terminal stage, which is the
// common case of a job with one answer and one place to put it.
//
// Delivery is at-least-once and every batch carries a stable identity (see
// stream.Batch.Key), so a sink that overwrites by key sees each pane exactly
// once however many times the job restarts. A sink that appends sees duplicates
// after a crash, and that is the honest cost of not implementing a transaction.
func WithSink(stage string, sink stream.Sink) Option {
	return func(c *Config) {
		if c.Sinks == nil {
			c.Sinks = map[string]stream.Sink{}
		}
		c.Sinks[stage] = sink
	}
}

// WithJobID names a stream job. It is the identity a restart resumes: the same
// ID with the same state directory picks up the last checkpoint, a different
// one starts cold.
//
// Unset, a job gets a random ID and therefore never resumes, which is right for
// an experiment and wrong for anything deployed.
func WithJobID(id string) Option { return func(c *Config) { c.JobID = id } }

// WithCheckpoints supplies the store a job records recoverable points in.
// Unset, a job with WithStateDir checkpoints to disk under it, and a job
// without one checkpoints to memory — which still exercises the commit
// ordering, and still loses everything when the process does.
func WithCheckpoints(s stream.Store) Option { return func(c *Config) { c.Checkpoints = s } }

// WithCheckpointEvery sets how often a job records a recoverable point
// (default 30s; a negative duration disables checkpointing entirely, which is a
// defensible choice only for a job that can replay its whole source).
//
// The interval is the amount of work a crash costs, and in stream mode that
// cost is wall-clock rather than money: replayed records hit the result cache.
// The reason not to make it tiny is the other side of the trade — a checkpoint
// briefly stops the job, so an interval near the task latency would spend more
// time holding still than running.
func WithCheckpointEvery(d time.Duration) Option {
	return func(c *Config) { c.CheckpointEvery = d }
}

// WithLateness sets the source's bounded out-of-orderness: how far behind its
// own largest event time a split may still deliver. It is the allowance the
// watermark is computed with, and it is a property of the source rather than of
// any window.
func WithLateness(d time.Duration) Option { return func(c *Config) { c.Lateness = d } }

// WithIdleTimeout sets how long a split may produce nothing before it stops
// holding the watermark back (default 30s; zero disables the release).
//
// Without it, one quiet partition holds every window in the job open, because
// the watermark is a minimum and a silent split's claim never improves.
func WithIdleTimeout(d time.Duration) Option { return func(c *Config) { c.IdleTimeout = d } }

// WithStreamLimit stops a job after a bound is reached. It is what makes a
// stream job testable and demonstrable: the same code that runs forever in
// production runs over exactly two hundred records in a test.
func WithStreamLimit(l stream.Limit) Option { return func(c *Config) { c.StreamLimit = l } }

// WithPolling tunes ingestion: how many events a reader asks for at a time and
// how long it waits for them (defaults 256 and 250ms).
func WithPolling(max int, wait time.Duration) Option {
	return func(c *Config) { c.PollRecords, c.PollWait = max, wait }
}

// WithDrainOnStop overrides what happens to windows that are still open when a
// job stops.
//
// The default depends on why it stopped, because the right answer does. A job
// whose sources ran out drains: there is no more evidence coming, so every open
// window is as complete as it will ever be. A job cancelled for a deploy does
// not: its windows are half full, firing them would publish a partial answer as
// if it were the real one, and the next start resumes them from the checkpoint.
func WithDrainOnStop(drain bool) Option {
	return func(c *Config) { c.DrainOnStop = &drain }
}

// --- The job -------------------------------------------------------------

// streamJob is one running stream. It borrows the driver's plan, scheduler and
// event bus, so a stage's tasks, envelopes, caching and recovery are identical
// to a run's, and adds what an unbounded input needs: ingestion, watermarks,
// windows, sinks and checkpoints.
type streamJob struct {
	*driver
	engine *runtime.Engine
	pipes  map[string]*runtime.Pipe
	store  stream.Store
	wm     *stream.Watermarks
	gate   *ingestGate
	limit  stream.Limit
	drain  bool

	// inflight counts tasks admitted and not yet forwarded downstream, and
	// reading counts source readers between the gate and their push. Together
	// with every pipe being idle they are the definition of "the job is holding
	// still", which is what a checkpoint needs.
	inflight atomic.Int64
	reading  atomic.Int64

	windows map[string]*windowStage
	sinks   map[string]*sinkWriter
	panes   sync.Map // pane ID → stream.Pane

	readersMu sync.Mutex
	readers   map[string]stream.Reader
	// restoredPositions is where each split was when the job last checkpointed,
	// and is what Source.Open is handed instead of the source's own default.
	restoredPositions map[string]stream.Position

	records     atomic.Int64
	panesFired  atomic.Int64
	late        atomic.Int64
	batches     atomic.Int64
	epoch       atomic.Int64
	checkpoints atomic.Int64
	skipped     atomic.Int64
	resumed     int64
	started     time.Time

	stopOnce sync.Once
	stopped  chan struct{}
	reason   atomic.Value
}

// windowStage is a window's engine plus the lock that lets a checkpoint read it
// while the stage is between batches.
type windowStage struct {
	mu sync.Mutex
	w  *stream.Windower
}

// launchStream provisions and runs a stream job on this host.
func (h *host) launchStream(ctx context.Context, p *pipeline.Pipeline, cfg Config) (*StreamResult, error) {
	jobID := cfg.JobID
	if jobID == "" {
		jobID = core.NewID("job")
	}

	snapshot := h.shared.Hashes()
	pl, err := plan.Compile(p, cfg.Registry,
		plan.WithBroadcasts(snapshot), plan.WithContinuations(cfg.Continuations),
		plan.WithMCP(h.manifest))
	if err != nil {
		return nil, err
	}
	if err := validateStream(pl, cfg); err != nil {
		return nil, err
	}
	runners, err := ops.BuildRunners(pl)
	if err != nil {
		return nil, err
	}

	exec := h.executorFor(runners)
	sched := runtime.Scheduler{
		Workers: cfg.Workers, Retry: cfg.Retry, Limiter: h.limiter,
		Governor: h.gov, Registry: cfg.Registry, Exec: exec, Bus: h.bus,
		ContinueOnError: cfg.ContinueOnError, Router: h.router,
	}

	store, err := checkpointStore(cfg)
	if err != nil {
		return nil, err
	}

	tr := h.open(jobID)
	h.bus.Publish(observe.Event{
		Type: observe.RunStarted, RunID: jobID, Pipeline: p.Name, Kind: "stream"})

	workers := cfg.Workers
	if workers <= 0 {
		workers = 8
	}
	j := &streamJob{
		driver: &driver{
			plan: pl, runID: jobID, cfg: cfg, sched: sched, bus: h.bus,
			outputs: map[string][]core.Record{},
		},
		engine:            runtime.NewEngine(&sched, workers),
		pipes:             map[string]*runtime.Pipe{},
		store:             store,
		gate:              newIngestGate(),
		limit:             cfg.StreamLimit,
		windows:           map[string]*windowStage{},
		sinks:             map[string]*sinkWriter{},
		readers:           map[string]stream.Reader{},
		restoredPositions: map[string]stream.Position{},
		stopped:           make(chan struct{}),
		started:           time.Now(),
	}
	j.wm = stream.NewWatermarks(cfg.Lateness, idleTimeout(cfg))
	for _, sp := range pl.Order {
		j.pipes[sp.Stage.ID] = runtime.NewPipe()
		if sp.Stage.Kind == pipeline.KindWindow {
			j.windows[sp.Stage.ID] = &windowStage{w: stream.NewWindower(*sp.Stage.Window)}
		}
	}
	j.bindSinks(pl, cfg)

	restored, err := j.restore(ctx, jobID)
	if err != nil {
		return nil, err
	}

	runErr := j.run(ctx)

	h.bus.Publish(observe.Event{Type: observe.RunFinished, RunID: jobID, Pipeline: p.Name})

	res := &StreamResult{
		JobID:        jobID,
		Report:       tr.collector.Report(),
		Stream:       j.report(restored),
		StageOutputs: j.outputs,
		Failures:     j.failures,
		Iterations:   j.iterations,
		Spent:        h.gov.Spent(),
	}
	return res, runErr
}

// validateStream refuses the pipelines stream mode cannot mean anything for,
// before a source is opened.
func validateStream(pl *plan.Plan, cfg Config) error {
	var streamStages []string
	for _, sp := range pl.Order {
		s := sp.Stage
		if pipeline.StreamSource(s) {
			streamStages = append(streamStages, s.ID)
			if _, ok := cfg.Sources[s.ID]; !ok {
				return fmt.Errorf("stream source %q has no source bound "+
					"(add loom.WithSource(%q, src))", s.ID, s.ID)
			}
		}
	}
	if len(streamStages) == 0 {
		return errors.New("pipeline has no stream source: declare one with " +
			"pipeline.FromStream, or run it to completion with loom.Run")
	}
	for name := range cfg.Sources {
		if sp, ok := pl.ByID[name]; !ok || !pipeline.StreamSource(sp.Stage) {
			return fmt.Errorf("loom.WithSource(%q): no such stream source stage", name)
		}
	}
	for name := range cfg.Sinks {
		if name == "" {
			continue
		}
		if _, ok := pl.ByID[name]; !ok {
			return fmt.Errorf("loom.WithSink(%q): no such stage", name)
		}
	}

	// An aggregate needs a set, and on an unbounded input only a window makes
	// one. Saying so here, with the fix in the message, is worth more than any
	// amount of documentation: the alternative is a stage that buffers the
	// stream forever and never fires.
	for _, sp := range pl.Order {
		s := sp.Stage
		if !plan.Aggregate(s.Kind) || pl.Windowed(s.ID) {
			continue
		}
		if !unboundedInput(pl, s) {
			continue
		}
		return fmt.Errorf("stage %q folds an unbounded input: put a Window "+
			"between it and the stream source, or its aggregate can never "+
			"complete", s.ID)
	}
	return nil
}

// unboundedInput reports whether a stage's input ultimately comes from a stream
// source.
func unboundedInput(pl *plan.Plan, s *pipeline.Stage) bool {
	for cur := s; cur != nil; cur = cur.Upstream {
		if pipeline.StreamSource(cur) {
			return true
		}
		if cur.Kind == pipeline.KindWindow {
			return false
		}
	}
	return false
}

func checkpointStore(cfg Config) (stream.Store, error) {
	if cfg.Checkpoints != nil {
		return cfg.Checkpoints, nil
	}
	if cfg.StateDir == "" {
		return stream.NewMemStore(), nil
	}
	return stream.NewFileStore(cfg.StateDir+"/stream", 3)
}

func idleTimeout(cfg Config) time.Duration {
	if cfg.IdleTimeout != 0 {
		return cfg.IdleTimeout
	}
	return DefaultIdleTimeout
}

// bindSinks resolves the configured sinks onto stages, expanding the empty name
// to every terminal.
func (j *streamJob) bindSinks(pl *plan.Plan, cfg Config) {
	for name, sink := range cfg.Sinks {
		if name != "" {
			j.sinks[name] = &sinkWriter{job: j, stage: name, sink: sink}
			continue
		}
		for _, term := range pl.Terminal() {
			if _, taken := j.sinks[term]; !taken {
				j.sinks[term] = &sinkWriter{job: j, stage: term, sink: sink}
			}
		}
	}
}

// restore loads the last checkpoint and puts the window stages back where they
// were. It returns the checkpoint so ingestion can open each split at the
// position that state agrees with.
func (j *streamJob) restore(ctx context.Context, jobID string) (stream.Checkpoint, error) {
	ck, ok, err := j.store.Load(ctx, jobID)
	if err != nil {
		return stream.Checkpoint{}, err
	}
	if !ok {
		return stream.Checkpoint{}, nil
	}
	for id, blob := range ck.Windows {
		ws := j.windows[id]
		if ws == nil {
			// A window stage that no longer exists is a changed pipeline. The
			// records it was holding are gone, which is a fact worth reporting
			// rather than a reason to refuse to start.
			j.bus.Publish(observe.Event{Type: observe.CheckpointSkipped, RunID: j.runID,
				Stage: id, Note: "window state in checkpoint has no matching stage"})
			continue
		}
		if err := ws.w.Restore(blob); err != nil {
			return stream.Checkpoint{}, err
		}
	}
	j.epoch.Store(ck.Epoch)
	j.records.Store(ck.Progress.Records)
	j.panesFired.Store(ck.Progress.Panes)
	j.late.Store(ck.Progress.Late)
	j.batches.Store(ck.Progress.Batches)
	j.resumed = ck.Epoch
	for key, pos := range ck.Positions {
		j.restoredPositions[key] = pos
	}
	j.wm.Force(ck.Watermark)
	return ck, nil
}

// run starts every stage, the ingestors, and the checkpointer, and returns when
// the job has stopped and come to rest.
func (j *streamJob) run(parent context.Context) error {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)

	// A cancelled context is how a stream job is asked to stop, so it is
	// translated into a stop rather than treated as a failure.
	go func() {
		select {
		case <-ctx.Done():
			// A stage that failed cancelled this context with a cause, and that
			// cause is a better answer to "why did the job stop?" than the
			// cancellation it produced.
			cause := context.Cause(ctx)
			switch {
			case cause == nil, errors.Is(cause, context.Canceled),
				errors.Is(cause, context.DeadlineExceeded):
				j.stop("context cancelled")
			default:
				j.stop("failed: " + cause.Error())
			}
		case <-j.stopped:
		}
	}()
	if d := j.limit.Duration; d > 0 {
		timer := time.AfterFunc(d, func() { j.stop("duration limit reached") })
		defer timer.Stop()
	}

	var wg sync.WaitGroup
	for _, sp := range j.plan.Order {
		wg.Add(1)
		go func(sp *plan.StagePlan) {
			defer wg.Done()
			j.stage(ctx, cancel, sp)
		}(sp)
	}

	ckDone := make(chan struct{})
	go func() {
		defer close(ckDone)
		j.checkpointer(ctx)
	}()

	wg.Wait()
	j.engine.Wait()
	// Every stage has ended. If nothing has claimed a reason by now the graph
	// simply ran out — a pipeline of bounded sources alongside the stream one,
	// say — and the report should say so rather than "running".
	j.stop("stages ended")
	<-ckDone

	// The last checkpoint is taken after every stage has stopped, so it records
	// a state nothing is still changing — including the windows that were
	// deliberately left open by a job that was cancelled rather than exhausted.
	if err := j.finalCheckpoint(parent); err != nil {
		j.bus.Publish(observe.Event{Type: observe.CheckpointSkipped, RunID: j.runID,
			Note: err.Error()})
	}
	j.closeReaders()
	j.closeSinks()

	cause := context.Cause(ctx)
	if cause != nil && !errors.Is(cause, context.Canceled) &&
		!errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	return nil
}

// stop asks the job to wind down, recording the first reason given.
func (j *streamJob) stop(reason string) {
	j.stopOnce.Do(func() {
		j.reason.Store(reason)
		close(j.stopped)
	})
}

func (j *streamJob) stopReason() string {
	if v, ok := j.reason.Load().(string); ok {
		return v
	}
	return "running"
}

// stage drives one stage for the life of the job.
func (j *streamJob) stage(ctx context.Context, cancel context.CancelCauseFunc, sp *plan.StagePlan) {
	s := sp.Stage
	children := j.plan.Children[s.ID]

	emit := func(els []runtime.Element) {
		if w := j.sinks[s.ID]; w != nil {
			w.observe(els)
		}
		for _, id := range children {
			j.pipes[id].Push(els...)
		}
	}

	defer func() {
		for _, id := range children {
			j.pipes[id].Close()
		}
		j.stageFinished(s)
	}()
	j.stageStarted(s)

	switch {
	case s.Kind == pipeline.KindSource:
		j.ingest(ctx, cancel, sp, emit)
	case s.Kind == pipeline.KindWindow:
		j.windowLoop(ctx, cancel, sp, emit)
	case plan.Aggregate(s.Kind):
		j.aggregateLoop(ctx, cancel, sp, emit)
	default:
		j.pumpLoop(ctx, cancel, sp, emit)
	}
}

// records converts records into pipe elements stamped with an event time.
func elements(recs []core.Record, at time.Time) []runtime.Element {
	els := make([]runtime.Element, len(recs))
	for i, r := range recs {
		els[i] = runtime.Element{Record: r, Time: at}
	}
	return els
}

func mark(kind runtime.MarkKind, at time.Time, pane string) runtime.Element {
	return runtime.Element{Mark: runtime.Mark{Kind: kind, Time: at, Pane: pane}}
}
