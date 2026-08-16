package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/stream"
	"github.com/zionrubin/loom/task"
)

// --- Ingestion -----------------------------------------------------------

// ingest drives one stream source stage: discover its splits, read them in
// parallel, and turn what comes back into records and watermarks on the pipe.
//
// It returns when the job is stopping or when every split has ended, and its
// return is what closes the graph: the pipes it feeds close, their consumers
// drain and close theirs, and the job winds down front to back.
func (j *streamJob) ingest(ctx context.Context, cancel context.CancelCauseFunc,
	sp *plan.StagePlan, emit func([]runtime.Element)) {

	s := sp.Stage
	src := j.cfg.Sources[s.ID]
	if src == nil {
		cancel(fmt.Errorf("stream source %q has no source bound", s.ID))
		return
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		open    = map[string]bool{}
		retired int
		total   int
	)

	start := func(split stream.Split) {
		key := s.ID + "/" + split.ID
		mu.Lock()
		if open[key] {
			mu.Unlock()
			return
		}
		open[key] = true
		total++
		mu.Unlock()

		from := j.positionOf(key)
		reader, err := src.Open(ctx, split, from)
		if err != nil {
			cancel(fmt.Errorf("stream source %q: opening split %q: %w", s.ID, split.ID, err))
			return
		}
		j.readersMu.Lock()
		j.readers[key] = reader
		j.readersMu.Unlock()
		j.wm.Track(key)
		j.bus.Publish(observe.Event{Type: observe.SplitOpened, RunID: j.runID,
			Stage: s.ID, Split: split.ID, Note: positionNote(from)})

		wg.Add(1)
		go func() {
			defer wg.Done()
			j.readSplit(ctx, s.ID, key, reader, emit)
			mu.Lock()
			retired++
			done := retired == total
			mu.Unlock()
			j.wm.Retire(key)
			j.bus.Publish(observe.Event{Type: observe.SplitRetired, RunID: j.runID,
				Stage: s.ID, Split: split.ID})
			if done {
				// Every split of this source has ended. For a file source
				// read without Follow that is the whole point — the job is a
				// backfill and has finished its input.
				j.stop("sources exhausted")
			}
		}()
	}

	splits, err := src.Splits(ctx)
	if err != nil {
		cancel(fmt.Errorf("stream source %q: %w", s.ID, err))
		return
	}
	if len(splits) == 0 {
		j.stop("source has no splits")
	}
	for _, split := range splits {
		start(split)
	}

	// The watermark is published by one goroutine rather than by each reader,
	// so the marks on the pipe are monotonic by construction. A reader pushes
	// its records *before* it reports their event times, so a watermark can
	// never claim to be past a record that has not yet been forwarded.
	wmDone := make(chan struct{})
	go func() {
		defer close(wmDone)
		j.publishWatermarks(ctx, emit)
	}()

	// Rescan for splits that appeared after the job started: a new partition, a
	// new file in the directory being followed. A split already open is
	// recognized and skipped, so rescanning is idempotent.
	j.rescan(ctx, s.ID, src, start)

	wg.Wait()
	<-wmDone
	_ = src.Close()
}

// rescanInterval is how often a source is asked whether it has grown new
// splits. It is slow on purpose: discovering a partition a few seconds late
// costs a few seconds of lag, and asking constantly costs a metadata request
// per interval for the life of the job.
const rescanInterval = 15 * time.Second

// rescan periodically re-enumerates a source's splits, opening any that have
// appeared, until the job ends. It returns when the job is stopping, which is
// what lets the ingest stage close its downstream pipes.
func (j *streamJob) rescan(ctx context.Context, stageID string,
	src stream.Source, start func(stream.Split)) {

	tick := time.NewTicker(rescanInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-j.stopped:
			return
		case <-tick.C:
		}
		found, err := src.Splits(ctx)
		if err != nil {
			j.bus.Publish(observe.Event{Type: observe.SplitOpened, RunID: j.runID,
				Stage: stageID, Err: err.Error()})
			continue
		}
		for _, split := range found {
			start(split)
		}
	}
}

// readSplit is one split's read loop.
func (j *streamJob) readSplit(ctx context.Context, stageID, key string,
	reader stream.Reader, emit func([]runtime.Element)) {

	maxRecords, wait := j.cfg.PollRecords, j.cfg.PollWait
	if maxRecords <= 0 {
		maxRecords = DefaultPollRecords
	}
	if wait <= 0 {
		wait = DefaultPollWait
	}

	for {
		// The gate is checked before the read rather than after it, so a paused
		// job parks its readers within one poll and a checkpoint has something
		// to wait for that actually converges.
		if !j.gate.wait(ctx, j.stopped) {
			return
		}
		j.reading.Add(1)
		events, err := reader.Read(ctx, maxRecords, wait)
		j.forward(key, events, emit)
		j.reading.Add(-1)

		switch {
		case errors.Is(err, stream.ErrSplitDone):
			return
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			// A source that fails mid-stream retires its split rather than
			// killing the job: the other partitions are still good, and the
			// checkpoint holds this one's position for the next start.
			j.bus.Publish(observe.Event{Type: observe.SplitRetired, RunID: j.runID,
				Stage: stageID, Split: key, Err: err.Error()})
			return
		}
		if j.limitReached() {
			return
		}
	}
}

// forward pushes a read batch downstream and only then reports its event times,
// which is the ordering that makes the watermark safe: the watermark is derived
// from what has been observed, so observing after forwarding means it can never
// claim to be past a record still on its way.
func (j *streamJob) forward(key string, events []stream.Event, emit func([]runtime.Element)) {
	if len(events) == 0 {
		return
	}
	els := make([]runtime.Element, 0, len(events))
	now := time.Now()
	for i := range events {
		at := events[i].Time
		if at.IsZero() {
			// A source with no notion of event time gets ingestion time, which
			// makes windows a function of when the job ran. It is the right
			// fallback and the wrong thing to rely on.
			at = now
			events[i].Time = at
		}
		els = append(els, runtime.Element{Record: events[i].Record, Time: at})
	}
	emit(els)
	for _, ev := range events {
		j.wm.Observe(key, ev.Time, ev.Pos)
	}
	j.records.Add(int64(len(events)))
}

// publishWatermarks emits a watermark mark whenever event time advances.
func (j *streamJob) publishWatermarks(ctx context.Context, emit func([]runtime.Element)) {
	wait := j.cfg.PollWait
	if wait <= 0 {
		wait = DefaultPollWait
	}
	// Half a poll, so a watermark follows the records that justify it closely,
	// with a floor so an aggressive poll setting cannot ask for a zero ticker.
	every := max(wait/2, time.Millisecond)
	tick := time.NewTicker(every)
	defer tick.Stop()

	var last time.Time
	push := func() {
		if !j.gate.open() {
			return
		}
		wm := j.wm.Now()
		if !wm.After(last) {
			return
		}
		last = wm
		emit([]runtime.Element{mark(runtime.MarkWatermark, wm, "")})
		j.bus.Publish(observe.Event{Type: observe.WatermarkAdvanced, RunID: j.runID,
			Watermark: wm})
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-j.stopped:
			push()
			return
		case <-tick.C:
			push()
		}
	}
}

// positionOf returns the checkpointed position for a split, if the job restored
// one.
func (j *streamJob) positionOf(key string) stream.Position {
	j.readersMu.Lock()
	defer j.readersMu.Unlock()
	return j.restoredPositions[key]
}

func positionNote(p stream.Position) string {
	if p.Zero() {
		return "from the source's default start"
	}
	return fmt.Sprintf("resumed at offset %d", p.Offset)
}

// --- Per-record stages ---------------------------------------------------

// pumpLoop is the continuous-batching loop for a stage that runs per record:
// take what has arrived, submit it, and move straight on without waiting for
// the previous group. It is the bounded driver's pump with the two things an
// unbounded input adds.
//
// The first is watermark holdback. A watermark says nothing older than this
// will arrive, and a stage with tasks in flight cannot honestly repeat that
// claim — its own outputs are still to come. So the watermark it forwards is
// the smallest of the one it received and the event times it is still holding,
// which lets it keep pipelining instead of stopping at every mark.
//
// The second is pane alignment. A pane boundary *is* an ordering claim, so it
// cannot be forwarded until the records before it have been: the stage waits
// for its in-flight tasks and then passes the mark on. Panes are rare — one per
// window firing — so the wait costs a stage's depth, once per pane, and buys
// aggregates downstream that can trust their boundaries.
func (j *streamJob) pumpLoop(ctx context.Context, cancel context.CancelCauseFunc,
	sp *plan.StagePlan, emit func([]runtime.Element)) {

	s := sp.Stage
	in := j.pipes[s.ID]
	batch := s.Opts.BatchSize
	if batch <= 0 {
		batch = 1
	}
	wait := j.cfg.BatchWait
	if wait <= 0 {
		wait = DefaultBatchWait
	}

	var stageSlots chan struct{}
	if n := s.Opts.Parallelism; n > 0 {
		stageSlots = make(chan struct{}, n)
	}
	acquire := func() bool {
		if stageSlots == nil {
			return true
		}
		select {
		case stageSlots <- struct{}{}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	release := func() {
		if stageSlots != nil {
			<-stageSlots
		}
	}

	var (
		mu        sync.Mutex
		inflight  = map[int]time.Time{}
		pendingWM time.Time
		lastWM    time.Time
		gen       sync.WaitGroup
		seq       int
	)

	// forwardWatermark emits the largest watermark this stage can honestly
	// assert: the one it was given, held back by whatever it is still working
	// on.
	forwardWatermark := func() {
		mu.Lock()
		cand := pendingWM
		for _, t := range inflight {
			if t.Before(cand) {
				cand = t
			}
		}
		if !cand.After(lastWM) {
			mu.Unlock()
			return
		}
		lastWM = cand
		mu.Unlock()
		emit([]runtime.Element{mark(runtime.MarkWatermark, cand, "")})
	}

	submit := func(pending []runtime.Element) bool {
		if len(pending) == 0 {
			return true
		}
		recs := make([]core.Record, len(pending))
		for i, el := range pending {
			recs[i] = el.Record
		}
		tasks, err := sp.BuildTasksBatch(j.runID, recs, batch, j.cfg.EgressAllow)
		if err != nil {
			cancel(err)
			return false
		}
		for i, t := range tasks {
			t.Seq = seq
			seq++
			// Every output of a task inherits the earliest event time among its
			// inputs. It is the conservative choice — a batch's records may span
			// a window boundary, and attributing the output to the earliest of
			// them can only hold a watermark back, never let one run ahead.
			at := earliest(pending, i*batch, batch)
			if !acquire() {
				return false
			}
			mu.Lock()
			inflight[t.Seq] = at
			mu.Unlock()
			gen.Add(1)
			j.inflight.Add(1)

			j.engine.Submit(ctx, t, func(res task.Result, err error) {
				defer gen.Done()
				defer release()
				if err != nil {
					mu.Lock()
					delete(inflight, t.Seq)
					mu.Unlock()
					j.inflight.Add(-1)
					class := core.ClassOf(err)
					j.fail(runtime.Failure{Task: t, Err: err, Class: class})
					if class == core.FailBudget {
						cancel(runtime.ErrBudgetExhausted)
					} else if !j.cfg.ContinueOnError {
						cancel(err)
					}
					forwardWatermark()
					return
				}
				emit(elements(res.Output, at))
				j.record(s.ID, res.Output)
				mu.Lock()
				delete(inflight, t.Seq)
				mu.Unlock()
				// Decremented after the emit, so a job that observes no
				// in-flight tasks knows the records are already downstream.
				j.inflight.Add(-1)
				forwardWatermark()
			})
		}
		return true
	}

	for {
		if ctx.Err() != nil {
			break
		}
		els, ok := in.NextElements(ctx, batch, wait)
		if !ok {
			break
		}
		var pending []runtime.Element
		alive := true
		for _, el := range els {
			if !el.IsMark() {
				pending = append(pending, el)
				continue
			}
			if !submit(pending) {
				alive = false
				break
			}
			pending = nil
			switch el.Mark.Kind {
			case runtime.MarkWatermark:
				mu.Lock()
				if el.Mark.Time.After(pendingWM) {
					pendingWM = el.Mark.Time
				}
				mu.Unlock()
				forwardWatermark()
			default:
				// A pane boundary is an ordering claim: everything before it
				// must be downstream before it is.
				gen.Wait()
				emit([]runtime.Element{el})
			}
		}
		if alive {
			alive = submit(pending)
		}
		in.Done()
		if !alive {
			break
		}
	}
	gen.Wait()
}

// earliest returns the earliest event time in els[from:from+n], which is the
// slice BuildTasksBatch grouped into one task. Event times ride on the
// elements rather than on the records, so this is where a task's inputs and
// their times are put back together.
func earliest(els []runtime.Element, from, n int) time.Time {
	var at time.Time
	for i := from; i < from+n && i < len(els); i++ {
		if t := els[i].Time; !t.IsZero() && (at.IsZero() || t.Before(at)) {
			at = t
		}
	}
	return at
}

// --- Window stages -------------------------------------------------------

// windowLoop drives a window stage: feed records and watermarks into the
// windower, and emit whatever panes come back.
//
// This is where a stream becomes a sequence of finite sets, and it is the only
// stage in Loom that holds records between batches on purpose. That state is
// the thing a checkpoint exists to preserve.
func (j *streamJob) windowLoop(ctx context.Context, cancel context.CancelCauseFunc,
	sp *plan.StagePlan, emit func([]runtime.Element)) {

	s := sp.Stage
	in := j.pipes[s.ID]
	ws := j.windows[s.ID]

	for {
		if ctx.Err() != nil {
			break
		}
		els, ok := in.NextElements(ctx, windowBatch, j.cfg.BatchWait)
		if !ok {
			break
		}
		var fired []stream.Fired
		var failure error
		ws.mu.Lock()
		before := ws.w.Stats().Dropped
		for _, el := range els {
			if el.IsMark() {
				if el.Mark.Kind == runtime.MarkWatermark {
					fired = append(fired, ws.w.Advance(el.Mark.Time)...)
				}
				continue
			}
			f, err := ws.w.Add(el.Record, el.Time)
			if err != nil {
				failure = err
				break
			}
			fired = append(fired, f...)
		}
		after := ws.w.Stats().Dropped
		ws.mu.Unlock()

		j.emitPanes(emit, s.ID, fired)
		j.noteLate(s.ID, after-before)
		in.Done()
		if failure != nil {
			cancel(failure)
			break
		}
		j.limitReached()
	}

	// The pipe closed, which means ingestion has stopped. Whether the windows
	// still open should fire depends on why: a source that ran out has no more
	// evidence coming, so they are as complete as they will ever be; a job
	// cancelled for a deploy has half-full windows that belong in the
	// checkpoint rather than in a published answer.
	if j.drainWindows() {
		ws.mu.Lock()
		fired := ws.w.Drain()
		ws.mu.Unlock()
		j.emitPanes(emit, s.ID, fired)
	}
}

// windowBatch is how many elements a window stage takes at a time. It is larger
// than a task batch because assigning a record to a window is cheap and the
// only cost of taking more is latency to the next watermark.
const windowBatch = 512

// drainWindows decides whether open windows fire when the job stops.
func (j *streamJob) drainWindows() bool {
	if j.cfg.DrainOnStop != nil {
		return *j.cfg.DrainOnStop
	}
	return j.stopReason() == "sources exhausted"
}

// emitPanes puts a window firing on the wire: an opening mark, the records, a
// closing mark, and the watermark the pane was fired at.
//
// The brackets are what make an aggregate downstream possible. Between them is
// a complete set, which is exactly the thing ReduceAI, Combine and Iterate need
// and the thing an unbounded input otherwise never provides.
func (j *streamJob) emitPanes(emit func([]runtime.Element), stageID string, fired []stream.Fired) {
	for _, f := range fired {
		id := stageID + "#" + f.Pane.ID()
		j.panes.Store(id, f.Pane)

		els := make([]runtime.Element, 0, len(f.Records)+3)
		els = append(els, mark(runtime.MarkPaneOpen, f.Pane.Watermark, id))
		els = append(els, elements(f.Records, f.Pane.Watermark)...)
		els = append(els, mark(runtime.MarkPaneClose, f.Pane.Watermark, id))
		els = append(els, mark(runtime.MarkWatermark, f.Pane.Watermark, ""))
		emit(els)

		j.panesFired.Add(1)
		note := "final"
		switch {
		case f.Pane.Late:
			note = "late"
		case !f.Pane.Final:
			note = "early"
		}
		j.bus.Publish(observe.Event{Type: observe.PaneFired, RunID: j.runID,
			Stage: stageID, Pane: f.Pane.ID(), Records: len(f.Records),
			Watermark: f.Pane.Watermark, Detail: f.Pane.Window.String(), Note: note})
	}
}

// noteLate reports records dropped for windows already gone.
func (j *streamJob) noteLate(stageID string, n int64) {
	if n <= 0 {
		return
	}
	j.late.Add(n)
	j.bus.Publish(observe.Event{Type: observe.RecordsLate, RunID: j.runID,
		Stage: stageID, Records: int(n)})
}

// --- Aggregate stages ----------------------------------------------------

// aggregateLoop drives a stage that folds its input — ReduceAI, Combine,
// Iterate — once per pane.
//
// The stage is unchanged from the bounded driver: it is the same tree reduce,
// the same pairwise fold, the same supersteps, over the same task envelopes.
// The only difference is what "the end of the input" means, and a pane mark is
// what supplies it.
func (j *streamJob) aggregateLoop(ctx context.Context, cancel context.CancelCauseFunc,
	sp *plan.StagePlan, emit func([]runtime.Element)) {

	s := sp.Stage
	in := j.pipes[s.ID]

	var (
		buf  []core.Record
		pane string
		open bool
	)
	fold := func() bool {
		out, err := j.aggregate(ctx, j.engine, sp, buf)
		j.record(s.ID, out)
		buf = nil
		if err != nil {
			cancel(err)
			return false
		}
		p, _ := j.pane(pane)
		els := make([]runtime.Element, 0, len(out)+2)
		els = append(els, mark(runtime.MarkPaneOpen, p.Watermark, pane))
		els = append(els, elements(out, p.Watermark)...)
		els = append(els, mark(runtime.MarkPaneClose, p.Watermark, pane))
		emit(els)
		return true
	}

	for {
		if ctx.Err() != nil {
			break
		}
		els, ok := in.NextElements(ctx, windowBatch, 0)
		if !ok {
			break
		}
		alive := true
		for _, el := range els {
			if !el.IsMark() {
				if open {
					buf = append(buf, el.Record)
				}
				continue
			}
			switch el.Mark.Kind {
			case runtime.MarkPaneOpen:
				pane, open, buf = el.Mark.Pane, true, nil
			case runtime.MarkPaneClose:
				if open {
					alive = fold()
					open = false
				}
			case runtime.MarkWatermark:
				if !open {
					emit([]runtime.Element{el})
				}
			}
			if !alive {
				break
			}
		}
		in.Done()
		if !alive {
			break
		}
	}
}

// pane looks up a pane's metadata by the identity carried on the wire.
func (j *streamJob) pane(id string) (stream.Pane, bool) {
	v, ok := j.panes.Load(id)
	if !ok {
		return stream.Pane{}, false
	}
	p, ok := v.(stream.Pane)
	return p, ok
}

// --- Sinks ---------------------------------------------------------------

// sinkWriter turns a stage's element stream back into pane-shaped batches.
//
// A sink is not a stage: it observes what a stage emits rather than
// transforming it, so attaching one changes nothing about how the pipeline
// runs. What it needs from the stream is the pane brackets, which is why it
// watches elements rather than records.
type sinkWriter struct {
	job   *streamJob
	stage string
	sink  stream.Sink

	mu   sync.Mutex
	pane string
	buf  []core.Record
	// unpaned accumulates output from a stage with no window upstream, which
	// has no pane boundaries to batch on. It is flushed at each checkpoint, so
	// such a sink's unit of delivery is the epoch rather than the window.
	unpaned []core.Record
}

func (w *sinkWriter) observe(els []runtime.Element) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, el := range els {
		if !el.IsMark() {
			if w.pane != "" {
				w.buf = append(w.buf, el.Record)
			} else {
				w.unpaned = append(w.unpaned, el.Record)
			}
			continue
		}
		switch el.Mark.Kind {
		case runtime.MarkPaneOpen:
			w.pane, w.buf = el.Mark.Pane, nil
		case runtime.MarkPaneClose:
			recs, pane := w.buf, w.pane
			w.buf, w.pane = nil, ""
			w.write(pane, recs)
		}
	}
}

// write emits one batch. Callers hold w.mu; the sink call happens under it so
// batches from one stage reach the sink in pane order.
func (w *sinkWriter) write(paneID string, recs []core.Record) {
	if len(recs) == 0 {
		return
	}
	p, _ := w.job.pane(paneID)
	batch := stream.Batch{
		Stage: w.stage, Pane: p, Epoch: w.job.epoch.Load(), Records: recs,
	}
	if err := w.sink.Write(context.Background(), batch); err != nil {
		w.job.sinkFailed(w.stage, err)
		return
	}
	w.job.batches.Add(1)
	w.job.bus.Publish(observe.Event{Type: observe.SinkWrote, RunID: w.job.runID,
		Stage: w.stage, Pane: p.ID(), Records: len(recs)})
}

// flush writes whatever a sink on an unwindowed stage has accumulated, as one
// batch identified by the epoch that is about to be recorded.
func (w *sinkWriter) flush(epoch int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.unpaned) == 0 {
		return
	}
	recs := w.unpaned
	w.unpaned = nil
	batch := stream.Batch{
		Stage: w.stage, Epoch: epoch, Records: recs,
		Pane: stream.Pane{Seq: int(epoch), Final: true, Count: len(recs)},
	}
	if err := w.sink.Write(context.Background(), batch); err != nil {
		w.job.sinkFailed(w.stage, err)
		return
	}
	w.job.batches.Add(1)
	w.job.bus.Publish(observe.Event{Type: observe.SinkWrote, RunID: w.job.runID,
		Stage: w.stage, Records: len(recs), Epoch: epoch})
}

// sinkFailed records a durability failure. It stops the job rather than
// continuing, because a stream whose output is not landing is not a stream that
// should keep advancing its source positions.
func (j *streamJob) sinkFailed(stage string, err error) {
	j.fail(runtime.Failure{
		Task:  task.Task{Stage: stage},
		Err:   fmt.Errorf("sink %q: %w", stage, err),
		Class: core.ClassOf(err),
	})
	j.stop("sink write failed: " + err.Error())
}

// --- Limits and gating ---------------------------------------------------

// limitReached reports whether a configured bound has been hit, and stops the
// job when one has.
func (j *streamJob) limitReached() bool {
	if j.limit.Records > 0 && j.records.Load() >= j.limit.Records {
		j.stop("record limit reached")
		return true
	}
	if j.limit.Panes > 0 && j.panesFired.Load() >= j.limit.Panes {
		j.stop("pane limit reached")
		return true
	}
	select {
	case <-j.stopped:
		return true
	default:
		return false
	}
}

// ingestGate parks source readers while a checkpoint is taken.
//
// It is the whole of Loom's checkpointing mechanism: stop pulling, let the
// graph finish what it is holding, write down where everything is. See
// streamJob.checkpoint for why that is preferred to an aligned barrier.
type ingestGate struct {
	mu     sync.Mutex
	cond   *sync.Cond
	paused bool
}

func newIngestGate() *ingestGate {
	g := &ingestGate{}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *ingestGate) pause() {
	g.mu.Lock()
	g.paused = true
	g.mu.Unlock()
}

func (g *ingestGate) resume() {
	g.mu.Lock()
	g.paused = false
	g.mu.Unlock()
	g.cond.Broadcast()
}

func (g *ingestGate) open() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return !g.paused
}

// wait blocks while the gate is closed, reporting false when the job is ending.
func (g *ingestGate) wait(ctx context.Context, stopped <-chan struct{}) bool {
	select {
	case <-ctx.Done():
		return false
	case <-stopped:
		return false
	default:
	}
	stop := context.AfterFunc(ctx, func() { g.cond.Broadcast() })
	defer stop()

	g.mu.Lock()
	for g.paused && ctx.Err() == nil {
		g.cond.Wait()
	}
	g.mu.Unlock()

	select {
	case <-ctx.Done():
		return false
	case <-stopped:
		return false
	default:
		return true
	}
}

// --- Checkpointing -------------------------------------------------------

// checkpointer takes a checkpoint on an interval for the life of the job.
func (j *streamJob) checkpointer(ctx context.Context) {
	every := j.cfg.CheckpointEvery
	if every == 0 {
		every = DefaultCheckpointEvery
	}
	if every < 0 {
		return // explicitly disabled
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-j.stopped:
			return
		case <-tick.C:
			if err := j.checkpoint(ctx); err != nil {
				j.bus.Publish(observe.Event{Type: observe.CheckpointSkipped,
					RunID: j.runID, Note: err.Error()})
			}
		}
	}
}

// checkpoint records a point the job can be restarted from.
//
// Loom checkpoints by quiescing: ingestion stops, the graph finishes what it
// is holding, and the snapshot is taken while nothing is moving. The classic
// alternative — barriers flowing through the graph, each operator snapshotting
// as they pass — exists because stopping a stream engine costs throughput
// measured against microsecond operators.
//
// Loom's operators are model calls. A pause of one task latency, once every
// thirty seconds, costs a fraction of a percent of a job's capacity, and buys a
// snapshot with no in-flight state to serialize, no alignment to get wrong, and
// no partial pane to reason about. It is the trade the workload makes obvious,
// and it is the reason this is fifty lines rather than a subsystem.
func (j *streamJob) checkpoint(ctx context.Context) error {
	start := time.Now()
	j.gate.pause()
	defer j.gate.resume()

	if !j.quiesce(ctx) {
		j.skipped.Add(1)
		j.bus.Publish(observe.Event{Type: observe.CheckpointSkipped, RunID: j.runID,
			Note: j.motion(), Latency: time.Since(start)})
		return nil
	}
	return j.snapshot(ctx, start)
}

// finalCheckpoint records the job's last state, after every stage has stopped.
// There is nothing to quiesce by then, which is why it does not try.
func (j *streamJob) finalCheckpoint(ctx context.Context) error {
	if j.cfg.CheckpointEvery < 0 {
		return nil
	}
	return j.snapshot(context.WithoutCancel(ctx), time.Now())
}

// closeSinks releases each sink once, after the last write and the last commit.
//
// It closes what was *configured* rather than what was bound, because
// WithSink("") binds one sink to every terminal stage and closing it once per
// terminal would close it several times. A sink bound explicitly to two stages
// is closed twice, which any sink has to tolerate anyway — Close is the one
// method a caller can be forgiven for calling more than once.
func (j *streamJob) closeSinks() {
	for stage, sink := range j.cfg.Sinks {
		if err := sink.Close(); err != nil {
			j.bus.Publish(observe.Event{Type: observe.SinkWrote, RunID: j.runID,
				Stage: stage, Err: err.Error()})
		}
	}
}

// snapshot writes the checkpoint and then commits, in the order that makes the
// guarantee: the record of where we are is durable before anything is told it
// may forget where it was.
func (j *streamJob) snapshot(ctx context.Context, start time.Time) error {
	epoch := j.epoch.Add(1)
	for _, w := range j.sinks {
		w.flush(epoch)
	}

	ck := stream.Checkpoint{
		JobID: j.runID, Epoch: epoch, Time: time.Now(),
		Watermark: j.wm.Now(),
		Positions: j.wm.Positions(),
		Windows:   map[string]json.RawMessage{},
		Progress: stream.Progress{
			Records: j.records.Load(), Panes: j.panesFired.Load(),
			Late: j.late.Load(), Batches: j.batches.Load(),
		},
	}
	windowRecords := 0
	for id, ws := range j.windows {
		ws.mu.Lock()
		blob, err := ws.w.Snapshot()
		windowRecords += ws.w.Stats().LiveRecords
		ws.mu.Unlock()
		if err != nil {
			return err
		}
		ck.Windows[id] = json.RawMessage(blob)
	}
	if err := j.store.Save(ctx, ck); err != nil {
		return fmt.Errorf("checkpoint %d: %w", epoch, err)
	}

	// Sinks first: a sink that stages its writes makes them visible now that
	// the checkpoint covering them exists. Sources last: their positions may
	// only advance once everything derived from them is durable.
	for stage, w := range j.sinks {
		if err := w.sink.Commit(ctx, epoch); err != nil {
			j.bus.Publish(observe.Event{Type: observe.CheckpointSkipped, RunID: j.runID,
				Stage: stage, Note: "sink commit: " + err.Error()})
		}
	}
	j.commitSources(ctx, ck.Positions)

	j.checkpoints.Add(1)
	j.bus.Publish(observe.Event{Type: observe.CheckpointCommitted, RunID: j.runID,
		Epoch: epoch, Records: windowRecords, Watermark: ck.Watermark,
		Latency: time.Since(start)})
	return nil
}

// commitSources tells each reader how far it may forget.
func (j *streamJob) commitSources(ctx context.Context, positions map[string]stream.Position) {
	j.readersMu.Lock()
	readers := make(map[string]stream.Reader, len(j.readers))
	for k, v := range j.readers {
		readers[k] = v
	}
	j.readersMu.Unlock()

	for key, reader := range readers {
		pos, ok := positions[key]
		if !ok {
			continue
		}
		if err := reader.Commit(ctx, pos); err != nil {
			j.bus.Publish(observe.Event{Type: observe.CheckpointSkipped, RunID: j.runID,
				Split: key, Note: "source commit: " + err.Error()})
		}
	}
}

func (j *streamJob) closeReaders() {
	j.readersMu.Lock()
	readers := j.readers
	j.readers = map[string]stream.Reader{}
	j.readersMu.Unlock()
	for _, r := range readers {
		_ = r.Close()
	}
}

// quiesce waits for the job to come to rest: no reader between the gate and its
// push, no task in flight, and every pipe empty with nothing checked out of it.
//
// The condition is observed twice in a row on purpose. With the gate closed
// nothing can start moving again on its own, so a single observation is already
// sound; the second is cheap insurance against a transition this code has not
// thought of, and costs one poll interval.
func (j *streamJob) quiesce(ctx context.Context) bool {
	timeout := DefaultQuiesceTimeout
	deadline := time.Now().Add(timeout)
	settled := 0
	for {
		if j.atRest() {
			settled++
			if settled >= 2 {
				return true
			}
		} else {
			settled = 0
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func (j *streamJob) atRest() bool {
	if j.reading.Load() != 0 || j.inflight.Load() != 0 {
		return false
	}
	for _, p := range j.pipes {
		if !p.Idle() {
			return false
		}
	}
	return true
}

// motion describes what was still moving when a checkpoint gave up, which is
// the only useful thing to say about a skipped one.
func (j *streamJob) motion() string {
	switch {
	case j.reading.Load() != 0:
		return fmt.Sprintf("%d source readers still reading", j.reading.Load())
	case j.inflight.Load() != 0:
		return fmt.Sprintf("%d tasks still in flight", j.inflight.Load())
	default:
		for id, p := range j.pipes {
			if !p.Idle() {
				return fmt.Sprintf("stage %q still has work queued", id)
			}
		}
	}
	return "the job would not come to rest"
}

// --- Reporting -----------------------------------------------------------

func (j *streamJob) report(restored stream.Checkpoint) StreamReport {
	rep := StreamReport{
		Started: j.started, Stopped: time.Now(), StopReason: j.stopReason(),
		Records: j.records.Load(), Panes: j.panesFired.Load(),
		Late: j.late.Load(), Batches: j.batches.Load(),
		Checkpoints: j.checkpoints.Load(), Epoch: j.epoch.Load(),
		Skipped: j.skipped.Load(), ResumedFrom: restored.Epoch,
		Watermark: j.wm.Now(), Splits: j.wm.Lags(),
		Windows: map[string]stream.WindowStats{},
	}
	for id, ws := range j.windows {
		ws.mu.Lock()
		rep.Windows[id] = ws.w.Stats()
		ws.mu.Unlock()
	}
	for _, src := range j.cfg.Sources {
		if u, ok := src.(interface{ Undecodable() int64 }); ok {
			rep.Undecodable += u.Undecodable()
		}
	}
	return rep
}
