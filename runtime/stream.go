package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/task"
)

// Engine executes tasks continuously rather than in stage-sized batches.
//
// Where Scheduler.ExecuteAll owns a batch from first task to last — every
// worker idle until the whole stage is submitted, and the stage unfinished
// until its slowest task returns — an Engine draws on a Pool of execution
// slots that any stage may claim at any time. A task becomes eligible the
// moment its input record exists, and a freed slot goes to whatever work is
// waiting, which may belong to a stage that started long after the one still
// draining. That is the difference between running a pipeline as a sequence
// of barriers and running it as one continuously-fed engine.
//
// The pool's slot count is the concurrency ceiling. Admission control, the
// budget governor, and class-aware recovery are unchanged: Engine delegates
// each task to the same Scheduler.RunTask that the batch path uses, so a
// task cannot tell which driver submitted it.
//
// A pool may be shared by several engines, which is how a fleet of concurrent
// pipelines occupies one bounded set of slots instead of one set each. When it
// is, admission stops being first-come-first-served and starts being fair
// across programs — see Pool.
type Engine struct {
	sched *Scheduler
	pool  *Pool

	wg sync.WaitGroup
}

// NewEngine returns an engine with a private pool of n execution slots.
func NewEngine(sched *Scheduler, n int) *Engine {
	return NewEngineOn(NewPool(n), sched)
}

// NewEngineOn returns an engine drawing on an existing pool, so several
// engines — one per concurrently running pipeline, each with its own executor
// and runners — share one bounded set of slots and one fairness policy.
func NewEngineOn(pool *Pool, sched *Scheduler) *Engine {
	if pool == nil {
		pool = NewPool(0)
	}
	return &Engine{sched: sched, pool: pool}
}

// Pool returns the slot pool this engine draws on.
func (e *Engine) Pool() *Pool { return e.pool }

// Submit claims a slot and runs t, invoking done with the outcome when it
// finishes. It blocks only while every slot is busy — that wait is the
// backpressure that keeps in-flight work bounded — and returns as soon as
// the task is admitted, leaving it to run concurrently.
//
// done runs on the executing goroutine and must not block indefinitely; the
// slot is released before it is called, so a slow callback delays only its
// own task's completion accounting, never the next admission.
func (e *Engine) Submit(ctx context.Context, t task.Task, done func(task.Result, error)) {
	// Announce the task as queued before waiting for a slot — the same
	// observation ExecuteAll publishes for a batch, at the same point in the
	// lifecycle. Lineage and the constellation view depend on it, and they
	// must not go dark just because the run is streaming.
	e.sched.publish(observe.Event{Type: observe.TaskScheduled, RunID: t.Envelope.RunID,
		Stage: t.Stage, TaskID: t.ID, Records: len(t.Input), Pane: t.Pane,
		Input: recordsJSON(t.Input), InputIDs: recordIDs(t.Input)})

	// The program a task is admitted against is its run: one pipeline is one
	// program, and its completion time is what a caller waits on.
	lease, err := e.pool.Acquire(ctx, t.Envelope.RunID)
	if err != nil {
		done(task.Result{}, err)
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		res, _, err := e.sched.RunTask(ctx, t, lease.Slot())
		lease.Release()
		done(res, err)
	}()
}

// Wait blocks until every submitted task has finished.
func (e *Engine) Wait() { e.wg.Wait() }

// MarkKind classifies an out-of-band signal travelling in order with the
// records on a pipe.
//
// Marks exist because a stream has to carry statements *about* the records as
// well as the records: that event time has reached here, that a window's
// contents are complete. A stage can only act on such a statement if it arrives
// in the right place relative to the data, which is why they travel on the same
// pipe rather than beside it.
//
// A bounded run produces no marks at all, which is what keeps the barrier and
// pipelined drivers exactly as they were.
type MarkKind uint8

const (
	// MarkNone is an ordinary record-carrying element.
	MarkNone MarkKind = iota
	// MarkWatermark asserts that no record with an event time before Mark.Time
	// will arrive on this pipe again.
	MarkWatermark
	// MarkPaneOpen begins a pane: every element until the matching MarkPaneClose
	// belongs to it.
	MarkPaneOpen
	// MarkPaneClose ends a pane, and is the signal an aggregate needs — it is
	// the end of the input, restricted to a window.
	MarkPaneClose
)

// Mark is a signal carried in order with the records.
type Mark struct {
	Kind MarkKind
	// Time is the watermark value on MarkWatermark.
	Time time.Time
	// Pane identifies the firing on MarkPaneOpen and MarkPaneClose. The
	// identity is a string rather than a structured pane so that this package
	// stays ignorant of windowing: a pipe carries the boundary, the driver
	// knows what is inside it.
	Pane string
}

// Element is one item on a pipe: a record with the event time it arrived
// carrying, or a mark.
type Element struct {
	Record core.Record
	Time   time.Time
	Mark   Mark
}

// IsMark reports whether e is a signal rather than a record.
func (e Element) IsMark() bool { return e.Mark.Kind != MarkNone }

// Pipe is the unbounded, single-producer element channel connecting two
// stages of a streaming run.
//
// Unbounded is a deliberate choice, not a shortcut. A bounded pipe would let
// a full downstream queue block an upstream task that is holding an execution
// slot, while the downstream stage waits for a slot to free — a deadlock that
// only appears under load. Buffering instead makes forwarding total: a stage
// can always hand a record downstream and release its slot. The memory
// ceiling is the records in flight, which is what the barrier driver
// materializes in full anyway.
//
// In stream mode the ceiling is the records in flight *between checkpoints*
// rather than for a whole run, and it is bounded on the other side too: the
// ingestor stops pulling from the source while a checkpoint is taken, so the
// pipes drain rather than growing without limit.
type Pipe struct {
	mu   sync.Mutex
	cond *sync.Cond
	buf  []Element
	// out counts batches taken by NextElements and not yet released with Done.
	// It is what makes Idle mean "nothing is anywhere in this stage" rather
	// than "the buffer happens to be empty": a stage that has taken a batch and
	// is still working on it holds a checkout, and a job looking for a moment to
	// checkpoint has to wait for it.
	out    int
	closed bool
}

// NewPipe returns an empty, open pipe.
func NewPipe() *Pipe {
	p := &Pipe{}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Send appends records. It never blocks.
func (p *Pipe) Send(recs ...core.Record) {
	if len(recs) == 0 {
		return
	}
	els := make([]Element, len(recs))
	for i, r := range recs {
		els[i] = Element{Record: r}
	}
	p.Push(els...)
}

// Push appends elements, records and marks alike. It never blocks.
func (p *Pipe) Push(els ...Element) {
	if len(els) == 0 {
		return
	}
	p.mu.Lock()
	p.buf = append(p.buf, els...)
	p.mu.Unlock()
	p.cond.Broadcast()
}

// Mark appends a signal.
func (p *Pipe) Mark(m Mark) { p.Push(Element{Mark: m}) }

// Len reports how many elements are buffered.
func (p *Pipe) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.buf)
}

// Done releases the batch most recently taken by NextElements, declaring that
// the stage has finished with it — forwarded downstream, folded into a window,
// or handed to the engine. Every NextElements owes exactly one Done.
//
// Next, the bounded view, releases its own batch before returning, because a
// driver that never checkpoints has nothing to tell.
func (p *Pipe) Done() {
	p.mu.Lock()
	if p.out > 0 {
		p.out--
	}
	p.mu.Unlock()
}

// Idle reports whether the pipe holds nothing and owes nothing: the buffer is
// empty and every batch taken from it has been released. It is one third of a
// stream job's definition of "the graph is holding still".
func (p *Pipe) Idle() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.buf) == 0 && p.out == 0
}

// Close signals that no further records will arrive. Readers drain what is
// buffered and then report exhaustion.
func (p *Pipe) Close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.cond.Broadcast()
}

// Next takes up to max records, forming the batch the way a continuously-fed
// engine has to: it waits for the first record, then takes whatever else has
// arrived, and gives up waiting for a full batch after maxWait. A stage with
// batching enabled therefore never stalls behind records that may never come,
// and never sends a one-record request when more are already queued.
//
// It reports false once the pipe is closed and drained, or the context ends.
//
// This is the bounded view of the pipe: marks are dropped, because a driver
// that does not produce them cannot need them. Stream mode reads the same pipe
// with NextElements.
func (p *Pipe) Next(ctx context.Context, max int, maxWait time.Duration) ([]core.Record, bool) {
	els, ok := p.NextElements(ctx, max, maxWait)
	if !ok {
		return nil, false
	}
	p.Done()
	out := make([]core.Record, 0, len(els))
	for _, e := range els {
		if !e.IsMark() {
			out = append(out, e.Record)
		}
	}
	return out, true
}

// NextElements takes up to max elements, records and marks alike, forming the
// batch the way a continuously-fed engine has to: it waits for the first
// element, then takes whatever else has arrived, and gives up waiting for a
// full batch after maxWait.
//
// It reports false once the pipe is closed and drained, or the context ends.
func (p *Pipe) NextElements(ctx context.Context, max int, maxWait time.Duration) ([]Element, bool) {
	if max <= 0 {
		max = 1
	}
	stop := context.AfterFunc(ctx, func() { p.cond.Broadcast() })
	defer stop()

	p.mu.Lock()
	for len(p.buf) == 0 {
		if p.closed || ctx.Err() != nil {
			p.mu.Unlock()
			return nil, false
		}
		p.cond.Wait()
	}
	batch := p.take(max)
	// One checkout per call, registered on the first take: the fill loop below
	// may take again, but it is still the same batch and still owes one Done.
	p.out++
	p.mu.Unlock()

	if len(batch) == max || maxWait <= 0 {
		return batch, true
	}

	// Partial batch: give the rest of the group a bounded chance to arrive.
	deadline := time.NewTimer(maxWait)
	defer deadline.Stop()
	for len(batch) < max {
		p.mu.Lock()
		for len(p.buf) == 0 && !p.closed && ctx.Err() == nil {
			// Re-check on a short tick rather than sleeping on the condition
			// variable, so the deadline can win the race.
			p.mu.Unlock()
			select {
			case <-deadline.C:
				return batch, true
			case <-ctx.Done():
				return batch, true
			case <-time.After(time.Millisecond):
			}
			p.mu.Lock()
		}
		if len(p.buf) == 0 {
			p.mu.Unlock()
			return batch, true // closed or cancelled: ship what we have
		}
		batch = append(batch, p.take(max-len(batch))...)
		p.mu.Unlock()
	}
	return batch, true
}

// take removes up to n buffered elements. Callers hold p.mu.
func (p *Pipe) take(n int) []Element {
	if n > len(p.buf) {
		n = len(p.buf)
	}
	out := make([]Element, n)
	copy(out, p.buf[:n])
	p.buf = p.buf[n:]
	return out
}
