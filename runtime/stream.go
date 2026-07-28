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
// until its slowest task returns — an Engine holds a fixed set of execution
// slots that any stage may claim at any time. A task becomes eligible the
// moment its input record exists, and a freed slot goes to whatever work is
// waiting, which may belong to a stage that started long after the one still
// draining. That is the difference between running a pipeline as a sequence
// of barriers and running it as one continuously-fed engine.
//
// The slot count is the run's concurrency ceiling. Admission control, the
// budget governor, and class-aware recovery are unchanged: Engine delegates
// each task to the same Scheduler.RunTask that the batch path uses, so a
// task cannot tell which driver submitted it.
type Engine struct {
	sched *Scheduler
	slots chan struct{}

	wg sync.WaitGroup
}

// NewEngine returns an engine with n execution slots, backed by sched.
func NewEngine(sched *Scheduler, n int) *Engine {
	if n <= 0 {
		n = 8
	}
	return &Engine{sched: sched, slots: make(chan struct{}, n)}
}

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
		Stage: t.Stage, TaskID: t.ID, Records: len(t.Input),
		Input: recordsJSON(t.Input), InputIDs: recordIDs(t.Input)})

	select {
	case e.slots <- struct{}{}:
	case <-ctx.Done():
		done(task.Result{}, core.Transient(ctx.Err()))
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		res, _, err := e.sched.RunTask(ctx, t, "engine")
		<-e.slots
		done(res, err)
	}()
}

// Wait blocks until every submitted task has finished.
func (e *Engine) Wait() { e.wg.Wait() }

// Pipe is the unbounded, single-producer record channel connecting two
// stages of a streaming run.
//
// Unbounded is a deliberate choice, not a shortcut. A bounded pipe would let
// a full downstream queue block an upstream task that is holding an execution
// slot, while the downstream stage waits for a slot to free — a deadlock that
// only appears under load. Buffering instead makes forwarding total: a stage
// can always hand a record downstream and release its slot. The memory
// ceiling is the records in flight, which is what the barrier driver
// materializes in full anyway.
type Pipe struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []core.Record
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
	p.mu.Lock()
	p.buf = append(p.buf, recs...)
	p.mu.Unlock()
	p.cond.Broadcast()
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
func (p *Pipe) Next(ctx context.Context, max int, maxWait time.Duration) ([]core.Record, bool) {
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

// take removes up to n buffered records. Callers hold p.mu.
func (p *Pipe) take(n int) []core.Record {
	if n > len(p.buf) {
		n = len(p.buf)
	}
	out := make([]core.Record, n)
	copy(out, p.buf[:n])
	p.buf = p.buf[n:]
	return out
}
