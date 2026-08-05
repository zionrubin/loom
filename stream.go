package loom

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/task"
)

// DefaultBatchWait bounds how long a streaming stage waits for a partial
// batch to fill. It is short relative to a model call: long enough to gather
// records that are already in flight, far too short to matter next to the
// latency of the request it is assembling.
const DefaultBatchWait = 25 * time.Millisecond

// stream runs the plan as a pipeline of concurrent stages instead of a
// sequence of barriers.
//
// Every stage gets a goroutine and an input pipe, and they all start at once.
// A stage pulls records as they arrive, forms tasks, submits them to a shared
// execution engine, and forwards each result downstream the moment it lands —
// so a record can be three stages deep while its neighbours are still on the
// first. Concurrency is bounded once, globally, by the engine's slots rather
// than per stage, which is what lets a stage with work waiting use capacity a
// draining stage isn't.
//
// Two stage kinds are genuinely order-dependent: Combine folds pairwise over
// everything, and ReduceAI aggregates a whole level before starting the next.
// They stay barriers here, buffering their input until upstream closes. That
// is not a limitation of the driver but of the operators — an aggregate over
// a set cannot begin before the set is known.
func (d *driver) stream(ctx context.Context) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	workers := d.cfg.Workers
	if workers <= 0 {
		workers = 8
	}
	// On a fleet the pool is shared with every other agent, so the slots this
	// run claims are slots another run cannot — which is the point. Alone, the
	// engine provisions its own and the fairness policy is inert.
	engine := runtime.NewEngine(&d.sched, workers)
	if d.pool != nil {
		engine = runtime.NewEngineOn(d.pool, &d.sched)
	}

	// One pipe per stage, created up front so a producer never races its
	// consumer's construction. Each pipe has exactly one writer: a stage's
	// upstream is a single node, so no stage's input is contended.
	pipes := make(map[string]*runtime.Pipe, len(d.plan.Order))
	for _, sp := range d.plan.Order {
		pipes[sp.Stage.ID] = runtime.NewPipe()
	}

	var wg sync.WaitGroup
	for _, sp := range d.plan.Order {
		wg.Add(1)
		go func(sp *plan.StagePlan) {
			defer wg.Done()
			d.streamStage(ctx, cancel, engine, sp, pipes)
		}(sp)
	}
	wg.Wait()
	engine.Wait()

	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return ctx.Err()
}

// streamStage drives one stage for the lifetime of a streaming run: consume,
// execute, forward, close.
func (d *driver) streamStage(ctx context.Context, cancel context.CancelCauseFunc,
	engine *runtime.Engine, sp *plan.StagePlan, pipes map[string]*runtime.Pipe) {

	s := sp.Stage
	children := d.plan.Children[s.ID]

	// Closing the downstream pipes on every exit path — completion, failure,
	// or cancellation — is what guarantees the run terminates: each stage's
	// close is the signal its successors need to finish draining.
	defer func() {
		for _, id := range children {
			pipes[id].Close()
		}
		d.stageFinished(s)
	}()
	d.stageStarted(s)

	emit := func(recs []core.Record) {
		for _, id := range children {
			pipes[id].Send(recs...)
		}
	}

	switch s.Kind {
	case pipeline.KindSource:
		recs, err := d.source(ctx, s)
		if err != nil {
			cancel(err)
			return
		}
		d.record(s.ID, recs)
		emit(recs)

	case pipeline.KindCombine, pipeline.KindReduceAI, pipeline.KindIterate:
		// Aggregates need the whole set: drain first, then fold. So does an
		// iterative stage, and for a stronger reason — a message can be
		// addressed to any vertex, so no vertex's inbox is closed until every
		// record has arrived. Streaming a superstep would mean running a
		// vertex before its mail did.
		input := drain(ctx, pipes[s.ID])
		if ctx.Err() != nil {
			return
		}
		out, err := d.aggregate(ctx, engine, sp, input)
		d.record(s.ID, out)
		if err != nil {
			cancel(err)
			return
		}
		emit(out)

	default: // fused pure stages and infer
		d.pump(ctx, cancel, engine, sp, pipes[s.ID], emit)
	}
}

// pump is the continuous-batching loop: take whatever records are available
// (up to the stage's batch size), turn them into a task, submit it, and move
// straight on to the next group without waiting for the previous one to
// finish. Results are forwarded downstream from the completion callback, so
// the loop never blocks on execution — only on admission, when every engine
// slot is busy.
func (d *driver) pump(ctx context.Context, cancel context.CancelCauseFunc,
	engine *runtime.Engine, sp *plan.StagePlan, in *runtime.Pipe, emit func([]core.Record)) {

	s := sp.Stage
	batch := s.Opts.BatchSize
	if batch <= 0 {
		batch = 1
	}
	wait := d.cfg.BatchWait
	if wait <= 0 {
		wait = DefaultBatchWait
	}

	// A stage-level parallelism cap sits inside the engine's global ceiling:
	// the engine bounds the run, this bounds one stage's share of it. Without
	// it, WithParallelism would silently stop meaning anything under
	// streaming.
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
		mu      sync.Mutex
		done    sync.WaitGroup
		ordered []seqRecords
	)
	seq := 0

	for {
		if ctx.Err() != nil {
			break
		}
		recs, ok := in.Next(ctx, batch, wait)
		if !ok {
			break
		}
		tasks, err := sp.BuildTasksBatch(d.runID, recs, batch, d.cfg.EgressAllow)
		if err != nil {
			cancel(err)
			break
		}
		for _, t := range tasks {
			t.Seq = seq
			seq++
			if !acquire() {
				break
			}
			done.Add(1)
			engine.Submit(ctx, t, func(res task.Result, err error) {
				defer done.Done()
				defer release()
				if err != nil {
					class := core.ClassOf(err)
					d.fail(runtime.Failure{Task: t, Err: err, Class: class})
					if class == core.FailBudget {
						cancel(runtime.ErrBudgetExhausted)
					} else if !d.cfg.ContinueOnError {
						cancel(err)
					}
					return
				}
				mu.Lock()
				ordered = append(ordered, seqRecords{seq: res.Seq, recs: res.Output})
				mu.Unlock()
				// Forwarded here, not after the stage finishes: this is the
				// moment the record becomes downstream's problem.
				emit(res.Output)
			})
		}
	}
	done.Wait()

	// Downstream already saw these in completion order; the stage's recorded
	// output is sorted by submission so StageOutputs stays reproducible.
	mu.Lock()
	defer mu.Unlock()
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].seq < ordered[j].seq })
	var out []core.Record
	for _, o := range ordered {
		out = append(out, o.recs...)
	}
	d.record(s.ID, out)
}

// seqRecords keeps a task's output paired with its submission order.
type seqRecords struct {
	seq  int
	recs []core.Record
}

// aggregate runs a barrier stage — Combine's pairwise fold, or ReduceAI's
// tree — over a fully drained input, using the shared engine for the model
// calls so aggregation competes for the same slots as everything else.
func (d *driver) aggregate(ctx context.Context, engine *runtime.Engine,
	sp *plan.StagePlan, input []core.Record) ([]core.Record, error) {

	s := sp.Stage
	switch s.Kind {
	case pipeline.KindCombine:
		folded, err := foldCombine(s, input)
		if err != nil {
			return nil, fmt.Errorf("combine %q: %w", s.ID, err)
		}
		return folded, nil
	case pipeline.KindIterate:
		// The loop is driver-agnostic: it hands each round's tasks to whatever
		// runner it is given, and here that is the shared engine, so a
		// superstep competes for the same slots as every other stage instead
		// of provisioning its own.
		return d.iterate(ctx, sp, input, func(ctx context.Context, tasks []task.Task) ([]task.Result, error) {
			return d.runLevel(ctx, engine, tasks)
		})
	}

	fanIn := s.Reduce.FanIn
	if fanIn <= 1 {
		fanIn = 8
	}
	cur := input
	for len(cur) > 0 {
		tasks, err := sp.BuildTasksBatch(d.runID, cur, fanIn, d.cfg.EgressAllow)
		if err != nil {
			return cur, err
		}
		results, err := d.runLevel(ctx, engine, tasks)
		cur = flatten(results)
		if err != nil {
			return cur, err
		}
		if len(tasks) == 1 {
			break // final aggregation level completed
		}
	}
	return cur, nil
}

// runLevel submits one aggregation level and waits for it. The level itself
// is a barrier — the next level consumes this one's outputs — but the tasks
// within it run concurrently against the shared engine.
func (d *driver) runLevel(ctx context.Context, engine *runtime.Engine,
	tasks []task.Task) ([]task.Result, error) {

	var (
		mu      sync.Mutex
		results []task.Result
		firstEr error
		wg      sync.WaitGroup
	)
	for _, t := range tasks {
		wg.Add(1)
		engine.Submit(ctx, t, func(res task.Result, err error) {
			defer wg.Done()
			if err != nil {
				// Dead letters go through the driver's own lock: this local
				// one guards only this level's results, and other stages may
				// be recording failures concurrently.
				d.fail(runtime.Failure{Task: t, Err: err, Class: core.ClassOf(err)})
				mu.Lock()
				defer mu.Unlock()
				if firstEr == nil && !d.cfg.ContinueOnError {
					firstEr = err
				}
				return
			}
			mu.Lock()
			defer mu.Unlock()
			results = append(results, res)
		})
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	sort.Slice(results, func(i, j int) bool { return results[i].Seq < results[j].Seq })
	return results, firstEr
}

// drain reads a pipe to exhaustion, for the stages that cannot start until
// their whole input is known.
func drain(ctx context.Context, in *runtime.Pipe) []core.Record {
	var all []core.Record
	for {
		recs, ok := in.Next(ctx, 1024, 0)
		if !ok {
			return all
		}
		all = append(all, recs...)
	}
}
