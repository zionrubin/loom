package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/task"
)

// Client executes tasks by putting them on a queue and waiting for a worker to
// report back. It implements executor.Executor.
//
// That sentence is the design. Everything above the executor seam — the
// planner, the scheduler's admission control and class-aware recovery, the
// governor, the cache, lineage, the event stream — is written against
// Execute(ctx, task) (Result, error) and cannot tell which side of a process
// boundary the work happened on. Remote execution is therefore an adapter
// rather than a mode: Local remains the default, a Client is the same one
// method over a queue, and a pipeline moves between them without a line
// changing.
//
// What the client adds to a submission is only what the queue needs and the
// task does not carry:
//
//   - the input records are detached into shared storage above a size
//     threshold, so a redelivered task does not carry its payload through the
//     queue again;
//   - the requirements are derived from the envelope, so a worker that cannot
//     serve the task never claims it;
//   - the await is bounded, so a task no worker in the fleet advertises for
//     surfaces as a transient failure the scheduler can retry rather than as a
//     run that hangs.
//
// A Client is safe for concurrent use, and one is enough for a whole run: the
// scheduler already bounds how many tasks are in flight.
type Client struct {
	cfg ClientConfig

	mu    sync.Mutex
	stats ClientStats
}

// ClientConfig wires a client to a fleet.
type ClientConfig struct {
	// Queue is where tasks go. Required.
	Queue Queue
	// Blobs is the content-addressed storage the workers can also reach.
	// Required: outputs come back by hash, and a client that cannot resolve
	// them has no results.
	Blobs Blobs
	// Name identifies this client on submissions, for reporting.
	Name string
	// Wait bounds how long one task may sit unfinished before the client gives
	// up on it (default 0: until the caller's context ends).
	//
	// Giving up is not cancelling. The submission is idempotent on the task ID,
	// so the scheduler's retry re-attaches to the same queued task rather than
	// enqueueing a second copy of the work — a timeout costs a wait, never a
	// duplicate. What it buys is a run that fails loudly when no worker in the
	// fleet advertises a stage, instead of one that hangs until someone looks.
	Wait time.Duration
	// Inline is the size in bytes below which input records travel inside the
	// submission instead of being detached into shared storage (default 4096).
	//
	// Small inputs are the common case and a round trip to storage for a
	// two-field record is pure latency; large ones are where detaching pays,
	// because the queue entry stays small however big the batch is and a
	// redelivery re-sends a hash rather than a megabyte. Set it to -1 to detach
	// everything, which is what a queue with a hard row-size limit wants.
	Inline int
	// Deliveries bounds redelivery per task (zero uses the queue's default).
	Deliveries int
	// Affinity is how long the queue holds a task carrying a continuation back
	// from workers that do not hold its state (default zero: no waiting, pure
	// preference).
	//
	// It buys locality under contention and costs, at most and at worst, this
	// much latency once per task whose state-holder has died. A poll interval
	// or two is the useful size; anything approaching a lease is trading the
	// wrong thing, since the work would run correctly on any worker in the
	// fleet the whole time it was being held.
	Affinity time.Duration
	// Calls bounds how many times a queue call is retried through an
	// unreachable queue (default 5), and Backoff is the first delay between
	// tries, doubling to a tenth of a second (default 10ms).
	//
	// Retrying here rather than letting the scheduler do it is not redundancy.
	// A submission that did not go through and an await that was interrupted
	// are both calls that failed rather than tasks that failed, and the
	// difference matters: raising them would mark a task failed on a run whose
	// work is very likely already finished on some worker, and would make an
	// ordinary network blip look like a pipeline problem.
	Calls   int
	Backoff time.Duration
	// Cancel withdraws a task from the queue when the client stops waiting for
	// it. It defaults off, because the ordinary reason to stop waiting is a
	// scheduler retry that is about to re-submit the same task ID.
	Cancel bool
	// Bus publishes what the client observes, so a remote run's tasks appear in
	// the same event stream a local run's do.
	Bus *observe.Bus
}

// ClientStats is what the client saw: how much work it dispatched, and the
// shape of what came back.
type ClientStats struct {
	Submitted int `json:"submitted"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
	Detached  int `json:"detached"`
	// Redelivered counts results whose task had been handed out more than once
	// — the client's view of a worker having died holding it. Duplicate
	// *execution* is not visible from here: only the worker that lost the race
	// knows it ran, so worker.WorkerStats.Duplicate is where that is counted.
	Redelivered int           `json:"redelivered"`
	TimedOut    int           `json:"timed_out"`
	Wait        time.Duration `json:"wait"`
}

const (
	defaultInline  = 4096
	defaultCalls   = 5
	defaultBackoff = 10 * time.Millisecond
	backoffCeiling = 100 * time.Millisecond
)

// NewClient returns an executor that runs tasks on a fleet.
//
//	q, err := filequeue.Open(dir, filequeue.Options{})
//	exec := worker.NewClient(worker.ClientConfig{Queue: q, Blobs: cas})
//
// It is the same value loom.WithWorkerService builds for a run; construct one
// by hand when driving runtime.Scheduler directly.
func NewClient(cfg ClientConfig) *Client {
	if cfg.Name == "" {
		cfg.Name = ID("client")
	}
	if cfg.Inline == 0 {
		cfg.Inline = defaultInline
	}
	if cfg.Calls <= 0 {
		cfg.Calls = defaultCalls
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = defaultBackoff
	}
	return &Client{cfg: cfg}
}

// Execute implements executor.Executor.
//
// Submit, await, rehydrate. The three steps are worth reading as three
// failure domains rather than three calls: a submission that fails is
// transient (the queue is unreachable and the scheduler should back off), an
// await that ends is the task's own outcome carried back with its original
// class, and a rehydration that fails means the two sides are not sharing
// storage — a deployment error, and permanent.
func (c *Client) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	if c.cfg.Queue == nil {
		return task.Result{}, core.Permanent(errors.New("worker: no queue configured"))
	}
	if c.cfg.Blobs == nil {
		return task.Result{}, core.Permanent(errors.New("worker: no shared storage configured"))
	}
	start := time.Now()

	sub, err := c.submission(t)
	if err != nil {
		return task.Result{}, err
	}
	if err := c.retry(ctx, func(ctx context.Context) error {
		_, err := c.cfg.Queue.Submit(ctx, sub)
		return err
	}); err != nil {
		return task.Result{}, core.Transient(fmt.Errorf("worker: submit %s: %w", t.ID, err))
	}
	c.count(func(s *ClientStats) { s.Submitted++ })

	status, err := c.await(ctx, t)
	c.count(func(s *ClientStats) { s.Wait += time.Since(start) })
	if err != nil {
		return task.Result{}, err
	}

	switch status.State {
	case StateFailed:
		c.count(func(s *ClientStats) { s.Failed++ })
		if status.Failure == nil {
			return task.Result{}, core.Transient(fmt.Errorf("worker: task %s failed without a reason", t.ID))
		}
		return task.Result{}, status.Failure.Err()
	case StateDone:
		if status.Receipt == nil {
			return task.Result{}, core.Transient(fmt.Errorf("worker: task %s done without a receipt", t.ID))
		}
		res, err := c.result(t, *status.Receipt)
		if err != nil {
			return task.Result{}, err
		}
		c.count(func(s *ClientStats) {
			s.Completed++
			if status.Deliveries > 1 {
				s.Redelivered++
			}
		})
		c.account(t, status, *status.Receipt)
		return res, nil
	default:
		return task.Result{}, core.Transient(fmt.Errorf(
			"worker: task %s is %s, not finished", t.ID, status.State))
	}
}

// submission builds the queue entry for a task, detaching the input records
// into shared storage when they are big enough to be worth storing once.
func (c *Client) submission(t task.Task) (Submission, error) {
	sub := Submission{
		Task: t, Needs: Require(t), Client: c.cfg.Name,
		Deliveries: c.cfg.Deliveries,
	}
	// Where the work would rather run, derived from what it carries rather than
	// declared beside it: a task whose envelope references an evolving context
	// belongs, if anywhere, on the worker that has already materialized one.
	if key := t.Locality(); key != "" {
		sub.Affinity = Affinity{Key: key, Grace: c.cfg.Affinity}
	}
	if len(t.Input) == 0 {
		return sub, nil
	}
	if c.cfg.Inline >= 0 {
		blob, err := json.Marshal(t.Input)
		if err != nil {
			return Submission{}, core.Permanent(fmt.Errorf(
				"worker: task %s input must be JSON-serializable: %w", t.ID, err))
		}
		if len(blob) <= c.cfg.Inline {
			return sub, nil
		}
	}
	hash, err := putRecords(c.cfg.Blobs, t.Input)
	if err != nil {
		return Submission{}, core.Permanent(err)
	}
	// The detached copy is the only one. Leaving the records on the task as
	// well would double every large batch on the queue for no benefit — the
	// worker rehydrates from the hash either way.
	sub.Task.Input = nil
	sub.Input = hash
	c.count(func(s *ClientStats) { s.Detached++ })
	return sub, nil
}

// await waits for a terminal state, bounded by the configured wait.
func (c *Client) await(ctx context.Context, t task.Task) (Status, error) {
	wctx := ctx
	if c.cfg.Wait > 0 {
		var cancel context.CancelFunc
		wctx, cancel = context.WithTimeout(ctx, c.cfg.Wait)
		defer cancel()
	}

	var status Status
	err := c.retry(wctx, func(ctx context.Context) error {
		var err error
		status, err = c.cfg.Queue.Await(ctx, t.ID)
		return err
	})
	if err == nil {
		return status, nil
	}

	// The task is still out there. Whether to withdraw it depends on why we
	// stopped waiting: a cancelled run wants the fleet to stop spending on it,
	// while a wait that merely ran out is usually followed by the scheduler
	// re-submitting the same ID.
	if c.cfg.Cancel || ctx.Err() != nil {
		c.withdraw(t.ID, err)
	}
	if ctx.Err() != nil {
		return Status{}, core.Transient(ctx.Err())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		c.count(func(s *ClientStats) { s.TimedOut++ })
		return Status{}, core.Transient(fmt.Errorf(
			"worker: task %s unfinished after %s — no worker in the fleet has "+
				"claimed it (does one advertise stage %q?)", t.ID, c.cfg.Wait, t.Stage))
	}
	return Status{}, core.Transient(fmt.Errorf("worker: await %s: %w", t.ID, err))
}

// retry runs a queue call through an interruption, stopping at the first
// answer — including an answer that is an error the queue meant.
func (c *Client) retry(ctx context.Context, fn func(context.Context) error) error {
	wait := c.cfg.Backoff
	var err error
	for attempt := 1; attempt <= c.cfg.Calls; attempt++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		// A closed queue, an unknown task or a cancelled caller are answers,
		// not interruptions, and asking again cannot change any of them.
		if ctx.Err() != nil || errors.Is(err, ErrClosed) || errors.Is(err, ErrNotFound) ||
			errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if attempt == c.cfg.Calls {
			break
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
		wait = min(wait*2, backoffCeiling)
	}
	return err
}

// withdraw cancels a task the client has stopped waiting for. It runs on a
// fresh context: the reason we are here is usually that the caller's is gone.
func (c *Client) withdraw(taskID string, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reason := "client stopped waiting"
	if cause != nil {
		reason = fmt.Sprintf("%s: %v", reason, cause)
	}
	_ = c.cfg.Queue.Cancel(ctx, taskID, reason)
}

// result rebuilds a task.Result from a receipt and the shared store.
//
// The receipt carries the accounting and the output's content hash; the
// records themselves are fetched from storage, which is where the worker put
// them and where a duplicate execution would have put identical bytes.
func (c *Client) result(t task.Task, r Receipt) (task.Result, error) {
	res := task.Result{
		TaskID: r.TaskID, Seq: r.Seq, Stage: r.Stage,
		Usage: r.Usage, Model: r.Model, CacheHit: r.CacheHit,
		Artifact: r.Artifact, Latency: r.Latency,
	}
	if res.TaskID == "" {
		res.TaskID, res.Seq, res.Stage = t.ID, t.Seq, t.Stage
	}
	if r.Output == "" {
		return res, nil
	}
	recs, err := getRecords(c.cfg.Blobs, r.Output)
	if err != nil {
		return task.Result{}, core.Permanent(err)
	}
	res.Output = recs
	return res, nil
}

// account folds a remote completion back onto this process's event stream, so a
// run executed on a fleet reports what a local one does.
//
// It exists because the events that carry usage are published where the model
// was called, and that is now somebody else's process. Without this the
// governor would charge the run correctly — usage rides home on the receipt —
// while the run report said the pipeline cost nothing, which is the worst of
// both: right where nobody looks, wrong where everybody does.
//
// The granularity is per task rather than per call, and that is the one thing
// a fleet genuinely loses. A receipt carries the task's summed usage, not the
// prompt and latency of each call inside it, so a stage that issues several
// calls per task reports its tokens, cost and cache rate exactly and its call
// *count* as tasks. Sending per-call telemetry back would mean putting a
// prompt on the queue, which is a much larger thing to pay for a number.
func (c *Client) account(t task.Task, s Status, r Receipt) {
	if c.cfg.Bus == nil {
		return
	}
	if r.CacheHit {
		// A cache hit costs nothing and is reported as one wherever it happens.
		c.cfg.Bus.Publish(observe.Event{
			Type: observe.CacheHit, RunID: t.Envelope.RunID, Stage: t.Stage, TaskID: t.ID,
		})
	} else if r.Usage.Requests > 0 {
		c.cfg.Bus.Publish(observe.Event{
			Type: observe.ModelCalled, RunID: t.Envelope.RunID, Stage: t.Stage,
			TaskID: t.ID, Model: r.Model, Usage: r.Usage, Latency: r.Latency,
			Worker: r.Worker,
		})
	}
	// Redelivery is the one fact only the queue holds: the scheduler counts its
	// own retries and cannot see a lease that expired under a dead worker.
	if s.Deliveries > 1 {
		c.cfg.Bus.Publish(observe.Event{
			Type: observe.TaskRetried, RunID: t.Envelope.RunID, Stage: t.Stage,
			TaskID: t.ID, Worker: r.Worker, Attempt: t.Attempt,
			Note: fmt.Sprintf("redelivered %d times: an earlier worker's lease expired",
				s.Deliveries-1),
		})
	}
}

func (c *Client) count(fn func(*ClientStats)) {
	c.mu.Lock()
	fn(&c.stats)
	c.mu.Unlock()
}

// Stats reports what this client dispatched.
func (c *Client) Stats() ClientStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Compile-time proof that the adapter really is one: if this stops compiling,
// remote execution has stopped being a drop-in for local execution.
var _ executor.Executor = (*Client)(nil)
