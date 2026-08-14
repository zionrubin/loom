package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/task"
)

// Worker is the other side of the queue: a process that advertises what it can
// do, claims tasks it can serve, executes them through an ordinary
// executor.Executor, and commits the results.
//
// The inner executor is almost always executor.Local, and that nesting is the
// design rather than an implementation detail. Local execution stays the thing
// that actually runs a task — the cache short-circuit, the capability-scoped
// runtime, the broker-mediated model call, the lineage record, all unchanged —
// and this type adds only what distribution costs: a lease to hold, a
// heartbeat to keep it, and a commit that cannot land twice. A worker is
// therefore a very thin process, and how a task is executed remains one
// implementation rather than two that have to be kept in agreement.
//
// Three behaviours are worth knowing before deploying one:
//
//   - a worker that loses its lease mid-task cancels the work. The task now
//     belongs to somebody else, and continuing would mean paying a model for
//     output that is already refused;
//   - a commit is retried through a network interruption, because by then the
//     tokens are spent and the receipt is the only thing standing between the
//     run and what it paid for;
//   - a cancelled context stops the worker *claiming*, not the work it holds.
//     In-flight tasks run to completion and commit, bounded by Drain. Killing a
//     worker is safe — that is what leases are for — but letting it stop is
//     cheaper, and the difference is exactly the tasks it was holding.
type Worker struct {
	cfg Config

	mu    sync.Mutex
	stats WorkerStats
	live  map[string]Lease // task ID → the lease currently held
}

// Config wires a worker to a fleet.
type Config struct {
	// Queue is where work comes from. Required.
	Queue Queue
	// Blobs is the shared content-addressed storage: where detached inputs are
	// read from, where broadcast values already live, and where outputs go.
	// Required.
	Blobs Blobs
	// Exec runs one task. Required — in practice an *executor.Local built from
	// the same pipeline the client compiled.
	Exec executor.Executor
	// Caps is what this worker advertises. Stages is the field that matters
	// most: a worker only claims work it has a runner for.
	Caps Capabilities
	// Name identifies this worker. It is the lease owner, so it must be unique
	// across the fleet; the default (host, pid, entropy) is.
	Name string
	// LeaseTTL is how long a claim stands without a heartbeat (default 30s),
	// and Heartbeat how often it is renewed (default LeaseTTL/3).
	//
	// The ratio is the interesting number. Renewing at a third of the TTL
	// survives two lost heartbeats before the task is redelivered, which is
	// about the right tolerance for a network that occasionally drops a call.
	LeaseTTL  time.Duration
	Heartbeat time.Duration
	// Poll is how long the worker waits before asking for work again after
	// finding none, doubling to PollCeiling (defaults 25ms and 500ms). Polling
	// is the whole protocol: it needs no connection the queue must hold open,
	// and an idle fleet costs one indexed read per worker per ceiling.
	Poll        time.Duration
	PollCeiling time.Duration
	// Drain bounds how long a stopping worker waits for the tasks it is
	// holding (default 30s). Past it the work is cancelled and its leases are
	// left to expire, which costs a redelivery — the same cost as a kill.
	Drain time.Duration
	// Commits bounds how many times a commit is retried through a failing
	// queue (default 5). It is deliberately generous: the work is done and
	// paid for by the time it runs, and the alternative to landing it is a
	// redelivery that spends the money again.
	Commits int
	// Locality reports the state keys this worker currently holds, so the queue
	// can offer it work it can serve faster. Nil means this worker expresses no
	// locality, which is the default and costs nothing.
	//
	// It is a function rather than a list because residency changes underneath
	// the worker: a state is admitted when a context is materialized and
	// evicted when the ceiling is reached, and a snapshot taken at startup
	// would be wrong within a round. It is called on every claim, so it wants
	// to be cheap — delta.Store.Resident is a map walk under a mutex — and it
	// wants to be bounded, because a claim carrying ten thousand keys is a
	// claim nobody wants to send.
	//
	// Nothing depends on it being right. A key reported after the state was
	// evicted costs a rebuild; a key not reported costs the affinity that would
	// have avoided one.
	Locality func() []string
	// Bus publishes this worker's task events.
	Bus *observe.Bus
}

// WorkerStats is what a worker did.
type WorkerStats struct {
	Claimed     int `json:"claimed"`
	Completed   int `json:"completed"`
	Failed      int `json:"failed"`
	Redelivered int `json:"redelivered"`
	// Fenced counts tasks dropped mid-execution because the lease was lost.
	// It is the number that says the TTL is short relative to the work.
	Fenced int `json:"fenced"`
	// Dropped counts tasks cut short because this worker was stopped past its
	// drain window — work the fleet will redeliver, and the cost of not
	// letting a worker finish.
	Dropped int `json:"dropped"`
	// Duplicate counts committed results the queue already had: this worker
	// executed a task somebody else had finished. Above zero is normal for
	// at-least-once delivery; growing is a signal the TTL is too short.
	Duplicate int `json:"duplicate"`
	// Local counts tasks the queue sent here because this worker already held
	// their state. Read against Claimed it is how well locality is working, and
	// it is a performance number rather than a correctness one: the tasks in
	// the difference ran on a worker that had to rebuild, and produced the same
	// results a moment later.
	Local      int           `json:"local"`
	Rehydrated int           `json:"rehydrated"`
	Busy       time.Duration `json:"busy"`
}

const (
	defaultPoll        = 25 * time.Millisecond
	defaultPollCeiling = 500 * time.Millisecond
	defaultDrain       = 30 * time.Second
	defaultCommits     = 5
	// minHeartbeat floors the renewal interval, so a lease granted for
	// milliseconds cannot turn one task into a renewal storm.
	minHeartbeat = 5 * time.Millisecond
	// settleTimeout bounds a commit or abandon made on a detached context.
	settleTimeout = 10 * time.Second
)

// New returns a worker for the fleet described by cfg.
func New(cfg Config) (*Worker, error) {
	switch {
	case cfg.Queue == nil:
		return nil, errors.New("worker: a queue is required")
	case cfg.Blobs == nil:
		return nil, errors.New("worker: shared storage is required")
	case cfg.Exec == nil:
		return nil, errors.New("worker: an executor is required")
	}
	if cfg.Name == "" {
		cfg.Name = cfg.Caps.Worker
	}
	if cfg.Name == "" {
		cfg.Name = ID("worker")
	}
	cfg.Caps.Worker = cfg.Name
	if cfg.Caps.Concurrency <= 0 {
		cfg.Caps.Concurrency = 1
	}
	if len(cfg.Caps.Sandboxes) == 0 && !cfg.Caps.Wildcard {
		cfg.Caps.Sandboxes = []task.SandboxProfile{task.SandboxInline}
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultLeaseTTL
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = cfg.LeaseTTL / 3
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = time.Second
	}
	if cfg.Poll <= 0 {
		cfg.Poll = defaultPoll
	}
	if cfg.PollCeiling < cfg.Poll {
		cfg.PollCeiling = max(defaultPollCeiling, cfg.Poll)
	}
	if cfg.Drain <= 0 {
		cfg.Drain = defaultDrain
	}
	if cfg.Commits <= 0 {
		cfg.Commits = defaultCommits
	}
	return &Worker{cfg: cfg, live: map[string]Lease{}}, nil
}

// Name returns this worker's identity in the fleet.
func (w *Worker) Name() string { return w.cfg.Name }

// Capabilities returns what this worker advertises.
func (w *Worker) Capabilities() Capabilities { return w.cfg.Caps }

// Run claims and executes tasks until ctx ends.
//
// The return is nil for an ordinary shutdown — a cancelled context is how a
// worker is asked to stop, not a failure — and non-nil only when the queue is
// gone. Everything in between is a task's problem rather than the worker's,
// and is reported through the queue where the client can see it.
func (w *Worker) Run(ctx context.Context) error {
	// Work runs on a context the shutdown does not reach, so stopping a worker
	// means "stop taking new tasks" rather than "abandon the ones you are
	// holding". Drain is what bounds that promise.
	work, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()

	slots := make(chan struct{}, w.cfg.Caps.Concurrency)
	var wg sync.WaitGroup
	defer w.drain(&wg, stopWork)

	wait := w.cfg.Poll
	for {
		if ctx.Err() != nil {
			return nil
		}
		free := cap(slots) - len(slots)
		if free <= 0 {
			if !sleep(ctx, w.cfg.Poll) {
				return nil
			}
			continue
		}

		claim := Claim{
			Worker: w.cfg.Name, Caps: w.cfg.Caps, Max: free, TTL: w.cfg.LeaseTTL,
		}
		if w.cfg.Locality != nil {
			claim.Resident = w.cfg.Locality()
		}
		assignments, err := w.cfg.Queue.Claim(ctx, claim)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, ErrClosed) {
				return err
			}
			// A queue that cannot be reached is not a worker that should exit:
			// the fleet's whole recovery story is that processes outlive
			// interruptions. Back off and ask again.
			if !sleep(ctx, wait) {
				return nil
			}
			wait = min(wait*2, w.cfg.PollCeiling)
			continue
		}
		if len(assignments) == 0 {
			if !sleep(ctx, wait) {
				return nil
			}
			wait = min(wait*2, w.cfg.PollCeiling)
			continue
		}
		wait = w.cfg.Poll

		for _, a := range assignments {
			slots <- struct{}{}
			wg.Add(1)
			go func(a Assignment) {
				defer wg.Done()
				defer func() { <-slots }()
				w.handle(work, a)
			}(a)
		}
	}
}

// drain waits for in-flight tasks, then cancels whatever is left.
func (w *Worker) drain(wg *sync.WaitGroup, stopWork context.CancelFunc) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.cfg.Drain):
		// Past the drain window the remaining work is cancelled and its leases
		// left to expire — which costs a redelivery, the same price a kill
		// costs, and is what the lease exists to make survivable.
		stopWork()
		<-done
	}
}

// handle executes one assignment and settles it.
func (w *Worker) handle(ctx context.Context, a Assignment) {
	start := time.Now()
	w.track(a.Lease, true)
	defer func() {
		w.track(a.Lease, false)
		w.count(func(s *WorkerStats) {
			s.Claimed++
			s.Busy += time.Since(start)
			if a.Delivery > 1 {
				s.Redelivered++
			}
			if a.Local {
				s.Local++
			}
		})
	}()

	t := a.Task
	if a.Input != "" {
		recs, err := getRecords(w.cfg.Blobs, a.Input)
		if err != nil {
			// The two sides are not sharing storage. Nothing about this task
			// will work on this worker, and it is not the task's fault, so it
			// is reported as the deployment error it is rather than retried.
			w.settleFailure(ctx, a, core.Permanent(err))
			return
		}
		t.Input = recs
		w.count(func(s *WorkerStats) { s.Rehydrated++ })
	}

	// The execution context dies with the lease. A worker whose claim was
	// taken over is producing output nobody will accept, and every further
	// model call on it is money spent on a result that is already refused.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := w.heartbeat(ctx, a.Lease, cancel)

	w.publish(observe.Event{
		Type: observe.TaskStarted, RunID: t.Envelope.RunID, Stage: t.Stage,
		TaskID: t.ID, Worker: w.cfg.Name, Attempt: a.Delivery, Model: t.ResolvedModel,
	})

	res, err := w.cfg.Exec.Execute(runCtx, t)
	lease, held := stop()
	if !held {
		// Fenced mid-flight: whatever happened here is not ours to report.
		// Somebody else holds the task and will commit or fail it themselves.
		w.count(func(s *WorkerStats) { s.Fenced++ })
		w.publish(observe.Event{
			Type: observe.TaskRetried, RunID: t.Envelope.RunID, Stage: t.Stage,
			TaskID: t.ID, Worker: w.cfg.Name, Attempt: a.Delivery,
			Note: "lease lost: the task was redelivered to another worker",
		})
		return
	}

	a.Lease, a.Task = lease, t
	if err != nil {
		if ctx.Err() != nil {
			// Not the task's failure: this worker was stopped past its drain
			// window and cut its own work short. Reporting it would mark the
			// task failed and put its recovery under the scheduler's retry
			// policy, when what should happen is the thing a kill would have
			// done anyway — the lease expires and the task is redelivered.
			w.count(func(s *WorkerStats) { s.Dropped++ })
			w.publish(observe.Event{
				Type: observe.TaskRetried, RunID: t.Envelope.RunID, Stage: t.Stage,
				TaskID: t.ID, Worker: w.cfg.Name, Attempt: a.Delivery,
				Note: "worker stopped: the lease will expire and the task be redelivered",
			})
			return
		}
		w.settleFailure(ctx, a, err)
		return
	}
	w.settleResult(ctx, a, res, start)
}

// settleResult stores the output and commits the receipt.
func (w *Worker) settleResult(ctx context.Context, a Assignment, res task.Result, start time.Time) {
	t, l := a.Task, a.Lease
	receipt := Receipt{
		TaskID: t.ID, Seq: t.Seq, Stage: t.Stage, Worker: w.cfg.Name,
		Token: l.Token, Records: len(res.Output), Usage: res.Usage,
		Model: res.Model, CacheHit: res.CacheHit, Artifact: res.Artifact,
		Latency: res.Latency, Delivery: a.Delivery, At: time.Now(),
	}
	if receipt.Latency == 0 {
		receipt.Latency = time.Since(start)
	}
	if len(res.Output) > 0 {
		// Written before the commit, and to an address derived from the bytes:
		// a duplicate execution of this task stores identical output at the
		// identical hash, so whichever receipt wins names a blob that is
		// already there and already correct.
		hash, err := putRecords(w.cfg.Blobs, res.Output)
		if err != nil {
			w.settleFailure(ctx, a, core.Permanent(err))
			return
		}
		receipt.Output = hash
	}

	stored, err := w.commit(ctx, l, receipt)
	if err != nil {
		// The result is real and paid for, and it could not be recorded. The
		// lease expires and the task is redelivered — and the re-execution is
		// cheap because the result cache is keyed on content rather than on
		// attempt, which is exactly the property that makes at-least-once
		// delivery affordable.
		w.count(func(s *WorkerStats) { s.Failed++ })
		w.publish(observe.Event{
			Type: observe.TaskFailed, RunID: t.Envelope.RunID, Stage: t.Stage,
			TaskID: t.ID, Worker: w.cfg.Name, Attempt: a.Delivery,
			Err: fmt.Sprintf("result computed but not committed: %v", err),
		})
		return
	}
	if stored.Token != l.Token {
		// Somebody else's receipt was already there. This execution was the
		// redundant half of an at-least-once delivery: the money is spent, the
		// bytes are identical, and the queue holds exactly one result.
		w.count(func(s *WorkerStats) { s.Duplicate++ })
		return
	}

	w.count(func(s *WorkerStats) { s.Completed++ })
	w.publish(observe.Event{
		Type: observe.TaskCompleted, RunID: t.Envelope.RunID, Stage: t.Stage,
		TaskID: t.ID, Worker: w.cfg.Name, Attempt: a.Delivery, Model: res.Model,
		Usage: res.Usage, Latency: receipt.Latency,
	})
}

// settleFailure reports a failed execution to the queue.
func (w *Worker) settleFailure(ctx context.Context, a Assignment, err error) {
	w.count(func(s *WorkerStats) { s.Failed++ })
	f := Failed(err, w.cfg.Name)
	f.Delivery = a.Delivery

	sctx, cancel := settle(ctx)
	defer cancel()
	_, aerr := w.cfg.Queue.Abandon(sctx, a.Lease, f)
	if aerr != nil && !errors.Is(aerr, ErrFenced) {
		// Reporting failed too. Silence is the one thing the queue handles on
		// its own: the lease expires and the task is redelivered.
		w.publish(observe.Event{
			Type: observe.TaskFailed, RunID: a.Task.Envelope.RunID, Stage: a.Task.Stage,
			TaskID: a.Task.ID, Worker: w.cfg.Name, Attempt: a.Delivery,
			Err: fmt.Sprintf("%v (and the failure could not be reported: %v)", err, aerr),
		})
		return
	}
	w.publish(observe.Event{
		Type: observe.TaskFailed, RunID: a.Task.Envelope.RunID, Stage: a.Task.Stage,
		TaskID: a.Task.ID, Worker: w.cfg.Name, Attempt: a.Delivery, Err: err.Error(),
	})
}

// commit lands a receipt, retrying through an unreachable queue.
//
// This is the one place in the package where retrying is unambiguously right.
// Everywhere else a call that failed can be answered by doing the work again;
// here the work is finished and paid for, and the only thing between the run
// and its result is a call that did not go through.
func (w *Worker) commit(ctx context.Context, l Lease, r Receipt) (Receipt, error) {
	// A detached context: a worker being shut down should still land the
	// result it has already paid for.
	ctx, cancel := settle(ctx)
	defer cancel()

	wait := w.cfg.Poll
	var err error
	for attempt := 1; attempt <= w.cfg.Commits; attempt++ {
		var stored Receipt
		stored, err = w.cfg.Queue.Commit(ctx, l, r)
		if err == nil {
			return stored, nil
		}
		// A fenced, unknown or closed queue is not a call that failed; it is
		// an answer, and repeating the question cannot change it.
		if errors.Is(err, ErrFenced) || errors.Is(err, ErrNotFound) || errors.Is(err, ErrClosed) {
			return Receipt{}, err
		}
		if attempt == w.cfg.Commits {
			break
		}
		if !sleep(ctx, wait) {
			break
		}
		wait = min(wait*2, w.cfg.PollCeiling)
	}
	return Receipt{}, err
}

// heartbeat renews a lease until the returned stop is called, cancelling the
// work when the lease is lost.
//
// It reports whether the lease still stands, which is what the caller needs to
// know before committing anything. A renewal that fails on a broken connection
// is deliberately *not* treated as a lost lease — it is a call that did not
// arrive, and the queue may well still consider this worker the owner. Only an
// explicit refusal fences the worker; the lease's own expiry is the backstop
// for the case where the calls never resume.
func (w *Worker) heartbeat(ctx context.Context, l Lease, cancel context.CancelFunc) func() (Lease, bool) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	current, held := l, true

	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTimer(w.beat(l))
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				mu.Lock()
				lease := current
				mu.Unlock()

				next, still, err := w.cfg.Queue.Renew(ctx, lease, w.cfg.LeaseTTL)
				if err != nil {
					if !errors.Is(err, ErrFenced) && !errors.Is(err, ErrNotFound) {
						continue // an unreachable queue is not a lost lease
					}
					still = false
				}
				if !still {
					mu.Lock()
					held = false
					mu.Unlock()
					w.track(lease, false)
					cancel()
					return
				}
				mu.Lock()
				current = next
				mu.Unlock()
				w.track(next, true)
				t.Reset(w.beat(next))
			}
		}
	}()

	return func() (Lease, bool) {
		close(done)
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		return current, held
	}
}

// beat is how long to wait before renewing a lease: a third of what the queue
// actually granted, capped by the configured interval.
//
// Reading it off the grant rather than off this worker's config removes a
// whole class of misconfiguration. A queue may clamp the TTL a worker asks for
// — most do, so that one badly configured worker cannot park a task for an
// hour — and a worker that then renewed on its own schedule would let a lease
// it was granted for one second run out while it waited ten to refresh it. The
// symptom would be live work being redelivered under a healthy worker, which
// is the most expensive failure this package has and the hardest to read.
func (w *Worker) beat(l Lease) time.Duration {
	d := w.cfg.Heartbeat
	if life := time.Until(l.Expires); life > 0 {
		if third := life / 3; third < d {
			d = third
		}
	}
	return max(d, minHeartbeat)
}

// track records which leases this worker holds, so a report can say what would
// be redelivered if the process died right now.
func (w *Worker) track(l Lease, held bool) {
	w.mu.Lock()
	if held {
		w.live[l.TaskID] = l
	} else {
		delete(w.live, l.TaskID)
	}
	w.mu.Unlock()
}

// Holding lists the leases this worker currently holds.
func (w *Worker) Holding() []Lease {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]Lease, 0, len(w.live))
	for _, l := range w.live {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID < out[j].TaskID })
	return out
}

func (w *Worker) count(fn func(*WorkerStats)) {
	w.mu.Lock()
	fn(&w.stats)
	w.mu.Unlock()
}

// Stats reports what this worker has done.
func (w *Worker) Stats() WorkerStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

func (w *Worker) publish(e observe.Event) {
	if w.cfg.Bus != nil {
		w.cfg.Bus.Publish(e)
	}
}

// settle returns a bounded context that outlives the caller's cancellation,
// for the calls that must still happen while the process is shutting down.
func settle(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
}

// sleep waits for d, reporting false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// CapabilitiesFor derives what a worker can do from what it was provisioned
// with: the stages it has runners for, the tools it has registered, and the
// models its registry can reach.
//
// Deriving beats declaring for the same reason requirements are read off the
// envelope rather than the pipeline definition: an advertisement maintained by
// hand goes stale, and a stale one is a worker claiming work it cannot do —
// the exact failure this mechanism exists to prevent. Pass the registry and
// executor the worker was built with and the advertisement cannot disagree
// with what the worker will actually manage.
func CapabilitiesFor(local *executor.Local, reg *model.Registry, concurrency int) Capabilities {
	c := Capabilities{
		Worker: ID("worker"), Concurrency: concurrency,
		Sandboxes: []task.SandboxProfile{task.SandboxInline},
	}
	if local != nil {
		for stage := range local.Runners {
			c.Stages = append(c.Stages, stage)
		}
		sort.Strings(c.Stages)
		if local.Tools != nil {
			c.Tools = local.Tools.Names()
		}
	}
	if reg != nil {
		for _, info := range reg.All() {
			c.Providers = append(c.Providers, info.ID)
		}
		sort.Strings(c.Providers)
	}
	return c
}
