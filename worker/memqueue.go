package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
)

// MemQueue is a Queue in one process's memory, shared by any number of
// goroutines.
//
// It is the default, and it is not a toy. A great many fleets are one process
// with several workers in it — a service that wants execution decoupled from
// scheduling, a batch job spreading tasks over a fixed set of slots, a test
// that needs the distributed code path without a directory — and for all of
// them the durability a file buys is durability against a failure that takes
// the queue with it anyway. What this queue does give is the full protocol:
// real leases, real expiry, real fencing tokens, real at-least-once delivery.
// A worker cannot tell it apart from a durable one, which is what makes it the
// right place to write the failure tests.
//
// Use filequeue when the workers are separate processes.
type MemQueue struct {
	opts MemOptions

	mu     sync.Mutex
	tasks  map[string]*entry
	order  []string // submission order, so claims are roughly FIFO
	token  int64    // the fencing counter, monotone across the whole queue
	stats  Stats
	closed bool

	// waiters are the clients blocked in Await, woken on every terminal
	// transition. A channel per task keeps a run's fan-out from waking every
	// waiter on every completion.
	waiters map[string][]chan struct{}
}

// MemOptions tunes the queue. The zero value works.
type MemOptions struct {
	// LeaseTTL is the default and maximum lease a claim is granted (default
	// 30s). A worker asking for longer is clamped to it, which is what keeps
	// one badly configured worker from parking a task for an hour.
	LeaseTTL time.Duration
	// Deliveries bounds redelivery after expiry (default 3).
	Deliveries int
	// Now is the clock, injectable so expiry can be tested without waiting for
	// it.
	Now func() time.Time
}

// entry is one task's durable state.
type entry struct {
	sub        Submission
	state      State
	lease      Lease
	receipt    *Receipt
	failure    *Failure
	deliveries int
	submitted  time.Time
	updated    time.Time
}

// NewMemQueue returns an empty in-memory queue.
func NewMemQueue(opts MemOptions) *MemQueue {
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = DefaultLeaseTTL
	}
	if opts.Deliveries <= 0 {
		opts.Deliveries = DefaultDeliveries
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &MemQueue{
		opts: opts, tasks: map[string]*entry{}, waiters: map[string][]chan struct{}{},
	}
}

func (q *MemQueue) now() time.Time { return q.opts.Now() }

// Submit implements Queue.
func (q *MemQueue) Submit(ctx context.Context, s Submission) (Status, error) {
	if s.Task.ID == "" {
		return Status{}, core.Permanent(errors.New("worker: a submission needs a task ID"))
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return Status{}, ErrClosed
	}
	now := q.now()

	e, ok := q.tasks[s.Task.ID]
	if !ok {
		e = &entry{sub: s, state: StatePending, submitted: now, updated: now}
		q.tasks[s.Task.ID] = e
		q.order = append(q.order, s.Task.ID)
		q.stats.Submitted++
		return q.statusLocked(e, now), nil
	}

	q.expireLocked(e, now)
	if e.state == StateFailed {
		// Re-arm. The new submission replaces the old one wholesale: a retry
		// that escalated up the model ladder is the same task ID carrying a
		// different resolved model, and running the envelope that already
		// failed would make the escalation invisible to the fleet.
		e.sub = s
		e.state = StatePending
		e.failure, e.lease, e.deliveries = nil, Lease{}, 0
		e.updated = now
		q.stats.Submitted++
	}
	return q.statusLocked(e, now), nil
}

// Claim implements Queue.
func (q *MemQueue) Claim(ctx context.Context, c Claim) ([]Assignment, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	maxN := c.Max
	if maxN <= 0 {
		maxN = DefaultClaimBatch
	}
	ttl := c.TTL
	if ttl <= 0 || ttl > q.opts.LeaseTTL {
		ttl = q.opts.LeaseTTL
	}
	now := q.now()

	var out []Assignment
	for _, id := range q.order {
		if len(out) >= maxN {
			break
		}
		e := q.tasks[id]
		if e == nil {
			continue
		}
		q.expireLocked(e, now)
		if e.state != StatePending {
			continue
		}
		if err := c.Caps.CanRun(e.sub.Needs); err != nil {
			continue // somebody else's work
		}

		q.token++
		e.lease = Lease{
			TaskID: id, Worker: c.Worker, Token: q.token,
			Granted: now, Expires: now.Add(ttl),
		}
		e.state = StateLeased
		e.deliveries++
		e.updated = now
		q.stats.Claims++
		out = append(out, Assignment{
			Lease: e.lease, Task: e.sub.Task, Input: e.sub.Input, Delivery: e.deliveries,
		})
	}
	return out, nil
}

// Renew implements Queue.
func (q *MemQueue) Renew(ctx context.Context, l Lease, ttl time.Duration) (Lease, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return Lease{}, false, ErrClosed
	}
	now := q.now()
	e, ok := q.tasks[l.TaskID]
	if !ok {
		return Lease{}, false, ErrNotFound
	}
	q.expireLocked(e, now)
	// Token equality is the whole check. Comparing owners would let a worker
	// that was fenced and re-claimed the same task renew the wrong lease; the
	// token is unique to one grant and cannot be confused with the next.
	if e.state != StateLeased || e.lease.Token != l.Token {
		return Lease{}, false, nil
	}
	if ttl <= 0 || ttl > q.opts.LeaseTTL {
		ttl = q.opts.LeaseTTL
	}
	e.lease.Expires = now.Add(ttl)
	e.updated = now
	return e.lease, true, nil
}

// Commit implements Queue.
func (q *MemQueue) Commit(ctx context.Context, l Lease, r Receipt) (Receipt, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return Receipt{}, ErrClosed
	}
	now := q.now()
	e, ok := q.tasks[l.TaskID]
	if !ok {
		return Receipt{}, fmt.Errorf("%w: %s", ErrNotFound, l.TaskID)
	}

	// A task that is already done answers every commit with the receipt it
	// holds, whoever asks and whatever token they carry. This is the
	// idempotency the whole design rests on: duplicate delivery, a retried
	// commit whose acknowledgement was lost, and a resurrected worker all land
	// here and all learn the same single outcome.
	if e.state == StateDone && e.receipt != nil {
		if e.receipt.Token != l.Token {
			q.stats.Duplicates++
		}
		return *e.receipt, nil
	}

	q.expireLocked(e, now)
	if e.state != StateLeased || e.lease.Token != l.Token {
		// The late result. Refusing it is what keeps a worker everybody had
		// given up on from overwriting the one that replaced it — and the
		// refusal costs only the redundant execution, never the result.
		q.stats.Fenced++
		return Receipt{}, fmt.Errorf("%w: task %s is %s under token %d, not %d",
			ErrFenced, l.TaskID, e.state, e.lease.Token, l.Token)
	}

	r.TaskID, r.Token = l.TaskID, l.Token
	if r.At.IsZero() {
		r.At = now
	}
	if r.Delivery == 0 {
		r.Delivery = e.deliveries
	}
	e.receipt = &r
	e.state = StateDone
	e.lease = Lease{}
	e.updated = now
	q.wakeLocked(l.TaskID)
	return r, nil
}

// Abandon implements Queue.
func (q *MemQueue) Abandon(ctx context.Context, l Lease, f Failure) (Status, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return Status{}, ErrClosed
	}
	now := q.now()
	e, ok := q.tasks[l.TaskID]
	if !ok {
		return Status{}, fmt.Errorf("%w: %s", ErrNotFound, l.TaskID)
	}
	if e.state.Terminal() {
		// Already settled by somebody else — most often this worker's
		// replacement, which succeeded where it failed. Its outcome stands.
		return q.statusLocked(e, now), nil
	}
	q.expireLocked(e, now)
	if e.state != StateLeased || e.lease.Token != l.Token {
		q.stats.Fenced++
		return Status{}, fmt.Errorf("%w: task %s", ErrFenced, l.TaskID)
	}
	if f.At.IsZero() {
		f.At = now
	}
	e.failure = &f
	e.state = StateFailed
	e.lease = Lease{}
	e.updated = now
	q.wakeLocked(l.TaskID)
	return q.statusLocked(e, now), nil
}

// Status implements Queue.
func (q *MemQueue) Status(ctx context.Context, taskID string) (Status, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.tasks[taskID]
	if !ok {
		return Status{}, false, nil
	}
	now := q.now()
	q.expireLocked(e, now)
	return q.statusLocked(e, now), true, nil
}

// Await implements Queue.
//
// It waits on a per-task channel rather than polling, which is what an
// in-process queue can offer and a file cannot. The retry loop around it
// exists because expiry is evaluated lazily: a task whose lease runs out while
// a client waits produces no event, so the wait wakes periodically to let the
// expiry be noticed and the task be claimable again.
func (q *MemQueue) Await(ctx context.Context, taskID string) (Status, error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return Status{}, ErrClosed
		}
		e, ok := q.tasks[taskID]
		if !ok {
			q.mu.Unlock()
			return Status{}, fmt.Errorf("%w: %s", ErrNotFound, taskID)
		}
		now := q.now()
		q.expireLocked(e, now)
		if e.state.Terminal() {
			s := q.statusLocked(e, now)
			q.mu.Unlock()
			return s, nil
		}
		ch := make(chan struct{})
		q.waiters[taskID] = append(q.waiters[taskID], ch)
		q.mu.Unlock()

		select {
		case <-ch:
		case <-time.After(q.opts.LeaseTTL / 4):
		case <-ctx.Done():
			return Status{}, ctx.Err()
		}
	}
}

// Cancel implements Queue.
func (q *MemQueue) Cancel(ctx context.Context, taskID, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	e, ok := q.tasks[taskID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, taskID)
	}
	if e.state.Terminal() {
		return nil
	}
	now := q.now()
	msg := ErrCancelled.Error()
	if reason != "" {
		msg = fmt.Sprintf("%s: %s", msg, reason)
	}
	e.failure = &Failure{Class: core.FailPermanent, Message: msg, At: now}
	e.state = StateFailed
	// Dropping the lease is what tells the holder: its next heartbeat finds a
	// task it does not own, and it stops paying for output nobody wants.
	e.lease = Lease{}
	e.updated = now
	q.stats.Cancelled++
	q.wakeLocked(taskID)
	return nil
}

// Stats implements Queue. The counters are cumulative; the depths are counted
// fresh, with expiry applied as it is on every other read.
func (q *MemQueue) Stats(ctx context.Context) (Stats, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	s := q.stats
	s.Pending, s.Leased, s.Done, s.Failed = 0, 0, 0, 0
	for _, id := range q.order {
		e := q.tasks[id]
		q.expireLocked(e, now)
		switch e.state {
		case StatePending:
			s.Pending++
		case StateLeased:
			s.Leased++
		case StateDone:
			s.Done++
		case StateFailed:
			s.Failed++
		}
	}
	return s, nil
}

// Close implements Queue. Waiting clients are woken and told.
func (q *MemQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for id := range q.waiters {
		q.wakeLocked(id)
	}
	return nil
}

// expireLocked reclaims a lease nobody renewed.
//
// It runs on every read and every write rather than on a timer, which is the
// property that makes expiry correct without a sweeper goroutine: there is no
// window in which a task is expired but not yet noticed, because "expired" is
// evaluated by whoever looks. A task redelivered past its budget is failed
// rather than handed out again — a task that kills every worker that touches
// it has to be declared the problem at some point, or it will keep the fleet
// busy forever.
func (q *MemQueue) expireLocked(e *entry, now time.Time) {
	if e == nil || e.state != StateLeased || e.lease.Live(now) {
		return
	}
	q.stats.Expired++
	e.lease = Lease{}
	e.updated = now

	limit := e.sub.Deliveries
	if limit <= 0 {
		limit = q.opts.Deliveries
	}
	if e.deliveries >= limit {
		e.state = StateFailed
		e.failure = &Failure{
			Class: core.FailTransient,
			Message: fmt.Sprintf(
				"worker: task delivered %d times without a result (the workers holding it "+
					"stopped heartbeating)", e.deliveries),
			At: now,
		}
		q.wakeLocked(e.sub.Task.ID)
		return
	}
	e.state = StatePending
}

func (q *MemQueue) statusLocked(e *entry, now time.Time) Status {
	s := Status{
		TaskID: e.sub.Task.ID, RunID: e.sub.Task.Envelope.RunID, Stage: e.sub.Task.Stage,
		State: e.state, Lease: e.lease, Receipt: e.receipt, Failure: e.failure,
		Deliveries: e.deliveries, Submitted: e.submitted, Updated: e.updated,
	}
	return s
}

// wakeLocked releases everyone waiting on a task.
func (q *MemQueue) wakeLocked(taskID string) {
	for _, ch := range q.waiters[taskID] {
		close(ch)
	}
	delete(q.waiters, taskID)
}

// Tasks lists the queue's entries, newest state first by submission order.
// Reports and tests use it; the protocol does not.
func (q *MemQueue) Tasks() []Status {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	out := make([]Status, 0, len(q.order))
	for _, id := range q.order {
		e := q.tasks[id]
		q.expireLocked(e, now)
		out = append(out, q.statusLocked(e, now))
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Submitted.Before(out[j].Submitted) })
	return out
}

var _ Queue = (*MemQueue)(nil)
