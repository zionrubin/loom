// Package filequeue is a worker queue over a shared directory: a fleet of
// executor processes with no broker, no database and no coordination service
// between them.
//
// It exists for the two reasons findings/filestore exists, and they are the
// same reasons.
//
// The first is that a contract with one implementation is not a contract. The
// cheapest honest test of whether worker.Queue is really the seam it claims to
// be is to implement it twice, over storage with nothing in common — a map
// guarded by a mutex, and an append-only file several processes fold
// independently — and run one conformance suite against both. Nothing in
// worker.Client or worker.Worker distinguishes them.
//
// The second is that a great many fleets are one machine. Four worker
// processes started by a systemd unit, a laptop proving a pipeline survives a
// kill -9, a CI job that must not depend on a server: all of them want
// cross-process leases and none of them wants infrastructure. A directory
// gives them exactly what the protocol needs:
//
//   - an append-only log, replayed incrementally from a byte offset, so a
//     reader pays for what has changed rather than for what exists;
//   - a lock directory created with O_EXCL, atomic on every POSIX filesystem
//     and on Windows, so the read-modify-write that grants a lease is
//     serialized between processes;
//   - durability that outlives every process holding it, which is the whole
//     point of leasing work rather than assigning it.
//
// Two limits are worth knowing before deploying it. It is not a *distributed*
// queue: a shared directory is a machine (or an NFS mount, whose locking
// guarantees are exactly as good as its administrator's claims), so use it for
// many processes on one host and put a real queue behind worker.Queue for many
// hosts. And the log does not compact — every submission, claim, heartbeat and
// commit is a line that stays — so a queue serving continuous work indefinitely
// grows a replay a new handle has to fold and a claim has to scan past. A
// directory per batch, or per day, is the answer that needs no code; compaction
// under the same lock is the answer that would need some.
package filequeue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/worker"
)

// Queue is a worker queue rooted at a directory. It implements worker.Queue and
// is safe for concurrent use by any number of goroutines and processes.
type Queue struct {
	dir  string
	opts Options
	id   string // this handle's identity, stamped on the locks it takes

	mu     sync.Mutex
	offset int64 // how far into the log this process has replayed
	state  *state
	closed bool
}

// Options tunes the queue. The zero value works.
type Options struct {
	// LeaseTTL is the default and maximum lease granted (default 30s). A worker
	// asking for longer is clamped, which keeps one badly configured worker
	// from parking a task for an hour.
	LeaseTTL time.Duration
	// Deliveries bounds redelivery after expiry (default 3).
	Deliveries int
	// Poll and PollCeiling bound the backoff Await waits with (defaults 20ms
	// and 250ms). A file cannot notify, so waiting is polling — and a poll is
	// one stat and a read of whatever was appended since the last one.
	Poll        time.Duration
	PollCeiling time.Duration
	// LockStale is how long a lock must sit unmoved before a waiter may break
	// it (default 5s), and LockTimeout how long a writer waits for it before
	// giving up (default 12s, and always more than twice LockStale — a wait
	// shorter than the stale window could never break a dead holder's lock).
	LockTimeout time.Duration
	LockStale   time.Duration
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

const (
	logName            = "queue.jsonl"
	lockName           = "queue.lock"
	defaultPoll        = 20 * time.Millisecond
	defaultPollCeiling = 250 * time.Millisecond
	// A mutation is a handful of file operations — microseconds to
	// milliseconds — so five seconds of no movement is three orders of
	// magnitude past "slow" and firmly into "gone". Breaking a lock takes a
	// full stale window of watching, so the wait has to outlast one or a
	// process that died holding the lock would fail every call instead of
	// costing one pause.
	defaultLockStale   = 5 * time.Second
	defaultLockTimeout = 12 * time.Second
)

func (o Options) normalize() Options {
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = worker.DefaultLeaseTTL
	}
	if o.Deliveries <= 0 {
		o.Deliveries = worker.DefaultDeliveries
	}
	if o.Poll <= 0 {
		o.Poll = defaultPoll
	}
	if o.PollCeiling < o.Poll {
		o.PollCeiling = max(defaultPollCeiling, o.Poll)
	}
	if o.LockStale <= 0 {
		o.LockStale = defaultLockStale
	}
	if o.LockTimeout <= 0 {
		o.LockTimeout = defaultLockTimeout
	}
	if o.LockTimeout <= o.LockStale {
		// A wait shorter than the stale window can never break a dead holder's
		// lock: it gives up while still earning the right to remove it.
		o.LockTimeout = o.LockStale * 2
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Open opens (and creates) a queue at dir.
func Open(dir string, opts Options) (*Queue, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("filequeue: a directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Queue{dir: dir, opts: opts.normalize(), id: worker.ID("fq"), state: newState()}, nil
}

// Dir returns the directory this queue lives in.
func (q *Queue) Dir() string { return q.dir }

func (q *Queue) now() time.Time { return q.opts.Now() }

// --- the log ------------------------------------------------------------

// op is one mutation. The log is a sequence of them and the state every process
// holds is their fold, which is what makes a file into a queue several
// processes share: they never have to agree on the state, only on the bytes,
// and the bytes are append-only.
//
// What is deliberately *not* in the log is expiry. A lease that ran out
// produces no record, because it is a fact about the lease already written and
// the current time — every reader derives it identically, and a queue that had
// to write something when a worker died would be a queue that needed a live
// process to notice deaths.
type op struct {
	Kind   string             `json:"kind"`
	Task   string             `json:"task"`
	Sub    *worker.Submission `json:"sub,omitempty"`
	Lease  *worker.Lease      `json:"lease,omitempty"`
	Rcpt   *worker.Receipt    `json:"receipt,omitempty"`
	Fail   *worker.Failure    `json:"failure,omitempty"`
	Token  int64              `json:"token,omitempty"`
	Reason string             `json:"reason,omitempty"`
	At     time.Time          `json:"at,omitempty"`
}

const (
	opSubmit    = "submit"
	opClaim     = "claim"
	opRenew     = "renew"
	opCommit    = "commit"
	opAbandon   = "abandon"
	opCancel    = "cancel"
	opFenced    = "fenced"
	opDuplicate = "duplicate"
)

// entry is one task's folded state.
type entry struct {
	sub        worker.Submission
	state      worker.State
	lease      worker.Lease
	receipt    *worker.Receipt
	failure    *worker.Failure
	deliveries int
	submitted  time.Time
	updated    time.Time
}

type state struct {
	tasks map[string]*entry
	order []string
	token int64
	stats worker.Stats
}

func newState() *state {
	return &state{tasks: map[string]*entry{}}
}

// apply folds one operation into the state. It is the only place the state
// changes, which is what keeps this process's view identical to every other
// process's given the same bytes.
func (s *state) apply(o op) {
	e := s.tasks[o.Task]
	switch o.Kind {
	case opSubmit:
		if o.Sub == nil {
			return
		}
		if e == nil {
			e = &entry{submitted: o.At}
			s.tasks[o.Task] = e
			s.order = append(s.order, o.Task)
		}
		// A submit lands on an existing entry only when the queue accepted a
		// re-arm, so the new submission replaces the old outright — including
		// an envelope the scheduler escalated between the two attempts.
		e.sub = *o.Sub
		e.state = worker.StatePending
		e.lease, e.failure, e.receipt, e.deliveries = worker.Lease{}, nil, nil, 0
		e.updated = o.At
		s.stats.Submitted++
	case opClaim:
		if e == nil || o.Lease == nil {
			return
		}
		if e.state == worker.StateLeased {
			s.stats.Expired++ // this claim took over a lease nobody renewed
		}
		e.lease = *o.Lease
		e.state = worker.StateLeased
		e.deliveries++
		e.updated = o.At
		if o.Lease.Token > s.token {
			s.token = o.Lease.Token
		}
		s.stats.Claims++
	case opRenew:
		if e == nil || o.Lease == nil || e.lease.Token != o.Lease.Token {
			return
		}
		e.lease = *o.Lease
		e.updated = o.At
	case opCommit:
		if e == nil || o.Rcpt == nil || e.state == worker.StateDone {
			return // the first receipt stands, forever
		}
		r := *o.Rcpt
		e.receipt = &r
		e.state = worker.StateDone
		e.lease = worker.Lease{}
		e.updated = o.At
		s.stats.Done++
	case opAbandon, opCancel:
		if e == nil || o.Fail == nil || e.state.Terminal() {
			return
		}
		f := *o.Fail
		e.failure = &f
		e.state = worker.StateFailed
		e.lease = worker.Lease{}
		e.updated = o.At
		s.stats.Failed++
		if o.Kind == opCancel {
			s.stats.Cancelled++
		}
	case opFenced:
		s.stats.Fenced++
	case opDuplicate:
		s.stats.Duplicates++
	}
}

// effective applies expiry to a folded entry, returning the state it really has
// at now and, when the delivery budget is spent, the failure that goes with it.
//
// Deriving rather than recording is what lets a queue with no live process
// still be correct: a worker killed with its lease held leaves a record that
// says "leased until T", and every reader past T agrees the task is claimable
// again without anybody having written anything.
func (q *Queue) effective(e *entry, now time.Time) (worker.State, *worker.Failure) {
	if e.state != worker.StateLeased || e.lease.Live(now) {
		return e.state, e.failure
	}
	limit := e.sub.Deliveries
	if limit <= 0 {
		limit = q.opts.Deliveries
	}
	if e.deliveries >= limit {
		return worker.StateFailed, &worker.Failure{
			Class: core.FailTransient,
			Message: fmt.Sprintf(
				"worker: task delivered %d times without a result (the workers holding "+
					"it stopped heartbeating)", e.deliveries),
			At: e.lease.Expires,
		}
	}
	return worker.StatePending, nil
}

func (q *Queue) status(e *entry, now time.Time) worker.Status {
	st, fail := q.effective(e, now)
	s := worker.Status{
		TaskID: e.sub.Task.ID, RunID: e.sub.Task.Envelope.RunID, Stage: e.sub.Task.Stage,
		State: st, Receipt: e.receipt, Failure: fail,
		Deliveries: e.deliveries, Submitted: e.submitted, Updated: e.updated,
	}
	if st == worker.StateLeased {
		s.Lease = e.lease
	}
	return s
}

// --- replay, lock, mutate ------------------------------------------------

// sync replays whatever other processes have appended since this one last
// looked. Callers hold q.mu.
func (q *Queue) sync() error {
	f, err := os.Open(filepath.Join(q.dir, logName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	base := q.offset
	if _, err := f.Seek(base, io.SeekStart); err != nil {
		return err
	}
	dec := json.NewDecoder(f)
	for {
		var o op
		if err := dec.Decode(&o); err != nil {
			// io.EOF is the ordinary end. Anything else is a partial line —
			// another process is mid-append — and both stop the replay in the
			// same place: the offset advances only past records folded whole,
			// so the next sync resumes exactly where this one stopped.
			break
		}
		q.state.apply(o)
		q.offset = base + dec.InputOffset()
	}
	return nil
}

// lock takes the cross-process write lock.
//
// A directory created with O_EXCL is the portable atomic test-and-set: the
// filesystem either creates it or reports that it exists, everywhere, with no
// advisory-locking semantics to reason about. The interesting part is what
// happens when the holder dies, and the answer is the same one the leases a
// level up give — with the same two mechanisms, for the same reasons.
//
// The lock is *stamped* with a token unique to the acquisition, and:
//
//   - a breaker may only remove a lock whose token *it* has watched, unchanged,
//     for the whole stale window. A process that just arrived cannot break a
//     lock it has never seen, so a lock taken and released quickly by a healthy
//     writer is never a candidate however slow the clock;
//   - a release only removes the lock if the token still there is its own. That
//     is the fencing check, and it is what stops the one hazard a timeout-based
//     break really has: a holder that was declared dead, was broken, and then
//     woke up would otherwise release the *new* owner's lock and leave two
//     writers appending at once.
//
// What remains is a window in which a suspended holder and its replacement both
// believe they hold the lock. It is bounded by the stale window, which is three
// orders of magnitude longer than a mutation takes, and it is the price of a
// lock whose owner can die. A queue that cannot afford it wants a backend with
// real transactions.
func (q *Queue) lock() (func(), error) {
	path := filepath.Join(q.dir, lockName)
	stamp := filepath.Join(path, "owner")
	token := fmt.Sprintf("%s-%d-%d", q.id, os.Getpid(), time.Now().UnixNano())

	deadline := time.Now().Add(q.opts.LockTimeout)
	wait := 200 * time.Microsecond
	// seen is the token this process has been watching, and since when. It is
	// per-attempt state deliberately: the right to break a lock is earned by
	// waiting on it, and cannot be inherited from an earlier wait.
	var seenToken string
	var seenAt time.Time

	for {
		err := os.Mkdir(path, 0o755)
		if err == nil {
			_ = os.WriteFile(stamp, []byte(token), 0o644)
			return func() { q.unlock(path, stamp, token) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}

		held, _ := os.ReadFile(stamp)
		switch cur := string(held); {
		case cur == "" || cur != seenToken:
			// A lock we have not been watching, or one that changed hands while
			// we waited. Either way the clock starts now.
			seenToken, seenAt = cur, time.Now()
		case time.Since(seenAt) > q.opts.LockStale:
			// The same holder, unmoved for the whole stale window. Remove the
			// stamp before the directory so a holder that wakes up finds its
			// token gone and declines to release anyone else's lock.
			_ = os.Remove(stamp)
			_ = os.Remove(path)
			seenToken, seenAt = "", time.Time{}
			continue
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("filequeue: lock at %s is held (waited %s)",
				path, q.opts.LockTimeout)
		}
		time.Sleep(wait)
		if wait < 5*time.Millisecond {
			wait *= 2
		}
	}
}

// unlock releases the lock, but only if it is still ours.
func (q *Queue) unlock(path, stamp, token string) {
	if held, err := os.ReadFile(stamp); err != nil || string(held) != token {
		return // broken and re-taken: releasing now would release its owner
	}
	_ = os.Remove(stamp)
	_ = os.Remove(path)
}

// read replays and runs fn against the state, under q.mu.
func (q *Queue) read(fn func(st *state, now time.Time)) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return worker.ErrClosed
	}
	if err := q.sync(); err != nil {
		return err
	}
	fn(q.state, q.now())
	return nil
}

// mutate takes the directory lock, replays, and runs fn — which returns the
// operations to append. It is the read-modify-write every grant in the protocol
// rests on: claiming a lease, fencing a late commit and re-arming a failed task
// all decide what to write from what is already there, and all of them are
// wrong if another process writes in between.
func (q *Queue) mutate(fn func(st *state, now time.Time) ([]op, error)) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return worker.ErrClosed
	}
	q.mu.Unlock()

	unlock, err := q.lock()
	if err != nil {
		return err
	}
	defer unlock()

	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.sync(); err != nil {
		return err
	}
	ops, err := fn(q.state, q.now())
	if err != nil || len(ops) == 0 {
		return err
	}
	// Written and not applied: the next sync folds them from the log like any
	// other process's writes, so one fold of one log is the only state there
	// is. Folding an increment twice is exactly the bug that costs.
	return q.append(ops...)
}

func (q *Queue) append(ops ...op) error {
	f, err := os.OpenFile(filepath.Join(q.dir, logName),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	for i := range ops {
		line, err := json.Marshal(ops[i])
		if err != nil {
			return err
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	return nil
}

// --- worker.Queue --------------------------------------------------------

// Submit implements worker.Queue.
func (q *Queue) Submit(ctx context.Context, s worker.Submission) (worker.Status, error) {
	if s.Task.ID == "" {
		return worker.Status{}, core.Permanent(errors.New("filequeue: a submission needs a task ID"))
	}
	var out worker.Status
	err := q.mutate(func(st *state, now time.Time) ([]op, error) {
		if e, ok := st.tasks[s.Task.ID]; ok {
			// Anything but a failure stands as it is; a failure is re-armed,
			// carrying the new submission. A task failed by *derivation* — its
			// delivery budget spent, with no record saying so — re-arms here
			// too, which is what makes the submit the thing that revives it.
			if cur, _ := q.effective(e, now); cur != worker.StateFailed {
				out = q.status(e, now)
				return nil, nil
			}
		}
		return []op{{Kind: opSubmit, Task: s.Task.ID, Sub: &s, At: now}}, nil
	})
	if err != nil {
		return worker.Status{}, err
	}
	if out.TaskID != "" {
		return out, nil
	}
	st, _, err := q.Status(ctx, s.Task.ID)
	return st, err
}

// Claim implements worker.Queue.
func (q *Queue) Claim(ctx context.Context, c worker.Claim) ([]worker.Assignment, error) {
	maxN := c.Max
	if maxN <= 0 {
		maxN = worker.DefaultClaimBatch
	}
	ttl := c.TTL
	if ttl <= 0 || ttl > q.opts.LeaseTTL {
		ttl = q.opts.LeaseTTL
	}

	var out []worker.Assignment
	err := q.mutate(func(st *state, now time.Time) ([]op, error) {
		var ops []op
		token := st.token
		for _, id := range st.order {
			if len(ops) >= maxN {
				break
			}
			e := st.tasks[id]
			if e == nil {
				continue
			}
			if cur, _ := q.effective(e, now); cur != worker.StatePending {
				continue
			}
			if err := c.Caps.CanRun(e.sub.Needs); err != nil {
				continue // somebody else's work
			}
			token++
			l := worker.Lease{
				TaskID: id, Worker: c.Worker, Token: token,
				Granted: now, Expires: now.Add(ttl),
			}
			ops = append(ops, op{Kind: opClaim, Task: id, Lease: &l, At: now})
			out = append(out, worker.Assignment{
				Lease: l, Task: e.sub.Task, Input: e.sub.Input, Delivery: e.deliveries + 1,
			})
		}
		return ops, nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Renew implements worker.Queue.
func (q *Queue) Renew(ctx context.Context, l worker.Lease, ttl time.Duration) (worker.Lease, bool, error) {
	if ttl <= 0 || ttl > q.opts.LeaseTTL {
		ttl = q.opts.LeaseTTL
	}
	var out worker.Lease
	var still bool
	var missing bool
	err := q.mutate(func(st *state, now time.Time) ([]op, error) {
		e, ok := st.tasks[l.TaskID]
		if !ok {
			missing = true
			return nil, nil
		}
		cur, _ := q.effective(e, now)
		// Token equality is the whole check. Comparing owners would let a
		// worker that was fenced and later re-claimed the same task renew the
		// wrong grant; a token belongs to one grant and cannot be confused
		// with the next.
		if cur != worker.StateLeased || e.lease.Token != l.Token {
			return nil, nil
		}
		next := e.lease
		next.Expires = now.Add(ttl)
		out, still = next, true
		return []op{{Kind: opRenew, Task: l.TaskID, Lease: &next, At: now}}, nil
	})
	if err != nil {
		return worker.Lease{}, false, err
	}
	if missing {
		return worker.Lease{}, false, worker.ErrNotFound
	}
	return out, still, nil
}

// Commit implements worker.Queue.
func (q *Queue) Commit(ctx context.Context, l worker.Lease, r worker.Receipt) (worker.Receipt, error) {
	var out worker.Receipt
	var fenced, missing bool
	var reason string
	err := q.mutate(func(st *state, now time.Time) ([]op, error) {
		e, ok := st.tasks[l.TaskID]
		if !ok {
			missing = true
			return nil, nil
		}
		// A task already done answers every commit with the receipt it holds,
		// whoever asks and whatever token they carry. This is the idempotency
		// the design rests on: duplicate delivery, a retried commit whose
		// acknowledgement was lost, and a resurrected worker all land here and
		// all learn the same single outcome.
		if e.state == worker.StateDone && e.receipt != nil {
			out = *e.receipt
			if out.Token != l.Token {
				// A duplicate execution, recorded so the fleet's accounting
				// shows what at-least-once delivery actually cost.
				return []op{{Kind: opDuplicate, Task: l.TaskID, Token: l.Token, At: now}}, nil
			}
			return nil, nil
		}
		cur, _ := q.effective(e, now)
		if cur != worker.StateLeased || e.lease.Token != l.Token {
			// The late result. Refusing it is what keeps a worker everybody had
			// given up on from overwriting the one that replaced it — and the
			// refusal costs the redundant execution, never the result.
			fenced = true
			reason = fmt.Sprintf("task %s is %s under token %d, not %d",
				l.TaskID, cur, e.lease.Token, l.Token)
			return []op{{Kind: opFenced, Task: l.TaskID, Token: l.Token, At: now}}, nil
		}
		r.TaskID, r.Token = l.TaskID, l.Token
		if r.At.IsZero() {
			r.At = now
		}
		if r.Delivery == 0 {
			r.Delivery = e.deliveries
		}
		out = r
		return []op{{Kind: opCommit, Task: l.TaskID, Rcpt: &r, At: now}}, nil
	})
	if err != nil {
		return worker.Receipt{}, err
	}
	if missing {
		return worker.Receipt{}, fmt.Errorf("%w: %s", worker.ErrNotFound, l.TaskID)
	}
	if fenced {
		return worker.Receipt{}, fmt.Errorf("%w: %s", worker.ErrFenced, reason)
	}
	return out, nil
}

// Abandon implements worker.Queue.
func (q *Queue) Abandon(ctx context.Context, l worker.Lease, f worker.Failure) (worker.Status, error) {
	var out worker.Status
	var fenced, missing bool
	err := q.mutate(func(st *state, now time.Time) ([]op, error) {
		e, ok := st.tasks[l.TaskID]
		if !ok {
			missing = true
			return nil, nil
		}
		if e.state.Terminal() {
			// Already settled by somebody else — most often this worker's
			// replacement, which succeeded where it failed. That outcome stands.
			out = q.status(e, now)
			return nil, nil
		}
		cur, _ := q.effective(e, now)
		if cur != worker.StateLeased || e.lease.Token != l.Token {
			fenced = true
			return []op{{Kind: opFenced, Task: l.TaskID, Token: l.Token, At: now}}, nil
		}
		if f.At.IsZero() {
			f.At = now
		}
		return []op{{Kind: opAbandon, Task: l.TaskID, Fail: &f, At: now}}, nil
	})
	if err != nil {
		return worker.Status{}, err
	}
	if missing {
		return worker.Status{}, fmt.Errorf("%w: %s", worker.ErrNotFound, l.TaskID)
	}
	if fenced {
		return worker.Status{}, fmt.Errorf("%w: task %s", worker.ErrFenced, l.TaskID)
	}
	if out.TaskID != "" {
		return out, nil
	}
	st, _, err := q.Status(ctx, l.TaskID)
	return st, err
}

// Status implements worker.Queue.
func (q *Queue) Status(ctx context.Context, taskID string) (worker.Status, bool, error) {
	var out worker.Status
	var found bool
	err := q.read(func(st *state, now time.Time) {
		e, ok := st.tasks[taskID]
		if !ok {
			return
		}
		out, found = q.status(e, now), true
	})
	return out, found, err
}

// Await implements worker.Queue by polling.
//
// A file cannot notify, and adding something that could — a socket, a
// watch, a second daemon — would trade the property that makes this backend
// worth having. The poll costs one open and a read of whatever was appended
// since the last one, and it backs off to the ceiling while nothing changes.
func (q *Queue) Await(ctx context.Context, taskID string) (worker.Status, error) {
	wait := q.opts.Poll
	for {
		s, found, err := q.Status(ctx, taskID)
		if err != nil {
			return worker.Status{}, err
		}
		if !found {
			return worker.Status{}, fmt.Errorf("%w: %s", worker.ErrNotFound, taskID)
		}
		if s.Terminal() {
			return s, nil
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return worker.Status{}, ctx.Err()
		}
		wait = min(wait*2, q.opts.PollCeiling)
	}
}

// Cancel implements worker.Queue.
func (q *Queue) Cancel(ctx context.Context, taskID, reason string) error {
	var missing bool
	err := q.mutate(func(st *state, now time.Time) ([]op, error) {
		e, ok := st.tasks[taskID]
		if !ok {
			missing = true
			return nil, nil
		}
		if e.state.Terminal() {
			return nil, nil
		}
		msg := worker.ErrCancelled.Error()
		if reason != "" {
			msg = fmt.Sprintf("%s: %s", msg, reason)
		}
		f := worker.Failure{Class: core.FailPermanent, Message: msg, At: now}
		// The lease goes with it: the holder's next heartbeat finds a task it
		// no longer owns and stops paying for output nobody wants.
		return []op{{Kind: opCancel, Task: taskID, Fail: &f, Reason: reason, At: now}}, nil
	})
	if err != nil {
		return err
	}
	if missing {
		return fmt.Errorf("%w: %s", worker.ErrNotFound, taskID)
	}
	return nil
}

// Stats implements worker.Queue. The depths are derived with expiry applied;
// the counters are folded from the log, so they cover every process that has
// ever written to this queue rather than only this one.
func (q *Queue) Stats(ctx context.Context) (worker.Stats, error) {
	var out worker.Stats
	err := q.read(func(st *state, now time.Time) {
		out = st.stats
		out.Pending, out.Leased, out.Done, out.Failed = 0, 0, 0, 0
		for _, id := range st.order {
			cur, _ := q.effective(st.tasks[id], now)
			switch cur {
			case worker.StatePending:
				out.Pending++
			case worker.StateLeased:
				out.Leased++
			case worker.StateDone:
				out.Done++
			case worker.StateFailed:
				out.Failed++
			}
		}
	})
	return out, err
}

// Tasks lists every entry, in submission order. Reports and tests use it; the
// protocol does not.
func (q *Queue) Tasks() ([]worker.Status, error) {
	var out []worker.Status
	err := q.read(func(st *state, now time.Time) {
		out = make([]worker.Status, 0, len(st.order))
		for _, id := range st.order {
			out = append(out, q.status(st.tasks[id], now))
		}
	})
	return out, err
}

// Close releases the queue. The log is the durable state, so there is nothing
// to flush: every mutation was already appended before its call returned.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	return nil
}

var _ worker.Queue = (*Queue)(nil)
