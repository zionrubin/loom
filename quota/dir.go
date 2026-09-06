package quota

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
)

// Dir is a shared quota over a directory: buckets and a ledger that any number
// of processes on one host read and write, with no database, no broker and no
// coordination system between them.
//
// It is the same argument findings/filestore makes, for the same reason: a
// great many fleets are one machine. A batch of workers started by a systemd
// unit, a cron job and a service sharing an API key, a laptop running four
// executors — all of them are several processes against one account, and none
// of them wants to stand up infrastructure to say so. What they need is:
//
//   - a lock directory created with O_EXCL, which is an atomic test-and-set on
//     every POSIX filesystem, so mutation is serialized between processes;
//   - one small state file, rewritten whole under that lock, so there is no
//     log to compact and no offset to fold — a bucket's level is a number
//     rather than a history, and a ledger of the last week fits in a page.
//
// What it is not is a distributed store. A shared directory is a machine (or an
// NFS mount, whose locking guarantees are exactly as good as its
// administrator's claims), so use it for many processes on one host and use
// [Dial] against a [Handler] for many hosts.
type Dir struct {
	dir  string
	opts Options

	mu sync.Mutex
}

// Options tunes the directory store. The zero value works.
type Options struct {
	// LockTimeout is how long a writer waits for the directory lock before
	// giving up (default 12s), and LockStale how long a lock must sit unmoved
	// before a waiter may break it (default 5s). LockTimeout is always more
	// than twice LockStale: a wait shorter than the stale window could never
	// break a dead holder's lock.
	LockTimeout time.Duration
	LockStale   time.Duration
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

const (
	stateName = "quota.json"
	lockName  = "quota.lock"

	defaultLockStale   = 5 * time.Second
	defaultLockTimeout = 12 * time.Second
)

func (o Options) normalize() Options {
	if o.LockStale <= 0 {
		o.LockStale = defaultLockStale
	}
	if o.LockTimeout <= 0 {
		o.LockTimeout = defaultLockTimeout
	}
	if o.LockTimeout <= 2*o.LockStale {
		o.LockTimeout = 2 * o.LockStale
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Open opens (creating if needed) a quota directory.
func Open(dir string, opts Options) (*Dir, error) {
	if dir == "" {
		return nil, fmt.Errorf("quota: Open needs a directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("quota: open %s: %w", dir, err)
	}
	return &Dir{dir: dir, opts: opts.normalize()}, nil
}

// Dir returns the directory this store is rooted at.
func (d *Dir) Dir() string { return d.dir }

// Close releases the handle. The state is the directory, so there is nothing
// to flush.
func (d *Dir) Close() error { return nil }

func (d *Dir) now() time.Time { return d.opts.Now() }

// --- state ---------------------------------------------------------------

// state is the whole shared quota, and it is deliberately small: a level per
// bucket rather than a log of draws, and an hour per ledger entry rather than a
// line per charge. A file rewritten in place under a lock has no compaction
// problem to solve and no partial read to defend against, which is worth more
// here than the append-only history would be — nothing downstream replays a
// bucket, and what a fleet spent last Tuesday at 3pm is not a number anybody
// needs to the cent.
type state struct {
	Version int                     `json:"version"`
	Buckets map[string]*bucketState `json:"buckets,omitempty"`
	Ledger  ledgerState             `json:"ledger"`
}

type bucketState struct {
	// Requests and Tokens are what is left in the two per-minute buckets, and
	// At is when they were last refilled. Refill is computed rather than
	// scheduled: nothing has to be running for a bucket to fill up.
	Requests float64   `json:"requests"`
	Tokens   float64   `json:"tokens"`
	At       time.Time `json:"at"`
	// The caps the level above was last computed against. A process whose
	// registry states different limits changes the shape of the bucket from
	// its next draw, which is the only resolution available to a store that is
	// bookkeeping rather than policy — and the conservative one, since a lower
	// cap clamps the level down immediately.
	ReqCap float64 `json:"req_cap"`
	TokCap float64 `json:"tok_cap"`

	Admitted int64 `json:"admitted"`
	Returned int64 `json:"returned"`
	Deferred int64 `json:"deferred"`
}

type ledgerState struct {
	// Total is all-time spend, which a wallet with no window reads directly.
	Total   core.Usage `json:"total"`
	Charges int        `json:"charges"`
	// Hours is spend at hourly resolution for the last Retention, keyed by
	// hours since the epoch. Hourly because a window is policy — one process
	// may ask for a day and another for an hour — so the store cannot keep the
	// aggregate any single caller wants and has to keep something both can sum.
	Hours map[int64]core.Usage `json:"hours,omitempty"`
	First time.Time            `json:"first,omitempty"`
	Last  time.Time            `json:"last,omitempty"`
}

func newState() *state {
	return &state{Version: 1, Buckets: map[string]*bucketState{},
		Ledger: ledgerState{Hours: map[int64]core.Usage{}}}
}

func (s *state) normalize() {
	if s.Buckets == nil {
		s.Buckets = map[string]*bucketState{}
	}
	if s.Ledger.Hours == nil {
		s.Ledger.Hours = map[int64]core.Usage{}
	}
}

// --- the five operations -------------------------------------------------

// Draw admits as many of the batch as the model's buckets allow.
func (d *Dir) Draw(ctx context.Context, dr Draw) (Grant, error) {
	if !dr.Metered() || len(dr.Tokens) == 0 {
		return Grant{Admitted: len(dr.Tokens)}, nil
	}
	var g Grant
	err := d.mutate(func(st *state, now time.Time) {
		g = draw(st.bucket(dr.Model), dr, now)
	})
	return g, err
}

// Return gives draws back to the bucket they came from.
func (d *Dir) Return(ctx context.Context, dr Draw) error {
	if !dr.Metered() || len(dr.Tokens) == 0 {
		return nil
	}
	return d.mutate(func(st *state, now time.Time) {
		give(st.bucket(dr.Model), dr, now)
	})
}

// Charge folds usage into the ledger and reports it.
func (d *Dir) Charge(ctx context.Context, c Charge) (Ledger, error) {
	if c.Window > Retention {
		return Ledger{}, fmt.Errorf("%w: %s > %s", ErrWindowTooLong, c.Window, Retention)
	}
	var out Ledger
	err := d.mutate(func(st *state, now time.Time) {
		l := &st.Ledger
		l.Total.Add(c.Usage)
		l.Charges++
		if l.First.IsZero() {
			l.First = now
		}
		l.Last = now
		h := hourOf(now)
		u := l.Hours[h]
		u.Add(c.Usage)
		l.Hours[h] = u
		prune(l, now)
		out = read(l, c.Window, now)
	})
	return out, err
}

// Ledger reports spend without changing it.
func (d *Dir) Ledger(ctx context.Context, window time.Duration) (Ledger, error) {
	if window > Retention {
		return Ledger{}, fmt.Errorf("%w: %s > %s", ErrWindowTooLong, window, Retention)
	}
	var out Ledger
	err := d.view(func(st *state, now time.Time) {
		out = read(&st.Ledger, window, now)
	})
	return out, err
}

// Models reports every bucket as it currently stands, refilled to now.
func (d *Dir) Models(ctx context.Context) ([]ModelState, error) {
	var out []ModelState
	err := d.view(func(st *state, now time.Time) {
		for name, b := range st.Buckets {
			c := *b
			refill(&c, now)
			out = append(out, ModelState{
				Model: name, Requests: c.Requests, Tokens: c.Tokens,
				Admitted: c.Admitted, Returned: c.Returned, Deferred: c.Deferred,
			})
		}
	})
	sortModels(out)
	return out, err
}

func (s *state) bucket(name string) *bucketState {
	b, ok := s.Buckets[name]
	if !ok {
		b = &bucketState{}
		s.Buckets[name] = b
	}
	return b
}

// --- bucket arithmetic ---------------------------------------------------

// refill brings a bucket up to date. It is the same computation the in-process
// limiter does, and it has to be, because the two are the same mechanism with
// the level kept somewhere else.
func refill(b *bucketState, now time.Time) {
	if b.At.IsZero() {
		b.At = now
		return
	}
	dt := now.Sub(b.At).Seconds()
	if dt <= 0 {
		return
	}
	b.Requests = min(b.ReqCap, b.Requests+dt*b.ReqCap/60)
	b.Tokens = min(b.TokCap, b.Tokens+dt*b.TokCap/60)
	b.At = now
}

// reshape sets the bucket's caps from the limits the caller stated, filling a
// bucket seen for the first time and clamping one whose limits have shrunk.
func reshape(b *bucketState, lim model.Limits) {
	reqCap, tokCap := float64(lim.RequestsPerMinute), float64(lim.TokensPerMinute)
	if b.ReqCap == 0 && b.At.IsZero() {
		b.Requests, b.Tokens = reqCap, tokCap
	}
	b.ReqCap, b.TokCap = reqCap, tokCap
	b.Requests = min(b.Requests, reqCap)
	b.Tokens = min(b.Tokens, tokCap)
}

// drawFor is how many tokens one request of est draws from a bucket of this
// capacity: its estimate, or the whole bucket when the estimate exceeds it, so
// a single oversized request is admitted at a full bucket rather than never.
//
// It mirrors the in-process limiter's rule exactly, and the mirroring is the
// point: a fleet whose shared bucket rounded differently from its local one
// would drift, and the drift would be invisible until a provider complained.
func drawFor(est int, capacity float64) float64 {
	need := float64(est)
	if capacity > 0 && need > capacity {
		return capacity
	}
	return need
}

// draw walks the batch from the front, admitting while both buckets allow, and
// stops at the first request that does not fit — reporting how long until it
// would.
func draw(b *bucketState, dr Draw, now time.Time) Grant {
	reshape(b, dr.Limits)
	refill(b, now)
	lim := dr.Limits

	var g Grant
	for _, est := range dr.Tokens {
		needTok := 0.0
		if lim.TokensPerMinute > 0 {
			needTok = drawFor(est, b.TokCap)
		}
		reqOK := lim.RequestsPerMinute <= 0 || b.Requests >= 1
		tokOK := lim.TokensPerMinute <= 0 || b.Tokens >= needTok
		if !reqOK || !tokOK {
			g.Wait = waitFor(b, lim, reqOK, tokOK, needTok)
			b.Deferred += int64(len(dr.Tokens) - g.Admitted)
			return g
		}
		if lim.RequestsPerMinute > 0 {
			b.Requests--
		}
		if lim.TokensPerMinute > 0 {
			b.Tokens -= needTok
		}
		g.Admitted++
		b.Admitted++
	}
	return g
}

// waitFor is how long until the request that did not fit would.
func waitFor(b *bucketState, lim model.Limits, reqOK, tokOK bool, needTok float64) time.Duration {
	wait := 10 * time.Millisecond
	if !reqOK && b.ReqCap > 0 {
		wait = max(wait, time.Duration((1-b.Requests)/(b.ReqCap/60)*float64(time.Second)))
	}
	if !tokOK && b.TokCap > 0 {
		wait = max(wait, time.Duration((needTok-b.Tokens)/(b.TokCap/60)*float64(time.Second)))
	}
	return wait
}

// give returns draws to the bucket, mirroring draw's arithmetic — including
// the clamp — because a refund that does not match its draw is a leak in one
// direction or a licence to overrun in the other.
func give(b *bucketState, dr Draw, now time.Time) {
	reshape(b, dr.Limits)
	refill(b, now)
	lim := dr.Limits
	for _, est := range dr.Tokens {
		if lim.RequestsPerMinute > 0 {
			b.Requests = min(b.ReqCap, b.Requests+1)
		}
		if lim.TokensPerMinute > 0 {
			b.Tokens = min(b.TokCap, b.Tokens+drawFor(est, b.TokCap))
		}
		b.Returned++
	}
}

// --- ledger arithmetic ---------------------------------------------------

func hourOf(t time.Time) int64 { return t.UTC().Unix() / 3600 }

// prune drops hours that have fallen out of the retained history. The all-time
// total keeps them; what they stop being is attributable to an hour.
func prune(l *ledgerState, now time.Time) {
	cutoff := hourOf(now.Add(-Retention))
	for h := range l.Hours {
		if h < cutoff {
			delete(l.Hours, h)
		}
	}
}

// read sums the ledger over a window.
//
// A window is a span ending now rather than a calendar slot, so "the last 24
// hours" means the last 24 hours to every process that asks, whatever time it
// started. The resolution is the hour the spend landed in, which makes the
// boundary fuzzy by less than an hour and the arithmetic something two
// processes cannot disagree about.
func read(l *ledgerState, window time.Duration, now time.Time) Ledger {
	out := Ledger{Total: l.Total, Window: window, Charges: l.Charges, Read: now}
	if window <= 0 {
		out.Spent, out.Since = l.Total, l.First
		return out
	}
	from := hourOf(now.Add(-window))
	var spent core.Usage
	for h, u := range l.Hours {
		if h >= from {
			spent.Add(u)
		}
	}
	out.Spent = spent
	out.Since = time.Unix(from*3600, 0).UTC()
	return out
}

func sortModels(ms []ModelState) {
	slices.SortFunc(ms, func(a, b ModelState) int { return strings.Compare(a.Model, b.Model) })
}

// --- read, mutate, lock --------------------------------------------------

// view runs fn against the state without taking the write lock.
//
// A reader that took the lock would serialize the run report and the budget
// check behind every draw in the fleet, for an answer that is a snapshot the
// moment it is returned anyway. What it must not do is read a file another
// process is halfway through replacing — which is what the rename in save
// makes impossible.
func (d *Dir) view(fn func(*state, time.Time)) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, err := d.load()
	if err != nil {
		return err
	}
	fn(st, d.now())
	return nil
}

// mutate runs fn against the state under the cross-process lock and writes the
// result back.
func (d *Dir) mutate(fn func(*state, time.Time)) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	unlock, err := d.lock()
	if err != nil {
		return err
	}
	defer unlock()

	st, err := d.load()
	if err != nil {
		return err
	}
	fn(st, d.now())
	return d.save(st)
}

func (d *Dir) load() (*state, error) {
	b, err := os.ReadFile(filepath.Join(d.dir, stateName))
	if os.IsNotExist(err) {
		return newState(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("quota: read state: %w", err)
	}
	st := newState()
	if err := json.Unmarshal(b, st); err != nil {
		return nil, fmt.Errorf("quota: parse state: %w", err)
	}
	st.normalize()
	return st, nil
}

// save writes the state by rename, so a reader never sees half of one.
func (d *Dir) save(st *state) error {
	b, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("quota: encode state: %w", err)
	}
	tmp, err := os.CreateTemp(d.dir, stateName+".*")
	if err != nil {
		return fmt.Errorf("quota: write state: %w", err)
	}
	name := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(name)
		return fmt.Errorf("quota: write state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return fmt.Errorf("quota: write state: %w", err)
	}
	if err := os.Rename(name, filepath.Join(d.dir, stateName)); err != nil {
		os.Remove(name)
		return fmt.Errorf("quota: publish state: %w", err)
	}
	return nil
}

// lock takes the cross-process write lock.
//
// A directory created with O_EXCL is the portable atomic test-and-set, and the
// interesting part is what happens when its holder dies. The answer is the one
// worker/filequeue gives, with the same two mechanisms: the lock is stamped
// with a token unique to the acquisition; a breaker may only remove a lock
// whose token *it* has watched, unchanged, for the whole stale window, so a
// lock taken and released quickly by a healthy writer is never a candidate; and
// a release only removes the lock if the token still there is its own, so a
// holder that was declared dead and then woke up cannot release its
// replacement's lock.
//
// What remains is a window in which a suspended holder and its replacement both
// believe they hold it, bounded by the stale window. Here that is cheaper than
// it is for a queue: the worst a torn write can do to a bucket is set its level
// to one of two recent values, and both of them are levels the fleet was
// entitled to be at within the same second.
func (d *Dir) lock() (func(), error) {
	path := filepath.Join(d.dir, lockName)
	stamp := filepath.Join(path, "owner")
	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())

	deadline := time.Now().Add(d.opts.LockTimeout)
	wait := 200 * time.Microsecond
	var seenToken string
	var seenAt time.Time

	for {
		err := os.Mkdir(path, 0o755)
		if err == nil {
			_ = os.WriteFile(stamp, []byte(token), 0o644)
			return func() { d.unlock(path, stamp, token) }, nil
		}
		if !os.IsExist(err) {
			return nil, fmt.Errorf("quota: lock: %w", err)
		}

		held, _ := os.ReadFile(stamp)
		switch cur := string(held); {
		case cur == "" || cur != seenToken:
			seenToken, seenAt = cur, time.Now()
		case time.Since(seenAt) > d.opts.LockStale:
			_ = os.Remove(stamp)
			_ = os.Remove(path)
			seenToken, seenAt = "", time.Time{}
			continue
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("quota: lock at %s is held (waited %s)", path, d.opts.LockTimeout)
		}
		time.Sleep(wait)
		if wait < 5*time.Millisecond {
			wait *= 2
		}
	}
}

func (d *Dir) unlock(path, stamp, token string) {
	if held, err := os.ReadFile(stamp); err != nil || string(held) != token {
		return // broken and re-taken: releasing now would release its owner
	}
	_ = os.Remove(stamp)
	_ = os.Remove(path)
}
