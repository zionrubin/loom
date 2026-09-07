// Package quota holds the two things a provider account has exactly one of —
// a rate limit and a wallet — somewhere every process that spends them can see
// them.
//
// A fleet already fixed this once, one level down. `loom.Run` provisions a rate
// limiter, a budget governor and a result cache per pipeline, and none of those
// is a property of a pipeline: a rate limit belongs to an account, a ceiling
// belongs to a wallet, a cache belongs to work already done. `loom.Fleet` is
// the answer for a *process* — one limiter, one governor, one cache, borrowed
// by every agent on it.
//
// The same sentence is true one level out, and there the answer was missing. A
// worker fleet, a batch of jobs started by a systemd unit, a service that runs
// a pipeline per request, two teams sharing one API key: every one of them is
// several processes against one account, and every one of them gets a fresh
// limiter and a fresh governor per process. Which means:
//
//   - **the rate limit is multiplied by processes.** Ten workers each admitting
//     against 4,000 requests/min collectively admit against 40,000, and the
//     provider answers the difference with 429s the scheduler was built to
//     avoid;
//   - **the ceiling is multiplied by processes.** `WithRunBudget(core.Budget{
//     MaxCostUSD: 100})` is a hard cap in one process and a $1,000 cap in ten.
//     That one is not a throughput bug. It is a money bug, and it is silent.
//
// This package is where those two live so that they are not multiplied: a
// [Store] several processes read and write, a [Shared] front end that speaks the
// limiter's and the governor's own language, and two backends — [Open] over a
// shared directory for many processes on one host, [Dial] over HTTP for a fleet
// spanning hosts.
//
//	q, err := quota.Open("/var/lib/loom/quota", quota.Options{})
//	defer q.Close()
//
//	loom.Run(ctx, p,
//	    loom.WithSharedQuota(q, core.Budget{MaxCostUSD: 500}, 24*time.Hour),
//	    loom.WithRunBudget(core.Budget{MaxCostUSD: 5}),  // this run's own ceiling
//	)
//
// The two ceilings compose the obvious way: a run stops at whichever it reaches
// first, so a runaway pipeline still cannot outspend its own budget and a fleet
// of well-behaved pipelines still cannot outspend the wallet.
//
// # What the store is, and is not
//
// A [Store] is bookkeeping, not policy. It records what a fleet has drawn and
// what it has spent; what that spend is *allowed* to reach is a decision, and
// the decision belongs to the process that was configured with it. So the
// ceiling is never written to the store: each process compares the shared spend
// against the budget it was given, and a fleet whose processes disagree needs no
// reconciling — each stops at its own ceiling and the strictest stops first.
//
// # Two ways to be wrong, and only one of them is allowed
//
// Coordination can fail. A process can die holding a draw it never used; a
// directory can be on an NFS mount having a bad minute; a quota service can be
// restarting. Every one of those is resolved in the same direction:
//
// The shared quota errs low. Never high.
//
//   - A draw a dead process never returns is a request the fleet was entitled
//     to make and did not. The bucket refills toward its cap regardless, so the
//     loss costs throughput for as long as it takes to refill and never lets the
//     fleet exceed the limit.
//   - A store that cannot be reached cannot admit anything, so [Shared.Acquire]
//     fails — as a transient failure, which is the scheduler's cue to back off
//     and retry rather than to dead-letter a task. A quota you cannot reach is a
//     quota you cannot respect.
//   - A wallet that cannot be read is treated as spent once the outage outlasts
//     [Config.Grace]. A blip is absorbed; an outage stops the fleet.
//
// # What is deliberately not shared
//
// [model.Limits.MaxConcurrent] stays in the process. It is the ceiling a model
// running on *your* hardware imposes — llama.cpp's slots, a serving engine's
// batch width — which is a property of a device rather than of an account, and
// two processes pointed at two GPUs would be wrong to share one. A single local
// server behind several processes wants a shared semaphore with heartbeats and
// expiry, which is a different mechanism than a bucket that refills on its own,
// and it is not built.
package quota

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
)

// Retention is how far back a store keeps spend at hourly resolution, and so
// the longest [Config.Window] it can answer. Spend older than this is still in
// the all-time total — which is what a wallet with no window reads — it just
// stops being attributable to an hour.
const Retention = 7 * 24 * time.Hour

// ErrWindowTooLong is returned for a window longer than [Retention].
var ErrWindowTooLong = errors.New("quota: window exceeds the retained history")

// Store is the shared state itself: per-model rate buckets and one ledger of
// spend, held where every process can reach them.
//
// Every method is non-blocking in the sense that matters: a store never waits
// for quota to become available, it reports that there is none and how long
// until there is. Waiting is the caller's, and [Shared] is the caller that does
// it — which is what lets one process coalesce a hundred waiting tasks into one
// round trip instead of a hundred.
//
// Implementations are safe for concurrent use by any number of goroutines and
// any number of processes.
type Store interface {
	// Draw admits as many of a batch of requests as one model's per-minute
	// buckets allow, in the order given.
	Draw(ctx context.Context, d Draw) (Grant, error)
	// Return gives back draws whose requests were never issued — a task settled
	// from cache, or one whose admission was paid for and then abandoned.
	Return(ctx context.Context, d Draw) error
	// Charge adds usage to the ledger and reports it as it now stands.
	Charge(ctx context.Context, c Charge) (Ledger, error)
	// Ledger reports spend without changing it. A zero window reads the
	// all-time total; a non-zero one reads the spend inside it.
	Ledger(ctx context.Context, window time.Duration) (Ledger, error)
	// Models reports the buckets as they currently stand, for the run report.
	Models(ctx context.Context) ([]ModelState, error)
	Close() error
}

// Draw is a batch of admission requests against one model's buckets, in the
// order the caller wants them served.
//
// A batch rather than a request because the cost of a shared bucket is the round
// trip to it, and a process with a hundred tasks waiting on one model has one
// question to ask, not a hundred. Order is FIFO and is the caller's: a store
// walks the batch from the front, drawing while the buckets allow, and stops at
// the first request that does not fit.
type Draw struct {
	Model  string       `json:"model"`
	Limits model.Limits `json:"limits"`
	// Tokens is one entry per request: what that request is estimated to cost
	// the tokens/min bucket. A request costs one from the requests/min bucket
	// whatever its size.
	Tokens []int `json:"tokens"`
}

// Requests is how many requests this batch covers.
func (d Draw) Requests() int { return len(d.Tokens) }

// Metered reports whether this model has per-minute limits at all. A model
// with none never reaches the store: unlimited is unlimited in every process,
// and coordinating about it would be round trips spent agreeing that there is
// nothing to agree about.
func (d Draw) Metered() bool {
	return d.Limits.RequestsPerMinute > 0 || d.Limits.TokensPerMinute > 0
}

// Grant is what a [Draw] was given.
type Grant struct {
	// Admitted is how many of the *leading* entries of Draw.Tokens were
	// admitted. A partial grant is normal and is the point: the front of the
	// queue goes now and the rest waits, rather than everyone waiting for the
	// batch to fit at once.
	Admitted int `json:"admitted"`
	// Wait is how long until the first unadmitted request could be admitted,
	// computed from the refill rate. Zero when the whole batch was admitted.
	Wait time.Duration `json:"wait,omitempty"`
}

// Charge is one task's bill, on its way into the shared ledger.
type Charge struct {
	Usage core.Usage `json:"usage"`
	// Window is the span the returned ledger should sum over — the caller's
	// policy, not the store's. Zero reads the all-time total.
	Window time.Duration `json:"window,omitempty"`
}

// Ledger is what the fleet has spent.
type Ledger struct {
	// Spent is the spend inside the requested window, or the all-time total
	// when the window was zero.
	Spent core.Usage `json:"spent"`
	// Total is the all-time spend, whatever the window.
	Total core.Usage `json:"total"`
	// Window is the span Spent covers, echoed back.
	Window time.Duration `json:"window,omitempty"`
	// Since is when the window Spent covers begins.
	Since time.Time `json:"since"`
	// Charges is how many bills have been folded in, all time. It is the
	// cheapest evidence that other processes are writing here at all.
	Charges int `json:"charges"`
	// Read is when this ledger was read. A [Shared] serving a cached answer
	// says how stale it is rather than pretending it is fresh.
	Read time.Time `json:"read"`
}

// Fits reports whether b still covers what has been spent in the window.
func (l Ledger) Fits(b core.Budget) bool {
	if b.MaxCostUSD > 0 && l.Spent.CostUSD >= b.MaxCostUSD {
		return false
	}
	if b.MaxTokens > 0 && l.Spent.TotalTokens() >= b.MaxTokens {
		return false
	}
	return true
}

// RemainingUSD is what b leaves unspent, or -1 when b sets no dollar ceiling.
func (l Ledger) RemainingUSD(b core.Budget) float64 {
	if b.MaxCostUSD <= 0 {
		return -1
	}
	return max(0, b.MaxCostUSD-l.Spent.CostUSD)
}

// ModelState is one shared bucket as it currently stands.
type ModelState struct {
	Model string `json:"model"`
	// Requests and Tokens are what is left in the two buckets right now.
	Requests float64 `json:"requests"`
	Tokens   float64 `json:"tokens"`
	// Admitted, Returned and Deferred count requests across the whole fleet:
	// admitted through this bucket, given back unissued, and turned away for
	// want of room. Deferred is the number that says whether the fleet is
	// actually contending for this model or merely sharing a bucket it never
	// fills.
	Admitted int64 `json:"admitted"`
	Returned int64 `json:"returned"`
	Deferred int64 `json:"deferred"`
}

// --- The process's view -------------------------------------------------

// Config tunes how this process talks to a shared store. The zero value works.
type Config struct {
	// Wallet is the ceiling this process holds the fleet's spend to. Zero
	// means unlimited, which still records spend — a shared ledger with no
	// ceiling is a fleet-wide cost report, and worth having on its own.
	Wallet core.Budget
	// Window is the span the ceiling applies to: 24h for a daily budget, zero
	// for a pot that does not refill. Windows are aligned to the clock rather
	// than to a process's start, so every process agrees on which window it is
	// in without having to ask.
	Window time.Duration
	// Refresh bounds how stale this process's idea of the fleet's spend may be
	// when nothing it did changed it (default 1s). It is what lets one process
	// learn that another has emptied the wallet.
	Refresh time.Duration
	// Grace is how long the ledger may be unreadable before the wallet is
	// treated as spent (default 15s). A blip is absorbed; an outage stops the
	// fleet, because a wallet you cannot read is a wallet you cannot respect.
	Grace time.Duration
	// Timeout bounds one round trip to the store (default 10s).
	Timeout time.Duration
}

func (c Config) normalize() Config {
	if c.Refresh <= 0 {
		c.Refresh = time.Second
	}
	if c.Grace <= 0 {
		c.Grace = 15 * time.Second
	}
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Second
	}
	return c
}

// Shared is one process's face of a shared quota: the blocking, coalescing
// front end that turns "the bucket had no room" into "wait until it does", and
// the cached view of the ledger that answers the scheduler's budget check
// without a round trip per task.
//
// It implements the two seams the scheduler admits and charges through
// (runtime.Buckets and runtime.Wallet), which is the whole of its integration:
// nothing in planning, execution or recovery knows that the bucket it drew from
// was in another process.
type Shared struct {
	store Store
	cfg   Config

	mu    sync.Mutex
	pools map[string]*pool

	lmu     sync.Mutex
	ledger  Ledger
	ledgerA time.Time // when the cached ledger was last confirmed by the store
	lastOK  time.Time // when the store last answered anything
	lastErr error

	smu   sync.Mutex
	stats Stats
}

// New returns the process's view of a shared store.
//
// It does not take ownership of the store: several Shared over one store is
// exactly what a process running two fleets wants, and closing the store is
// the caller's, because the caller is the one who knows when nobody is left.
func New(store Store, cfg Config) *Shared {
	c := cfg.normalize()
	now := time.Now()
	return &Shared{
		store: store, cfg: c, pools: map[string]*pool{},
		lastOK: now,
	}
}

// Budget returns the ceiling this process holds the fleet to.
func (s *Shared) Budget() core.Budget { return s.cfg.Wallet }

// Window returns the span that ceiling applies to.
func (s *Shared) Window() time.Duration { return s.cfg.Window }

// Stats reports what this process has drawn and spent, alongside the shared
// state as it was last read. It makes one round trip for the buckets.
func (s *Shared) Stats() Stats {
	s.smu.Lock()
	out := s.stats
	s.smu.Unlock()
	out.Budget, out.Window = s.cfg.Wallet, s.cfg.Window

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	defer cancel()
	if l, err := s.store.Ledger(ctx, s.cfg.Window); err == nil {
		s.observe(l, nil)
		out.Ledger = l
	} else {
		s.observe(Ledger{}, err)
		s.lmu.Lock()
		out.Ledger = s.ledger
		s.lmu.Unlock()
	}
	if m, err := s.store.Models(ctx); err == nil {
		out.Models = m
	}
	s.smu.Lock()
	out.Errors, out.LastError = s.stats.Errors, s.stats.LastError
	s.smu.Unlock()
	return out
}

// Stats is what a run made of a shared quota: this process's own traffic
// through it, and the shared state it was drawing against.
type Stats struct {
	// Ledger and Models are the fleet's, read from the store.
	Ledger Ledger
	Models []ModelState
	// Budget and Window are the policy this process applied to that ledger.
	Budget core.Budget
	Window time.Duration

	// Admitted, Refunded and Charges are this process's own traffic.
	Admitted int
	Refunded int
	Charges  int
	// Waits is how many times a request found the shared bucket empty, and
	// Waited how long they waited in total. Together they are the cost of
	// sharing: a fleet that never contends reports zero for both.
	Waits  int
	Waited time.Duration
	// Rounds is how many times this process spoke to the store at all —
	// draws, refunds, charges and balance reads — and is the number to look at
	// when tuning Config.Refresh.
	Rounds int
	// DrawRounds is how many of those carried admission requests, and
	// Coalesced how many requests rode on a round somebody else was already
	// making. Together they are the receipt for batching: a hundred tasks
	// waiting on one model should show far fewer draw rounds than requests.
	DrawRounds int
	Coalesced  int
	// Errors counts round trips that failed, and LastError is the most recent.
	Errors    int
	LastError string
}

// String renders the shared quota's line in a run report.
func (s Stats) String() string {
	var b strings.Builder
	l := s.Ledger
	b.WriteString("shared quota: ")
	if s.Budget.MaxCostUSD > 0 {
		fmt.Fprintf(&b, "$%.4f of $%.4f", l.Spent.CostUSD, s.Budget.MaxCostUSD)
	} else {
		fmt.Fprintf(&b, "$%.4f", l.Spent.CostUSD)
	}
	if s.Window > 0 {
		fmt.Fprintf(&b, " spent by the fleet in the last %s", s.Window)
	} else {
		b.WriteString(" spent by the fleet")
	}
	fmt.Fprintf(&b, " over %d charge(s)", l.Charges)
	if s.Budget.MaxCostUSD > 0 {
		fmt.Fprintf(&b, "; $%.4f left", l.RemainingUSD(s.Budget))
	}
	b.WriteString("\n")

	if s.Admitted > 0 || s.Refunded > 0 {
		fmt.Fprintf(&b, "  admission: %d request(s) drawn from the shared buckets, "+
			"%d given back unissued, in %d round trip(s)", s.Admitted, s.Refunded, s.DrawRounds)
		if s.Coalesced > 0 {
			fmt.Fprintf(&b, " (%d coalesced onto a round somebody else was already making)", s.Coalesced)
		}
		b.WriteString("\n")
	}
	// Reported whether or not it is zero when anything was drawn, because
	// "we waited nothing" is the answer that says the fleet is under its
	// limit — and that is the number an operator is looking for.
	if s.Admitted > 0 {
		fmt.Fprintf(&b, "  contention: %d wait(s), %s spent waiting for room the fleet had already taken\n",
			s.Waits, s.Waited.Round(time.Millisecond))
	}
	for _, m := range s.Models {
		if m.Deferred == 0 && m.Admitted == 0 {
			continue
		}
		fmt.Fprintf(&b, "  %s: %d admitted, %d deferred, %d returned (fleet-wide)\n",
			m.Model, m.Admitted, m.Deferred, m.Returned)
	}
	if s.Rounds > 0 {
		fmt.Fprintf(&b, "  store: %d round trip(s) in all — admission, charges and "+
			"balance reads\n", s.Rounds)
	}
	if s.Errors > 0 {
		fmt.Fprintf(&b, "  %d of them failed; last: %s\n", s.Errors, s.LastError)
	}
	return b.String()
}

func (s *Shared) count(fn func(*Stats)) {
	s.smu.Lock()
	fn(&s.stats)
	s.smu.Unlock()
}

// observe folds a round trip's outcome into what this process believes about
// the ledger and about whether the store is reachable at all.
func (s *Shared) observe(l Ledger, err error) {
	now := time.Now()
	if err != nil {
		s.count(func(st *Stats) { st.Errors++; st.LastError = err.Error() })
		s.lmu.Lock()
		s.lastErr = err
		s.lmu.Unlock()
		return
	}
	s.lmu.Lock()
	if l.Read.IsZero() {
		l.Read = now
	}
	s.ledger, s.ledgerA, s.lastOK, s.lastErr = l, now, now, nil
	s.lmu.Unlock()
}

// --- The wallet ----------------------------------------------------------

// Charge adds a task's bill to the shared ledger and reports the fleet's spend
// and whether this process's ceiling has been reached.
//
// It satisfies runtime.Wallet, which is why it takes no context: the scheduler
// charges from the path that has already finished with the task's, and a bill
// that arrived is a bill that must be recorded whatever the caller's deadline
// was doing. The round trip is bounded by Config.Timeout instead.
func (s *Shared) Charge(u core.Usage) (core.Usage, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	defer cancel()
	l, err := s.store.Charge(ctx, Charge{Usage: u, Window: s.cfg.Window})
	s.count(func(st *Stats) { st.Charges++; st.Rounds++ })
	s.observe(l, err)
	if err != nil {
		// The bill is real whether or not it was recorded, so the caller is
		// told; whether that stops the fleet is Exhausted's decision, which is
		// the one that knows how long the store has been unreachable.
		return s.cached().Spent, s.Exhausted(), fmt.Errorf("quota: charge: %w", err)
	}
	return l.Spent, !l.Fits(s.cfg.Wallet), nil
}

// Exhausted reports whether the fleet has reached this process's ceiling.
//
// The scheduler asks before every attempt, so the answer is served from a
// cached ledger and refreshed at most once per Config.Refresh — which is what
// makes "another process emptied the wallet" something this one finds out about
// in about a second rather than never.
func (s *Shared) Exhausted() bool {
	if s.cfg.Wallet.MaxCostUSD <= 0 && s.cfg.Wallet.MaxTokens <= 0 {
		// No ceiling to reach. The ledger is still written — a shared spend
		// report is worth having on its own — but there is nothing to check it
		// against, and an unreachable store cannot exhaust a budget that does
		// not exist.
		return false
	}
	s.lmu.Lock()
	stale := time.Since(s.ledgerA) > s.cfg.Refresh
	s.lmu.Unlock()
	if stale {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
		l, err := s.store.Ledger(ctx, s.cfg.Window)
		cancel()
		s.count(func(st *Stats) { st.Rounds++ })
		s.observe(l, err)
	}

	s.lmu.Lock()
	defer s.lmu.Unlock()
	if s.lastErr != nil && time.Since(s.lastOK) > s.cfg.Grace {
		// Past the grace window the store is not having a blip, it is gone.
		// Spend that nobody can record is spend nobody can bound, so the
		// ceiling is treated as reached.
		return true
	}
	return !s.ledger.Fits(s.cfg.Wallet)
}

// Ledger returns the fleet's spend as this process last saw it, without a
// round trip.
func (s *Shared) Ledger() Ledger { return s.cached() }

func (s *Shared) cached() Ledger {
	s.lmu.Lock()
	defer s.lmu.Unlock()
	return s.ledger
}

// Refresh reads the ledger from the store now, whatever the cache says. It is
// what Explain uses: a projection wants the balance as it is, not as this
// process last happened to see it.
func (s *Shared) Refresh(ctx context.Context) (Ledger, error) {
	l, err := s.store.Ledger(ctx, s.cfg.Window)
	s.count(func(st *Stats) { st.Rounds++ })
	s.observe(l, err)
	if err != nil {
		return s.cached(), err
	}
	return l, nil
}
