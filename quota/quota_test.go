package quota_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/quota"
	"github.com/zionrubin/loom/quota/quotatest"
)

// --- the clock -----------------------------------------------------------

type clock struct {
	mu sync.Mutex
	at time.Time
}

func newClock() *clock { return &clock{at: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.at = c.at.Add(d)
	c.mu.Unlock()
}

func openDir(t *testing.T) (*quota.Dir, *clock) {
	t.Helper()
	c := newClock()
	d, err := quota.Open(t.TempDir(), quota.Options{Now: c.Now})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, c
}

// --- conformance ---------------------------------------------------------

// Both backends owe the same thing, and this is where that stops being a claim.
func TestConformance(t *testing.T) {
	t.Run("Dir", func(t *testing.T) {
		quotatest.Run(t, func(t *testing.T) (quota.Store, quotatest.Clock) {
			d, c := openDir(t)
			return d, c
		})
	})
	t.Run("Service", func(t *testing.T) {
		quotatest.Run(t, func(t *testing.T) (quota.Store, quotatest.Clock) {
			d, c := openDir(t)
			srv := httptest.NewServer(quota.Handler(d))
			t.Cleanup(srv.Close)
			cl := quota.Dial(srv.URL, quota.ClientOptions{})
			t.Cleanup(func() { cl.Close() })
			return cl, c
		})
	})
}

// --- the directory as two processes --------------------------------------

// Two handles on one directory are what two processes are, and the whole
// package rests on their not each getting a bucket of their own.
func TestTwoHandlesShareOneBucket(t *testing.T) {
	dir := t.TempDir()
	c := newClock()
	open := func() *quota.Dir {
		d, err := quota.Open(dir, quota.Options{Now: c.Now})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { d.Close() })
		return d
	}
	a, b := open(), open()
	lim := model.Limits{RequestsPerMinute: 4}
	ctx := context.Background()

	g, err := a.Draw(ctx, quota.Draw{Model: "m", Limits: lim, Tokens: []int{1, 1, 1}})
	if err != nil || g.Admitted != 3 {
		t.Fatalf("first handle admitted %d (err %v), want 3", g.Admitted, err)
	}
	g, err = b.Draw(ctx, quota.Draw{Model: "m", Limits: lim, Tokens: []int{1, 1, 1}})
	if err != nil {
		t.Fatalf("draw: %v", err)
	}
	if g.Admitted != 1 {
		t.Fatalf("the second handle admitted %d against a bucket of 4 with 3 already "+
			"drawn, want 1 — each process has its own bucket", g.Admitted)
	}
}

// The money bug, stated as a test: without a shared ledger, N processes hold N
// ceilings. This is the assertion that they hold one.
func TestOneCeilingAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	c := newClock()
	budget := core.Budget{MaxCostUSD: 1.00}
	shared := func() *quota.Shared {
		d, err := quota.Open(dir, quota.Options{Now: c.Now})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { d.Close() })
		return quota.New(d, quota.Config{Wallet: budget, Refresh: time.Nanosecond})
	}
	a, b := shared(), shared()

	// Process A spends most of the wallet.
	for i := 0; i < 3; i++ {
		if _, exhausted, err := a.Charge(core.Usage{Requests: 1, CostUSD: 0.30}); err != nil {
			t.Fatalf("charge: %v", err)
		} else if exhausted {
			t.Fatalf("A reported the wallet spent after $%.2f of $1.00", 0.30*float64(i+1))
		}
	}
	// Process B has spent nothing itself and is charged for A's spending anyway,
	// because the wallet is the fleet's.
	spent, exhausted, err := b.Charge(core.Usage{Requests: 1, CostUSD: 0.20})
	if err != nil {
		t.Fatalf("charge: %v", err)
	}
	if spent.CostUSD < 1.09 || spent.CostUSD > 1.11 {
		t.Fatalf("B reads the fleet's spend as $%.4f, want $1.10 — it cannot see A's $0.90",
			spent.CostUSD)
	}
	if !exhausted {
		t.Fatal("the charge that crossed $1.00 did not report the wallet spent")
	}
	// And A, which has not spent a cent since, learns it from the same ledger.
	if !a.Exhausted() {
		t.Fatal("A is still spending: its own $0.90 is under the ceiling and it has not " +
			"noticed B's $0.20")
	}
}

// A wallet with no ceiling is still worth having: it is a fleet-wide cost
// report, and nothing about it should stop a run.
func TestAWalletWithNoCeilingNeverStops(t *testing.T) {
	d, _ := openDir(t)
	s := quota.New(d, quota.Config{})
	for i := 0; i < 5; i++ {
		if _, exhausted, err := s.Charge(core.Usage{Requests: 1, CostUSD: 1000}); err != nil || exhausted {
			t.Fatalf("charge %d: exhausted=%v err=%v", i, exhausted, err)
		}
	}
	if s.Exhausted() {
		t.Fatal("a wallet with no ceiling reported itself spent")
	}
	if got := s.Ledger().Spent.CostUSD; got != 5000 {
		t.Fatalf("the ledger recorded $%.2f, want $5000 — spend is recorded whether or "+
			"not there is a ceiling to check it against", got)
	}
}

// --- admission through Shared -------------------------------------------

// The front end's contract is the in-process limiter's: block until admission
// is possible. Same shape, different bucket.
func TestAcquireBlocksUntilTheBucketRefills(t *testing.T) {
	d, c := openDir(t)
	s := quota.New(d, quota.Config{})
	lim := model.Limits{RequestsPerMinute: 60} // one a second
	ctx := context.Background()

	for i := 0; i < 60; i++ {
		if err := s.Acquire(ctx, "m", lim, 1); err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- s.Acquire(ctx, "m", lim, 1) }()

	select {
	case err := <-done:
		t.Fatalf("the 61st acquisition returned %v immediately against a full bucket", err)
	case <-time.After(50 * time.Millisecond):
	}
	c.Advance(2 * time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("after the bucket refilled: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the acquisition never woke up after the bucket refilled")
	}
}

// The batch exists to make coordination cheaper as contention rises, not more
// expensive. A hundred tasks waiting on one model must not be a hundred round
// trips.
func TestWaitingTasksShareOneRoundTrip(t *testing.T) {
	d, _ := openDir(t)
	counted := &counting{Store: d}
	s := quota.New(counted, quota.Config{})
	lim := model.Limits{RequestsPerMinute: 1000}

	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if err := s.Acquire(context.Background(), "m", lim, 1); err != nil {
				t.Errorf("acquire: %v", err)
			}
		}()
	}
	wg.Wait()

	draws := counted.draws.Load()
	if draws >= n {
		t.Fatalf("%d concurrent acquisitions took %d round trips; the batch bought nothing", n, draws)
	}
	st := s.Stats()
	if st.Admitted != n {
		t.Fatalf("stats report %d admitted, want %d", st.Admitted, n)
	}
	if st.Coalesced == 0 {
		t.Fatal("no acquisition was reported as coalesced onto somebody else's round trip")
	}
	t.Logf("%d acquisitions in %d round trip(s)", n, draws)
}

// Every task admitted has to be admitted exactly once, however the batching
// interleaved. This is the test that says the leader hands out what it drew and
// no more.
func TestNoRequestIsAdmittedTwice(t *testing.T) {
	d, _ := openDir(t)
	s := quota.New(d, quota.Config{})
	// A bucket of exactly 20 against 50 hopefuls: 20 succeed, and the rest must
	// still be waiting when the test gives up on them.
	lim := model.Limits{RequestsPerMinute: 20}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	var ok, failed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Acquire(ctx, "m", lim, 1); err != nil {
				failed.Add(1)
				return
			}
			ok.Add(1)
		}()
	}
	wg.Wait()
	if got := ok.Load(); got != 20 {
		t.Fatalf("%d acquisitions succeeded against a bucket of 20", got)
	}
	if got := failed.Load(); got != 30 {
		t.Fatalf("%d acquisitions failed, want 30", got)
	}
}

// A task settled from cache reached no provider, so its admission goes back to
// the fleet rather than throttling the calls that still have to be made — the
// same rule the in-process limiter follows, now across processes.
func TestARefundReturnsAdmissionToTheFleet(t *testing.T) {
	d, _ := openDir(t)
	s := quota.New(d, quota.Config{})
	lim := model.Limits{RequestsPerMinute: 2}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := s.Acquire(ctx, "m", lim, 1); err != nil {
			t.Fatalf("acquire: %v", err)
		}
	}
	s.Refund("m", lim, 1)
	s.Flush()

	done := make(chan error, 1)
	go func() { done <- s.Acquire(ctx, "m", lim, 1) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquire after refund: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the refunded admission never came back to the bucket")
	}
}

// --- failing closed ------------------------------------------------------

// A quota you cannot reach is a quota you cannot respect. The failure has to be
// transient, though: the scheduler backs off and retries a transient failure
// and dead-letters a permanent one, and a quota service restarting is the first
// of those.
func TestAnUnreachableStoreRefusesAdmission(t *testing.T) {
	broken := &failing{err: errors.New("connection refused")}
	s := quota.New(broken, quota.Config{Grace: time.Nanosecond})
	err := s.Acquire(context.Background(), "m", model.Limits{RequestsPerMinute: 10}, 1)
	if err == nil {
		t.Fatal("an unreachable store admitted a request")
	}
	if class := core.ClassOf(err); class != core.FailTransient {
		t.Fatalf("the refusal classified as %s, want transient so the scheduler retries", class)
	}
}

// A blip is absorbed; an outage stops the fleet. Grace is how long "blip"
// lasts, and past it an unreadable wallet is treated as spent.
func TestAnOutageEventuallyStopsTheWallet(t *testing.T) {
	broken := &failing{err: errors.New("connection refused")}
	s := quota.New(broken, quota.Config{
		Wallet: core.Budget{MaxCostUSD: 100}, Grace: 60 * time.Millisecond,
		Refresh: time.Nanosecond,
	})
	if s.Exhausted() {
		t.Fatal("the wallet gave up inside its grace window")
	}
	time.Sleep(120 * time.Millisecond)
	if !s.Exhausted() {
		t.Fatal("the wallet is still spending against a store nobody can read")
	}
}

// An error from the store must not be mistaken for an answer. A charge that was
// never recorded is reported as such, so the run report says the number it has
// is incomplete rather than presenting it as the fleet's spend.
func TestAFailedChargeIsReported(t *testing.T) {
	broken := &failing{err: errors.New("connection refused")}
	s := quota.New(broken, quota.Config{})
	if _, _, err := s.Charge(core.Usage{CostUSD: 1}); err == nil {
		t.Fatal("a charge into an unreachable store reported success")
	}
	if st := s.Stats(); st.Errors == 0 || st.LastError == "" {
		t.Fatalf("the failure left no trace in the stats: %+v", st)
	}
}

// --- the report ----------------------------------------------------------

func TestStatsReadLikeAReport(t *testing.T) {
	d, _ := openDir(t)
	s := quota.New(d, quota.Config{Wallet: core.Budget{MaxCostUSD: 10}, Window: time.Hour})
	lim := model.Limits{RequestsPerMinute: 100, TokensPerMinute: 10000}
	for i := 0; i < 4; i++ {
		if err := s.Acquire(context.Background(), "claude-sonnet-5", lim, 100); err != nil {
			t.Fatalf("acquire: %v", err)
		}
	}
	if _, _, err := s.Charge(core.Usage{Requests: 1, InputTokens: 90, CostUSD: 2.5}); err != nil {
		t.Fatalf("charge: %v", err)
	}
	out := s.Stats().String()
	for _, want := range []string{"shared quota", "$2.5000 of $10.0000", "$7.5000 left",
		"admission: 4 request(s)", "claude-sonnet-5"} {
		if !strings.Contains(out, want) {
			t.Errorf("the report does not mention %q:\n%s", want, out)
		}
	}
}

// --- helpers -------------------------------------------------------------

// counting wraps a store and counts the round trips made through it.
type counting struct {
	quota.Store
	draws atomic.Int64
}

func (c *counting) Draw(ctx context.Context, d quota.Draw) (quota.Grant, error) {
	c.draws.Add(1)
	return c.Store.Draw(ctx, d)
}

// failing is a store that is never reachable.
type failing struct{ err error }

func (f *failing) Draw(context.Context, quota.Draw) (quota.Grant, error) {
	return quota.Grant{}, f.err
}
func (f *failing) Return(context.Context, quota.Draw) error { return f.err }
func (f *failing) Charge(context.Context, quota.Charge) (quota.Ledger, error) {
	return quota.Ledger{}, f.err
}
func (f *failing) Ledger(context.Context, time.Duration) (quota.Ledger, error) {
	return quota.Ledger{}, f.err
}
func (f *failing) Models(context.Context) ([]quota.ModelState, error) { return nil, f.err }
func (f *failing) Close() error                                       { return nil }
