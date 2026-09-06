// Package quotatest is the conformance suite every quota.Store must pass: one
// set of tests, run against a shared directory, against the same directory
// behind an HTTP service, and against whatever anybody writes next.
//
// It exists for the reason worker/queuetest exists. "The quota store is
// replaceable behind a clean interface" is a claim, and the only way to hold an
// interface to a claim like that is to have two implementations and one
// written-down definition of what they both owe. Everything quota.Shared
// assumes is a test here rather than a paragraph: that a bucket is drawn down
// by whoever asks first, that a batch is served from the front and reports the
// wait for the rest, that a return is exactly the inverse of the draw that
// produced it, that two handles on one store see each other's spend, and that a
// bucket refills on its own with nobody running.
//
// A new backend is finished when this passes:
//
//	func TestConformance(t *testing.T) {
//	    quotatest.Run(t, func(t *testing.T) (quota.Store, quotatest.Clock) {
//	        return open(t)
//	    })
//	}
package quotatest

import (
	"context"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/quota"
)

// Clock advances a store's idea of the time. Half of what a quota store owes
// its callers is only observable across a minute — a bucket that refills, a
// window that rolls — and a suite that waited those out would take minutes to
// say so. A backend that cannot be driven this way returns nil and skips the
// tests that need one, which is itself worth knowing.
type Clock interface {
	Advance(d time.Duration)
	Now() time.Time
}

// Open builds an empty store, plus the clock that drives it (or nil). Each
// call must produce a store with no history; cleaning it up is the
// implementation's, and t.Cleanup is the natural place.
type Open func(t *testing.T) (quota.Store, Clock)

// Limits the suite draws against: small enough to empty in a test, shaped like
// a real provider's.
var Limits = model.Limits{RequestsPerMinute: 6, TokensPerMinute: 600}

// Run executes the conformance suite against a store implementation.
func Run(t *testing.T, open Open) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(t *testing.T, s quota.Store, c Clock)
	}{
		{"AFreshBucketIsFull", testFreshBucketIsFull},
		{"ABatchIsServedFromTheFront", testBatchFromTheFront},
		{"RequestsAndTokensBothBind", testBothLimitsBind},
		{"AnUnmeteredModelIsAlwaysAdmitted", testUnmetered},
		{"AReturnIsTheInverseOfItsDraw", testReturnInverse},
		{"AReturnNeverOverfillsTheBucket", testReturnClamps},
		{"AnOversizedRequestIsAdmittedAtAFullBucket", testOversized},
		{"ADeniedDrawSaysHowLongToWait", testWaitIsReported},
		{"ABucketRefillsWithNobodyRunning", testRefill},
		{"SpendIsSharedBetweenHandles", testSpendShared},
		{"AWindowSumsOnlyWhatFellInsideIt", testWindow},
		{"AWindowLongerThanRetentionIsRefused", testWindowTooLong},
		{"ModelsReportsTheFleetsTraffic", testModelsReport},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, c := open(t)
			tc.fn(t, s, c)
		})
	}
}

func drawN(t *testing.T, s quota.Store, n, tokens int) quota.Grant {
	t.Helper()
	est := make([]int, n)
	for i := range est {
		est[i] = tokens
	}
	g, err := s.Draw(context.Background(), quota.Draw{Model: "m", Limits: Limits, Tokens: est})
	if err != nil {
		t.Fatalf("draw: %v", err)
	}
	return g
}

func returnN(t *testing.T, s quota.Store, n, tokens int) {
	t.Helper()
	est := make([]int, n)
	for i := range est {
		est[i] = tokens
	}
	if err := s.Return(context.Background(), quota.Draw{Model: "m", Limits: Limits, Tokens: est}); err != nil {
		t.Fatalf("return: %v", err)
	}
}

// A store nobody has drawn on holds the whole minute's allowance. Starting
// empty would make the first process of a fleet wait for a bucket it is the
// only claimant of.
func testFreshBucketIsFull(t *testing.T, s quota.Store, _ Clock) {
	if g := drawN(t, s, 6, 10); g.Admitted != 6 {
		t.Fatalf("a fresh bucket admitted %d of 6 requests, want all", g.Admitted)
	}
	if g := drawN(t, s, 1, 10); g.Admitted != 0 {
		t.Fatalf("the seventh request was admitted against a limit of 6")
	}
}

// The batch exists so a process can ask one question for a hundred waiting
// tasks, which is only useful if a partial answer means "the front of your
// queue goes now". A store that admitted an arbitrary subset would leave the
// caller unable to say which of its tasks may proceed.
func testBatchFromTheFront(t *testing.T, s quota.Store, _ Clock) {
	g := drawN(t, s, 10, 10)
	if g.Admitted != 6 {
		t.Fatalf("a batch of 10 against a limit of 6 admitted %d, want 6", g.Admitted)
	}
	if g.Wait <= 0 {
		t.Fatalf("a partial grant reported no wait; the caller has nothing to sleep on")
	}
}

// Two limits, and either one of them binds. A store that enforced only the
// requests bucket would let a fleet of large prompts sail past a tokens/min
// limit that is the one providers actually meter hardest.
func testBothLimitsBind(t *testing.T, s quota.Store, _ Clock) {
	// 600 tokens/min, six requests/min: three requests of 200 tokens empty the
	// token bucket while the request bucket still has three left.
	g := drawN(t, s, 6, 200)
	if g.Admitted != 3 {
		t.Fatalf("admitted %d requests of 200 tokens against 600 tokens/min, want 3", g.Admitted)
	}
}

// A model with no stated limits is unlimited, and unlimited is unlimited in
// every process. Making the fleet agree about it would be round trips spent
// agreeing that there is nothing to agree about.
func testUnmetered(t *testing.T, s quota.Store, _ Clock) {
	g, err := s.Draw(context.Background(), quota.Draw{
		Model: "free", Limits: model.Limits{}, Tokens: []int{1, 2, 3, 4, 5}})
	if err != nil {
		t.Fatalf("draw: %v", err)
	}
	if g.Admitted != 5 {
		t.Fatalf("an unmetered model admitted %d of 5, want all", g.Admitted)
	}
}

// A refund that does not match its draw is a leak in one direction or a licence
// to overrun in the other. This is the test that says the two are the same
// arithmetic run backwards.
func testReturnInverse(t *testing.T, s quota.Store, _ Clock) {
	drawN(t, s, 6, 100)
	if g := drawN(t, s, 1, 100); g.Admitted != 0 {
		t.Fatal("the bucket should be empty")
	}
	returnN(t, s, 2, 100)
	if g := drawN(t, s, 3, 100); g.Admitted != 2 {
		t.Fatalf("after returning 2, the bucket admitted %d, want exactly 2", g.Admitted)
	}
}

// A return of something never drawn — or one racing a refill — must not lift a
// bucket above the minute it stands for. Otherwise a fleet that replays from
// cache could hand itself burst capacity the provider never offered.
func testReturnClamps(t *testing.T, s quota.Store, _ Clock) {
	returnN(t, s, 20, 100)
	if g := drawN(t, s, 8, 10); g.Admitted != 6 {
		t.Fatalf("after 20 spurious returns the bucket admitted %d, want the cap of 6", g.Admitted)
	}
}

// A request larger than the whole minute's token allowance has to be admitted
// at a full bucket or it is admitted never — and "never" is a pipeline that
// hangs on its first record rather than one that reports a limit it cannot fit.
func testOversized(t *testing.T, s quota.Store, _ Clock) {
	g, err := s.Draw(context.Background(), quota.Draw{
		Model: "m", Limits: Limits, Tokens: []int{5000}})
	if err != nil {
		t.Fatalf("draw: %v", err)
	}
	if g.Admitted != 1 {
		t.Fatal("a request larger than the token bucket was never admitted")
	}
	if g2 := drawN(t, s, 1, 10); g2.Admitted != 0 {
		t.Fatal("an oversized request should have drawn the whole token bucket")
	}
}

// The wait is the whole reason a store may answer "no": the caller has to know
// how long to sleep, and a store that made it guess would turn every contended
// bucket into a spin.
func testWaitIsReported(t *testing.T, s quota.Store, _ Clock) {
	drawN(t, s, 6, 10)
	g := drawN(t, s, 1, 10)
	if g.Admitted != 0 {
		t.Fatal("the bucket should be empty")
	}
	// Six requests a minute is one every ten seconds.
	if g.Wait < 5*time.Second || g.Wait > 15*time.Second {
		t.Fatalf("wait for one of 6 requests/min was %s, want about 10s", g.Wait)
	}
}

// Refill is computed rather than scheduled: nothing has to be running for a
// bucket to fill up, which is what makes an idle fleet's first request free
// instead of throttled by whatever it did yesterday.
func testRefill(t *testing.T, s quota.Store, c Clock) {
	if c == nil {
		t.Skip("backend has no injectable clock")
	}
	drawN(t, s, 6, 10)
	if g := drawN(t, s, 1, 10); g.Admitted != 0 {
		t.Fatal("the bucket should be empty")
	}
	c.Advance(30 * time.Second)
	// Half a minute at six a minute is three requests back.
	g := drawN(t, s, 5, 10)
	if g.Admitted != 3 {
		t.Fatalf("after 30s the bucket admitted %d, want 3", g.Admitted)
	}
}

// The point of the whole package: what one handle spends, another sees. Two
// handles on one store are what two processes are.
func testSpendShared(t *testing.T, s quota.Store, _ Clock) {
	ctx := context.Background()
	spend := core.Usage{InputTokens: 100, OutputTokens: 20, Requests: 1, CostUSD: 0.25}
	if _, err := s.Charge(ctx, quota.Charge{Usage: spend}); err != nil {
		t.Fatalf("charge: %v", err)
	}
	if _, err := s.Charge(ctx, quota.Charge{Usage: spend}); err != nil {
		t.Fatalf("charge: %v", err)
	}
	l, err := s.Ledger(ctx, 0)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if l.Charges != 2 {
		t.Fatalf("ledger folded %d charges, want 2", l.Charges)
	}
	if got, want := l.Spent.CostUSD, 0.50; got < want-1e-9 || got > want+1e-9 {
		t.Fatalf("ledger reports $%.4f spent, want $%.4f", got, want)
	}
	if l.Spent.TotalTokens() != 240 {
		t.Fatalf("ledger reports %d tokens, want 240", l.Spent.TotalTokens())
	}
	if l.Total.CostUSD != l.Spent.CostUSD {
		t.Fatalf("with no window, Spent ($%.4f) and Total ($%.4f) must agree",
			l.Spent.CostUSD, l.Total.CostUSD)
	}
}

// A window is what makes a wallet a daily budget rather than a pot that empties
// once. Spend that has fallen out of it is still in the all-time total, because
// a ledger that forgot money would be a ledger nobody could audit.
func testWindow(t *testing.T, s quota.Store, c Clock) {
	if c == nil {
		t.Skip("backend has no injectable clock")
	}
	ctx := context.Background()
	old := core.Usage{Requests: 1, CostUSD: 4}
	if _, err := s.Charge(ctx, quota.Charge{Usage: old}); err != nil {
		t.Fatalf("charge: %v", err)
	}
	c.Advance(6 * time.Hour)
	recent := core.Usage{Requests: 1, CostUSD: 1}
	l, err := s.Charge(ctx, quota.Charge{Usage: recent, Window: 2 * time.Hour})
	if err != nil {
		t.Fatalf("charge: %v", err)
	}
	if got := l.Spent.CostUSD; got < 0.999 || got > 1.001 {
		t.Fatalf("a 2h window reports $%.4f, want $1 — the $4 fell outside it", got)
	}
	if got := l.Total.CostUSD; got < 4.999 || got > 5.001 {
		t.Fatalf("all-time total is $%.4f, want $5 — a window must not erase history", got)
	}
}

// A window the store cannot answer has to be refused rather than silently
// answered short. A daily budget quietly evaluated over an hour is a ceiling
// nobody would notice was wrong.
func testWindowTooLong(t *testing.T, s quota.Store, _ Clock) {
	if _, err := s.Ledger(context.Background(), quota.Retention+time.Hour); err == nil {
		t.Fatal("a window longer than the retained history was accepted")
	}
}

// The buckets are the fleet's, so what they did is the fleet's number: an
// operator asking "are we actually contending for this model" wants the count
// of requests turned away, not this process's share of it.
func testModelsReport(t *testing.T, s quota.Store, _ Clock) {
	drawN(t, s, 8, 10) // six admitted, two deferred
	returnN(t, s, 1, 10)
	ms, err := s.Models(context.Background())
	if err != nil {
		t.Fatalf("models: %v", err)
	}
	var found bool
	for _, m := range ms {
		if m.Model != "m" {
			continue
		}
		found = true
		if m.Admitted != 6 {
			t.Errorf("admitted %d, want 6", m.Admitted)
		}
		if m.Deferred != 2 {
			t.Errorf("deferred %d, want 2", m.Deferred)
		}
		if m.Returned != 1 {
			t.Errorf("returned %d, want 1", m.Returned)
		}
	}
	if !found {
		t.Fatalf("Models did not report the bucket that was drawn on: %+v", ms)
	}
}
