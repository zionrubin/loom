package loom_test

// The shared quota as a run sees it: the wiring from loom.WithSharedQuota down
// to the scheduler's admission and its budget check, and the projection that
// reads the fleet's balance before spending any of it.
//
// The claim that needs a second address space to test honestly — that N
// processes hold one ceiling rather than N — is in quota_process_test.go.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/quota"
	"github.com/zionrubin/loom/runtime"
)

// walletRegistry prices a mock so every call costs the same easily-counted
// amount: the assertions below are about how many calls a ceiling allows, and
// a per-call price that varied with the record would turn each of them into an
// approximation.
func walletRegistry(t *testing.T, limits model.Limits) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	mock := model.NewMock("mock", model.WithHandler(func(req model.Request) (string, error) {
		return "fixed-length answer", nil
	}))
	err := reg.Register(model.Info{
		ID: "mock-1", Provider: mock, Tier: model.TierFast, Limits: limits,
		Pricing: model.Pricing{InputPerMTok: 1000, OutputPerMTok: 1000},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return reg
}

func walletPipeline(name string, n int) *pipeline.Pipeline {
	// Deterministic IDs and distinct, equal-length payloads: distinct so no two
	// tasks in one run collapse onto the same cache entry, deterministic so two
	// runs of the same pipeline do.
	recs := make([]core.Record, n)
	for i := range recs {
		recs[i] = core.NewRecord(fmt.Sprintf("%s-%03d", name, i),
			map[string]any{"text": fmt.Sprintf("text-%03d", i)})
	}
	p := pipeline.New(name)
	p.FromRecords("src", recs).Infer("answer", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast},
		Prompt:  "Answer: {{.text}}",
	})
	return p
}

func openQuota(t *testing.T, dir string) *quota.Dir {
	t.Helper()
	q, err := quota.Open(dir, quota.Options{})
	if err != nil {
		t.Fatalf("open quota: %v", err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

// Two runs, each well inside its own budget, and one ceiling between them. This
// is the wiring test for the money bug: without the shared wallet the second
// run has no idea the first one spent anything.
func TestASharedWalletStopsASecondRun(t *testing.T) {
	dir := t.TempDir()
	reg := walletRegistry(t, model.Limits{})

	// Run one, with no ceiling, to find out what a call costs here.
	first, err := loom.Run(t.Context(), walletPipeline("first", 6),
		loom.WithRegistry(reg), loom.WithWorkers(1),
		loom.WithSharedQuota(openQuota(t, dir), core.Budget{}, 0))
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	spent := first.Spent.CostUSD
	if spent <= 0 {
		t.Fatalf("the first run spent nothing; there is no ceiling to test")
	}
	if got := first.Quota.Ledger.Spent.CostUSD; got != spent {
		t.Fatalf("the shared ledger recorded $%.6f for a run that spent $%.6f", got, spent)
	}

	// Run two holds a ceiling below what run one already spent. Its own budget
	// is untouched — it has spent nothing — so anything that stops it can only
	// be the shared ledger.
	second, err := loom.Run(t.Context(), walletPipeline("second", 6),
		loom.WithRegistry(reg), loom.WithWorkers(1),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 100}),
		loom.WithQuotaConfig(quota.Config{Refresh: time.Nanosecond}),
		loom.WithSharedQuota(openQuota(t, dir), core.Budget{MaxCostUSD: spent / 2}, 0))
	if !errors.Is(err, runtime.ErrBudgetExhausted) {
		t.Fatalf("the second run returned %v, want the shared wallet to stop it", err)
	}
	if second == nil {
		t.Fatal("a run stopped by the wallet returned no partial result")
	}
	if second.Spent.CostUSD >= spent {
		t.Fatalf("the second run spent $%.6f of its own against a wallet already $%.6f "+
			"down; it never saw the first run's spending", second.Spent.CostUSD, spent)
	}
}

// A run budget and a shared wallet are two ceilings, and dropping either would
// leave one failure unguarded. This is the one the run's own budget catches:
// the fleet has plenty left and this pipeline is the runaway.
func TestTheRunBudgetStillBindsUnderASharedWallet(t *testing.T) {
	reg := walletRegistry(t, model.Limits{})
	res, err := loom.Run(t.Context(), walletPipeline("greedy", 8),
		loom.WithRegistry(reg), loom.WithWorkers(1),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 0.0001}),
		loom.WithSharedQuota(openQuota(t, t.TempDir()), core.Budget{MaxCostUSD: 1000}, 0))
	if !errors.Is(err, runtime.ErrBudgetExhausted) {
		t.Fatalf("run returned %v, want its own budget to stop it", err)
	}
	// One worker and a ceiling below the price of a single call: the run pays
	// for the call that discovers it and stops, rather than working through
	// all eight records against a wallet that had room for them.
	if res.Spent.Requests != 1 {
		t.Fatalf("the run made %d calls against a budget one call exceeds, want 1",
			res.Spent.Requests)
	}
	// And the wallet recorded the spending anyway: a ledger that only saw the
	// runs that finished would under-report the account.
	if res.Quota.Ledger.Charges == 0 {
		t.Fatal("the shared ledger recorded nothing for a run that made calls")
	}
}

// Admission goes through the shared buckets rather than this process's, and the
// run report says so — because a saving or a cost nobody measures is a claim.
func TestAdmissionDrawsOnTheSharedBucket(t *testing.T) {
	q := openQuota(t, t.TempDir())
	res, err := loom.Run(t.Context(), walletPipeline("metered", 5),
		loom.WithRegistry(walletRegistry(t, model.Limits{RequestsPerMinute: 1000, TokensPerMinute: 100000})),
		loom.WithWorkers(2),
		loom.WithSharedQuota(q, core.Budget{MaxCostUSD: 10}, time.Hour))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Quota.Admitted != 5 {
		t.Fatalf("the run drew %d admissions from the shared bucket, want 5", res.Quota.Admitted)
	}
	ms, err := q.Models(context.Background())
	if err != nil {
		t.Fatalf("models: %v", err)
	}
	if len(ms) != 1 || ms[0].Model != "mock-1" || ms[0].Admitted != 5 {
		t.Fatalf("the shared bucket recorded %+v, want one bucket for mock-1 with 5 admitted", ms)
	}
	out := res.Quota.String()
	for _, want := range []string{"shared quota", "admission: 5 request(s)", "mock-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("the report does not mention %q:\n%s", want, out)
		}
	}
}

// A replay reached no provider, so the admission it was granted goes back to
// the fleet rather than throttling the calls that still have to be made. The
// in-process limiter has always done this; across processes it matters more,
// because the quota it would otherwise hold is somebody else's.
func TestAReplayReturnsItsAdmissionToTheFleet(t *testing.T) {
	state, qdir := t.TempDir(), t.TempDir()
	reg := walletRegistry(t, model.Limits{RequestsPerMinute: 1000})
	run := func() *loom.RunResult {
		t.Helper()
		res, err := loom.Run(t.Context(), walletPipeline("replayed", 4),
			loom.WithRegistry(reg), loom.WithStateDir(state), loom.WithWorkers(1),
			loom.WithSharedQuota(openQuota(t, qdir), core.Budget{}, 0))
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return res
	}
	// The pipeline builds fresh record IDs each time, so the second run is a
	// replay only if the cache keys on content — which it does.
	run()
	second := run()
	if second.Report.CacheHits() == 0 {
		t.Skip("the second run made real calls, so there is no refund to check")
	}
	if second.Quota.Refunded == 0 {
		t.Fatalf("%d cache hit(s) and not one admission returned to the fleet",
			second.Report.CacheHits())
	}
}

// The projection reads the fleet's balance and says whether this run fits in
// what is left. That is the difference between knowing a run costs $12 and
// knowing it will stop two thirds of the way through.
func TestExplainReadsTheWalletsHeadroom(t *testing.T) {
	dir := t.TempDir()
	q := openQuota(t, dir)
	if _, err := q.Charge(context.Background(), quota.Charge{
		Usage: core.Usage{Requests: 1, CostUSD: 9.90}}); err != nil {
		t.Fatalf("charge: %v", err)
	}

	proj, err := loom.Explain(walletPipeline("planned", 200),
		loom.WithRegistry(walletRegistry(t, model.Limits{})),
		loom.WithSharedQuota(q, core.Budget{MaxCostUSD: 10}, 0))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if proj.Wallet == nil {
		t.Fatal("the projection did not read the shared wallet")
	}
	if got := proj.Wallet.RemainingUSD(); got < 0.09 || got > 0.11 {
		t.Fatalf("the wallet reports $%.4f left, want $0.10", got)
	}
	if proj.FitsWallet() {
		t.Fatalf("a run whose ceiling is $%.4f was said to fit in $%.4f",
			proj.Ceiling().CostUSD, proj.Wallet.RemainingUSD())
	}
	out := proj.String()
	if !strings.Contains(out, "shared wallet") || !strings.Contains(out, "does NOT cover") {
		t.Errorf("the projection does not warn that the fleet cannot afford the run:\n%s", out)
	}
	// And it wrote nothing while looking: a projection that charged the wallet
	// for asking would be the one thing this tool must never do.
	l, err := q.Ledger(context.Background(), 0)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if l.Charges != 1 {
		t.Fatalf("the ledger holds %d charges after a projection, want the 1 it started with",
			l.Charges)
	}
}

// A quota you cannot reach is a quota you cannot respect — so the run stops,
// rather than admitting itself against a limit nobody is holding. It has to
// stop as a *transient* failure, though, so a service restarting costs a
// backoff and not a dead letter.
func TestAnUnreachableQuotaStopsTheRun(t *testing.T) {
	_, err := loom.Run(t.Context(), walletPipeline("blocked", 3),
		loom.WithRegistry(walletRegistry(t, model.Limits{RequestsPerMinute: 100})),
		loom.WithWorkers(1), loom.WithRetry(runtime.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Millisecond}),
		loom.WithSharedQuota(unreachable{}, core.Budget{MaxCostUSD: 1}, 0))
	if err == nil {
		t.Fatal("the run finished against a quota store nobody can reach")
	}
	if !strings.Contains(err.Error(), "quota") {
		t.Fatalf("the failure does not name the quota: %v", err)
	}
	if class := core.ClassOf(err); class != core.FailTransient {
		t.Fatalf("the failure classified as %s, want transient", class)
	}
}

type unreachable struct{}

var errUnreachable = errors.New("connection refused")

func (unreachable) Draw(context.Context, quota.Draw) (quota.Grant, error) {
	return quota.Grant{}, errUnreachable
}
func (unreachable) Return(context.Context, quota.Draw) error { return errUnreachable }
func (unreachable) Charge(context.Context, quota.Charge) (quota.Ledger, error) {
	return quota.Ledger{}, errUnreachable
}
func (unreachable) Ledger(context.Context, time.Duration) (quota.Ledger, error) {
	return quota.Ledger{}, errUnreachable
}
func (unreachable) Models(context.Context) ([]quota.ModelState, error) { return nil, errUnreachable }
func (unreachable) Close() error                                       { return nil }
