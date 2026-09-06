package loom_test

// The exit criterion for a shared quota, tested the only way it can honestly be
// tested: two real processes, one directory, and the same pipeline run both
// ways.
//
// Everything else in this repository's test suite can be checked in one address
// space, because everything else is one address space. This is not. The claim
// is "N processes hold one ceiling rather than N", and a test that used two
// goroutines to stand in for two processes would be checking that a mutex
// works. So the children are this test binary re-executed — the same trick
// worker_process_test.go and findings/distributed_test.go use, for the same
// reason — and the measurement is the control:
//
//	without the shared wallet, two $0.10 processes spend $0.20.
//	with it, they spend $0.10 between them.
//
// The second number alone would be a claim. The pair is a measurement.

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/quota"
)

// quotaChildEnv carries the spec to a child. Its presence is what turns this
// test binary into one of the processes sharing an account.
const quotaChildEnv = "LOOM_QUOTA_SPEC"

// quotaSpec is one process of the fleet: who to be, what ceiling to hold, and
// whether to hold it alone.
type quotaSpec struct {
	Name string `json:"name"`
	// Quota is the shared directory, or "" for the control: a process that
	// believes it owns the whole account, which is what every process was
	// before this feature.
	Quota string `json:"quota"`
	// Wallet is the ceiling, in dollars. In the control it is applied as the
	// run's own budget, so both arms are configured with the same number and
	// differ only in whether that number is shared.
	Wallet  float64 `json:"wallet"`
	Records int     `json:"records"`
	Out     string  `json:"out"`   // where this child writes what it spent
	Ready   string  `json:"ready"` // touched once this process is provisioned
	Go      string  `json:"go"`    // waited for, so both processes start together
}

type quotaOutcome struct {
	Name     string  `json:"name"`
	SpentUSD float64 `json:"spent_usd"`
	Requests int     `json:"requests"`
	Err      string  `json:"err"`
}

// TestQuotaChildProcess is the process a spawned child runs. It is a test
// function because a test binary has no other entry point; without a spec in
// its environment it does nothing at all.
func TestQuotaChildProcess(t *testing.T) {
	blob := os.Getenv(quotaChildEnv)
	if blob == "" {
		t.Skip("not a quota child")
	}
	var spec quotaSpec
	if err := json.Unmarshal([]byte(blob), &spec); err != nil {
		t.Fatalf("spec: %v", err)
	}

	reg := model.NewRegistry()
	mock := model.NewMock("mock",
		// A call that takes a measurable moment. Without it the first process
		// to start empties the wallet in microseconds and the second finds it
		// gone — which is correct behaviour and a useless measurement, because
		// what this test is about is two processes spending at once.
		model.WithLatency(3*time.Millisecond),
		model.WithHandler(func(model.Request) (string, error) {
			return "fixed-length answer", nil
		}))
	err := reg.Register(model.Info{
		ID: "mock-1", Provider: mock, Tier: model.TierFast,
		// Metered, so admission goes through the shared buckets too — but
		// generously, because what this test measures is the ceiling and a
		// bucket that made the children wait would only make it slower.
		Limits:  model.Limits{RequestsPerMinute: 100000, TokensPerMinute: 10000000},
		Pricing: model.Pricing{InputPerMTok: 1000, OutputPerMTok: 1000},
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}

	budget := core.Budget{MaxCostUSD: spec.Wallet}
	opts := []loom.Option{
		loom.WithRegistry(reg),
		// One task in flight per process, so the overrun a post-hoc charge
		// allows is one call per process rather than a number that depends on
		// scheduling.
		loom.WithWorkers(1),
	}
	if spec.Quota == "" {
		// The control: this process's own ceiling, held alone.
		opts = append(opts, loom.WithRunBudget(budget))
	} else {
		q, err := quota.Open(spec.Quota, quota.Options{})
		if err != nil {
			t.Fatalf("quota: %v", err)
		}
		defer q.Close()
		opts = append(opts,
			// Refreshed on every check: the point of the test is how quickly
			// one process notices the other's spending, and a second of
			// staleness at mock-provider speeds is thousands of calls.
			loom.WithQuotaConfig(quota.Config{Refresh: time.Nanosecond}),
			loom.WithSharedQuota(q, budget, 0))
	}

	// Provisioned, and now waiting for its sibling: the fleet starts spending
	// together or the arithmetic below is about scheduling rather than about
	// the wallet.
	if err := os.WriteFile(spec.Ready, []byte(spec.Name), 0o644); err != nil {
		t.Fatalf("ready: %v", err)
	}
	if !waitForPath(spec.Go, 60*time.Second) {
		t.Fatal("the start signal never arrived")
	}

	res, runErr := loom.Run(context.Background(), walletPipeline(spec.Name, spec.Records), opts...)
	out := quotaOutcome{Name: spec.Name}
	if res != nil {
		out.SpentUSD, out.Requests = res.Spent.CostUSD, res.Spent.Requests
	}
	if runErr != nil {
		out.Err = runErr.Error()
	}
	blob2, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("outcome: %v", err)
	}
	if err := os.WriteFile(spec.Out, blob2, 0o644); err != nil {
		t.Fatalf("write outcome: %v", err)
	}
}

// Two processes, one account. The control arm is the same two processes without
// the shared directory, and it is what turns "the wallet held" into a number
// somebody can check.
func TestOneWalletAcrossTwoProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	const (
		wallet  = 0.10
		records = 200
	)

	// Control: each process holds the ceiling alone, which is what every
	// process did before this feature existed.
	alone := runQuotaChildren(t, "", wallet, records)
	// Shared: the same two processes, one directory between them.
	dir := t.TempDir()
	together := runQuotaChildren(t, dir, wallet, records)

	aloneTotal := alone[0].SpentUSD + alone[1].SpentUSD
	sharedTotal := together[0].SpentUSD + together[1].SpentUSD
	t.Logf("ceiling $%.4f: alone $%.4f (%d+%d calls), shared $%.4f (%d+%d calls)",
		wallet, aloneTotal, alone[0].Requests, alone[1].Requests,
		sharedTotal, together[0].Requests, together[1].Requests)

	// The bug, measured. Two processes each stopping at $0.10 spend $0.20.
	if aloneTotal < 1.8*wallet {
		t.Fatalf("the control spent $%.4f against two $%.4f ceilings; it was supposed "+
			"to demonstrate the ceiling being multiplied by processes", aloneTotal, wallet)
	}
	// The fix. The overrun a post-hoc charge allows is bounded by the tasks in
	// flight across the fleet — one per process here — so the honest bound is
	// the ceiling plus a call from each.
	overrun := 2 * (sharedTotal / float64(together[0].Requests+together[1].Requests))
	if sharedTotal > wallet+overrun {
		t.Fatalf("two processes sharing a $%.4f wallet spent $%.4f, over the $%.4f "+
			"that in-flight work can account for", wallet, sharedTotal, wallet+overrun)
	}
	if sharedTotal < 0.5*wallet {
		t.Fatalf("two processes sharing a $%.4f wallet spent only $%.4f; the ceiling "+
			"is not being shared, it is being divided", wallet, sharedTotal)
	}

	// Both processes have to have participated. One process doing all the work
	// while the other found the wallet empty on its first check would pass every
	// assertion above and would not be a fleet.
	for _, o := range together {
		if o.Requests == 0 {
			t.Errorf("%s made no calls at all: the wallet was spent before it started", o.Name)
		}
		if o.Err == "" {
			t.Errorf("%s finished all %d records inside a wallet that could not "+
				"cover them; nothing stopped it", o.Name, records)
		} else if !strings.Contains(o.Err, "budget") {
			t.Errorf("%s stopped for a reason other than the ceiling: %s", o.Name, o.Err)
		}
	}

	// And the ledger the two of them wrote agrees with what they report having
	// spent, which is the record an operator would actually read.
	q, err := quota.Open(dir, quota.Options{})
	if err != nil {
		t.Fatalf("open quota: %v", err)
	}
	defer q.Close()
	l, err := q.Ledger(context.Background(), 0)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if diff := l.Total.CostUSD - sharedTotal; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("the shared ledger holds $%.6f and the two processes report $%.6f",
			l.Total.CostUSD, sharedTotal)
	}
	if l.Charges != together[0].Requests+together[1].Requests {
		t.Errorf("the ledger folded %d charges for %d calls", l.Charges,
			together[0].Requests+together[1].Requests)
	}
	// The buckets, too: one bucket admitted every call both processes made.
	ms, err := q.Models(context.Background())
	if err != nil {
		t.Fatalf("models: %v", err)
	}
	if len(ms) != 1 {
		t.Fatalf("two processes produced %d buckets, want the one they share: %+v", len(ms), ms)
	}
	if ms[0].Admitted < int64(l.Charges) {
		t.Errorf("the shared bucket admitted %d requests for %d calls", ms[0].Admitted, l.Charges)
	}
}

// runQuotaChildren spawns two processes and waits for both to write what they
// spent.
func runQuotaChildren(t *testing.T, quotaDir string, wallet float64, records int) []quotaOutcome {
	t.Helper()
	outDir := t.TempDir()
	names := []string{"proc-a", "proc-b"}
	cmds := make([]*exec.Cmd, len(names))
	start := filepath.Join(outDir, "go")

	for i, name := range names {
		spec := quotaSpec{
			Name: name, Quota: quotaDir, Wallet: wallet, Records: records,
			Out:   filepath.Join(outDir, name+".json"),
			Ready: filepath.Join(outDir, "ready-"+name),
			Go:    start,
		}
		blob, err := json.Marshal(spec)
		if err != nil {
			t.Fatalf("spec: %v", err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestQuotaChildProcess$", "-test.timeout=120s")
		cmd.Env = append(os.Environ(), quotaChildEnv+"="+string(blob))
		cmd.Stdout, cmd.Stderr = &strings.Builder{}, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		cmds[i] = cmd
	}
	for _, name := range names {
		if !waitForPath(filepath.Join(outDir, "ready-"+name), 60*time.Second) {
			t.Fatalf("%s never became ready", name)
		}
	}
	if err := os.WriteFile(start, []byte("go"), 0o644); err != nil {
		t.Fatalf("start signal: %v", err)
	}
	for i, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("%s exited: %v\n%s", names[i], err, cmd.Stdout.(*strings.Builder))
		}
	}

	out := make([]quotaOutcome, len(names))
	for i, name := range names {
		blob, err := os.ReadFile(filepath.Join(outDir, name+".json"))
		if err != nil {
			t.Fatalf("%s wrote no outcome: %v", name, err)
		}
		if err := json.Unmarshal(blob, &out[i]); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	return out
}

// waitForPath polls until path exists.
func waitForPath(path string, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}
