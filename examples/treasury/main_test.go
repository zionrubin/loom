package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/quota"
)

// The example's headline claim, checked rather than printed: two runs that each
// hold the ceiling alone spend it twice, and the same two sharing it spend it
// once.
//
// The process boundary itself is covered in the repository root's
// quota_process_test.go, which spawns real processes. What this asserts is the
// wiring above it: that loom.WithSharedQuota makes one run's spending visible
// to another's ceiling.
func TestOneCeilingHeldTogether(t *testing.T) {
	ctx := context.Background()
	const wallet = 0.02

	alone := runRound(t, ctx, "", "", wallet, false)
	dir := filepath.Join(t.TempDir(), "quota")
	shared := runRound(t, ctx, dir, "", wallet, true)

	aloneTotal := alone[0].SpentUSD + alone[1].SpentUSD
	sharedTotal := shared[0].SpentUSD + shared[1].SpentUSD
	t.Logf("ceiling $%.4f: alone $%.4f, shared $%.4f", wallet, aloneTotal, sharedTotal)

	if aloneTotal < 1.8*wallet {
		t.Fatalf("two runs each holding a $%.4f ceiling spent $%.4f between them; "+
			"the example's baseline is that they spend it twice", wallet, aloneTotal)
	}
	// The overrun is bounded by the calls in flight when the ceiling was
	// crossed: two workers in each of two runs.
	perCall := sharedTotal / float64(shared[0].Calls+shared[1].Calls)
	if sharedTotal > wallet+4*perCall {
		t.Fatalf("two runs sharing a $%.4f ceiling spent $%.4f, more than the "+
			"in-flight work can account for", wallet, sharedTotal)
	}
	for _, r := range shared {
		if r.Calls == 0 {
			t.Errorf("%s made no calls: it found the wallet empty before it started", r.Name)
		}
		if r.Stopped == "" {
			t.Errorf("%s was never stopped by the shared ceiling", r.Name)
		}
	}
}

// The service backend is the one a fleet spanning hosts uses, and it has to
// behave exactly like the directory or the example is demonstrating two things
// rather than one.
func TestTheServiceBackendHoldsTheSameCeiling(t *testing.T) {
	ctx := context.Background()
	const wallet = 0.02

	d, err := quota.Open(filepath.Join(t.TempDir(), "quota"), quota.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()
	srv := httptest.NewServer(quota.Handler(d))
	defer srv.Close()

	shared := runRound(t, ctx, "", srv.URL, wallet, true)
	total := shared[0].SpentUSD + shared[1].SpentUSD
	perCall := total / float64(shared[0].Calls+shared[1].Calls)
	if total > wallet+4*perCall {
		t.Fatalf("two runs sharing a $%.4f ceiling over HTTP spent $%.4f", wallet, total)
	}
	// And the service's own store is what recorded it.
	l, err := d.Ledger(ctx, 0)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if diff := l.Total.CostUSD - total; diff > 1e-9 || diff < -1e-9 {
		t.Errorf("the service's ledger holds $%.6f and the runs report $%.6f",
			l.Total.CostUSD, total)
	}
}

// runRound runs two processes' worth of work in this one, which is enough to
// check the wiring: two Runs with two quota handles are what two processes are,
// minus the address space.
//
// They run at the same time, because a fleet is processes spending at once. Run
// one after the other, the first would empty the wallet and the second would
// find it gone — correct behaviour, and a measurement about scheduling rather
// than about the ceiling.
func runRound(t *testing.T, ctx context.Context, dir, url string, wallet float64, shared bool) []report {
	t.Helper()
	out := t.TempDir()
	names := []string{"proc-1", "proc-2"}
	errs := make([]error, len(names))

	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = runChild(ctx, config{
				Name: name, Dir: dir, URL: url,
				Out:    filepath.Join(out, name+".json"),
				Shared: shared, Wallet: wallet, Latency: 2 * time.Millisecond, RPM: 6000,
			})
		}()
	}
	wg.Wait()

	reports := make([]report, len(names))
	for i, name := range names {
		if errs[i] != nil {
			t.Fatalf("%s: %v", name, errs[i])
		}
		blob, err := os.ReadFile(filepath.Join(out, name+".json"))
		if err != nil {
			t.Fatalf("%s wrote no report: %v", name, err)
		}
		if err := json.Unmarshal(blob, &reports[i]); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	return reports
}
