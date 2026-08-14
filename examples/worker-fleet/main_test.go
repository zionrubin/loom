package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/worker/filequeue"
)

// The example's headline claim, checked rather than printed: the same pipeline,
// executed by workers that are not this process, produces the same answers —
// and this process does not call a model at all.
//
// The workers here are goroutines rather than processes, because what this test
// is guarding is the example's *wiring*: that buildPipeline and fleetOptions
// are enough on both sides. The process boundary itself, and the kill, are
// covered exhaustively in worker_process_test.go at the repository root, which
// spawns real workers and SIGKILLs one mid-call.
func TestFleetProducesTheSameAnswersAsALocalRun(t *testing.T) {
	dir := t.TempDir()
	cfg := config{
		State:   filepath.Join(dir, "state"),
		Queue:   filepath.Join(dir, "queue"),
		Calls:   filepath.Join(dir, "fleet-calls.log"),
		Docs:    12,
		Slots:   2,
		Latency: time.Millisecond,
		Lease:   time.Second,
	}
	for _, d := range []string{cfg.State, cfg.Queue} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	local, err := runLocal(cfg, filepath.Join(dir, "local"))
	if err != nil {
		t.Fatalf("local run: %v", err)
	}
	if len(local.answers) != cfg.Docs {
		t.Fatalf("the local run produced %d records, want %d", len(local.answers), cfg.Docs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		w := cfg
		w.Name = fmt.Sprintf("worker-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := serve(ctx, w); err != nil && ctx.Err() == nil {
				t.Errorf("worker %s: %v", w.Name, err)
			}
		}()
		if !awaitFile(readyFile(cfg.Queue, w.Name), 30*time.Second) {
			t.Fatalf("%s never started serving", w.Name)
		}
	}
	defer func() { cancel(); wg.Wait() }()

	// The client's provider is a trap: if this process calls a model, execution
	// did not leave it and the example is not demonstrating what it says.
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(model.Request) (string, error) {
			return "", fmt.Errorf("the client process called a model")
		})); err != nil {
		t.Fatal(err)
	}

	q, err := filequeue.Open(cfg.Queue, filequeue.Options{LeaseTTL: cfg.Lease})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	defer q.Close()

	res, err := loom.Run(ctx, buildPipeline(cfg.Docs),
		append(fleetOptions(cfg, reg), loom.WithWorkerService(q), loom.WithWorkers(8))...)
	if err != nil {
		t.Fatalf("fleet run: %v", err)
	}

	if diff := compare(local.answers, answers(res)); diff != "" {
		t.Fatalf("the fleet's answers differ from a local run's:\n%s", diff)
	}
	if calls := countLines(cfg.Calls); calls != cfg.Docs {
		t.Fatalf("the fleet made %d model calls for %d documents, want one each",
			calls, cfg.Docs)
	}
	if res.Spent.Requests != cfg.Docs {
		t.Fatalf("the governor recorded %d requests, want %d — usage did not come "+
			"home with the results", res.Spent.Requests, cfg.Docs)
	}

	// The work really was spread: with three workers and twelve documents,
	// more than one process-equivalent claimed tasks.
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Expired != 0 {
		t.Fatalf("%d leases expired under live workers: the heartbeat is not keeping "+
			"up with the queue's TTL", stats.Expired)
	}
	if stats.Done != stats.Submitted {
		t.Fatalf("%d of %d tasks finished", stats.Done, stats.Submitted)
	}
}
