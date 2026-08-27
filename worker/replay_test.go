package worker_test

// A remote execution that cost nothing has to say so, and say which kind of
// nothing it was. The result cache's single-flight lease lives on the worker —
// it is the worker that holds the entry and the worker that waits for it — so
// without the receipt carrying the distinction home, a fleet's report would
// call a coalesced serve an ordinary replay and the lease would be invisible
// exactly where several processes make it matter most.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/task"
	"github.com/zionrubin/loom/worker"
)

func TestCoalescedServeRidesHomeFromAWorker(t *testing.T) {
	f := newFleet(t)

	bus := observe.NewBus()
	defer bus.Close()
	var mu sync.Mutex
	var seen []observe.Event
	bus.On(func(e observe.Event) {
		mu.Lock()
		seen = append(seen, e)
		mu.Unlock()
	})
	// The client under test is the one holding the bus: accounting a remote
	// completion is its job, not the queue's.
	client := worker.NewClient(worker.ClientConfig{
		Queue: f.q, Blobs: f.cas, Name: "client", Bus: bus,
		Backoff: 5 * time.Millisecond,
	})

	exec := newScripted()
	exec.coalesce()
	f.start("worker-1", f.q, exec)

	tk := task.Task{
		ID: "t1", Stage: "summarize", CacheKey: "ck",
		Input:    []core.Record{core.NewRecord("r1", map[string]any{"text": "hello"})},
		Envelope: task.Envelope{RunID: "run-1"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := client.Execute(ctx, tk)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !res.CacheHit {
		t.Error("a replayed result must come home as a cache hit")
	}
	if !res.Coalesced {
		t.Error("the receipt must carry which kind of hit it was")
	}
	if len(res.Output) != 1 || res.Output[0].String("summary") != "HELLO" {
		t.Errorf("output = %v, want the worker's records", res.Output)
	}

	mu.Lock()
	defer mu.Unlock()
	var hits []observe.Event
	for _, e := range seen {
		if e.Type == observe.CacheHit {
			hits = append(hits, e)
		}
	}
	// One event, of the type every existing handler already counts — the
	// distinction is an attribute on it, so a fleet does not silently stop
	// reporting cache hits to anybody watching for them.
	if len(hits) != 1 {
		t.Fatalf("published %d cache.hit events, want exactly 1", len(hits))
	}
	if !hits[0].Coalesced {
		t.Error("the hit must say it was coalesced: a fleet's report should read " +
			"the way a local run's does")
	}
	if hits[0].Latency != 7*time.Millisecond {
		t.Errorf("coalesced latency = %s, want the wait the worker reported", hits[0].Latency)
	}
}
