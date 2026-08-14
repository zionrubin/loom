package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/delta"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/worker/filequeue"
)

// The example's headline claim, checked rather than printed: a session carried
// across rounds by a fleet produces the same answers as one process doing the
// whole thing, and the rounds after the first cost the turn rather than the
// transcript.
//
// The workers here are goroutines rather than processes, because what this test
// guards is the example's *wiring* — that sessionPipeline and serve are enough
// on both sides. The process boundary and the kill are covered at the
// repository root, in continuation_process_test.go, which spawns real workers
// and SIGKILLs the one holding the state.
func TestSessionOnAFleetMatchesOneProcess(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}
	calls := filepath.Join(dir, "calls.log")

	cas, err := store.NewCAS(filepath.Join(state, "cas"))
	if err != nil {
		t.Fatal(err)
	}
	chain, err := delta.NewChain(cas, delta.Tags{}, sessionKey)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := chain.Root(transcript(30)...)
	if err != nil {
		t.Fatal(err)
	}

	q, err := filequeue.Open(filepath.Join(dir, "queue"), filequeue.Options{LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	// One worker, in this process but on the other side of the queue.
	wreg := model.NewRegistry()
	if err := digestMock(wreg, calls, "w1", 0); err != nil {
		t.Fatal(err)
	}
	w, err := loom.NewWorker(sessionPipeline(),
		loom.WithRegistry(wreg), loom.WithStateDir(state),
		loom.WithWorkerService(q), loom.WithWorkerName("w1"),
		loom.WithWorkerLease(time.Second), loom.WithWorkers(2),
		loom.WithDeltaPolicy(delta.Policy{Verify: 1}))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = w.Run(ctx) }()
	defer func() { stop(); wg.Wait() }()

	// A client whose provider fails if it is ever called.
	client := model.NewRegistry()
	if _, err := model.RegisterMock(client, "mock-fast", model.TierFast,
		model.WithHandler(func(model.Request) (string, error) {
			t.Error("the client executed a task itself")
			return "", nil
		})); err != nil {
		t.Fatal(err)
	}

	const rounds = 4
	var refs []delta.Ref
	var answers []string
	for n := 1; n <= rounds; n++ {
		if ref, err = chain.Append(ref, nextTurn(n)); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
		res, err := loom.Run(ctx, sessionPipeline(),
			loom.WithRegistry(client), loom.WithStateDir(state),
			loom.WithWorkerService(q), loom.WithWorkerWait(60*time.Second),
			loom.WithContinuation("session", ref),
			loom.WithAffinity(100*time.Millisecond))
		if err != nil {
			t.Fatalf("round %d: %v", n, err)
		}
		if len(res.Output) != 1 {
			t.Fatalf("round %d: %d records", n, len(res.Output))
		}
		answers = append(answers, res.Output[0].String("reply"))
	}

	// The first round has nothing to build on; every one after it should be
	// splicing, which the stable-prefix number on each request reports.
	for n := 2; n <= rounds; n++ {
		_, stable, prompt := nthCall(t, calls, n)
		if stable == 0 {
			t.Fatalf("round %d rendered the whole context again", n)
		}
		if ratio := float64(stable) / float64(prompt); ratio < 0.8 {
			t.Fatalf("round %d certified only %.0f%% of its prompt unchanged", n, 100*ratio)
		}
	}

	// And the answers are what one process, with a cache of its own, produces.
	fresh := t.TempDir()
	if err := copyCAS(filepath.Join(state, "cas"), filepath.Join(fresh, "cas")); err != nil {
		t.Fatal(err)
	}
	local := model.NewRegistry()
	if err := digestMock(local, "", "baseline", 0); err != nil {
		t.Fatal(err)
	}
	for i, r := range refs {
		res, err := loom.Run(context.Background(), sessionPipeline(),
			loom.WithRegistry(local), loom.WithStateDir(fresh),
			loom.WithContinuation("session", r))
		if err != nil {
			t.Fatalf("baseline round %d: %v", i+1, err)
		}
		if got := res.Output[0].String("reply"); got != answers[i] {
			t.Fatalf("round %d: the fleet answered %s, one process answers %s",
				i+1, answers[i], got)
		}
	}
}

// nthCall reads the nth line of the shared call log (1-based).
func nthCall(t *testing.T, path string, n int) (worker string, stable, prompt int) {
	t.Helper()
	lines := callLines(path)
	if len(lines) < n {
		t.Fatalf("the log holds %d calls, wanted call %d", len(lines), n)
	}
	return parseCall(lines[n-1])
}
