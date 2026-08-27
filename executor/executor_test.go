package executor_test

// The local executor's cache path, tested where the two things that make it
// subtle live: the lease a task may wait on before it runs, and the shape of
// the records it hands back afterwards.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/task"
)

// runner is an OpRunner a test can script.
type runner struct {
	mu    sync.Mutex
	calls int
	fn    func(ctx context.Context, t task.Task) ([]core.Record, error)
}

func (r *runner) Run(ctx context.Context, rt *executor.Runtime, t task.Task) (
	[]core.Record, core.Usage, string, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	out, err := r.fn(ctx, t)
	return out, core.Usage{Requests: 1}, "mock", err
}

func (r *runner) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func newLocal(t *testing.T, r *runner, coalesce time.Duration) (*executor.Local, *store.Cache) {
	t.Helper()
	cas, err := store.NewCAS("")
	if err != nil {
		t.Fatal(err)
	}
	cache, err := store.NewCache(cas, "")
	if err != nil {
		t.Fatal(err)
	}
	return &executor.Local{
		Runners:  map[string]executor.OpRunner{"s": r},
		Cache:    cache,
		Coalesce: coalesce,
	}, cache
}

func mkTask(id string, budget core.Budget) task.Task {
	return task.Task{
		ID: id, Stage: "s", CacheKey: "ck",
		Input:    []core.Record{core.NewRecord("r1", map[string]any{"text": "hello"})},
		Envelope: task.Envelope{RunID: "run", Stage: "s", Budget: budget},
	}
}

// TestBudgetedDurationCoversTheWaitAndTheWork is the property a per-task
// duration has to have to mean anything: it bounds the task, not the call
// inside it. A follower that spends time waiting for an identical task has
// spent the budget, and starting a fresh deadline afterwards would hand it a
// second one.
func TestBudgetedDurationCoversTheWaitAndTheWork(t *testing.T) {
	const budget = 200 * time.Millisecond

	r := &runner{fn: func(ctx context.Context, _ task.Task) ([]core.Record, error) {
		<-ctx.Done() // never finishes on its own: only the deadline ends it
		return nil, ctx.Err()
	}}
	l, cache := newLocal(t, r, time.Minute)

	// The test holds the lease and never lets go, so the task below has
	// nothing to wait for and must give up on its own bound rather than on
	// somebody else's release.
	held := cache.Claim(context.Background(), "ck", time.Minute)
	defer held.Release()

	start := time.Now()
	_, err := l.Execute(context.Background(), mkTask("t1", core.Budget{MaxDuration: budget}))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a task that can neither coalesce nor finish must fail")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the task's own deadline", err)
	}
	if elapsed > budget+budget/2 {
		t.Errorf("took %s against a %s budget: the wait and the work must share "+
			"one deadline, not get one each", elapsed, budget)
	}
	// And the work got a real share of it: a wait capped at half the budget is
	// what leaves the task time to do the thing it was waiting to avoid.
	if r.count() != 1 {
		t.Errorf("runner ran %d times, want 1: the task must still get to run", r.count())
	}
}

// TestCoalesceWaitLeavesTimeToRun is the same rule from the other side: the
// wait is capped so a task that waited in vain has at least as long to work as
// it spent hoping not to have to.
func TestCoalesceWaitLeavesTimeToRun(t *testing.T) {
	const budget = 200 * time.Millisecond

	ran := make(chan time.Duration, 1)
	start := time.Now()
	r := &runner{fn: func(ctx context.Context, _ task.Task) ([]core.Record, error) {
		ran <- time.Since(start)
		return []core.Record{core.NewRecord("r1", map[string]any{"done": true})}, nil
	}}
	l, cache := newLocal(t, r, time.Minute)

	held := cache.Claim(context.Background(), "ck", time.Minute)
	defer held.Release()

	if _, err := l.Execute(context.Background(), mkTask("t1", core.Budget{MaxDuration: budget})); err != nil {
		t.Fatalf("execute: %v", err)
	}
	select {
	case waited := <-ran:
		if waited > budget*3/4 {
			t.Errorf("the runner started after %s of a %s budget: the wait must "+
				"leave the work room", waited, budget)
		}
	default:
		t.Fatal("the runner never ran")
	}
}

// TestComputedAndReplayedRecordsHaveOneShape is what makes a replay
// substitutable for the call it replaces. Records cross the cache as JSON, so
// a replay's ints come back as float64 and its times as strings; a task that
// returned the runner's Go values directly would hand downstream code a
// different type for the same field depending on which task won a race.
//
// The replay path here is the same one a coalesced follower takes — both are
// Cache.Get — so pinning it sequentially pins it for the lease too.
func TestComputedAndReplayedRecordsHaveOneShape(t *testing.T) {
	when := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	r := &runner{fn: func(context.Context, task.Task) ([]core.Record, error) {
		return []core.Record{core.NewRecord("r1", map[string]any{
			"n":    7,
			"when": when,
			"tags": []string{"a", "b"},
		})}, nil
	}}
	l, _ := newLocal(t, r, time.Minute)

	computed, err := l.Execute(context.Background(), mkTask("t1", core.Budget{}))
	if err != nil {
		t.Fatalf("first execute: %v", err)
	}
	replayed, err := l.Execute(context.Background(), mkTask("t2", core.Budget{}))
	if err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if !replayed.CacheHit {
		t.Fatal("the second task must replay the first")
	}
	if r.count() != 1 {
		t.Fatalf("runner ran %d times, want 1", r.count())
	}

	for _, field := range []string{"n", "when", "tags"} {
		a := computed.Output[0].Data[field]
		b := replayed.Output[0].Data[field]
		if got, want := typeOf(a), typeOf(b); got != want {
			t.Errorf("field %q: computed is %s, replayed is %s — one stage, two types",
				field, got, want)
		}
	}
	// And the shape is the canonical one, because that is the shape every
	// executor, process and rerun can agree on.
	if _, ok := computed.Output[0].Data["n"].(float64); !ok {
		t.Errorf("n = %T, want float64: a cacheable stage emits what the cache serves",
			computed.Output[0].Data["n"])
	}
}

func typeOf(v any) string {
	switch v.(type) {
	case float64:
		return "float64"
	case string:
		return "string"
	case []any:
		return "[]any"
	default:
		return "other"
	}
}

// TestIdenticalTasksRunOnce is the lease at the executor boundary: two tasks
// with one key, one call.
func TestIdenticalTasksRunOnce(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	r := &runner{fn: func(context.Context, task.Task) ([]core.Record, error) {
		close(entered)
		<-release
		return []core.Record{core.NewRecord("r1", map[string]any{"answer": "42"})}, nil
	}}
	l, cache := newLocal(t, r, time.Minute)

	type out struct {
		res task.Result
		err error
	}
	done := make(chan out, 1)
	go func() {
		res, err := l.Execute(context.Background(), mkTask("t1", core.Budget{}))
		done <- out{res, err}
	}()
	<-entered

	follower := make(chan out, 1)
	go func() {
		res, err := l.Execute(context.Background(), mkTask("t2", core.Budget{}))
		follower <- out{res, err}
	}()
	waitFor(t, "the follower to park on the lease", func() bool { return cache.Waiting() == 1 })

	close(release)
	leader := <-done
	if leader.err != nil {
		t.Fatalf("leader: %v", leader.err)
	}
	got := <-follower
	if got.err != nil {
		t.Fatalf("follower: %v", got.err)
	}
	if !got.res.CacheHit || !got.res.Coalesced {
		t.Errorf("follower = %+v, want a coalesced cache hit", got.res)
	}
	if r.count() != 1 {
		t.Errorf("runner ran %d times, want 1", r.count())
	}
	if got.res.Output[0].String("answer") != "42" {
		t.Errorf("follower got %v, want the leader's answer", got.res.Output)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
