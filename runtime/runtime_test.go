package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/task"
)

func TestRetryPolicyDelay(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5, BaseDelay: 100 * time.Millisecond, MaxDelay: 400 * time.Millisecond}
	if d := p.Delay(1); d != 100*time.Millisecond {
		t.Errorf("Delay(1) = %v", d)
	}
	if d := p.Delay(2); d != 200*time.Millisecond {
		t.Errorf("Delay(2) = %v", d)
	}
	if d := p.Delay(10); d != 400*time.Millisecond {
		t.Errorf("Delay(10) should clamp to MaxDelay, got %v", d)
	}
}

func TestGovernor(t *testing.T) {
	g := NewGovernor(core.Budget{MaxCostUSD: 0.05})
	if err := g.Charge(core.Usage{CostUSD: 0.02, Requests: 1}); err != nil {
		t.Fatalf("under budget: %v", err)
	}
	if g.Exhausted() {
		t.Fatal("should not be exhausted yet")
	}
	if err := g.Charge(core.Usage{CostUSD: 0.04, Requests: 1}); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("over budget should return ErrBudgetExhausted, got %v", err)
	}
	if !g.Exhausted() {
		t.Fatal("should be exhausted")
	}
	if g.Spent().CostUSD != 0.06 {
		t.Errorf("Spent = %+v", g.Spent())
	}
}

func TestRateLimiterUnlimitedAndCancel(t *testing.T) {
	l := NewRateLimiter()
	if err := l.Acquire(context.Background(), "m", model.Limits{}, 100); err != nil {
		t.Fatalf("unlimited acquire: %v", err)
	}

	// Consume the single request in the bucket, then expect the next acquire
	// to block until the context deadline.
	lim := model.Limits{RequestsPerMinute: 1}
	if err := l.Acquire(context.Background(), "m2", lim, 1); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := l.Acquire(ctx, "m2", lim, 1)
	if err == nil {
		t.Fatal("acquire should fail when rate limited past deadline")
	}
	if core.ClassOf(err) != core.FailTransient {
		t.Errorf("rate-limit wait cancellation should be transient, got %v", err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Error("acquire returned before blocking on the limiter")
	}
}

// fakeExec is a scriptable executor.
type fakeExec struct {
	mu    sync.Mutex
	calls map[string]int // task ID → executions
	fn    func(t task.Task, call int) (task.Result, error)
}

func newFakeExec(fn func(t task.Task, call int) (task.Result, error)) *fakeExec {
	return &fakeExec{calls: map[string]int{}, fn: fn}
}

func (f *fakeExec) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	f.mu.Lock()
	f.calls[t.ID]++
	n := f.calls[t.ID]
	f.mu.Unlock()
	return f.fn(t, n)
}

func mkTasks(n int, binding model.Binding) []task.Task {
	out := make([]task.Task, n)
	for i := range out {
		out[i] = task.Task{
			ID: fmt.Sprintf("t%d", i), Seq: i, Stage: "s",
			Envelope: task.Envelope{RunID: "run", Stage: "s", Binding: binding},
		}
	}
	return out
}

func testRegistry(t *testing.T) (*model.Registry, *model.Mock, *model.Mock) {
	t.Helper()
	reg := model.NewRegistry()
	small, err := model.RegisterMock(reg, "small", model.TierFast)
	if err != nil {
		t.Fatal(err)
	}
	big, err := model.RegisterMock(reg, "big", model.TierDeep)
	if err != nil {
		t.Fatal(err)
	}
	return reg, small, big
}

func quickRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
}

func TestSchedulerTransientRetry(t *testing.T) {
	reg, _, _ := testRegistry(t)
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		if call < 3 {
			return task.Result{}, core.Transient(errors.New("hiccup"))
		}
		return task.Result{TaskID: tk.ID, Seq: tk.Seq, Model: tk.ResolvedModel,
			Usage: core.Usage{Requests: 1}}, nil
	})
	s := &Scheduler{Workers: 2, Retry: quickRetry(), Registry: reg, Exec: exec}
	results, failures, err := s.ExecuteAll(context.Background(),
		mkTasks(1, model.Binding{Model: "small"}))
	if err != nil || len(failures) != 0 {
		t.Fatalf("err=%v failures=%v", err, failures)
	}
	if len(results) != 1 || results[0].Model != "small" {
		t.Fatalf("results = %+v", results)
	}
	if exec.calls["t0"] != 3 {
		t.Errorf("expected 3 attempts, got %d", exec.calls["t0"])
	}
}

func TestSchedulerSemanticEscalation(t *testing.T) {
	reg, _, _ := testRegistry(t)
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		if tk.ResolvedModel == "small" {
			return task.Result{}, core.Semantic(errors.New("invalid output"))
		}
		return task.Result{TaskID: tk.ID, Seq: tk.Seq, Model: tk.ResolvedModel,
			Usage: core.Usage{Requests: 1}}, nil
	})
	s := &Scheduler{Workers: 1, Retry: quickRetry(), Registry: reg, Exec: exec}
	binding := model.Binding{Model: "small", Escalation: []string{"big"}}
	results, failures, err := s.ExecuteAll(context.Background(), mkTasks(1, binding))
	if err != nil || len(failures) != 0 {
		t.Fatalf("err=%v failures=%v", err, failures)
	}
	if results[0].Model != "big" {
		t.Fatalf("semantic failure should escalate to 'big', ran on %q", results[0].Model)
	}
}

func TestSchedulerPermanentFailsFast(t *testing.T) {
	reg, _, _ := testRegistry(t)
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		return task.Result{}, core.Permanent(errors.New("bad request"))
	})
	s := &Scheduler{Workers: 1, Retry: quickRetry(), Registry: reg, Exec: exec}
	_, failures, err := s.ExecuteAll(context.Background(), mkTasks(1, model.Binding{Model: "small"}))
	if err == nil {
		t.Fatal("run should abort on permanent failure by default")
	}
	if len(failures) != 1 || failures[0].Class != core.FailPermanent {
		t.Fatalf("failures = %+v", failures)
	}
	if exec.calls["t0"] != 1 {
		t.Errorf("permanent failure must not retry, got %d attempts", exec.calls["t0"])
	}
}

func TestSchedulerContinueOnError(t *testing.T) {
	reg, _, _ := testRegistry(t)
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		if tk.Seq == 1 {
			return task.Result{}, core.Permanent(errors.New("poison record"))
		}
		return task.Result{TaskID: tk.ID, Seq: tk.Seq, Usage: core.Usage{Requests: 1}}, nil
	})
	s := &Scheduler{Workers: 2, Retry: quickRetry(), Registry: reg, Exec: exec, ContinueOnError: true}
	results, failures, err := s.ExecuteAll(context.Background(), mkTasks(3, model.Binding{Model: "small"}))
	if err != nil {
		t.Fatalf("continue-on-error run should not abort: %v", err)
	}
	if len(results) != 2 || len(failures) != 1 {
		t.Fatalf("results=%d failures=%d", len(results), len(failures))
	}
	if results[0].Seq != 0 || results[1].Seq != 2 {
		t.Errorf("results must be in Seq order: %+v", results)
	}
}

func TestSchedulerBudgetAbort(t *testing.T) {
	reg, _, _ := testRegistry(t)
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		return task.Result{TaskID: tk.ID, Seq: tk.Seq,
			Usage: core.Usage{Requests: 1, CostUSD: 1.0}}, nil
	})
	s := &Scheduler{
		Workers: 1, Retry: quickRetry(), Registry: reg, Exec: exec,
		Governor: NewGovernor(core.Budget{MaxCostUSD: 1.5}),
	}
	results, failures, err := s.ExecuteAll(context.Background(), mkTasks(5, model.Binding{Model: "small"}))
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("expected ErrBudgetExhausted, got %v", err)
	}
	if len(results) == 0 || len(results) >= 5 {
		t.Fatalf("expected partial results, got %d", len(results))
	}
	for _, f := range failures {
		if f.Class != core.FailBudget && f.Class != core.FailTransient {
			t.Errorf("unexpected failure class after budget abort: %v", f.Class)
		}
	}
}

func TestSchedulerObservabilityEvents(t *testing.T) {
	reg, _, _ := testRegistry(t)
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		return task.Result{TaskID: tk.ID, Seq: tk.Seq, Usage: core.Usage{Requests: 1},
			Output: []core.Record{core.NewRecord("out-"+tk.ID, map[string]any{"ok": true})}}, nil
	})
	bus := observe.NewBus()
	var mu sync.Mutex
	var events []observe.Event
	bus.On(func(e observe.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})

	tasks := mkTasks(3, model.Binding{Model: "small"})
	for i := range tasks {
		tasks[i].Input = []core.Record{core.NewRecord(fmt.Sprintf("r%d", i),
			map[string]any{"subject": "hello"})}
	}
	s := &Scheduler{Workers: 2, Retry: quickRetry(), Registry: reg, Exec: exec, Bus: bus}
	if _, _, err := s.ExecuteAll(context.Background(), tasks); err != nil {
		t.Fatal(err)
	}

	scheduled, started, completed := 0, 0, 0
	for _, e := range events {
		switch e.Type {
		case observe.TaskScheduled:
			scheduled++
			if e.Records != 1 || !strings.Contains(e.Input, "hello") {
				t.Errorf("scheduled event missing input payload: %+v", e)
			}
			if len(e.InputIDs) != 1 {
				t.Errorf("scheduled event missing input IDs: %+v", e)
			}
		case observe.TaskStarted:
			started++
			if e.Worker == "" {
				t.Errorf("task.started missing worker: %+v", e)
			}
		case observe.TaskCompleted:
			completed++
			if e.Worker == "" {
				t.Errorf("task.completed missing worker: %+v", e)
			}
			if !strings.Contains(e.Output, "out-") || len(e.OutIDs) != 1 {
				t.Errorf("task.completed missing output payload: %+v", e)
			}
		}
	}
	if scheduled != 3 || started != 3 || completed != 3 {
		t.Fatalf("scheduled=%d started=%d completed=%d, want 3 each", scheduled, started, completed)
	}
}

func TestRecordsJSONClipped(t *testing.T) {
	long := strings.Repeat("é", observe.PayloadCap)
	got := recordsJSON([]core.Record{core.NewRecord("r1", map[string]any{"text": long})})
	if runes := []rune(got); len(runes) > observe.PayloadCap+20 {
		t.Fatalf("payload too long: %d runes", len(runes))
	}
	if !strings.HasSuffix(got, "[truncated]") {
		t.Fatalf("clipped payload should end with marker, got %q", got[len(got)-24:])
	}
	if !strings.Contains(got, "r1") {
		t.Fatal("payload should include the record ID")
	}

	small := recordsJSON([]core.Record{core.NewRecord("r2", map[string]any{"subject": "hi"})})
	if !strings.Contains(small, `"subject": "hi"`) || strings.Contains(small, "truncated") {
		t.Fatalf("small payload should be complete: %q", small)
	}
}
