package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/route"
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
	if _, err := l.Acquire(context.Background(), "m", model.Limits{}, 100); err != nil {
		t.Fatalf("unlimited acquire: %v", err)
	}

	// Consume the single request in the bucket, then expect the next acquire
	// to block until the context deadline.
	lim := model.Limits{RequestsPerMinute: 1}
	if _, err := l.Acquire(context.Background(), "m2", lim, 1); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := l.Acquire(ctx, "m2", lim, 1)
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

// TestRateLimiterMaxConcurrent pins the ceiling a local backend imposes: not
// a rate, but a number of calls that may be in flight at once. Admissions
// past the ceiling block until a holder releases, and releasing is what makes
// the next one admissible — which is the whole difference from a bucket that
// refills on a clock.
func TestRateLimiterMaxConcurrent(t *testing.T) {
	l := NewRateLimiter()
	lim := model.Limits{MaxConcurrent: 2}

	rel1, err := l.Acquire(context.Background(), "local", lim, 1)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	rel2, err := l.Acquire(context.Background(), "local", lim, 1)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}

	// blocked reports how an admission fares against a short deadline.
	blocked := func() (time.Duration, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := l.Acquire(ctx, "local", lim, 1)
		return time.Since(start), err
	}

	// Both slots are held: a third admission cannot proceed.
	waited, err := blocked()
	if err == nil {
		t.Fatal("acquire should block while every slot is occupied")
	} else if core.ClassOf(err) != core.FailTransient {
		t.Errorf("slot-wait cancellation should be transient, got %v", err)
	}
	if waited < 50*time.Millisecond {
		t.Error("acquire returned before blocking on the semaphore")
	}

	// A different model has its own slots, so it is unaffected.
	if _, err := l.Acquire(context.Background(), "other", lim, 1); err != nil {
		t.Errorf("a second model's slots are its own: %v", err)
	}

	// Releasing readmits, and a repeated release must not hand out a slot that
	// was never held — a released slot belongs to whoever takes it next.
	rel1()
	rel1()
	if _, err := l.Acquire(context.Background(), "local", lim, 1); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if _, err := blocked(); err == nil {
		t.Fatal("a repeated release must not free a second slot")
	}
	rel2()
}

// fakeExec is a scriptable executor.
type fakeExec struct {
	mu     sync.Mutex
	calls  map[string]int // task ID → executions
	models []string       // the model each execution resolved to, in order
	fn     func(t task.Task, call int) (task.Result, error)
}

func newFakeExec(fn func(t task.Task, call int) (task.Result, error)) *fakeExec {
	return &fakeExec{calls: map[string]int{}, fn: fn}
}

func (f *fakeExec) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	f.mu.Lock()
	f.calls[t.ID]++
	n := f.calls[t.ID]
	f.models = append(f.models, t.ResolvedModel)
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

// TestGovernorChargesTheAttemptsThatFailed: what climbing the ladder costs.
//
// The bottom rung answers, its answer is rejected, and the task escalates.
// One result comes back and two calls were billed — so a governor that
// charged only the surviving result would report this run at half its price,
// and would report a stage that escalates on every record as costing exactly
// what one that never escalates costs.
func TestGovernorChargesTheAttemptsThatFailed(t *testing.T) {
	reg, _, _ := testRegistry(t)
	perCall := core.Usage{InputTokens: 100, OutputTokens: 20, Requests: 1, CostUSD: 0.01}
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		// A call that was made and answered, either way: the small model's
		// answer is the one that fails validation, and it is billed for the
		// tokens it produced before anything looked at them.
		res := task.Result{TaskID: tk.ID, Seq: tk.Seq, Model: tk.ResolvedModel, Usage: perCall}
		if tk.ResolvedModel == "small" {
			return res, core.Semantic(errors.New("invalid output"))
		}
		return res, nil
	})
	gov := NewGovernor(core.Budget{})
	s := &Scheduler{Workers: 1, Retry: quickRetry(), Registry: reg, Exec: exec, Governor: gov}
	binding := model.Binding{Model: "small", Escalation: []string{"big"}}
	results, failures, err := s.ExecuteAll(context.Background(), mkTasks(1, binding))
	if err != nil || len(failures) != 0 || len(results) != 1 {
		t.Fatalf("err=%v failures=%v results=%d", err, failures, len(results))
	}
	spent := gov.Spent()
	if spent.Requests != 2 {
		t.Errorf("governor charged %d requests; the rejected call and the accepted one are both calls", spent.Requests)
	}
	if math.Abs(spent.CostUSD-0.02) > 1e-9 {
		t.Errorf("governor charged $%.4f, want $0.0200", spent.CostUSD)
	}
	if spent.TotalTokens() != 240 {
		t.Errorf("governor charged %d tokens, want 240", spent.TotalTokens())
	}
}

// TestFailedCallsAloneExhaustTheBudget: the ceiling has to hold against work
// that produces nothing at all.
//
// Every record here is answered and every answer is rejected, so the run
// produces no results and spends money on all of them. A budget blind to that
// would keep admitting tasks until the input ran out — which is the one
// direction a ceiling must never fail in.
func TestFailedCallsAloneExhaustTheBudget(t *testing.T) {
	reg, _, _ := testRegistry(t)
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		return task.Result{TaskID: tk.ID, Seq: tk.Seq, Model: tk.ResolvedModel,
				Usage: core.Usage{Requests: 1, CostUSD: 0.01}},
			core.Semantic(errors.New("nothing usable came back"))
	})
	gov := NewGovernor(core.Budget{MaxCostUSD: 0.05})
	s := &Scheduler{Workers: 1, Retry: RetryPolicy{MaxAttempts: 1}, Registry: reg,
		Exec: exec, Governor: gov, ContinueOnError: true}
	results, failures, err := s.ExecuteAll(context.Background(), mkTasks(20, model.Binding{Model: "small"}))
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("a run that only ever fails must still hit its ceiling, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("no task produced output, yet %d results came back", len(results))
	}
	// Five calls at a cent each reach the ceiling; the sixth task finds the
	// governor exhausted before it is dispatched, and the rest are drained.
	if len(exec.calls) != 5 {
		t.Errorf("%d of 20 tasks executed, want 5 before the budget stopped the run", len(exec.calls))
	}
	if math.Abs(gov.Spent().CostUSD-0.05) > 1e-9 {
		t.Errorf("governor recorded $%.4f, want $0.0500", gov.Spent().CostUSD)
	}
	if len(failures) == 0 {
		t.Error("the rejected tasks should be dead-lettered")
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

// ---------------------------------------------------------------------------
// Model routing: the escalation ladder as policy.
// ---------------------------------------------------------------------------

// pricedRegistry gives the ladder real prices, because a router compares rungs
// in dollars: two models priced at zero are one rung as far as it is
// concerned.
func pricedRegistry(t *testing.T) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	for _, m := range []struct {
		id   string
		tier model.Tier
		in   float64
	}{{"small", model.TierFast, 1}, {"big", model.TierDeep, 15}} {
		mock := model.NewMock(m.id)
		err := reg.Register(model.Info{ID: m.id, Provider: mock, Tier: m.tier,
			Pricing: model.Pricing{InputPerMTok: m.in, OutputPerMTok: m.in * 5}})
		if err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

func routedTasks(n int, binding model.Binding) []task.Task {
	out := mkTasks(n, binding)
	for i := range out {
		out[i].EstTokens = 4000
	}
	return out
}

// smallAlwaysFails is the shape routing exists for: a bottom rung that cannot
// answer this work, charging for the privilege of finding that out again.
func smallAlwaysFails() func(task.Task, int) (task.Result, error) {
	return func(tk task.Task, call int) (task.Result, error) {
		if tk.ResolvedModel == "small" {
			return task.Result{}, core.Semantic(errors.New("invalid output"))
		}
		return task.Result{TaskID: tk.ID, Seq: tk.Seq, Model: tk.ResolvedModel,
			Usage: core.Usage{Requests: 1}}, nil
	}
}

// TestRouterStopsPayingForTheCallThatAlwaysFails: the whole feature, end to
// end through the scheduler. Early tasks pay for a doomed cheap call; once
// enough verdicts exist, later tasks stop.
func TestRouterStopsPayingForTheCallThatAlwaysFails(t *testing.T) {
	reg := pricedRegistry(t)
	exec := newFakeExec(smallAlwaysFails())
	router := route.New(route.Config{
		Features: func(route.Request) string { return "b" }, MinSamples: 10, NoProbe: true})
	s := &Scheduler{Workers: 1, Retry: quickRetry(), Registry: reg, Exec: exec, Router: router}

	binding := model.Binding{Model: "small", Escalation: []string{"big"}}
	results, failures, err := s.ExecuteAll(context.Background(), routedTasks(40, binding))
	if err != nil || len(failures) != 0 {
		t.Fatalf("err=%v failures=%v", err, failures)
	}
	if len(results) != 40 {
		t.Fatalf("results = %d, want 40", len(results))
	}
	// Every record still gets the right answer, from the model that can give it.
	for _, r := range results {
		if r.Model != "big" {
			t.Fatalf("task %d answered by %q", r.Seq, r.Model)
		}
	}
	// A flat ladder pays 40 doomed calls. A router that learned pays far fewer.
	var wasted int
	for _, m := range exec.models {
		if m == "small" {
			wasted++
		}
	}
	if wasted >= 40 {
		t.Fatalf("%d calls to the model that never answers: the router learned nothing", wasted)
	}
	if wasted < 10 {
		t.Fatalf("only %d calls to the bottom rung: it must not skip before it has evidence", wasted)
	}
	if st := router.Stats(); st.Moved == 0 {
		t.Fatalf("router stats show nothing moved: %+v", st)
	}
}

// TestRoutingNeverChangesTheAnswer: the safety invariant. A router that starts
// tasks high must produce exactly the results a flat ladder produces, because
// validation and escalation are untouched — it can cost work, never an answer.
func TestRoutingNeverChangesTheAnswer(t *testing.T) {
	run := func(router route.Router) []task.Result {
		exec := newFakeExec(smallAlwaysFails())
		s := &Scheduler{Workers: 2, Retry: quickRetry(), Registry: pricedRegistry(t),
			Exec: exec, Router: router}
		res, failures, err := s.ExecuteAll(context.Background(),
			routedTasks(30, model.Binding{Model: "small", Escalation: []string{"big"}}))
		if err != nil || len(failures) != 0 {
			t.Fatalf("err=%v failures=%v", err, failures)
		}
		return res
	}

	router := route.New(route.Config{
		Features: func(route.Request) string { return "b" }, MinSamples: 5, NoProbe: true})
	flat, routed := run(nil), run(router)
	if len(flat) != len(routed) {
		t.Fatalf("flat produced %d results, routed %d", len(flat), len(routed))
	}
	for i := range flat {
		if flat[i].Seq != routed[i].Seq || flat[i].Model != routed[i].Model {
			t.Fatalf("result %d differs: flat %+v routed %+v", i, flat[i], routed[i])
		}
	}
}

// TestRouterIsNeverAskedAboutASingleRungLadder: a stage with no escalation has
// nothing to decide, and a decision recorded against it would be evidence
// about a choice that was never made.
func TestRouterIsNeverAskedAboutASingleRungLadder(t *testing.T) {
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		return task.Result{TaskID: tk.ID, Seq: tk.Seq, Model: tk.ResolvedModel,
			Usage: core.Usage{Requests: 1}}, nil
	})
	spy := &countingRouter{}
	s := &Scheduler{Workers: 1, Retry: quickRetry(), Registry: pricedRegistry(t),
		Exec: exec, Router: spy}
	if _, _, err := s.ExecuteAll(context.Background(),
		routedTasks(5, model.Binding{Model: "small"})); err != nil {
		t.Fatal(err)
	}
	if spy.routes != 0 || spy.observes != 0 {
		t.Fatalf("router consulted %d times and told %d verdicts about a one-rung ladder",
			spy.routes, spy.observes)
	}
}

// TestOnlySemanticVerdictsTeachTheRouter: a transient failure says the network
// was unwell and a permanent one says the code is wrong. Recording either as
// evidence would teach the router to escalate away from a bug.
func TestOnlySemanticVerdictsTeachTheRouter(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"transient", core.Transient(errors.New("502")), 0},
		{"permanent", core.Permanent(errors.New("bad request")), 0},
		{"semantic", core.Semantic(errors.New("invalid output")), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
				calls++
				if calls == 1 {
					return task.Result{}, tc.err
				}
				return task.Result{TaskID: tk.ID, Seq: tk.Seq, Model: tk.ResolvedModel,
					Usage: core.Usage{Requests: 1}}, nil
			})
			spy := &countingRouter{}
			s := &Scheduler{Workers: 1, Retry: quickRetry(), Registry: pricedRegistry(t),
				Exec: exec, Router: spy, ContinueOnError: true}
			//nolint:errcheck // the permanent case dead-letters, which is the point
			s.ExecuteAll(context.Background(),
				routedTasks(1, model.Binding{Model: "small", Escalation: []string{"big"}}))
			if spy.invalid != tc.want {
				t.Fatalf("%d invalid verdicts recorded, want %d", spy.invalid, tc.want)
			}
		})
	}
}

// TestCacheHitsAreNotVerdicts: a cache hit replays work some earlier task paid
// for at some other rung. Counting it as this rung succeeding would teach the
// router that the cheap model handles everything the cache already holds.
func TestCacheHitsAreNotVerdicts(t *testing.T) {
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		return task.Result{TaskID: tk.ID, Seq: tk.Seq, Model: "big", CacheHit: true}, nil
	})
	spy := &countingRouter{}
	s := &Scheduler{Workers: 1, Retry: quickRetry(), Registry: pricedRegistry(t),
		Exec: exec, Router: spy}
	if _, _, err := s.ExecuteAll(context.Background(),
		routedTasks(5, model.Binding{Model: "small", Escalation: []string{"big"}})); err != nil {
		t.Fatal(err)
	}
	if spy.routes == 0 {
		t.Fatal("router should still be asked: nothing knows a task will hit the cache")
	}
	if spy.observes != 0 {
		t.Fatalf("%d verdicts recorded from cache hits", spy.observes)
	}
}

// TestRoutingIsReported: the saving has to reach the run report, and it must
// never appear without the probes that measure whether it was right.
func TestRoutingIsReported(t *testing.T) {
	bus := observe.NewBus()
	collector := observe.NewCollector()
	bus.On(collector.Handle)

	// The fake executor stands in for ModelClient, which is what publishes
	// model.called — and those events are the only thing that says what a call
	// on a given model actually cost. Without them the report can count
	// skipped calls but must not price them.
	inner := smallAlwaysFails()
	exec := newFakeExec(func(tk task.Task, call int) (task.Result, error) {
		cost := map[string]float64{"small": 0.0002, "big": 0.0030}[tk.ResolvedModel]
		bus.Publish(observe.Event{Type: observe.ModelCalled, Stage: tk.Stage,
			Model: tk.ResolvedModel, Usage: core.Usage{Requests: 1, CostUSD: cost}})
		return inner(tk, call)
	})
	router := route.New(route.Config{
		Features: func(route.Request) string { return "b" }, MinSamples: 5, ProbeRate: 0.25})
	s := &Scheduler{Workers: 1, Retry: quickRetry(), Registry: pricedRegistry(t),
		Exec: exec, Router: router, Bus: bus}
	if _, _, err := s.ExecuteAll(context.Background(),
		routedTasks(60, model.Binding{Model: "small", Escalation: []string{"big"}})); err != nil {
		t.Fatal(err)
	}
	bus.Close()

	rep := collector.Report()
	routed, skipped, saved, probes, hits := rep.Routing()
	if routed == 0 {
		t.Fatal("report shows nothing routed")
	}
	if skipped != routed {
		t.Errorf("skipped %d calls across %d routed tasks on a two-rung ladder", skipped, routed)
	}
	// Priced from the calls that actually ran on the skipped model, not from
	// the planner's estimate: every skipped call was one on "small", each of
	// which measurably cost $0.0002.
	if want := float64(skipped) * 0.0002; math.Abs(saved-want) > 1e-9 {
		t.Errorf("saved $%.6f over %d skipped calls, want $%.6f measured from the "+
			"calls that did run on that model", saved, skipped, want)
	}
	if probes == 0 {
		t.Error("no probes held back: the saving is then a claim rather than a measurement")
	}
	// The bottom rung never answers here, so no probe can have hit.
	if hits != 0 {
		t.Errorf("%d probes answered at a rung that always fails", hits)
	}
	if got := rep.String(); !strings.Contains(got, "routing:") ||
		!strings.Contains(got, "probe(s) held back") {
		t.Errorf("report does not carry the routing line and its correction:\n%s", got)
	}
}

// countingRouter records what it was asked without deciding anything.
type countingRouter struct {
	mu                        sync.Mutex
	routes, observes, invalid int
}

func (c *countingRouter) Route(route.Request) route.Decision {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.routes++
	return route.Decision{}
}

func (c *countingRouter) Observe(o route.Outcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observes++
	if !o.Valid {
		c.invalid++
	}
}
