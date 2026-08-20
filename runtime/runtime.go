// Package runtime implements Loom's scheduling machinery: retry policy with
// exponential backoff, rate-limit-aware admission control (token buckets per
// model for both requests/min and tokens/min), the run-level budget
// governor, and the scheduler that drives a batch of tasks through an
// executor with bounded concurrency and class-aware recovery.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/route"
	"github.com/zionrubin/loom/task"
)

// RetryPolicy controls transient/semantic retry behavior.
type RetryPolicy struct {
	MaxAttempts int           // total attempts per task (>=1)
	BaseDelay   time.Duration // first backoff delay
	MaxDelay    time.Duration // backoff ceiling
	Jitter      bool          // randomize ±50%
}

// DefaultRetry is the standard policy.
var DefaultRetry = RetryPolicy{MaxAttempts: 3, BaseDelay: 200 * time.Millisecond, MaxDelay: 5 * time.Second, Jitter: true}

// Delay returns the backoff before the given retry (attempt starts at 1).
func (p RetryPolicy) Delay(attempt int) time.Duration {
	d := p.BaseDelay
	for i := 1; i < attempt; i++ {
		d *= 2
		if p.MaxDelay > 0 && d >= p.MaxDelay {
			d = p.MaxDelay
			break
		}
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	if p.Jitter && d > 0 {
		half := int64(d) / 2
		d = time.Duration(half + rand.Int63n(int64(d)-half+1))
	}
	return d
}

// RateLimiter provides per-model admission control: token buckets for
// requests/min and tokens/min, plus a semaphore for requests in flight.
// Acquire blocks until admission is possible (or ctx ends), so the scheduler
// never dispatches work the provider would immediately 429 — or, for a model
// running on local hardware, work that would queue inside the server instead
// of inside the scheduler.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	slots   map[string]chan struct{}
}

type bucket struct {
	req, tok         float64
	reqCap, tokCap   float64
	reqRate, tokRate float64 // per second
	last             time.Time
}

// NewRateLimiter returns an empty limiter; buckets are created on first use.
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{buckets: map[string]*bucket{}, slots: map[string]chan struct{}{}}
}

func (b *bucket) refill(now time.Time) {
	dt := now.Sub(b.last).Seconds()
	if dt <= 0 {
		return
	}
	b.req = min(b.reqCap, b.req+dt*b.reqRate)
	b.tok = min(b.tokCap, b.tok+dt*b.tokRate)
	b.last = now
}

// Acquire admits one request of ~estTokens against modelID's limits,
// blocking as needed, and returns the release for whatever the admission
// holds for the duration of the call. Zero limits admit immediately; the
// returned release is never nil and must be called exactly once when the
// request finishes, successfully or not.
//
// The rate buckets are drawn on before the in-flight slot, not after. A
// request that holds a scarce device slot while waiting on a per-minute
// quota idles the device; a bucket drawn slightly ahead of issuance only
// makes the limiter conservative, which is the safe direction for a ceiling.
func (l *RateLimiter) Acquire(ctx context.Context, modelID string, lim model.Limits, estTokens int) (func(), error) {
	if err := l.acquireRate(ctx, modelID, lim, estTokens); err != nil {
		return noRelease, err
	}
	return l.acquireSlot(ctx, modelID, lim)
}

// noRelease is the release returned by an admission that holds nothing.
func noRelease() {}

// acquireSlot takes one of modelID's in-flight slots, blocking until one is
// free. This is the ceiling a local backend imposes: a fixed number of
// sequences decoded at once, which no amount of waiting per minute expresses.
func (l *RateLimiter) acquireSlot(ctx context.Context, modelID string, lim model.Limits) (func(), error) {
	if lim.MaxConcurrent <= 0 {
		return noRelease, nil
	}
	l.mu.Lock()
	sem, ok := l.slots[modelID]
	if !ok {
		sem = make(chan struct{}, lim.MaxConcurrent)
		l.slots[modelID] = sem
	}
	l.mu.Unlock()

	select {
	case sem <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-sem }) }, nil
	case <-ctx.Done():
		return noRelease, core.Transient(ctx.Err())
	}
}

// acquireRate blocks until modelID's requests/min and tokens/min buckets can
// both cover one request of ~estTokens.
func (l *RateLimiter) acquireRate(ctx context.Context, modelID string, lim model.Limits, estTokens int) error {
	if lim.RequestsPerMinute <= 0 && lim.TokensPerMinute <= 0 {
		return nil
	}
	for {
		l.mu.Lock()
		b, ok := l.buckets[modelID]
		if !ok {
			b = &bucket{
				reqCap: float64(lim.RequestsPerMinute), reqRate: float64(lim.RequestsPerMinute) / 60,
				tokCap: float64(lim.TokensPerMinute), tokRate: float64(lim.TokensPerMinute) / 60,
				last: time.Now(),
			}
			b.req, b.tok = b.reqCap, b.tokCap
			l.buckets[modelID] = b
		}
		now := time.Now()
		b.refill(now)

		needTok := float64(estTokens)
		if lim.TokensPerMinute > 0 && needTok > b.tokCap {
			needTok = b.tokCap // single oversized request: admit at full bucket
		}
		reqOK := lim.RequestsPerMinute <= 0 || b.req >= 1
		tokOK := lim.TokensPerMinute <= 0 || b.tok >= needTok
		if reqOK && tokOK {
			if lim.RequestsPerMinute > 0 {
				b.req--
			}
			if lim.TokensPerMinute > 0 {
				b.tok -= needTok
			}
			l.mu.Unlock()
			return nil
		}

		// Compute the wait until both constraints can be satisfied.
		wait := 10 * time.Millisecond
		if !reqOK && b.reqRate > 0 {
			wait = max(wait, time.Duration((1-b.req)/b.reqRate*float64(time.Second)))
		}
		if !tokOK && b.tokRate > 0 {
			wait = max(wait, time.Duration((needTok-b.tok)/b.tokRate*float64(time.Second)))
		}
		l.mu.Unlock()

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return core.Transient(ctx.Err())
		}
	}
}

// ErrBudgetExhausted signals the run-level budget was spent; the scheduler
// stops admitting new work and the run returns partial results.
var ErrBudgetExhausted = errors.New("run budget exhausted")

// Governor enforces the run-level budget (cost and tokens) across all
// concurrent tasks. Charging is post-hoc, so overrun is bounded by the
// number of in-flight tasks.
type Governor struct {
	mu        sync.Mutex
	budget    core.Budget
	spent     core.Usage
	exhausted bool
}

// NewGovernor returns a governor for the budget (zero fields = unlimited).
func NewGovernor(b core.Budget) *Governor { return &Governor{budget: b} }

// Charge records usage; it returns ErrBudgetExhausted once a limit is
// crossed (the usage is still recorded).
func (g *Governor) Charge(u core.Usage) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.spent.Add(u)
	if (g.budget.MaxCostUSD > 0 && g.spent.CostUSD >= g.budget.MaxCostUSD) ||
		(g.budget.MaxTokens > 0 && g.spent.TotalTokens() >= g.budget.MaxTokens) {
		g.exhausted = true
	}
	if g.exhausted {
		return ErrBudgetExhausted
	}
	return nil
}

// Exhausted reports whether the budget has been spent.
func (g *Governor) Exhausted() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.exhausted
}

// Spent returns the usage recorded so far.
func (g *Governor) Spent() core.Usage {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.spent
}

// Failure is a task that exhausted its recovery options (a dead letter).
type Failure struct {
	Task     task.Task
	Err      error
	Class    core.FailureClass
	Attempts int
}

// Scheduler drives batches of tasks through an executor with bounded
// concurrency, admission control, budget enforcement, and class-aware
// retries (transient → backoff; semantic → escalate up the model ladder;
// permanent → dead-letter).
type Scheduler struct {
	Workers         int
	Retry           RetryPolicy
	Limiter         *RateLimiter
	Governor        *Governor
	Registry        *model.Registry
	Exec            executor.Executor
	Bus             *observe.Bus
	ContinueOnError bool
	// Router chooses where on the escalation ladder a task starts, turning the
	// ladder from a recovery path into a policy. Nil — the default — starts
	// every task at the bottom, which is the behaviour the ladder has always
	// had. It changes the starting rung and nothing else: validation still
	// runs, escalation still climbs, and the top of the ladder is still the
	// ceiling, so a router that guesses wrong costs a call rather than an
	// answer.
	Router route.Router
}

// ExecuteAll runs all tasks. Results are returned in input (Seq) order.
// Failures lists dead-lettered tasks. The error is non-nil only for
// run-level aborts: budget exhaustion or context cancellation.
func (s *Scheduler) ExecuteAll(ctx context.Context, tasks []task.Task) ([]task.Result, []Failure, error) {
	if len(tasks) == 0 {
		return nil, nil, nil
	}
	workers := s.Workers
	if workers <= 0 {
		workers = 8
	}
	if workers > len(tasks) {
		workers = len(tasks)
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	var (
		mu       sync.Mutex
		results  []task.Result
		failures []Failure
		wg       sync.WaitGroup
	)
	queue := make(chan task.Task)

	abort := func(cause error) { cancel(cause) }

	// Announce the batch so observers see queued (not-yet-running) tasks,
	// with the full input payload and record IDs for lineage tracking.
	for _, t := range tasks {
		s.publish(observe.Event{Type: observe.TaskScheduled, RunID: t.Envelope.RunID,
			Stage: t.Stage, TaskID: t.ID, Records: len(t.Input), Pane: t.Pane,
			Input: recordsJSON(t.Input), InputIDs: recordIDs(t.Input)})
	}

	worker := func(name string) {
		defer wg.Done()
		for t := range queue {
			if ctx.Err() != nil {
				continue // drain without executing
			}
			res, attempts, err := s.runTask(ctx, t, name)
			if err == nil {
				mu.Lock()
				results = append(results, res)
				mu.Unlock()
				continue
			}
			class := core.ClassOf(err)
			mu.Lock()
			failures = append(failures, Failure{Task: t, Err: err, Class: class, Attempts: attempts})
			mu.Unlock()
			if class == core.FailBudget {
				abort(ErrBudgetExhausted)
			} else if !s.ContinueOnError && ctx.Err() == nil {
				abort(err)
			}
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go worker(fmt.Sprintf("w%d", i+1))
	}
	for _, t := range tasks {
		queue <- t
	}
	close(queue)
	wg.Wait()

	sort.Slice(results, func(i, j int) bool { return results[i].Seq < results[j].Seq })

	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return results, failures, cause
	}
	if err := ctx.Err(); err != nil {
		return results, failures, err
	}
	return results, failures, nil
}

// RunTask executes one task through its full recovery lifecycle — admission
// control, budget check, class-aware retry and escalation — and reports the
// result along with how many attempts it took.
//
// It is the single execution path both drivers share: ExecuteAll calls it
// from a fixed worker pool per stage batch, and Engine calls it from a
// continuously-fed slot pool. Recovery semantics therefore cannot drift
// between batch and streaming execution.
func (s *Scheduler) RunTask(ctx context.Context, t task.Task, worker string) (task.Result, int, error) {
	return s.runTask(ctx, t, worker)
}

// runTask executes one task through its full recovery lifecycle.
func (s *Scheduler) runTask(ctx context.Context, t task.Task, worker string) (task.Result, int, error) {
	maxAttempts := s.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if t.Envelope.Budget.MaxAttempts > 0 {
		maxAttempts = t.Envelope.Budget.MaxAttempts
	}

	var ladder []model.Info
	if !t.Envelope.Binding.IsZero() && s.Registry != nil {
		if c, err := s.Registry.Candidates(t.Envelope.Binding); err == nil {
			ladder = c
		}
	}
	candidates := len(ladder)

	attempt := 1
	// Where this task enters the ladder. Zero — the bottom — unless a router
	// has evidence that the bottom rung would only charge for a call this
	// input was going to fail.
	escalation, routing := s.route(t, ladder, worker)
	// Whether a verdict has been reported for this task yet. The first one is
	// at the rung it entered on, which is what tells a profile how many
	// records a bucket holds — see route.Outcome.Start.
	var observed bool
	for {
		t.Attempt = attempt
		t.Escalation = escalation

		// Resolve the model for this attempt and pass admission control. The
		// admission is held across the call and released however it ends, so a
		// model's in-flight ceiling bounds the calls actually in flight rather
		// than the calls dispatched — and a backoff sleep between attempts
		// gives the slot back instead of sitting on it.
		release := noRelease
		if !t.Envelope.Binding.IsZero() && s.Registry != nil {
			info, err := s.Registry.Resolve(t.Envelope.Binding, escalation)
			if err != nil {
				return task.Result{}, attempt, core.Permanent(err)
			}
			t.ResolvedModel = info.ID
			if s.Limiter != nil {
				est := t.EstTokens
				if est <= 0 {
					est = 1
				}
				rel, err := s.Limiter.Acquire(ctx, info.ID, info.Limits, est)
				if err != nil {
					return task.Result{}, attempt, err
				}
				release = rel
			}
		}

		if s.Governor != nil && s.Governor.Exhausted() {
			release()
			return task.Result{}, attempt, core.BudgetExceeded(ErrBudgetExhausted)
		}
		if ctx.Err() != nil {
			release()
			return task.Result{}, attempt, core.Transient(ctx.Err())
		}

		s.publish(observe.Event{Type: observe.TaskStarted, RunID: t.Envelope.RunID,
			Stage: t.Stage, TaskID: t.ID, Worker: worker, Attempt: attempt,
			Model: t.ResolvedModel, Pane: t.Pane})

		res, err := s.Exec.Execute(ctx, t)
		release()
		// Charged before the outcome is read. A provider bills for the call it
		// answered, and whether that answer then survived parsing, validation,
		// or the rest of the stage is a fact about the result rather than about
		// the bill.
		s.charge(t, res)
		if err == nil {
			// A verdict, but only if a model produced it. A cache hit is a
			// replay of work some earlier task paid for at some other rung;
			// counting it as this rung succeeding would teach the router that
			// the cheap model handles everything the cache already holds.
			if !res.CacheHit {
				routing.observe(t, escalation, true, !observed)
				observed = true
			}
			s.publish(observe.Event{Type: observe.TaskCompleted, RunID: t.Envelope.RunID,
				Stage: t.Stage, TaskID: t.ID, Worker: worker, Attempt: attempt, Model: res.Model,
				Usage: res.Usage, Latency: res.Latency, Pane: t.Pane,
				Rung: escalation, Probe: routing.decision.Probe && !res.CacheHit,
				Output: recordsJSON(res.Output), OutIDs: recordIDs(res.Output)})
			return res, attempt, nil
		}

		if ctx.Err() != nil {
			return task.Result{}, attempt, err
		}

		class := core.ClassOf(err)
		// The output was produced and rejected: the one failure class that is
		// evidence about the model rather than about the network or the code.
		// It is recorded whether or not the task has attempts left, because a
		// task that runs out of retries still learned something true about
		// this rung.
		if class == core.FailSemantic {
			routing.observe(t, escalation, false, !observed)
			observed = true
		}
		retryable := class == core.FailTransient || class == core.FailSemantic
		if !retryable || attempt >= maxAttempts {
			s.publish(observe.Event{Type: observe.TaskFailed, RunID: t.Envelope.RunID,
				Stage: t.Stage, TaskID: t.ID, Worker: worker, Attempt: attempt,
				Rung: escalation, Pane: t.Pane, Err: err.Error(), Usage: res.Usage})
			return task.Result{}, attempt, err
		}

		note := string(class)
		if class == core.FailSemantic && escalation+1 < candidates {
			escalation++ // climb the model ladder for semantic failures
			note = fmt.Sprintf("semantic: escalating to ladder level %d", escalation)
		}
		s.publish(observe.Event{Type: observe.TaskRetried, RunID: t.Envelope.RunID,
			Stage: t.Stage, TaskID: t.ID, Worker: worker, Attempt: attempt, Err: err.Error(), Note: note})

		select {
		case <-time.After(s.Retry.Delay(attempt)):
		case <-ctx.Done():
			return task.Result{}, attempt, core.Transient(ctx.Err())
		}
		attempt++
	}
}

// charge records what one attempt cost against the run budget.
//
// Every attempt goes through here, successful or not. A call that was made
// was paid for: an output rejected by a validator, or one that parsed into
// nothing usable, costs exactly what a usable one costs, and the stage then
// climbs its ladder and pays again. Charging only the attempts that produced
// a result is what used to make that climb look free — the money left the
// account, and RunResult.Spent never mentioned it.
//
// A cache hit replays work some earlier task already paid for and carries no
// usage, so it charges nothing.
func (s *Scheduler) charge(t task.Task, res task.Result) {
	if s.Governor == nil || res.Usage == (core.Usage{}) {
		return
	}
	if err := s.Governor.Charge(res.Usage); err != nil {
		// What was spent stands; the governor stops *future* work.
		s.publish(observe.Event{Type: observe.BudgetExceeded,
			RunID: t.Envelope.RunID, Stage: t.Stage, TaskID: t.ID,
			Note: err.Error()})
	}
}

// route asks the router where this task should enter its ladder, and reports
// the rung and the feature bucket the decision was made in.
//
// A ladder with fewer than two rungs has nothing to decide, and a task with no
// binding calls no model at all; both skip the router entirely rather than ask
// it a question with one answer.
func (s *Scheduler) route(t task.Task, ladder []model.Info, worker string) (int, routing) {
	if s.Router == nil || len(ladder) < 2 {
		return 0, routing{}
	}
	// EstTokens is the planner's admission-control estimate, which reserves the
	// *maximum* output a call may produce. That makes it far larger than a
	// typical call and useless as an absolute price — but a router compares
	// rungs, and inflating every rung by the same factor leaves the comparison
	// where it was. Anything reported in dollars is measured from calls that
	// actually happened rather than from this.
	in, out := route.SplitTokens(t.EstTokens)
	d := s.Router.Route(route.Request{
		Stage:     t.Stage,
		Key:       t.ID,
		Rungs:     route.PriceLadder(ladder, in, out),
		Records:   t.Input,
		EstTokens: t.EstTokens,
	})
	rung := d.Rung
	if rung < 0 {
		rung = 0
	}
	if rung >= len(ladder) {
		rung = len(ladder) - 1
	}
	// Only a decision that changed something is worth an event. A router that
	// leaves every task where it was would otherwise double the event volume
	// of a run to say nothing happened.
	if rung > 0 || d.Probe {
		skipped := make([]string, 0, rung)
		for i := 0; i < rung; i++ {
			skipped = append(skipped, ladder[i].ID)
		}
		s.publish(observe.Event{Type: observe.TaskRouted, RunID: t.Envelope.RunID,
			Stage: t.Stage, TaskID: t.ID, Worker: worker, Model: ladder[rung].ID,
			Rung: rung, Probe: d.Probe, Bucket: d.Bucket, Skipped: skipped,
			Note: d.Reason})
	}
	d.Rung = rung
	return rung, routing{router: s.Router, decision: d}
}

// routing is a task's live relationship with the router: the decision that was
// made about it, and the router to report back to.
//
// Its zero value is a task the router was never asked about — a stage with no
// ladder, or a scheduler with no router — and it swallows verdicts rather than
// recording them. That matters more than it looks: a one-rung stage that fed
// the profile would be recording evidence about a choice nobody made, and a
// later edit adding an escalation to that stage would read it as though
// somebody had.
type routing struct {
	router   route.Router
	decision route.Decision
}

// observe feeds one verdict back to the router that asked for it. start marks
// the task's first verdict, at the rung it entered the ladder on.
func (r routing) observe(t task.Task, rung int, valid, start bool) {
	if r.router == nil {
		return
	}
	r.router.Observe(route.Outcome{Stage: t.Stage, Bucket: r.decision.Bucket,
		Rung: rung, Valid: valid, Start: start, Probe: r.decision.Probe})
}

func (s *Scheduler) publish(e observe.Event) {
	if s.Bus != nil {
		s.Bus.Publish(e)
	}
}

// recordsJSON renders records as indented JSON for observability consumers,
// clipped to the observe payload cap.
func recordsJSON(recs []core.Record) string {
	if len(recs) == 0 {
		return ""
	}
	b, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return fmt.Sprintf("[unserializable input: %v]", err)
	}
	return observe.Clip(string(b))
}

// recordIDs lists record IDs, the lineage currency between tasks.
func recordIDs(recs []core.Record) []string {
	if len(recs) == 0 {
		return nil
	}
	ids := make([]string, len(recs))
	for i, r := range recs {
		ids[i] = r.ID
	}
	return ids
}
