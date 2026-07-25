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

// RateLimiter provides per-model token buckets for requests/min and
// tokens/min. Acquire blocks until admission is possible (or ctx ends), so
// the scheduler never dispatches work the provider would immediately 429.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	req, tok         float64
	reqCap, tokCap   float64
	reqRate, tokRate float64 // per second
	last             time.Time
}

// NewRateLimiter returns an empty limiter; buckets are created on first use.
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{buckets: map[string]*bucket{}}
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
// blocking as needed. Zero limits admit immediately.
func (l *RateLimiter) Acquire(ctx context.Context, modelID string, lim model.Limits, estTokens int) error {
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
			Stage: t.Stage, TaskID: t.ID, Records: len(t.Input),
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

// runTask executes one task through its full recovery lifecycle.
func (s *Scheduler) runTask(ctx context.Context, t task.Task, worker string) (task.Result, int, error) {
	maxAttempts := s.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if t.Envelope.Budget.MaxAttempts > 0 {
		maxAttempts = t.Envelope.Budget.MaxAttempts
	}

	var candidates int
	if !t.Envelope.Binding.IsZero() && s.Registry != nil {
		if c, err := s.Registry.Candidates(t.Envelope.Binding); err == nil {
			candidates = len(c)
		}
	}

	attempt := 1
	escalation := 0
	for {
		t.Attempt = attempt
		t.Escalation = escalation

		// Resolve the model for this attempt and pass admission control.
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
				if err := s.Limiter.Acquire(ctx, info.ID, info.Limits, est); err != nil {
					return task.Result{}, attempt, err
				}
			}
		}

		if s.Governor != nil && s.Governor.Exhausted() {
			return task.Result{}, attempt, core.BudgetExceeded(ErrBudgetExhausted)
		}
		if ctx.Err() != nil {
			return task.Result{}, attempt, core.Transient(ctx.Err())
		}

		s.publish(observe.Event{Type: observe.TaskStarted, RunID: t.Envelope.RunID,
			Stage: t.Stage, TaskID: t.ID, Worker: worker, Attempt: attempt, Model: t.ResolvedModel})

		res, err := s.Exec.Execute(ctx, t)
		if err == nil {
			if s.Governor != nil && res.Usage.Requests > 0 {
				if cerr := s.Governor.Charge(res.Usage); cerr != nil {
					// The result stands; the governor aborts *future* work.
					s.publish(observe.Event{Type: observe.BudgetExceeded,
						RunID: t.Envelope.RunID, Stage: t.Stage, TaskID: t.ID,
						Note: cerr.Error()})
				}
			}
			s.publish(observe.Event{Type: observe.TaskCompleted, RunID: t.Envelope.RunID,
				Stage: t.Stage, TaskID: t.ID, Worker: worker, Attempt: attempt, Model: res.Model,
				Usage: res.Usage, Latency: res.Latency,
				Output: recordsJSON(res.Output), OutIDs: recordIDs(res.Output)})
			return res, attempt, nil
		}

		if ctx.Err() != nil {
			return task.Result{}, attempt, err
		}

		class := core.ClassOf(err)
		retryable := class == core.FailTransient || class == core.FailSemantic
		if !retryable || attempt >= maxAttempts {
			s.publish(observe.Event{Type: observe.TaskFailed, RunID: t.Envelope.RunID,
				Stage: t.Stage, TaskID: t.ID, Worker: worker, Attempt: attempt, Err: err.Error()})
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
