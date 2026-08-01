// Package observe provides Loom's observability spine: a typed event bus for
// every lifecycle transition (runs, stages, tasks, model calls, cache hits,
// budget trips) and a collector that folds the event stream into a RunReport
// with per-stage throughput, latency percentiles, retries, cache hit rates,
// token usage, and dollar cost.
package observe

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
)

// EventType enumerates lifecycle events.
type EventType string

const (
	RunStarted     EventType = "run.started"
	RunFinished    EventType = "run.finished"
	StageStarted   EventType = "stage.started"
	StageFinished  EventType = "stage.finished"
	TaskScheduled  EventType = "task.scheduled"
	TaskStarted    EventType = "task.started"
	TaskCompleted  EventType = "task.completed"
	TaskRetried    EventType = "task.retried"
	TaskFailed     EventType = "task.failed"
	ModelCalled    EventType = "model.called"
	CacheHit       EventType = "cache.hit"
	BudgetExceeded EventType = "budget.exceeded"
	// BroadcastRegistered announces one run-level shared value, once, before
	// any task reads it.
	BroadcastRegistered EventType = "broadcast.registered"
	// BroadcastRead reports a task reaching a shared value, once per task and
	// name — the "where" of a broadcast, as distinct from the "what".
	BroadcastRead EventType = "broadcast.read"
	// StageProjected and RunProjected carry a pre-flight cost projection
	// (loom.Explain): what a stage and a whole pipeline are expected to spend,
	// published before anything is spent. They are the only events on this bus
	// that describe work which has not happened, which is why they carry a
	// Ceiling beside Usage — one is an estimate, the other a bound.
	//
	// Emitting them on the same bus the run uses is what lets an observer hold
	// both halves of the comparison: point loom.Explain and loom.Run at one
	// handler and it sees expected against actual, live.
	StageProjected EventType = "stage.projected"
	RunProjected   EventType = "run.projected"
)

// Event is one observation. Fields are populated as relevant per type.
type Event struct {
	Type  EventType `json:"type"`
	Time  time.Time `json:"time"`
	RunID string    `json:"run_id,omitempty"`
	// Pipeline names the pipeline the run (or projection) belongs to. A
	// process often runs several — a fan-out run and the run that fuses its
	// results, say — and a run ID alone does not say which is which, so an
	// observer holding more than one run needs the name to tell them apart.
	Pipeline string   `json:"pipeline,omitempty"`
	Stage    string   `json:"stage,omitempty"`
	Upstream string   `json:"upstream,omitempty"` // stage.started: upstream stage ID
	Kind     string   `json:"kind,omitempty"`     // stage.started: stage kind (infer, fused, …)
	Detail   string   `json:"detail,omitempty"`   // stage.started: human-readable stage spec
	TaskID   string   `json:"task_id,omitempty"`
	Worker   string   `json:"worker,omitempty"` // scheduler worker executing the task
	Model    string   `json:"model,omitempty"`
	Attempt  int      `json:"attempt,omitempty"`
	Records  int      `json:"records,omitempty"`    // task.scheduled: input record count
	Input    string   `json:"input,omitempty"`      // task.scheduled: input records JSON (clipped)
	Output   string   `json:"output,omitempty"`     // task.completed: output records JSON (clipped)
	InputIDs []string `json:"input_ids,omitempty"`  // task.scheduled: input record IDs (lineage)
	OutIDs   []string `json:"output_ids,omitempty"` // task.completed: output record IDs (lineage)
	Prompt   string   `json:"prompt,omitempty"`     // model.called: rendered request (clipped)
	Response string   `json:"response,omitempty"`   // model.called: response text (clipped)
	// Broadcast names the shared value on broadcast.* events; Artifact is its
	// content hash and Bytes its serialized size (broadcast.registered carries
	// the value itself in Detail, clipped).
	Broadcast string        `json:"broadcast,omitempty"`
	Artifact  string        `json:"artifact,omitempty"`
	Bytes     int           `json:"bytes,omitempty"`
	Usage     core.Usage    `json:"usage,omitempty"`
	Latency   time.Duration `json:"latency,omitempty"`
	// Saved is what this model call's prompt-prefix cache activity was worth
	// in dollars against paying the full input rate (model.called). Negative
	// while a freshly written entry is still unamortized.
	Saved float64 `json:"saved,omitempty"`
	Err   string  `json:"err,omitempty"`
	Note  string  `json:"note,omitempty"`
	// Ceiling bounds a projection (stage.projected / run.projected): the same
	// accounting as Usage with every response filling MaxTokens, which the
	// provider enforces. Usage on those events is the expected case and rests
	// on an assumption about response length; this rests on nothing.
	Ceiling core.Usage `json:"ceiling,omitempty"`
	// Budget is the run budget a projection was measured against
	// (run.projected), so an observer can say whether it covers the ceiling.
	Budget core.Budget `json:"budget,omitempty"`
}

// PayloadCap bounds the large observability payloads (record JSON, prompts,
// responses) so events stay shippable; Clip enforces it.
const PayloadCap = 16 << 10

// Clip truncates s to PayloadCap runes, appending a marker when it does.
func Clip(s string) string {
	if len(s) <= PayloadCap { // fast path: byte length bounds rune length
		return s
	}
	runes := []rune(s)
	if len(runes) <= PayloadCap {
		return s
	}
	return string(runes[:PayloadCap]) + "\n… [truncated]"
}

// Bus fans events out to synchronous handlers (deterministic, used by the
// collector) and asynchronous channel subscribers (for external consumers;
// sends are non-blocking and drop when a subscriber's buffer is full, so a
// slow consumer can never stall the pipeline).
type Bus struct {
	mu       sync.RWMutex
	handlers []func(Event)
	subs     []chan Event
	closed   bool
}

// NewBus returns an empty bus.
func NewBus() *Bus { return &Bus{} }

// On attaches a synchronous handler invoked inline on every publish.
func (b *Bus) On(fn func(Event)) {
	b.mu.Lock()
	b.handlers = append(b.handlers, fn)
	b.mu.Unlock()
}

// Subscribe returns a buffered channel of events. Events are dropped rather
// than blocking when the buffer is full.
func (b *Bus) Subscribe(buffer int) <-chan Event {
	ch := make(chan Event, buffer)
	b.mu.Lock()
	b.subs = append(b.subs, ch)
	b.mu.Unlock()
	return ch
}

// Publish delivers an event, stamping Time if zero.
func (b *Bus) Publish(e Event) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	for _, h := range b.handlers {
		h(e)
	}
	for _, ch := range b.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

// Close closes subscriber channels; further publishes are no-ops.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, ch := range b.subs {
		close(ch)
	}
}

// StageStats aggregates one stage's execution.
type StageStats struct {
	Stage      string
	Tasks      int
	Completed  int
	Failed     int
	Retries    int
	CacheHits  int
	ModelCalls int
	Usage      core.Usage
	// PrefixSavedUSD is what this stage's shared prompt prefix was worth:
	// the difference between what its cached prompt tokens cost and what
	// they would have cost at the full input rate.
	PrefixSavedUSD float64
	Started        time.Time
	Finished       time.Time

	latencies []time.Duration
}

// Duration is wall time from stage start to finish.
func (s *StageStats) Duration() time.Duration {
	if s.Started.IsZero() || s.Finished.IsZero() {
		return 0
	}
	return s.Finished.Sub(s.Started)
}

// LatencyP50 returns the median model-call latency.
func (s *StageStats) LatencyP50() time.Duration { return s.percentile(0.50) }

// LatencyP95 returns the 95th percentile model-call latency.
func (s *StageStats) LatencyP95() time.Duration { return s.percentile(0.95) }

func (s *StageStats) percentile(p float64) time.Duration {
	if len(s.latencies) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(s.latencies))
	copy(sorted, s.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

// RunReport is the aggregate view of one run.
type RunReport struct {
	RunID    string
	Started  time.Time
	Finished time.Time
	Stages   []*StageStats
}

// Totals sums usage across stages.
func (r RunReport) Totals() core.Usage {
	var u core.Usage
	for _, s := range r.Stages {
		u.Add(s.Usage)
	}
	return u
}

// PrefixSavedUSD sums what prompt-prefix caching was worth across the run.
func (r RunReport) PrefixSavedUSD() float64 {
	var total float64
	for _, s := range r.Stages {
		total += s.PrefixSavedUSD
	}
	return total
}

// Duration is total run wall time.
func (r RunReport) Duration() time.Duration {
	if r.Started.IsZero() || r.Finished.IsZero() {
		return 0
	}
	return r.Finished.Sub(r.Started)
}

// String renders a human-readable report table.
func (r RunReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "run %s  (%s)\n", r.RunID, r.Duration().Round(time.Millisecond))
	fmt.Fprintf(&b, "%-22s %6s %6s %6s %6s %6s %8s %7s %10s %10s\n",
		"stage", "tasks", "ok", "fail", "retry", "cache", "tokens", "prefix", "cost($)", "p95")
	for _, s := range r.Stages {
		fmt.Fprintf(&b, "%-22s %6d %6d %6d %6d %6d %8d %6.0f%% %10.4f %10s\n",
			s.Stage, s.Tasks, s.Completed, s.Failed, s.Retries, s.CacheHits,
			s.Usage.TotalTokens(), 100*s.Usage.CacheHitRate(),
			s.Usage.CostUSD, s.LatencyP95().Round(time.Millisecond))
	}
	t := r.Totals()
	fmt.Fprintf(&b, "%-22s %6s %6s %6s %6s %6s %8d %6.0f%% %10.4f\n",
		"TOTAL", "", "", "", "", "", t.TotalTokens(), 100*t.CacheHitRate(), t.CostUSD)
	if saved := r.PrefixSavedUSD(); saved != 0 {
		fmt.Fprintf(&b, "prefix cache: %d tokens served from shared prefixes, $%.4f saved\n",
			t.CacheReadTokens, saved)
	}
	return b.String()
}

// Collector folds events into a RunReport. Attach via bus.On(c.Handle).
type Collector struct {
	mu       sync.Mutex
	runID    string
	started  time.Time
	finished time.Time
	stages   map[string]*StageStats
	order    []string
}

// NewCollector returns an empty collector.
func NewCollector() *Collector {
	return &Collector{stages: map[string]*StageStats{}}
}

func (c *Collector) stage(name string) *StageStats {
	s, ok := c.stages[name]
	if !ok {
		s = &StageStats{Stage: name}
		c.stages[name] = s
		c.order = append(c.order, name)
	}
	return s
}

// Handle consumes one event.
func (c *Collector) Handle(e Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch e.Type {
	case RunStarted:
		c.runID = e.RunID
		c.started = e.Time
	case RunFinished:
		c.finished = e.Time
	case StageStarted:
		c.stage(e.Stage).Started = e.Time
	case StageFinished:
		c.stage(e.Stage).Finished = e.Time
	case TaskStarted:
		if e.Attempt <= 1 {
			c.stage(e.Stage).Tasks++
		}
	case TaskCompleted:
		c.stage(e.Stage).Completed++
	case TaskFailed:
		c.stage(e.Stage).Failed++
	case TaskRetried:
		c.stage(e.Stage).Retries++
	case CacheHit:
		c.stage(e.Stage).CacheHits++
	case ModelCalled:
		s := c.stage(e.Stage)
		s.ModelCalls++
		s.Usage.Add(e.Usage)
		s.PrefixSavedUSD += e.Saved
		s.latencies = append(s.latencies, e.Latency)
	}
}

// Report returns a snapshot of the aggregated run.
func (c *Collector) Report() RunReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	rep := RunReport{RunID: c.runID, Started: c.started, Finished: c.finished}
	for _, name := range c.order {
		s := c.stages[name]
		cp := *s
		cp.latencies = append([]time.Duration(nil), s.latencies...)
		rep.Stages = append(rep.Stages, &cp)
	}
	return rep
}
