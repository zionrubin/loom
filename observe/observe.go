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
	// BlackboardPosted announces one entry appended to a fleet's blackboard: a
	// conclusion one agent published for later agents to read. It carries no
	// RunID, because it belongs to the fleet rather than to any single run —
	// the agent that posted it has usually finished, and the agents that will
	// read it have not started.
	BlackboardPosted EventType = "blackboard.posted"
	// FindingServed reports the shared research layer answering a question from
	// what another agent already learned, instead of reaching a public source.
	// Note carries how it matched (exact, class, near, coalesced), Saved and
	// Usage what the avoided research originally cost, Artifact the finding's
	// content hash, and Latency the served finding's age — which is the number
	// that says whether a hit was fresh knowledge or an old one still inside
	// its topic's horizon.
	FindingServed EventType = "finding.served"
	// FindingLearned reports a new finding contributed to the commons, with
	// Usage the research it cost. Every later FindingServed naming this
	// Artifact is that cost avoided again.
	FindingLearned EventType = "finding.learned"
	// FindingCoalesced reports a duplicate question collapsing into a call
	// another task already had in flight — the saving a result cache cannot
	// make, because it serves the second asker only after the first has
	// finished, and these two asked at the same moment. Note distinguishes the
	// two scopes it can happen in: "coalesced" is two agents in this process,
	// "remote-coalesced" is two executors on different machines.
	FindingCoalesced EventType = "finding.coalesced"
	// FindingPublished reports a finding written to the shared backend, where
	// executors this process knows nothing about can be served it. It is the
	// counterpart of FindingLearned one layer out: learned says this process
	// paid for the research, published says the fleet now holds it.
	FindingPublished EventType = "finding.published"
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
	// RoundStarted and RoundFinished bracket one superstep of an iterative
	// stage (pipeline.Iterate): how many vertices are active, how many
	// messages reached them, and what the round cost.
	//
	// They are the only events that describe a stage running more than once,
	// and the pair is what makes convergence observable rather than inferable
	// — a frontier that shrinks round over round is a computation settling,
	// and one that does not is the case the round cap exists for.
	RoundStarted  EventType = "round.started"
	RoundFinished EventType = "round.finished"
	// MCPConnected announces one MCP server the host has connected to: its
	// transport, the tools it offers, the digest plans are compiled against,
	// and the ceiling on concurrent calls to it.
	//
	// It is the only event on this bus that carries no RunID, and that is what
	// it means: a connection is made before any run starts and outlives every
	// run on the host, so it belongs to the host rather than to a run.
	// Observers should hold it beside the universe rather than inside one sky.
	MCPConnected EventType = "mcp.connected"
	// MCPCalled reports one tool call to an MCP server: which server, which
	// tool, how long it waited for a call slot, how long the call took, and
	// whether it failed. It is the only event on this bus that describes work
	// with no token cost, which is the point — a stage's wall-clock can be
	// dominated by tools that cost nothing, and the cost report alone would
	// never show it.
	MCPCalled EventType = "mcp.called"
	// StageConverged closes an iterative stage with the reason it stopped
	// (Note), the number of rounds it took, and the size of the graph it left
	// behind. Convergence and exhaustion produce the same records, so the
	// reason is the only thing that distinguishes them.
	StageConverged EventType = "stage.converged"

	// DeltaSpliced is a context materialized from state this process already
	// held, plus the change: Retained bytes were reused under a certificate and
	// Repaired bytes re-rendered.
	DeltaSpliced EventType = "delta.spliced"
	// DeltaRebuilt is a context rendered in full. It is the reference path, and
	// it is not a failure — Note carries the reason, which is as likely to be
	// "the router chose it" as "this worker has never seen this session".
	DeltaRebuilt EventType = "delta.rebuilt"
	// DeltaDiverged is a certified splice that disagreed with a full render.
	//
	// It is the only event in this list that means something is wrong rather
	// than that something happened. The renderer that produced it is
	// quarantined for the life of the process, every later context is rendered
	// in full, and the run continues on exact results — but a divergence says a
	// renderer is not what this system assumed it was, and no amount of
	// falling back makes that uninteresting.
	DeltaDiverged EventType = "delta.diverged"
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
	Broadcast string `json:"broadcast,omitempty"`
	Artifact  string `json:"artifact,omitempty"`
	Bytes     int    `json:"bytes,omitempty"`
	// Topic names the blackboard topic on blackboard.posted, and Posts is how
	// many entries it holds after the append — together they identify the
	// snapshot (topic@n) that later agents pin, with Artifact its content hash.
	Topic string `json:"topic,omitempty"`
	Posts int    `json:"posts,omitempty"`
	// Server and Tool name the MCP server and tool on mcp.* events.
	Server string `json:"server,omitempty"`
	Tool   string `json:"tool,omitempty"`
	// Queued is how long a tool call waited for one of its server's call
	// slots before it could run (mcp.called). It is the number that says
	// whether a server's concurrency bound is the pipeline's bottleneck —
	// latency alone cannot distinguish a slow server from a busy one.
	Queued time.Duration `json:"queued,omitempty"`
	// InFlight is how many calls that server was carrying when this one
	// started, and Slots its ceiling (mcp.connected carries Slots alone).
	// Together they are the occupancy of the semaphore a task leases from,
	// which is what this design rations instead of connections.
	InFlight int `json:"in_flight,omitempty"`
	Slots    int `json:"slots,omitempty"`
	// Continuation is the evolving context's key on delta.* events, with
	// Artifact its revision hash and Detail the renderer version.
	//
	// Base is the rendered size of the state the materialization built on and
	// Delta the source bytes of change since it — the two numbers the router
	// compared. Retained is what was reused without rendering it again and
	// Repaired what had to be rendered; on a rebuild the first is zero and the
	// second is the whole context, which is exactly how a report should read.
	// Window is how many already-rendered segments the repair covered.
	Continuation string `json:"continuation,omitempty"`
	Base         int    `json:"base,omitempty"`
	Delta        int    `json:"delta,omitempty"`
	Retained     int    `json:"retained,omitempty"`
	Repaired     int    `json:"repaired,omitempty"`
	Window       int    `json:"window,omitempty"`
	// Round is the 1-based superstep number on round.* events, and the total
	// number of rounds on stage.converged. Messages is how many messages were
	// delivered into the round.
	Round    int           `json:"round,omitempty"`
	Messages int           `json:"messages,omitempty"`
	Usage    core.Usage    `json:"usage,omitempty"`
	Latency  time.Duration `json:"latency,omitempty"`
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
	// Rounds counts the supersteps an iterative stage ran (0 for every other
	// kind of stage). Tasks and cost are already per stage; this is what says
	// whether they were spent once or ten times over.
	Rounds int
	// Splices and Rebuilds count how this stage's evolving contexts were
	// materialized, and RetainedBytes totals what the splices did not have to
	// render. Read together they are the delta layer's whole claim: a stage
	// with rebuilds and no splices is paying full price every round, and one
	// with splices whose retained bytes are small is splicing contexts too
	// small to be worth it.
	Splices       int
	Rebuilds      int
	RetainedBytes int64
	// ToolCalls counts MCP tool calls the stage's tasks made, and ToolTime the
	// wall-clock they took. They buy nothing in tokens and can dominate a
	// stage's duration, so a report that only totalled cost would explain a
	// slow stage as a fast one.
	ToolCalls int
	ToolTime  time.Duration
	Started   time.Time
	Finished  time.Time

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

// ToolCalls sums the MCP tool calls the run made and the time they took.
func (r RunReport) ToolCalls() (int, time.Duration) {
	var calls int
	var dur time.Duration
	for _, s := range r.Stages {
		calls += s.ToolCalls
		dur += s.ToolTime
	}
	return calls, dur
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
	// Reported on tokens rather than on dollars, because the two come apart:
	// a model running on local hardware serves a shared prefix from its KV
	// cache and saves real work for exactly $0, and a report that mentioned
	// the cache only when it saved money would say nothing about the run
	// where it did the most.
	if t.CacheReadTokens > 0 || t.CacheWriteTokens > 0 {
		fmt.Fprintf(&b, "prefix cache: %d tokens served from shared prefixes, $%.4f saved\n",
			t.CacheReadTokens, r.PrefixSavedUSD())
	}
	for _, s := range r.Stages {
		if s.Rounds > 0 {
			fmt.Fprintf(&b, "%s: %d rounds\n", s.Stage, s.Rounds)
		}
	}
	if calls, dur := r.ToolCalls(); calls > 0 {
		fmt.Fprintf(&b, "mcp: %d tool call(s), %s spent in them (no tokens, no cost)\n",
			calls, dur.Round(time.Millisecond))
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
	case RoundFinished:
		c.stage(e.Stage).Rounds++
	case DeltaSpliced:
		s := c.stage(e.Stage)
		s.Splices++
		s.RetainedBytes += int64(e.Retained)
	case DeltaRebuilt:
		c.stage(e.Stage).Rebuilds++
	case MCPCalled:
		s := c.stage(e.Stage)
		s.ToolCalls++
		s.ToolTime += e.Latency
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
