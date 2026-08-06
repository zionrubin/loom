// Package loom is an AI-native data processing framework: MapReduce-style
// declarative pipelines where the operators are model calls, executors are
// provisioned per task with exactly the model, secrets, tools, network
// access, context, and budget the task needs, and the scheduler understands
// the realities of AI workloads — rate limits, dollar budgets, semantic
// failures, and content-addressed caching.
//
// This package wires the pieces (pipeline → plan → scheduler → executor)
// into a single entry point, Run.
package loom

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/memory"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/task"
)

// Config controls a run. Build it with Options passed to Run.
type Config struct {
	Workers         int
	Retry           runtime.RetryPolicy
	RunBudget       core.Budget
	Registry        *model.Registry
	Secrets         map[security.SecretRef]string
	EgressAllow     []string
	StateDir        string
	ContinueOnError bool
	Tools           []executor.Tool
	Broadcasts      map[string]any
	Topics          map[string]bool
	Memory          memory.Store
	Embedder        memory.Embedder
	NoMemoryCommit  bool
	EventHandler    func(observe.Event)
	Streaming       bool
	BatchWait       time.Duration
	// AdmissionAging tunes a fleet's slot-admission fairness (zero = the
	// runtime default). It has no effect on a single Run, whose tasks all
	// belong to one program and therefore tie.
	AdmissionAging float64

	// Explain-only settings. They configure the pre-flight projection and are
	// ignored by Run, so one config can describe a run and be asked what that
	// run would cost.
	ExpectedOutput float64
	SourceSamples  map[string][]core.Record
	StageSamples   map[string]map[string]any
}

// Option configures a run.
type Option func(*Config)

// WithWorkers sets default concurrency (default 8).
func WithWorkers(n int) Option { return func(c *Config) { c.Workers = n } }

// WithRetry overrides the retry policy.
func WithRetry(p runtime.RetryPolicy) Option { return func(c *Config) { c.Retry = p } }

// WithRunBudget caps total run spend (cost and/or tokens); when exhausted
// the run stops admitting work and returns partial results.
func WithRunBudget(b core.Budget) Option { return func(c *Config) { c.RunBudget = b } }

// WithRegistry supplies the model registry.
func WithRegistry(r *model.Registry) Option { return func(c *Config) { c.Registry = r } }

// WithSecrets loads secrets into the run's broker. Tasks can resolve only
// the secrets their envelopes grant.
func WithSecrets(s map[security.SecretRef]string) Option {
	return func(c *Config) { c.Secrets = s }
}

// WithEgress allows additional egress hosts beyond the providers'
// endpoints (which are allowed automatically per stage, least-privilege).
func WithEgress(hosts ...string) Option {
	return func(c *Config) { c.EgressAllow = append(c.EgressAllow, hosts...) }
}

// WithStateDir enables persistent state under dir: the content-addressed
// artifact store and result cache survive process restarts, making reruns
// resume completed AI work instead of re-spending tokens.
func WithStateDir(dir string) Option { return func(c *Config) { c.StateDir = dir } }

// WithContinueOnError dead-letters failing tasks and continues instead of
// aborting the run on first failure.
func WithContinueOnError() Option { return func(c *Config) { c.ContinueOnError = true } }

// WithTools registers tools ops may invoke when granted the matching
// capability.
func WithTools(tools ...executor.Tool) Option {
	return func(c *Config) { c.Tools = append(c.Tools, tools...) }
}

// WithBroadcast registers a value shared by every task that declares it with
// pipeline.WithBroadcast — a lookup table, a taxonomy, a rubric, anything the
// whole run needs to agree on.
//
// The value is serialized and stored once by content hash; task envelopes
// carry the hash rather than the bytes, so a large shared table costs one copy
// per run instead of one per task, and the tasks stay small enough to ship to
// a remote executor. Values must be JSON-serializable, and are read-only:
// every reader in the run sees the same value, and mutating it is a data race.
//
// Because the content hash is part of each reading stage's fingerprint,
// editing a broadcast invalidates exactly the cached results that could have
// seen it and leaves the rest of the run's cache warm.
func WithBroadcast(name string, value any) Option {
	return func(c *Config) {
		if c.Broadcasts == nil {
			c.Broadcasts = map[string]any{}
		}
		c.Broadcasts[name] = value
	}
}

// WithMemory attaches a long-term knowledge base to the run: a durable store
// of facts that outlives it, retrieved by meaning rather than by name, and
// shared with every other pipeline pointed at the same store.
//
// It is the third and longest-lived of Loom's sharing mechanisms. A broadcast
// is one value, fixed before the run and read whole; a fleet's blackboard is
// one process's agents reaching each other's conclusions; memory is what an
// application knows, accumulated across runs and machines and months.
//
// Two rules keep it compatible with content-addressed replay, and both are
// consequences of the store being versioned rather than merely mutable:
//
//   - Reads are pinned. The run fixes each space's epoch before its first task
//     and reads it there throughout, so a commit landing mid-run — from
//     another process sharing the store, say — cannot change what this run
//     retrieves. The pinned epoch joins the fingerprint of every stage that
//     reads the space.
//   - Writes are staged. Remember stages an item for the next epoch; nothing
//     the run writes is visible to the run that wrote it. A standalone Run
//     commits what it staged when it finishes; on a fleet, where several
//     agents share the store, the commit belongs to the fleet's owner
//     (Fleet.CommitMemory) for the same reason posting to the blackboard does.
//
// embedder may be nil, in which case a memory.HashEmbedder is used — offline
// and deterministic, ideal for tests and development, and lexical rather than
// semantic, so anything whose recall quality matters wants a real one.
func WithMemory(store memory.Store, embedder memory.Embedder) Option {
	return func(c *Config) {
		c.Memory = store
		if embedder != nil {
			c.Embedder = embedder
		}
	}
}

// WithoutMemoryCommit leaves a standalone run's staged memory writes
// uncommitted, so nothing it learned becomes visible to later runs.
//
// It is for the run that should read the knowledge base without being trusted
// to extend it: an evaluation, a dry run, a backfill being inspected before
// publication. The items stay staged in the store and a later CommitMemory
// publishes them.
func WithoutMemoryCommit() Option { return func(c *Config) { c.NoMemoryCommit = true } }

// WithFleetBudget caps what a whole fleet may spend, across every agent on it.
//
// It is WithRunBudget under the name that says what it means on a fleet: the
// governor is shared, so the ceiling is the fleet's rather than each agent's.
// That distinction is the reason a fleet exists — a budget enforced once per
// pipeline is not a budget on the work, it is a budget multiplied by however
// many pipelines happen to be running.
func WithFleetBudget(b core.Budget) Option { return WithRunBudget(b) }

// WithAdmissionAging tunes how fast a fleet's queued tasks earn priority
// credit for waiting (default runtime.DefaultAging).
//
// A fleet admits a contended slot to the agent that has been served least, so
// a short agent overtakes a long one instead of queueing behind it. Aging is
// what bounds the other side of that trade: an agent is held back by at most
// its own attained service divided by this rate, however many fresh agents
// arrive while it waits. Raise it for a fleet that agents keep joining
// indefinitely, where the incumbents need protecting; leave it alone for a
// fleet whose agents are launched together and drain.
func WithAdmissionAging(rate float64) Option {
	return func(c *Config) { c.AdmissionAging = rate }
}

// WithTopic declares a blackboard topic on a fleet, so an agent may read it
// before anything has been posted to it.
//
// Posting to a topic declares it too (see Fleet.Post). Declaring it up front is
// for the agent that runs first: it should find an empty board rather than fail
// to compile against a name nobody has defined yet.
func WithTopic(names ...string) Option {
	return func(c *Config) {
		if c.Topics == nil {
			c.Topics = map[string]bool{}
		}
		for _, n := range names {
			c.Topics[n] = true
		}
	}
}

// WithStreaming replaces the stage-barrier driver with pipelined execution:
// a record becomes eligible for the next stage the moment its own task
// completes, instead of when its whole stage does.
//
// The pipeline, planner, envelopes, caching, and recovery are identical —
// only the driver changes. What changes observably is occupancy and latency:
// downstream stages start while upstream ones are still running, so a slow
// task no longer idles the workers behind it, and the first end-to-end result
// arrives without waiting for the widest stage to drain.
//
// The tradeoff is ordering. Records flow in completion order rather than
// input order, so a stage's outputs are no longer guaranteed to line up with
// its inputs positionally. Stages that must see the whole dataset — Combine
// and ReduceAI — remain natural barriers and still do. Use the default
// barrier driver when output order is part of the contract.
func WithStreaming() Option { return func(c *Config) { c.Streaming = true } }

// WithBatchWait bounds how long a streaming stage with WithBatchSize waits
// for a partial batch to fill before sending it anyway (default 25ms).
//
// It is the knob that keeps batching from reintroducing the barrier it was
// meant to remove: without a deadline, the last few records of a stream would
// wait indefinitely for a group that will never arrive.
func WithBatchWait(d time.Duration) Option { return func(c *Config) { c.BatchWait = d } }

// WithEventHandler attaches a synchronous observer of all run events.
func WithEventHandler(fn func(observe.Event)) Option {
	return func(c *Config) { c.EventHandler = fn }
}

// RunResult is the outcome of a run: outputs, telemetry, dead letters,
// lineage, and the audit trail.
type RunResult struct {
	RunID        string
	Output       []core.Record            // output of the terminal stage (if exactly one)
	StageOutputs map[string][]core.Record // outputs of every stage
	Report       observe.RunReport
	Failures     []runtime.Failure
	Lineage      []store.LineageEntry
	Audit        []security.AuditEntry
	Broadcasts   map[string]string // shared value name → content hash
	// Memory reports the long-term memory spaces this run read and the epoch
	// it pinned each at — the version of the knowledge base every result below
	// was computed against.
	Memory map[string]uint64
	// Committed reports the epochs each space reached when the run published
	// what it staged. Empty when the run wrote nothing, or when committing was
	// left to the caller (WithoutMemoryCommit, or a fleet).
	Committed map[string]uint64
	Spent     core.Usage
	// Iterations reports how each iterative stage ran: rounds, per-round
	// frontier sizes, and which bound halted it. Empty for a pipeline with no
	// Iterate stage.
	Iterations []IterationReport
}

// Iteration returns the report for one iterative stage.
func (r *RunResult) Iteration(stage string) (IterationReport, bool) {
	for _, it := range r.Iterations {
		if it.Stage == stage {
			return it, true
		}
	}
	return IterationReport{}, false
}

// Run executes a pipeline to completion (or budget/failure abort, in which
// case partial results are returned along with the error).
//
// Everything a run needs is provisioned for it and released afterwards: a rate
// limiter, a budget governor, a result cache, a set of execution slots. That is
// the right scope for one pipeline. When a process runs several at once those
// same things need to be shared instead — one quota, one ceiling, one cache,
// one bounded set of slots scheduled fairly between them — which is what a
// Fleet is. Run is a fleet of one, built through the same path so the two
// cannot drift apart.
func Run(ctx context.Context, p *pipeline.Pipeline, opts ...Option) (*RunResult, error) {
	cfg := Config{Workers: 8, Retry: runtime.DefaultRetry}
	for _, o := range opts {
		o(&cfg)
	}
	h, err := newHost(cfg)
	if err != nil {
		return nil, err
	}
	defer h.close()
	return h.launch(ctx, core.NewID("run"), p, h.cfg, nil)
}

// driver holds the state a run's execution strategy accumulates. Both
// strategies — the stage-barrier driver below and the streaming driver in
// stream.go — fill the same fields from the same plan, scheduler, and event
// bus, so which one ran is invisible to everything downstream of Run.
type driver struct {
	plan  *plan.Plan
	runID string
	cfg   Config
	sched runtime.Scheduler
	bus   *observe.Bus
	// pool, when set, is the slot pool this run shares with the other agents
	// of a fleet. Nil means the streaming driver provisions its own.
	pool *runtime.Pool

	mu         sync.Mutex
	outputs    map[string][]core.Record
	failures   []runtime.Failure
	iterations []IterationReport
}

func (d *driver) record(stage string, recs []core.Record) {
	d.mu.Lock()
	d.outputs[stage] = recs
	d.mu.Unlock()
}

func (d *driver) fail(fails ...runtime.Failure) {
	if len(fails) == 0 {
		return
	}
	d.mu.Lock()
	d.failures = append(d.failures, fails...)
	d.mu.Unlock()
}

// stageStarted publishes the stage header every driver owes observers.
func (d *driver) stageStarted(s *pipeline.Stage) {
	e := observe.Event{Type: observe.StageStarted, RunID: d.runID, Stage: s.ID,
		Kind: string(s.Kind), Detail: stageDetail(s)}
	if s.Upstream != nil {
		e.Upstream = s.Upstream.ID
	}
	d.bus.Publish(e)
}

func (d *driver) stageFinished(s *pipeline.Stage) {
	d.bus.Publish(observe.Event{Type: observe.StageFinished, RunID: d.runID, Stage: s.ID})
}

// source materializes a source stage's records.
func (d *driver) source(ctx context.Context, s *pipeline.Stage) ([]core.Record, error) {
	if s.SourceFn == nil {
		return s.SourceRecords, nil
	}
	recs, err := s.SourceFn(ctx)
	if err != nil {
		return nil, fmt.Errorf("source %q: %w", s.ID, err)
	}
	return recs, nil
}

// barrier runs the plan one stage at a time: every task of a stage completes
// before the next stage starts. Simple, order-preserving, and the default.
func (d *driver) barrier(ctx context.Context) error {
	for _, sp := range d.plan.Order {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s := sp.Stage
		d.stageStarted(s)

		var input []core.Record
		if s.Upstream != nil {
			input = d.outputs[s.Upstream.ID]
		}

		stageSched := d.sched
		if s.Opts.Parallelism > 0 {
			stageSched.Workers = s.Opts.Parallelism
		}

		switch s.Kind {
		case pipeline.KindSource:
			recs, err := d.source(ctx, s)
			if err != nil {
				return err
			}
			d.record(s.ID, recs)

		case pipeline.KindCombine:
			folded, err := foldCombine(s, input)
			if err != nil {
				return fmt.Errorf("combine %q: %w", s.ID, err)
			}
			d.record(s.ID, folded)

		case pipeline.KindIterate:
			out, err := d.iterate(ctx, sp, input, d.barrierRunner(stageSched))
			d.record(s.ID, out)
			if err != nil {
				return err
			}

		case pipeline.KindReduceAI:
			cur := input
			fanIn := s.Reduce.FanIn
			if fanIn <= 1 {
				fanIn = 8
			}
			for len(cur) > 0 {
				tasks, err := sp.BuildTasksBatch(d.runID, cur, fanIn, d.cfg.EgressAllow)
				if err != nil {
					return err
				}
				results, fails, execErr := stageSched.ExecuteAll(ctx, tasks)
				d.fail(fails...)
				cur = flatten(results)
				if execErr != nil {
					d.record(s.ID, cur)
					return execErr
				}
				if len(tasks) == 1 {
					break // final aggregation level completed
				}
			}
			d.record(s.ID, cur)

		default: // fused pure stages and infer
			tasks, err := sp.BuildTasks(d.runID, input, d.cfg.EgressAllow)
			if err != nil {
				return err
			}
			results, fails, execErr := stageSched.ExecuteAll(ctx, tasks)
			d.fail(fails...)
			d.record(s.ID, flatten(results))
			if execErr != nil {
				return execErr
			}
		}

		d.stageFinished(s)
	}
	return nil
}

// stageDetail renders a human-readable description of a stage's declaration
// (binding, prompts, fan-in, options) for observability consumers such as
// the constellation view's stage inspector.
func stageDetail(s *pipeline.Stage) string {
	var b strings.Builder
	line := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	switch s.Kind {
	case pipeline.KindSource:
		if s.SourceFn != nil {
			line("source function (records produced at run time)")
		} else {
			line("%d source records", len(s.SourceRecords))
		}
	case pipeline.KindFused:
		line("fused pure stages, applied per record in order:")
		for _, sub := range s.Fused {
			line("  %s (%s)", sub.ID, sub.Kind)
		}
	case pipeline.KindInfer:
		line("per-record inference · %s", bindingDetail(s.Infer.Binding))
		if s.Infer.ParseJSON {
			line("output: JSON parsed and merged into the record")
		} else {
			field := s.Infer.OutputField
			if field == "" {
				field = "output"
			}
			line("output: raw text into field %q", field)
		}
		if s.Infer.Validate != nil {
			line("validated: semantic gate on each produced record")
		}
		if s.Infer.Prefix != "" {
			line("shared prefix (rendered once per task, cacheable):\n%s", s.Infer.Prefix)
		}
		line("prompt template:\n%s", s.Infer.Prompt)
	case pipeline.KindReduceAI:
		fanIn := s.Reduce.FanIn
		if fanIn <= 1 {
			fanIn = 8
		}
		itemField := s.Reduce.ItemField
		if itemField == "" {
			itemField = "output"
		}
		line("hierarchical AI reduce · %s", bindingDetail(s.Reduce.Binding))
		line("fan-in: %d records per aggregation call, repeated until one remains", fanIn)
		line("aggregates field %q from each input record", itemField)
		if s.Reduce.Prefix != "" {
			line("shared prefix (rendered once per task, cacheable):\n%s", s.Reduce.Prefix)
		}
		line("prompt template:\n%s", s.Reduce.Prompt)
	case pipeline.KindIterate:
		spec := s.Iterate
		line("iterative · %s algorithm · %s", spec.Algorithm.Name(), bindingDetail(spec.Step.Binding))
		line("one call per active vertex per round, at most %d rounds", spec.Halt.MaxRounds)
		if b := budgetDetail(spec.Halt.Budget); b != "" {
			line("stage budget: %s", b)
		}
		if spec.MaxFrontier > 0 {
			line("frontier: at most %d vertices per round", spec.MaxFrontier)
		}
		if spec.MaxInbox > 0 {
			line("inbox: at most %d messages per vertex", spec.MaxInbox)
		}
		if spec.Grow != nil {
			line("open world: messages may create vertices the input did not contain")
		}
		if spec.Step.ParseJSON {
			line("output: JSON parsed and merged into the vertex")
		}
		if spec.Step.Prefix != "" {
			line("shared prefix (rendered once per task, cacheable):\n%s", spec.Step.Prefix)
		}
		line("vertex program:\n%s", spec.Step.Prompt)
	case pipeline.KindRecall:
		spec := s.Recall
		k := spec.K
		if k <= 0 {
			k = 5
		}
		line("recall from long-term memory %q · %d nearest items per record", spec.Space, k)
		if spec.MinScore > 0 {
			line("similarity floor: %.2f", spec.MinScore)
		}
		if len(spec.Filter) > 0 {
			keys := slices.Sorted(maps.Keys(spec.Filter))
			line("metadata filter: %s", strings.Join(keys, ", "))
		}
		line("writes hits into %q and their IDs into %q",
			orField(spec.OutputField, "memory"), orField(spec.IDField, "memory_ids"))
		if spec.Require {
			line("required: a record that recalls nothing fails")
		}
		line("query template:\n%s", spec.Query)
	case pipeline.KindRemember:
		spec := s.Remember
		line("write into long-term memory %q, staged for the next epoch", spec.Space)
		if len(spec.Meta) > 0 {
			keys := slices.Sorted(maps.Keys(spec.Meta))
			line("metadata: %s", strings.Join(keys, ", "))
		}
		line("item ID into %q", orField(spec.IDField, "memory_id"))
		line("text template:\n%s", spec.Text)
	case pipeline.KindCombine:
		line("pairwise fold over all records (driver-executed)")
	default:
		line("%s stage, applied per record", s.Kind)
	}

	if len(s.Opts.Broadcasts) > 0 {
		names := slices.Sorted(slices.Values(s.Opts.Broadcasts))
		line("broadcasts readable: %s", strings.Join(slices.Compact(names), ", "))
	}
	if len(s.Opts.Memory) > 0 {
		names := slices.Sorted(slices.Values(s.Opts.Memory))
		line("memory readable: %s", strings.Join(slices.Compact(names), ", "))
	}
	if len(s.Opts.MemoryWrite) > 0 {
		names := slices.Sorted(slices.Values(s.Opts.MemoryWrite))
		line("memory writable: %s", strings.Join(slices.Compact(names), ", "))
	}
	if s.Opts.BatchSize > 1 {
		line("batch: %d records per task", s.Opts.BatchSize)
	}
	if s.Opts.Parallelism > 0 {
		line("parallelism: %d workers", s.Opts.Parallelism)
	}
	if s.Opts.NoCache {
		line("caching disabled")
	}
	if s.Opts.NoPrefixCache {
		line("prompt-prefix caching disabled")
	}
	return strings.TrimRight(b.String(), "\n")
}

// orField renders a spec's output field, substituting the runner's default
// when the author left it unset — so the description says what the pipeline
// will actually do rather than what was typed.
func orField(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func bindingDetail(bind model.Binding) string {
	var d string
	switch {
	case bind.Model != "":
		d = "model " + bind.Model
	case bind.Tier != "":
		d = fmt.Sprintf("tier %q", bind.Tier)
	default:
		d = "no model binding"
	}
	if len(bind.Escalation) > 0 {
		d += " · escalation: " + strings.Join(bind.Escalation, " → ")
	}
	return d
}

func flatten(results []task.Result) []core.Record {
	var out []core.Record
	for _, r := range results {
		out = append(out, r.Output...)
	}
	return out
}

func foldCombine(s *pipeline.Stage, input []core.Record) ([]core.Record, error) {
	if len(input) == 0 {
		return nil, nil
	}
	acc := input[0].Clone()
	for _, r := range input[1:] {
		next, err := s.Combine(acc, r.Clone())
		if err != nil {
			return nil, err
		}
		acc = next
	}
	return []core.Record{acc}, nil
}
