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
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/ops"
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
	EventHandler    func(observe.Event)
	Streaming       bool
	BatchWait       time.Duration
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
	Spent        core.Usage
}

// Run executes a pipeline to completion (or budget/failure abort, in which
// case partial results are returned along with the error).
func Run(ctx context.Context, p *pipeline.Pipeline, opts ...Option) (*RunResult, error) {
	cfg := Config{Workers: 8, Retry: runtime.DefaultRetry}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.Registry == nil {
		cfg.Registry = model.NewRegistry()
	}

	// --- Wiring ---------------------------------------------------------
	bus := observe.NewBus()
	defer bus.Close()
	collector := observe.NewCollector()
	bus.On(collector.Handle)
	if cfg.EventHandler != nil {
		bus.On(cfg.EventHandler)
	}

	audit := &security.AuditLog{}
	broker := security.NewStaticBroker(cfg.Secrets, audit)
	lineage := &store.Lineage{}

	casDir, cacheDir := "", ""
	if cfg.StateDir != "" {
		casDir = cfg.StateDir + "/cas"
		cacheDir = cfg.StateDir
	}
	cas, err := store.NewCAS(casDir)
	if err != nil {
		return nil, err
	}
	cache, err := store.NewCache(cas, cacheDir)
	if err != nil {
		return nil, err
	}
	defer cache.Close()

	// Broadcasts are stored once, before anything runs: from here on tasks
	// carry content hashes, not copies.
	broadcasts := store.NewBroadcasts(cas)
	for _, name := range slices.Sorted(maps.Keys(cfg.Broadcasts)) {
		if _, err := broadcasts.Register(name, cfg.Broadcasts[name]); err != nil {
			return nil, err
		}
	}

	pl, err := plan.Compile(p, cfg.Registry, plan.WithBroadcasts(broadcasts.Hashes()))
	if err != nil {
		return nil, err
	}
	runners, err := ops.BuildRunners(pl)
	if err != nil {
		return nil, err
	}

	client := &executor.ModelClient{Registry: cfg.Registry, Broker: broker, Audit: audit, Bus: bus}
	local := &executor.Local{
		Runners: runners, Client: client, Tools: executor.NewToolSet(cfg.Tools...),
		Broadcasts: broadcasts,
		Audit:      audit, Cache: cache, Lineage: lineage, Bus: bus,
	}
	governor := runtime.NewGovernor(cfg.RunBudget)
	limiter := runtime.NewRateLimiter()

	sched := runtime.Scheduler{
		Workers: cfg.Workers, Retry: cfg.Retry, Limiter: limiter,
		Governor: governor, Registry: cfg.Registry, Exec: local, Bus: bus,
		ContinueOnError: cfg.ContinueOnError,
	}

	// --- Drive stages in topological order ------------------------------
	runID := core.NewID("run")
	bus.Publish(observe.Event{Type: observe.RunStarted, RunID: runID})

	// Announce the shared values after the run header (which resets observer
	// state) and before any task runs, so a viewer sees what the run agreed to
	// share before it sees anything read it.
	for _, e := range broadcasts.Entries() {
		bus.Publish(observe.Event{
			Type: observe.BroadcastRegistered, RunID: runID,
			Broadcast: e.Name, Artifact: e.Hash, Bytes: e.Bytes,
			Detail: observe.Clip(e.JSON),
		})
	}

	d := &driver{
		plan: pl, runID: runID, cfg: cfg, sched: sched, bus: bus,
		outputs: map[string][]core.Record{},
	}

	run := d.barrier
	if cfg.Streaming {
		run = d.stream
	}
	runErr := run(ctx)

	bus.Publish(observe.Event{Type: observe.RunFinished, RunID: runID})
	res := &RunResult{
		RunID:        runID,
		StageOutputs: d.outputs,
		Report:       collector.Report(),
		Failures:     d.failures,
		Lineage:      lineage.Entries(),
		Audit:        audit.Entries(),
		Broadcasts:   broadcasts.Hashes(),
		Spent:        governor.Spent(),
	}
	if term := pl.Terminal(); len(term) == 1 {
		res.Output = d.outputs[term[0]]
	}
	return res, runErr
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

	mu       sync.Mutex
	outputs  map[string][]core.Record
	failures []runtime.Failure
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
	case pipeline.KindCombine:
		line("pairwise fold over all records (driver-executed)")
	default:
		line("%s stage, applied per record", s.Kind)
	}

	if len(s.Opts.Broadcasts) > 0 {
		names := slices.Sorted(slices.Values(s.Opts.Broadcasts))
		line("broadcasts readable: %s", strings.Join(slices.Compact(names), ", "))
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
