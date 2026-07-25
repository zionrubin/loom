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

	outputs := map[string][]core.Record{}
	var failures []runtime.Failure

	finish := func(runErr error) (*RunResult, error) {
		bus.Publish(observe.Event{Type: observe.RunFinished, RunID: runID})
		res := &RunResult{
			RunID:        runID,
			StageOutputs: outputs,
			Report:       collector.Report(),
			Failures:     failures,
			Lineage:      lineage.Entries(),
			Audit:        audit.Entries(),
			Broadcasts:   broadcasts.Hashes(),
			Spent:        governor.Spent(),
		}
		if term := pl.Terminal(); len(term) == 1 {
			res.Output = outputs[term[0]]
		}
		return res, runErr
	}

	for _, sp := range pl.Order {
		if ctx.Err() != nil {
			return finish(ctx.Err())
		}
		s := sp.Stage
		started := observe.Event{Type: observe.StageStarted, RunID: runID, Stage: s.ID,
			Kind: string(s.Kind), Detail: stageDetail(s)}
		if s.Upstream != nil {
			started.Upstream = s.Upstream.ID
		}
		bus.Publish(started)

		var input []core.Record
		if s.Upstream != nil {
			input = outputs[s.Upstream.ID]
		}

		stageSched := sched
		if s.Opts.Parallelism > 0 {
			stageSched.Workers = s.Opts.Parallelism
		}

		switch s.Kind {
		case pipeline.KindSource:
			recs := s.SourceRecords
			if s.SourceFn != nil {
				recs, err = s.SourceFn(ctx)
				if err != nil {
					return finish(fmt.Errorf("source %q: %w", s.ID, err))
				}
			}
			outputs[s.ID] = recs

		case pipeline.KindCombine:
			folded, err := foldCombine(s, input)
			if err != nil {
				return finish(fmt.Errorf("combine %q: %w", s.ID, err))
			}
			outputs[s.ID] = folded

		case pipeline.KindReduceAI:
			cur := input
			fanIn := s.Reduce.FanIn
			if fanIn <= 1 {
				fanIn = 8
			}
			for len(cur) > 0 {
				tasks, err := sp.BuildTasksBatch(runID, cur, fanIn, cfg.EgressAllow)
				if err != nil {
					return finish(err)
				}
				results, fails, execErr := stageSched.ExecuteAll(ctx, tasks)
				failures = append(failures, fails...)
				cur = flatten(results)
				if execErr != nil {
					outputs[s.ID] = cur
					return finish(execErr)
				}
				if len(tasks) == 1 {
					break // final aggregation level completed
				}
			}
			outputs[s.ID] = cur

		default: // fused pure stages and infer
			tasks, err := sp.BuildTasks(runID, input, cfg.EgressAllow)
			if err != nil {
				return finish(err)
			}
			results, fails, execErr := stageSched.ExecuteAll(ctx, tasks)
			failures = append(failures, fails...)
			outputs[s.ID] = flatten(results)
			if execErr != nil {
				return finish(execErr)
			}
		}

		bus.Publish(observe.Event{Type: observe.StageFinished, RunID: runID, Stage: s.ID})
	}

	return finish(nil)
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
