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
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/delta"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/findings"
	"github.com/zionrubin/loom/mcp"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/stream"
	"github.com/zionrubin/loom/task"
	"github.com/zionrubin/loom/worker"
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
	MCPServers      []mcp.Server
	MCPResources    []MCPResource
	Broadcasts      map[string]any
	// Continuations are the run's evolving contexts, key → revision, supplied
	// with WithContinuation and read by stages that declared the key with
	// pipeline.WithContinuation.
	Continuations map[string]delta.Ref
	// DeltaPolicy routes how those contexts are materialized, and DeltaRenderer
	// turns their segments into bytes. Both have defaults that work; see
	// package delta before changing either, because the renderer is pinned into
	// every revision a chain holds.
	DeltaPolicy   delta.Policy
	DeltaRenderer delta.Renderer
	// DeltaBytes bounds the rendered context this process keeps resident (zero:
	// the delta package's default).
	DeltaBytes int64
	// Affinity is how long the queue holds a task carrying a continuation back
	// from workers that do not hold its state (zero: no waiting, pure
	// preference). It applies only to a run on a fleet.
	Affinity     time.Duration
	Topics       map[string]bool
	Findings     *findings.Config
	EventHandler func(observe.Event)
	Streaming    bool
	BatchWait    time.Duration
	// Queue routes execution to a worker fleet instead of this process. Nil —
	// the default — keeps every task local.
	Queue worker.Queue
	// QueueWait bounds how long one task may sit on the queue unfinished
	// before the client gives up on it (zero: until the run's context ends).
	QueueWait time.Duration
	// WorkerName is this process's identity in the fleet, and WorkerLease how
	// long its claims stand without a heartbeat. Both apply to Serve only.
	WorkerName  string
	WorkerLease time.Duration
	// AdmissionAging tunes a fleet's slot-admission fairness (zero = the
	// runtime default). It has no effect on a single Run, whose tasks all
	// belong to one program and therefore tie.
	AdmissionAging float64

	// Stream-mode settings. They configure Stream and are ignored by Run, so
	// one config can describe both a backfill and the live job that follows it.
	//
	// Sources bind stream.Source implementations to the stages declared with
	// pipeline.FromStream, and Sinks bind destinations to the stages whose
	// output leaves the job. JobID is the identity a restart resumes under, and
	// Checkpoints is where the recoverable points are kept.
	Sources         map[string]stream.Source
	Sinks           map[string]stream.Sink
	JobID           string
	Checkpoints     stream.Store
	CheckpointEvery time.Duration
	// Lateness is the source's bounded out-of-orderness and IdleTimeout is how
	// long a split may be silent before it stops holding the watermark back.
	Lateness    time.Duration
	IdleTimeout time.Duration
	// StreamLimit stops the job once a bound is reached, which is how an
	// endless pipeline becomes a test.
	StreamLimit stream.Limit
	PollRecords int
	PollWait    time.Duration
	// DrainOnStop overrides whether windows still open when the job stops are
	// fired. Nil defers to why it stopped: a job whose sources ran out drains,
	// a job that was cancelled does not.
	DrainOnStop *bool

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

// WithContinuation supplies one of the run's evolving contexts: a revision of a
// chain written with delta.Chain, read by every stage that declared the key
// with pipeline.WithContinuation.
//
// It is the moving counterpart of WithBroadcast, and the difference is the
// whole feature. A broadcast is registered once because every task reads the
// same bytes. A continuation is registered per run because each run reads a
// little more than the last — a turn, a finding, a critique — and the point is
// that saying so costs a hash rather than a transcript:
//
//	chain, _ := state.Chain("session/" + id)
//	ref, _ := chain.Root(delta.Segment{Name: "brief", Body: brief})
//	for _, turn := range turns {
//	    ref, _ = chain.Append(ref, delta.Segment{Name: "turn", Body: turn})
//	    loom.Run(ctx, p, loom.WithContinuation("session", ref), …)
//	}
//
// The envelope carries the revision hash, so a task stays small however long
// the session gets; the hash joins the fingerprint of every stage that reads
// it, so the cache invalidates exactly the round that changed; and an executor
// that has already materialized an earlier revision extends it instead of
// rendering the whole thing again. None of which any stage has to know: what a
// prompt receives is the context, entire, every round.
func WithContinuation(key string, ref delta.Ref) Option {
	return func(c *Config) {
		if c.Continuations == nil {
			c.Continuations = map[string]delta.Ref{}
		}
		if ref.Key == "" {
			ref.Key = key
		}
		c.Continuations[key] = ref
	}
}

// WithDeltaPolicy tunes how evolving contexts are materialized: when to splice
// onto state this process holds and when to render everything, how far a repair
// window may widen, and how often an accepted splice is recomputed from scratch
// and compared.
//
// The defaults are sane and the knobs are performance knobs. Every route
// produces identical bytes, so the worst a bad policy can do is spend time —
// with one exception worth stating: setting Verify to zero turns off the only
// check that can catch a renderer whose output is not as local as it claims.
// Leave it on unless the renderer is one of this package's.
func WithDeltaPolicy(p delta.Policy) Option {
	return func(c *Config) { c.DeltaPolicy = p }
}

// WithDeltaRenderer sets how a continuation's segments become bytes (default
// delta.Tags, which matches the format ordinary context fragments arrive in).
//
// It must be the same renderer the chain was written with — the version is
// stored in every revision and a mismatch is refused on the first read — so
// changing it means starting a new chain rather than reinterpreting an old one.
func WithDeltaRenderer(r delta.Renderer) Option {
	return func(c *Config) { c.DeltaRenderer = r }
}

// WithStateBytes bounds the rendered context this process keeps resident
// (default 64 MiB). Past it the least recently used state is dropped, which
// costs a rebuild and nothing else.
func WithStateBytes(n int64) Option {
	return func(c *Config) { c.DeltaBytes = n }
}

// WithAffinity asks the queue to hold a task carrying a continuation back from
// workers that do not hold its state, for at most d.
//
// Without it, locality is a pure preference: the state-holder is offered the
// work first and loses it to anyone who asks while it is busy. With it, the
// state-holder gets a moment to ask. The cost is bounded and paid only when
// nobody holds the state — after d the task goes to whoever wants it, which is
// what keeps a dead worker from stalling a session rather than slowing it.
//
// A poll interval or two is the useful size.
func WithAffinity(d time.Duration) Option {
	return func(c *Config) { c.Affinity = d }
}

// WithMCPServer registers Model Context Protocol servers whose tools stages
// may call after declaring them with pipeline.WithMCP.
//
// The connections are made once, here, before anything runs — one set for the
// whole host, shared by every agent on a fleet. That is the same reasoning
// that gives a fleet one rate limiter and one budget governor: a connection is
// a property of an account and a server process, not of a pipeline, and a
// second copy of one is a second copy of its quota. A misconfigured server
// therefore fails the run at provisioning rather than at the first record that
// reaches it, and no task ever pays for a handshake.
//
// What a task gets is not a connection but a lease on a call slot, bounded per
// server (mcp.Server.MaxConcurrent, defaulting to the engine's slot count), so
// a fleet of agents cannot collectively hit a server harder than one of them
// could. Credentials named in the descriptor are resolved through the run's
// broker here and reach the connection only: no task, op, or executor ever
// holds them.
func WithMCPServer(servers ...mcp.Server) Option {
	return func(c *Config) { c.MCPServers = append(c.MCPServers, servers...) }
}

// MCPResource binds an MCP resource to a broadcast name.
type MCPResource struct {
	Name   string // the broadcast name stages declare
	Server string // the MCP server holding it
	URI    string // the resource URI
}

// WithMCPResource reads a resource from an MCP server once at provisioning and
// registers it as a broadcast under name — so a document that lives behind a
// server becomes an ordinary shared value: stored once by content hash,
// referenced rather than copied by every task that declares it with
// pipeline.WithBroadcast, and folded into those stages' fingerprints so editing
// it upstream recomputes exactly the stages that read it.
//
// It is read once per run rather than per task on purpose. A resource read
// inside a task would be a network call per record whose result silently joins
// a cached artifact; read here it is a value with a hash, which is what the
// rest of the framework already knows how to reason about.
func WithMCPResource(name, server, uri string) Option {
	return func(c *Config) {
		c.MCPResources = append(c.MCPResources, MCPResource{Name: name, Server: server, URI: uri})
	}
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

// WithFindings puts a shared research layer in front of the tools that reach
// public sources, so agents reuse each other's research instead of repeating
// it.
//
//	loom.NewFleet(
//	    loom.WithMCPServer(web),
//	    loom.WithFindings(findings.Config{
//	        Gate: []string{"mcp/web/search"},
//	        Policy: findings.Policy{
//	            Topics: map[string]findings.TopicPolicy{
//	                "mcp/web/search": {Volatility: findings.Daily},
//	            },
//	        },
//	    }),
//	)
//
// It is a fleet-wide facility for the same reason the result cache is, and it
// exists because the result cache cannot do this job. The cache's key is the
// bytes going in, so it serves the second asker only if the first asked
// *identically* and only after the first has *finished*. Neither holds for
// concurrent agents doing research: they phrase one question three ways, and
// they ask at the same instant. The gate keys on the question instead of the
// bytes, and collapses simultaneous askers onto one call.
//
// A gated tool takes on one contract — it returns `{"text", "structured"}`,
// because a served answer is rebuilt from a stored finding — and gains four
// properties: questions already answered are served without a call, duplicate
// questions in flight together become one call, findings carry the capabilities
// their research consumed so the commons can never serve a reader research it
// was not allowed to do itself, and every serve is recorded against the finding
// so a retraction can say what rested on it.
//
// # Sharing it between executors
//
// By default the commons is this process's: one fleet, one ledger, no network
// on any path. Setting findings.Config.Shared connects it to a backend every
// executor reads and writes, so what one machine learns another can be served —
// and so executors that miss the same question at the same instant produce one
// external call between them rather than one each.
//
//	backend, err := pgstore.Open(ctx, dsn, pgstore.Options{Dimensions: 1536})
//	loom.WithFindings(findings.Config{
//	    Gate:   []string{"mcp/web/search"},
//	    Shared: findings.NewShared(findings.SharedConfig{Backend: backend}),
//	})
//
// It adds a rung to the ladder rather than replacing one: the in-process ledger
// is still consulted first and still answers without I/O, the backend is
// reached only when it had nothing, and what comes back is checked by the same
// sufficiency rules as a local finding — including the capability and egress
// containment, which a shared store makes more important rather than less. An
// unavailable backend degrades to ordinary research unless strict mode is
// configured.
//
// See the findings package and docs/FINDINGS.md for the design.
func WithFindings(cfg findings.Config) Option {
	return func(c *Config) { c.Findings = &cfg }
}

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

// WithWorkerService runs this pipeline's tasks on a fleet of worker processes
// instead of executing them here: tasks go onto q with leases, workers claim
// them, and the results come back through shared content-addressed storage.
//
// It changes one thing and deliberately nothing else. The planner still
// compiles the same plan, the scheduler still applies the same admission
// control, class-aware retry, escalation ladder and budget, and the run still
// returns the same RunResult — because Executor is one method over serializable
// data, and this option swaps which implementation of it the scheduler calls.
// A pipeline does not know it has been distributed.
//
// The other side of the queue is loom.Serve, and it takes the same options:
//
//	// every worker process
//	loom.Serve(ctx, pipeline, opts...)
//
//	// the process driving the run
//	loom.Run(ctx, pipeline, append(opts, loom.WithWorkerService(q))...)
//
// Two things must be true of the fleet, and both fail loudly rather than
// quietly. Every worker needs the pipeline compiled into it, because an op is
// code and a runner cannot be serialized — a worker advertises the stages it
// has and claims nothing else. And every worker must share this process's
// content-addressed storage, because inputs, broadcast values and outputs
// travel by hash: use WithStateDir pointing at shared storage, and a task whose
// blob cannot be resolved fails as the deployment error it is.
func WithWorkerService(q worker.Queue) Option {
	return func(c *Config) { c.Queue = q }
}

// WithWorkerWait bounds how long the client waits for one task before giving
// up on it and letting the scheduler retry.
//
// Giving up is not cancelling: re-submitting the same task ID re-attaches to
// the work already in flight rather than enqueueing a second copy, so the
// bound costs a wait and never a duplicate. What it buys is a run that fails
// loudly when no worker in the fleet advertises a stage, instead of one that
// hangs until somebody looks at it.
func WithWorkerWait(d time.Duration) Option {
	return func(c *Config) { c.QueueWait = d }
}

// WithWorkerName names this process in the fleet. It is the lease owner
// recorded against every task the worker claims, so it must be unique across
// the fleet; the default — host, pid and entropy — is.
//
// Set it when the deployment already has stable identities worth seeing in a
// report: a pod name, a systemd instance, a shard number.
func WithWorkerName(name string) Option {
	return func(c *Config) { c.WorkerName = name }
}

// WithWorkerLease sets how long this worker's claims stand without a
// heartbeat (default 30s, and never longer than the queue's own maximum).
//
// It is the bound on how long a killed worker delays the task it was holding,
// so shorter recovers faster — and the reason it can be short at all is that a
// live worker renews. The renewal interval follows the lease the queue
// actually granted rather than this number, so a queue that clamps cannot
// starve a healthy worker of heartbeats.
func WithWorkerLease(d time.Duration) Option {
	return func(c *Config) { c.WorkerLease = d }
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
	// MCP reports each configured MCP server's connection accounting: how many
	// sessions were opened, how many calls went through them, and how long the
	// run spent waiting on tools that cost no tokens.
	MCP []mcp.Stats
	// Findings reports the shared research layer: questions asked at the gate,
	// how many were answered from what had already been learned, and what that
	// avoided. On a fleet these numbers are the fleet's — the commons has no
	// per-agent owner — so a single Run's are its own only because a run is a
	// fleet of one.
	Findings findings.Stats
	Spent    core.Usage
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
		switch {
		case s.Stream:
			line("stream source (records arrive from a bound stream.Source)")
		case s.SourceFn != nil:
			line("source function (records produced at run time)")
		default:
			line("%d source records", len(s.SourceRecords))
		}
	case pipeline.KindWindow:
		line("window · %s", s.Window.Describe())
		line("everything downstream runs once per pane")
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
	case pipeline.KindCombine:
		line("pairwise fold over all records (driver-executed)")
	default:
		line("%s stage, applied per record", s.Kind)
	}

	if len(s.Opts.Broadcasts) > 0 {
		names := slices.Sorted(slices.Values(s.Opts.Broadcasts))
		line("broadcasts readable: %s", strings.Join(slices.Compact(names), ", "))
	}
	for _, use := range s.Opts.MCP {
		if len(use.Tools) == 0 {
			line("mcp %s: every tool the server offers", use.Server)
			continue
		}
		line("mcp %s: %s", use.Server, strings.Join(use.Tools, ", "))
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
