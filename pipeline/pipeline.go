// Package pipeline is Loom's authoring API: a declarative builder for
// dataflow graphs mixing plain Go transforms (Map/Filter/FlatMap) with
// AI-native operations (Infer, ReduceAI). Datasets are handles to stage
// outputs; branching from one dataset builds a DAG.
//
// AI operations are fully declarative and serializable — they can execute on
// remote or sandboxed workers. Go-function stages execute wherever the
// closure lives (the local executor); give them a Version to make them
// cacheable.
package pipeline

import (
	"context"
	"fmt"
	"text/template"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/stream"
	"github.com/zionrubin/loom/task"
)

// StageKind enumerates operation types.
type StageKind string

const (
	KindSource   StageKind = "source"
	KindMap      StageKind = "map"
	KindFilter   StageKind = "filter"
	KindFlatMap  StageKind = "flatmap"
	KindInfer    StageKind = "infer"
	KindReduceAI StageKind = "reduce_ai"
	KindCombine  StageKind = "combine"
	// KindIterate is a model operation applied repeatedly under an
	// algorithm's control until it converges, runs out of rounds, or runs out
	// of budget. See IterateSpec.
	KindIterate StageKind = "iterate"
	// KindFused is produced by the planner: a run of adjacent pure stages
	// collapsed into one task boundary.
	KindFused StageKind = "fused"
	// KindWindow cuts an unbounded input into finite sets so the aggregates
	// downstream of it have an end to fold to. See Dataset.Window.
	KindWindow StageKind = "window"
)

// StageOpts carries per-stage tuning, applied via Option functions.
type StageOpts struct {
	Parallelism int
	BatchSize   int
	Budget      core.Budget
	Sandbox     task.SandboxProfile
	Grants      []security.Capability
	Broadcasts  []string // run-level shared values this stage may read
	// Continuation names the evolving context this stage's tasks read, if any.
	Continuation string
	MCP          []MCPUse // MCP servers and tools this stage may call
	Version      string   // content-version for Go-func stages; enables caching
	NoCache      bool
	// NoPrefixCache opts the stage out of provider prompt-prefix caching.
	NoPrefixCache bool
}

// Option configures a stage.
type Option func(*StageOpts)

// WithParallelism bounds concurrent tasks for this stage (0 = run default).
func WithParallelism(n int) Option { return func(o *StageOpts) { o.Parallelism = n } }

// WithBatchSize groups n records per task (default 1).
func WithBatchSize(n int) Option { return func(o *StageOpts) { o.BatchSize = n } }

// WithBudget sets the per-task budget (timeout, attempts, tokens).
func WithBudget(b core.Budget) Option { return func(o *StageOpts) { o.Budget = b } }

// WithSandbox requests an isolation profile for this stage's tasks.
func WithSandbox(p task.SandboxProfile) Option { return func(o *StageOpts) { o.Sandbox = p } }

// WithGrants adds extra capabilities (e.g. tool access) to the stage's
// envelopes.
func WithGrants(caps ...security.Capability) Option {
	return func(o *StageOpts) { o.Grants = append(o.Grants, caps...) }
}

// WithBroadcast declares which run-level shared values (registered with
// loom.WithBroadcast) this stage's tasks may read. Least privilege applies as
// it does to models, secrets, and tools: a stage sees only the broadcasts it
// names, reading an undeclared one is a permanent failure, and the declared
// values' content hashes join the stage fingerprint so a changed broadcast
// invalidates the cached results that saw it.
func WithBroadcast(names ...string) Option {
	return func(o *StageOpts) { o.Broadcasts = append(o.Broadcasts, names...) }
}

// WithContinuation declares that this stage's tasks read an evolving context —
// a growing transcript, a research thread, an agent session — supplied for the
// run with loom.WithContinuation and held in shared storage as an immutable
// chain of revisions.
//
// It is the third way a stage can be given context, and the three differ in
// what they are for rather than in what they can hold:
//
//	Context fragments   fixed at planning time and carried in every envelope.
//	                    For the small and unchanging: a rubric, a style note.
//	Broadcasts          registered once per run and carried as a hash. For the
//	                    large and unchanging, read by name from a template.
//	Continuations       a chain of revisions, carried as a hash. For the large
//	                    and *changing*, where each run reads a little more than
//	                    the last.
//
// The revision's hash joins the stage fingerprint, so a round that appended a
// turn recomputes exactly the tasks that could have seen it and leaves every
// earlier round's results in the cache — the same treatment a changed
// broadcast gets. Unlike a broadcast it needs no capability grant: it is not
// reachable by name from a prompt, so there is no name for a grant to guard.
//
// What it costs an executor is described in package delta. What it costs a
// pipeline is this line.
func WithContinuation(key string) Option {
	return func(o *StageOpts) { o.Continuation = key }
}

// MCPUse is one stage's declaration of an MCP server it may call, and
// optionally the specific tools on it.
type MCPUse struct {
	Server string   `json:"server"`
	Tools  []string `json:"tools,omitempty"`
}

// WithMCP declares that this stage's tasks may call tools on an MCP server
// registered with loom.WithMCPServer. Naming tools narrows the grant to
// exactly those; naming none grants every tool the server offers, which is
// convenient and less than least privilege.
//
//	src.MapTools("enrich", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
//	    out, err := s.Invoke(ctx, mcp.ToolName("catalog", "lookup_sku"),
//	        map[string]any{"sku": r.String("sku")})
//	    ...
//	}, pipeline.WithMCP("catalog", "lookup_sku"), pipeline.WithVersion("v1"))
//
// Declaring is what produces the grant, the egress entry for a networked
// server, and the fingerprint component: the digest of the tools' descriptors
// joins this stage's fingerprint, so upgrading the server recomputes exactly
// the stages that could have called the tools that changed and leaves the rest
// of the cache warm. A server nobody registered fails compilation rather than
// the first record that reaches it.
//
// Tool calls are side effects, and a cached stage does not repeat them. Give a
// stage that must call its tools on every run pipeline.WithNoCache; a stage
// left cacheable is asserting that replaying the recorded result is as good as
// calling again, which for a lookup is usually true and for a write never is.
func WithMCP(server string, tools ...string) Option {
	return func(o *StageOpts) {
		o.MCP = append(o.MCP, MCPUse{Server: server, Tools: tools})
	}
}

// WithVersion declares a content version for Go-function stages, enabling
// caching: bump it when the function's behavior changes.
func WithVersion(v string) Option { return func(o *StageOpts) { o.Version = v } }

// TemplateFuncs returns the functions available inside Infer and ReduceAI
// prompt templates:
//
//	{{broadcast "name"}}      the shared value (index it for structured data)
//	{{broadcastJSON "name"}}  the shared value rendered as indented JSON
//
// The implementations returned here are parse-time placeholders, so templates
// can be validated at compile time before anything is provisioned. The
// executor rebinds them per task to exactly the broadcasts that task's
// envelope grants.
func TemplateFuncs() template.FuncMap {
	unbound := func(name string) (any, error) {
		return nil, fmt.Errorf("broadcast %q: not bound to this task "+
			"(declare it with pipeline.WithBroadcast)", name)
	}
	return template.FuncMap{"broadcast": unbound, "broadcastJSON": unbound}
}

// WithNoCache disables caching for this stage.
func WithNoCache() Option { return func(o *StageOpts) { o.NoCache = true } }

// WithoutPrefixCache opts an AI stage out of provider prompt-prefix caching.
//
// The planner enables prefix caching whenever a stage issues more than one
// model call, because a written cache entry pays for itself on its second
// read. Use this to opt out where that reasoning doesn't hold — a provider
// whose cache you don't want to populate, or a prefix you know is unique per
// task despite the stage shape.
func WithoutPrefixCache() Option { return func(o *StageOpts) { o.NoPrefixCache = true } }

// InferSpec declares a per-record model operation.
type InferSpec struct {
	// Binding declares which model (or tier) to use, with an optional
	// escalation ladder for semantic retries.
	Binding model.Binding
	// System is the system prompt.
	System string
	// Prefix is an optional shared prompt head, rendered once per task
	// instead of once per record and placed ahead of every rendered Prompt.
	//
	// It is a text/template with no record data in scope — only the broadcast
	// functions — which is precisely what makes it shared: every call this
	// stage issues sends the same leading bytes, so the provider's prompt
	// cache serves the prefix instead of reprocessing it per record. Put the
	// rubric, taxonomy, or few-shot examples here and keep Prompt down to
	// what actually varies.
	Prefix string
	// Prompt is a text/template rendered against each record's Data
	// (e.g. "Classify: {{.subject}}").
	Prompt string
	// MaxTokens caps the response (default 1024).
	MaxTokens int
	// Context fragments are delivered to the task envelope and prefixed to
	// the prompt — the exact context the task needs, no more.
	Context []task.Fragment
	// ParseJSON parses the model output as a JSON object merged into the
	// record's Data. Parse failures are semantic failures (they trigger
	// escalation).
	ParseJSON bool
	// OutputField receives the raw text when ParseJSON is false
	// (default "output").
	OutputField string
	// Validate, if set, checks the produced record; errors are semantic
	// failures.
	Validate func(core.Record) error
}

// ReduceAISpec declares a hierarchical AI aggregation: records are grouped
// FanIn at a time, each group summarized by the model, and levels repeat
// until one record remains.
type ReduceAISpec struct {
	Binding model.Binding
	System  string
	// Prefix is an optional shared prompt head rendered once per aggregation
	// task and placed ahead of Prompt — the same shared-prefix mechanism as
	// InferSpec.Prefix, and worth using for the aggregation rubric that every
	// level of the reduce tree repeats.
	Prefix string
	// Prompt is a text/template over {"Items": []string, "Count": int}.
	Prompt string
	// FanIn is the group size per aggregation call (default 8).
	FanIn int
	// MaxTokens caps each response (default 1024).
	MaxTokens int
	// ItemField selects which record field feeds aggregation
	// (default "output").
	ItemField string
	// OutputField receives the aggregate text (default "output").
	OutputField string
}

// Stage is one node of the dataflow graph.
type Stage struct {
	ID       string
	Kind     StageKind
	Upstream *Stage
	Opts     StageOpts

	SourceRecords []core.Record
	SourceFn      func(ctx context.Context) ([]core.Record, error)
	// Stream marks a source stage as unbounded: its records arrive from a
	// stream.Source bound at run time rather than from this declaration.
	Stream bool

	MapFn     func(r core.Record) (core.Record, error)
	MapCtxFn  func(ctx context.Context, s core.Session, r core.Record) (core.Record, error)
	FilterFn  func(r core.Record) (bool, error)
	FlatMapFn func(r core.Record) ([]core.Record, error)

	Infer   *InferSpec
	Reduce  *ReduceAISpec
	Iterate *IterateSpec
	Window  *stream.WindowSpec
	Combine func(a, b core.Record) (core.Record, error)

	// Fused is populated by the planner for KindFused stages.
	Fused []*Stage
}

// Pipeline is a named dataflow graph under construction.
type Pipeline struct {
	Name   string
	stages []*Stage
}

// New creates an empty pipeline.
func New(name string) *Pipeline { return &Pipeline{Name: name} }

// Stages returns all stages added so far.
func (p *Pipeline) Stages() []*Stage { return p.stages }

func (p *Pipeline) add(s *Stage) Dataset {
	p.stages = append(p.stages, s)
	return Dataset{p: p, stage: s}
}

// Dataset is a handle to a stage's output; transformations derive new
// datasets, and multiple derivations from one dataset branch the DAG.
type Dataset struct {
	p     *Pipeline
	stage *Stage
}

// StageID returns the underlying stage's ID.
func (d Dataset) StageID() string { return d.stage.ID }

func applyOpts(opts []Option) StageOpts {
	var o StageOpts
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// FromRecords starts a pipeline from in-memory records.
func (p *Pipeline) FromRecords(name string, recs []core.Record) Dataset {
	return p.add(&Stage{ID: name, Kind: KindSource, SourceRecords: recs})
}

// FromFunc starts a pipeline from a loader invoked at run time.
func (p *Pipeline) FromFunc(name string, fn func(ctx context.Context) ([]core.Record, error)) Dataset {
	return p.add(&Stage{ID: name, Kind: KindSource, SourceFn: fn})
}

// Map applies a pure transformation per record.
func (d Dataset) Map(name string, fn func(core.Record) (core.Record, error), opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindMap, Upstream: d.stage, MapFn: fn, Opts: applyOpts(opts)})
}

// MapTools applies a transformation with access to the task's capability-
// checked session: tool invocation, granted via
// WithGrants(security.ToolCap("...")), and broadcast reads, granted via
// WithBroadcast("..."). This is the pure-transform escape hatch for anything
// that needs more than the record in front of it.
func (d Dataset) MapTools(name string, fn func(ctx context.Context, s core.Session, r core.Record) (core.Record, error), opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindMap, Upstream: d.stage, MapCtxFn: fn, Opts: applyOpts(opts)})
}

// Filter keeps records for which fn returns true.
func (d Dataset) Filter(name string, fn func(core.Record) (bool, error), opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindFilter, Upstream: d.stage, FilterFn: fn, Opts: applyOpts(opts)})
}

// FlatMap expands each record into zero or more records.
func (d Dataset) FlatMap(name string, fn func(core.Record) ([]core.Record, error), opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindFlatMap, Upstream: d.stage, FlatMapFn: fn, Opts: applyOpts(opts)})
}

// Infer applies a model operation per record.
func (d Dataset) Infer(name string, spec InferSpec, opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindInfer, Upstream: d.stage, Infer: &spec, Opts: applyOpts(opts)})
}

// ReduceAI hierarchically aggregates all records into one via model calls.
func (d Dataset) ReduceAI(name string, spec ReduceAISpec, opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindReduceAI, Upstream: d.stage, Reduce: &spec, Opts: applyOpts(opts)})
}

// Combine folds records pairwise with an associative function (no model).
func (d Dataset) Combine(name string, fn func(a, b core.Record) (core.Record, error), opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindCombine, Upstream: d.stage, Combine: fn, Opts: applyOpts(opts)})
}
