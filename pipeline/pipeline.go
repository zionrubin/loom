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

	"github.com/zionrubin/brian-ai/loom/core"
	"github.com/zionrubin/brian-ai/loom/model"
	"github.com/zionrubin/brian-ai/loom/security"
	"github.com/zionrubin/brian-ai/loom/task"
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
	// KindFused is produced by the planner: a run of adjacent pure stages
	// collapsed into one task boundary.
	KindFused StageKind = "fused"
)

// StageOpts carries per-stage tuning, applied via Option functions.
type StageOpts struct {
	Parallelism int
	BatchSize   int
	Budget      core.Budget
	Sandbox     task.SandboxProfile
	Grants      []security.Capability
	Version     string // content-version for Go-func stages; enables caching
	NoCache     bool
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

// WithVersion declares a content version for Go-function stages, enabling
// caching: bump it when the function's behavior changes.
func WithVersion(v string) Option { return func(o *StageOpts) { o.Version = v } }

// WithNoCache disables caching for this stage.
func WithNoCache() Option { return func(o *StageOpts) { o.NoCache = true } }

// InferSpec declares a per-record model operation.
type InferSpec struct {
	// Binding declares which model (or tier) to use, with an optional
	// escalation ladder for semantic retries.
	Binding model.Binding
	// System is the system prompt.
	System string
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

	MapFn     func(r core.Record) (core.Record, error)
	MapCtxFn  func(ctx context.Context, tools core.Tools, r core.Record) (core.Record, error)
	FilterFn  func(r core.Record) (bool, error)
	FlatMapFn func(r core.Record) ([]core.Record, error)

	Infer   *InferSpec
	Reduce  *ReduceAISpec
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

// MapTools applies a transformation with access to capability-checked tools;
// grant the needed tools via WithGrants(security.ToolCap("...")).
func (d Dataset) MapTools(name string, fn func(ctx context.Context, tools core.Tools, r core.Record) (core.Record, error), opts ...Option) Dataset {
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
