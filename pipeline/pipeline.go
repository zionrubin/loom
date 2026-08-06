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
	// KindRecall retrieves from long-term memory by similarity and writes what
	// it found into the record. See RecallSpec.
	KindRecall StageKind = "recall"
	// KindRemember writes records into long-term memory, staged for the next
	// epoch. See RememberSpec.
	KindRemember StageKind = "remember"
	// KindIterate is a model operation applied repeatedly under an
	// algorithm's control until it converges, runs out of rounds, or runs out
	// of budget. See IterateSpec.
	KindIterate StageKind = "iterate"
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
	Broadcasts  []string // run-level shared values this stage may read
	Memory      []string // long-term memory spaces this stage may recall from
	MemoryWrite []string // long-term memory spaces this stage may write to
	Version     string   // content-version for Go-func stages; enables caching
	NoCache     bool
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

// WithMemory declares which long-term memory spaces this stage's tasks may
// recall from, for stages that reach memory through the session (MapTools)
// rather than declaring it in a spec.
//
// A Recall or Remember stage grants its own space automatically — the spec
// already names it, and making the author repeat it would be ceremony, not
// least privilege. This option is the escape hatch for everything else, and it
// carries the same guarantees: a stage sees only the spaces it names, reading
// an undeclared one is a permanent failure, and the pinned epoch of each
// declared space joins the stage fingerprint.
func WithMemory(spaces ...string) Option {
	return func(o *StageOpts) { o.Memory = append(o.Memory, spaces...) }
}

// WithMemoryWrite declares which long-term memory spaces this stage's tasks
// may write to. Writes are staged for the next epoch, so they are invisible to
// the run that made them.
//
// It is deliberately separate from WithMemory: in a knowledge base shared
// across an application, nearly every stage should be able to consult what is
// known and very few should be able to add to it.
func WithMemoryWrite(spaces ...string) Option {
	return func(o *StageOpts) { o.MemoryWrite = append(o.MemoryWrite, spaces...) }
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

// RecallSpec declares a retrieval from long-term memory: a per-record query
// against one memory space, as of the epoch the run pinned.
//
// It is a stage rather than a tool the model may call, and that is the whole
// point. A tool call is opaque to the framework: its result never reaches a
// cache key, its cost never reaches a projection, its access is never checked
// against an envelope, and what it returned is never recorded in lineage.
// Making retrieval a stage puts it inside all four. In particular, the
// retrieved item IDs land in the record, so the expensive stage below inherits
// them in its cache key and recomputes only when what was retrieved actually
// changed.
type RecallSpec struct {
	// Space is the memory space to search.
	Space string
	// Query is a text/template rendered against each record's Data — the text
	// whose nearest neighbours are wanted (e.g. "{{.subject}}\n{{.body}}").
	Query string
	// K is how many items to retrieve (default 5).
	K int
	// Filter constrains results by item metadata, matched exactly. Values are
	// templates over the record's Data, so a filter can be per-record — a
	// tenant ID, a document class, a language.
	Filter map[string]string
	// MinScore drops hits below a similarity floor. Nearest-neighbour search
	// always returns something; a floor is what lets a record legitimately
	// recall nothing.
	MinScore float32
	// OutputField receives the rendered hits, ready to interpolate into a
	// downstream prompt (default "memory").
	OutputField string
	// IDField receives the ordered item IDs (default "memory_ids"). This is
	// the field that carries what was retrieved into every downstream cache
	// key; clearing it is possible but gives up record-granular invalidation.
	IDField string
	// ScoreField, when set, receives the hits' similarity scores.
	ScoreField string
	// Require fails the record when nothing is recalled, instead of passing it
	// through with an empty memory field. Use it when the prompt below is
	// meaningless without context.
	Require bool
}

// RememberSpec declares a write into long-term memory: one item per record,
// staged for the next epoch.
//
// Writes never become visible to the run that made them. That is the same rule
// a fleet's blackboard follows — publish between units of work, not inside one
// — and it exists for the same reason: a task that could change what a later
// task in its own run retrieves would make its cached result depend on
// execution order, which content-addressed replay assumes away.
type RememberSpec struct {
	// Space is the memory space to write into.
	Space string
	// Text is a text/template over the record's Data producing the item's
	// content.
	Text string
	// Meta are templates over the record's Data producing the item's
	// metadata — what later Filter clauses will match on.
	Meta map[string]string
	// IDField receives the written item's content-addressed ID
	// (default "memory_id").
	IDField string
	// WriteEmpty stores an item even when the rendered Text is blank. By
	// default a blank record passes through unwritten, because a knowledge
	// base full of empty items is worse than one with gaps.
	WriteEmpty bool
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
	MapCtxFn  func(ctx context.Context, s core.Session, r core.Record) (core.Record, error)
	FilterFn  func(r core.Record) (bool, error)
	FlatMapFn func(r core.Record) ([]core.Record, error)

	Infer    *InferSpec
	Reduce   *ReduceAISpec
	Iterate  *IterateSpec
	Recall   *RecallSpec
	Remember *RememberSpec
	Combine  func(a, b core.Record) (core.Record, error)

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

// Recall retrieves from long-term memory per record, writing what it found
// into each record for the stages below to use.
//
// It is the cheap half of retrieval-augmented work — one embedding and one
// index lookup — and separating it from the expensive half is what makes a
// growing knowledge base affordable. A commit to the space invalidates this
// stage, because the pinned epoch is in its fingerprint; it invalidates an
// Infer below it only for the records whose recalled set actually moved,
// because what reaches that stage's cache key is the record, and the record
// now names what it retrieved.
func (d Dataset) Recall(name string, spec RecallSpec, opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindRecall, Upstream: d.stage, Recall: &spec, Opts: applyOpts(opts)})
}

// Remember writes each record into long-term memory, staged for the next
// epoch and therefore invisible to the run that wrote it.
//
// This is the write half of a long-term application: what one run concludes,
// the next run recalls. Items are content-addressed, so a conclusion reached
// again costs nothing to store again, and each carries the run, stage, task,
// and model that produced it — a knowledge base of model outputs is only worth
// having if a later reader can tell where each entry came from.
func (d Dataset) Remember(name string, spec RememberSpec, opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindRemember, Upstream: d.stage, Remember: &spec, Opts: applyOpts(opts)})
}

// Combine folds records pairwise with an associative function (no model).
func (d Dataset) Combine(name string, fn func(a, b core.Record) (core.Record, error), opts ...Option) Dataset {
	return d.p.add(&Stage{ID: name, Kind: KindCombine, Upstream: d.stage, Combine: fn, Opts: applyOpts(opts)})
}
