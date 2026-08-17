package loom

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"text/template"
	"time"

	"github.com/zionrubin/loom/algo"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/mcp"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/task"
)

// defaultExpectedOutput is the share of a stage's MaxTokens a response is
// assumed to actually use. Output length is the one quantity a plan cannot
// determine — everything else in a projection is computed — so it is a stated
// assumption with a knob (WithExpectedOutput) rather than a hidden constant.
const defaultExpectedOutput = 0.35

// WithExpectedOutput sets the fraction of each stage's MaxTokens that
// responses are assumed to fill, for Explain's expected-cost column (default
// 0.35). It does not affect a run: MaxTokens still caps responses, and the
// projection's ceiling is always computed at the full cap.
func WithExpectedOutput(ratio float64) Option {
	return func(c *Config) { c.ExpectedOutput = ratio }
}

// WithSourceSample supplies the records a function-backed source (FromFunc)
// would produce, so Explain can project the stages downstream of it.
//
// Explain never invokes a source function — it is the one stage whose
// execution may touch the outside world, and a cost projection that reads a
// database or an object store to tell you what a run would cost has defeated
// its own purpose. FromRecords sources need no sample; their records are
// already in the pipeline.
func WithSourceSample(stage string, recs []core.Record) Option {
	return func(c *Config) {
		if c.SourceSamples == nil {
			c.SourceSamples = map[string][]core.Record{}
		}
		c.SourceSamples[stage] = recs
	}
}

// WithStageSample supplies the fields a ParseJSON inference stage is expected
// to add to each record, so the stages after it can be projected exactly.
//
// It exists because ParseJSON is the one operator that changes a record's
// *shape* in a way no plan can know: the field names come out of the model. A
// downstream Filter testing one of those fields therefore sees it missing and
// drops every record, and everything past it projects as zero work — an
// under-count, and the one way this projection can be wrong in the dangerous
// direction. Naming the fields once removes the guess:
//
//	loom.WithStageSample("classify", map[string]any{
//	    "category": "billing", "urgent": true,
//	})
//
// A sample is one scenario applied to every record, not a distribution, so a
// filter below it keeps all the records or none of them. Choose the values
// that make the most downstream work — the bool a filter accepts, the label
// with the most expensive branch — and the projection becomes a conservative
// bound rather than a mid-case guess, which is what a ceiling is for.
func WithStageSample(stage string, fields map[string]any) Option {
	return func(c *Config) {
		if c.StageSamples == nil {
			c.StageSamples = map[string]map[string]any{}
		}
		c.StageSamples[stage] = fields
	}
}

// StageProjection is one stage's forecast.
type StageProjection struct {
	Stage   string
	Kind    string
	Model   string // base model of the binding; "" for stages that call none
	Records int    // records entering the stage
	Calls   int    // model calls the stage issues
	Tasks   int    // scheduled units those calls are grouped into

	// Estimated marks a stage whose record count descends from a ParseJSON
	// stage with no WithStageSample: the model invents the fields the stages
	// below it filter and template on, so this stage's counts — and therefore
	// its cost, in either column — are a guess rather than a computation.
	Estimated bool

	// Usage is the projected accounting at the expected response length,
	// priced from the registry exactly as a run would price it — including
	// the three disjoint prompt-token classes, so a stage that shares a
	// prefix shows what it reads from the provider's cache rather than pays
	// for.
	Usage core.Usage

	// Ceiling is the same accounting with every response filling MaxTokens.
	// Unlike Usage it rests on no assumption: MaxTokens is a cap the provider
	// enforces, so a first attempt of this stage cannot cost more than this.
	Ceiling core.Usage

	// AdmissionFloor is the shortest wall-clock time these calls can take
	// under the model's requests/min and tokens/min limits — the scheduler
	// will not admit them faster, however many workers are available.
	AdmissionFloor time.Duration

	// CachePrefix reports whether the planner will enable provider prompt-
	// prefix caching for this stage.
	CachePrefix bool
}

// Projection is what a pipeline is expected to cost and how long it can least
// take, computed without issuing a single model call.
type Projection struct {
	Pipeline string
	Driver   string
	Budget   core.Budget
	Stages   []StageProjection

	// Warnings name every place the projection had to fall back to an
	// approximation, and every discrepancy worth seeing before spending
	// money. A projection with no warnings is computed end to end.
	Warnings []string
}

// Partial reports whether any stage's record count was guessed rather than
// computed, which happens when a ParseJSON stage has no WithStageSample.
//
// It matters more than it looks: Ceiling is a genuine bound only over the
// records the projection knows about. When a stage is Estimated, the run can
// do work this projection never saw, so the ceiling becomes a figure for the
// part that was understood rather than a cap on the whole run — and the report
// says so instead of quietly claiming otherwise.
func (p *Projection) Partial() bool {
	for _, s := range p.Stages {
		if s.Estimated {
			return true
		}
	}
	return false
}

// Expected returns the whole-run projection at the expected response length.
func (p *Projection) Expected() core.Usage { return sumUsage(p.Stages, false) }

// Ceiling returns the whole-run projection with every response filling its
// stage's MaxTokens: the most a run can spend before retries.
func (p *Projection) Ceiling() core.Usage { return sumUsage(p.Stages, true) }

func sumUsage(stages []StageProjection, ceiling bool) core.Usage {
	var t core.Usage
	for _, s := range stages {
		if ceiling {
			t.Add(s.Ceiling)
		} else {
			t.Add(s.Usage)
		}
	}
	return t
}

// AdmissionFloor returns the shortest time the run can take under provider
// rate limits. Stages that cannot overlap add up, so the barrier driver sums
// every stage's floor while the streaming driver — which runs all stages
// against one slot pool — is bounded by the longest dependency chain.
func (p *Projection) AdmissionFloor() time.Duration {
	if p.Driver != "streaming" {
		var total time.Duration
		for _, s := range p.Stages {
			total += s.AdmissionFloor
		}
		return total
	}
	var longest time.Duration
	for _, s := range p.Stages {
		longest = max(longest, s.AdmissionFloor)
	}
	return longest
}

// FitsBudget reports whether the run's budget covers the projected ceiling.
// A false here does not mean the run fails: the governor stops admitting work
// and returns partial results, which is sometimes exactly what you want.
func (p *Projection) FitsBudget() bool {
	c := p.Ceiling()
	if p.Budget.MaxCostUSD > 0 && c.CostUSD > p.Budget.MaxCostUSD {
		return false
	}
	if p.Budget.MaxTokens > 0 && c.TotalTokens() > p.Budget.MaxTokens {
		return false
	}
	return true
}

// Explain projects what running p would cost, without running it.
//
// It compiles the pipeline exactly as Run does — same validation, same
// fusion, same fingerprints, same envelopes — then walks the plan computing
// per-stage call counts, rendered prompt sizes, prompt-cache economics, and
// registry-priced cost. Two numbers come out of every stage: an expected cost
// under a stated assumption about response length, and a ceiling that rests
// on none, because MaxTokens is a cap the provider enforces. The ceiling is
// the number to hand WithRunBudget.
//
// What makes the projection sharp rather than a guess is a property specific
// to this framework: a pipeline's cheap stages are ordinary Go functions and
// its expensive ones are declarative data. So Explain *executes* the cheap
// skeleton — Map, Filter, FlatMap, Combine really run — and only models the
// paid calls. Record counts are therefore exact, not extrapolated, and every
// prompt is measured after rendering against the record that will produce it.
//
// Three things it cannot compute, each of which produces a warning rather
// than a wrong number: the length of a response, the fields a ParseJSON stage
// will introduce (so a downstream template reading them falls back to
// estimating from the template source), and the output of a source function
// or a MapTools stage, which Explain declines to execute because one may
// touch the network and the other needs a provisioned session.
//
// Explain issues no model calls, resolves no secrets, opens no sockets, and
// writes nothing to the state directory, so it is safe to run against a
// production pipeline config. Retries and escalation are excluded: both are
// responses to failures a projection cannot predict, and both spend above the
// ceiling when they happen.
func Explain(p *pipeline.Pipeline, opts ...Option) (*Projection, error) {
	cfg := Config{Workers: 8, Retry: runtime.DefaultRetry}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.Registry == nil {
		cfg.Registry = model.NewRegistry()
	}

	// An in-memory CAS: a projection must not touch the run's state dir, and
	// the broadcast hashes it needs are only there to make the compiled plan —
	// and therefore the fingerprints — identical to the one Run would compile.
	cas, err := store.NewCAS("")
	if err != nil {
		return nil, err
	}
	broadcasts := store.NewBroadcasts(cas)
	for _, name := range slices.Sorted(maps.Keys(cfg.Broadcasts)) {
		if _, err := broadcasts.Register(name, cfg.Broadcasts[name]); err != nil {
			return nil, err
		}
	}
	// MCP servers are compiled against their *declaration* rather than a
	// connection: Explain opens no sockets, and dialing a server to discover
	// its tools would be exactly that. A stage naming an unregistered server
	// or a tool outside the deployment's allowlist still fails here; what is
	// missing is the tool-descriptor digest, so a stage that declares MCP
	// fingerprints differently under Explain than under Run. Nothing in a
	// projection reads a fingerprint, so this costs a warning rather than a
	// wrong number.
	pl, err := plan.Compile(p, cfg.Registry,
		plan.WithBroadcasts(broadcasts.Hashes()),
		plan.WithContinuations(cfg.Continuations),
		plan.WithMCP(mcp.Declared(cfg.MCPServers...)))
	if err != nil {
		return nil, err
	}

	values, err := decodeBroadcasts(cfg.Broadcasts)
	if err != nil {
		return nil, err
	}

	e := &explainer{
		cfg:     cfg,
		values:  values,
		ratio:   cfg.ExpectedOutput,
		recs:    map[string][]core.Record{},
		tainted: map[string]bool{},
	}
	if e.ratio <= 0 {
		e.ratio = defaultExpectedOutput
	}
	for _, sp := range pl.Order {
		if len(sp.Stage.Opts.MCP) > 0 {
			e.warnf("stage %q calls MCP tools, which cost no tokens and are not "+
				"priced here; its wall-clock is bounded by the server rather than "+
				"by the rate limits below", sp.Stage.ID)
			break
		}
	}

	proj := &Projection{
		Pipeline: p.Name,
		Driver:   "barrier",
		Budget:   cfg.RunBudget,
	}
	if cfg.Streaming {
		proj.Driver = "streaming"
	}
	for _, sp := range pl.Order {
		sproj, err := e.stage(sp)
		if err != nil {
			return nil, err
		}
		proj.Stages = append(proj.Stages, sproj)
	}
	proj.Warnings = e.warnings
	if cfg.EventHandler != nil {
		proj.publish(cfg.EventHandler)
	}
	return proj, nil
}

// publish emits the projection on the run's event handler. Pointing Explain
// and Run at one handler is what lets an observer — the constellation view,
// say — hold both halves of the comparison: the stages arrive with what they
// are expected to cost, and then the tasks arrive with what they actually do.
//
// Stages go first and the run-level total last, so a consumer that treats
// run.projected as "the projection is complete" sees a consistent picture.
func (p *Projection) publish(fn func(observe.Event)) {
	now := time.Now()
	// Note carries the one qualifier an observer must not miss: a stage whose
	// counts were guessed, and a run whose ceiling is therefore not a bound.
	for _, s := range p.Stages {
		e := observe.Event{
			Type: observe.StageProjected, Time: now, Pipeline: p.Pipeline,
			Stage: s.Stage, Kind: s.Kind, Model: s.Model,
			Records: s.Records, Usage: s.Usage, Ceiling: s.Ceiling,
			Latency: s.AdmissionFloor,
		}
		if s.Estimated {
			e.Note = "estimated"
		}
		fn(e)
	}
	// The pipeline's name is what lets an observer holding several forecasts
	// hand each one to the run it actually predicted.
	run := observe.Event{
		Type: observe.RunProjected, Time: now, Pipeline: p.Pipeline,
		Kind: p.Driver, Usage: p.Expected(), Ceiling: p.Ceiling(),
		Budget: p.Budget, Latency: p.AdmissionFloor(),
		Detail: strings.Join(p.Warnings, "\n"),
	}
	if p.Partial() {
		run.Note = "partial"
	}
	fn(run)
}

type explainer struct {
	cfg    Config
	values map[string]any // broadcast name → value as a task would read it
	ratio  float64
	recs   map[string][]core.Record
	// tainted marks stages whose output records have a shape the plan could
	// not know — a ParseJSON stage with no sample — so every stage downstream
	// can be reported as estimated rather than computed.
	tainted  map[string]bool
	seen     map[string]bool
	warnings []string
}

func (e *explainer) warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if e.seen == nil {
		e.seen = map[string]bool{}
	}
	if e.seen[msg] {
		return
	}
	e.seen[msg] = true
	e.warnings = append(e.warnings, msg)
}

// decodeBroadcasts round-trips registered values through JSON the way the
// store does, so a prefix rendering {{broadcastJSON "x"}} is measured on the
// bytes a task would actually receive.
func decodeBroadcasts(values map[string]any) (map[string]any, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(values))
	for name, v := range values {
		blob, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("broadcast %q: %w", name, err)
		}
		var decoded any
		if err := json.Unmarshal(blob, &decoded); err != nil {
			return nil, fmt.Errorf("broadcast %q: %w", name, err)
		}
		out[name] = decoded
	}
	return out, nil
}

// funcs binds the broadcast template functions to exactly the values this
// stage declared. Scoping them to the declaration is deliberate: a prefix
// that reads a broadcast the stage never declared fails at run time, and this
// is the cheapest possible place to find that out.
func (e *explainer) funcs(declared map[string]string) template.FuncMap {
	get := func(name string) (any, error) {
		if _, ok := declared[name]; !ok {
			return nil, fmt.Errorf("broadcast %q: not declared by this stage "+
				"(add pipeline.WithBroadcast(%q))", name, name)
		}
		return e.values[name], nil
	}
	return template.FuncMap{
		"broadcast": get,
		"broadcastJSON": func(name string) (string, error) {
			v, err := get(name)
			if err != nil {
				return "", err
			}
			b, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				return "", fmt.Errorf("broadcast %q: %w", name, err)
			}
			return string(b), nil
		},
	}
}

func (e *explainer) stage(sp *plan.StagePlan) (StageProjection, error) {
	s := sp.Stage
	var input []core.Record
	if up := s.Upstream; up != nil {
		input = e.recs[up.ID]
		// Uncertainty about a record's shape flows downstream with the record.
		if e.tainted[up.ID] {
			e.tainted[s.ID] = true
		}
	}
	out := StageProjection{Stage: s.ID, Kind: string(s.Kind),
		Records: len(input), Estimated: e.tainted[s.ID]}

	switch s.Kind {
	case pipeline.KindSource:
		recs := s.SourceRecords
		if s.SourceFn != nil {
			sample, ok := e.cfg.SourceSamples[s.ID]
			if !ok {
				e.warnf("source %q is a function, which Explain does not invoke: "+
					"nothing downstream of it is projected (supply records with "+
					"loom.WithSourceSample(%q, ...))", s.ID, s.ID)
			}
			recs = sample
		}
		e.recs[s.ID] = recs
		out.Records = len(recs)

	case pipeline.KindFused:
		e.recs[s.ID] = e.applyFused(s, input)

	case pipeline.KindWindow:
		// A window neither creates nor changes records; it decides when a set
		// of them is closed. Projecting it as a pass-through therefore prices
		// one pane — which is the right unit, because a pane is what a stream
		// job's downstream stages run once per. Multiply by the panes you
		// expect per hour to get the hourly bill.
		e.recs[s.ID] = input

	case pipeline.KindCombine:
		folded, err := foldCombine(s, input)
		if err != nil {
			e.warnf("stage %q: combine function returned an error during "+
				"projection (%v); downstream estimates use the first record", s.ID, err)
			if len(input) > 0 {
				folded = []core.Record{input[0].Clone()}
			}
		}
		e.recs[s.ID] = folded

	case pipeline.KindInfer:
		e.infer(sp, input, &out)

	case pipeline.KindReduceAI:
		e.reduce(sp, input, &out)

	case pipeline.KindIterate:
		e.iterate(sp, input, &out)
	}
	return out, nil
}

// iterate projects an iterative stage: cost per round, multiplied out to the
// round cap.
//
// The round count is the one thing a plan genuinely cannot know — it is a
// property of the data and of a model's judgement about it — so the projection
// does not pretend to. It prices MaxRounds rounds, which is the number
// HaltWhen.Budget needs and the only one that is safe to be wrong about in the
// direction it is wrong: a loop that converges early costs less than this, and
// under-counting a loop is how a budget gets set below what the first
// unconverged run will spend.
//
// Round 0 is exact rather than assumed. An algorithm's Seed is a pure function
// of the vertex table, so the projection calls it and counts the frontier it
// returns instead of guessing that every record starts active — a stage seeded
// from one vertex of a thousand is projected as one call, not a thousand.
func (e *explainer) iterate(sp *plan.StagePlan, input []core.Record, out *StageProjection) {
	s := sp.Stage
	spec := s.Iterate
	rounds := spec.Halt.MaxRounds

	frontier := len(input)
	if tbl, err := newVertexTable(input); err != nil {
		e.warnf("stage %q: %v; the projection assumes every record starts active", s.ID, err)
	} else if msgs, err := spec.Algorithm.Seed(tbl); err != nil {
		e.warnf("stage %q: the %s algorithm's Seed failed during projection (%v), "+
			"so round 0 is assumed to start every record", s.ID, spec.Algorithm.Name(), err)
	} else {
		seen := map[string]bool{}
		for _, m := range msgs {
			seen[m.To] = true
		}
		frontier = len(seen)
	}

	// How wide a later round can get. A frontier cap bounds it outright; a
	// closed graph bounds it at the vertex count, because a message can only
	// wake a vertex that exists. A stage that can grow and has no cap has no
	// bound at all, and saying so is the whole job here.
	bound := spec.MaxFrontier
	switch {
	case bound > 0:
		if spec.Grow == nil {
			bound = min(bound, len(input))
		}
	case spec.Grow == nil:
		bound = len(input)
	default:
		bound = frontier
		out.Estimated = true
		e.warnf("stage %q can create vertices (Grow) and sets no MaxFrontier, so its "+
			"frontier — and its cost — is unbounded by the plan: this row prices %d "+
			"round(s) at round 0's frontier of %d and is a floor, not a ceiling "+
			"(set pipeline.IterateSpec.MaxFrontier to bound it)", s.ID, rounds, frontier)
	}

	out.Calls = frontier + (rounds-1)*bound
	// One vertex per task, whatever the stage's batch size: the loop forces it
	// so that per-vertex cache keys stay per-vertex.
	out.Tasks = out.Calls

	if len(sp.Candidates) == 0 {
		return
	}
	info := sp.Candidates[0]
	out.Model = info.ID
	out.CachePrefix = out.Tasks > 1 && !s.Opts.NoPrefixCache && !spec.Step.Binding.IsZero()

	maxTokens := spec.Step.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	expected := max(1, int(e.ratio*float64(maxTokens)))
	shared := model.EstimateTokens(spec.Step.System) +
		model.EstimateTokens(e.renderPrefix(s.ID, spec.Step.Prefix, spec.Step.Context, sp.Broadcasts))

	// Round 0's prompts, measured against the records that will produce them
	// with the empty inbox a seeded vertex actually reads.
	seeded := make([]core.Record, 0, len(input))
	for _, r := range input {
		c := r.Clone()
		c.Data[algo.FieldInbox] = []string{}
		c.Data[algo.FieldSenders] = []string{}
		seeded = append(seeded, c)
	}
	perRound := e.renderPrompts(s.ID, spec.Step.Prompt, seeded, sp.Broadcasts)

	// Later rounds carry inboxes, and an inbox is model output: its size is not
	// derivable from the plan any more than a response length is. Repeating
	// round 0's prompt sizes is therefore a floor on the prompt side, and it is
	// named as one rather than presented as a measurement.
	prompts := make([]int, 0, out.Calls)
	for i := 0; i < out.Calls; i++ {
		if len(perRound) == 0 {
			break
		}
		prompts = append(prompts, perRound[i%len(perRound)])
	}
	if rounds > 1 && spec.MaxInbox != 1 {
		e.warnf("stage %q prices every round at round 0's prompt sizes, but rounds "+
			"after the first also carry an inbox, whose size is model output: the "+
			"prompt side of this row is a floor (pipeline.IterateSpec.MaxInbox bounds it)",
			s.ID)
	}

	e.accumulate(out, info, prompts, shared, expected, maxTokens)
	// The vertices the stage leaves behind, standing in for downstream stages.
	// A stage that grows its graph leaves more than this, and the warning
	// above says so.
	e.recs[s.ID] = e.inferOutputs(s.ID, &spec.Step, input, expected)
}

// applyFused runs a fused stage's pure functions for real. They are ordinary
// deterministic Go transforms — that is the definition of the fusion the
// planner performed — so executing them is what makes every downstream record
// count exact instead of extrapolated from a selectivity guess.
func (e *explainer) applyFused(s *pipeline.Stage, input []core.Record) []core.Record {
	cur := input
	for _, sub := range s.Fused {
		if sub.MapCtxFn != nil {
			e.warnf("stage %q: MapTools needs a provisioned session, so Explain "+
				"treats it as identity; record counts past it assume it drops nothing", sub.ID)
			continue
		}
		next := make([]core.Record, 0, len(cur))
		for _, r := range cur {
			recs, err := applyPure(sub, r)
			if err != nil {
				e.warnf("stage %q: %v during projection; the record is kept as-is", sub.ID, err)
				next = append(next, r)
				continue
			}
			next = append(next, recs...)
		}
		cur = next
	}
	return cur
}

// applyPure applies one pure transform to one record, converting a panic into
// an error: Explain is a pre-flight check and must not be the thing that
// brings down the caller.
func applyPure(s *pipeline.Stage, r core.Record) (out []core.Record, err error) {
	defer func() {
		if p := recover(); p != nil {
			out, err = nil, fmt.Errorf("%s panicked: %v", s.Kind, p)
		}
	}()
	switch {
	case s.MapFn != nil:
		nr, err := s.MapFn(r.Clone())
		if err != nil {
			return nil, err
		}
		return []core.Record{nr}, nil
	case s.FilterFn != nil:
		keep, err := s.FilterFn(r)
		if err != nil {
			return nil, err
		}
		if !keep {
			return nil, nil
		}
		return []core.Record{r}, nil
	case s.FlatMapFn != nil:
		return s.FlatMapFn(r.Clone())
	}
	return []core.Record{r}, nil
}

// infer projects a per-record inference stage. One model call per record —
// batching groups records into scheduled tasks, not into requests — and one
// shared prefix per task.
func (e *explainer) infer(sp *plan.StagePlan, input []core.Record, out *StageProjection) {
	s := sp.Stage
	spec := s.Infer

	batch := s.Opts.BatchSize
	if batch <= 0 {
		batch = 1
	}
	out.Tasks = (len(input) + batch - 1) / batch
	out.Calls = len(input)

	// Mirror the planner's rule rather than restate its intent: it enables
	// prefix caching when a stage builds more than one *task*, so a stage
	// whose whole input fits one batch pays full price on every record even
	// though it issues many calls. Worth knowing before a run, not worth
	// silently modelling differently here.
	out.CachePrefix = out.Tasks > 1 && !s.Opts.NoPrefixCache && !spec.Binding.IsZero()
	if !out.CachePrefix && out.Calls > 1 && !s.Opts.NoPrefixCache && spec.Prefix != "" {
		e.warnf("stage %q issues %d calls in %d task(s), and the planner keys prefix "+
			"caching on task count, so the shared prefix is sent uncached on every call "+
			"(a smaller pipeline.WithBatchSize would cache it)", s.ID, out.Calls, out.Tasks)
	}

	if len(sp.Candidates) == 0 {
		return
	}
	info := sp.Candidates[0]
	out.Model = info.ID

	maxTokens := spec.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	expected := max(1, int(e.ratio*float64(maxTokens)))

	shared := model.EstimateTokens(spec.System) +
		model.EstimateTokens(e.renderPrefix(s.ID, spec.Prefix, spec.Context, sp.Broadcasts))
	prompts := e.renderPrompts(s.ID, spec.Prompt, input, sp.Broadcasts)

	e.accumulate(out, info, prompts, shared, expected, maxTokens)
	e.recs[s.ID] = e.inferOutputs(s.ID, spec, input, expected)
}

// reduce projects a hierarchical AI reduce: each level groups the level below
// it FanIn at a time and issues one call per group, until one record remains.
func (e *explainer) reduce(sp *plan.StagePlan, input []core.Record, out *StageProjection) {
	s := sp.Stage
	spec := s.Reduce

	fanIn := spec.FanIn
	if fanIn <= 1 {
		fanIn = 8
	}
	if len(sp.Candidates) == 0 {
		e.recs[s.ID] = nil
		return
	}
	info := sp.Candidates[0]
	out.Model = info.ID

	maxTokens := spec.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	expected := max(1, int(e.ratio*float64(maxTokens)))
	itemField := spec.ItemField
	if itemField == "" {
		itemField = "output"
	}
	outField := spec.OutputField
	if outField == "" {
		outField = "output"
	}

	shared := model.EstimateTokens(spec.System) +
		model.EstimateTokens(e.renderPrefix(s.ID, spec.Prefix, nil, sp.Broadcasts))

	// Walk the tree the way the driver does: build the level's groups, count a
	// call per group, and feed the level's outputs into the next one.
	cur := input
	var prompts []int
	for len(cur) > 0 {
		groups := (len(cur) + fanIn - 1) / fanIn
		for i := 0; i < len(cur); i += fanIn {
			group := cur[i:min(i+fanIn, len(cur))]
			items := make([]string, 0, len(group))
			for _, r := range group {
				// Mirror the operator: above the bottom level the inputs are
				// this stage's own aggregates, which carry OutputField.
				item := r.String(itemField)
				if item == "" && itemField != outField {
					item = r.String(outField)
				}
				items = append(items, item)
			}
			prompts = append(prompts, e.renderReducePrompt(s.ID, spec.Prompt, items, sp.Broadcasts))
		}
		out.Tasks += groups
		cur = e.reduceOutputs(s, groups, expected)
		if groups == 1 {
			break
		}
	}
	out.Calls = len(prompts)
	out.CachePrefix = out.Tasks > 1 && !s.Opts.NoPrefixCache && !spec.Binding.IsZero()

	e.accumulate(out, info, prompts, shared, expected, maxTokens)
	e.recs[s.ID] = cur
}

// accumulate turns per-call prompt sizes into the stage's two projections,
// splitting prompt tokens across the three disjoint classes exactly as a
// provider's prompt cache does: the first call carrying a shared head writes
// the entry, every later one reads it.
func (e *explainer) accumulate(out *StageProjection, info model.Info, prompts []int, shared, expected, maxTokens int) {
	for i, p := range prompts {
		u := core.Usage{InputTokens: p, Requests: 1}
		switch {
		case !out.CachePrefix:
			u.InputTokens += shared
		case i == 0:
			u.CacheWriteTokens = shared
		default:
			u.CacheReadTokens = shared
		}

		u.OutputTokens = expected
		u.CostUSD = info.Pricing.Cost(u)
		out.Usage.Add(u)

		u.OutputTokens = maxTokens
		u.CostUSD = info.Pricing.Cost(u)
		out.Ceiling.Add(u)
	}
	out.AdmissionFloor = admissionFloor(info, len(prompts), out.Ceiling.TotalTokens())
}

// admissionFloor is the shortest time the scheduler's token buckets can
// release this many calls and tokens. Workers do not enter into it: no amount
// of concurrency moves work through a provider's per-minute ceiling faster.
func admissionFloor(info model.Info, calls, tokens int) time.Duration {
	var floor time.Duration
	per := func(units, limit int) time.Duration {
		if limit <= 0 || units <= 0 {
			return 0
		}
		return time.Duration(float64(units) / float64(limit) * float64(time.Minute))
	}
	floor = max(floor, per(calls, info.Limits.RequestsPerMinute))
	floor = max(floor, per(tokens, info.Limits.TokensPerMinute))
	return floor
}

// renderPrefix renders a stage's shared prompt head the way ops.sharedPrefix
// does — context fragments, then the prefix template with no record data in
// scope — so its token count is measured on the bytes the provider receives.
func (e *explainer) renderPrefix(stageID, prefix string, frags []task.Fragment, declared map[string]string) string {
	var b strings.Builder
	if len(frags) > 0 {
		b.WriteString("<context>\n")
		for _, f := range frags {
			fmt.Fprintf(&b, "<%s>\n%s\n</%s>\n", f.Name, f.Content, f.Name)
		}
		b.WriteString("</context>\n\n")
	}
	if prefix == "" {
		return b.String()
	}
	tmpl, err := template.New(stageID + ".prefix").Funcs(e.funcs(declared)).Parse(prefix)
	if err != nil {
		e.warnf("stage %q: prefix template does not parse (%v)", stageID, err)
		return b.String() + prefix
	}
	var rendered strings.Builder
	if err := tmpl.Execute(&rendered, nil); err != nil {
		e.warnf("stage %q: shared prefix does not render (%v), so its size is "+
			"estimated from the template source", stageID, err)
		return b.String() + prefix
	}
	return b.String() + rendered.String()
}

// renderPrompts renders the stage's prompt against every record that will
// reach it and returns the per-call prompt token counts. Broadcast functions
// are bound to exactly what the stage declared, the way the executor binds
// them — a taxonomy interpolated into every prompt is usually far larger than
// the template that reads it, so leaving it unrendered would under-count the
// stage. When a template cannot render — most often because it reads a field a
// ParseJSON stage upstream will introduce, whose name no plan knows — it falls
// back to the template source and says so once.
func (e *explainer) renderPrompts(stageID, prompt string, input []core.Record, declared map[string]string) []int {
	tmpl, err := template.New(stageID).Funcs(e.funcs(declared)).
		Option("missingkey=error").Parse(prompt)
	if err != nil {
		e.warnf("stage %q: prompt template does not parse (%v)", stageID, err)
		return estimateEach(prompt, len(input))
	}
	out := make([]int, 0, len(input))
	for _, r := range input {
		var sb strings.Builder
		if err := tmpl.Execute(&sb, r.Data); err != nil {
			e.warnf("stage %q: prompt does not render against a projected record "+
				"(%v), so prompt sizes for this stage are estimated from the "+
				"template source", stageID, err)
			return estimateEach(prompt, len(input))
		}
		out = append(out, model.EstimateTokens(sb.String()))
	}
	return out
}

func (e *explainer) renderReducePrompt(stageID, prompt string, items []string, declared map[string]string) int {
	tmpl, err := template.New(stageID).Funcs(e.funcs(declared)).Parse(prompt)
	if err != nil {
		e.warnf("stage %q: prompt template does not parse (%v)", stageID, err)
		return model.EstimateTokens(prompt)
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, map[string]any{"Items": items, "Count": len(items)}); err != nil {
		e.warnf("stage %q: aggregation prompt does not render (%v), so its size is "+
			"estimated from the template source", stageID, err)
		return model.EstimateTokens(prompt)
	}
	return model.EstimateTokens(sb.String())
}

func estimateEach(prompt string, n int) []int {
	est := model.EstimateTokens(prompt)
	out := make([]int, n)
	for i := range out {
		out[i] = est
	}
	return out
}

// inferOutputs stands in for the records this stage has not produced. The
// placeholder is sized to the projected response so that a downstream stage
// reading this stage's output measures a prompt of the right order — the
// alternative, an empty field, would under-count every prompt after the first
// inference in a chain.
// The spec is passed rather than read off the stage because an iterative
// stage's step is an InferSpec too, and stands in for its vertices the same
// way.
func (e *explainer) inferOutputs(stageID string, spec *pipeline.InferSpec, input []core.Record, expected int) []core.Record {
	field := spec.OutputField
	if field == "" {
		field = "output"
	}
	sample, sampled := e.cfg.StageSamples[stageID]
	if spec.ParseJSON && !sampled {
		// This is the one way a projection can be wrong in the dangerous
		// direction. A downstream Filter testing a field the model was going to
		// invent sees it missing, drops every record, and everything past it
		// projects as no work at all — so the total understates the run rather
		// than bounding it. Say so, and say how to fix it.
		e.tainted[stageID] = true
		e.warnf("stage %q parses its output as JSON, so the fields it adds to each "+
			"record are not knowable from the plan: stages below it are estimated, "+
			"and a filter testing one of those fields will drop every record here "+
			"while keeping them in the real run. Name the fields with "+
			"loom.WithStageSample(%q, ...) to project them exactly", stageID, stageID)
	}
	placeholder := strings.Repeat("x", 4*expected)
	out := make([]core.Record, 0, len(input))
	for _, r := range input {
		nr := r.Clone()
		if spec.ParseJSON {
			for k, v := range sample {
				nr.Data[k] = v
			}
		} else {
			nr.Data[field] = placeholder
		}
		out = append(out, nr)
	}
	return out
}

func (e *explainer) reduceOutputs(s *pipeline.Stage, n, expected int) []core.Record {
	field := s.Reduce.OutputField
	if field == "" {
		field = "output"
	}
	placeholder := strings.Repeat("x", 4*expected)
	out := make([]core.Record, 0, n)
	for i := range n {
		out = append(out, core.NewRecord(fmt.Sprintf("%s-%d", s.ID, i),
			map[string]any{field: placeholder}))
	}
	return out
}

// String renders the projection as a table, in the shape of the run report it
// is trying to predict.
func (p *Projection) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "projection  %s  (%s driver, no calls issued)\n", p.Pipeline, p.Driver)
	fmt.Fprintf(&b, "%-22s %-24s %6s %6s %8s %8s %10s %10s %8s\n",
		"stage", "model", "recs", "calls", "prompt", "cached", "exp($)", "max($)", "floor")
	for _, s := range p.Stages {
		name := s.Stage
		if s.Estimated {
			name = "~" + name // counts guessed, not computed
		}
		fmt.Fprintf(&b, "%-22s %-24s %6d %6d %8d %8d %10.4f %10.4f %8s\n",
			name, s.Model, s.Records, s.Calls,
			s.Usage.PromptTokens(), s.Usage.CacheReadTokens,
			s.Usage.CostUSD, s.Ceiling.CostUSD,
			s.AdmissionFloor.Round(time.Second))
	}
	exp, ceil := p.Expected(), p.Ceiling()
	fmt.Fprintf(&b, "%-22s %-24s %6s %6d %8d %8d %10.4f %10.4f %8s\n",
		"TOTAL", "", "", exp.Requests, exp.PromptTokens(), exp.CacheReadTokens,
		exp.CostUSD, ceil.CostUSD, p.AdmissionFloor().Round(time.Second))

	if p.Partial() {
		// Being explicit here is the whole point. A ceiling that covers only
		// the stages the projection understood is not a cap on the run, and
		// presenting it as one is exactly the failure this tool exists to
		// prevent.
		fmt.Fprintf(&b, "expected %d tokens for $%.4f across the stages that could be "+
			"computed\nstages marked ~ are estimated, so $%.4f is NOT a bound on the "+
			"run — work below them is unaccounted for\n",
			exp.TotalTokens(), exp.CostUSD, ceil.CostUSD)
	} else {
		fmt.Fprintf(&b, "expected %d tokens for $%.4f; cannot exceed %d tokens / $%.4f before retries\n",
			exp.TotalTokens(), exp.CostUSD, ceil.TotalTokens(), ceil.CostUSD)
	}
	if p.Budget.MaxCostUSD > 0 || p.Budget.MaxTokens > 0 {
		verdict := "covers the ceiling"
		if !p.FitsBudget() {
			verdict = "is below the ceiling: the governor will stop the run and " +
				"return partial results"
		} else if p.Partial() {
			verdict = "covers the projected stages, but the projection is incomplete"
		}
		fmt.Fprintf(&b, "run budget %s %s\n", budgetDetail(p.Budget), verdict)
	}
	if len(p.Warnings) > 0 {
		b.WriteString("warnings:\n")
		for _, w := range p.Warnings {
			fmt.Fprintf(&b, "  - %s\n", w)
		}
	}
	return b.String()
}

func budgetDetail(b core.Budget) string {
	var parts []string
	if b.MaxCostUSD > 0 {
		parts = append(parts, fmt.Sprintf("$%.4f", b.MaxCostUSD))
	}
	if b.MaxTokens > 0 {
		parts = append(parts, fmt.Sprintf("%d tokens", b.MaxTokens))
	}
	return strings.Join(parts, " / ")
}
