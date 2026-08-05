// Package plan compiles a pipeline's logical graph into an executable plan:
// it validates the graph, fuses adjacent pure stages into single task
// boundaries, resolves model bindings to candidate ladders, computes
// deterministic operation fingerprints (the basis of caching and lineage),
// and assembles least-privilege task envelopes.
package plan

import (
	"fmt"
	"sort"
	"sync/atomic"
	"text/template"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/task"
)

// StagePlan is one executable stage.
type StagePlan struct {
	Stage       *pipeline.Stage
	Fingerprint string
	Candidates  []model.Info      // resolved binding ladder for AI stages
	Broadcasts  map[string]string // declared shared values → content hash
	Cacheable   bool

	// built counts the tasks this stage has produced across every call to
	// BuildTasks. It is what makes the prefix-cache break-even test work
	// under both drivers: the barrier driver hands over a stage's whole
	// input at once, while a streaming driver arrives with a couple of
	// records at a time, and only a running total can tell the difference
	// between "this stage makes one call" and "this stage is being fed".
	built atomic.Int64
}

// Option configures compilation.
type Option func(*compileOpts)

type compileOpts struct {
	broadcasts map[string]string
}

// WithBroadcasts supplies the run's registered shared values as name →
// content hash. A stage may only declare broadcasts present here (typos fail
// compilation rather than the first model call), and every hash a stage
// declares is folded into its fingerprint, so changing a broadcast's value
// invalidates exactly the cached results that could have observed it.
func WithBroadcasts(hashes map[string]string) Option {
	return func(o *compileOpts) { o.broadcasts = hashes }
}

// Plan is the compiled pipeline.
type Plan struct {
	Pipeline *pipeline.Pipeline
	Order    []*StagePlan // topological execution order
	ByID     map[string]*StagePlan
	Children map[string][]string // stage → downstream stage IDs
}

// Terminal returns the IDs of stages with no downstream consumers.
func (p *Plan) Terminal() []string {
	var out []string
	for _, sp := range p.Order {
		if len(p.Children[sp.Stage.ID]) == 0 {
			out = append(out, sp.Stage.ID)
		}
	}
	return out
}

func isPure(k pipeline.StageKind) bool {
	return k == pipeline.KindMap || k == pipeline.KindFilter || k == pipeline.KindFlatMap
}

// Compile validates and optimizes the pipeline against the registry.
func Compile(p *pipeline.Pipeline, reg *model.Registry, opts ...Option) (*Plan, error) {
	var co compileOpts
	for _, o := range opts {
		o(&co)
	}
	stages := p.Stages()
	if len(stages) == 0 {
		return nil, fmt.Errorf("pipeline %q has no stages", p.Name)
	}

	seen := map[string]*pipeline.Stage{}
	for _, s := range stages {
		if s.ID == "" {
			return nil, fmt.Errorf("pipeline %q: stage with empty name", p.Name)
		}
		if _, dup := seen[s.ID]; dup {
			return nil, fmt.Errorf("pipeline %q: duplicate stage name %q", p.Name, s.ID)
		}
		seen[s.ID] = s
		if s.Upstream == nil && s.Kind != pipeline.KindSource {
			return nil, fmt.Errorf("stage %q: non-source stage without upstream", s.ID)
		}
	}

	// Child counts on the logical graph drive fusion decisions: a pure stage
	// can absorb its successor only if it is the successor's sole consumer.
	childCount := map[string]int{}
	for _, s := range stages {
		if s.Upstream != nil {
			childCount[s.Upstream.ID]++
		}
	}

	// Fuse maximal runs of adjacent pure stages (single-consumer links) into
	// synthetic fused stages: fewer task boundaries, less serialization.
	fusedInto := map[string]*pipeline.Stage{} // original stage ID → fused stage
	var order []*pipeline.Stage
	for _, s := range stages {
		if fusedInto[s.ID] != nil {
			continue // already absorbed
		}
		if !isPure(s.Kind) {
			order = append(order, s)
			continue
		}
		run := []*pipeline.Stage{s}
		cur := s
		for childCount[cur.ID] == 1 {
			next := soleChild(stages, cur.ID)
			if next == nil || !isPure(next.Kind) {
				break
			}
			run = append(run, next)
			cur = next
		}
		fused := &pipeline.Stage{
			ID:       run[len(run)-1].ID, // fused stage keeps the last op's name
			Kind:     pipeline.KindFused,
			Upstream: s.Upstream,
			Fused:    run,
			Opts:     mergeOpts(run),
		}
		for _, r := range run {
			fusedInto[r.ID] = fused
		}
		order = append(order, fused)
	}

	// Rewire upstream pointers that referenced absorbed stages.
	for _, s := range order {
		if s.Upstream != nil {
			if f := fusedInto[s.Upstream.ID]; f != nil && f != s {
				s.Upstream = f
			}
		}
	}

	pl := &Plan{
		Pipeline: p,
		ByID:     map[string]*StagePlan{},
		Children: map[string][]string{},
	}
	for _, s := range order {
		sp := &StagePlan{Stage: s}

		bcast, err := resolveBroadcasts(s, co.broadcasts)
		if err != nil {
			return nil, err
		}
		sp.Broadcasts = bcast

		switch s.Kind {
		case pipeline.KindInfer:
			spec := s.Infer
			if spec.Prompt == "" {
				return nil, fmt.Errorf("stage %q: empty prompt template", s.ID)
			}
			if _, err := template.New(s.ID).Funcs(pipeline.TemplateFuncs()).
				Option("missingkey=error").Parse(spec.Prompt); err != nil {
				return nil, fmt.Errorf("stage %q: prompt template: %w", s.ID, err)
			}
			cands, err := reg.Candidates(spec.Binding)
			if err != nil {
				return nil, fmt.Errorf("stage %q: %w", s.ID, err)
			}
			if spec.Prefix != "" {
				if _, err := template.New(s.ID + ".prefix").Funcs(pipeline.TemplateFuncs()).
					Parse(spec.Prefix); err != nil {
					return nil, fmt.Errorf("stage %q: prefix template: %w", s.ID, err)
				}
			}
			sp.Candidates = cands
			sp.Cacheable = !s.Opts.NoCache
			sp.Fingerprint, err = fingerprint(sp, prefixKey(spec.Prefix,
				"infer", bindingKey(spec.Binding), spec.System,
				spec.Prompt, spec.MaxTokens, spec.ParseJSON, spec.OutputField,
				fragmentKey(spec.Context), s.Opts.Version)...)
			if err != nil {
				return nil, err
			}

		case pipeline.KindReduceAI:
			spec := s.Reduce
			if spec.Prompt == "" {
				return nil, fmt.Errorf("stage %q: empty prompt template", s.ID)
			}
			if _, err := template.New(s.ID).Funcs(pipeline.TemplateFuncs()).
				Parse(spec.Prompt); err != nil {
				return nil, fmt.Errorf("stage %q: prompt template: %w", s.ID, err)
			}
			if spec.Prefix != "" {
				if _, err := template.New(s.ID + ".prefix").Funcs(pipeline.TemplateFuncs()).
					Parse(spec.Prefix); err != nil {
					return nil, fmt.Errorf("stage %q: prefix template: %w", s.ID, err)
				}
			}
			cands, err := reg.Candidates(spec.Binding)
			if err != nil {
				return nil, fmt.Errorf("stage %q: %w", s.ID, err)
			}
			sp.Candidates = cands
			sp.Cacheable = !s.Opts.NoCache
			sp.Fingerprint, err = fingerprint(sp, prefixKey(spec.Prefix,
				"reduce_ai", bindingKey(spec.Binding), spec.System,
				spec.Prompt, spec.FanIn, spec.MaxTokens, spec.ItemField, spec.OutputField,
				s.Opts.Version)...)
			if err != nil {
				return nil, err
			}

		case pipeline.KindIterate:
			spec := s.Iterate
			if spec.Algorithm == nil {
				return nil, fmt.Errorf("stage %q: iterate without an algorithm "+
					"(try algo.NewBSP, algo.NewRefine, or algo.NewBeam)", s.ID)
			}
			// A loop over paid calls with no round cap is the one authoring
			// error whose cost is unbounded, and compilation is the last point
			// at which catching it is free.
			if spec.Halt.MaxRounds <= 0 {
				return nil, fmt.Errorf("stage %q: iterate needs Halt.MaxRounds > 0", s.ID)
			}
			if spec.Step.Prompt == "" {
				return nil, fmt.Errorf("stage %q: empty prompt template", s.ID)
			}
			if _, err := template.New(s.ID).Funcs(pipeline.TemplateFuncs()).
				Option("missingkey=error").Parse(spec.Step.Prompt); err != nil {
				return nil, fmt.Errorf("stage %q: prompt template: %w", s.ID, err)
			}
			if spec.Step.Prefix != "" {
				if _, err := template.New(s.ID + ".prefix").Funcs(pipeline.TemplateFuncs()).
					Parse(spec.Step.Prefix); err != nil {
					return nil, fmt.Errorf("stage %q: prefix template: %w", s.ID, err)
				}
			}
			cands, err := reg.Candidates(spec.Step.Binding)
			if err != nil {
				return nil, fmt.Errorf("stage %q: %w", s.ID, err)
			}
			sp.Candidates = cands
			sp.Cacheable = !s.Opts.NoCache
			// The algorithm is deliberately absent from the fingerprint. It
			// decides which vertices run and what they carry, and both of
			// those are already in the task's cache key — the key is the
			// record plus its inbox. Two stages whose steps are identical
			// genuinely perform the same operation on a given (state, inbox),
			// whatever routed the messages there, so they should share a
			// cached result. Hashing the algorithm in would break that reuse
			// to record something no consumer of the fingerprint needs.
			sp.Fingerprint, err = fingerprint(sp, prefixKey(spec.Step.Prefix,
				"iterate", bindingKey(spec.Step.Binding), spec.Step.System,
				spec.Step.Prompt, spec.Step.MaxTokens, spec.Step.ParseJSON,
				spec.Step.OutputField, fragmentKey(spec.Step.Context),
				s.Opts.Version)...)
			if err != nil {
				return nil, err
			}

		case pipeline.KindFused:
			var names []string
			for _, r := range s.Fused {
				names = append(names, string(r.Kind)+":"+r.ID)
			}
			sp.Fingerprint, err = fingerprint(sp, "fused", names, s.Opts.Version)
			if err != nil {
				return nil, err
			}
			// Go closures aren't content-addressable: only Version makes a
			// pure stage cacheable.
			sp.Cacheable = s.Opts.Version != "" && !s.Opts.NoCache

		case pipeline.KindSource, pipeline.KindCombine:
			sp.Cacheable = false

		default:
			return nil, fmt.Errorf("stage %q: unexpected kind %q after fusion", s.ID, s.Kind)
		}

		pl.Order = append(pl.Order, sp)
		pl.ByID[s.ID] = sp
	}

	for _, sp := range pl.Order {
		if up := sp.Stage.Upstream; up != nil {
			if _, ok := pl.ByID[up.ID]; !ok {
				return nil, fmt.Errorf("stage %q: upstream %q not found", sp.Stage.ID, up.ID)
			}
			pl.Children[up.ID] = append(pl.Children[up.ID], sp.Stage.ID)
		}
	}
	return pl, nil
}

func soleChild(stages []*pipeline.Stage, parentID string) *pipeline.Stage {
	for _, s := range stages {
		if s.Upstream != nil && s.Upstream.ID == parentID {
			return s
		}
	}
	return nil
}

// mergeOpts combines fused stages' options: the strictest/last-set wins.
func mergeOpts(run []*pipeline.Stage) pipeline.StageOpts {
	var o pipeline.StageOpts
	for _, s := range run {
		if s.Opts.Parallelism > 0 {
			o.Parallelism = s.Opts.Parallelism
		}
		if s.Opts.BatchSize > 0 {
			o.BatchSize = s.Opts.BatchSize
		}
		if s.Opts.Sandbox != "" {
			o.Sandbox = s.Opts.Sandbox
		}
		if s.Opts.Version != "" {
			o.Version += s.Opts.Version + ";"
		}
		if s.Opts.NoCache {
			o.NoCache = true
		}
		o.Broadcasts = append(o.Broadcasts, s.Opts.Broadcasts...)
		if s.Opts.Budget.MaxDuration > 0 {
			o.Budget.MaxDuration = s.Opts.Budget.MaxDuration
		}
		if s.Opts.Budget.MaxAttempts > 0 {
			o.Budget.MaxAttempts = s.Opts.Budget.MaxAttempts
		}
		o.Grants = append(o.Grants, s.Opts.Grants...)
	}
	// A fused run is cache-consistent only if every member declared a
	// version; a partial version string would produce wrong cache reuse
	// when an unversioned member changes.
	for _, s := range run {
		if s.Opts.Version == "" {
			o.Version = ""
			break
		}
	}
	return o
}

func bindingKey(b model.Binding) map[string]any {
	return map[string]any{"model": b.Model, "tier": string(b.Tier), "escalation": b.Escalation}
}

func fragmentKey(frags []task.Fragment) []map[string]string {
	out := make([]map[string]string, 0, len(frags))
	for _, f := range frags {
		out = append(out, map[string]string{"name": f.Name, "content": f.Content})
	}
	return out
}

// resolveBroadcasts maps the names a stage declared to the content hashes of
// the run's registered values, rejecting names that were never registered —
// a typo should fail compilation, not the first model call.
func resolveBroadcasts(s *pipeline.Stage, registered map[string]string) (map[string]string, error) {
	if len(s.Opts.Broadcasts) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(s.Opts.Broadcasts))
	for _, name := range s.Opts.Broadcasts {
		hash, ok := registered[name]
		if !ok {
			return nil, fmt.Errorf(
				"stage %q: broadcast %q is not registered for this run (loom.WithBroadcast)", s.ID, name)
		}
		out[name] = hash
	}
	return out, nil
}

// broadcastKey renders a stage's declared broadcasts as a deterministic
// fingerprint component. Hashing the *values* (via their content hashes), not
// just the names, is what makes the cache honest: edit a broadcast and the
// stages that read it recompute, while stages that never declared it keep
// their cached results.
func broadcastKey(m map[string]string) []map[string]string {
	if len(m) == 0 {
		return nil
	}
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]map[string]string, 0, len(names))
	for _, n := range names {
		out = append(out, map[string]string{"name": n, "hash": m[n]})
	}
	return out
}

// prefixKey appends the shared prompt prefix to a stage's fingerprint
// components, and only when the stage declares one — a stage without a prefix
// hashes exactly as it did before the feature existed, so adopting prefix
// caching elsewhere in a pipeline leaves untouched stages' caches warm.
func prefixKey(prefix string, parts ...any) []any {
	if prefix == "" {
		return parts
	}
	return append(parts, map[string]string{"prefix": prefix})
}

// fingerprint hashes a stage's op spec, appending the broadcast component
// only when the stage declares one — so adding this feature leaves the
// fingerprints (and therefore the warm caches) of existing pipelines
// untouched.
func fingerprint(sp *StagePlan, parts ...any) (string, error) {
	if bk := broadcastKey(sp.Broadcasts); bk != nil {
		parts = append(parts, bk)
	}
	return store.Key(parts...)
}

// Envelope assembles the least-privilege envelope for one of this stage's
// tasks: capability grants for exactly the binding's candidate models and
// their secrets, an egress allowlist of exactly those providers' endpoints
// (plus extraEgress for tools), the stage's context bundle, the content
// hashes of exactly the broadcasts it declared, budget, and sandbox profile.
func (sp *StagePlan) Envelope(runID string, extraEgress []string) task.Envelope {
	s := sp.Stage
	var caps []security.Capability
	var hosts []string
	var binding model.Binding
	var ctxBundle task.ContextBundle

	switch s.Kind {
	case pipeline.KindInfer:
		binding = s.Infer.Binding
		ctxBundle = task.ContextBundle{System: s.Infer.System, Fragments: s.Infer.Context}
	case pipeline.KindReduceAI:
		binding = s.Reduce.Binding
		ctxBundle = task.ContextBundle{System: s.Reduce.System}
	case pipeline.KindIterate:
		// Every round of the loop runs under one envelope, assembled once
		// from the step's binding. A vertex program that discovers its own
		// next target therefore discovers it inside a grant set and an egress
		// allowlist fixed before the first round — which is the property that
		// makes a self-directed loop safe to run at all.
		binding = s.Iterate.Step.Binding
		ctxBundle = task.ContextBundle{
			System: s.Iterate.Step.System, Fragments: s.Iterate.Step.Context}
	}
	for _, info := range sp.Candidates {
		caps = append(caps, security.ModelCap(info.ID))
		if info.SecretRef != "" {
			caps = append(caps, security.SecretCap(info.SecretRef))
		}
		if h := info.Provider.Endpoint(); h != "" {
			hosts = append(hosts, h)
		}
	}
	caps = append(caps, s.Opts.Grants...)

	// Broadcasts travel as references: the grant authorizes the name, the
	// hash locates the bytes in content-addressed storage.
	var bcast map[string]string
	if len(sp.Broadcasts) > 0 {
		bcast = make(map[string]string, len(sp.Broadcasts))
		for name, hash := range sp.Broadcasts {
			bcast[name] = hash
			caps = append(caps, security.DataCap(name))
		}
	}

	sandbox := s.Opts.Sandbox
	if sandbox == "" {
		sandbox = task.SandboxInline
	}

	return task.Envelope{
		RunID:      runID,
		Stage:      s.ID,
		Binding:    binding,
		Grants:     security.NewGrantSet(caps...),
		Egress:     security.EgressPolicy{}.With(append(hosts, extraEgress...)...),
		Context:    ctxBundle,
		Broadcasts: bcast,
		Budget:     s.Opts.Budget,
		Sandbox:    sandbox,
	}
}

// BuildTasks splits input records into scheduled tasks for this stage,
// computing per-task cache keys and admission-control token estimates.
func (sp *StagePlan) BuildTasks(runID string, input []core.Record, extraEgress []string) ([]task.Task, error) {
	batch := sp.Stage.Opts.BatchSize
	if batch <= 0 {
		batch = 1
	}
	return sp.BuildTasksBatch(runID, input, batch, extraEgress)
}

// BuildTasksBatch is BuildTasks with an explicit batch size (used by the
// driver for ReduceAI fan-in groups).
func (sp *StagePlan) BuildTasksBatch(runID string, input []core.Record, batch int, extraEgress []string) ([]task.Task, error) {
	if batch <= 0 {
		batch = 1
	}
	env := sp.Envelope(runID, extraEgress)

	// Prompt-prefix caching pays for itself from the second call onward: the
	// entry costs a write premium and every later hit is a fraction of a
	// fresh input token. A stage that only ever issues one call would pay
	// the premium and never read it back, so the test is whether this stage
	// has more than one task to share the prefix across — counted across the
	// stage's whole life, not just this batch, since a streaming driver
	// builds a stage's tasks a few at a time.
	//
	// A driver that hands over a stage's whole input at once therefore caches
	// from the first task. A driver that streams records in learns the prefix
	// is shared only when the second task arrives, so it writes the entry
	// there and reads it from the third on — one call of warm-up, whatever
	// the stage's eventual size. That is the price of never writing an entry
	// nothing will read, and it is the cheaper side of the trade.
	nTasks := (len(input) + batch - 1) / batch
	total := sp.built.Add(int64(nTasks))
	if !sp.Stage.Opts.NoPrefixCache && !env.Binding.IsZero() {
		env.CachePrefix = total > 1
	}

	var maxTokens int
	switch sp.Stage.Kind {
	case pipeline.KindInfer:
		maxTokens = sp.Stage.Infer.MaxTokens
	case pipeline.KindReduceAI:
		maxTokens = sp.Stage.Reduce.MaxTokens
	case pipeline.KindIterate:
		maxTokens = sp.Stage.Iterate.Step.MaxTokens
	}
	if maxTokens <= 0 {
		maxTokens = 1024
	}

	var tasks []task.Task
	seq := 0
	for i := 0; i < len(input); i += batch {
		end := min(i+batch, len(input))
		group := input[i:end]

		t := task.Task{
			ID:          core.NewID("task"),
			Seq:         seq,
			Stage:       sp.Stage.ID,
			Fingerprint: sp.Fingerprint,
			Input:       group,
			Envelope:    env,
		}
		if sp.Cacheable {
			key, err := store.Key(sp.Fingerprint, group)
			if err != nil {
				return nil, err
			}
			t.CacheKey = key
		}
		est := maxTokens
		for _, r := range group {
			est += model.EstimateTokens(r.String("output")) + 64
		}
		t.EstTokens = est
		tasks = append(tasks, t)
		seq++
	}
	return tasks, nil
}
