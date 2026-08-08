package studio

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
)

// Estimate is what a document is expected to cost, computed without issuing a
// single model call: one row per step, the totals, and everything the price
// rests on that the reader should see before spending it.
//
// It is [loom.Explain]'s projection, joined back onto the steps on the canvas.
// The studio adds one thing to it, and it is the thing that makes the number
// worth showing: it reads the source's records itself — locally, off the disk,
// at no cost — so the projection walks the real 412 records rather than a
// guess about how many there might be.
type Estimate struct {
	Pipeline string `json:"pipeline"`

	Steps []StepPrice `json:"steps"`

	// ExpectedUSD is the run at the assumed response length; CeilingUSD is the
	// same run with every response filling its cap. The ceiling is the number
	// to set the guardrail from, because it rests on no assumption.
	ExpectedUSD float64 `json:"expected_usd"`
	CeilingUSD  float64 `json:"ceiling_usd"`

	// Calls is the model calls the run issues before any retry; Records is how
	// many records enter the first step.
	Calls   int `json:"calls"`
	Records int `json:"records"`

	// InputTokens and OutputTokens are the projected token totals.
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	// CachedTokens are prompt tokens the provider is expected to serve from
	// its prefix cache rather than reprocess.
	CachedTokens int `json:"cached_tokens"`

	// CapUSD is the document's guardrail and FitsCap reports whether the
	// ceiling is under it. A false is not an error: the run stops at the cap
	// with partial results rather than spending past it, which is sometimes
	// exactly what you want.
	CapUSD  float64 `json:"cap_usd"`
	FitsCap bool    `json:"fits_cap"`

	// FloorMS is the shortest wall-clock time the run can take under the
	// models' rate limits, however many workers are free.
	FloorMS int64 `json:"floor_ms"`

	// Partial marks a projection in which some step's record count was guessed
	// rather than computed — the one direction in which this number can be
	// wrong and matter.
	Partial bool `json:"partial"`

	// Warnings name every approximation the projection had to make.
	Warnings []string `json:"warnings,omitempty"`

	// Error is set when the document could not be priced at all: an
	// unregistered model, a prompt that does not parse, a folder that is not
	// there. The studio shows it in place of a price rather than showing a
	// zero, because a pipeline that cannot be compiled has no cost.
	Error string `json:"error,omitempty"`
}

// StepPrice is one step's row in the price table.
type StepPrice struct {
	ID    string `json:"id"`
	Kind  Kind   `json:"kind"`
	Title string `json:"title,omitempty"`

	// Model is the base model of the step's binding, empty for steps that call
	// none.
	Model string `json:"model,omitempty"`

	Records int `json:"records"`
	Calls   int `json:"calls"`

	ExpectedUSD float64 `json:"expected_usd"`
	CeilingUSD  float64 `json:"ceiling_usd"`

	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	CachedTokens int `json:"cached_tokens"`

	// Share is this step's fraction of the run's expected cost, which is the
	// number that answers "where is the money going".
	Share float64 `json:"share"`

	// Free marks a step that issues no model calls: a filter, a derived field,
	// a write. They are the cheap skeleton of the pipeline, and Explain runs
	// them for real rather than modelling them.
	Free bool `json:"free"`

	// Fused marks a step the planner collapsed into the one after it, so it
	// has no separate task boundary and no row of its own in the plan.
	Fused bool `json:"fused"`

	// Estimated marks a step whose record count descends from a model-invented
	// field, making its cost a guess rather than a computation.
	Estimated bool `json:"estimated"`

	// CachePrefix reports whether the planner will share this step's prompt
	// head through the provider's prefix cache.
	CachePrefix bool `json:"cache_prefix"`
}

// PriceOption configures [Price].
type PriceOption func(*priceCfg)

type priceCfg struct {
	registry *model.Registry
	records  map[string][]core.Record
	ratio    float64
	ctx      context.Context
}

// WithRegistry supplies the models the document's steps bind to. Without one
// nothing can be priced: a cost is a property of a model, not of a pipeline.
func WithRegistry(r *model.Registry) PriceOption {
	return func(c *priceCfg) { c.registry = r }
}

// WithRecords supplies a source's records instead of reading them from disk.
// The studio's server uses it to price on every keystroke without re-reading a
// folder of thousands of files.
func WithRecords(step string, recs []core.Record) PriceOption {
	return func(c *priceCfg) {
		if c.records == nil {
			c.records = map[string][]core.Record{}
		}
		c.records[step] = recs
	}
}

// WithExpectedOutput sets the fraction of each step's token cap that responses
// are assumed to fill, for the expected column (default 0.35). It does not
// affect the ceiling, which assumes every response fills its cap.
func WithExpectedOutput(ratio float64) PriceOption {
	return func(c *priceCfg) { c.ratio = ratio }
}

// WithContext bounds the source reads Price performs.
func WithContext(ctx context.Context) PriceOption {
	return func(c *priceCfg) { c.ctx = ctx }
}

// Price projects what running the document would cost.
//
// Every source is read first — on this machine, off the local disk, with no
// model call and no network — and the records are handed to [loom.Explain] as
// samples. That is the difference between this number and the estimates one
// usually gets from a builder UI: Explain executes the pipeline's cheap
// skeleton (every filter, every derived field) against the real records, so
// the call counts below are counted rather than extrapolated, and a branch
// that keeps 148 of 412 days is priced as 148.
//
// A document that cannot be compiled returns an Estimate carrying the error
// rather than an error alone, because the studio has to draw something and a
// zero would be a lie.
func Price(d *Doc, opts ...PriceOption) *Estimate {
	cfg := priceCfg{ctx: context.Background()}
	for _, o := range opts {
		o(&cfg)
	}
	est := &Estimate{Pipeline: d.Name, CapUSD: d.CapUSD, FitsCap: true}
	for i := range d.Steps {
		s := &d.Steps[i]
		est.Steps = append(est.Steps, StepPrice{
			ID: s.ID, Kind: s.Kind, Title: s.Title,
			Free: s.Kind != KindInfer && s.Kind != KindReduce && s.Kind != KindLoop,
		})
	}

	// Dry: Explain runs the pure Go stages for real, and a write step is a
	// pure Go stage that writes files. Pricing a pipeline must not perform any
	// part of it.
	p, err := d.build(true)
	if err != nil {
		est.Error = err.Error()
		return est
	}
	if cfg.registry == nil {
		est.Error = "no models registered: attach a registry to price this pipeline " +
			"(studio.WithRegistry)"
		return est
	}

	lopts := []loom.Option{loom.WithRegistry(cfg.registry)}
	if cfg.ratio > 0 {
		lopts = append(lopts, loom.WithExpectedOutput(cfg.ratio))
	}
	if d.Workers > 0 {
		lopts = append(lopts, loom.WithWorkers(d.Workers))
	}
	if d.CapUSD > 0 {
		lopts = append(lopts, loom.WithRunBudget(core.Budget{MaxCostUSD: d.CapUSD}))
	}
	// A declared answer shape closes the one blind spot in a projection. Explain
	// cannot know which fields a ParseJSON stage will add — the model invents
	// them — so a filter below one drops every record and the rest of the run
	// projects as no work at all. Here the fields are not invented: the step
	// declares them, so the projection can be told exactly what to expect, and
	// the assumption that remains is only how long each one will be.
	for i := range d.Steps {
		s := &d.Steps[i]
		var spec *InferSpec
		switch s.Kind {
		case KindInfer:
			spec = s.Infer
		case KindLoop:
			spec = &s.Loop.Step
		default:
			continue
		}
		if len(spec.Answer) == 0 {
			continue
		}
		lopts = append(lopts, loom.WithStageSample(s.ID, spec.sample(cfg.ratio)))
		est.Warnings = append(est.Warnings, fmt.Sprintf(
			"%q is priced with its declared answer shape (%s), each field assumed to fill an "+
				"equal share of the step's %d-token cap — which is what the steps below it read",
			s.ID, spec.answerNames(), orInt(spec.MaxTokens, 1024)))
	}

	for i := range d.Steps {
		s := &d.Steps[i]
		if s.Kind != KindSource {
			continue
		}
		recs, ok := cfg.records[s.ID]
		if !ok {
			recs, err = LoadRecords(cfg.ctx, s.Source)
			if err != nil {
				est.Error = err.Error()
				return est
			}
		}
		lopts = append(lopts, loom.WithSourceSample(s.ID, recs))
		if est.Records == 0 {
			est.Records = len(recs)
		}
	}

	proj, err := loom.Explain(p, lopts...)
	if err != nil {
		est.Error = err.Error()
		return est
	}
	projections := []*loom.Projection{proj}

	// The second pass is priced against the first pass's declared output
	// length, because its input does not exist yet: each merged fold
	// contributes one record, and the most that record can hold is the fold's
	// own token cap. That is an upper bound rather than a guess, and it is
	// stated as a warning rather than folded silently into the total.
	if merges := d.Merges(); len(merges) > 0 {
		seed := map[string][]core.Record{}
		for _, m := range merges {
			item := field(m.Reduce.ItemField, "output")
			for _, up := range m.Merge {
				if _, done := seed[up]; done {
					continue
				}
				u := d.Find(up)
				seed[up] = []core.Record{core.NewRecord(up, map[string]any{
					item: placeholder(orInt(u.Reduce.MaxTokens, 1024)),
				})}
			}
		}
		second, err := d.buildSecond(seed, true)
		if err != nil {
			est.Error = err.Error()
			return est
		}
		sproj, err := loom.Explain(second, lopts...)
		if err != nil {
			est.Error = err.Error()
			return est
		}
		sproj.Warnings = append(sproj.Warnings, fmt.Sprintf(
			"the second pass (%s) is priced against the first pass's declared output "+
				"length, since the records it folds do not exist until the first run finishes",
			d.SecondName()))
		projections = append(projections, sproj)
	}

	rows := map[string]*StepPrice{}
	for i := range est.Steps {
		rows[est.Steps[i].ID] = &est.Steps[i]
	}
	seen := map[string]bool{}
	var exp, ceil core.Usage
	var floor time.Duration
	for _, pr := range projections {
		for _, sp := range pr.Stages {
			seen[sp.Stage] = true
			row := rows[sp.Stage]
			if row == nil {
				continue
			}
			row.Model = sp.Model
			row.Records = sp.Records
			row.Calls = sp.Calls
			row.ExpectedUSD = sp.Usage.CostUSD
			row.CeilingUSD = sp.Ceiling.CostUSD
			row.InputTokens = sp.Usage.PromptTokens()
			row.OutputTokens = sp.Usage.OutputTokens
			row.CachedTokens = sp.Usage.CacheReadTokens
			row.Estimated = sp.Estimated
			row.CachePrefix = sp.CachePrefix
			row.Free = sp.Calls == 0
		}
		exp.Add(pr.Expected())
		ceil.Add(pr.Ceiling())
		// Two passes cannot overlap — the second starts when the first
		// finishes — so their rate-limit floors add.
		floor += pr.AdmissionFloor()
		est.Partial = est.Partial || pr.Partial()
		est.Warnings = append(est.Warnings, pr.Warnings...)
	}
	// A step the planner fused into the one after it has no plan row of its
	// own. It is not missing — it is running inside its neighbour's task
	// boundary, which is worth saying rather than leaving as a blank line.
	for i := range est.Steps {
		if est.Steps[i].Kind != KindSource && !seen[est.Steps[i].ID] {
			est.Steps[i].Fused = true
			est.Steps[i].Free = true
		}
	}

	est.ExpectedUSD = exp.CostUSD
	est.CeilingUSD = ceil.CostUSD
	est.InputTokens = exp.PromptTokens()
	est.OutputTokens = exp.OutputTokens
	est.CachedTokens = exp.CacheReadTokens
	est.Calls = exp.Requests
	est.FloorMS = floor.Milliseconds()
	est.FitsCap = d.CapUSD <= 0 || ceil.CostUSD <= d.CapUSD
	if est.ExpectedUSD > 0 {
		for i := range est.Steps {
			est.Steps[i].Share = est.Steps[i].ExpectedUSD / est.ExpectedUSD
		}
	}
	return est
}

// placeholder is a string of roughly n tokens, for pricing a record that does
// not exist yet. Four characters to the token is the same rough conversion the
// planner's own estimator uses.
func placeholder(tokens int) string {
	return strings.Repeat("x", 4*max(1, tokens))
}

// defaultExpectedOutput mirrors loom's assumption about how much of a token cap
// a response actually fills. It is the one number in a projection that is
// assumed rather than computed, which is why it is a named constant in both
// places rather than a literal in either.
const defaultExpectedOutput = 0.35

// sample renders the declared answer shape as the record fields a downstream
// step will see: one placeholder per field, sized so the whole answer adds up
// to the response length this step is priced at.
func (in *InferSpec) sample(ratio float64) map[string]any {
	if ratio <= 0 {
		ratio = defaultExpectedOutput
	}
	expected := max(1, int(ratio*float64(orInt(in.MaxTokens, 1024))))
	share := max(1, expected/len(in.Answer))
	out := make(map[string]any, len(in.Answer))
	for _, a := range in.Answer {
		if a.Kind == "list" {
			items := make([]any, 3)
			for i := range items {
				items[i] = placeholder(max(1, share/3))
			}
			out[a.Name] = items
			continue
		}
		out[a.Name] = placeholder(share)
	}
	return out
}

func (in *InferSpec) answerNames() string {
	names := make([]string, len(in.Answer))
	for i, a := range in.Answer {
		names[i] = a.Name
	}
	return strings.Join(names, ", ")
}

// orInt returns v, or def when v is zero.
func orInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// Step returns one step's row.
func (e *Estimate) Step(id string) *StepPrice {
	for i := range e.Steps {
		if e.Steps[i].ID == id {
			return &e.Steps[i]
		}
	}
	return nil
}

// Priciest returns the step with the largest expected cost, which is the one
// worth changing first. It returns nil when nothing costs anything.
func (e *Estimate) Priciest() *StepPrice {
	var best *StepPrice
	for i := range e.Steps {
		if e.Steps[i].ExpectedUSD > 0 && (best == nil || e.Steps[i].ExpectedUSD > best.ExpectedUSD) {
			best = &e.Steps[i]
		}
	}
	return best
}

// Paid returns the steps that issue model calls, priciest first.
func (e *Estimate) Paid() []StepPrice {
	var out []StepPrice
	for _, s := range e.Steps {
		if s.Calls > 0 {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExpectedUSD > out[j].ExpectedUSD })
	return out
}

// Floor is the shortest time the run can take under provider rate limits.
func (e *Estimate) Floor() time.Duration { return time.Duration(e.FloorMS) * time.Millisecond }

// String renders the estimate as the one-line summary the footer shows.
func (e *Estimate) String() string {
	if e.Error != "" {
		return "cannot price: " + e.Error
	}
	s := fmt.Sprintf("%d steps · %d records · %d calls · priced at $%.2f",
		len(e.Steps), e.Records, e.Calls, e.CeilingUSD)
	if e.CapUSD > 0 {
		s += fmt.Sprintf(" against a $%.2f cap", e.CapUSD)
	}
	if !e.FitsCap {
		s += " · over the cap: the run would stop partway"
	}
	return s
}
