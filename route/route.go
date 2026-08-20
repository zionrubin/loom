// Package route decides where on a stage's escalation ladder a task should
// start.
//
// # The call that was always going to fail
//
// A stage bound to a fast model with a stronger one behind it already has a
// recovery story: the fast model runs, its output fails validation, and the
// scheduler climbs the ladder. That story is correct and it is expensive,
// because it is *reactive*. Every record enters at the bottom rung, so every
// record the fast model cannot handle pays twice — once for the call that was
// always going to fail, and once for the call that answers. A stage with a
// 40% escalation rate spends 1.4 cheap calls per record to buy 1.0 answers,
// and the 0.4 is pure waste that no amount of caching recovers, because a
// failed call has no result to cache.
//
// The waste is structural rather than accidental: the ladder is stateless.
// Record 10,000 enters at the same rung as record 1, having learned nothing
// from the 9,999 verdicts in between.
//
// # The verdicts are already labels
//
// Those verdicts are the interesting part. An InferSpec's Validate function
// is an oracle that has already been written, already runs on every record,
// and already says the exact thing a router needs to know: whether the model
// that ran was strong enough for this input. A semantic failure is a labelled
// negative. A success is a labelled positive. Loom throws both away.
//
// This package keeps them. A Router sees a task before it is dispatched and
// returns the rung it should start on; the scheduler feeds every verdict back
// as an Outcome. Nothing has to be trained, configured or annotated, because
// the training signal is a by-product of work the pipeline was doing anyway.
//
// # What a router may and may not do
//
// A Router chooses a *starting* rung and nothing else. Validation still runs,
// escalation still climbs, the ceiling is still the top of the ladder. Three
// consequences follow, and they are the reason this is safe to turn on:
//
//  1. **A wrong guess costs work, never an answer.** Routing a record too low
//     costs the failed call the flat ladder would have paid anyway. Routing it
//     too high costs the difference between two models' prices. Neither can
//     produce output that would not have passed the same Validate.
//
//  2. **Routing cannot exceed the projection.** Starting at rung k walks rungs
//     k..n, which is a subset of the rungs 0..n a flat ladder walks. So the
//     ceiling loom.Explain reports — and the budget handed to WithRunBudget on
//     the strength of it — bounds a routed run without being recomputed.
//
//  3. **A cold router is today's behaviour.** With no evidence for a bucket,
//     Adaptive returns rung 0 with a reason saying so. Turning routing on can
//     only start saving; it cannot start costing.
//
// # Estimating, and where the estimate is biased
//
// Adaptive keeps one Beta posterior per (stage, bucket, rung) over "the output
// at this rung passed validation", and picks the rung minimising the expected
// cost of *reaching a valid answer*, by backward induction over the ladder:
//
//	E[last] = price(last)
//	E[i]    = price(i) + (1 - p̂(i)) · E[i+1]
//
// which is the whole model. It naturally starts low when the cheap rung works,
// and skips rungs when the cheap rung's failure rate makes its price a toll
// rather than a saving.
//
// One bias is worth stating plainly, because it is not removable. Observations
// at rung i > 0 come mostly from tasks that *escalated* into that rung, and a
// task that escalated is by construction one the rung below could not handle —
// a harder subpopulation than the rung would see if records arrived there
// directly. So p̂ for upper rungs is pessimistic. That biases E upward for the
// upper rungs, which biases the router toward the bottom of the ladder, which
// is toward the behaviour Loom has today. The bias is real and it points the
// safe way.
//
// The bottom rung has no such excuse, and it is the estimate every saving
// rests on: the claim "this record would have failed on the cheap model" is
// exactly what justifies not making the cheap call. Once a bucket is routed
// upward, the bottom rung stops being sampled and the estimate that sent it
// there can never be contradicted. Adaptive therefore keeps a **probe**: a
// deterministic fraction of the tasks it would have routed up are started at
// the bottom anyway. Probes cost real money and they buy the only thing that
// makes the saving a measurement rather than an assertion — an unbiased
// bottom-rung success rate on the population actually being routed. A report
// that says "skipped 412 calls, and the 26 probes say 11% of them would have
// worked" is honest in a way that "skipped 412 calls" is not.
//
// # Determinism
//
// Decisions are a pure function of the profile and the request key. Nothing
// samples from a clock or a global source, so a task asked twice routes twice
// the same way, two workers holding the same profile agree, and a run's report
// says the same thing when it is regenerated. That is why Adaptive minimises
// expected cost under the posterior *mean* and confines its exploration to the
// probe, rather than Thompson-sampling the posterior: sampling would explore
// more smoothly and would make every decision unreproducible.
//
// # Across runs
//
// A Profile is plain serializable data. Loom writes it into the state
// directory beside the result cache, so the calibration a run pays for is
// available to the next one: the second run over similar input starts routed
// rather than starting over. Cache is to results what profile is to
// decisions — the same trick applied to the choice instead of the answer.
package route

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
)

// Rung is one step of a stage's escalation ladder: a model, and what one call
// on it is expected to cost.
type Rung struct {
	Model string
	// PriceUSD is the expected dollar cost of a single call at this rung.
	// Only the ratios between rungs affect a decision, so an estimate good
	// enough to order the ladder is good enough to route on.
	PriceUSD float64
}

// Request is what a router is asked about: one task, before it runs.
type Request struct {
	// Stage is the stage the task belongs to. Buckets are per stage, because
	// "hard" means something different for a classifier than for a summarizer.
	Stage string
	// Key identifies this decision. It seeds the probe draw, so the same key
	// asked twice routes the same way.
	Key string
	// Rungs is the resolved ladder, cheapest first. A ladder shorter than two
	// rungs has nothing to decide.
	Rungs []Rung
	// Records are the task's inputs and EstTokens the planner's size estimate
	// for it. Both are here for featurizers; the default one reads EstTokens.
	Records   []core.Record
	EstTokens int
}

// Decision is where a task should start, and why.
type Decision struct {
	// Rung indexes Request.Rungs.
	Rung int
	// Reason is a short human-readable account of the choice, carried into
	// events and the constellation view. A router that cannot say why it moved
	// a record is not one anybody will leave switched on.
	Reason string
	// Probe reports that this task was started at the bottom rung deliberately,
	// against the router's own estimate, to keep that estimate honest. The
	// scheduler counts probes separately so their cost is visible as the price
	// of measurement rather than hidden in the saving.
	Probe bool
	// Bucket is the feature bucket the decision was made in, for telemetry.
	Bucket string
}

// Outcome is one verdict, fed back after a task's attempt at a rung.
//
// Only semantic verdicts belong here. A transient failure says the network
// was unwell and a permanent one says the code is wrong; neither is evidence
// about whether the model was strong enough, and recording them as failures
// would teach the router to escalate away from a bug.
type Outcome struct {
	Stage  string
	Bucket string
	Rung   int
	// Valid reports whether the output at this rung passed validation.
	Valid bool
	// Start marks a task's *first* verdict — the one at the rung it entered
	// the ladder on, as opposed to one it climbed into.
	//
	// It is what makes a bucket's record count knowable. Verdict counts alone
	// cannot supply it: a record that escalates leaves a verdict at two rungs,
	// and — worse — a bucket the router has moved off the bottom stops leaving
	// bottom-rung verdicts at all, so counting those would shrink exactly the
	// buckets routing is working on. A forecast weighted that way understates
	// the expensive part of a stage by however well the router is doing.
	Start bool
	// Probe marks a verdict from a task the router wanted to move and held
	// back anyway. It is the same evidence as any other verdict for
	// estimation, and it is counted separately because it is the only evidence
	// that can contradict a skip — a saving nobody ever tests is a claim, not
	// a measurement.
	Probe bool
}

// Router decides where a task starts and learns from how it ended.
//
// Implementations must be safe for concurrent use: the scheduler calls Route
// and Observe from every worker goroutine at once.
type Router interface {
	// Route returns the starting rung for a task. It must return a rung in
	// range for the request's ladder; the scheduler clamps regardless.
	Route(Request) Decision
	// Observe records a verdict. It is called once per attempt that produced
	// one — never for cache hits, which cost nothing and prove nothing.
	Observe(Outcome)
}

// Featurizer buckets a request into the class the router estimates over.
//
// A bucket is the unit of generalization: records in one bucket are assumed to
// be alike in how hard they are, so a verdict about one is evidence about the
// rest. Buckets must be cheap and deterministic — this runs before every task,
// on the scheduler's hot path, and a featurizer that called a model to decide
// how hard a record is would cost the thing it is trying to save.
type Featurizer func(Request) string

// SizeBucket is the default featurizer: the task's estimated token count,
// bucketed by powers of two.
//
// Length is a coarse proxy for difficulty and an honest one — a long input is
// more likely to exhaust a small model's ability to hold it — and it is
// available for free on every task the planner builds. It is also usually not
// the best feature for a given pipeline, which is what Config.Features is for:
// a stage classifying support tickets probably knows a `product` or `tier`
// field that predicts difficulty far better than length does.
func SizeBucket(r Request) string {
	if r.EstTokens <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("~2^%d", int(math.Log2(float64(r.EstTokens))))
}

// ByField returns a featurizer bucketing on a record field's value, which is
// the common case for a pipeline that already knows what makes its records
// differ. A task holding several records buckets on its first.
func ByField(field string) Featurizer {
	return func(r Request) string {
		if len(r.Records) == 0 {
			return "empty"
		}
		v, ok := r.Records[0].Data[field]
		if !ok {
			return "unset"
		}
		return fmt.Sprintf("%s=%v", field, v)
	}
}

// DefaultOutputShare is the fraction of a task's estimated token count
// attributed to output when the caller holds only a total.
//
// The scheduler is one such caller: a task carries the planner's single
// EstTokens estimate, sized for admission control, where what a call costs in
// total is the only thing that matters. Splitting it by a constant is enough
// here because a router compares rungs rather than predicts bills, and
// providers price input and output at close to the same ratio across a family
// — so the split moves every rung together and leaves the ordering, and very
// nearly the gaps, where they were.
const DefaultOutputShare = 0.25

// SplitTokens divides a total token estimate into input and output parts at
// DefaultOutputShare.
func SplitTokens(total int) (in, out int) {
	if total <= 0 {
		return 0, 0
	}
	out = int(float64(total) * DefaultOutputShare)
	return total - out, out
}

// PriceLadder prices a ladder of candidate models for a task of the given
// shape, so a router can compare rungs in dollars rather than in position.
//
// The estimate is deliberately crude: input tokens at the model's input rate
// plus the expected output at its output rate. A router needs the ladder
// *ordered by price with the right ratios between the steps*, and both survive
// a rough absolute estimate.
func PriceLadder(candidates []model.Info, inTokens, outTokens int) []Rung {
	rungs := make([]Rung, 0, len(candidates))
	for _, c := range candidates {
		u := core.Usage{InputTokens: inTokens, OutputTokens: outTokens, Requests: 1}
		rungs = append(rungs, Rung{Model: c.ID, PriceUSD: c.Pricing.Cost(u)})
	}
	return rungs
}

// Config tunes an Adaptive router. The zero value is usable and conservative.
type Config struct {
	// Features buckets requests (default SizeBucket).
	Features Featurizer
	// MinSamples is how many bottom-rung verdicts a bucket needs before the
	// router is allowed to move anything in it (default DefaultMinSamples).
	//
	// This is what makes a cold start indistinguishable from today's
	// behaviour. It is not redundant with the posterior: a Beta(1,1) mean of
	// 0.5 after one observation is a number, but it is not evidence, and
	// spending a run's money on it would be routing by coin flip.
	MinSamples int
	// ProbeRate is the fraction of would-be-routed tasks started at the bottom
	// rung anyway, to keep the bottom-rung estimate unbiased (default
	// DefaultProbeRate). Zero uses the default; set Probe to disable probing
	// explicitly.
	ProbeRate float64
	// NoProbe turns probing off. The router then saves slightly more and can
	// no longer measure whether it should have — a trade worth making only for
	// a stage whose difficulty distribution is known not to drift.
	NoProbe bool
	// Prior is the Beta prior on a rung's success rate, as (successes,
	// failures) pseudo-counts (default DefaultPrior). Both must be at least 1;
	// smaller values are raised.
	PriorAlpha, PriorBeta float64
	// Profile seeds the router with calibration from an earlier run. Nil
	// starts cold.
	Profile *Profile
}

// Defaults for Config.
const (
	DefaultMinSamples = 25
	DefaultProbeRate  = 0.05
	DefaultPriorAlpha = 1.0
	DefaultPriorBeta  = 1.0
)

func (c Config) withDefaults() Config {
	if c.Features == nil {
		c.Features = SizeBucket
	}
	if c.MinSamples <= 0 {
		c.MinSamples = DefaultMinSamples
	}
	if c.ProbeRate <= 0 {
		c.ProbeRate = DefaultProbeRate
	}
	if c.NoProbe {
		c.ProbeRate = 0
	}
	if c.PriorAlpha < 1 {
		c.PriorAlpha = DefaultPriorAlpha
	}
	if c.PriorBeta < 1 {
		c.PriorBeta = DefaultPriorBeta
	}
	return c
}

// Adaptive is the default Router: a per-bucket, per-rung estimate of "does
// this rung produce valid output", turned into a starting rung by minimising
// the expected cost of reaching a valid answer.
type Adaptive struct {
	cfg Config

	mu   sync.RWMutex
	prof *Profile
	// learned holds only what *this* router observed, separately from the
	// profile it was seeded with. Persistence appends a contribution rather
	// than rewriting a total, so a run that started from a loaded profile does
	// not write that profile back out and count it twice — and two processes
	// calibrating the same pipeline at once each append their own share.
	learned *Profile
	stats   Stats
}

// Stats counts what a router did, for reports and tests.
type Stats struct {
	// Decisions is every request routed, Moved those started above the bottom
	// rung, and Probes those held at the bottom against the router's estimate.
	Decisions int
	Moved     int
	Probes    int
	// Cold counts decisions declined for want of evidence.
	Cold int
	// Observations is every verdict recorded.
	Observations int
	// ProbeHits counts probes the bottom rung answered after all: the measured
	// rate at which the router's skips would have been wrong.
	ProbeHits int
}

// New returns an Adaptive router.
func New(cfg Config) *Adaptive {
	cfg = cfg.withDefaults()
	prof := cfg.Profile
	if prof == nil {
		prof = NewProfile()
	} else {
		prof = prof.Clone()
	}
	return &Adaptive{cfg: cfg, prof: prof, learned: NewProfile()}
}

// Route implements Router.
func (a *Adaptive) Route(r Request) Decision {
	bucket := a.cfg.Features(r)
	d := Decision{Bucket: bucket}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.Decisions++

	if len(r.Rungs) < 2 {
		d.Reason = "ladder has nothing to choose between"
		return d
	}

	counts := a.prof.stage(r.Stage).bucket(bucket)
	if n := counts.at(0).total(); n < a.cfg.MinSamples {
		a.stats.Cold++
		d.Reason = fmt.Sprintf("%d/%d verdicts at the bottom rung: not enough to move on",
			n, a.cfg.MinSamples)
		return d
	}

	best, costs := a.choose(r.Rungs, counts)
	if best == 0 {
		d.Reason = fmt.Sprintf("%s is the cheapest way to a valid answer (%s)",
			r.Rungs[0].Model, formatCosts(r.Rungs, costs))
		return d
	}

	// The router wants to move this one. A deterministic slice of those are
	// held back at the bottom rung anyway: they are the only evidence that
	// ever contradicts the estimate that moved the rest.
	if a.cfg.ProbeRate > 0 && probe(r.Key, a.cfg.ProbeRate) {
		a.stats.Probes++
		d.Probe = true
		d.Reason = fmt.Sprintf("probe: held at %s to keep the estimate that skips it honest",
			r.Rungs[0].Model)
		return d
	}

	a.stats.Moved++
	d.Rung = best
	d.Reason = fmt.Sprintf("%s answers %.0f%% of this bucket; starting at %s is cheaper (%s)",
		r.Rungs[0].Model, 100*counts.at(0).rate(a.cfg.PriorAlpha, a.cfg.PriorBeta),
		r.Rungs[best].Model, formatCosts(r.Rungs, costs))
	return d
}

// choose returns the rung minimising the expected cost of reaching a valid
// answer, and the expected cost of starting at each rung.
//
// Backward induction over the ladder: starting at the top there is nowhere to
// escalate to, so its expected cost is its price; below that, a rung costs its
// own price plus, with the probability it fails, everything starting one rung
// up would cost.
func (a *Adaptive) choose(rungs []Rung, counts *bucketCounts) (int, []float64) {
	n := len(rungs)
	exp := make([]float64, n)
	exp[n-1] = rungs[n-1].PriceUSD
	for i := n - 2; i >= 0; i-- {
		p := counts.at(i).rate(a.cfg.PriorAlpha, a.cfg.PriorBeta)
		exp[i] = rungs[i].PriceUSD + (1-p)*exp[i+1]
	}
	best := 0
	for i := 1; i < n; i++ {
		if exp[i] < exp[best] {
			best = i
		}
	}
	return best, exp
}

// Observe implements Router.
func (a *Adaptive) Observe(o Outcome) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.Observations++
	if o.Probe && o.Rung == 0 && o.Valid {
		a.stats.ProbeHits++
	}
	a.prof.stage(o.Stage).bucket(o.Bucket).at(o.Rung).record(o.Valid, o.Start)
	a.learned.stage(o.Stage).bucket(o.Bucket).at(o.Rung).record(o.Valid, o.Start)
}

// Stats returns a snapshot of what this router has done.
func (a *Adaptive) Stats() Stats {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.stats
}

// Profile returns a snapshot of everything the router is deciding on — what it
// was seeded with plus what it has since observed — safe to serialize while the
// run continues.
func (a *Adaptive) Profile() *Profile {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.prof.Clone()
}

// Learned returns only the verdicts this router observed, which is what gets
// persisted: a contribution to add to whatever is already on disk, rather than
// a total that would re-count the profile this run was seeded with.
func (a *Adaptive) Learned() *Profile {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.learned.Clone()
}

// probe reports whether key falls in the probe slice. Hashing the key rather
// than drawing from a source keeps the decision reproducible and keeps the
// router free of shared mutable randomness on its hot path.
func probe(key string, rate float64) bool {
	h := fnv.New64a()
	_, _ = h.Write([]byte("probe:" + key))
	const scale = 1 << 20
	return h.Sum64()%scale < uint64(rate*scale)
}

func formatCosts(rungs []Rung, exp []float64) string {
	var b strings.Builder
	for i, r := range rungs {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s $%.5f", r.Model, exp[i])
	}
	return b.String()
}

// Off is a Router that never moves anything: the flat ladder, expressed as a
// router so a caller can turn routing off without a nil check.
type Off struct{}

// Route implements Router.
func (Off) Route(Request) Decision { return Decision{Reason: "routing disabled"} }

// Observe implements Router.
func (Off) Observe(Outcome) {}

// ---------------------------------------------------------------------------
// Profile: the learned calibration, as serializable data.
// ---------------------------------------------------------------------------

// Profile is what a router has learned: for each stage, each feature bucket,
// and each rung of that stage's ladder, how often the rung's output passed
// validation.
//
// It is plain data on purpose. A profile can be written to a state directory,
// read by the next run, shipped to a worker with a task, or diffed between two
// runs to see how a pipeline's difficulty distribution moved.
type Profile struct {
	mu     sync.RWMutex
	stages map[string]*stageCounts
}

// NewProfile returns an empty profile.
func NewProfile() *Profile { return &Profile{stages: map[string]*stageCounts{}} }

type stageCounts struct {
	buckets map[string]*bucketCounts
}

type bucketCounts struct {
	rungs []rungCounts
}

type rungCounts struct {
	Valid   int `json:"valid"`
	Invalid int `json:"invalid"`
	// Starts counts the tasks that *entered* the ladder at this rung, as
	// opposed to climbing into it. Summed across a bucket's rungs it is the
	// bucket's record count, which no combination of Valid and Invalid gives:
	// those double-count a record that escalated and undercount a bucket the
	// router has moved.
	Starts int `json:"starts,omitempty"`
}

func (r rungCounts) total() int { return r.Valid + r.Invalid }

// rate is the posterior mean of the rung's success probability under a
// Beta(α, β) prior — Laplace smoothing, so a rung with three successes and no
// failures is not treated as certain.
func (r rungCounts) rate(alpha, beta float64) float64 {
	return (float64(r.Valid) + alpha) / (float64(r.total()) + alpha + beta)
}

func (p *Profile) stage(name string) *stageCounts {
	if s, ok := p.stages[name]; ok {
		return s
	}
	s := &stageCounts{buckets: map[string]*bucketCounts{}}
	p.stages[name] = s
	return s
}

func (s *stageCounts) bucket(name string) *bucketCounts {
	if b, ok := s.buckets[name]; ok {
		return b
	}
	b := &bucketCounts{}
	s.buckets[name] = b
	return b
}

// at returns the counts for a rung, growing the ladder as needed. A ladder
// that gains a rung between runs is a pipeline edit, not an error: the new
// rung simply starts with no evidence.
func (b *bucketCounts) at(rung int) *rungCounts {
	if rung < 0 {
		rung = 0
	}
	for len(b.rungs) <= rung {
		b.rungs = append(b.rungs, rungCounts{})
	}
	return &b.rungs[rung]
}

func (r *rungCounts) record(valid, start bool) {
	if start {
		r.Starts++
	}
	if valid {
		r.Valid++
		return
	}
	r.Invalid++
}

// records is how many of a bucket's records the profile has seen, or zero when
// nothing recorded a start.
func (b *bucketCounts) records() int {
	var n int
	for _, r := range b.rungs {
		n += r.Starts
	}
	return n
}

// Clone returns an independent copy.
func (p *Profile) Clone() *Profile {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := NewProfile()
	for name, s := range p.stages {
		cs := out.stage(name)
		for bname, b := range s.buckets {
			cb := cs.bucket(bname)
			cb.rungs = append([]rungCounts(nil), b.rungs...)
		}
	}
	return out
}

// Merge folds another profile's counts into this one, which is how the
// calibration of several worker processes — or of an earlier run — becomes
// one. Counts are additive, so merging is associative and order-free: two
// workers that each saw half a stage's records produce the same profile
// whichever way round they are merged.
func (p *Profile) Merge(other *Profile) {
	if other == nil {
		return
	}
	p.mergeSnapshot(other.Snapshot())
}

// Rate returns the observed success rate of a rung in a bucket and how many
// verdicts it rests on. It is what a projection reads and what a test asserts
// on.
func (p *Profile) Rate(stage, bucket string, rung int) (rate float64, samples int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	s, ok := p.stages[stage]
	if !ok {
		return 0, 0
	}
	b, ok := s.buckets[bucket]
	if !ok || rung < 0 || rung >= len(b.rungs) {
		return 0, 0
	}
	c := b.rungs[rung]
	if c.total() == 0 {
		return 0, 0
	}
	return float64(c.Valid) / float64(c.total()), c.total()
}

// Snapshot is the serializable form of a profile: sorted, so two profiles
// holding the same counts marshal to the same bytes.
type Snapshot struct {
	Version int             `json:"version"`
	Stages  []StageSnapshot `json:"stages"`
}

// StageSnapshot is one stage's buckets.
type StageSnapshot struct {
	Stage   string           `json:"stage"`
	Buckets []BucketSnapshot `json:"buckets"`
}

// BucketSnapshot is one feature bucket's per-rung counts.
type BucketSnapshot struct {
	Bucket string       `json:"bucket"`
	Rungs  []rungCounts `json:"rungs"`
}

// SnapshotVersion is the on-disk format version. A profile written by a
// different version is discarded rather than migrated: it is a cache of
// decisions, and the cost of losing one is a run that starts uncalibrated.
const SnapshotVersion = 1

// Snapshot returns the profile as sorted plain data.
func (p *Profile) Snapshot() Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	snap := Snapshot{Version: SnapshotVersion}
	names := make([]string, 0, len(p.stages))
	for name := range p.stages {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		s := p.stages[name]
		ss := StageSnapshot{Stage: name}
		bnames := make([]string, 0, len(s.buckets))
		for b := range s.buckets {
			bnames = append(bnames, b)
		}
		sort.Strings(bnames)
		for _, bname := range bnames {
			ss.Buckets = append(ss.Buckets, BucketSnapshot{
				Bucket: bname,
				Rungs:  append([]rungCounts(nil), s.buckets[bname].rungs...),
			})
		}
		snap.Stages = append(snap.Stages, ss)
	}
	return snap
}

// MarshalJSON implements json.Marshaler.
func (p *Profile) MarshalJSON() ([]byte, error) { return json.Marshal(p.Snapshot()) }

// UnmarshalJSON implements json.Unmarshaler.
func (p *Profile) UnmarshalJSON(b []byte) error {
	var snap Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return err
	}
	if snap.Version != SnapshotVersion {
		return fmt.Errorf("route: profile version %d, want %d", snap.Version, SnapshotVersion)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stages = map[string]*stageCounts{}
	for _, s := range snap.Stages {
		cs := p.stage(s.Stage)
		for _, b := range s.Buckets {
			cb := cs.bucket(b.Bucket)
			cb.rungs = append([]rungCounts(nil), b.Rungs...)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Forecast: what a ladder is expected to cost, before it runs.
// ---------------------------------------------------------------------------

// RungForecast is one rung's share of a stage's expected calls, per record.
//
// Both figures are fractional because they are expectations over records
// rather than counts of them: "0.38 calls at claude-sonnet-5" is the statement
// that 38% of records are expected to reach that rung.
type RungForecast struct {
	Model string
	// FlatCalls is the expected calls at this rung when every record starts at
	// the bottom, which is what Loom does without a router.
	FlatCalls float64
	// RoutedCalls is the expected calls at this rung when each bucket starts
	// where the profile says it should.
	RoutedCalls float64
}

// Forecast is a stage's ladder economics projected from what a router has
// learned: what the escalations cost, and what routing them would save.
//
// It exists so that a projection and a run read the same estimator. The rung a
// forecast assumes a bucket starts on is chosen by the same code that chooses
// it during a run, so "what Explain said this would cost" and "where the
// scheduler actually started each task" cannot drift into disagreeing.
type Forecast struct {
	Rungs []RungForecast
	// Samples is how many verdicts the forecast rests on. Zero means the
	// profile has never seen this stage and the forecast is the flat ladder
	// with no escalation, which is exactly what a projection assumes today.
	Samples int
	// FlatUSD is the expected per-record cost of the ladder with every record
	// starting at the bottom, and RoutedUSD the expected per-record cost with
	// the router choosing. The gap between them is what routing is worth on
	// this stage; the gap between FlatUSD and the bottom rung's price alone is
	// what the escalations cost, which a projection that prices only the base
	// model does not show at all.
	FlatUSD, RoutedUSD float64
	// Buckets is how many feature buckets the stage's records fell into. One
	// bucket means the featurizer is not separating easy records from hard
	// ones, and routing can then only move the whole stage or none of it.
	Buckets int
}

// Forecast projects a stage's ladder from the profile this router holds.
//
// The mix of buckets comes from the profile's own bottom-rung observations
// rather than from the records about to be processed: a projection would
// otherwise have to reconstruct the scheduler's batching to reproduce the
// featurizer's input, and a forecast that rested on a re-derivation of that
// could disagree with the run for reasons that have nothing to do with cost.
// The assumption it makes instead is stated plainly — that the next run's
// records are distributed like the last one's — and it is the same assumption
// every other part of a profile already rests on.
func (a *Adaptive) Forecast(stage string, rungs []Rung) Forecast {
	f := Forecast{Rungs: make([]RungForecast, len(rungs))}
	for i, r := range rungs {
		f.Rungs[i].Model = r.Model
	}
	if len(rungs) == 0 {
		return f
	}

	a.mu.RLock()
	defer a.mu.RUnlock()
	s, ok := a.prof.stages[stage]
	if !ok || len(rungs) < 2 {
		// Nothing learned, or nothing to choose: one call per record at the
		// bottom rung, which is what a projection assumes today.
		f.Rungs[0].FlatCalls, f.Rungs[0].RoutedCalls = 1, 1
		f.FlatUSD, f.RoutedUSD = rungs[0].PriceUSD, rungs[0].PriceUSD
		return f
	}
	f.Buckets = len(s.buckets)

	// Weight each bucket by the records that entered it. Falling back to
	// bottom-rung verdicts when nothing recorded a start keeps a
	// programmatically driven router working, at the cost of the bias Starts
	// exists to remove.
	var weight float64
	var starts int
	for _, b := range s.buckets {
		starts += b.records()
	}
	weights := make(map[string]float64, len(s.buckets))
	for name, b := range s.buckets {
		w := float64(b.records())
		if starts == 0 {
			w = float64(b.at(0).total())
		}
		weights[name], weight = w, weight+w
		f.Samples += b.at(0).total()
	}
	if weight == 0 {
		f.Rungs[0].FlatCalls, f.Rungs[0].RoutedCalls = 1, 1
		f.FlatUSD, f.RoutedUSD = rungs[0].PriceUSD, rungs[0].PriceUSD
		return f
	}

	for name, b := range s.buckets {
		w := weights[name] / weight
		start := 0
		if b.at(0).total() >= a.cfg.MinSamples {
			start, _ = a.choose(rungs, b)
		}
		a.accumulate(&f, rungs, b, w, 0, func(rf *RungForecast, calls float64) {
			rf.FlatCalls += calls
		}, &f.FlatUSD)
		a.accumulate(&f, rungs, b, w, start, func(rf *RungForecast, calls float64) {
			rf.RoutedCalls += calls
		}, &f.RoutedUSD)
	}
	return f
}

// accumulate walks the ladder from start, adding each rung's expected calls
// and cost weighted by the bucket's share of the stage.
//
// A record reaches rung i only by failing every rung below it, so the expected
// calls at each rung are the running product of the failure rates beneath —
// the same chain the decision rule integrates over, walked forwards instead of
// backwards.
func (a *Adaptive) accumulate(f *Forecast, rungs []Rung, b *bucketCounts, share float64,
	start int, add func(*RungForecast, float64), cost *float64) {
	reach := share
	for i := start; i < len(rungs); i++ {
		add(&f.Rungs[i], reach)
		*cost += reach * rungs[i].PriceUSD
		if i == len(rungs)-1 {
			break
		}
		reach *= 1 - b.at(i).rate(a.cfg.PriorAlpha, a.cfg.PriorBeta)
	}
}
