package findings

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
)

// Request is one agent asking one question, carrying the identity the gate
// needs to answer it safely: the envelope's grants and egress allowlist, which
// decide what this reader may be served, and the run, stage and task, which is
// what a serve is recorded against.
type Request struct {
	Question Question
	Grants   security.GrantSet
	Egress   security.EgressPolicy
	RunID    string
	Stage    string
	TaskID   string
}

func (r Request) dependent() Dependent {
	return Dependent{RunID: r.RunID, Stage: r.Stage, TaskID: r.TaskID}
}

// Result is what a public source came back with, on its way into the ledger.
type Result struct {
	// Text is the answer as an agent should read it, and Fields the structured
	// payload a stage can use directly. Covers defaults to the field names.
	Text   string
	Fields map[string]any
	Covers []string

	Sources    []Source
	Confidence float64
	// Cost is what this research cost. Reporting it honestly is what lets every
	// later serve credit a real number as avoided spend rather than a guess,
	// and what makes the gate's own break-even rule a measurement.
	Cost core.Usage
	// Latency is how long the research took. The guard measures it; a caller
	// using the gate directly should set it, because for a tool-backed source
	// it is the entire saving — a search call costs no tokens and seconds of
	// wall clock, and a layer reporting only dollars would score that zero.
	Latency time.Duration

	// NoEvidence marks a searched-and-found-nothing outcome, which is stored
	// and served like any other finding. It is the entry class that saves the
	// most and is left out of most designs.
	NoEvidence bool
	// Ephemeral withholds the result from the ledger. It is for answers that
	// are correct now and misleading later, when that is a property of the
	// answer rather than of the topic — the topic-level version is
	// Volatility Live.
	Ephemeral bool

	// ID revises an existing claim instead of adding a new one.
	ID string
	// Requires and Hosts state what this research consumed, when the caller
	// knows better than the sources say. Empty means derive from Sources.
	Requires []security.Capability
	Hosts    []string
}

// Fetch is the external research the gate stands in front of: whatever an
// executor would have done — an MCP tool call, a web fetch, a model with a
// search tool — expressed as a function of the question.
//
// The gate may call it with a *narrowed* question after a partial hit, which is
// the contract that makes topping up worth anything: an implementation that
// ignores Needs and always does the same full search turns every partial hit
// back into a full miss.
type Fetch func(ctx context.Context, q Question) (Result, error)

// Origin says how an answer was obtained. It is the field that makes the
// layer's claims auditable rather than asserted: a report can say how many
// answers came from where, and a lineage entry can say which one this was.
type Origin string

const (
	OriginExact     Origin = "exact"     // the same question, already answered
	OriginClass     Origin = "class"     // the same subject, different words
	OriginNear      Origin = "near"      // similar question, checked before serving
	OriginToppedUp  Origin = "topped-up" // partial hit, narrowed fetch for the gap
	OriginCoalesced Origin = "coalesced" // another task was already fetching this
	OriginFresh     Origin = "fresh"     // researched here
	OriginBypass    Origin = "bypass"    // a Live topic: never consulted, never stored
)

// Answer is what the gate returns.
type Answer struct {
	Question Question
	Origin   Origin
	Text     string
	Fields   map[string]any
	Sources  []Source

	// Finding and Hash identify what was served. The hash is what a lineage
	// entry should name: it stays resolvable after the claim is revised or
	// retracted, which is the whole reason the ledger never overwrites.
	Finding Finding
	Hash    string

	// Age is how long ago the served finding was learned, and Similarity the
	// score that admitted a near match (0 for exact and class hits).
	Age        time.Duration
	Similarity float64
	// Gap is what the served finding did not cover, when the caller chose to
	// accept a partial answer rather than top it up.
	Gap []string

	// Avoided is the research this answer did not have to pay for; Spent is
	// what it did. Exactly one is non-zero for a given answer.
	Avoided core.Usage
	Spent   core.Usage
	// AvoidedTime is the wall clock the original research took and this answer
	// did not — the saving that shows up when the source costs no tokens.
	AvoidedTime time.Duration
}

// Reused reports whether the answer came from the commons rather than from a
// public source.
func (a Answer) Reused() bool {
	switch a.Origin {
	case OriginExact, OriginClass, OriginNear, OriginCoalesced:
		return true
	}
	return false
}

// Gate is the single entrance to external research: every agent consults it
// before calling out and contributes back to it afterwards.
//
// One gate per fleet, alongside the rate limiter, the governor and the result
// cache, and for the same reason those are shared: what an agent has already
// learned is a property of the work rather than of the pipeline that happened
// to learn it.
type Gate struct {
	Ledger *Ledger
	Policy Policy
	Bus    *observe.Bus

	mu      sync.Mutex
	flights map[string]*flight
	stats   Stats
}

// NewGate builds a gate over a ledger.
func NewGate(l *Ledger, p Policy) *Gate {
	return &Gate{Ledger: l, Policy: p, flights: map[string]*flight{}}
}

// flight is one in-progress piece of research that later askers wait on
// instead of repeating.
//
// This is the mechanism the result cache structurally cannot provide, and on a
// fleet launched all at once it is the one that saves the most. A cache serves
// the second asker only after the first has *finished*; agents started together
// all miss, all call out, and all write the same entry. Regulating the herd
// with a lease is the standard fix — Facebook's memcache work reports peak
// database load dropping from 17k to 1.3k queries per second from exactly this
// — and the version needed here is the small one: one asker researches, the
// rest wait on its result.
//
// A flight carries no answer, only a completion signal and an error. A released
// follower re-consults the ledger rather than taking the leader's return value,
// which costs one more map lookup and buys the property that matters: the
// follower's answer has passed the same sufficiency checks as any other served
// answer, so collapsing two askers together can never hand one of them
// something that does not actually answer its question.
type flight struct {
	done  chan struct{}
	err   error
	owner string
}

// flightKey decides which askers are "the same asker" for deduplication.
//
// The question key is the wrong unit here, and getting it wrong loses most of
// the saving. Two agents asking about one company in their own words have two
// question keys, arrive together, both miss, and both call out — which is
// exactly the case this layer exists for. The right unit is the gate's own
// claim about sameness: the subject, plus what is being asked of it. Needs
// belong in the key because a leader researching revenue must not be treated as
// answering a follower that asked for headcount.
//
// With no facets there is no claim about the subject to make, so the key falls
// back to the question — the same condition, and the same reasoning, as the
// class tier.
func flightKey(q Question) string {
	if len(q.Facets) == 0 {
		return q.Key()
	}
	h, _ := store.Key("findings.flight.v1", q.Class(), q.Needs)
	return h
}

// Research is the gate: consult the commons, and only call out for what the
// commons could not answer.
//
//	ans, err := gate.Research(ctx, req, func(ctx context.Context, q findings.Question) (findings.Result, error) {
//	    return searchTheWeb(ctx, q)   // whatever the executor would have done
//	})
//
// The path through it is a ladder that stops at the first rung that answers:
//
//  1. a Live topic bypasses the gate entirely — declared, counted, never stored;
//  2. exact, class, and near lookup, each candidate checked for sufficiency;
//  3. a partial hit narrows the question to the gap it left;
//  4. a miss takes the single-flight lease, or waits behind whoever holds it;
//  5. what comes back is contributed, so the next asker stops at rung 2.
//
// Everything before rung 4 is map lookups and, at most, one memoized model call
// per (question, finding) pair — so the common path adds microseconds to a task
// that was about to spend a network round trip and a model call.
func (g *Gate) Research(ctx context.Context, req Request, fetch Fetch) (Answer, error) {
	if fetch == nil {
		return Answer{}, fmt.Errorf("findings: Research needs a fetch function")
	}
	start := time.Now()
	q := req.Question.Normalize()
	req.Question = q
	tp := g.Policy.For(q.Topic)

	g.count(func(s *Stats) { s.Asked++ })

	// A Live topic is never consulted and never stored. Having the escape hatch
	// inside the gate rather than beside it is what keeps it visible: the
	// bypass is declared in policy and counted in the stats, instead of being a
	// call site that quietly does not use the commons.
	if tp.Volatility == Live {
		g.charge(start)
		res, err := fetch(ctx, q)
		if err != nil {
			return Answer{}, err
		}
		g.count(func(s *Stats) {
			s.Bypassed++
			s.Spent.Add(res.Cost)
		})
		return answerOf(q, OriginBypass, res), nil
	}

	if ans, ok := g.consult(ctx, req, tp, true); ok {
		g.charge(start)
		g.countOrigin(ans.Origin)
		g.serve(req, ans)
		return ans, nil
	}

	// A partial hit narrows the request rather than repeating it.
	ask, gap := q, []string(nil)
	var partial *Answer
	if p, ok := g.partial(ctx, req, tp); ok {
		partial, gap = &p, p.Gap
		ask = q.Narrow(gap)
	}

	g.charge(start)
	ans, served, err := g.acquire(ctx, req, ask, tp, fetch)
	if err != nil {
		return Answer{}, err
	}
	if served {
		g.countOrigin(ans.Origin)
		g.serve(req, ans)
		return ans, nil
	}
	if partial != nil {
		// A topped-up answer is both halves: what the commons already knew and
		// what the narrowed fetch added. The caller asked one question and
		// should get one answer.
		ans = merge(*partial, ans)
		g.count(func(s *Stats) { s.ToppedUp++ })
	}
	return ans, nil
}

// Lookup consults the commons without falling back to research. It is the
// read-only half of the gate: what a stage calls when it wants to know whether
// a question has already been answered, and what the recall tool exposes to a
// model so it can ask the fleet before it asks the world.
func (g *Gate) Lookup(ctx context.Context, req Request) (Answer, bool) {
	start := time.Now()
	req.Question = req.Question.Normalize()
	tp := g.Policy.For(req.Question.Topic)
	g.count(func(s *Stats) { s.Asked++ })
	if tp.Volatility == Live {
		g.charge(start)
		g.count(func(s *Stats) { s.Bypassed++ })
		return Answer{}, false
	}
	ans, ok := g.consult(ctx, req, tp, true)
	g.charge(start)
	if ok {
		g.countOrigin(ans.Origin)
		g.serve(req, ans)
	}
	return ans, ok
}

// countOrigin records which tier answered. It is the caller's job rather than
// consult's, because a follower released from a single-flight wait re-runs
// consult to get a *checked* answer and must be counted as a coalesced call
// rather than as whichever tier happened to serve it.
func (g *Gate) countOrigin(o Origin) {
	g.count(func(s *Stats) {
		switch o {
		case OriginExact:
			s.Exact++
		case OriginClass:
			s.Class++
		case OriginNear:
			s.Near++
		case OriginCoalesced:
			s.Coalesced++
		}
	})
}

// consult walks the three lookup tiers and returns the first sufficient
// candidate. Tiers are ordered by cost and by certainty at once, which is not a
// coincidence: the cheaper test is the one that proves more.
// tally is false on the re-consults the single-flight path performs — the
// leader's double check and a released follower's lookup — because the caller
// already counted its own pass over the ladder, and a rejection seen twice is
// still one rejection.
func (g *Gate) consult(ctx context.Context, req Request, tp TopicPolicy, tally bool) (Answer, bool) {
	q := req.Question
	seen := map[string]bool{}

	// Tier 1 — exact. One hash and one map lookup: no I/O, no model, and no
	// possibility of a false match, because the key merges two questions only
	// when normalization proved them the same.
	for _, e := range g.Ledger.Exact(q.Key()) {
		seen[e.Hash] = true
		if v := g.admit(ctx, req, tp, e, 0, false, tally); v.Sufficient {
			return g.answerFrom(e, q, OriginExact, 0), true
		}
	}

	// Tier 2 — class. Same topic, same facets: one subject, however worded.
	// Still free, and still certain about the subject; what it is not certain
	// about is whether the finding covers what this caller needs, which is what
	// the coverage check settles.
	//
	// It requires facets, and that condition is the tier's entire warrant. With
	// facets, "same class" says the two questions are about the same thing.
	// Without them it says only "same topic" — which would serve any answer
	// filed under `web-search` to any question asked of it. A tier that is free
	// and certain becomes free and wrong the moment it has no structure to be
	// certain about, so with no facets the ladder skips straight to the tier
	// that checks its candidates.
	class := g.Ledger.Class(q.Class())
	if len(q.Facets) > 0 {
		for _, e := range class {
			if seen[e.Hash] {
				continue
			}
			seen[e.Hash] = true
			if v := g.admit(ctx, req, tp, e, 0, false, tally); v.Sufficient {
				return g.answerFrom(e, q, OriginClass, 0), true
			}
		}
	}

	// Tier 3 — near. Only reached on a miss, only over the class's own
	// entries, and only when an embedder exists. It produces candidates, never
	// hits: every one is checked, and each entry is checked against its own
	// boundary rather than a global constant.
	for _, c := range g.near(ctx, q, class) {
		if v := g.admit(ctx, req, tp, c.entry, c.similarity, true, tally); v.Sufficient {
			return g.answerFrom(c.entry, q, OriginNear, c.similarity), true
		}
	}
	return Answer{}, false
}

// partial returns the best insufficient candidate — the one leaving the
// smallest gap — so the external request can be narrowed to what is missing.
func (g *Gate) partial(ctx context.Context, req Request, tp TopicPolicy) (Answer, bool) {
	q := req.Question
	// Topping up needs both halves: something to be missing, and grounds to
	// believe the partial answer is about the same subject. Facets are the
	// grounds, for the reason the class tier states.
	if len(q.Needs) == 0 || len(q.Facets) == 0 {
		return Answer{}, false
	}
	var best *Entry
	var bestGap []string
	for _, e := range g.Ledger.Class(q.Class()) {
		if ok, _ := Reachable(e.Finding, req.Grants, req.Egress); !ok {
			continue
		}
		if !g.fresh(tp, e) || !g.visible(tp, req, e) {
			continue
		}
		gap := e.Finding.Gap(q.Needs)
		if len(gap) == 0 || len(gap) == len(q.Needs) {
			continue // fully sufficient (handled above) or covers nothing
		}
		if best == nil || len(gap) < len(bestGap) {
			best, bestGap = e, gap
		}
	}
	if best == nil {
		return Answer{}, false
	}
	ans := g.answerFrom(best, q, OriginToppedUp, 0)
	ans.Gap = bestGap
	return ans, true
}

// admit runs the sufficiency ladder over one candidate: reachability, then
// visibility, then freshness, then coverage, then corroboration and confidence,
// and only then — if the topic asks for it and the economics allow — a model.
//
// The order is the design. Every rung before the last is a comparison over data
// the ledger already holds, so the expensive rung is reached only for the
// candidates that survived every cheap one, and its verdicts are memoized so no
// pairing is ever judged twice.
func (g *Gate) admit(ctx context.Context, req Request, tp TopicPolicy,
	e *Entry, similarity float64, allowJudge, tally bool) Verdict {

	f := e.Finding

	// Capability containment comes first because its failure is not a cache
	// miss but a denial: this reader may not be told what that research found.
	if ok, why := Reachable(f, req.Grants, req.Egress); !ok {
		if tally {
			g.count(func(s *Stats) { s.Denied++ })
		}
		return insufficient(why)
	}
	if !g.visible(tp, req, e) {
		return insufficient("private to the agent that learned it")
	}
	if !g.fresh(tp, e) {
		if tally {
			g.count(func(s *Stats) { s.Stale++ })
		}
		return insufficient("stale")
	}
	if gap := f.Gap(req.Question.Needs); len(gap) > 0 {
		return insufficient("does not cover "+strings.Join(gap, ", "), gap...)
	}
	if support := g.Ledger.Support(e); support < tp.MinSources {
		return insufficient(fmt.Sprintf("support %d < %d", support, tp.MinSources))
	}
	if tp.MinConfidence > 0 && f.Confidence < tp.MinConfidence {
		return insufficient(fmt.Sprintf("confidence %.2f < %.2f", f.Confidence, tp.MinConfidence))
	}
	if !allowJudge || !tp.Adjudicate || g.Policy.Judge == nil {
		return sufficient()
	}

	qKey := req.Question.Key()
	if ok, memoized := g.Ledger.Verdict(qKey, e.Hash); memoized {
		if ok {
			return sufficient()
		}
		return insufficient("adjudicated: does not answer the question")
	}
	// The break-even rule: the gate may not spend more looking than the lookup
	// could save. The ledger knows what this topic's research actually costs,
	// so a topic whose answers are cheap to fetch is one the gate declines to
	// think hard about — and declining means serving the structural verdict it
	// already reached, not rejecting the candidate.
	if mean := g.Ledger.MeanCost(f.Topic); mean < g.Policy.JudgeCostUSD*g.Policy.breakEven() {
		return sufficient()
	}
	ok, err := g.Policy.Judge(ctx, req.Question, f)
	if tally {
		g.count(func(s *Stats) { s.Judged++ })
	}
	if err != nil {
		// A judge that fails is not evidence about the finding. Fall back to
		// the structural verdict rather than inventing one.
		return sufficient()
	}
	g.Ledger.RecordVerdict(qKey, e, similarity, ok)
	if ok {
		return sufficient()
	}
	return insufficient("adjudicated: does not answer the question")
}

func (g *Gate) fresh(tp TopicPolicy, e *Entry) bool {
	ttl := tp.TTL
	if ttl <= 0 {
		return true // static: no expiry
	}
	return e.Age(g.Policy.now()) <= ttl
}

func (g *Gate) visible(tp TopicPolicy, req Request, e *Entry) bool {
	return tp.Scope != ScopePrivate || e.Learner == "" || e.Learner == req.RunID
}

// --- The near tier ------------------------------------------------------

type candidate struct {
	entry      *Entry
	similarity float64
}

// near scores a class's entries against the question and returns those over
// their own thresholds, best first.
func (g *Gate) near(ctx context.Context, q Question, class []*Entry) []candidate {
	if g.Policy.Embedder == nil || len(class) == 0 {
		return nil
	}
	withVectors := make([]*Entry, 0, len(class))
	for _, e := range class {
		if len(e.Vector) > 0 {
			withVectors = append(withVectors, e)
		}
	}
	if len(withVectors) == 0 {
		return nil
	}
	vecs, err := g.Policy.Embedder.Embed(ctx, []string{q.Text})
	if err != nil || len(vecs) == 0 {
		return nil
	}
	out := make([]candidate, 0, len(withVectors))
	for _, e := range withVectors {
		sim := cosine(vecs[0], e.Vector)
		if sim >= g.Ledger.Threshold(e) {
			out = append(out, candidate{entry: e, similarity: sim})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].similarity > out[j].similarity })
	return out
}

// cosine is the similarity between two vectors, 0 when either is empty or
// degenerate.
func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// --- Single flight ------------------------------------------------------

// fetchOnce performs the research, or waits for whoever is already performing
// it. It returns either a fresh Result or the answer the leader produced.
// acquire performs the research or joins whoever is already performing it. It
// reports whether the answer came from the commons (served) or from the source.
func (g *Gate) acquire(ctx context.Context, req Request, ask Question,
	tp TopicPolicy, fetch Fetch) (Answer, bool, error) {

	key := flightKey(ask)

	g.mu.Lock()
	fl, busy := g.flights[key]
	if !busy {
		fl = &flight{done: make(chan struct{}), owner: req.TaskID}
		g.flights[key] = fl
	}
	g.mu.Unlock()

	if busy {
		ans, err, research := g.wait(ctx, fl, req, tp)
		if err != nil {
			return Answer{}, false, err
		}
		if !research {
			return *ans, true, nil
		}
		// The leader answered a question that turned out not to be ours, or was
		// slower than the wait bound allows. Research it without taking a lease:
		// we already know this question is not being shared, and a second lease
		// on the same key would put the next follower behind us for nothing.
		return g.lead(ctx, req, ask, "", nil, fetch)
	}

	// Consulting again now that the lease is held closes the check-then-act
	// window between this asker's own lookup and this moment. Without it a fleet
	// launched together loses deduplication in a way that looks like noise: two
	// askers miss, one takes the lease and finishes, and the other arrives at
	// the flight after it was retired — so it leads a flight of its own and buys
	// research the ledger was already holding. One extra map lookup on the miss
	// path is a cheap price for a saving otherwise lost to a race rather than to
	// a decision.
	recheck := time.Now()
	ans, ok := g.consult(ctx, req, tp, false)
	g.charge(recheck) // the double check is gate overhead and is reported as such
	if ok {
		g.retire(key, fl, nil)
		return ans, true, nil
	}
	return g.lead(ctx, req, ask, key, fl, fetch)
}

// lead researches a question and contributes what comes back. When it holds a
// flight, the flight is retired only *after* the contribution lands — a
// follower released before the ledger holds the answer would look, find
// nothing, and research it again, which is precisely the duplication the lease
// exists to prevent.
func (g *Gate) lead(ctx context.Context, req Request, ask Question,
	key string, fl *flight, fetch Fetch) (Answer, bool, error) {

	res, err := fetch(ctx, ask)
	if err != nil {
		if fl != nil {
			g.retire(key, fl, err)
		}
		return Answer{}, false, err
	}
	// A contribution that fails to land is not a research failure. The answer
	// in hand is correct and paid for; all that is lost is the next agent's
	// chance to reuse it, which is counted as Unrecorded rather than raised.
	// The result cache makes the same call for the same reason — a cache that
	// can fail the work it was meant to accelerate is worse than no cache.
	ans, err := g.Contribute(ctx, req, ask, res)
	if err != nil {
		g.count(func(s *Stats) { s.Unrecorded++ })
	}
	if fl != nil {
		g.retire(key, fl, nil)
	}
	return ans, false, nil
}

// retire removes a flight and releases everyone waiting on it. The flight is
// removed from the map before it is signalled, so the next asker starts a fresh
// one rather than joining a finished one.
func (g *Gate) retire(key string, fl *flight, err error) {
	fl.err = err
	g.mu.Lock()
	delete(g.flights, key)
	g.mu.Unlock()
	close(fl.done)
}

// wait blocks a follower on the leader's flight, bounded three ways — the
// leader finishing, the caller's context, and the policy's wait ceiling — and
// reports whether the follower must go and research the question itself.
func (g *Gate) wait(ctx context.Context, fl *flight, req Request, tp TopicPolicy) (*Answer, error, bool) {
	timer := time.NewTimer(g.Policy.maxWait())
	defer timer.Stop()

	select {
	case <-fl.done:
		if fl.err != nil {
			// The leader's failure is the follower's too. Releasing the herd
			// onto a source that just failed is the behaviour the lease exists
			// to prevent, so a failed flight fails everyone waiting on it and
			// the scheduler's own class-aware retry decides what happens next.
			return nil, fl.err, false
		}
		// The leader has contributed by now, so the ordinary lookup ladder is
		// what serves the follower — checked for reachability, freshness and
		// coverage like any other hit. A leader whose answer does not in fact
		// cover the follower's question therefore sends the follower to the
		// source rather than fobbing it off, which is what makes collapsing
		// same-subject askers safe rather than merely cheap.
		ans, ok := g.consult(ctx, req, tp, false)
		if !ok {
			return nil, nil, true
		}
		ans.Origin = OriginCoalesced
		return &ans, nil, false
	case <-ctx.Done():
		return nil, ctx.Err(), false
	case <-timer.C:
		// The leader is slower than the wait bound allows. Correctness is not at
		// stake — research it directly — but the deduplication is lost, and a
		// fleet seeing this often wants a longer bound or a faster source, so it
		// is counted rather than hidden.
		g.count(func(s *Stats) { s.Overtaken++ })
		return nil, nil, true
	}
}

// --- Contribution -------------------------------------------------------

// Contribute records what a public source returned, so the next agent to ask
// stops at the ledger. It returns the answer the caller should use.
//
// Contribution is where the layer's cost accounting is established: the
// research cost recorded here is what every later serve credits as avoided
// spend, which is why Result.Cost matters even though nothing enforces it.
func (g *Gate) Contribute(ctx context.Context, req Request, asked Question, res Result) (Answer, error) {
	asked = asked.Normalize()
	tp := g.Policy.For(asked.Topic)

	g.count(func(s *Stats) {
		s.Fresh++
		s.Spent.Add(res.Cost)
	})
	ans := answerOf(asked, OriginFresh, res)

	if res.Ephemeral || tp.Volatility == Live {
		return ans, nil
	}

	f := Finding{
		ID:         res.ID,
		Topic:      asked.Topic,
		Asked:      asked,
		Answer:     res.Text,
		Fields:     res.Fields,
		Covers:     res.Covers,
		Sources:    res.Sources,
		Confidence: res.Confidence,
		NoEvidence: res.NoEvidence,
		Requires:   res.Requires,
		Hosts:      res.Hosts,
		Cost:       res.Cost,
		Volatility: tp.Volatility,
	}
	// A negative result covers exactly what was asked of it: that is what makes
	// it reusable. Without this, "we looked and there is nothing" answers no
	// question and every agent looks again.
	if f.NoEvidence && len(f.Covers) == 0 {
		f.Covers = asked.Needs
	}
	if len(f.Requires) == 0 || len(f.Hosts) == 0 {
		reqs, hosts := provenanceOf(res.Sources)
		if len(f.Requires) == 0 {
			f.Requires = reqs
		}
		if len(f.Hosts) == 0 {
			f.Hosts = hosts
		}
	}

	var vec []float32
	if g.Policy.Embedder != nil {
		if vecs, err := g.Policy.Embedder.Embed(ctx, []string{asked.Text}); err == nil && len(vecs) > 0 {
			vec = vecs[0]
		}
	}

	e, err := g.Ledger.Append(Entry{
		Finding: f, Key: asked.Key(), Class: asked.Class(),
		Learned: g.Policy.now(), Learner: req.RunID,
		Latency: res.Latency, Threshold: tp.Near, Vector: vec,
	})
	if err != nil {
		return ans, err
	}
	ans.Finding, ans.Hash = e.Finding, e.Hash
	g.publish(req, ans, observe.FindingLearned)
	return ans, nil
}

// provenanceOf derives the capabilities and hosts a set of sources implies, so
// a finding carries its own containment without the caller restating it.
func provenanceOf(sources []Source) ([]security.Capability, []string) {
	var caps []security.Capability
	var hosts []string
	seenCap := map[security.Capability]bool{}
	seenHost := map[string]bool{}
	for _, s := range sources {
		if s.Tool != "" {
			c := security.ToolCap(s.Tool)
			if !seenCap[c] {
				seenCap[c] = true
				caps = append(caps, c)
			}
		}
		if s.Host != "" && !seenHost[s.Host] {
			seenHost[s.Host] = true
			hosts = append(hosts, s.Host)
		}
	}
	return caps, hosts
}

// --- Answers and accounting ---------------------------------------------

func answerOf(q Question, origin Origin, res Result) Answer {
	return Answer{
		Question: q, Origin: origin,
		Text: res.Text, Fields: res.Fields, Sources: res.Sources,
		Spent: res.Cost,
	}
}

// answerFrom builds the answer a served finding produces, crediting what the
// original research cost as spend this caller avoided.
func (g *Gate) answerFrom(e *Entry, q Question, origin Origin, similarity float64) Answer {
	f := e.Finding
	return Answer{
		Question: q, Origin: origin,
		Text: f.Answer, Fields: f.Fields, Sources: f.Sources,
		Finding: f, Hash: e.Hash,
		Age:         e.Age(g.Policy.now()),
		Similarity:  similarity,
		Avoided:     f.Cost,
		AvoidedTime: e.Latency,
	}
}

// merge folds a topped-up answer into the partial one it extended.
func merge(partial, fresh Answer) Answer {
	out := fresh
	out.Origin = OriginToppedUp
	if partial.Text != "" && fresh.Text != "" {
		out.Text = partial.Text + "\n\n" + fresh.Text
	} else if out.Text == "" {
		out.Text = partial.Text
	}
	out.Fields = map[string]any{}
	for k, v := range partial.Fields {
		out.Fields[k] = v
	}
	for k, v := range fresh.Fields {
		out.Fields[k] = v
	}
	out.Sources = append(append([]Source(nil), partial.Sources...), fresh.Sources...)
	out.Avoided = partial.Avoided
	out.Gap = nil
	return out
}

// serve records a hit: the justification edge for retraction, the event, and
// the avoided spend.
func (g *Gate) serve(req Request, ans Answer) {
	g.Ledger.Cite(ans.Hash, req.dependent())
	g.count(func(s *Stats) {
		s.Avoided.Add(ans.Avoided)
		s.AvoidedTime += ans.AvoidedTime
	})
	kind := observe.FindingServed
	if ans.Origin == OriginCoalesced {
		kind = observe.FindingCoalesced
	}
	g.publish(req, ans, kind)
}

func (g *Gate) publish(req Request, ans Answer, kind observe.EventType) {
	if g.Bus == nil {
		return
	}
	e := observe.Event{
		Type: kind, RunID: req.RunID, Stage: req.Stage, TaskID: req.TaskID,
		Topic: ans.Question.Topic, Detail: observe.Clip(ans.Question.String()),
		Artifact: ans.Hash, Note: string(ans.Origin),
	}
	switch kind {
	case observe.FindingLearned:
		e.Usage = ans.Spent
	default:
		e.Usage = ans.Avoided
		e.Saved = ans.Avoided.CostUSD
		e.Latency = ans.Age
	}
	g.Bus.Publish(e)
}

// --- Stats --------------------------------------------------------------

// Stats is what the layer did, and it is deliberately the shape of an argument
// rather than a dashboard: every reuse class is counted separately, the two
// kinds of refusal are visible, and the gate's own overhead is measured next to
// the spend it avoided — because "reduces duplicated work without adding
// meaningful latency" is a claim, and a claim needs both numbers.
type Stats struct {
	Asked int

	Exact     int
	Class     int
	Near      int
	Coalesced int
	ToppedUp  int

	Fresh    int // researched here
	Bypassed int // Live topics, never consulted

	Denied     int // capability containment refused a reader
	Stale      int // a candidate was past its topic's horizon
	Judged     int // adjudications actually paid for
	Overtaken  int // a follower gave up waiting and researched it itself
	Unrecorded int // research succeeded but the ledger would not take it

	// Avoided is research this layer did not have to buy; Spent is what it did.
	// AvoidedTime is the wall clock the reused research originally took, which
	// is the whole saving when a source costs no tokens.
	Avoided     core.Usage
	Spent       core.Usage
	AvoidedTime time.Duration

	// Overhead is the wall-clock time spent inside the gate, excluding the
	// research it delegates. It is the number that decides whether the layer
	// pays for itself.
	Overhead time.Duration
}

// Reused is the number of answers served from the commons.
func (s Stats) Reused() int { return s.Exact + s.Class + s.Near + s.Coalesced }

// HitRate is the share of questions answered without new external research.
func (s Stats) HitRate() float64 {
	if s.Asked == 0 {
		return 0
	}
	return float64(s.Reused()) / float64(s.Asked)
}

// Overshoot is the average gate overhead per question asked.
func (s Stats) Overshoot() time.Duration {
	if s.Asked == 0 {
		return 0
	}
	return s.Overhead / time.Duration(s.Asked)
}

// Stats returns a snapshot of what the gate has done.
func (g *Gate) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

func (g *Gate) count(fn func(*Stats)) {
	g.mu.Lock()
	fn(&g.stats)
	g.mu.Unlock()
}

// charge folds the time spent inside the gate into the overhead total. It is
// called before any external research begins, so the number is the layer's own
// cost and not the cost of what it stands in front of.
func (g *Gate) charge(start time.Time) {
	d := time.Since(start)
	g.mu.Lock()
	g.stats.Overhead += d
	g.mu.Unlock()
}

// String renders the stats as the two lines a report wants: what was reused,
// and what it cost to find that out.
func (s Stats) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "findings  %d asked · %d reused (%.0f%%) · %d researched",
		s.Asked, s.Reused(), 100*s.HitRate(), s.Fresh)
	if s.Bypassed > 0 {
		fmt.Fprintf(&b, " · %d live", s.Bypassed)
	}
	b.WriteByte('\n')
	fmt.Fprintf(&b, "  exact %d · class %d · near %d · coalesced %d · topped-up %d",
		s.Exact, s.Class, s.Near, s.Coalesced, s.ToppedUp)
	if s.Stale > 0 {
		fmt.Fprintf(&b, " · %d stale", s.Stale)
	}
	if s.Denied > 0 {
		fmt.Fprintf(&b, " · %d denied", s.Denied)
	}
	if s.Judged > 0 {
		fmt.Fprintf(&b, " · %d judged", s.Judged)
	}
	if s.Overtaken > 0 {
		fmt.Fprintf(&b, " · %d overtaken", s.Overtaken)
	}
	if s.Unrecorded > 0 {
		fmt.Fprintf(&b, " · %d unrecorded", s.Unrecorded)
	}
	b.WriteByte('\n')
	fmt.Fprintf(&b, "  avoided $%.4f and %s of research, spent $%.4f\n",
		s.Avoided.CostUSD, s.AvoidedTime.Round(time.Millisecond), s.Spent.CostUSD)
	fmt.Fprintf(&b, "  gate overhead %s total, %s per question\n",
		s.Overhead.Round(time.Microsecond), s.Overshoot().Round(time.Microsecond))
	return b.String()
}
