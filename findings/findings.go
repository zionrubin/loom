// Package findings is Loom's shared research layer: one gate that every agent
// passes through before it reaches a public source, and contributes back to
// afterwards.
//
// # The problem it exists for
//
// A fleet's agents already share a result cache, and that cache already stops
// identical work from being paid for twice. It cannot stop *this* work, because
// its key is the bytes going in. Two agents researching the same company phrase
// the question differently, arrive at the gate a millisecond apart, and hash to
// two different keys — so both call out, both pay, and the fleet learns one
// thing twice. The duplication that costs the most is exactly the duplication a
// content-addressed cache is blind to:
//
//   - **Different words, one question.** "2024 revenue for Northwind" and
//     "what did Northwind earn last year" are one lookup and two cache keys.
//   - **Same instant, no prior.** A cold key with fifty concurrent askers is
//     fifty external calls, because nobody has written the entry that would
//     have served the other forty-nine.
//   - **Enough, not identical.** An agent needing a founding date is served by
//     a finding gathered for a different purpose that happens to carry one.
//
// So the unit of sharing here is not the task's input but the **question**, and
// the unit of reuse is not the task's output but the **finding** — a sourced,
// durable answer about the world that outlives the agent that learned it.
//
// # What makes shared research safe in front of a content-addressed cache
//
// Loom's existing sharing primitives are deliberately narrow. A broadcast is
// read-only for the run's whole life; a blackboard topic is append-only and
// read only at agent boundaries. Both restrictions exist because shared mutable
// state would make a cached result depend on execution order. A findings ledger
// is written *inside* a task — that is the whole point of it — so it has to earn
// the same safety a different way. Four properties do it:
//
//  1. **A hit is substitutable for the call it replaces.** The gate serves a
//     finding only when it would answer the question the caller was about to
//     ask a public source. Whether a task hit the ledger or called out is
//     therefore not observable in its output, which is the same claim the
//     result cache makes and the reason a cache hit is not a semantic event.
//     What *is* observable is cost: the ledger makes spend order-dependent and
//     answers order-independent. That is the trade, stated plainly, and it is
//     the right way round.
//  2. **Entries are append-only and content-addressed.** A contribution never
//     mutates a finding another task is holding; it appends a revision under a
//     new hash and leaves the old one resolvable. A retraction marks a head
//     dead without removing the bytes any lineage entry already names.
//  3. **The knowledge hash excludes the clock.** A finding's semantic body —
//     the claim, its fields, its warrant — hashes without wall-clock time,
//     which lives on the ledger entry around it. Two agents that independently
//     learn the same thing converge on one hash, so the second is recorded as
//     corroboration rather than as a rival claim. This is the same rule
//     Fleet.Post follows in carrying no timestamp, applied one level down.
//  4. **The ledger may save you a call you were allowed to make; it may never
//     make a call you were not.** Every finding records the capabilities and
//     hosts its research consumed, and a reader is served only if its own
//     envelope would have let it do that research itself. Without this a shared
//     cache is a capability-laundering channel: the cheapest way around an
//     egress allowlist is to wait for someone who has it.
//
// # The three tiers, and why they are ordered this way
//
// Lookup is a ladder, cheapest and most decisive first, and it stops at the
// first tier that answers:
//
//	exact   canonical question key → O(1) map hit, no I/O, no model
//	class   same topic and same facets → a small candidate set
//	near    embedding similarity within the class → candidates, never hits
//
// The exact key is deliberately conservative: it merges two questions only when
// it can prove them identical after whitespace and case normalization. Anything
// cleverer belongs in the near tier, where a candidate is *checked* before it is
// served, rather than in the exact key, where it cannot be.
//
// Promotion from candidate to hit is the sufficiency check (see Verdict), and
// it is also a ladder: reachability, then liveness, then freshness, then
// coverage, then corroboration, and only then — if the topic asks for it and
// the economics justify it — an adjudicating model call whose verdict is itself
// memoized, so no question and finding are ever judged twice.
//
// # Reading order
//
// findings.go   the vocabulary: questions, findings, policy, sufficiency
// ledger.go     the store: append-only entries, three indices, retraction
// gate.go       the gate: the lookup ladder, single-flight, contribution
// guard.go      the seam: any tool that reaches a public source, gated
package findings

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
)

// --- Questions ----------------------------------------------------------

// Question is what an agent wants to know, in the form the gate can reason
// about: a topic that says what kind of question it is, the text as a model
// would ask it, the structured facets that pin its subject, and the fields the
// caller actually needs an answer to cover.
//
// Facets and Needs are what let the gate decide anything without a model. Two
// questions with the same facets are about the same subject whatever words they
// use, and a finding that covers every Need is sufficient whatever else it
// omits. A Question with neither degrades gracefully — it can still hit exactly
// and, with an Embedder, approximately — but it gives up the tier that is both
// free and certain.
type Question struct {
	// Topic names the kind of question, and is the unit policy is declared on:
	// how volatile its answers are, how much corroboration they need, whether
	// they may be shared beyond the agent that learned them.
	Topic string `json:"topic"`
	// Text is the question as asked. It is what the near tier embeds and what
	// an adjudicator reads; it is not, by itself, evidence that two questions
	// differ.
	Text string `json:"text"`
	// Facets pin the subject: {"company": "northwind", "year": "2024"}. Equal
	// facets under one topic mean one subject, which is the cheapest true
	// statement the gate can make about two differently-worded questions.
	Facets map[string]string `json:"facets,omitempty"`
	// Needs lists the fields an answer must carry for this caller. It is the
	// difference between "is there a finding" and "is there a *sufficient*
	// finding", and it is what makes a partial hit useful: the gap becomes the
	// narrowed question sent to the public source.
	Needs []string `json:"needs,omitempty"`
}

// Normalize returns the question in canonical form: topic and facet keys and
// values lowercased and trimmed, text lowercased with internal whitespace
// collapsed and trailing punctuation dropped, needs deduplicated and sorted.
//
// The normalization is deliberately shallow. Every transformation here is one
// that cannot change what is being asked; stemming, stop-word removal and
// paraphrase folding all can, so they belong in the near tier where a candidate
// is checked before it is served rather than in the exact key where a false
// merge is served silently.
func (q Question) Normalize() Question {
	n := Question{
		Topic: strings.ToLower(strings.TrimSpace(q.Topic)),
		Text:  normalizeText(q.Text),
	}
	if len(q.Facets) > 0 {
		n.Facets = make(map[string]string, len(q.Facets))
		for k, v := range q.Facets {
			key := strings.ToLower(strings.TrimSpace(k))
			if key == "" {
				continue
			}
			n.Facets[key] = strings.ToLower(strings.TrimSpace(v))
		}
	}
	if len(q.Needs) > 0 {
		seen := make(map[string]struct{}, len(q.Needs))
		for _, need := range q.Needs {
			need = strings.ToLower(strings.TrimSpace(need))
			if need == "" {
				continue
			}
			if _, dup := seen[need]; dup {
				continue
			}
			seen[need] = struct{}{}
			n.Needs = append(n.Needs, need)
		}
		sort.Strings(n.Needs)
	}
	return n
}

func normalizeText(s string) string {
	s = strings.ToLower(strings.Join(strings.Fields(s), " "))
	return strings.TrimRight(s, " ?.!:;,")
}

// Key is the exact-tier identity of a question: topic, canonical text, and
// facets. Needs are excluded on purpose — two callers asking the same question
// and wanting different fields out of it are asking the same question, and
// folding Needs in would split the ledger by caller instead of by subject.
func (q Question) Key() string {
	n := q.Normalize()
	h, _ := store.Key("findings.question.v1", n.Topic, n.Text, sortedPairs(n.Facets))
	return h
}

// Class is the subject identity: topic and facets, without the text. It is the
// second tier's index, and the reason two agents that phrase a question
// differently still land in one candidate set — provided they agree on what
// they are asking about, which is what facets are for.
func (q Question) Class() string {
	n := q.Normalize()
	h, _ := store.Key("findings.class.v1", n.Topic, sortedPairs(n.Facets))
	return h
}

// Narrow returns the question restricted to the fields a finding did not
// cover. It is what the gate sends to the public source after a partial hit:
// the point of a partial hit is to shrink the external request, not to justify
// making the same one over again.
func (q Question) Narrow(gap []string) Question {
	out := q
	out.Needs = append([]string(nil), gap...)
	if len(gap) > 0 {
		out.Text = strings.TrimSpace(q.Text) + " (only: " + strings.Join(gap, ", ") + ")"
	}
	return out
}

// String renders the question for logs and events.
func (q Question) String() string {
	if len(q.Facets) == 0 {
		return q.Topic + ": " + q.Text
	}
	pairs := sortedPairs(q.Facets)
	parts := make([]string, 0, len(pairs))
	for _, p := range pairs {
		parts = append(parts, p[0]+"="+p[1])
	}
	return q.Topic + "[" + strings.Join(parts, " ") + "]: " + q.Text
}

func sortedPairs(m map[string]string) [][2]string {
	out := make([][2]string, 0, len(m))
	for k, v := range m {
		out = append(out, [2]string{k, v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// --- Findings -----------------------------------------------------------

// Source is one warrant for a finding: the tool that was called, the host it
// reached, the identifier it returned, and the content hash of what it said.
//
// The digest is what makes a source checkable rather than decorative. A finding
// whose source document has changed underneath it is a finding whose warrant
// has moved, which is one of the three ways a finding is invalidated.
type Source struct {
	Tool   string `json:"tool,omitempty"`
	Host   string `json:"host,omitempty"`
	URI    string `json:"uri,omitempty"`
	Digest string `json:"digest,omitempty"`
}

// Finding is one durable, sourced answer about the world.
//
// The distinction that decides what belongs here is whether the answer is about
// the *world* or about the *record*. "Northwind was founded in 1996" is a
// finding: any agent asking that question wants that answer, and asking again
// costs money to learn the same thing. "This support ticket is angry" is not:
// it is a judgment about one record, it is what the result cache already keys
// on, and putting it here would fill the commons with entries nobody can reuse.
//
// A finding is immutable. Correcting one appends a revision that names the
// hash it supersedes; withdrawing one marks the head retracted. Neither removes
// bytes, because lineage entries elsewhere already name them.
type Finding struct {
	// ID is stable across revisions: revisions of one claim share it, so a
	// retraction reaches every version and a reader can ask what the current
	// head is.
	ID  string `json:"id"`
	Rev int    `json:"rev"`

	Topic string   `json:"topic"`
	Asked Question `json:"asked"`

	// Answer is the prose an agent is served in place of doing the research,
	// and Fields the structured payload a stage can read directly.
	Answer string         `json:"answer"`
	Fields map[string]any `json:"fields,omitempty"`
	// Covers names what this finding answers. It defaults to the keys of
	// Fields, which is usually right and always checkable; state it explicitly
	// when the prose covers something the fields do not.
	Covers []string `json:"covers,omitempty"`

	Sources    []Source `json:"sources,omitempty"`
	Confidence float64  `json:"confidence,omitempty"`

	// NoEvidence marks a searched-and-found-nothing result. Storing these is
	// the single highest-value entry class in the ledger and the one most
	// often left out: without them, a question with no answer is re-researched
	// by every agent that asks it, forever, at full price. DNS has cached
	// negative answers since RFC 2308 for the same reason.
	NoEvidence bool `json:"no_evidence,omitempty"`

	// Requires and Hosts are the capabilities and egress hosts this research
	// consumed. A reader is served only if its own envelope holds all of them —
	// the ledger saves a call the reader was allowed to make and never makes one
	// it was not.
	Requires []security.Capability `json:"requires,omitempty"`
	Hosts    []string              `json:"hosts,omitempty"`

	// Cost is what the research that produced this finding actually cost. It is
	// what a later serve credits as avoided spend, and — averaged per topic —
	// what the gate's break-even rule weighs before it spends a model call
	// deciding whether a near match is good enough.
	Cost core.Usage `json:"cost,omitempty"`

	Volatility Volatility `json:"volatility,omitempty"`

	// Supersedes names the finding hash this revision replaces; Retracted marks
	// a head withdrawn, with Note saying why.
	Supersedes string `json:"supersedes,omitempty"`
	Retracted  bool   `json:"retracted,omitempty"`
	Note       string `json:"note,omitempty"`
}

// Hash is the finding's content address: the whole value, canonically encoded.
func (f Finding) Hash() string {
	blob, err := json.Marshal(f.canonical())
	if err != nil {
		return ""
	}
	return store.Hash(blob)
}

// Knowledge is the hash of the claim alone — what is asserted, what it covers,
// and whether it is a negative result — with identity, revision history,
// provenance, cost and warrant excluded.
//
// It is what makes independent rediscovery legible. Two agents that reach the
// same conclusion by different routes produce different findings and one
// knowledge hash, so the ledger records the second as corroboration of the
// first rather than as a rival claim; two agents that disagree produce two
// knowledge hashes in one class, which is a contradiction the ledger can see
// and report. Excluding the clock is what makes this hold at all, and it is the
// same rule Fleet.Post follows in carrying no timestamp: a wall clock in the
// body would make identical knowledge hash differently every time it was
// learned.
func (f Finding) Knowledge() string {
	body := struct {
		Topic      string         `json:"topic"`
		Answer     string         `json:"answer"`
		Fields     map[string]any `json:"fields,omitempty"`
		Covers     []string       `json:"covers,omitempty"`
		NoEvidence bool           `json:"no_evidence,omitempty"`
	}{
		Topic:      strings.ToLower(strings.TrimSpace(f.Topic)),
		Answer:     normalizeText(f.Answer),
		Fields:     f.Fields,
		Covers:     f.covers(),
		NoEvidence: f.NoEvidence,
	}
	blob, err := json.Marshal(body)
	if err != nil {
		return ""
	}
	return store.Hash(blob)
}

// canonical returns the finding with its derived and order-dependent parts
// normalized, so equal findings hash equally.
func (f Finding) canonical() Finding {
	c := f
	c.Covers = f.covers()
	c.Requires = append([]security.Capability(nil), f.Requires...)
	sort.Slice(c.Requires, func(i, j int) bool { return c.Requires[i] < c.Requires[j] })
	c.Hosts = append([]string(nil), f.Hosts...)
	sort.Strings(c.Hosts)
	c.Asked = f.Asked.Normalize()
	return c
}

// covers returns the finding's coverage set, defaulting to the field names when
// it was not stated, deduplicated and sorted.
func (f Finding) covers() []string {
	src := f.Covers
	if len(src) == 0 {
		for k := range f.Fields {
			src = append(src, k)
		}
	}
	seen := make(map[string]struct{}, len(src))
	out := make([]string, 0, len(src))
	for _, c := range src {
		c = strings.ToLower(strings.TrimSpace(c))
		if c == "" {
			continue
		}
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// Gap returns the needs this finding does not cover. An empty result means the
// finding covers everything asked of it; a result equal to the needs means it
// covers nothing and is not a candidate at all.
func (f Finding) Gap(needs []string) []string {
	if len(needs) == 0 {
		return nil
	}
	covered := make(map[string]struct{})
	for _, c := range f.covers() {
		covered[c] = struct{}{}
	}
	var gap []string
	for _, need := range needs {
		need = strings.ToLower(strings.TrimSpace(need))
		if need == "" {
			continue
		}
		if _, ok := covered[need]; !ok {
			gap = append(gap, need)
		}
	}
	return gap
}

// --- Volatility ---------------------------------------------------------

// Volatility is how fast a topic's answers go wrong, and it is declared per
// topic rather than guessed per finding.
//
// That placement is the whole design. Freshness is a property of the *question*
// — a company's founding year does not become stale because the crawler was
// slow, and a share price is stale in minutes however carefully it was
// gathered — so a per-finding TTL is a per-finding guess about something the
// author of the topic already knows.
type Volatility string

const (
	// Static answers do not expire: history, definitions, identifiers.
	Static Volatility = "static"
	// Slow answers drift over months: org charts, product lines, biographies.
	Slow Volatility = "slow"
	// Daily answers turn over within a day: rankings, availability, weather.
	Daily Volatility = "daily"
	// Hourly answers turn over within the hour: incident status, queue depth.
	Hourly Volatility = "hourly"
	// Live answers are never reused. Declaring a topic Live is how a caller
	// says "this must always be fetched" without having to route around the
	// gate — the escape hatch is inside the mechanism rather than beside it,
	// so it is visible in policy and reported in the stats.
	Live Volatility = "live"
)

// Horizon is how long an answer of this volatility stays servable. Static
// returns zero, which the freshness check reads as "no expiry".
func (v Volatility) Horizon() time.Duration {
	switch v {
	case Slow:
		return 30 * 24 * time.Hour
	case Daily:
		return 24 * time.Hour
	case Hourly:
		return time.Hour
	case Live:
		return -1
	default: // Static and the zero value
		return 0
	}
}

// --- Policy -------------------------------------------------------------

// Scope bounds who a finding may be served to.
type Scope string

const (
	// ScopeFleet shares a finding with every agent on the fleet. The default,
	// and the point of the layer.
	ScopeFleet Scope = "fleet"
	// ScopePrivate keeps a finding readable only by the agent that learned it.
	// It exists because a question can itself be sensitive: a topic whose facets
	// carry a customer identifier is a question that should not be answerable by
	// asking whether anyone has asked it. Capability containment protects the
	// answer; scope protects the question.
	ScopePrivate Scope = "private"
)

// TopicPolicy declares how one topic behaves. The zero value is a static,
// fleet-scoped topic needing one source and no adjudication, which is the right
// default for reference facts and the wrong one for anything that moves — so
// declare the topics that move.
type TopicPolicy struct {
	Volatility Volatility
	// TTL overrides the volatility horizon when a topic needs a specific one.
	TTL time.Duration
	// MinSources is the corroboration a finding needs before it may be served
	// (default 1). Raising it makes the ledger serve only claims more than one
	// source stands behind, at the cost of researching more of them twice.
	MinSources int
	// MinConfidence rejects findings the contributor was not sure of.
	MinConfidence float64
	// Near is the cosine-similarity floor for the near tier (default 0.92).
	// It seeds each entry's own threshold rather than fixing it: a static
	// global threshold is the known failure mode of embedding-similarity
	// caching, so an entry that has been judged moves its own boundary.
	Near float64
	// Adjudicate asks a model whether a near candidate really answers the
	// question. It is the only part of the gate that can cost anything, it is
	// never reached by an exact hit, and its verdicts are memoized per
	// (question, finding) pair.
	Adjudicate bool
	Scope      Scope
}

// Judge decides whether a candidate finding answers a question. It is the
// escape hatch for the cases structure cannot settle, and it is deliberately a
// plain function: the gate does not care whether it is a model call, a rule, or
// a human in a loop.
type Judge func(ctx context.Context, q Question, f Finding) (bool, error)

// Embedder maps question text to vectors for the near tier. Nil disables the
// tier entirely — the gate then serves exact and class matches only, which is
// the mode with no model dependency at all.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// Policy is the gate's configuration.
type Policy struct {
	// Default applies to topics with no entry in Topics.
	Default TopicPolicy
	Topics  map[string]TopicPolicy

	// MaxWait bounds how long a follower waits behind the leader holding the
	// single-flight lease for its question (default 30s). On expiry the
	// follower researches the question itself: correctness is preserved and
	// deduplication is lost, which is the right way round for a bound whose
	// purpose is to stop a stuck leader from stalling a fleet.
	MaxWait time.Duration

	// Embedder and Judge are the two optional model-backed seams. Both nil is
	// a fully deterministic gate.
	Embedder Embedder
	Judge    Judge

	// JudgeCostUSD is what one adjudication costs, and BreakEven how many times
	// over the research it might save must exceed it before the gate is willing
	// to spend one (default 3).
	//
	// This is the rule that keeps the gate honest about its own overhead: the
	// ledger records what every finding cost to produce, so the mean research
	// cost of a topic is a measurement rather than an estimate, and a topic
	// whose answers are cheap to fetch is one the gate declines to think hard
	// about. It is the same shape as the planner's prefix-cache rule, which
	// only writes a cache entry when a second call exists to read it.
	JudgeCostUSD float64
	BreakEven    float64

	// Now is the clock, injectable so freshness is testable.
	Now func() time.Time
}

const (
	defaultMaxWait   = 30 * time.Second
	defaultNear      = 0.92
	defaultBreakEven = 3.0
)

// For returns the effective policy for a topic.
func (p Policy) For(topic string) TopicPolicy {
	tp, ok := p.Topics[strings.ToLower(strings.TrimSpace(topic))]
	if !ok {
		tp = p.Default
	}
	if tp.MinSources <= 0 {
		tp.MinSources = 1
	}
	if tp.Near <= 0 {
		tp.Near = defaultNear
	}
	if tp.Scope == "" {
		tp.Scope = ScopeFleet
	}
	if tp.TTL == 0 {
		tp.TTL = tp.Volatility.Horizon()
	}
	return tp
}

func (p Policy) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p Policy) maxWait() time.Duration {
	if p.MaxWait > 0 {
		return p.MaxWait
	}
	return defaultMaxWait
}

func (p Policy) breakEven() float64 {
	if p.BreakEven > 0 {
		return p.BreakEven
	}
	return defaultBreakEven
}

// --- Sufficiency --------------------------------------------------------

// Verdict is the outcome of asking whether one finding answers one question.
// Gap is the needs it left uncovered, which is what turns a rejection into a
// narrowed external request rather than a repeat of the original one.
type Verdict struct {
	Sufficient bool
	Gap        []string
	Reason     string
}

func sufficient() Verdict { return Verdict{Sufficient: true} }

func insufficient(reason string, gap ...string) Verdict {
	return Verdict{Gap: gap, Reason: reason}
}

// Reachable reports whether an envelope's grants and egress allowlist cover
// everything the finding's research consumed.
//
// This is the invariant that makes a shared ledger safe to put in front of
// least-privilege execution, and it is checked first because it is both the
// cheapest test and the one whose failure is not a cache miss but a denial.
// Without it the ledger is a capability-laundering channel: an agent denied a
// host reaches its contents by waiting for an agent that was granted it.
func Reachable(f Finding, grants security.GrantSet, egress security.EgressPolicy) (bool, string) {
	for _, cap := range f.Requires {
		if !grants.Has(cap) {
			return false, fmt.Sprintf("reader lacks %s", cap)
		}
	}
	for _, host := range f.Hosts {
		if host == "" {
			continue
		}
		if !egress.Allowed(host) {
			return false, fmt.Sprintf("reader cannot reach %s", host)
		}
	}
	return true, ""
}
