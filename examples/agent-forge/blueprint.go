package main

// The blueprint schema, and the arithmetic that decides the org shape.
//
// The corpus has to answer two different questions, and collapsing them is the
// easiest way to get a wrong answer:
//
//	partition — what separates one agent INSTANCE from another. "One PPC agent"
//	            is {axis: global, instances: 1}. "An owner per vertical" is
//	            {axis: vertical, instances: 5}.
//	remembers — what one instance accumulates knowledge ABOUT, ranked. A bizdev
//	            agent remembers partners first, verticals second. A PPC agent
//	            partitioned by vertical remembers campaigns and channels — never
//	            its own partition key, which is a constant inside the instance.
//
// Everything downstream — the roster prompt, DESIGN.md, the UI — keys off these
// two fields. The numbers below are computed in Go from counted observations,
// not asked of a model, so the same corpus always gives the same recommendation
// and the model's job is to name and justify it rather than to invent it.

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// Job families. A fixed vocabulary keeps the spread metric meaningful: free-text
// function labels would scatter the same work across a dozen near-synonyms and
// silently flatten every distribution. The cost is that a corpus this vocabulary
// does not fit piles up in "other" — which is why the "other" share is reported
// everywhere rather than quietly dropped.
var functions = []string{
	"ppc",       // media buying: bids, budgets, keywords, campaign structure
	"partners",  // bizdev: counterparties, commercials, integrations, payouts
	"creative",  // ads, landing pages, copy, design iteration
	"analytics", // reporting, dashboards, attribution, BI pulls
	"product",   // site, funnel and product changes
	"quality",   // lead quality, fraud, compliance
	"ops",       // process, access, scheduling, vendor admin
	"finance",   // invoicing, payouts, budget approval
	"other",
}

// Entity axes: the kinds of thing work is *about*. These become partition keys
// and memory keys, so the vocabulary is deliberately small and concrete.
var axes = []string{
	"vertical", // the business line / channel the conversation lives in
	"partner",  // an external commercial counterparty
	"channel",  // an ad platform: google, meta, bing, tiktok
	"campaign", // a campaign, ad group or creative set
	"account",  // an ad account, property or integration endpoint
	"geo",      // a market or region
	"person",   // an internal owner
}

func knownFunction(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	for _, f := range functions {
		if f == s {
			return f
		}
	}
	return "other"
}

func knownAxis(s string) (string, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	for _, a := range axes {
		if a == s {
			return a, true
		}
	}
	return "", false
}

// jobObs is one job observed on one day in one space: the unit the census
// produces and everything downstream counts.
type jobObs struct {
	Space     string   `json:"space"`
	Date      string   `json:"date"`
	Label     string   `json:"label"`
	Function  string   `json:"function"`
	Entities  []string `json:"entities"` // axes the work operates on
	Memory    []string `json:"memory"`   // axes it must recall across days
	Trigger   string   `json:"trigger"`
	Cadence   string   `json:"cadence"`
	Decision  string   `json:"decision"`
	Tools     []string `json:"tools"`
	HandoffTo string   `json:"handoff_to"` // a function, when work crosses families
	Quote     string   `json:"quote"`
}

// capability is a canonical job family after the taxonomy pass folds spelling
// and phrasing variants together.
type capability struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Function    string   `json:"function"`
	Aliases     []string `json:"aliases,omitempty"`
	Summary     string   `json:"summary,omitempty"`
	Synthesized bool     `json:"synthesized,omitempty"` // no taxonomy entry matched; kept from the raw label
}

// memoryAxis is one entry in an agent's ranked recall list.
type memoryAxis struct {
	Axis   string  `json:"axis"`
	Weight float64 `json:"weight"`
	Why    string  `json:"why,omitempty"`
}

// partition is what separates one agent instance from another.
type partition struct {
	Axis      string   `json:"axis"` // global | vertical | partner | channel | ...
	Instances int      `json:"instances"`
	Keys      []string `json:"keys,omitempty"`
}

func (p partition) String() string {
	if p.Axis == "global" || p.Instances <= 1 {
		return "one shared instance"
	}
	return fmt.Sprintf("one instance per %s (%d)", p.Axis, p.Instances)
}

// capStats is everything counted about one canonical capability.
type capStats struct {
	Cap       capability         `json:"capability"`
	Count     int                `json:"count"`
	Spaces    map[string]int     `json:"spaces"`
	Days      int                `json:"days"`
	Spread    float64            `json:"spread"` // 0 = lives in one space, 1 = even across all
	MemoryMix map[string]float64 `json:"memory_mix"`
	Remembers []memoryAxis       `json:"remembers"`
	Tools     []string           `json:"tools,omitempty"`
	Quotes    []string           `json:"quotes,omitempty"`
}

// spaceStats is everything counted about one space (a vertical's channel).
type spaceStats struct {
	Space       string         `json:"space"`
	Count       int            `json:"count"`
	Days        int            `json:"days"`
	Functions   map[string]int `json:"functions"`
	Diversity   float64        `json:"diversity"`    // normalised entropy of the function mix
	HandoffRate float64        `json:"handoff_rate"` // share of jobs handing off to another function
	Coupling    float64        `json:"coupling"`     // 0 = one kind of work, 1 = everything tangled together
}

// topology is the org-shape verdict: four candidate shapes scored on the same
// two numbers, so the rejected options carry their own reason.
type topology struct {
	Spread      float64            `json:"spread"`   // S: do capabilities recur across spaces?
	Coupling    float64            `json:"coupling"` // C: is work inside one space entangled across functions?
	Scores      map[string]float64 `json:"scores"`
	Recommended string             `json:"recommended"`
	Rationale   string             `json:"rationale"`
	Rejected    []rejectedShape    `json:"rejected"`
}

type rejectedShape struct {
	Shape  string  `json:"shape"`
	Score  float64 `json:"score"`
	Reason string  `json:"reason"`
}

// agentProposal is the deterministic roster: what Go concludes before any model
// is asked. The roster stage may rename, merge or split these, but if it returns
// nothing usable this is what ships.
type agentProposal struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	Scope        string       `json:"scope"` // function | vertical | generalist
	Partition    partition    `json:"partition"`
	Capabilities []string     `json:"capabilities"`
	Remembers    []memoryAxis `json:"remembers"`
	Volume       int          `json:"volume"`
	Share        float64      `json:"share"`
	Why          string       `json:"why"`
}

// sharedMemory is a ledger more than one agent reads: an axis several agents
// must remember is a service, not a copy per agent.
type sharedMemory struct {
	Axis    string   `json:"axis"`
	Readers []string `json:"readers"`
	Weight  float64  `json:"weight"`
	Why     string   `json:"why"`
}

// census is the whole counted picture handed to the roster stage and the UI.
type census struct {
	Spaces       []string        `json:"spaces"`
	Days         int             `json:"days"`
	Observations int             `json:"observations"`
	Matched      int             `json:"matched"`
	OtherShare   float64         `json:"other_share"`  // share of jobs the function vocabulary did not fit
	MatchRate    float64         `json:"match_rate"`   // share of labels the taxonomy did fold
	Capabilities []capStats      `json:"capabilities"` // sorted by volume
	SpaceStats   []spaceStats    `json:"space_stats"`  // sorted by name
	FunctionMix  map[string]int  `json:"function_mix"`
	Topology     topology        `json:"topology"`
	Proposal     []agentProposal `json:"proposal"`
	Shared       []sharedMemory  `json:"shared_memory"`
}

// ---------------------------------------------------------------------------
// scoring
// ---------------------------------------------------------------------------

// entropyNorm is Shannon entropy over the counts, normalised to [0,1] by the
// entropy of a uniform distribution over the same number of buckets. One bucket
// scores 0 — a capability seen in a single space has no spread by definition.
func entropyNorm(counts map[string]int, buckets int) float64 {
	if buckets < 2 {
		return 0
	}
	total := 0
	for _, c := range counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	h := 0.0
	for _, c := range counts {
		if c <= 0 {
			continue
		}
		p := float64(c) / float64(total)
		h -= p * math.Log(p)
	}
	return h / math.Log(float64(buckets))
}

// rankAxes turns a weight map into a ranked, normalised recall list, dropping
// the agent's own partition axis: inside one instance that axis is a constant,
// not something to remember. This is what makes "an agent per vertical that
// remembers partners" come out as a ranking rather than a slogan.
func rankAxes(mix map[string]float64, exclude string) []memoryAxis {
	total := 0.0
	for a, w := range mix {
		if a == exclude {
			continue
		}
		total += w
	}
	if total == 0 {
		return nil
	}
	out := make([]memoryAxis, 0, len(mix))
	for a, w := range mix {
		if a == exclude || w <= 0 {
			continue
		}
		out = append(out, memoryAxis{Axis: a, Weight: w / total})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Weight != out[j].Weight {
			return out[i].Weight > out[j].Weight
		}
		return out[i].Axis < out[j].Axis
	})
	// Keep the tail out of the design: an axis under 5% is noise, not memory.
	kept := out[:0]
	for _, m := range out {
		if m.Weight >= 0.05 || len(kept) == 0 {
			kept = append(kept, m)
		}
	}
	if len(kept) > 4 {
		kept = kept[:4]
	}
	// Renormalise over what survived, so the published weights are shares of the
	// memory that is actually being designed rather than of a longer list the
	// reader never sees.
	total = 0
	for _, m := range kept {
		total += m.Weight
	}
	for i := range kept {
		kept[i].Weight /= total
	}
	return kept
}

// axisWeights turns one observation into axis weights. What a job must *recall*
// counts double what it merely *touches*: memory design follows recall.
func axisWeights(o jobObs, into map[string]float64) {
	add := func(list []string, w float64) {
		valid := make([]string, 0, len(list))
		for _, raw := range list {
			if a, ok := knownAxis(raw); ok {
				valid = append(valid, a)
			}
		}
		if len(valid) == 0 {
			return
		}
		each := w / float64(len(valid))
		for _, a := range valid {
			into[a] += each
		}
	}
	add(o.Memory, 1.0)
	add(o.Entities, 0.5)
}

// buildCensus counts everything: per-capability spread, per-space coupling, the
// topology verdict, a deterministic roster and the shared ledgers it implies.
func buildCensus(obs []jobObs, caps []capability, spaces []string) census {
	c := census{Spaces: append([]string(nil), spaces...), Observations: len(obs), FunctionMix: map[string]int{}}
	sort.Strings(c.Spaces)
	if len(obs) == 0 {
		return c
	}

	m := newMatcher(caps)
	byCap := map[string]*capStats{}
	bySpace := map[string]*spaceStats{}
	spaceDays := map[string]map[string]bool{}
	capDays := map[string]map[string]bool{}
	dayFns := map[string]map[string]bool{} // space|date -> functions present

	for _, o := range obs {
		fn := knownFunction(o.Function)
		c.FunctionMix[fn]++

		cap, matched := m.match(o.Label, fn)
		if matched {
			c.Matched++
		}
		cs := byCap[cap.ID]
		if cs == nil {
			cs = &capStats{Cap: cap, Spaces: map[string]int{}, MemoryMix: map[string]float64{}}
			byCap[cap.ID] = cs
			capDays[cap.ID] = map[string]bool{}
		}
		cs.Count++
		cs.Spaces[o.Space]++
		capDays[cap.ID][o.Space+"|"+o.Date] = true
		axisWeights(o, cs.MemoryMix)
		for _, t := range o.Tools {
			if t = strings.TrimSpace(t); t != "" && !contains(cs.Tools, t) && len(cs.Tools) < 8 {
				cs.Tools = append(cs.Tools, t)
			}
		}
		if q := strings.TrimSpace(o.Quote); q != "" && len(cs.Quotes) < 4 {
			cs.Quotes = append(cs.Quotes, fmt.Sprintf("%s %s — %s", o.Space, o.Date, q))
		}

		ss := bySpace[o.Space]
		if ss == nil {
			ss = &spaceStats{Space: o.Space, Functions: map[string]int{}}
			bySpace[o.Space] = ss
			spaceDays[o.Space] = map[string]bool{}
		}
		ss.Count++
		ss.Functions[fn]++
		spaceDays[o.Space][o.Date] = true
		if h := knownFunction(o.HandoffTo); o.HandoffTo != "" && h != fn {
			ss.HandoffRate++ // counted now, divided below
		}

		key := o.Space + "|" + o.Date
		if dayFns[key] == nil {
			dayFns[key] = map[string]bool{}
		}
		dayFns[key][fn] = true
	}

	c.MatchRate = float64(c.Matched) / float64(len(obs))
	c.OtherShare = float64(c.FunctionMix["other"]) / float64(len(obs))
	c.Days = len(dayFns)

	// Per capability: how evenly does this work spread across spaces? Even
	// spread is the case for one shared, function-scoped agent.
	nSpaces := len(c.Spaces)
	for id, cs := range byCap {
		cs.Spread = entropyNorm(cs.Spaces, nSpaces)
		cs.Days = len(capDays[id])
		cs.Remembers = rankAxes(cs.MemoryMix, "")
		c.Capabilities = append(c.Capabilities, *cs)
	}
	sort.Slice(c.Capabilities, func(i, j int) bool {
		if c.Capabilities[i].Count != c.Capabilities[j].Count {
			return c.Capabilities[i].Count > c.Capabilities[j].Count
		}
		return c.Capabilities[i].Cap.ID < c.Capabilities[j].Cap.ID
	})

	// Per space: how tangled is the work? Two halves, both printed — a diverse
	// function mix, and explicit handoffs between families. Either one is a case
	// for an agent that owns the whole space.
	nFns := len(functions)
	for _, ss := range bySpace {
		ss.Days = len(spaceDays[ss.Space])
		ss.Diversity = entropyNorm(ss.Functions, nFns)
		ss.HandoffRate = ss.HandoffRate / float64(ss.Count)
		ss.Coupling = 0.5*ss.Diversity + 0.5*math.Min(1, ss.HandoffRate*2)
		c.SpaceStats = append(c.SpaceStats, *ss)
	}
	sort.Slice(c.SpaceStats, func(i, j int) bool { return c.SpaceStats[i].Space < c.SpaceStats[j].Space })

	c.Topology = scoreTopology(c)
	c.Proposal = proposeAgents(c)
	c.Shared = sharedLedgers(c.Proposal)
	return c
}

// scoreTopology reduces the whole corpus to two numbers and reads the org shape
// off the quadrant they land in.
//
//	S (spread)   — volume-weighted mean capability spread across spaces.
//	               High: the same job is done five times in five places.
//	C (coupling) — volume-weighted mean within-space entanglement.
//	               High: one space runs many families of work that hand off.
//
// The four shapes are the four corners, so a shape's score is also the argument
// against the others: function-scoped work wants recurrence without tangle,
// vertical-scoped work wants tangle without recurrence, hybrid wants both, and
// a single generalist wants neither.
func scoreTopology(c census) topology {
	var sNum, sDen float64
	for _, cs := range c.Capabilities {
		sNum += cs.Spread * float64(cs.Count)
		sDen += float64(cs.Count)
	}
	var cNum, cDen float64
	for _, ss := range c.SpaceStats {
		cNum += ss.Coupling * float64(ss.Count)
		cDen += float64(ss.Count)
	}
	S, C := 0.0, 0.0
	if sDen > 0 {
		S = sNum / sDen
	}
	if cDen > 0 {
		C = cNum / cDen
	}

	t := topology{Spread: S, Coupling: C, Scores: map[string]float64{
		"function": S * (1 - C),
		"vertical": C * (1 - S),
		"hybrid":   math.Min(S, C),
		"single":   (1 - S) * (1 - C),
	}}

	best, bestScore := "single", -1.0
	order := []string{"hybrid", "function", "vertical", "single"} // ties break toward more structure
	for _, k := range order {
		if t.Scores[k] > bestScore {
			best, bestScore = k, t.Scores[k]
		}
	}
	t.Recommended = best

	reasons := map[string]string{
		"function": fmt.Sprintf("capabilities recur across spaces (spread %.2f) and work inside a space is not tangled (coupling %.2f) — the same agent can serve every vertical", S, C),
		"vertical": fmt.Sprintf("work inside a space is tangled across families (coupling %.2f) and capabilities do not recur elsewhere (spread %.2f) — context, not craft, is what an agent has to hold", C, S),
		"hybrid":   fmt.Sprintf("both forces are live (spread %.2f, coupling %.2f): recurring craft wants shared function agents, tangled context wants a per-vertical owner, so run both over shared memory", S, C),
		"single":   fmt.Sprintf("neither force is strong (spread %.2f, coupling %.2f) — the corpus does not justify splitting the work at all", S, C),
	}
	t.Rationale = reasons[best]
	against := map[string]string{
		"function": "would duplicate context five times over, and each copy would drift",
		"vertical": "would rebuild the same skill in every vertical and share nothing",
		"hybrid":   "adds a coordination surface the evidence does not pay for",
		"single":   "one agent would carry unrelated jobs and remember all of them badly",
	}
	for _, k := range order {
		if k == best {
			continue
		}
		t.Rejected = append(t.Rejected, rejectedShape{Shape: k, Score: t.Scores[k], Reason: against[k]})
	}
	sort.Slice(t.Rejected, func(i, j int) bool { return t.Rejected[i].Score > t.Rejected[j].Score })
	return t
}

const minAgentShare = 0.06 // below this a family is a skill inside another agent, not an agent

// proposeAgents builds the deterministic roster the topology implies. This is
// the fallback the pipeline ships if the roster stage returns nothing usable,
// and the baseline its output is diffed against.
func proposeAgents(c census) []agentProposal {
	if c.Observations == 0 {
		return nil
	}
	total := float64(c.Observations)
	shape := c.Topology.Recommended

	byFunction := func(caps []capStats) []agentProposal {
		vol := map[string]int{}
		mix := map[string]map[string]float64{}
		owned := map[string][]string{}
		for _, cs := range caps {
			fn := cs.Cap.Function
			vol[fn] += cs.Count
			if mix[fn] == nil {
				mix[fn] = map[string]float64{}
			}
			for a, w := range cs.MemoryMix {
				mix[fn][a] += w
			}
			owned[fn] = append(owned[fn], cs.Cap.ID)
		}
		var out []agentProposal
		for _, fn := range functions {
			if vol[fn] == 0 {
				continue
			}
			share := float64(vol[fn]) / total
			if share < minAgentShare {
				continue
			}
			// A function agent is global unless its own work never crosses
			// spaces, in which case it is really a vertical role wearing a
			// function's name.
			part := partition{Axis: "global", Instances: 1}
			out = append(out, agentProposal{
				ID:           fn,
				Name:         titleFunction(fn),
				Scope:        "function",
				Partition:    part,
				Capabilities: owned[fn],
				Remembers:    rankAxes(mix[fn], ""),
				Volume:       vol[fn],
				Share:        share,
				Why:          fmt.Sprintf("%s work recurs across %d of %d spaces", titleFunction(fn), spacesTouched(caps, fn), len(c.Spaces)),
			})
		}
		return out
	}

	byVertical := func() []agentProposal {
		var out []agentProposal
		for _, ss := range c.SpaceStats {
			share := float64(ss.Count) / total
			if share < minAgentShare {
				continue
			}
			mix := map[string]float64{}
			var owned []string
			for _, cs := range c.Capabilities {
				if cs.Spaces[ss.Space] == 0 {
					continue
				}
				owned = append(owned, cs.Cap.ID)
				w := float64(cs.Spaces[ss.Space]) / float64(cs.Count)
				for a, v := range cs.MemoryMix {
					mix[a] += v * w
				}
			}
			out = append(out, agentProposal{
				ID:           "owner-" + ss.Space,
				Name:         titleSpace(ss.Space) + " owner",
				Scope:        "vertical",
				Partition:    partition{Axis: "vertical", Instances: 1, Keys: []string{ss.Space}},
				Capabilities: owned,
				// The partition axis is dropped here: inside this instance the
				// vertical is a constant, so what it remembers is everything else.
				Remembers: rankAxes(mix, "vertical"),
				Volume:    ss.Count,
				Share:     share,
				Why:       fmt.Sprintf("%s runs %d job families with coupling %.2f", ss.Space, len(ss.Functions), ss.Coupling),
			})
		}
		return out
	}

	switch shape {
	case "function":
		return withGeneralist(byFunction(c.Capabilities), c, total)
	case "vertical":
		agents := byVertical()
		// Collapse to one instance-per-vertical agent rather than N named ones
		// when the spaces look alike: same roster, different key.
		return withGeneralist(agents, c, total)
	case "hybrid":
		// Recurring craft goes to shared function agents; the rest stays with a
		// per-vertical owner that holds the context those agents do not.
		var recurring, local []capStats
		for _, cs := range c.Capabilities {
			if cs.Spread >= 0.5 && len(cs.Spaces) > 1 {
				recurring = append(recurring, cs)
			} else {
				local = append(local, cs)
			}
		}
		agents := byFunction(recurring)
		localVol := 0
		mix := map[string]float64{}
		var owned []string
		for _, cs := range local {
			localVol += cs.Count
			owned = append(owned, cs.Cap.ID)
			for a, w := range cs.MemoryMix {
				mix[a] += w
			}
		}
		if float64(localVol)/total >= minAgentShare {
			keys := append([]string(nil), c.Spaces...)
			agents = append(agents, agentProposal{
				ID:           "vertical-owner",
				Name:         "Vertical owner",
				Scope:        "vertical",
				Partition:    partition{Axis: "vertical", Instances: len(keys), Keys: keys},
				Capabilities: owned,
				Remembers:    rankAxes(mix, "vertical"),
				Volume:       localVol,
				Share:        float64(localVol) / total,
				Why:          fmt.Sprintf("%d capabilities stay inside one space and carry the context the shared agents lack", len(local)),
			})
		}
		return withGeneralist(agents, c, total)
	default:
		mix := map[string]float64{}
		var owned []string
		for _, cs := range c.Capabilities {
			owned = append(owned, cs.Cap.ID)
			for a, w := range cs.MemoryMix {
				mix[a] += w
			}
		}
		return []agentProposal{{
			ID: "operator", Name: "Operations agent", Scope: "generalist",
			Partition: partition{Axis: "global", Instances: 1}, Capabilities: owned,
			Remembers: rankAxes(mix, ""), Volume: c.Observations, Share: 1,
			Why: "the corpus does not separate cleanly by function or by vertical",
		}}
	}
}

// withGeneralist sweeps up whatever the thresholds left unowned. Work that no
// agent owns is the failure mode this catches: it shows up as a named agent with
// a small share rather than as silence.
func withGeneralist(agents []agentProposal, c census, total float64) []agentProposal {
	owned := map[string]bool{}
	for _, a := range agents {
		for _, id := range a.Capabilities {
			owned[id] = true
		}
	}
	rest := 0
	mix := map[string]float64{}
	var ids []string
	for _, cs := range c.Capabilities {
		if owned[cs.Cap.ID] {
			continue
		}
		rest += cs.Count
		ids = append(ids, cs.Cap.ID)
		for a, w := range cs.MemoryMix {
			mix[a] += w
		}
	}
	if rest > 0 {
		agents = append(agents, agentProposal{
			ID: "generalist", Name: "Generalist", Scope: "generalist",
			Partition: partition{Axis: "global", Instances: 1}, Capabilities: ids,
			Remembers: rankAxes(mix, ""), Volume: rest, Share: float64(rest) / total,
			Why: fmt.Sprintf("%d long-tail capabilities below the %.0f%% threshold for their own agent", len(ids), minAgentShare*100),
		})
	}
	sort.Slice(agents, func(i, j int) bool {
		if agents[i].Volume != agents[j].Volume {
			return agents[i].Volume > agents[j].Volume
		}
		return agents[i].ID < agents[j].ID
	})
	return agents
}

// sharedLedgers finds the axes more than one agent has to remember. Two agents
// keeping private notes on the same partner is the thing to design away, so an
// axis with two or more serious readers becomes a memory service they share.
func sharedLedgers(agents []agentProposal) []sharedMemory {
	readers := map[string][]string{}
	weight := map[string]float64{}
	for _, a := range agents {
		for _, m := range a.Remembers {
			if m.Weight < 0.15 {
				continue
			}
			readers[m.Axis] = append(readers[m.Axis], a.ID)
			weight[m.Axis] += m.Weight
		}
	}
	var out []sharedMemory
	for axis, rs := range readers {
		if len(rs) < 2 {
			continue
		}
		sort.Strings(rs)
		out = append(out, sharedMemory{
			Axis: axis, Readers: rs, Weight: weight[axis] / float64(len(rs)),
			Why: fmt.Sprintf("%d agents need %s recall — keep one ledger they all read rather than %d drifting copies", len(rs), axis, len(rs)),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i].Readers) != len(out[j].Readers) {
			return len(out[i].Readers) > len(out[j].Readers)
		}
		return out[i].Axis < out[j].Axis
	})
	return out
}

func spacesTouched(caps []capStats, fn string) int {
	seen := map[string]bool{}
	for _, cs := range caps {
		if cs.Cap.Function != fn {
			continue
		}
		for s := range cs.Spaces {
			seen[s] = true
		}
	}
	return len(seen)
}

func titleFunction(fn string) string {
	switch fn {
	case "ppc":
		return "PPC"
	case "partners":
		return "Bizdev"
	case "analytics":
		return "Analytics"
	default:
		return strings.ToUpper(fn[:1]) + fn[1:]
	}
}

// agentTitle names an agent from its id. Function-scoped ids get the function
// vocabulary's own capitalisation ("ppc" is PPC, never Ppc); everything else is
// a slug and gets title-cased.
func agentTitle(id string) string {
	if knownFunction(id) == id {
		return titleFunction(id)
	}
	return titleSpace(id)
}

func titleSpace(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '-' || r == '_' })
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
