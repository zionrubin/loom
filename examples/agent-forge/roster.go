package main

// Run 2 — the reduce half, and the decision.
//
//	label-catalog ─ capability-map ─ score ─ roster
//
// Loom's DAG fans out but never fans back in, so the cross-cutting question —
// what shape should the org be — cannot live in run 1. It gets its own run,
// seeded with what run 1 produced.
//
// The chain is linear on purpose. label-catalog carries every DISTINCT job
// label with its count and the spaces it appeared in, deduplicated in Go: on a
// three-thousand-day corpus that is a few hundred items instead of fifteen
// thousand, and the taxonomy reduce costs a few calls instead of thousands.
// score is plain Go — the spread, coupling and memory arithmetic — so the same
// corpus always yields the same numbers, and the roster stage is left with the
// job models are good at: naming the shape and arguing for it.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

const taxonomyPrefix = `You are folding a list of job labels, harvested day by day from one company's chat, into a
canonical capability list.

The same job was written down differently every time it came up: "pause losing keywords",
"keyword pausing", "pause bad kws" are one capability. Cluster them. Prefer the clearest phrasing
as the canonical name and keep every variant as an alias, because a later step matches raw labels
against these aliases and an alias that never appears is dead weight.

Judgement about grain: a capability should be something you could staff or automate as a unit.
"Manage Google Ads" is too coarse. "Pause keyword ABC" is too fine. Aim for 15-40 capabilities in
total across all functions. Merge aggressively — two labels that would be done by the same person
with the same tools at the same moment are one capability.

Keep each capability inside a single function, using exactly the function the labels carry.

Respond with a single JSON object and nothing else:
{"capabilities": [{"id": "<kebab-case, unique, function-prefixed e.g. ppc-budget-pacing>",
                   "name": "<canonical phrasing, verb first>",
                   "function": "<ppc|partners|creative|analytics|product|quality|ops|finance|other>",
                   "aliases": ["<every raw label that folds in here>"],
                   "summary": "<one line: what doing this actually involves>"}]}

If the input is itself a list of partial capability lists, merge them the same way: same id for
the same capability, union the aliases.`

const rosterPrefix = `You are deciding the shape of an agentic system from measured evidence about how a company
actually works, and you are being handed the measurements rather than the raw chat.

Two numbers decide the shape, and you must reason from them rather than from intuition:

  spread   how evenly a capability recurs across spaces. High spread means five teams do the same
           job five times — one shared agent can serve all of them.
  coupling how entangled the work inside one space is across job families, counting both the mix
           of families and explicit handoffs between them. High coupling means what matters is
           holding one space's whole context, not one craft.

Keep two things separate, because they have different answers and conflating them is the usual
way this goes wrong:

  partition  what separates one agent INSTANCE from another. "One PPC agent for the company" is
             {"axis": "global", "instances": 1}. "An owner per vertical" is {"axis": "vertical",
             "instances": 5, "keys": [...]}.
  remembers  what a single instance accumulates knowledge ABOUT, RANKED, heaviest first. This is
             the memory design. An agent partitioned by vertical must NOT list "vertical" — inside
             one instance that is a constant, not something to recall. A bizdev agent that is
             global but tracks counterparties over time remembers partner first, vertical second.

Each agent is a full agent: its own memory, its own tools, its own triggers. That is what makes
the split expensive and the memory ranking the real design decision. Where two agents would have
to remember the same axis, say so as shared memory rather than giving each a private copy that
drifts.

You are given a computed recommendation. Adopt it unless the evidence contradicts it, and if you
override it, say which number you are overriding and why in the verdict. Name agents for the work,
not for the org chart. Six agents is a lot; prefer fewer with sharper memory.

Respond with a single JSON object and nothing else:
{"topology": "function|vertical|hybrid|single",
 "verdict": "<2-4 sentences: the shape, and the numbers that force it>",
 "agents": [{"id": "<kebab-case>",
             "name": "<short, human>",
             "mission": "<one sentence: what it is accountable for>",
             "partition": {"axis": "global|vertical|partner|channel", "instances": <int>, "keys": ["<if not global>"]},
             "remembers": [{"axis": "<partner|campaign|channel|account|geo|person|vertical>",
                            "weight": <0-1, descending, summing to about 1>,
                            "why": "<what it would get wrong without this>"}],
             "capabilities": ["<capability ids it owns>"],
             "why": "<why this is one agent and not two, or part of another>"}],
 "shared_memory": [{"axis": "<axis>", "readers": ["<agent ids>"], "why": "<why one ledger, not copies>"}],
 "rejected": [{"shape": "<shape not taken>", "why_not": "<the number that rules it out>"}]}`

// buildRosterPipeline is run 2: consolidate the job labels into a capability
// taxonomy, score the corpus against it in Go, then decide the org shape.
func buildRosterPipeline(obs []jobObs, spaces, systems []string, lu lineup) *pipeline.Pipeline {
	p := pipeline.New("agent-roster")

	labels := dedupeLabels(obs)
	recs := make([]core.Record, 0, len(labels))
	for i, l := range labels {
		recs = append(recs, core.NewRecord(fmt.Sprintf("label-%03d", i), map[string]any{
			"item": fmt.Sprintf("%s | function=%s | seen %dx in %d space(s): %s",
				l.Label, l.Function, l.Count, len(l.Spaces), strings.Join(l.Spaces, ", ")),
		}))
	}
	if len(recs) == 0 {
		recs = append(recs, core.NewRecord("label-000", map[string]any{
			"item": "no jobs were extracted from the corpus | function=other | seen 0x",
		}))
	}

	tax := p.FromRecords("label-catalog", recs).
		ReduceAI("capability-map", pipeline.ReduceAISpec{
			Binding:   model.Binding{Tier: model.TierBalanced, Escalation: []string{lu.deep}},
			System:    "You build capability taxonomies. You answer only with JSON.",
			Prefix:    taxonomyPrefix,
			Prompt:    "{{.Count}} job labels harvested from the corpus:\n\n{{range .Items}}{{.}}\n{{end}}",
			FanIn:     40,
			MaxTokens: 3000,
			ItemField: "item",
		})

	// Everything quantitative happens here, in Go. The stage is in the DAG so
	// it shows up in the run view next to the model calls it feeds.
	scored := tax.Map("score", func(r core.Record) (core.Record, error) {
		out := r.Clone()
		caps := parseCapabilities(r.String("output"))
		c := buildCensus(obs, caps, spaces)
		blob, err := json.Marshal(c)
		if err != nil {
			return core.Record{}, err
		}
		out.Data["census_json"] = string(blob)
		out.Data["brief"] = metricsBrief(c, systems)
		return out, nil
	}, pipeline.WithVersion("v1"))

	scored.Infer("roster", pipeline.InferSpec{
		Binding:   model.Binding{Tier: model.TierDeep},
		System:    "You design agentic systems from measured evidence. You answer only with JSON.",
		Prefix:    rosterPrefix,
		Prompt:    "{{.brief}}",
		MaxTokens: 4000,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			agents, ok := r.Data["agents"].([]any)
			if !ok || len(agents) == 0 {
				return fmt.Errorf("no agents proposed")
			}
			for _, a := range agents {
				m, ok := a.(map[string]any)
				if !ok {
					return fmt.Errorf("agent is not an object")
				}
				if str(m["id"]) == "" {
					return fmt.Errorf("agent without an id")
				}
				part, _ := m["partition"].(map[string]any)
				if part == nil || str(part["axis"]) == "" {
					return fmt.Errorf("agent %q without a partition axis", str(m["id"]))
				}
			}
			return nil
		},
	})
	return p
}

func parseCapabilities(text string) []capability {
	var wrap struct {
		Capabilities []capability `json:"capabilities"`
	}
	blob := extractJSON(text)
	if blob == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(blob), &wrap); err != nil {
		return nil
	}
	return wrap.Capabilities
}

// extractJSON pulls the first balanced JSON object out of text that may be
// wrapped in a code fence or padded with prose. ReduceAI returns raw text, so
// unlike an Infer stage there is no ParseJSON to lean on.
func extractJSON(text string) string {
	start := strings.Index(text, "{")
	if start < 0 {
		return ""
	}
	depth, inStr, esc := 0, false, false
	for i := start; i < len(text); i++ {
		ch := text[i]
		switch {
		case esc:
			esc = false
		case ch == '\\' && inStr:
			esc = true
		case ch == '"':
			inStr = !inStr
		case inStr:
		case ch == '{':
			depth++
		case ch == '}':
			depth--
			if depth == 0 {
				return text[start : i+1]
			}
		}
	}
	return ""
}

// metricsBrief renders the counted evidence the roster stage reasons over. It
// is the whole prompt: no conversation text reaches this stage, only numbers
// and the quotes already scrubbed at load.
func metricsBrief(c census, systems []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CORPUS\n%d spaces, %d days, %d job observations.\n",
		len(c.Spaces), c.Days, c.Observations)
	fmt.Fprintf(&b, "Spaces: %s\n", strings.Join(c.Spaces, ", "))
	fmt.Fprintf(&b, "Taxonomy folded %.0f%% of labels; %.0f%% of jobs fell outside the function vocabulary (\"other\").\n",
		c.MatchRate*100, c.OtherShare*100)
	if len(systems) > 0 {
		if len(systems) > 12 {
			systems = systems[:12]
		}
		fmt.Fprintf(&b, "Systems named: %s\n", strings.Join(systems, ", "))
	}

	b.WriteString("\nFUNCTION MIX\n")
	type fc struct {
		fn string
		n  int
	}
	var mix []fc
	for fn, n := range c.FunctionMix {
		mix = append(mix, fc{fn, n})
	}
	sort.Slice(mix, func(i, j int) bool {
		if mix[i].n != mix[j].n {
			return mix[i].n > mix[j].n
		}
		return mix[i].fn < mix[j].fn
	})
	for _, m := range mix {
		fmt.Fprintf(&b, "  %-10s %4d  %4.1f%%\n", m.fn, m.n, 100*float64(m.n)/float64(max(1, c.Observations)))
	}

	b.WriteString("\nCAPABILITIES  (count · spread across spaces · what it must remember)\n")
	shown := c.Capabilities
	if len(shown) > 28 {
		shown = shown[:28]
	}
	for _, cs := range shown {
		fmt.Fprintf(&b, "  %-34s %-10s n=%-4d spread=%.2f  in %d/%d spaces  remembers: %s\n",
			trunc(cs.Cap.ID, 34), cs.Cap.Function, cs.Count, cs.Spread,
			len(cs.Spaces), len(c.Spaces), axisList(cs.Remembers))
	}
	if len(c.Capabilities) > len(shown) {
		fmt.Fprintf(&b, "  … and %d more below the top %d by volume\n", len(c.Capabilities)-len(shown), len(shown))
	}

	b.WriteString("\nSPACES  (job volume · function diversity · handoff rate · coupling)\n")
	for _, ss := range c.SpaceStats {
		fmt.Fprintf(&b, "  %-22s n=%-4d days=%-4d diversity=%.2f handoffs=%.2f coupling=%.2f  families: %s\n",
			ss.Space, ss.Count, ss.Days, ss.Diversity, ss.HandoffRate, ss.Coupling, topFunctions(ss.Functions, 4))
	}

	fmt.Fprintf(&b, "\nTOPOLOGY SCORES  (spread S=%.2f, coupling C=%.2f)\n", c.Topology.Spread, c.Topology.Coupling)
	fmt.Fprintf(&b, "  function  %.3f = S·(1−C)   one agent per job family, shared across spaces\n", c.Topology.Scores["function"])
	fmt.Fprintf(&b, "  vertical  %.3f = C·(1−S)   one agent per space, owning every family in it\n", c.Topology.Scores["vertical"])
	fmt.Fprintf(&b, "  hybrid    %.3f = min(S,C)  shared function agents plus a per-space owner\n", c.Topology.Scores["hybrid"])
	fmt.Fprintf(&b, "  single    %.3f = (1−S)·(1−C)  one generalist\n", c.Topology.Scores["single"])
	fmt.Fprintf(&b, "  computed recommendation: %s — %s\n", strings.ToUpper(c.Topology.Recommended), c.Topology.Rationale)
	// Each alternative comes with the argument against it, so the model is
	// choosing between reasoned options rather than between four numbers.
	for _, r := range c.Topology.Rejected {
		fmt.Fprintf(&b, "  against %-9s %s\n", r.Shape, r.Reason)
	}

	b.WriteString("\nCOMPUTED ROSTER  (the baseline to adopt or override)\n")
	for _, a := range c.Proposal {
		fmt.Fprintf(&b, "  %-18s %-11s %-32s %4.1f%% of work  remembers: %s\n",
			a.ID, a.Scope, a.Partition.String(), a.Share*100, axisList(a.Remembers))
		fmt.Fprintf(&b, "      owns: %s\n", trunc(strings.Join(a.Capabilities, ", "), 150))
		fmt.Fprintf(&b, "      why:  %s\n", a.Why)
	}
	if len(c.Shared) > 0 {
		b.WriteString("\nSHARED MEMORY IMPLIED\n")
		for _, s := range c.Shared {
			fmt.Fprintf(&b, "  %-9s read by %s\n", s.Axis, strings.Join(s.Readers, ", "))
		}
	}

	b.WriteString("\nEVIDENCE  (verbatim, already pseudonymised)\n")
	n := 0
	for _, cs := range c.Capabilities {
		for _, q := range cs.Quotes {
			if n >= 12 {
				break
			}
			fmt.Fprintf(&b, "  [%s] %s\n", cs.Cap.ID, trunc(q, 160))
			n++
		}
		if n >= 12 {
			break
		}
	}
	return b.String()
}

func axisList(ms []memoryAxis) string {
	if len(ms) == 0 {
		return "nothing across days"
	}
	parts := make([]string, 0, len(ms))
	for _, m := range ms {
		parts = append(parts, fmt.Sprintf("%s %.2f", m.Axis, m.Weight))
	}
	return strings.Join(parts, " > ")
}

func topFunctions(m map[string]int, n int) string {
	type kv struct {
		k string
		v int
	}
	var all []kv
	for k, v := range m {
		all = append(all, kv{k, v})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].v != all[j].v {
			return all[i].v > all[j].v
		}
		return all[i].k < all[j].k
	})
	if len(all) > n {
		all = all[:n]
	}
	parts := make([]string, 0, len(all))
	for _, kv := range all {
		parts = append(parts, fmt.Sprintf("%s:%d", kv.k, kv.v))
	}
	return strings.Join(parts, " ")
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
