package main

// Turning the pipeline's JSON into the documents a person reads.
//
// The models produce state; the layout is written here. That split is worth
// keeping: it means a prompt change cannot quietly reformat the deliverable,
// tables always line up, every number in the prose is the counted one, and the
// same structures render to markdown and to the UI without a second pass.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/zionrubin/loom/observe"
)

type agentDoc struct {
	agentDecl
	Spec    map[string]any `json:"spec,omitempty"`
	Charter map[string]any `json:"charter,omitempty"`
}

type blueprint struct {
	Generated string            `json:"generated"`
	Source    string            `json:"source"`
	Provider  string            `json:"provider"`
	Census    census            `json:"census"`
	Roster    rosterDecision    `json:"roster"`
	Agents    []agentDoc        `json:"agents"`
	Profiles  map[string]string `json:"profiles"`
	Systems   []string          `json:"systems"`
	Runs      []map[string]any  `json:"runs"`
	CostUSD   float64           `json:"cost_usd"`
}

func nowStamp() string { return time.Now().Format("2006-01-02 15:04") }

// renderDesign writes the one document that answers the question the corpus was
// read to answer: what agents, partitioned how, remembering what.
func renderDesign(b blueprint) string {
	c := b.Census
	var w strings.Builder

	fmt.Fprintf(&w, "# Agentic system design\n\n")
	fmt.Fprintf(&w, "Derived from %d job observations across %d spaces and %d days of conversation.\n",
		c.Observations, len(c.Spaces), c.Days)
	fmt.Fprintf(&w, "Generated %s from `%s` (%s models).\n\n", b.Generated, b.Source, b.Provider)
	fmt.Fprintf(&w, "> Names, @mentions, e-mail addresses, phone numbers and long identifiers were replaced with\n")
	fmt.Fprintf(&w, "> pseudonyms and placeholders at load, before any text reached a model.\n\n")

	fmt.Fprintf(&w, "## The verdict\n\n**%s.** %s\n\n", strings.ToUpper(b.Roster.Topology), orNone(b.Roster.Verdict))
	fmt.Fprintf(&w, "| agent | instances | remembers, in order | share of work |\n|---|---|---|---|\n")
	for _, a := range b.Agents {
		fmt.Fprintf(&w, "| **%s** | %s | %s | %s |\n", a.Name, a.Partition.String(),
			mdAxes(a.Remembers), sharePct(a, c))
	}
	w.WriteString("\nRead the memory column as the answer to \"what does it look up first\". ")
	w.WriteString("An agent partitioned by an axis never remembers that axis — inside one instance it is a constant.\n\n")

	if len(b.Roster.Shared) > 0 {
		w.WriteString("### Memory they share\n\n")
		for _, s := range b.Roster.Shared {
			fmt.Fprintf(&w, "- **%s ledger** — read by %s. %s\n", s.Axis, strings.Join(s.Readers, ", "), s.Why)
		}
		w.WriteString("\n")
	}

	// 1. what they talk about
	w.WriteString("## What they actually talk about\n\n")
	for _, s := range c.Spaces {
		prof := strings.TrimSpace(b.Profiles[s])
		if prof == "" {
			continue
		}
		fmt.Fprintf(&w, "### %s\n\n%s\n\n", titleSpace(s), prof)
	}

	// 2. the measurement
	w.WriteString("## What the corpus measures\n\n")
	fmt.Fprintf(&w, "The taxonomy folded %.0f%% of raw job labels onto canonical capabilities. ", c.MatchRate*100)
	fmt.Fprintf(&w, "%.0f%% of jobs fell outside the function vocabulary and are counted as `other`", c.OtherShare*100)
	if c.OtherShare > 0.25 {
		w.WriteString(" — **high enough that the vocabulary is a poor fit for this corpus, and the spread numbers below should be read with that in mind**")
	}
	w.WriteString(".\n\n")

	w.WriteString("### Capabilities\n\n")
	w.WriteString("`spread` is how evenly a capability recurs across spaces: 1.00 means every space does it equally, 0.00 means it lives in one place.\n\n")
	w.WriteString("| capability | function | n | spread | spaces | must remember |\n|---|---|--:|--:|--:|---|\n")
	shown := c.Capabilities
	if len(shown) > 30 {
		shown = shown[:30]
	}
	for _, cs := range shown {
		name := cs.Cap.Name
		if cs.Cap.Synthesized {
			name += " ¹"
		}
		fmt.Fprintf(&w, "| %s | %s | %d | %.2f | %d/%d | %s |\n",
			name, cs.Cap.Function, cs.Count, cs.Spread, len(cs.Spaces), len(c.Spaces), mdAxes(cs.Remembers))
	}
	if len(c.Capabilities) > len(shown) {
		fmt.Fprintf(&w, "\n%d further capabilities below the top %d by volume.\n", len(c.Capabilities)-len(shown), len(shown))
	}
	if anySynthesized(shown) {
		w.WriteString("\n¹ seen in the corpus but not folded into the taxonomy — a label the consolidation pass did not recognise.\n")
	}
	w.WriteString("\n### Spaces\n\n")
	w.WriteString("`coupling` is how tangled one space's work is across job families — half the mix of families, half the rate of explicit handoffs between them.\n\n")
	w.WriteString("| space | jobs | days | family mix | handoffs | coupling |\n|---|--:|--:|--:|--:|--:|\n")
	for _, ss := range c.SpaceStats {
		fmt.Fprintf(&w, "| %s | %d | %d | %.2f | %.2f | %.2f |\n",
			ss.Space, ss.Count, ss.Days, ss.Diversity, ss.HandoffRate, ss.Coupling)
	}

	// 3. the shape
	fmt.Fprintf(&w, "\n## Why this shape\n\nTwo numbers decide it: **spread S = %.2f** (do capabilities recur across spaces?) and **coupling C = %.2f** (is work inside one space tangled across families?).\n\n",
		c.Topology.Spread, c.Topology.Coupling)
	w.WriteString("| shape | score | what it would mean |\n|---|--:|---|\n")
	meanings := map[string]string{
		"function": "one agent per job family, shared across every space",
		"vertical": "one agent per space, owning every family inside it",
		"hybrid":   "shared function agents plus a per-space owner, over shared memory",
		"single":   "one generalist agent",
	}
	formulas := map[string]string{"function": "S·(1−C)", "vertical": "C·(1−S)", "hybrid": "min(S,C)", "single": "(1−S)·(1−C)"}
	for _, k := range []string{"function", "vertical", "hybrid", "single"} {
		mark := ""
		if k == c.Topology.Recommended {
			mark = " ←"
		}
		fmt.Fprintf(&w, "| %s = %s%s | %.3f | %s |\n", k, formulas[k], mark, c.Topology.Scores[k], meanings[k])
	}
	fmt.Fprintf(&w, "\nComputed recommendation: **%s** — %s\n\n", c.Topology.Recommended, c.Topology.Rationale)
	if len(b.Roster.Rejected) > 0 {
		w.WriteString("What was rejected, and why:\n\n")
		for _, r := range b.Roster.Rejected {
			fmt.Fprintf(&w, "- **%s** — %s\n", r.Shape, r.WhyNot)
		}
		w.WriteString("\n")
	}
	if b.Roster.Topology != c.Topology.Recommended {
		fmt.Fprintf(&w, "> The design stage overrode the computed shape (%s → %s). Its reasoning is in the verdict above.\n\n",
			c.Topology.Recommended, b.Roster.Topology)
	}

	// 4. the agents
	w.WriteString("## The agents\n\n")
	for _, a := range b.Agents {
		w.WriteString(renderAgent(a, c))
		w.WriteString("\n---\n\n")
	}

	if len(b.Systems) > 0 {
		names := b.Systems
		if len(names) > 20 {
			names = names[:20]
		}
		fmt.Fprintf(&w, "## Systems these agents would touch\n\n%s\n\n", strings.Join(names, " · "))
	}
	return w.String()
}

// renderAgent is one agent's section, and also the whole of its own file.
func renderAgent(a agentDoc, c census) string {
	var w strings.Builder
	fmt.Fprintf(&w, "### %s\n\n", a.Name)
	fmt.Fprintf(&w, "**%s**\n\n", orNone(firstNonEmpty(mstr(a.Spec, "mission"), a.Mission)))
	fmt.Fprintf(&w, "- **Instances** — %s", a.Partition.String())
	if len(a.Partition.Keys) > 0 && a.Partition.Axis != "global" {
		fmt.Fprintf(&w, ": %s", strings.Join(a.Partition.Keys, ", "))
	}
	w.WriteString("\n")
	if a.Why != "" {
		fmt.Fprintf(&w, "- **Why one agent** — %s\n", a.Why)
	}

	w.WriteString("\n#### Memory\n\n")
	if mem := mmap(a.Spec, "memory"); mem != nil {
		fmt.Fprintf(&w, "Keyed on **%s**", orNone(mstr(mem, "primary_key")))
		if sec := mlist(mem, "secondary_keys"); len(sec) > 0 {
			fmt.Fprintf(&w, ", cross-referenced by %s", strings.Join(sec, ", "))
		}
		w.WriteString(".")
		if why := mstr(mem, "why_this_key"); why != "" {
			fmt.Fprintf(&w, " %s.", strings.TrimSuffix(why, "."))
		}
		w.WriteString("\n\n")
		if v := mstr(mem, "working"); v != "" {
			fmt.Fprintf(&w, "- **Working** — %s\n", v)
		}
		if ep := mmap(mem, "episodic"); ep != nil {
			fmt.Fprintf(&w, "- **Episodic** — %s. Keyed by %s. Retained %s.\n",
				orNone(mstr(ep, "record")), orNone(mstr(ep, "keyed_by")), orNone(mstr(ep, "retention")))
		}
		if se := mmap(mem, "semantic"); se != nil {
			fmt.Fprintf(&w, "- **Semantic** — %s. Keyed by %s. Rewritten when %s.\n",
				orNone(mstr(se, "record")), orNone(mstr(se, "keyed_by")), orNone(mstr(se, "updated_when")))
		}
	}
	if len(a.Remembers) > 0 {
		w.WriteString("\nMeasured recall ranking, from the corpus:\n\n")
		for _, m := range a.Remembers {
			fmt.Fprintf(&w, "- **%s** (%.0f%%)", m.Axis, m.Weight*100)
			if m.Why != "" {
				fmt.Fprintf(&w, " — %s", m.Why)
			}
			w.WriteString("\n")
		}
	}

	if schema := mobjs(a.Charter, "memory_schema"); len(schema) > 0 {
		w.WriteString("\n#### Memory schema\n\n")
		for _, s := range schema {
			fmt.Fprintf(&w, "`%s` (%s) — key `%s`\n\n", orNone(mstr(s, "name")), orNone(mstr(s, "store")), orNone(mstr(s, "key")))
			fields := mobjs(s, "fields")
			if len(fields) > 0 {
				w.WriteString("| field | type | holds |\n|---|---|---|\n")
				for _, f := range fields {
					fmt.Fprintf(&w, "| `%s` | %s | %s |\n", mstr(f, "name"), mstr(f, "type"), mstr(f, "note"))
				}
				w.WriteString("\n")
			}
			if ex := mmap(s, "example"); ex != nil {
				if blob, err := json.Marshal(ex); err == nil {
					fmt.Fprintf(&w, "Example row: `%s`\n\n", string(blob))
				}
			}
		}
	}

	section := func(title string, items []string) {
		if len(items) == 0 {
			return
		}
		fmt.Fprintf(&w, "\n#### %s\n\n", title)
		for _, it := range items {
			fmt.Fprintf(&w, "- %s\n", it)
		}
	}
	if sc := mmap(a.Spec, "scope"); sc != nil {
		section("Owns", mlist(sc, "owns"))
		section("Explicitly not", mlist(sc, "excluded"))
	}
	section("Tools", pairs(a.Spec, "tools", "name", "use", "access"))
	section("Triggers", pairs(a.Spec, "triggers", "when", "does", ""))
	section("Outputs", pairs(a.Spec, "outputs", "what", "to", ""))
	section("Handoffs", pairs(a.Spec, "handoffs", "to", "when", "payload"))
	section("Guardrails", mlist(a.Spec, "guardrails"))
	section("Evals", pairs(a.Spec, "evals", "name", "checks", ""))
	section("What it cannot learn from chat", pairs(a.Spec, "external_lookups", "what", "where", ""))
	section("Risks", pairs(a.Spec, "risks", "what", "mitigation", ""))
	section("First week", mlist(a.Charter, "first_week"))

	if sp := strings.TrimSpace(mstr(a.Charter, "system_prompt")); sp != "" {
		fmt.Fprintf(&w, "\n#### System prompt\n\n```\n%s\n```\n", sp)
	}

	if caps := ownedCaps(a, c); len(caps) > 0 {
		w.WriteString("\n#### Evidence\n\n")
		for _, cs := range caps {
			fmt.Fprintf(&w, "- `%s` — %d observations, spread %.2f\n", cs.Cap.ID, cs.Count, cs.Spread)
			for _, q := range cs.Quotes {
				fmt.Fprintf(&w, "  - %s\n", q)
			}
		}
	}
	return w.String()
}

func ownedCaps(a agentDoc, c census) []capStats {
	owned := map[string]bool{}
	for _, id := range a.Capabilities {
		owned[id] = true
	}
	var out []capStats
	for _, cs := range c.Capabilities {
		if owned[cs.Cap.ID] {
			out = append(out, cs)
		}
		if len(out) >= 6 {
			break
		}
	}
	return out
}

func sharePct(a agentDoc, c census) string {
	for _, p := range c.Proposal {
		if p.ID == a.ID {
			return fmt.Sprintf("%.0f%%", p.Share*100)
		}
	}
	owned, total := 0, 0
	set := map[string]bool{}
	for _, id := range a.Capabilities {
		set[id] = true
	}
	for _, cs := range c.Capabilities {
		total += cs.Count
		if set[cs.Cap.ID] {
			owned += cs.Count
		}
	}
	if total == 0 {
		return "—"
	}
	return fmt.Sprintf("%.0f%%", 100*float64(owned)/float64(total))
}

func mdAxes(ms []memoryAxis) string {
	if len(ms) == 0 {
		return "*nothing across days*"
	}
	parts := make([]string, 0, len(ms))
	for i, m := range ms {
		if i == 0 {
			parts = append(parts, fmt.Sprintf("**%s** %.0f%%", m.Axis, m.Weight*100))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %.0f%%", m.Axis, m.Weight*100))
	}
	return strings.Join(parts, " → ")
}

func anySynthesized(cs []capStats) bool {
	for _, c := range cs {
		if c.Cap.Synthesized {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// map[string]any accessors. Model output is JSON of a declared shape, but a
// missing field must render as an omission, never a panic.
// ---------------------------------------------------------------------------

func mstr(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	return str(m[key])
}

func mmap(m map[string]any, key string) map[string]any {
	if m == nil {
		return nil
	}
	out, _ := m[key].(map[string]any)
	return out
}

func mlist(m map[string]any, key string) []string {
	if m == nil {
		return nil
	}
	return stringsOf(m[key])
}

func mobjs(m map[string]any, key string) []map[string]any {
	if m == nil {
		return nil
	}
	items, _ := m[key].([]any)
	out := make([]map[string]any, 0, len(items))
	for _, it := range items {
		if o, ok := it.(map[string]any); ok {
			out = append(out, o)
		}
	}
	return out
}

// pairs flattens a list of objects into readable lines: "**head** — body (tail)".
func pairs(m map[string]any, key, head, body, tail string) []string {
	objs := mobjs(m, key)
	if len(objs) == 0 {
		return mlist(m, key)
	}
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		line := "**" + mstr(o, head) + "**"
		if v := mstr(o, body); v != "" {
			line += " — " + v
		}
		if tail != "" {
			if v := mstr(o, tail); v != "" {
				line += " (" + v + ")"
			}
		}
		out = append(out, line)
	}
	return out
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// runStats is the per-run line the blueprint and the UI carry. It reads the
// run's own report rather than a tally kept off the event stream: the report is
// what the engine actually recorded, and most events do not carry a pipeline
// name, so counting them by hand mis-attributes nearly every stage.
func runStats(name string, rep observe.RunReport) map[string]any {
	stages := append([]*observe.StageStats(nil), rep.Stages...)
	sort.Slice(stages, func(i, j int) bool { return stages[i].Stage < stages[j].Stage })
	rows := make([]map[string]any, 0, len(stages))
	for _, s := range stages {
		rows = append(rows, map[string]any{
			"stage": s.Stage, "tasks": s.Tasks, "calls": s.ModelCalls,
			"cache_hits": s.CacheHits, "retries": s.Retries, "cost_usd": s.Usage.CostUSD,
		})
	}
	u := rep.Totals()
	return map[string]any{
		"run": name, "run_id": rep.RunID, "cost_usd": u.CostUSD,
		"seconds":       rep.Finished.Sub(rep.Started).Seconds(),
		"input_tokens":  u.InputTokens,
		"output_tokens": u.OutputTokens,
		"stages":        rows,
	}
}
