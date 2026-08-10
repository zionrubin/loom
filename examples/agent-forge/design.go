package main

// Run 3 — one full agent design per agent on the roster.
//
//	agents ─ agent-spec ─ agent-charter
//
// Two chained calls rather than one: the operating spec (what it does, what it
// remembers, what it may touch) is a different kind of thinking from the
// charter (the memory schema it writes against and the system prompt it runs
// under), and asking for both in one response reliably produces a thin version
// of each. The charter reads the spec, so the chain is real.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

type agentDecl struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	Mission      string       `json:"mission"`
	Partition    partition    `json:"partition"`
	Remembers    []memoryAxis `json:"remembers"`
	Capabilities []string     `json:"capabilities"`
	Why          string       `json:"why"`
}

type rejection struct {
	Shape  string `json:"shape"`
	WhyNot string `json:"why_not"`
}

type rosterDecision struct {
	Topology string         `json:"topology"`
	Verdict  string         `json:"verdict"`
	Agents   []agentDecl    `json:"agents"`
	Shared   []sharedMemory `json:"shared_memory"`
	Rejected []rejection    `json:"rejected"`
}

// rosterFrom reads the roster stage's output, falling back to the computed
// proposal. The fallback is not decoration: it is what makes the whole pipeline
// still produce a usable design when the deep model is unavailable, over
// budget, or returns something malformed.
func rosterFrom(rec core.Record, c census) rosterDecision {
	var d rosterDecision
	if len(rec.Data) > 0 {
		blob, err := json.Marshal(rec.Data)
		if err == nil {
			_ = json.Unmarshal(blob, &d)
		}
	}
	if len(d.Agents) == 0 {
		d = rosterDecision{Topology: c.Topology.Recommended, Verdict: c.Topology.Rationale, Shared: c.Shared}
		for _, a := range c.Proposal {
			d.Agents = append(d.Agents, agentDecl{
				ID: a.ID, Name: a.Name, Mission: a.Why, Partition: a.Partition,
				Remembers: a.Remembers, Capabilities: a.Capabilities, Why: a.Why,
			})
		}
		for _, r := range c.Topology.Rejected {
			d.Rejected = append(d.Rejected, rejection{Shape: r.Shape, WhyNot: r.Reason})
		}
	}
	if d.Topology == "" {
		d.Topology = c.Topology.Recommended
	}
	if len(d.Shared) == 0 {
		d.Shared = c.Shared
	}
	// Normalise: an agent must never list its own partition axis as memory,
	// whatever the model returned. Inside one instance that axis is a constant.
	for i := range d.Agents {
		d.Agents[i].Remembers = normaliseMemory(d.Agents[i].Remembers, d.Agents[i].Partition.Axis)
		if d.Agents[i].Name == "" {
			d.Agents[i].Name = agentTitle(d.Agents[i].ID)
		}
		if d.Agents[i].Partition.Instances <= 0 {
			d.Agents[i].Partition.Instances = 1
		}
	}
	return d
}

func normaliseMemory(ms []memoryAxis, partitionAxis string) []memoryAxis {
	mix := map[string]float64{}
	why := map[string]string{}
	for _, m := range ms {
		a, ok := knownAxis(m.Axis)
		if !ok {
			continue
		}
		w := m.Weight
		if w <= 0 {
			w = 0.01
		}
		mix[a] += w
		if why[a] == "" {
			why[a] = m.Why
		}
	}
	out := rankAxes(mix, partitionAxis)
	for i := range out {
		out[i].Why = why[out[i].Axis]
	}
	return out
}

const specPrefix = `You are writing the operating specification for one agent in a system whose shape has already
been decided from measured evidence. You are given that decision, the counted evidence behind this
agent's work, and verbatim lines from the chat showing the work happening.

This is a full agent: it has its own memory, its own tools, its own triggers, and it runs
unattended between the moments a person looks at it. Design for that. The two questions that
matter most, in order:

  What does it remember, keyed on what? Its memory has a PRIMARY KEY — the thing it looks up
  first when work arrives. For an agent that tracks counterparties over months that is the
  partner; for one tuning spend that is the campaign. Secondary keys are what it cross-references.
  Get the primary key wrong and every recall is a scan.

  What must it never do alone? An agent that can change spend or write to a partner needs the
  boundary written down, not implied.

Separate the three memory horizons and say what actually goes in each:
  working    within one task, discarded after
  episodic   what happened, keyed and dated — the events it will need to recall
  semantic   what is true and stable — the entity profiles it maintains and updates

Ground everything in the evidence you are given. Where the corpus does not say, write the gap into
external_lookups rather than inventing it. Do not invent tools that were never named.

Respond with a single JSON object and nothing else:
{"mission": "<one sentence: what it is accountable for>",
 "scope": {"owns": ["<the jobs it does end to end>"], "excluded": ["<nearby work it must not take>"]},
 "memory": {"primary_key": "<axis>", "secondary_keys": ["<axis>"],
            "working": "<what it holds inside one task>",
            "episodic": {"record": "<one event, described as a row>", "keyed_by": "<axis + date>", "retention": "<how long and why>"},
            "semantic": {"record": "<one entity profile, described as a row>", "keyed_by": "<axis>", "updated_when": "<what triggers a rewrite>"},
            "why_this_key": "<the recall pattern that forces this primary key>"},
 "tools": [{"name": "<system named in the evidence>", "use": "<what it does with it>", "access": "read|write|propose_only"}],
 "triggers": [{"when": "<schedule, event or request>", "does": "<what it runs>"}],
 "outputs": [{"what": "<artefact or message>", "to": "<who reads it>"}],
 "handoffs": [{"to": "<agent id or human role>", "when": "<condition>", "payload": "<what travels>"}],
 "guardrails": ["<a boundary, stated as a rule>"],
 "evals": [{"name": "<short>", "checks": "<what would prove it is working>"}],
 "external_lookups": [{"what": "<data the chat cannot give it>", "where": "<system to pull it from>"}],
 "risks": [{"what": "<failure mode>", "mitigation": "<what contains it>"}]}`

const charterPrefix = `You are turning an agent's operating specification into the two artefacts an engineer needs to
build it: the memory schema it writes against, and the system prompt it runs under.

The schema is concrete. Real field names, real types, one worked example row per store filled in
with plausible values drawn from the evidence — not placeholders. If the spec says the agent is
keyed on partner, the key column is named and typed and appears in the example.

The system prompt is the agent's own, written in the second person, and it must encode the memory
discipline: what to read before acting, what to write after, what never to write. It is not a
description of the agent — it is the text the agent runs on. Keep it under 350 words.

Respond with a single JSON object and nothing else:
{"memory_schema": [{"store": "episodic|semantic",
                    "name": "<table or collection name>",
                    "key": "<primary key column>",
                    "fields": [{"name": "<column>", "type": "<string|date|number|enum|text|json>", "note": "<what it holds>"}],
                    "example": {"<column>": "<value>"}}],
 "system_prompt": "<the agent's system prompt, second person>",
 "first_week": ["<what to run in week one to prove or kill this agent, in order>"]}`

// buildDesignPipeline is run 3: a spec and a charter for every agent, fanned
// out one task per agent.
func buildDesignPipeline(d rosterDecision, c census, profiles map[string]string, lu lineup, workers int) *pipeline.Pipeline {
	p := pipeline.New("agent-design")

	recs := make([]core.Record, 0, len(d.Agents))
	for _, a := range d.Agents {
		recs = append(recs, core.NewRecord("agent-"+a.ID, map[string]any{
			"agent_id": a.ID,
			"name":     a.Name,
			"topology": d.Topology,
			"verdict":  d.Verdict,
			"dossier":  agentDossier(a, d, c, profiles),
		}))
	}

	specs := p.FromRecords("agents", recs).
		Infer("agent-spec", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierDeep},
			System:    "You specify autonomous agents. You answer only with JSON.",
			Prefix:    specPrefix,
			Prompt:    "{{.dossier}}",
			MaxTokens: 3000,
			ParseJSON: true,
			Validate: func(r core.Record) error {
				mem, _ := r.Data["memory"].(map[string]any)
				if mem == nil || str(mem["primary_key"]) == "" {
					return fmt.Errorf("memory.primary_key is required")
				}
				return nil
			},
		}, pipeline.WithParallelism(workers))

	specs.Map("spec-json", func(r core.Record) (core.Record, error) {
		out := r.Clone()
		keep := map[string]any{}
		for _, k := range []string{"mission", "scope", "memory", "tools", "triggers", "outputs", "handoffs", "guardrails", "evals", "external_lookups", "risks"} {
			if v, ok := r.Data[k]; ok {
				keep[k] = v
			}
		}
		blob, err := json.MarshalIndent(keep, "", "  ")
		if err != nil {
			return core.Record{}, err
		}
		out.Data["spec_json"] = string(blob)
		return out, nil
	}, pipeline.WithVersion("v1")).
		Infer("agent-charter", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierBalanced, Escalation: []string{lu.deep}},
			System:    "You turn agent specifications into memory schemas and system prompts. You answer only with JSON.",
			Prefix:    charterPrefix,
			Prompt:    "Agent \"{{.name}}\" ({{.agent_id}}), in a {{.topology}} system.\n\nSpecification:\n{{.spec_json}}",
			MaxTokens: 2500,
			ParseJSON: true,
			Validate: func(r core.Record) error {
				if strings.TrimSpace(str(r.Data["system_prompt"])) == "" {
					return fmt.Errorf("system_prompt is empty")
				}
				return nil
			},
		}, pipeline.WithParallelism(workers))

	return p
}

// agentDossier is everything one agent's designer gets: its slot in the decided
// system, the capabilities it owns with their measured numbers, the memory
// ranking those numbers produced, and the lines from the chat behind them.
func agentDossier(a agentDecl, d rosterDecision, c census, profiles map[string]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "SYSTEM SHAPE: %s\n%s\n\n", strings.ToUpper(d.Topology), d.Verdict)

	fmt.Fprintf(&b, "THIS AGENT\nid: %s\nname: %s\nmission: %s\npartition: %s\nrationale: %s\n",
		a.ID, a.Name, orNone(a.Mission), a.Partition.String(), orNone(a.Why))
	if a.Partition.Axis != "global" && len(a.Partition.Keys) > 0 {
		fmt.Fprintf(&b, "instance keys: %s\n", strings.Join(a.Partition.Keys, ", "))
		fmt.Fprintf(&b, "(one instance handles ONE of these; %q is constant inside an instance and is not memory)\n", a.Partition.Axis)
	}
	fmt.Fprintf(&b, "measured recall ranking: %s\n", axisList(a.Remembers))
	for _, m := range a.Remembers {
		if m.Why != "" {
			fmt.Fprintf(&b, "  %s — %s\n", m.Axis, m.Why)
		}
	}

	owned := map[string]bool{}
	for _, id := range a.Capabilities {
		owned[id] = true
	}
	b.WriteString("\nCAPABILITIES IT OWNS  (count · spread · cadence-bearing evidence)\n")
	var quotes []string
	tools := map[string]bool{}
	n := 0
	for _, cs := range c.Capabilities {
		if !owned[cs.Cap.ID] {
			continue
		}
		n++
		fmt.Fprintf(&b, "  %s — %s [%s] n=%d spread=%.2f remembers: %s\n",
			cs.Cap.ID, orNone(cs.Cap.Summary), cs.Cap.Function, cs.Count, cs.Spread, axisList(cs.Remembers))
		for _, t := range cs.Tools {
			tools[t] = true
		}
		quotes = append(quotes, cs.Quotes...)
	}
	if n == 0 {
		b.WriteString("  (none matched by id — design from the mission and the ranking above)\n")
	}
	if len(tools) > 0 {
		names := make([]string, 0, len(tools))
		for t := range tools {
			names = append(names, t)
		}
		// Sorted, because this line goes into the prompt and the prompt is the
		// cache key: map order would make one agent miss the cache on every
		// re-run and come back with its tools shuffled.
		sort.Strings(names)
		fmt.Fprintf(&b, "\nSYSTEMS NAMED IN THIS WORK: %s\n", strings.Join(names, ", "))
	}

	if len(d.Agents) > 1 {
		b.WriteString("\nOTHER AGENTS IT MUST NOT DUPLICATE\n")
		for _, o := range d.Agents {
			if o.ID == a.ID {
				continue
			}
			fmt.Fprintf(&b, "  %s (%s) — %s | remembers %s\n", o.ID, o.Partition.String(), orNone(o.Mission), axisList(o.Remembers))
		}
	}
	if len(d.Shared) > 0 {
		b.WriteString("\nSHARED MEMORY IT READS RATHER THAN COPIES\n")
		for _, s := range d.Shared {
			if contains(s.Readers, a.ID) || len(s.Readers) == 0 {
				fmt.Fprintf(&b, "  %s ledger — shared with %s\n", s.Axis, strings.Join(s.Readers, ", "))
			}
		}
	}

	// One space profile for texture: how the work actually reads day to day.
	if len(profiles) > 0 {
		key := ""
		if a.Partition.Axis == "vertical" && len(a.Partition.Keys) > 0 {
			key = a.Partition.Keys[0]
		} else {
			for _, s := range c.Spaces {
				if profiles[s] != "" {
					key = s
					break
				}
			}
		}
		if prof := profiles[key]; prof != "" {
			fmt.Fprintf(&b, "\nHOW WORK READS IN %q\n%s\n", key, trunc(prof, 1800))
		}
	}

	if len(quotes) > 0 {
		b.WriteString("\nEVIDENCE  (verbatim, pseudonymised)\n")
		if len(quotes) > 14 {
			quotes = quotes[:14]
		}
		for _, q := range quotes {
			fmt.Fprintf(&b, "  %s\n", trunc(q, 200))
		}
	}
	return b.String()
}
