package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

// Three pipelines, three runs, one artifact.
//
//	game-design  brief ─ concept ─ [modules] ─ feasible ─ spec ─ seal ─ design-doc
//	game-build   specs ─ implement ─ lint ─┬─ review
//	                                       └─ build-notes
//	game-ship    modules ─ link ─ banner ─ collect ─ weave ─ title-card ─ emit
//
// They are separate runs because loom DAGs fan out and do not fan back in, and
// because each one answers a question the next one needs answered: what are the
// modules, what is in them, and what does the bundle look like. Pointed at one
// constellation handler they become one universe — press `u` and all three
// skies are there, each still enterable after the next has started.

// lineup names the three concrete models a run binds to. Tiers cover the base
// binding; escalation ladders need explicit IDs, and those differ between the
// offline studio and a real provider.
type lineup struct{ fast, balanced, deep string }

// --- run 1: design ----------------------------------------------------------

// briefRecord is the single record the whole forge starts from: a pitch, the
// constraints the engine imposes, and the module set the shell requires.
func briefRecord(pitch string) core.Record {
	return core.NewRecord("brief", map[string]any{
		"pitch":    pitch,
		"required": strings.Join(coreModuleIDs(), ", "),
		"shape":    "one canvas, one keyboard, no assets, no network, no build step",
	})
}

// buildDesign plans the game: one deep-model call turns the pitch into a module
// breakdown, a pure stage explodes it into one record per module, an infeasible
// module is cut before anything is spent on it, and every surviving module gets
// its own API specification task.
func buildDesign(pitch string, lu lineup) *pipeline.Pipeline {
	p := pipeline.New("game-design")
	src := p.FromRecords("brief", []core.Record{briefRecord(pitch)})

	// The one call in this run that has to hold the whole game in its head.
	concept := src.Infer("concept", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierDeep},
		System: "You are the technical director of a small game studio. You break a pitch " +
			"into modules that independent engineers can implement in parallel without " +
			"talking to each other.",
		Prompt: `Design the module breakdown for a browser game.

Pitch: {{.pitch}}
Shape: {{.shape}}

The engine contract every module must obey:
{{broadcastJSON "engine-contract"}}

The art direction:
{{broadcastJSON "art-direction"}}

Your breakdown MUST include exactly these core modules, and may add at most two
of your own: {{.required}}

For each module give the capabilities it needs from the "needs" vocabulary
["canvas2d", "keyboard", "webaudio", "requestAnimationFrame", "network", "storage"].
Modules needing a capability the contract does not grant will be cut, so be honest.

Return JSON:
{"title": "<the game's name, uppercase, two words>",
 "tagline": "<six words or fewer>",
 "loop": "<the core loop in one sentence>",
 "modules": [{"id": "<lowercase identifier>", "role": "<one line>",
              "uses": ["<other module ids>"], "needs": ["<capabilities>"]}]}`,
		MaxTokens: 2000,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			mods, _ := r.Data["modules"].([]any)
			if len(mods) < len(coreModuleIDs()) {
				return fmt.Errorf("breakdown has %d modules, want at least %d", len(mods), len(coreModuleIDs()))
			}
			if strings.TrimSpace(r.String("title")) == "" {
				return fmt.Errorf("no title")
			}
			return nil
		},
	}, pipeline.WithBroadcast("engine-contract", "art-direction"))

	// Pure Go from here to the next model call: the breakdown becomes one
	// record per module, and the planner fuses the expansion and the cut into
	// a single task boundary.
	proposed := concept.FlatMap("modules", func(r core.Record) ([]core.Record, error) {
		mods, _ := r.Data["modules"].([]any)
		out := make([]core.Record, 0, len(mods))
		for i, m := range mods {
			fields, _ := m.(map[string]any)
			id, _ := fields["id"].(string)
			if id == "" {
				continue
			}
			role, _ := fields["role"].(string)
			needs := stringList(fields["needs"])
			if len(needs) == 0 {
				needs = []string{"canvas2d"}
			}
			out = append(out, core.NewRecord("mod-"+id, map[string]any{
				"id":      id,
				"role":    role,
				"uses":    joinStrings(fields["uses"], ", "),
				"needs":   needs,
				"core":    moduleGraph[id].ID != "",
				"seq":     i,
				"title":   r.String("title"),
				"tagline": r.String("tagline"),
				"loop":    r.String("loop"),
			}))
		}
		return out, nil
	}, pipeline.WithVersion("v1"))

	// Least privilege, applied to scope: a module that needs a capability the
	// engine contract does not grant is cut here, before a single token is
	// spent specifying or implementing it.
	feasible := proposed.Filter("feasible", func(r core.Record) (bool, error) {
		caps := contractCapabilities()
		for _, need := range stringList(r.Data["needs"]) {
			if !caps[need] {
				return false, nil
			}
		}
		return true, nil
	}, pipeline.WithVersion("v1"))

	// One task per module: the cheap tier writes the API, and a specification
	// too vague to implement is a semantic failure that climbs to the deep model.
	spec := feasible.Infer("spec", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast, Escalation: []string{lu.deep}},
		System: "You write module specifications precise enough that an engineer who " +
			"cannot see any other module can implement this one correctly.",
		Prompt: `Specify one module of the engine.

MODULE: {{.id}}
Role: {{.role}}
Calls into: {{.uses}}
Game: {{.title}} — {{.tagline}}
Core loop: {{.loop}}

The shared contract:
{{broadcastJSON "engine-contract"}}

Return JSON:
{"api": "<the exported functions, comma separated, with argument names>",
 "notes": "<implementation guidance: state to hold, units, edge cases — 2-4 sentences>",
 "accept": ["<up to 3 checks a reviewer can run against the source>"]}`,
		MaxTokens: 700,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			if len(strings.TrimSpace(r.String("api"))) < 8 {
				return fmt.Errorf("module %s: api too thin to implement: %q", r.String("id"), r.String("api"))
			}
			return nil
		},
	}, pipeline.WithBroadcast("engine-contract"), pipeline.WithParallelism(6))

	// Pure: fold each spec into the one line the design doc aggregates.
	sealed := spec.Map("seal", func(r core.Record) (core.Record, error) {
		out := r.Clone()
		out.Data["symbol"] = "LOOM." + r.String("id")
		out.Data["accept_line"] = joinStrings(r.Data["accept"], "; ")
		out.Data["spec_line"] = fmt.Sprintf("%s — %s\n  api: %s\n  notes: %s",
			r.String("id"), r.String("role"), r.String("api"), r.String("notes"))
		return out, nil
	}, pipeline.WithVersion("v1"))

	sealed.ReduceAI("design-doc", pipeline.ReduceAISpec{
		Binding:   model.Binding{Tier: model.TierBalanced},
		System:    "You write the design document a studio builds from.",
		ItemField: "spec_line",
		FanIn:     5,
		MaxTokens: 1200,
		Prompt: `Summarize {{.Count}} module specifications into the design document section for them:

{{range .Items}}- {{.}}
{{end}}
Write markdown: a paragraph on how these modules fit together, then a bullet per
module with its responsibility. No preamble.`,
	})

	return p
}

// --- run 2: build -----------------------------------------------------------

// buildBuild implements the design. Every module is its own task, isolated from
// every other: the only thing they share is the contract, which rides in as the
// stage's prompt prefix — rendered once per task rather than once per record,
// and served by the provider's prompt cache on every call after the first.
func buildBuild(specs []core.Record, lu lineup) *pipeline.Pipeline {
	p := pipeline.New("game-build")
	src := p.FromRecords("specs", specs)

	coded := src.Infer("implement", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierBalanced, Escalation: []string{lu.deep}},
		System: "You are a senior engineer writing one module of a browser game engine. " +
			"You output source code and nothing else.",
		// No record data is in scope here, which is exactly what makes it
		// shareable: every call in this stage opens with these identical bytes.
		Prefix: `ENGINE CONTRACT — identical for every module in this build:
{{broadcastJSON "engine-contract"}}

ART DIRECTION:
{{broadcastJSON "art-direction"}}

THE FULL MODULE MAP — call siblings through the namespace, never redefine them:
{{broadcastJSON "module-graph"}}

`,
		Prompt: `Implement this module. Output ES5 browser JavaScript only.

MODULE: {{.id}}
Role: {{.role}}
Exported API: {{.api}}
Calls into: {{.uses}}
Notes: {{.notes}}
Acceptance: {{.accept_line}}
Game: {{.title}} — {{.tagline}}

The whole module is one IIFE assigning exactly one namespace key:
(function (G) { G.{{.id}} = { ... }; })(window.LOOM);`,
		MaxTokens:   4096,
		OutputField: "code",
		// The semantic gate. A truncated or off-contract module is not a
		// transport failure — the call succeeded — so retrying the same model
		// is a coin flip; the run climbs to the deep model instead.
		Validate: func(r core.Record) error {
			id, src := r.String("id"), stripFences(r.String("code"))
			switch {
			case len(src) < 120:
				return fmt.Errorf("module %s: %d bytes is not a module", id, len(src))
			case !strings.Contains(src, "G."+id+" =") && !strings.Contains(src, "LOOM."+id+" ="):
				return fmt.Errorf("module %s: source never assigns G.%s", id, id)
			case !balanced(src):
				return fmt.Errorf("module %s: unbalanced brackets (truncated response?)", id)
			}
			return nil
		},
	},
		pipeline.WithBroadcast("engine-contract", "art-direction", "module-graph"),
		pipeline.WithParallelism(6))

	// The lint stage reads the same contract the prompt prefix rendered — one
	// shared value, one content hash, two readers with nothing else in common.
	linted := coded.MapTools("lint", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		contract, err := core.BroadcastAs[map[string]any](ctx, s, "engine-contract")
		if err != nil {
			return core.Record{}, err
		}
		src := stripFences(r.String("code"))
		var findings []string
		for _, bad := range stringList(contract["forbidden"]) {
			if strings.Contains(src, bad) {
				findings = append(findings, "forbidden "+strings.TrimSuffix(bad, "("))
			}
		}
		if !balanced(src) {
			findings = append(findings, "unbalanced brackets")
		}
		id := r.String("id")
		if !strings.Contains(src, "G."+id+" =") && !strings.Contains(src, "LOOM."+id+" =") {
			findings = append(findings, "missing namespace assignment")
		}
		lint := "clean"
		if len(findings) > 0 {
			lint = strings.Join(findings, "; ")
		}
		out := r.Clone()
		out.Data["code"] = src
		out.Data["bytes"] = len(src)
		out.Data["lines"] = strings.Count(src, "\n") + 1
		out.Data["lint"] = lint
		out.Data["change_line"] = fmt.Sprintf("%s (%s): %d lines, lint %s — %s",
			id, r.String("symbol"), out.Data["lines"], lint, r.String("role"))
		return out, nil
	}, pipeline.WithVersion("v2"), pipeline.WithBroadcast("engine-contract"))

	// Branch A: a cheap read of every module against its own acceptance
	// criteria. It is deliberately off the critical path — a review that
	// dead-letters costs the build nothing but the review.
	linted.Infer("review", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast},
		System:  "You review one module against its stated acceptance criteria.",
		Prompt: `Review this module.

MODULE: {{.id}}
Acceptance: {{.accept_line}}
Lint: {{.lint}}

{{.code}}

Return JSON: {"verdict": "ship" | "revise", "risk": 1-5, "note": "<one sentence>"}`,
		MaxTokens: 400,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			if v := r.String("verdict"); v != "ship" && v != "revise" {
				return fmt.Errorf("module %s: verdict %q is neither ship nor revise", r.String("id"), v)
			}
			return nil
		},
	}, pipeline.WithParallelism(6))

	// Branch B: the build log, aggregated up a tree.
	linted.ReduceAI("build-notes", pipeline.ReduceAISpec{
		Binding:   model.Binding{Tier: model.TierFast},
		System:    "You write terse build logs.",
		ItemField: "change_line",
		FanIn:     6,
		MaxTokens: 700,
		Prompt: `Summarize {{.Count}} completed modules into a build log entry:

{{range .Items}}- {{.}}
{{end}}
One short paragraph, then one line per module. No preamble.`,
	})

	return p
}

// --- run 3: ship ------------------------------------------------------------

// buildShip links what the build run produced into the artifact: order the
// modules against the engine's own dependency table, band each one with its
// provenance, fold them into one bundle, ask for a title card, and emit the
// single HTML file that is the deliverable.
func buildShip(modules []core.Record, lu lineup, manifest func() map[string]any) *pipeline.Pipeline {
	p := pipeline.New("game-ship")
	src := p.FromRecords("modules", modules)

	// The broadcast join: link order is a property of the engine, not of any
	// one module, so it lives in a table every task reads and none carries.
	linked := src.MapTools("link", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		graph, err := core.BroadcastAs[map[string]module](ctx, s, "module-graph")
		if err != nil {
			return core.Record{}, err
		}
		out := r.Clone()
		id := r.String("id")
		if m, ok := graph[id]; ok {
			out.Data["order"] = m.Order
			out.Data["core"] = true
		} else {
			// An extra module the design proposed: keep it, link it after the
			// engine it extends, and keep the order deterministic.
			out.Data["order"] = 900 + len(id)
			out.Data["core"] = false
		}
		return out, nil
	}, pipeline.WithVersion("v1"), pipeline.WithBroadcast("module-graph"))

	// The band carries only what is a property of the module itself. Which
	// model wrote it and what that cost are properties of *this* run, and
	// stamping those into a cached stage's output would make the bundle differ
	// on every replay — so they go in the manifest, which the uncached emit
	// stage below writes fresh each time.
	banded := linked.Map("banner", func(r core.Record) (core.Record, error) {
		out := r.Clone()
		out.Data["unit"] = fmt.Sprintf("/* %s — %s\n   %d bytes · lint %s */\n%s\n",
			r.String("id"), r.String("role"), num(r.Data["bytes"]), r.String("lint"), r.String("code"))
		return out, nil
	}, pipeline.WithVersion("v1"))

	// Combine is a pairwise fold with no model in it: gather the units, then
	// linearize them deterministically in the stage below.
	collected := banded.Combine("collect", func(a, b core.Record) (core.Record, error) {
		units := append(unitsOf(a), unitsOf(b)...)
		out := core.NewRecord("bundle", map[string]any{
			"units":   units,
			"title":   firstNonEmpty(a.String("title"), b.String("title")),
			"tagline": firstNonEmpty(a.String("tagline"), b.String("tagline")),
			"loop":    firstNonEmpty(a.String("loop"), b.String("loop")),
		})
		return out, nil
	})

	woven := collected.Map("weave", func(r core.Record) (core.Record, error) {
		units := unitsOf(r)
		sort.SliceStable(units, func(i, j int) bool { return orderOf(units[i]) < orderOf(units[j]) })

		var b strings.Builder
		ids := make([]string, 0, len(units))
		lines := make([]string, 0, len(units))
		for _, u := range units {
			m, _ := u.(map[string]any)
			b.WriteString(fmt.Sprint(m["unit"]))
			b.WriteString("\n")
			id := fmt.Sprint(m["id"])
			ids = append(ids, id)
			lines = append(lines, fmt.Sprintf("%s: %s", id, m["role"]))
		}
		code := b.String()

		out := r.Clone()
		delete(out.Data, "units")
		out.Data["code"] = code
		out.Data["ids"] = ids
		out.Data["count"] = len(ids)
		out.Data["bytes"] = len(code)
		out.Data["summary"] = strings.Join(lines, "\n")
		return out, nil
	}, pipeline.WithVersion("v1"))

	// The one model call in the ship run, and the only one that sees the whole
	// game at once. It reads the module summary, not the bundle: a title card
	// does not need 70KB of source to be written, and the projection says so.
	carded := woven.Infer("title-card", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierDeep},
		System:  "You write the title screen of an arcade game.",
		Prompt: `Compose the title card for a finished game.

Working title: {{.title}}
Tagline: {{.tagline}}
Core loop: {{.loop}}
It shipped as {{.count}} modules:
{{.summary}}

Art direction:
{{broadcastJSON "art-direction"}}

The controls are: rotate with A/D or Left/Right, thrust with W or Up,
fire with Space, release the weave pulse with Z when the meter is full.

Return JSON:
{"title": "<the game's name, uppercase>",
 "tagline": "<six words or fewer, shown under the title>",
 "howto": ["<one control line>", "<one control line>", "<one line of strategy>"]}`,
		MaxTokens: 500,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			if strings.TrimSpace(r.String("title")) == "" {
				return fmt.Errorf("title card has no title")
			}
			if len(stringList(r.Data["howto"])) == 0 {
				return fmt.Errorf("title card has no instructions")
			}
			return nil
		},
	}, pipeline.WithBroadcast("art-direction"))

	// Emit is deliberately dumb: every decision was made upstream, so shipping
	// is substitution. It is also the one stage with no Version — the manifest
	// it stamps in describes this run, so caching it would be a lie.
	carded.Map("emit", func(r core.Record) (core.Record, error) {
		card := map[string]any{
			"title":   r.String("title"),
			"tagline": r.String("tagline"),
			"howto":   stringList(r.Data["howto"]),
		}
		cardJSON, err := json.Marshal(card)
		if err != nil {
			return core.Record{}, err
		}
		man := manifest()
		manJSON, err := json.MarshalIndent(man, "", " ")
		if err != nil {
			return core.Record{}, err
		}
		badge := fmt.Sprintf("%d runs · %d tasks · %d model calls · %s",
			man["runs_count"], man["tasks"], man["calls"], usd(man["cost"]))

		html := shell
		for placeholder, value := range map[string]string{
			"__LOOM_TITLE__":    r.String("title"),
			"__LOOM_TAGLINE__":  r.String("tagline"),
			"__LOOM_BADGE__":    badge,
			"__LOOM_CARD__":     string(cardJSON),
			"__LOOM_MANIFEST__": string(manJSON),
			"__LOOM_BUNDLE__":   r.String("code"),
			// Stamped by main after this run ends: a run cannot report its own
			// totals from inside itself.
			"__LOOM_SHIP__": "null",
		} {
			html = strings.ReplaceAll(html, placeholder, value)
		}

		out := core.NewRecord("index.html", map[string]any{
			"html":    html,
			"bytes":   len(html),
			"title":   r.String("title"),
			"tagline": r.String("tagline"),
			"modules": r.Data["ids"],
		})
		return out, nil
	})

	return p
}

// --- small helpers ----------------------------------------------------------

func unitsOf(r core.Record) []any {
	if us, ok := r.Data["units"].([]any); ok {
		return us
	}
	// A leaf of the fold: the record is itself one unit.
	return []any{map[string]any{
		"id":    r.String("id"),
		"role":  r.String("role"),
		"order": r.Data["order"],
		"unit":  r.String("unit"),
	}}
}

func orderOf(u any) int {
	m, _ := u.(map[string]any)
	switch v := m["order"].(type) {
	case int:
		return v
	case float64:
		return int(v)
	}
	return 999
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func usd(v any) string {
	f, _ := v.(float64)
	return fmt.Sprintf("$%.4f", f)
}
