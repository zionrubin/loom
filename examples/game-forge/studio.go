package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
)

// The offline studio: three scripted mock models standing in for a real
// provider, priced like one, so `go run ./examples/game-forge` produces a
// playable game with no key, no network, and zero real cost.
//
// The script is not just "return something plausible". It is written to put
// every recovery path in the framework on screen in one build:
//
//	shards     first implementation comes back truncated → semantic failure →
//	           the task climbs the escalation ladder to the deep model
//	audio      comes back wrapped in a markdown fence with prose around it →
//	           the lint stage strips it, as it must for real chat models
//	game       takes ~5s to write → a growing star with an activity ring
//	spec/review scattered 429s and a 503 → retry orbits in two different runs
//	motes      its review trips a content filter → a dead letter that costs
//	           the build nothing, because review is off the critical path
//
// Everything else is deterministic and keyed to the module ID, so reruns are
// cache-stable and takes are repeatable.

// mockStudio registers the three scripted models. Pricing is fictional but
// shaped like the real thing, which is what makes the cost column mean
// something in the constellation view.
func mockStudio() (*model.Registry, error) { return mockStudioPaced(1) }

// mockStudioPaced is mockStudio with the simulated "thinking" time scaled.
// Latency is what makes the sky move — stars that pulse, a straggler that
// grows a ring — so the demo wants it and the test does not.
func mockStudioPaced(pace float64) (*model.Registry, error) {
	reg := model.NewRegistry()
	think := func(ms int) {
		if d := time.Duration(float64(ms) * pace * float64(time.Millisecond)); d > 0 {
			time.Sleep(d)
		}
	}
	var attempts sync.Map // module ID → implement attempts, for the escalation script

	scoutFailures := make([]error, 24)
	scoutFailures[5] = core.Transient(errors.New("429: rate limited (scripted)"))
	scoutFailures[14] = core.Transient(errors.New("503: upstream connection reset (scripted)"))

	scout := model.NewMock("studio-scout",
		model.WithFailures(scoutFailures...),
		model.WithHandler(func(req model.Request) (string, error) {
			think(220 + rand.Intn(380))
			if strings.HasPrefix(req.Prompt, "Review this module") &&
				moduleOf(req.Prompt) == "motes" {
				return "", core.Permanent(errors.New("response blocked: source contains a pattern the safety filter rejects (scripted)"))
			}
			return respond(req, &attempts, "studio-scout")
		}))
	if err := reg.Register(model.Info{
		ID: "studio-scout", Provider: scout, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 0.20, OutputPerMTok: 1.25},
	}); err != nil {
		return nil, err
	}

	artisanFailures := make([]error, 10)
	artisanFailures[3] = core.Transient(errors.New("429: rate limited (scripted)"))

	artisan := model.NewMock("studio-artisan",
		model.WithFailures(artisanFailures...),
		model.WithHandler(func(req model.Request) (string, error) {
			// Writing code is slower than reading it, and the biggest module
			// is slower still — the straggler the activity ring is for.
			d := 600 + rand.Intn(700)
			if moduleOf(req.Prompt) == "game" {
				d = 5200 + rand.Intn(600) // the straggler
			}
			think(d)
			return respond(req, &attempts, "studio-artisan")
		}))
	if err := reg.Register(model.Info{
		ID: "studio-artisan", Provider: artisan, Tier: model.TierBalanced,
		Pricing: model.Pricing{InputPerMTok: 0.75, OutputPerMTok: 4.50},
	}); err != nil {
		return nil, err
	}

	master := model.NewMock("studio-master",
		model.WithHandler(func(req model.Request) (string, error) {
			think(900 + rand.Intn(800))
			return respond(req, &attempts, "studio-master")
		}))
	if err := reg.Register(model.Info{
		ID: "studio-master", Provider: master, Tier: model.TierDeep,
		Pricing: model.Pricing{InputPerMTok: 2.50, OutputPerMTok: 15.00},
	}); err != nil {
		return nil, err
	}
	return reg, nil
}

// lineupOf reads the three tier defaults out of a registry, so the pipelines
// can name an escalation target without knowing which provider is behind it.
func lineupOf(reg *model.Registry) (lineup, error) {
	var lu lineup
	for _, t := range []struct {
		tier model.Tier
		dst  *string
	}{{model.TierFast, &lu.fast}, {model.TierBalanced, &lu.balanced}, {model.TierDeep, &lu.deep}} {
		info, err := reg.ForTier(t.tier)
		if err != nil {
			return lu, fmt.Errorf("tier %q: %w", t.tier, err)
		}
		*t.dst = info.ID
	}
	return lu, nil
}

// respond is the shared deterministic "model". Every prompt in the forge opens
// with a distinct verb and (where it is about one module) carries a MODULE:
// line, which is all the routing this needs.
func respond(req model.Request, attempts *sync.Map, who string) (string, error) {
	p := req.Prompt
	switch {
	case strings.HasPrefix(p, "Design the module breakdown"):
		return conceptJSON()

	case strings.HasPrefix(p, "Specify one module"):
		return specJSON(moduleOf(p))

	case strings.HasPrefix(p, "Implement this module"):
		return implement(moduleOf(p), attempts, who)

	case strings.HasPrefix(p, "Review this module"):
		return reviewJSON(moduleOf(p))

	case strings.HasPrefix(p, "Summarize") && strings.Contains(p, "module specifications"):
		return designDoc(p), nil

	case strings.HasPrefix(p, "Summarize") && strings.Contains(p, "completed modules"):
		return buildLog(p), nil

	case strings.HasPrefix(p, "Summarize"):
		return "Summary of " + fmt.Sprint(len(reduceItems(p, ""))) + " items.", nil

	case strings.HasPrefix(p, "Compose the title card"):
		return titleCardJSON()
	}
	return "Acknowledged.", nil
}

// moduleOf pulls the module ID out of the "MODULE: <id>" line every
// per-module prompt carries.
func moduleOf(prompt string) string {
	i := strings.Index(prompt, "MODULE: ")
	if i < 0 {
		return ""
	}
	rest := prompt[i+len("MODULE: "):]
	if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
		rest = rest[:nl]
	}
	return strings.TrimSpace(rest)
}

// conceptJSON is the technical director's breakdown: the twelve core modules
// the engine contract requires, plus one perfectly reasonable idea — an online
// leaderboard — that the feasibility filter will cut, because the contract
// grants no network capability and this run is not going to pay to implement
// something it cannot ship.
func conceptJSON() (string, error) {
	type conceptModule struct {
		ID    string   `json:"id"`
		Role  string   `json:"role"`
		Uses  []string `json:"uses"`
		Needs []string `json:"needs"`
	}
	mods := make([]conceptModule, 0, len(moduleGraph)+1)
	for _, m := range coreModules() {
		needs := []string{"canvas2d"}
		switch m.ID {
		case "input":
			needs = []string{"keyboard"}
		case "audio":
			needs = []string{"webaudio"}
		case "game":
			needs = []string{"canvas2d", "keyboard", "requestAnimationFrame"}
		case "vec":
			needs = []string{}
		}
		mods = append(mods, conceptModule{ID: m.ID, Role: m.Role, Uses: m.Uses, Needs: needs})
	}
	mods = append(mods, conceptModule{
		ID:    "netplay",
		Role:  "global leaderboard: post each run's score and show the top ten on the title screen",
		Uses:  []string{"hud"},
		Needs: []string{"network", "storage"},
	})

	out, err := json.Marshal(map[string]any{
		"title":   "CONSTELLATION DRIFT",
		"tagline": "cut the dark, weave the light",
		"loop": "fly a shuttle through drifting shards, cut them into smaller ones, " +
			"collect the motes they shed, and spend a full charge on a weave pulse that clears the field",
		"modules": mods,
	})
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// specJSON writes the specification for one module out of the engine's own
// module table — the answer key a real model would have to derive.
func specJSON(id string) (string, error) {
	m, ok := moduleGraph[id]
	if !ok {
		m = module{ID: id, Role: "extra module proposed by the design run", API: "update(w), draw(ctx,w)"}
	}
	notes := map[string]string{
		"vec":       "Hold the RNG seed in a closure so a reload replays the same sky. All distances are squared where possible; wrap() takes a padding so large bodies do not pop at the edge.",
		"input":     "Keep two maps: held keys and this frame's fresh presses. endFrame() clears the second one, so hit() means 'pressed since the last frame' and never fires twice.",
		"audio":     "Create the AudioContext lazily on first unlock() — browsers refuse one before a gesture. Synthesize every sound; the bundle ships no asset files.",
		"starfield": "Three layers at different speeds and alphas; recycle a star to the top edge instead of allocating a new one.",
		"particles": "One flat array, splice on death, cap the pool so a chain reaction cannot stall the frame. Draw additively.",
		"bullets":   "Each thread carries a lifetime rather than a range so wrapping cannot make it immortal. Draw it as a short segment along its own velocity.",
		"shards":    "Generate the polygon once per shard as a list of radius multipliers; split() removes the parent and pushes two children one size down, size 1 shatters to nothing.",
		"motes":     "Motes drift, then home once the ship is within reach; they expire so a field left uncollected does not accumulate forever.",
		"ship":      "Thrust is acceleration, not velocity; clamp top speed and apply exponential damping against dt. Firing has a cooldown and a small recoil. The pulse spends the whole meter at once.",
		"collide":   "Resolve bullets against shards first (so a shard cut this frame cannot also kill the player), then motes, then the ship. Call w.onShipHit for the player collision instead of handling lives here.",
		"hud":       "Draw in screen space after the camera shake is restored. The provenance panel reads w.manifest and must survive a manifest with no modules.",
		"game":      "Own the world object and the loop. Clamp dt to 0.05 so a background tab does not teleport anything. A wave ends when shards.count() reaches zero.",
	}[id]
	if notes == "" {
		notes = "Keep state in the closure, expose only the API, and touch nothing outside the namespace."
	}
	accept := []string{
		"defines exactly one namespace key: G." + id,
		"no forbidden API from the contract appears in the source",
	}
	if strings.Contains(m.API, "draw(ctx,w)") {
		accept = append(accept, "draw() restores every canvas state it changes")
	}
	out, err := json.Marshal(map[string]any{"api": m.API, "notes": notes, "accept": accept})
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// implement returns the module source — the scripted studio's real work. Two
// modules are scripted to misbehave the way chat models actually misbehave.
func implement(id string, attempts *sync.Map, who string) (string, error) {
	src, ok := moduleSource[id]
	if !ok {
		// An extra module the design proposed and the contract allowed: a
		// stub that satisfies the contract is better than a failed build.
		return fmt.Sprintf("(function (G) {\n  G.%s = {\n    update: function (w) {},\n    draw: function (ctx, w) {}\n  };\n})(window.LOOM);", id), nil
	}

	n, _ := attempts.LoadOrStore(id, new(int))
	count := n.(*int)
	*count++

	// shards: the first response comes back truncated. The call succeeded, so
	// this is a semantic failure, not a transient one — retrying the same model
	// is a coin flip, and the task climbs to the deep model instead.
	if id == "shards" && *count == 1 {
		cut := len(src) * 2 / 3
		return src[:cut] + "\n      // …", nil
	}

	// audio: a chat model that ignored "source only" and wrapped its answer in
	// a markdown fence with prose either side. The lint stage strips it.
	if id == "audio" {
		return "Here is the module, implemented against the contract:\n\n```javascript\n" +
			src + "\n```\n\nLet me know if you want the envelope shapes tuned.", nil
	}
	return src, nil
}

func reviewJSON(id string) (string, error) {
	risk := 1 + hash(id)%3
	note := map[string]string{
		"ship":    "meets the acceptance criteria; state is closed over, namespace assignment is single",
		"collide": "resolution order matters here and is correct: shards are cut before the player is checked",
		"game":    "the dt clamp is the load-bearing line; without it a backgrounded tab teleports the field",
		"audio":   "context is created lazily, which is the only way this works after an autoplay policy",
	}[id]
	if note == "" {
		note = "clean against the contract; no forbidden API, no cross-module globals"
	}
	out, err := json.Marshal(map[string]any{"verdict": "ship", "risk": risk, "note": note})
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// A ReduceAI stage is a tree: the leaves see records, and every level above
// them sees what the level below wrote. The scripted studio has to answer both,
// which is why these two functions branch on what they were handed — a naive
// mock that counted "\n- " and called the answer a module total would report
// the top level's fan-in (3) as the size of a twelve-module game.

func designDoc(prompt string) string {
	items := reduceItems(prompt, "Write markdown:")
	if len(items) == 0 {
		return "No specifications reached this level."
	}
	if !isLeafLevel(items) {
		return "## Modules\n\nThe game divides along the only seam that lets twelve engineers " +
			"work without talking to each other: state ownership. Every module owns exactly one " +
			"pool and exposes update/draw over it, so the loop can call them in a fixed order and " +
			"no module needs to know how its neighbours store anything.\n\n" +
			strings.Join(items, "\n\n")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "### %s\n\nThese %d modules own disjoint state and are safe to write in "+
		"parallel: each exposes a narrow API over one pool, and the only coupling left is the "+
		"order the loop calls them in.\n", strings.Join(idsOf(items), ", "), len(items))
	for _, it := range items {
		id, role := idOf(it), roleOf(it)
		fmt.Fprintf(&b, "  · **%s** — %s\n", id, role)
	}
	return strings.TrimRight(b.String(), "\n")
}

func buildLog(prompt string) string {
	items := reduceItems(prompt, "One short paragraph")
	if len(items) == 0 {
		return "Nothing built at this level."
	}
	if !isLeafLevel(items) {
		return "Build complete. Every module linked clean against the contract: no forbidden " +
			"API, no cross-module globals, one namespace assignment each. One module was " +
			"re-issued on the deep model after a truncated first response; the bundle is " +
			"self-contained, with no imports, no network, and no assets.\n\n" +
			strings.Join(items, "\n\n")
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d modules linked in this batch:\n", len(items))
	for _, it := range items {
		fmt.Fprintf(&b, "  · %s\n", head(it))
	}
	return strings.TrimRight(b.String(), "\n")
}

// reduceItems recovers the items a reduce prompt listed. Everything from the
// first "- " bullet to the instruction tail is the list, and continuation lines
// are indented, which is what makes the split unambiguous.
func reduceItems(prompt, tail string) []string {
	body := prompt
	if tail != "" {
		if i := strings.Index(body, tail); i >= 0 {
			body = body[:i]
		}
	}
	i := strings.Index(body, "\n- ")
	if i < 0 {
		return nil
	}
	out := []string{}
	for _, raw := range strings.Split(body[i+1:], "\n- ") {
		if s := strings.TrimSpace(strings.TrimPrefix(raw, "- ")); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// isLeafLevel reports whether these items are records — the bottom of the tree
// — rather than summaries this same studio wrote one level down. Every summary
// it writes carries the "  · " bullet and no record field does, which is the
// cheapest reliable tell.
func isLeafLevel(items []string) bool {
	for _, it := range items {
		if strings.Contains(it, "  · ") {
			return false
		}
	}
	return true
}

func head(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

// idOf reads the module ID off the front of an item line: spec lines open
// "<id> — <role>", build lines open "<id> (LOOM.<id>): …".
func idOf(item string) string {
	h := head(item)
	if i := strings.Index(h, " — "); i >= 0 {
		return strings.TrimSpace(h[:i])
	}
	if i := strings.IndexAny(h, " ("); i >= 0 {
		return strings.TrimSpace(h[:i])
	}
	return h
}

func roleOf(item string) string {
	h := head(item)
	if i := strings.Index(h, " — "); i >= 0 {
		return strings.TrimSpace(h[i+len(" — "):])
	}
	return h
}

func idsOf(items []string) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, idOf(it))
	}
	return out
}

func titleCardJSON() (string, error) {
	out, err := json.Marshal(map[string]any{
		"title":   "CONSTELLATION DRIFT",
		"tagline": "cut the dark, weave the light",
		"howto": []string{
			"A / D or ← → to turn · W or ↑ to thrust · SPACE to fire",
			"Z releases the weave pulse when the meter is full",
			"Motes fade — fly through them before the field goes dark",
		},
	})
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func hash(s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % 1000)
}
