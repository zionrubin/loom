package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// The three things every task in this forge has to agree on: what a module
// *is* (the engine contract), what the game should look like (the art
// direction), and how the modules link (the module graph).
//
// They are broadcasts rather than constants pasted into prompts because each
// one is read by more than one kind of consumer: the contract is the shared
// prompt prefix of the implement stage *and* the rule set a Go lint stage
// enforces; the art direction is read by two prompts in two different runs;
// the module graph is a lookup table a Go stage joins against and no task
// should carry. Registered once per run, stored once by content hash,
// referenced — never copied — by the tasks that declare them.
//
// In the constellation view they appear as hexagons above the sky, each one
// wired to the stage clusters that read it.

// engineContract is the whole agreement between twelve modules written by
// twelve independent tasks that never see each other's code. It is the shared
// prompt prefix of the implement stage — rendered once per task and served by
// the provider's prompt cache on every call after the first — and it is also
// the rule set the lint stage enforces in Go, which is the point: one value,
// two very different readers, one content hash in the fingerprint.
var engineContract = map[string]any{
	"namespace": "window.LOOM",
	"module_form": "(function (G) { G.<id> = { ...exported functions... }; })(window.LOOM);\n" +
		"One IIFE. No imports, no exports, no bundler, no top-level side effects " +
		"beyond the single namespace assignment.",
	"language": "ES5-compatible browser JavaScript. No template literals, no arrow " +
		"functions, no classes, no async/await, no optional chaining.",
	"capabilities": []string{"canvas2d", "keyboard", "webaudio", "requestAnimationFrame"},
	"forbidden": []string{
		"fetch(", "XMLHttpRequest", "WebSocket", "navigator.sendBeacon",
		"localStorage", "sessionStorage", "document.cookie",
		"import ", "require(", "eval(", "document.write", "innerHTML",
	},
	"world_object": "Every update/draw function receives the shared world object w:\n" +
		"  w.w, w.h      canvas size in CSS pixels\n" +
		"  w.dt          seconds since the last frame, clamped to 0.05\n" +
		"  w.t           seconds since boot\n" +
		"  w.state       'title' | 'playing' | 'over'\n" +
		"  w.score, w.best, w.wave, w.lives\n" +
		"  w.charge      0..1, the weave-pulse meter\n" +
		"  w.shake       screen-shake magnitude in pixels\n" +
		"  w.flash       0..1 full-screen flash\n" +
		"  w.ship        the player object, or null\n" +
		"  w.manifest    the loom build manifest (read-only, drawn by the HUD)\n" +
		"  w.card        the generated title card (title, tagline, howto[])\n" +
		"  w.onShipHit(shard)  called by collide when the ship is struck",
	"rules": []string{
		"Draw with the canvas 2D context you are handed; never look up DOM nodes.",
		"Never allocate per frame what can be pooled; the loop must hold 60fps.",
		"Call sibling modules through the namespace (G.vec, G.audio, ...), never through globals.",
		"Every module must tolerate being loaded before the modules it calls, because " +
			"the bundle is concatenated in dependency order but nothing runs until boot.",
		"Position and velocity are in pixels and pixels/second; multiply by w.dt, never by frame count.",
	},
}

// artDirection is read by two prompts in two different runs — the implement
// prefix in the build run and the title-card prompt in the ship run — which is
// exactly the case broadcasts exist for: the palette lives in one place and
// two runs agree on it without either one carrying it.
var artDirection = map[string]any{
	"mood":       "cold neon vector arcade: a dark sky, thin bright strokes, additive glow",
	"background": "#05060c",
	"palette": map[string]string{
		"ship":    "#7ce7ff",
		"thread":  "#ffd479",
		"shard":   "#8b9bff",
		"shard_s": "#c58bff",
		"mote":    "#5cffb1",
		"danger":  "#ff5c7a",
		"text":    "#dfe7ff",
	},
	"typography": "monospace only, uppercase for HUD labels, generous letter spacing",
	"motion":     "everything eases; explosions are short and bright; screen shake is small and frequent rather than large and rare",
}

// module is one unit of the engine: what it is called, what it owes the rest
// of the bundle, and where it sits in the link order.
type module struct {
	ID    string   `json:"id"`
	Order int      `json:"order"`
	Role  string   `json:"role"`
	API   string   `json:"api"`
	Uses  []string `json:"uses"`
}

// moduleGraph is the broadcast-join case: a table every task in the ship run
// needs and none of them should carry. The link stage joins each generated
// module against it to recover the order the bundle must be concatenated in —
// an order that is a property of the engine, not of any one module.
var moduleGraph = map[string]module{
	"vec":       {ID: "vec", Order: 1, Role: "math, deterministic RNG, screen wrapping, circle overlap", API: "TAU, seed(n), rnd(), rand(a,b), randInt(a,b), pick(xs), clamp(v,a,b), lerp(a,b,t), dist2(ax,ay,bx,by), hit(a,b), wrap(o,w,h,pad)"},
	"input":     {ID: "input", Order: 2, Role: "keyboard state with per-frame edge detection", API: "attach(), down(...codes), hit(...codes), anyHit(), endFrame()"},
	"audio":     {ID: "audio", Order: 3, Role: "WebAudio blips synthesized at call time, no asset files", API: "unlock(), muted(), toggle(), blip(kind)"},
	"starfield": {ID: "starfield", Order: 4, Role: "three parallax star layers drifting behind the action", API: "init(w), update(w), draw(ctx,w)", Uses: []string{"vec"}},
	"particles": {ID: "particles", Order: 5, Role: "the spark pool every explosion and thruster draws from", API: "reset(), count(), burst(w,x,y,opts), update(w), draw(ctx,w)", Uses: []string{"vec"}},
	"bullets":   {ID: "bullets", Order: 6, Role: "the threads the shuttle fires, pooled and wrapped", API: "reset(), list(), fire(w,x,y,angle,speed), update(w), draw(ctx,w)", Uses: []string{"vec"}},
	"shards":    {ID: "shards", Order: 7, Role: "drifting polygon rocks that split into smaller ones when cut", API: "reset(), list(), count(), spawnWave(w,n), split(w,shard), clear(), update(w), draw(ctx,w)", Uses: []string{"vec"}},
	"motes":     {ID: "motes", Order: 8, Role: "collectible sparks that home toward the ship and charge the pulse", API: "reset(), list(), drop(w,x,y,n), update(w), draw(ctx,w)", Uses: []string{"vec"}},
	"ship":      {ID: "ship", Order: 9, Role: "player physics, firing, and the weave-pulse discharge", API: "spawn(w), update(w), draw(ctx,w)", Uses: []string{"vec", "input", "bullets", "particles", "audio", "shards"}},
	"collide":   {ID: "collide", Order: 10, Role: "every interaction between two things: bullet/shard, ship/mote, ship/shard", API: "resolve(w)", Uses: []string{"vec", "bullets", "shards", "motes", "particles", "audio"}},
	"hud":       {ID: "hud", Order: 11, Role: "score, wave, lives, charge meter, title and game-over overlays, provenance panel", API: "draw(ctx,w), overlay(ctx,w), provenance(ctx,w)", Uses: []string{"vec"}},
	"game":      {ID: "game", Order: 12, Role: "world state, the fixed-step loop, wave progression, and the wiring of every other module", API: "boot(canvas, manifest, card)", Uses: []string{"vec", "input", "audio", "starfield", "particles", "bullets", "shards", "motes", "ship", "collide", "hud"}},
}

// coreModules is the required set, in link order. The brief hands it to the
// concept model as a hard constraint, the ship run refuses to emit a bundle
// missing any of it, and the HTML shell calls LOOM.game.boot, so "game" is the
// one module whose absence is fatal rather than merely degrading.
func coreModules() []module {
	out := make([]module, 0, len(moduleGraph))
	for _, m := range moduleGraph {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Order < out[j].Order })
	return out
}

func coreModuleIDs() []string {
	mods := coreModules()
	ids := make([]string, len(mods))
	for i, m := range mods {
		ids[i] = m.ID
	}
	return ids
}

// contractCapabilities returns the capability names the engine contract grants
// a module. The design run's feasibility filter drops any proposed module that
// needs something outside this set — which is how a perfectly reasonable
// feature (an online leaderboard) gets cut before a single token is spent
// implementing it.
func contractCapabilities() map[string]bool {
	caps := map[string]bool{}
	for _, c := range engineContract["capabilities"].([]string) {
		caps[c] = true
	}
	return caps
}

func forbiddenAPIs() []string { return engineContract["forbidden"].([]string) }

// --- text helpers -----------------------------------------------------------

// stripFences removes the markdown code fence a chat model wraps code in, and
// any prose it puts either side of it. Offline the scripted studio returns bare
// source; a real model does this about half the time, so the lint stage — and
// the validator the implement stage runs before it — has to cope with both.
func stripFences(s string) string {
	s = strings.TrimSpace(s)
	if !strings.Contains(s, "```") {
		return s
	}
	i := strings.Index(s, "```")
	rest := s[i+3:]
	if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
		if lang := strings.TrimSpace(rest[:nl]); len(lang) < 12 && !strings.Contains(lang, "(") {
			rest = rest[nl+1:]
		}
	}
	if j := strings.Index(rest, "```"); j >= 0 {
		rest = rest[:j]
	}
	return strings.TrimSpace(rest)
}

// balanced reports whether every bracket in src closes, ignoring the contents
// of strings and comments. It is the cheapest possible stand-in for a parser
// and it catches the failure that actually happens: a truncated response.
func balanced(src string) bool {
	var depth int
	var inStr byte
	var lineComment, blockComment bool
	for i := 0; i < len(src); i++ {
		c := src[i]
		switch {
		case lineComment:
			if c == '\n' {
				lineComment = false
			}
		case blockComment:
			if c == '*' && i+1 < len(src) && src[i+1] == '/' {
				blockComment, i = false, i+1
			}
		case inStr != 0:
			if c == '\\' {
				i++
			} else if c == inStr {
				inStr = 0
			}
		case c == '/' && i+1 < len(src) && src[i+1] == '/':
			lineComment, i = true, i+1
		case c == '/' && i+1 < len(src) && src[i+1] == '*':
			blockComment, i = true, i+1
		case c == '\'' || c == '"':
			inStr = c
		case c == '{' || c == '(' || c == '[':
			depth++
		case c == '}' || c == ')' || c == ']':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0 && inStr == 0 && !blockComment
}

func joinStrings(v any, sep string) string {
	switch xs := v.(type) {
	case []string:
		return strings.Join(xs, sep)
	case []any:
		parts := make([]string, 0, len(xs))
		for _, x := range xs {
			if s, ok := x.(string); ok && s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, sep)
	case string:
		return xs
	case nil:
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func stringList(v any) []string {
	switch xs := v.(type) {
	case []string:
		return xs
	case []any:
		out := make([]string, 0, len(xs))
		for _, x := range xs {
			if s, ok := x.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if xs == "" {
			return nil
		}
		parts := strings.Split(xs, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	}
	return nil
}

// num reads an integer field off a record. Records make a JSON round trip
// through the content-addressed store, so a field written as an int comes back
// from the cache as a float64 — which is exactly the kind of difference that
// would otherwise show up as "1.104e+03 bytes" in a replayed build.
func num(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	}
	return 0
}

// --- the single-file artifact ----------------------------------------------

// shell is the page the ship run fills in: one HTML file, no requests, no
// assets, no build step. The emit stage substitutes the five placeholders and
// what comes out is the deliverable.
const shell = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__LOOM_TITLE__</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Cpath d='M26 16 6 26l4-10-4-10z' fill='none' stroke='%237ce7ff' stroke-width='3'/%3E%3C/svg%3E">
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; background: #05060c; overflow: hidden; }
  body {
    font: 400 13px/1.5 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    color: #dfe7ff; display: flex; flex-direction: column;
  }
  header {
    display: flex; align-items: baseline; gap: 14px; flex-wrap: wrap;
    padding: 10px 18px; border-bottom: 1px solid rgba(124,231,255,0.14);
    background: linear-gradient(180deg, rgba(12,16,38,0.9), rgba(5,6,12,0.6));
  }
  header .title { font-weight: 700; font-size: 15px; color: #7ce7ff; letter-spacing: 0.14em; text-transform: uppercase; }
  header .tagline { color: #c58bff; }
  header .forge { margin-left: auto; color: rgba(139,155,255,0.7); font-size: 11px; }
  header .forge b { color: #5cffb1; font-weight: 600; }
  main { position: relative; flex: 1; min-height: 0; }
  canvas { display: block; width: 100%; height: 100%; }
</style>
</head>
<body>
<header>
  <span class="title">__LOOM_TITLE__</span>
  <span class="tagline">__LOOM_TAGLINE__</span>
  <span class="forge">forged by <b>loom</b> · <span id="forge-badge">__LOOM_BADGE__</span></span>
</header>
<main><canvas id="stage"></canvas></main>
<script>
window.LOOM = window.LOOM || {};
window.LOOM_MANIFEST = __LOOM_MANIFEST__;
window.LOOM_CARD = __LOOM_CARD__;
(function (m) {
  // A run cannot report its own totals from inside itself, so the run that
  // emitted this file left a hole here and stamped it shut once it ended.
  var ship = __LOOM_SHIP__; /* loom:ship */
  if (ship) {
    m.runs.push(ship);
    m.tasks += ship.tasks;
    m.calls += ship.calls;
    m.tokens += ship.tokens;
    m.cost += ship.cost;
  }
  var badge = document.getElementById('forge-badge');
  if (badge) {
    badge.textContent = m.runs.length + ' runs · ' + m.modules.length + ' modules · ' +
      m.tasks + ' tasks · ' + m.calls + ' model calls · $' + m.cost.toFixed(4);
  }
})(window.LOOM_MANIFEST);
</script>
<script>
__LOOM_BUNDLE__
</script>
<script>
window.LOOM.game.boot(document.getElementById('stage'), window.LOOM_MANIFEST, window.LOOM_CARD);
</script>
</body>
</html>
`
