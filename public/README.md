# `public/` — the Loom page

A static, dependency-free landing page for Loom, with the **Woven Knot** as
its hero object.

| File | |
|---|---|
| `index.html` | The page: what Loom is, why it exists, a pipeline end to end, a run end to end, how to run it, and the docs index. One file — styles and scripts inline. |
| `woven-knot.html` | The object on its own, full-viewport, with the stage's OBJ + MTL / GLB export toolbar intact. |
| `knot-model.js` | Builds the knot: four braided strands, a lit core thread, and travelling pulses. |
| `three-d-stage.js` | The `<three-d-stage>` custom element — renderer, studio lighting, orbit controls, camera framing, exporters. Vendored verbatim. |
| `survey/` | The survey animation and its runtime. See below. |

## Running it

ES modules need a real origin, so `file://` will not do:

```sh
python3 -m http.server 8099 --directory public
# then open http://localhost:8099/
```

The page reads and works with no network beyond `unpkg.com`, which serves
three.js for the hero object. That import is pinned by version **and** by
SRI hash in the import map at the top of each HTML file — the two must stay
together if either is ever bumped. When it can't be fetched (offline, blocked
CDN, no WebGL), the hero drops the object and the copy takes the full width;
nothing else on the page depends on it.

## `survey/` — the animation

`examples/research` as a 65-second composition: 24 questions through the
findings gate, 50 papers down to one executive abstract and a set of open
questions, with the retries, the escalations and the dead letter the run
actually produces. It is a React piece, unlike everything else here.

| File | |
|---|---|
| `survey/survey-animation.dc.html` | The host page. Mounts the composition; opens on its own as a full-viewport player. |
| `survey/loom-survey-scene.jsx` | The composition — corpus, DAG, camera, captions. Vendored verbatim. |
| `survey/animations-v3.jsx` | The animation engine: authored-time axis, cue table, stage, transport. Vendored verbatim. |
| `survey/tweaks-panel.jsx` | The design tool's control panel. Vendored verbatim; loaded because the scene file imports it, never opened here. |
| `survey/support.js` | `dc-runtime`: parses the host page, loads React and Babel, transpiles and mounts the JSX. Vendored verbatim. |

Three things keep it from taking over the page it sits on.

**It waits to be asked.** React, ReactDOM and Babel are ~1.5 MB from
`unpkg.com`, and the JSX is transpiled in the browser. None of that is fetched
until a reader clicks the poster, so the landing page still costs what it
claims to. Without JavaScript the poster is an ordinary link to the animation's
own page, which is the whole fallback.

**It runs in an `<iframe>`.** The host page is a full-page document — its
`<helmet>` sets a `<title>` and `html, body` rules, and the runtime replaces
the document body with a React root. An iframe is what stops any of that from
reaching `index.html`, and it keeps React out of the page's own scope. The
frame is sized `calc(56.25% + 44px)` tall: 16:9 for the 1920×1080 canvas, plus
the 44px the stage reserves for its transport, so the canvas lands at exactly
the card's width with the transport below it rather than letterboxed or clipped.

**It stays dark in both page themes.** The composition ships light and dark
palettes — the same tokens as this page — but the stage paints its own
near-black surround and a dark transport either way. A light canvas inside that
chrome reads as a mistake, so the player is a dark panel on a light page, the
way a video is.

## GitHub Pages

[`.github/workflows/pages.yml`](../.github/workflows/pages.yml) publishes this
directory. When Pages builds from a branch it can only serve the repository
root or `docs/`, and this is neither — so the workflow uploads `public/` as a
Pages artifact from Actions instead. That requires the repository's Pages
source to be set to **GitHub Actions** (Settings → Pages → Build and
deployment → Source); the workflow cannot set it for you.

It runs on pushes to `main` that touch `public/**`, and can be started by hand
from the Actions tab. Once enabled the site is
<https://zionrubin.github.io/loom/>.

Every internal link on the page is relative, so it works unchanged from the
`/loom/` subpath a project Pages site is served under.

## Provenance

The object comes from the Claude Design project *3D object modeling request*
(`Woven Knot.html`, `knot-model.js`, `three-d-stage.js`).

`three-d-stage.js` is byte-identical to the design's copy — it is a starter
component that gets overwritten wholesale when re-copied, so it is left alone.
`knot-model.js` keeps the design's geometry, materials and motion exactly, and
adds one thing: a `prefers-reduced-motion` path that holds the knot in a
placed, still pose (fully orbitable) instead of animating it.

`index.html` uses the stage as an ambient hero, so it removes the export
toolbar and the orbit hint from the stage's shadow root — both belong on
`woven-knot.html`, where the object is the subject rather than the backdrop.

The animation comes from the Claude Design project *Animated map reduce task*
(`Loom Survey Animation.dc.html` and the four files it imports). All four
imports are byte-identical to the design's copies; the composition — scenes,
choreography, camera, captions, palettes — is untouched.

`survey-animation.dc.html` is that design's host page with the same helmet
(title, `OM_SCENES`, `OM_PLAYBACK`, `TWEAK_DEFAULTS`) and three changes, all of
them about being on the open web rather than in a design tool:

- it mounts `LoomSurveyEmbed` rather than `LoomSurvey` — both are exports of the
  unmodified scene file. `LoomSurvey` is the authoring entry point: it wires up
  the tweaks panel, which only ever opens when the Claude Design host asks it
  to. `LoomSurveyEmbed` is the composition and nothing else;
- the helmet's `background` rule is repeated in a plain `<style>` in the
  `<head>`, so it paints before the runtime boots instead of after — otherwise
  the frame is white for as long as React and Babel take to arrive;
- the transport's video-export button is hidden. It posts
  `omelette:request-video-export` to a host that isn't there, so it is a control
  that does nothing — the same reason the hero drops the 3D stage's toolbar.

When re-copying from the design, the thing to check by hand is that
`loom-survey-scene.jsx` still exports `LoomSurveyEmbed` and still reads
`window.OM_SCENES`. `.github/workflows/pages.yml` checks the rest: that the host
page mounts it, and that every section the choreography cues is one the scene
list actually names.

## Notes

- No build step, no framework, no webfonts, no trackers. System type throughout.
- Light and dark follow `prefers-color-scheme`.
- Syntax colouring is ~25 lines of regex over the `<pre>` text; the code blocks
  are plain text in the markup and stay readable if that never runs.
- Two `three.js` deprecation warnings (`Clock`, `PCFSoftShadowMap`) come from
  the vendored starter and the design's model code. They are warnings, not
  errors, and are left as-is rather than forking upstream code.
- The animation is the one part of the page that is not dependency-free, and it
  is quarantined behind a click and an iframe for exactly that reason. React,
  ReactDOM and Babel are pinned by version **and** SRI hash inside
  `survey/support.js`, the same arrangement as three.js in the import maps —
  the two must stay together if either is ever bumped.
- On a phone the animation's labels are below reading size; the shape of the
  DAG and the motion still carry, and the copy around it carries the rest.
