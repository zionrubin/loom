# `public/` — the Loom page

A static, dependency-free landing page for Loom, with the **Woven Knot** as
its hero object.

| File | |
|---|---|
| `index.html` | The page: what Loom is, why it exists, a pipeline end to end, a run end to end, how to run it, and the docs index. One file — styles and scripts inline. |
| `woven-knot.html` | The object on its own, full-viewport, with the stage's OBJ + MTL / GLB export toolbar intact. |
| `knot-model.js` | Builds the knot: four braided strands, a lit core thread, and travelling pulses. |
| `three-d-stage.js` | The `<three-d-stage>` custom element — renderer, studio lighting, orbit controls, camera framing, exporters. Vendored verbatim. |
| `survey/` | The run: `examples/research` as a 65-second animation, drawn on a canvas. See below. |

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

## `survey/` — the run

`examples/research` as a 65-second composition: 24 questions through the
findings gate, 50 papers down to one executive abstract and a set of open
questions, with the retries, the escalations and the dead letter the run
actually produces — then the rerun, replayed from cache with zero model
calls.

| File | |
|---|---|
| `survey/survey.js` | The whole run: scene table, corpus, camera, choreography, HUD, captions, and a small player. One ES module, no dependencies, no side effects at import. |
| `survey/index.html` | The run on its own: full-viewport, with a transport (play/pause, scrub, scene markers, captions). |
| `survey/survey-animation.dc.html` | A redirect to `survey/`, so links to the old player still land. |

### How it is drawn

Every frame is a pure function of the time: `drawFrame(ctx, T)` paints what
the run looks like at `T` and nothing else. Playing, scrubbing, looping, and
the still shown to a reader who asked for less motion are all the same call
at a different `T`. Each stage is a function that reads `T` against the
twelve scene cues (`CUES.Screen`, `CUES.Grade`, …), so retiming one scene
moves everything keyed to it and nothing else.

It is a `<canvas>` rather than DOM because of what it replaced: a React
composition that needed React, ReactDOM and Babel from a CDN (about 3.4 MB
fetched, the JSX transpiled in the reader's browser) and re-laid-out some
1,700 absolutely positioned nodes on every frame. On the same page, in the
same headless browser, that ran at about 11 fps with script holding the main
thread three quarters of the time. The canvas fetches 80 KB (26 KB gzipped),
holds 60 fps, and spends well under a millisecond of script per frame.

A few things keep it light and legible:

- **It runs only when it can be seen.** The module is not fetched until the
  section is 400px from view, and once mounted the loop stops whenever the
  canvas is off screen or the tab is hidden, and resumes where it left off.
  The hero's 3D stage gets the same treatment from `index.html`: its WebGL
  loop stops once the reader has scrolled past it, so the two never compete.
- **The camera is authored as shots.** Each key names the world rectangle the
  shot frames, fitted into a safe area clear of the HUD, and the path between
  keys is a monotone cubic in position and log-scale — it carries velocity
  through a key it is passing, never overshoots one it is settling on.
- **Text that would be too small to read is not set.** Below ~5 CSS px a
  label is drawn as a greeked bar of the same length, so the pulled-back
  shots read as the shape of a document rather than pixel noise, and cost
  a fraction of the text rendering.
- **Glyphs are paths**, not characters (✓ ✕ ↗ ✧ ↻), so they are the same
  weight on every platform and at every zoom.
- **Records move as comets**: each token is drawn with a tapered streak from
  where it was a few hundredths of a second earlier.

### On the page

The section is the run and nothing else — no heading, no copy, no captions —
the last thing on the page, after the docs index and before the footer, by
which point everything above has said what a pipeline is, what it gets you
and how to start one. It is the full width of the viewport, exactly 16:9, up
to a `max-width` of 1960px; past that the run would be more than 1100px tall
and become a screen to scroll through rather than a thing to watch. Beyond
the cap the frame centres, and because the run's ground is the page's
`--bg` in both palettes, the margins are invisible rather than letterboxing.
It follows the reader's colour scheme, live.

It has one control, pause, plus a link to the full player; both stay out of
the picture until a pointer is over the run or focus reaches them, and are
always up while it is paused and on touch screens. A loop that starts by
itself and runs for more than five seconds owes the reader a way to stop it.

A reader who has asked for less motion is shown the finished run — the whole
DAG, every task replayed from cache — held, with a play button that starts it
from the top. Without JavaScript, or if the module cannot be loaded, the
section is a plain link to `examples/research`.

`.github/workflows/pages.yml` imports the module in Node and checks that the
scene table still adds up to 65.5 seconds, that every cue the choreography
uses names a scene, that both pages still describe the run with the counts it
draws, and that every frame of the timeline, in both palettes, draws without
throwing.

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

The run began as the Claude Design project *Animated map reduce task*
(`Loom Survey Animation.dc.html` and the four files it imported). Its corpus,
scene table, choreography, captions and palettes are carried over into
`survey/survey.js`; the React engine, the Babel runtime and the design tool's
panel are not, and the composition has been re-authored for the canvas — the
camera as framed shots, the findings gate routing each question through the
tier that decides it, records as comets, glyphs as paths.

## Notes

- No build step, no framework, no webfonts, no trackers. System type throughout.
- Light and dark follow `prefers-color-scheme`.
- Syntax colouring is ~25 lines of regex over the `<pre>` text; the code blocks
  are plain text in the markup and stay readable if that never runs.
- Two `three.js` deprecation warnings (`Clock`, `PCFSoftShadowMap`) come from
  the vendored starter and the design's model code. They are warnings, not
  errors, and are left as-is rather than forking upstream code.
- The run is dependency-free like the rest of the page; three.js for the hero
  is the only thing fetched from a CDN.
- On a phone the run's smallest labels are below reading size; the shape of
  the DAG and the motion still carry, and the copy around it carries the rest.
