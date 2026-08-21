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
| `survey/survey-animation.dc.html` | The host page. Mounts the composition; opens on its own as a full-viewport player with a transport. |
| `survey/page-embed.jsx` | The wrapper `index.html` frames: picks the palette, turns the captions off. No animation content. |
| `survey/loom-survey-scene.jsx` | The composition — corpus, DAG, camera, captions. Vendored verbatim. |
| `survey/animations-v3.jsx` | The animation engine: authored-time axis, cue table, stage, transport. Vendored verbatim. |
| `survey/tweaks-panel.jsx` | The design tool's control panel. Vendored verbatim; loaded because the scene file imports it, never opened here. |
| `survey/support.js` | `dc-runtime`: parses the host page, loads React and Babel, transpiles and mounts the JSX. Vendored verbatim. |

Four things keep it from taking over the page it closes.

**It has no chrome and no copy.** No heading, no paragraph, no poster art,
no play control, no captions, no transport. The section is the run and
nothing else — it is the last thing on the page, after the docs index and
before the footer, by which point everything above has said what a pipeline
is, what it gets you and how to start one.

**It is the full width of the viewport.** No `.wrap`, no padding, no border
and no radius: the section rule above it and the footer rule below are the
only edges it needs, and the run is drawn across the page rather than into a
column. The cap is a `max-width` of 1960px rather than a height, so the 16:9
never breaks; past that the run would be more than 1100px tall and become a
screen to scroll through rather than a thing to watch. Beyond the cap the
frame centres, and because the composition's ground is this page's `--bg` in
both palettes, the margins either side are invisible rather than letterboxing.

**It loads when a reader arrives at it.** React, ReactDOM and Babel are
~1.5 MB from `unpkg.com`, and the JSX is transpiled in the browser. None of it
is fetched while the section is somewhere further down the page: an
`IntersectionObserver` builds the frame 400px before it comes into view, and
the run fades in over an empty ruled box.

The poster is a fallback rather than a cover, and stays hidden whenever
something is going to mount on its own. It appears — a plain link to the
animation's own page — for a reader without JavaScript, one who has asked for
less motion (a 65-second loop starting by itself is what that setting is for),
and one whose browser never reached the CDN, in which case the frame is taken
back out so there is a way through rather than a box that stays empty.

**It runs in an `<iframe>`.** The host page is a full-page document — its
`<helmet>` sets a `<title>` and `html, body` rules, and the runtime replaces
the document body with a React root. An iframe is what stops any of that from
reaching `index.html`, and it keeps React out of the page's own scope. Framed
from `index.html` (`?embed`) the composition follows the page's colour scheme,
so the canvas and the page around it are literally the same ground. Opened on
its own it is a full-viewport player with a transport.

### The 44px

The stage always subtracts 44px for its transport when it scales the canvas to
fit its box, whether or not the transport is visible. Two things follow, and
they have to agree or the canvas stops being flush:

- the iframe is 44px taller than the frame, and `.frame` clips the difference.
  Hand the stage those pixels and the canvas comes out at exactly the frame's
  width; withhold them and it shrinks to fit the height instead, with gutters
  down both sides;
- the hidden transport is pinned to `height: 44px`. Left alone it draws 37,
  which is not what the scale was computed against — the canvas centres 3.5px
  down and loses 3.5px off the bottom of the frame.

`.github/workflows/pages.yml` checks that `animations-v3.jsx` still reserves
44, since a re-copy from the design could change it and nothing else would say
so.

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
(title, `OM_SCENES`, `OM_PLAYBACK`, `TWEAK_DEFAULTS`) and four changes, all of
them about being on the open web rather than in a design tool:

- it mounts `LoomSurveyPageEmbed` from `page-embed.jsx` rather than
  `LoomSurvey`. That wrapper renders `LoomSurveyEmbed` — an export of the
  unmodified scene file — with a palette that follows the reader and
  `captions={false}`. Both are needed from a wrapper rather than from the tag:
  an `<x-import>` attribute is always a string, and `LoomSurveyEmbed` turns
  captions off only for the literal boolean `false`. `LoomSurvey`, the
  design's own entry, wires up the tweaks panel, which opens only when the
  Claude Design host asks it to;
- the helmet's `background` rule is repeated in a plain `<style>` in the
  `<head>`, so it paints before the runtime boots instead of after — otherwise
  the frame is white for as long as React and Babel take to arrive. It carries
  both palettes, since the composition now follows the colour scheme;
- the transport's video-export button is hidden. It posts
  `omelette:request-video-export` to a host that isn't there, so it is a control
  that does nothing — the same reason the hero drops the 3D stage's toolbar;
- under `?embed` the whole transport is hidden. In a page it is chrome on a
  figure, and its scrub track previews on hover — a cursor crossing the bar
  drags the composition to whatever frame it passed over, and a cursor that
  leaves the frame without crossing back out of the track leaves it stuck
  there.

When re-copying from the design, the thing to check by hand is that
`loom-survey-scene.jsx` still exports `LoomSurveyEmbed`, still reads
`window.OM_SCENES`, and still takes `theme` and `captions`.
`.github/workflows/pages.yml` checks the rest: that the host page mounts the
wrapper, that the 44px the layout is built around is still what the stage
reserves, and that every section the choreography cues is one the scene list
actually names.

## Notes

- No build step, no framework, no webfonts, no trackers. System type throughout.
- Light and dark follow `prefers-color-scheme`.
- Syntax colouring is ~25 lines of regex over the `<pre>` text; the code blocks
  are plain text in the markup and stay readable if that never runs.
- Two `three.js` deprecation warnings (`Clock`, `PCFSoftShadowMap`) come from
  the vendored starter and the design's model code. They are warnings, not
  errors, and are left as-is rather than forking upstream code.
- The animation is the one part of the page that is not dependency-free, and it
  is quarantined behind an iframe and a scroll for exactly that reason. React,
  ReactDOM and Babel are pinned by version **and** SRI hash inside
  `survey/support.js`, the same arrangement as three.js in the import maps —
  the two must stay together if either is ever bumped.
- On a phone the animation's labels are below reading size; the shape of the
  DAG and the motion still carry, and the copy around it carries the rest.
