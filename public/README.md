# `public/` — the Loom page

A static, dependency-free landing page for Loom, with the **Woven Knot** as
its hero object.

| File | |
|---|---|
| `index.html` | The page: what Loom is, why it exists, a pipeline end to end, how to run it, and the docs index. One file — styles and scripts inline. |
| `woven-knot.html` | The object on its own, full-viewport, with the stage's OBJ + MTL / GLB export toolbar intact. |
| `knot-model.js` | Builds the knot: four braided strands, a lit core thread, and travelling pulses. |
| `three-d-stage.js` | The `<three-d-stage>` custom element — renderer, studio lighting, orbit controls, camera framing, exporters. Vendored verbatim. |

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

## Notes

- No build step, no framework, no webfonts, no trackers. System type throughout.
- Light and dark follow `prefers-color-scheme`.
- Syntax colouring is ~25 lines of regex over the `<pre>` text; the code blocks
  are plain text in the markup and stay readable if that never runs.
- Two `three.js` deprecation warnings (`Clock`, `PCFSoftShadowMap`) come from
  the vendored starter and the design's model code. They are warnings, not
  errors, and are left as-is rather than forking upstream code.
