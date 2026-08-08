# studio

Opens [Loom Studio](../../docs/STUDIO.md) on the vertical-digest pipeline: the
same pipeline [examples/vertical-digest](../vertical-digest) writes in Go, held
as a document you can edit on a canvas, priced from the records on disk before
anything is spent, and exported back to Go.

```sh
go run ./examples/studio
# studio:             http://localhost:8078
# constellation view: http://localhost:8077
```

With no flags it invents a small archive (3 verticals × 24 days of synthetic
chat) in a temp directory and registers three deterministic mock models, so it
runs offline with no API key. The price in the header is real arithmetic over
real records at made-up per-token rates; the Run button really runs it.

## What to try

1. **Watch the price move.** Select *Daily digest* and change the model tier or
   the answer length. The header, the price table and the card all update from
   the same projection — computed on the server, because the browser cannot
   compile a pipeline or know what a model costs.
2. **Check the redactions.** Select *Load days*. The FIRST RECORD panel shows a
   real record after the sender names became `S1, S2…` and the emails became
   `<email>` — the one place to confirm that before anything leaves the machine.
3. **Ask.** Press ⌘K (Ctrl-K) and pick *Make this cheaper*. The proposal states
   what the edited document costs, because it priced it. Nothing changes until
   you press Accept; Discard leaves the canvas untouched.
4. **Follow the dashed band.** *Business overview* sits inside SECOND RUN ·
   FAN-IN. Loom's DAGs fan out but never fan back in, so a fold over three
   branches is a second run seeded from the first one's output — see the
   inspector for what that changes.
5. **Export it.** *Export Go* renders the canvas as a Go program that compiles:
   `build()`, `buildSecond()`, and a `main` that runs both.
6. **Run it.** Run ▸ runs both passes against the mock models and streams them
   into the constellation view next door, then writes
   `reports/business-overview.md`. Set `LOOM_STATE=/tmp/loom` and run it twice
   to watch the second run replay from cache.

## Flags

| Flag | Default | What it does |
|---|---|---|
| `-messages` | invents one | archive root: `<vertical>/<date>.jsonl` |
| `-doc` | — | a JSON document to open, and autosave every edit into |
| `-addr` | `localhost:8078` | the studio |
| `-viz` | `localhost:8077` | the constellation view (empty disables it) |
| `-out` | `reports` | where the write step puts the one-pager |
| `-budget` | `15` | the hard cost cap the document carries |
| `-openai` | off | use real OpenAI models (needs `OPENAI_API_KEY`) |
| `-state` | `$LOOM_STATE` | state dir for cache/resume |

Against real data and real models:

```sh
OPENAI_API_KEY=sk-... LOOM_STATE=~/.loom-state \
  go run ./examples/studio -openai \
  -messages /path/to/messages -doc vertical-digest.json -budget 15
```

`-doc` makes the studio autosave: the document is a JSON file, so it diffs,
reviews and merges like everything else in the repository.
