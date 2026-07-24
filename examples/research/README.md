# research — the constellation view, at scale

A systematic literature survey of ~50 papers on self-improving AI agents,
run entirely on scripted mock models: **no keys, no network, zero cost**,
~205 task stars across a branching DAG, finishing in about 30 seconds.

```sh
go run ./examples/research     # then open http://localhost:8077
```

The mocks are scripted so one run shows every visual state the view can
render. This is the example to run when you want to see (or record) the
whole UI in motion.

```
papers ─ screenable (fused map+filter) ─ screen ─ relevant-only ─ extract-findings
                                                                  ├─ grade-evidence ─ strong-evidence ─ synthesis (tree) ─ executive-abstract
                                                                  └─ claims (fused flatmap+filter) ─ open-questions (tree)
```

| scripted moment | where it appears |
|---|---|
| 6 transient 429/503 failures | retry orbits (`↻`) scattered across the sky |
| one 11-second straggler | growing star with a rotating activity ring, in `screen` |
| one retracted paper (permanent failure) | dead-lettered red cross in `screen` |
| garbled JSON ×2, invalid grade ×1 | escalation ladder → task inspector shows `mock-oracle` took over |
| two abstract-less preprint stubs | records dropped inside the fused `screenable` stage |
| three model tiers with real pricing | per-star tokens/cost, header cost ticking up |
| two ReduceAI trees (fan-in 4 and 6) | multi-level clusters whose lineage fans in |

## Flags & modes

```sh
go run ./examples/research -addr localhost:8077   # UI address
go run ./examples/research -budget 0.02           # squeeze the budget governor:
                                                  # header shows "⚠ budget exceeded",
                                                  # run stops gracefully, partial results

# cache = checkpoint: rerun with a state dir; the second run settles the
# whole sky instantly in the cache-replay hue (✧), zero model calls, $0
LOOM_STATE=/tmp/loom-research go run ./examples/research
LOOM_STATE=/tmp/loom-research go run ./examples/research
```

## Recording storyboard

A shot list that covers every UI feature in ~90 seconds of footage. Use a
1600×1000 window, dark browser chrome, 100% zoom. The run waits up to 60s
for the browser to connect, so start the recording on the empty sky.

1. **Empty sky → ignition** (0:00) — open the page before the run starts:
   the "Listening for a Loom run" empty state, then the reset as
   `run.started` arrives and the first stage clusters fade in.
2. **The sky fills** (0:05) — let the `screen` stage fan out: 48 stars
   pulsing, 8 executor diamonds linking to the tasks they're running,
   header stats (elapsed / tasks / active / retries / tokens / cost)
   ticking live.
3. **Retry orbit** (0:12) — a `↻` star gains an orbiting tick per failed
   attempt. Hover it for the tooltip, click it: the inspector's event log
   shows `attempt 1 failed: 429 … → retrying`.
4. **The straggler** (0:20) — one `screen` star stays lit long enough to
   grow a rotating activity ring while its whole cluster completes around
   it. Click it; the runtime counter ticks live in the inspector.
5. **The red cross** (0:28) — the retracted paper burns as a red ✕. Click:
   `failed after 1 attempt — permanent: paper withdrawn by publisher`.
   This is a dead letter; the run continues around it.
6. **Escalation ladder** (0:35) — click the star for "Emergent Deception…"
   in `screen`: the call log shows call #1 on `mock-scout` returning
   garbage and call #2 on `mock-oracle` succeeding — full request and
   response text for both, with per-call tokens, latency, and cost.
7. **Lineage** (0:45) — click a `synthesis` star: solid lines draw what
   merged into it, dashed lines where its output goes. Use the
   *merged from* / *feeds into* jump buttons to walk the reduce tree down
   to individual papers, and ←/→ to cycle sibling stars.
8. **Stage inspector** (0:55) — click the `screen` stage label: kind,
   upstream, task counts, records in/out, retries, cache hits, tokens,
   cost, p50/p95, the prompt template, and jump buttons to failed tasks.
   Click a `◇` executor too: busy/idle state, tasks done, busy time.
9. **Finale** (1:05) — the two branches converge: the synthesis tree
   narrows 30 findings → 8 → 2 → 1, `executive-abstract` flashes last,
   header flips to `✦ complete` with the final cost.
10. **Cache replay** (1:15) — cut to the second `LOOM_STATE` run: the same
    sky settles in ~2 seconds in the distinct ✧ hue, `cached: 205`,
    `$0` spent. Click any star: "⚡ replayed from cache".
11. **Budget governor** (optional coda) — `-budget 0.02`: mid-run the
    header gains `⚠ budget exceeded`, remaining tasks stop cleanly.

Every scripted beat is deterministic (keyed to paper titles), so takes are
repeatable; only latency jitter varies between runs.
