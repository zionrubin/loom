# The constellation view

`viz` is a live, zero-dependency web UI that renders every task and executor of
a run as a star — what's running, what's slow, what failed, at a glance.

```go
v := viz.New()                    // viz.New(viz.Retain(30)) to hold more runs
url, _ := v.Start("localhost:8077")

loom.Run(ctx, p, loom.WithEventHandler(v.Handle), ...)
```

It consumes the ordinary event bus, so it is an observer and nothing more: a run
behaves identically whether or not anything is watching.

## What a star carries

Tasks pulse while running, gain a ring when slow, flash on completion, and turn
red on failure. Per-node drill-down shows the full input and output records,
every model call's rendered request and response, runtime, tokens, cost, retries,
and logs. Selecting a star draws its **lineage** — which tasks merged into it and
where its output went — and clicking a stage name opens an inspector with the
stage's spec, prompt template, and live stats.

Heavy per-node payloads (rendered prompts, responses, record JSON) load only for
the node you open, which is what keeps the view responsive on runs with
thousands of tasks.

Other things drawn as themselves:

- **Shared values** sit in a band above the stage clusters, feeding down into the
  stages that read them, with what each one saved by being referenced instead of
  copied.
- **MCP servers** are rings in their own band below — circumference is the
  concurrency ceiling, the bright arc is the peak calls in flight, the dot at the
  centre is the session. See [MCP.md](./MCP.md).
- **An iterative stage** is drawn as concentric orbits, one ring per superstep,
  the outer rings thinning as vertices go quiet, with the per-round frontier and
  the halt reason in the stage inspector. See [ALGORITHMS.md](./ALGORITHMS.md).
- **Under streaming**, executors are the engine's slots, so overlapping stages
  and shared occupancy are visible as they happen.

## The run summary

When a run completes, a summary overlay (also on `s`) recaps every step — tasks,
records, retries, cache hits, tokens, cost, p95 per stage — what each shared
value saved, and per-executor utilization. The header names the driver that ran,
and stage and task inspectors carry the shared prefix's cache economics.

## Forecast against actual

Feed the view a projection (`loom.Explain` on the same handler) and it shows the
forecast before the run starts, then reads every stage against it live — spend
versus projection in the header, and a projected-versus-actual reconciliation in
the summary. See [EXPLAIN.md](./EXPLAIN.md).

## The universe: more than one run

A process that runs several pipelines gets a **universe**: every run it has
produced, side by side, each one still whole and enterable — so a pipeline that
finishes while the next one starts is still there to read. A fleet does this by
construction, since it publishes every agent onto one bus.

```go
v := viz.New()                    // viz.New(viz.Retain(30)) to hold more
url, _ := v.Start("localhost:8077")

loom.Run(ctx, digest,   loom.WithEventHandler(v.Handle), ...)  // run 1
loom.Run(ctx, overview, loom.WithEventHandler(v.Handle), ...)  // run 2
```

Press `u` for the overview: every run in the process, named by its pipeline,
with how it ended, what it cost, and the shape of its stages — click one to enter
it. Each run keeps its own stages, tasks, executors, shared values, prompts, and
responses, so a finished pipeline stays as inspectable as the running one.

The live view follows new runs as they start, but never out from under you: if
you are reading a run — its summary open, or a task's prompts on screen — the new
run waits in the header (`◉ <pipeline> live →`) instead of replacing what you
were looking at. Events are routed by run ID, so pipelines running *concurrently*
on one handler land in their own skies rather than interleaving into one. The
universe is bounded (12 runs by default, `viz.Retain(n)` to change it) — runs are
held whole, so the oldest is dropped when a new one pushes past the limit.

## Keyboard

| Key | |
|---|---|
| `→` / `j`, `←` / `k` | step through tasks |
| `]`, `[` | step through stages |
| `e` | step through executors (`shift` reverses) |
| `f` | step through failed tasks |
| `b` | step through shared values |
| `m` | step through MCP servers |
| `s` | run summary |
| `u` | universe |
| `,` / `.` | previous / next run |
| `l` | jump to the run still live |
| `Esc` | close the overlay, or drop the selection |

## Examples

- `go run ./examples/constellation` — the view itself, plus the projection loop.
- `go run ./examples/research` — at scale: ~205 tasks, three model tiers, a
  branching DAG, scripted to show every visual state in one run.
- `go run ./examples/newsroom` — a fleet's universe (press `u`).
- `go run ./examples/multi-hop -view localhost:8077 -slow 900ms` — an iterative
  stage converging, drawn as orbits.
