# Running more than one pipeline, and running one faster

- [Fleets](#fleets)
- [The blackboard](#the-blackboard)
- [Streaming](#streaming)
- [Batching and parallelism](#batching-and-parallelism)
- [The constellation view](#the-constellation-view)

## Fleets

`Run` provisions a rate limiter, a budget governor, a result cache, and a set
of execution slots, then releases them. That is the right scope for one
pipeline. When a process runs several at once those same things need to be
*shared* — one quota, one ceiling, one cache, one bounded set of slots
scheduled fairly between them. That is a `Fleet`.

A budget enforced once per pipeline is not a budget on the work; it is a budget
multiplied by however many pipelines happen to be running. Same for a rate
limiter: a second copy of a connection is a second copy of its quota.

```go
fleet, err := loom.NewFleet(
    loom.WithRegistry(reg),
    loom.WithWorkers(slots),                                  // the fleet's slots
    loom.WithFleetBudget(core.Budget{MaxCostUSD: budget}),    // the fleet's ceiling
    loom.WithContinueOnError(),
    loom.WithTopic("findings"),                               // declare a board up front
    loom.WithBroadcast("style-guide", styleGuide),
)
if err != nil {
    log.Fatal(err)
}
defer fleet.Close()

desk := fleet.Go(ctx, wirePipeline())        // launch, don't block
beat := fleet.Go(ctx, beatPipeline("tech"))
res, err := desk.Wait()                      // *RunResult when it finishes
err = fleet.Wait()                           // or wait for all agents
```

| Call | Purpose |
|---|---|
| `fleet.Go(ctx, p, opts...) *Agent` | Launch an agent; returns immediately |
| `fleet.Run(ctx, p, opts...)` | Launch and wait |
| `agent.Wait() (*RunResult, error)` / `agent.Done() <-chan struct{}` | Per-agent completion |
| `fleet.Wait() error` | All agents |
| `fleet.Slots()` / `fleet.Spent()` | Live capacity and spend |
| `fleet.Explain(p, opts...)` | Project one agent against the fleet's config |
| `fleet.Report()` | `FleetReport` — `.String()`, `.Duration()`, `.Occupancy()` |
| `fleet.Close()` | Release everything |

`Run` is a fleet of one, built through the same path, so the two cannot drift
apart.

**Admission fairness.** A contended slot goes to the agent that has been served
least, so a short agent overtakes a long one instead of queueing behind it —
three short agents launched after a 60-task sweep can finish in half its time.
`loom.WithAdmissionAging(rate)` bounds the other side: an agent is held back by
at most its own attained service divided by that rate, however many fresh
agents arrive while it waits. Raise it for a fleet agents keep joining
indefinitely; leave it alone for a fleet launched together that drains.

## The blackboard

Agents coordinate by posting to topics rather than calling each other.

```go
loom.WithTopic("findings")               // declare, so a first reader finds an empty board

v, err := fleet.PostFrom("beat-tech", "findings", map[string]any{...})
v, err := fleet.Post("findings", value)

posts, err := fleet.Await(ctx, "findings", len(beats))   // block until n posts
fleet.Posts("findings") []Post
fleet.Values("findings") []any
fleet.Topics() []string
```

A topic **grows**; a broadcast is read-only for the fleet's whole life. Use a
broadcast for a style guide or taxonomy, a topic for what agents discover.
Posting declares a topic too — declaring up front is for the agent that runs
first, so it finds an empty board rather than failing to compile against a name
nobody has defined yet.

`examples/newsroom` runs six agents on one engine with a blackboard.

## Streaming

`loom.WithStreaming()` replaces the stage-barrier driver with pipelined
execution: a record becomes eligible for the next stage the moment its own task
completes, instead of when its whole stage does.

Pipeline, planner, envelopes, caching, and recovery are identical — only the
driver changes. What changes observably is occupancy and latency: downstream
stages start while upstream ones are still running, so a slow task no longer
idles the workers behind it, and the first end-to-end result arrives without
waiting for the widest stage to drain.

**The tradeoff is ordering.** Records flow in completion order, not input
order, so a stage's outputs no longer line up positionally with its inputs. Use
the default barrier driver when output order is part of the contract. `Combine`
and `ReduceAI` need the whole dataset, so they remain barriers either way.

`loom.WithBatchWait(d)` (default 25ms) bounds how long a streaming stage with
`WithBatchSize` waits for a partial batch to fill — without it, the last few
records of a stream would wait forever for a group that never arrives.

## Batching and parallelism

| Knob | Scope | Use |
|---|---|---|
| `loom.WithWorkers(n)` | Run/fleet | Default concurrency (8) |
| `pipeline.WithParallelism(n)` | Stage | Override for one stage |
| `pipeline.WithBatchSize(n)` | Stage | n records per task — fewer, larger tasks |
| `pipeline.WithBudget(b)` | Stage | Per-*task* timeout, attempts, tokens, cost |

Concurrency above the provider's limit does not help: admission control holds
the extra work in a queue where you can see it. `proj.AdmissionFloor()` is the
wall-clock floor that no amount of concurrency gets under — if it dominates
your projection, the fix is a higher `RequestsPerMinute`, a different tier, or
fewer calls, not more workers.

`WithBatchSize` trades granularity for overhead: bigger batches mean fewer task
boundaries but coarser caching and retry, since a batch fails and replays as a
unit.

## The constellation view

A live web view of a run as a sky of task/executor stars — pulsing while
running, ringed when slow, flashing on completion, red on failure; click a star
for model, input, tokens, cost, retries, and logs. An iterative stage draws as
concentric orbits, one ring per superstep.

```go
v := viz.New()
url, err := v.Start(addr)      // e.g. ":8077"
fmt.Printf("constellation view: %s\n", url)

// Optionally wait for a browser; the page replays state on connect either way.
v.AwaitViewer(ctx)

loom.WithEventHandler(v.Handle)   // that is the entire integration
```

It is just an event handler, so it works for a `Run` or a whole `Fleet`, and
costs nothing when nobody is watching. `examples/constellation` is the small
version; `examples/research` scales it to ~205 tasks with retries, a straggler,
escalations, and a dead letter, all offline.

For editing a pipeline against a live projection rather than watching one run,
see `docs/STUDIO.md` — the studio compiles a document into a
`pipeline.Pipeline` and can export it as Go.
