# The algorithm seam

Loom had two extension points and neither of them was where algorithms go.

`executor.Executor` decides **where** a task runs — in-process, in a
subprocess, on a remote worker. `executor.OpRunner` decides **what** one task
does — render this prompt, call this model, parse this output. Between them
they cover the whole cost of a pipeline and none of its *shape*, because the
shape was fixed: walk the DAG once, forward, and stop.

That is one algorithm. It happens to be the right one for a lot of work, and
it is wrong for everything whose next step depends on the last one's answer.
Deep research reads a paper to learn which paper to read. Entity resolution
merges two records and changes what the merged record matches. A draft is
critiqued and revised until it is good enough. None of those fit in a pass,
and until now none of them had anywhere to live in Loom but inside a driver.

`algo.Algorithm` is that missing seam, and `pipeline.Iterate` is the operator
that drives one.

---

## 1. The interface

An algorithm decides, round by round, which records run next and what they
carry with them. That is all it decides.

```go
type Algorithm interface {
    Name() string
    Seed(g Graph) ([]Message, error)   // the messages that make round 0
    Route(r Round) ([]Message, error)  // the messages that make round N+1
}
```

```go
type Message struct {
    To   string   // receiving vertex; empty means "along my edges"
    From string   // sending vertex
    Body string   // what the receiver reads
    Rank float64  // orders an inbox, prunes a frontier
}
```

Returning no messages halts the computation. That is the only halt an
algorithm controls, and it is the one that means *converged*.

Everything expensive stays with the engine. An algorithm never schedules a
task, never touches a budget, never calls a model, never sees a provider. It
moves messages, and it is a pure function of the round that just finished —
which is why every test in `algo/algo_test.go` is a plain function call with
no model, no scheduler and no network in it.

### What a round looks like

```go
type Round struct {
    N     int      // superstep index
    Steps []Step   // one per vertex that ran, ordered by vertex ID
    Graph Graph    // the vertex table as it now stands, read-only
}

type Step struct {
    Vertex core.Record  // state after the step
    Before core.Record  // state before it — convergence by delta, not just by silence
    Inbox  []Message    // what this vertex consumed
}
```

The engine owns the vertex table; the algorithm reads it through `Graph` and
writes none of it. That split is the whole of the separation, and it is what
lets an algorithm consult a neighbour's current value without being able to
corrupt one.

## 2. The operator

```go
papers.Iterate("explore", pipeline.IterateSpec{
    Step: pipeline.InferSpec{            // the vertex program: an ordinary
        Binding: model.Binding{...},     // Infer stage, with the inbox in
        Prompt: `Paper {{.title}}        // template scope
{{if .Inbox}}Passed to you by papers that cite this one:
{{range .Inbox}}- {{.}}
{{end}}{{end}}
State what this contributes, and list what to follow.`,
        ParseJSON: true,
    },

    Algorithm: walk,                     // the control flow

    Halt: pipeline.HaltWhen{             // all three bounds, always
        MaxRounds: 5,
        Budget:    core.Budget{MaxCostUSD: 2},
    },

    Grow:        materialize,            // discovered vertices, or nil to close the world
    MaxInbox:    4,                      // messages one vertex reads per round
    MaxFrontier: 6,                      // vertices that run per round
})
```

From the outside it is one node of the pipeline: records in, records out, fed
by an upstream stage and feeding downstream ones. Inside it is a sequence of
rounds, and **a round is a stage batch**. Every active vertex's call is one
task through the same `Scheduler` the barrier and streaming drivers use, so
admission control, class-aware retry, the escalation ladder, the budget
governor, the content-addressed cache, lineage and the event stream all apply
to a superstep — not because they were extended to, but because a round is not
a new kind of thing.

`.Inbox` (message bodies) and `.Senders` (their origins, aligned by index) are
reserved for the stage's duration. A record already carrying one is rejected
before the loop starts rather than silently overwritten.

## 3. Why the loop gets cheaper as it runs

This is the part that makes iterating over paid calls affordable rather than
merely possible, and it comes from one decision:

> **A vertex's cache key is its state and its inbox — not the round it is in.**

Convergence *means* vertices stop changing. A vertex whose state and inbox are
unchanged has an unchanged key, so it costs nothing to run again. Cost per
round therefore **falls** as the computation settles, which is the opposite of
the usual economics of iterative model work, where context accumulates and
every round costs more than the last.

The engine goes one better than a cache hit. Before building a round's tasks it
checks whether each vertex has already run on exactly this (state, inbox); if
so the vertex is at a **local fixpoint** and is not scheduled at all. It votes
to halt with a witness. That costs one hash instead of one cache lookup, and
because the check is against every input the vertex has ever seen rather than
just the previous round's, it catches oscillation of any period — a two-cycle
repeats an input that comparing against the last round alone would miss.

When every vertex in a round quiesces, the loop stops and reports
`HaltFixpoint`: the next round could only have reproduced this one.

That contract has a cost, and it is worth stating. The vertex program must be
a function of (state, inbox). A step that consults the round number, a clock,
or a random source is not one, and it turns a priceable loop back into an
unbounded one. `.Round` is deliberately *not* offered in template scope for
exactly this reason: putting it in the record would put it in the cache key
and cost you the property above.

What that buys, end to end, from `examples/multi-hop`:

```
$ go run ./examples/multi-hop -state /tmp/hop
actually spent    2164 tokens / $0.0202 over 5 round(s), halted: quiet

$ go run ./examples/multi-hop -state /tmp/hop      # again
actually spent    0 tokens / $0.0000 over 5 round(s), halted: quiet
15 task(s) replayed from the result cache: rerunning a converged loop costs nothing
```

Edit one vertex and only what its change actually reaches recomputes — and
"reaches" means the messages it sends, not the record you touched. A vertex
whose state changed but whose message did not leaves its neighbours cached.
`TestIterateRecomputesIncrementally` is that claim as a test.

## 4. Halting: three bounds, always all three

```go
Halt: pipeline.HaltWhen{MaxRounds: 5, Budget: core.Budget{MaxCostUSD: 2}}
```

Quiet is implicit and is the algorithm's. The other two are the stage's, and
the compiler rejects a stage with no `MaxRounds` — a loop over paid,
non-deterministic calls with no bound on it is the one authoring mistake whose
cost is unbounded, and compilation is the last point at which catching it is
free.

Each condition covers the others' blind spot. Quiet alone does not terminate:
models do not converge monotonically, and two vertices can trade messages
indefinitely without either being wrong. A round cap alone does not bound cost:
rounds are not the same size, and the expensive one is usually the last. A
budget alone does not bound time.

Every loop reports which bound stopped it, because "it finished" and "it ran
out of money" produce identical records:

| `HaltReason` | Meaning |
|---|---|
| `quiet` | A round produced no messages. Converged. |
| `fixpoint` | Every vertex the round would have run had already run on that exact input. Converged, one round earlier. |
| `rounds` | Hit `MaxRounds`. **Did not converge.** |
| `budget` | Hit the stage budget or the run governor. **Did not converge.** |
| `failed` | A round could not complete. |

`HaltReason.Converged()` collapses that to the question usually being asked.

The stage budget is separate from the run governor on purpose. The governor
stopping means the run overspent and should return partial results; a stage
budget stopping means one loop did not converge inside what it was given, and
the rest of the pipeline should carry on with what it reached.

## 5. Explosion, and the two caps that are not the same cap

The failure mode of message passing over model calls is not a slow round, it is
a geometric one.

`BSPConfig.MaxMessages` caps what one vertex emits. That is necessary and it is
not sufficient: a thousand vertices each legally emitting two messages is a
two-thousand-call round. `IterateSpec.MaxFrontier` caps the round itself, which
is the one that bounds the bill — with it, the stage's worst case is
`MaxFrontier × MaxRounds` calls, a number that can be multiplied by a price
before anything runs.

`MaxInbox` is the third and it bounds a different resource: a high-degree
vertex's *prompt* grows with its degree until it exceeds the context window,
and the failure arrives as a provider error in round four rather than as a
planning problem in round zero. Capping is blunt and honest — truncation is
counted and reported. The sharp fix is to tree-reduce the inbox with `ReduceAI`
before the vertex sees it, which makes a round two stages instead of one.

## 6. Open worlds

A message may be addressed to a vertex the graph does not contain. That is the
interesting case — it is a vertex program choosing its own next input — and it
is off by default.

```go
Grow: func(id string, msgs []algo.Message) (core.Record, error) {
    return core.NewRecord(id, map[string]any{"ref": algo.Bodies(msgs)}), nil
},
```

Nil closes the world: such messages are dropped, **counted**, and reported.
That counting matters more than it looks, because "the search found nothing"
and "the search was not allowed to look" produce identical output.

Whatever `Grow` returns still runs under the stage's envelope — the same model
grants, the same deny-by-default egress allowlist, the same budget, assembled
once before the first round. Discovery widens what the computation *reads*,
never what it is allowed to *reach*. That asymmetry is what makes a
self-directed loop safe to run at all, and it is the reason this operator could
be built here and not on a generic DAG runner.

## 7. What ships

Three algorithms, deliberately three different shapes, because an interface
with one implementation has not been shown to be an interface.

### `algo.BSP` — bulk-synchronous message passing

Pregel with a model call as the vertex program. Messages travel along edges;
a vertex with an empty inbox does not run.

```go
walk, _ := algo.NewBSP(algo.BSPConfig{
    Edges: algo.EdgesFromField("cites"),      // adjacency from the records
    Emit:  algo.MessagesFromField("follow"),  // what to send — written by the model
    Seeds: func(r core.Record) bool { return r.ID == "p1" },
    MaxMessages: 3,
    Directed:    true,
})
```

The combination is the useful part: the author declares what *can* be reached
(edges), the vertex program decides what *is* reached (messages). A graph
traversal whose shape is discovered by the computation, inside an adjacency
someone bounded.

### `algo.Refine` — draft, critique, revise

The degenerate graph: one vertex, one self-edge. Records refine independently,
so a hundred drafts are a hundred concurrent self-loops each halting on its own
verdict — the stage's cost falls as the easy ones finish rather than being set
by the hardest.

```go
refine, _ := algo.NewRefine(algo.RefineConfig{
    Accept: func(r core.Record) bool { return r.String("verdict") == "ship" },
    Note:   func(r core.Record) string { return r.String("critique") },
})
```

`Accept` is an ordinary Go predicate and runs in the engine, not in a model.
That is deliberate: the thing deciding when to stop spending should not itself
be a call that costs money and can be talked out of its answer. A model's
judgement still drives it — have the step write the verdict and test it here.

### `algo.Beam` — frontier search

Beam search, one level up from decoding: the thing being expanded is a line of
reasoning rather than a token, and the thing scoring it is a model rather than
a logit. Every candidate proposes successors, all proposals are ranked
together, the best `Width` survive.

```go
beam, _ := algo.NewBeam(algo.BeamConfig{
    Width:    4,
    Expand:   algo.ExpandFromField("next", nil),   // ranked by the model's own score
    Terminal: func(r core.Record) bool { return r.String("done") == "yes" },
})
```

Pruning is where the money is: a beam of 4 over 5 rounds with 3 proposals each
is 20 calls, and the tree it searches has 243 leaves. Successor IDs are content
hashes, so two branches proposing the same candidate **merge** into one vertex
rather than forking — the second branch is a cache hit, and the agreement that
made them converge does not cost double. Provenance moves into the messages,
each naming its sender, which is a representation that can hold two parents
where a path string cannot.

## 8. Pricing a loop before running it

`loom.Explain` handles an iterative stage the only honest way available: the
round count is a property of the data and of a model's judgement about it, so
it prices **`MaxRounds` rounds** rather than guessing.

Round 0 is exact rather than assumed — an algorithm's `Seed` is a pure function
of the vertex table, so the projection calls it and counts the frontier it
returns. A stage seeded from one vertex of a thousand is projected as one call,
not a thousand.

The bound on later rounds comes from whichever of these applies: `MaxFrontier`
if set, the vertex count if the graph is closed, and **nothing at all** if the
stage can `Grow` and set no cap. In that last case the row is marked
`Estimated`, the projection reports itself `Partial()`, and the report stops
using the word *ceiling* — the same treatment `ParseJSON` already gets, for the
same reason: an under-count presented as a bound is precisely the failure this
tool exists to prevent.

```
stage                  model                      recs  calls   prompt   cached     exp($)     max($)    floor
explore                fast                          8     26     2643     1100     0.0132     0.0349       3s
synthesize             deep                          8      3      723       62     0.0504     0.1253       2s
TOTAL                                                      29     3366     1162     0.0636     0.1603       4s
```

The run that follows converges in five rounds and spends $0.0202 against that
$0.1603 ceiling. An over-estimate is the only safe direction for a number a
budget is set from.

## 9. Watching it converge

| Event | Carries |
|---|---|
| `round.started` | round number, active vertices, messages delivered |
| `round.finished` | round number, vertices that completed, usage |
| `stage.converged` | total rounds, final graph size, halt reason |

These are the only events describing a stage that runs more than once, and
they are what makes convergence *observable* rather than inferable: a frontier
that shrinks round over round is a computation settling, and one that does not
is the case the round cap exists for.

### In the constellation view

The view (`viz`) draws an iterative stage as **concentric orbits** — one ring
per superstep, round 1 innermost, the live ring dashed and turning while the
loop runs. Convergence is then something you watch rather than something you
reconstruct afterwards: the outer rings carry fewer stars than the ones inside
them, and a ring that does not thin is a loop that is not settling.

```sh
go run ./examples/multi-hop -view localhost:8077 -slow 900ms
```

Every task is stamped with the superstep it ran in — a round is a barrier, so
the round open when a task was scheduled is the round it belongs to — which is
what the rings are laid out from and what the task inspector shows.

The stage label carries the outcome (`explore ⟳5 · converged`), and the stage
inspector carries the mechanism: the per-round table of active vertices,
messages and cost, with bars whose widths are the frontier, under a plain
sentence about the halt.

```
SUPERSTEPS · 5                       SUPERSTEPS · 2
ROUND ACTIVE MSGS COST               ROUND ACTIVE MSGS COST
1     2      0    $0.0005 ▬▬▬        1     2      0    $0.0005 ▬▬▬
2     4      8    $0.0008 ▬▬▬▬▬▬     2     4      8    $0.0008 ▬▬▬▬▬▬
3     3      6    $0.0006 ▬▬▬▬▬
4     2      2    $0.0004 ▬▬▬        halted  rounds — hit the round cap —
5     1      1    $0.0002 ▬                  it did NOT converge

halted  quiet — a round produced no    frontier is still at its widest (4):
        messages; the computation      nothing has gone quiet yet — this
        converged                      loop was cut off, not finished

frontier peaked at 4, down to 1:
vertices are going quiet, so each
round costs less than the one before
```

Those two are the same pipeline over the same corpus, at `-rounds 5` and
`-rounds 2`. They return records that look alike; the reason is the only thing
that says one of them answered the question. It is coloured accordingly —
green for the two halts that mean converged, amber for the two that mean cut
off — because that distinction is the one a viewer must never have to infer.

`RunResult.Iteration(stage)` is the same thing after the fact:

```
explore  (bsp, 5 rounds, halted: quiet)
round      active   messages
0               2          0
1               4          8
2               3          6
3               2          2
4               1          1
9 vertices, 0 quiesced, 1 grown, 0 dropped, 0 truncated
1635 tokens, $0.0024
```

Grows while discovering, shrinks while converging. The counters are per round
because the useful signal in an iterative workload is a slope.

## 10. Writing your own

Implement two methods over plain data. Requirements, all of which the shipped
three satisfy and none of which need a framework to check:

1. **Deterministic.** No clock, no random source, no I/O. What `Route` returns
   participates in the next round's cache keys, so an algorithm that consults a
   clock silently turns a resumable computation into an unrepeatable one.
2. **Emit nothing to halt.** Convergence is silence.
3. **Rank what you want ordered.** Higher is better. Inboxes are delivered
   highest first and pruning keeps the highest.
4. **Let the engine own state.** Read the graph, return messages. Vertices are
   created, updated and retired by the engine.

The algorithm is deliberately **not** part of a stage's fingerprint. It decides
which vertices run and what they carry, and both of those are already in the
task's cache key, which is the record plus its inbox. Two stages whose steps
are identical genuinely perform the same operation on a given (state, inbox),
whatever routed the messages there — so they should share a cached result, and
hashing the algorithm in would break that reuse to record something no consumer
of the fingerprint needs.

## 11. What this is not

- **Not an agent framework.** No autonomy, no free-form tool loops, no agent
  choosing its own next action. A vertex program is a function of (state,
  inbox) that happens to be executed by a model.
- **Not an async swarm.** The barrier is the feature. Asynchronous agents
  calling each other freely is unbudgetable (no point at which spend is known),
  unreproducible (no barrier at which state is defined), and uninspectable (no
  round to show a user). A superstep buys all three back: each round has a
  price, a checkpoint, and a diff.
- **Not a graph database.** Edges live in records or in a broadcast adjacency
  table shared by hash. Loom computes over a graph; it does not store one.
- **Not general recursion.** Bulk-synchronous rounds over a vertex set. It is a
  restriction, and the restriction is what makes the thing priceable.

---

Read next: [ITERATION.md](./ITERATION.md) for why this primitive rather than
another, [ARCHITECTURE.md](./ARCHITECTURE.md) for the components it is built
on, and `examples/multi-hop` for it working end to end.
