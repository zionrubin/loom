# Iterative stages

`Iterate` is the operator for the four workloads one forward pass cannot
express: research that discovers its own next document, entity resolution
whose merges change what matches, knowledge extraction that improves with its
neighbours, and refine-until-good. All four are fixpoints, three of them over
a graph.

From outside, the stage is an ordinary node: records in, records out. Inside,
it is a sequence of rounds, and every round is an ordinary batch of tasks
through the ordinary scheduler — so admission control, class-aware retry, the
escalation ladder, the budget governor, the cache, lineage, and events apply
to a round exactly as they apply to a stage.

- [What makes the loop affordable](#what-makes-the-loop-affordable)
- [IterateSpec](#iteratespec)
- [Choosing the algorithm](#choosing-the-algorithm)
- [Bounding it](#bounding-it)
- [Reading the result](#reading-the-result)
- [Writing your own algorithm](#writing-your-own-algorithm)

## What makes the loop affordable

A vertex's cache key is **its state plus its inbox — not the round it is in**.
That is the whole economic argument. Convergence *means* vertices stop
changing, so a converged vertex's key stops changing too, and the cost of a
round falls as the computation settles instead of climbing as context
accumulates. The engine goes further: a vertex that has already run on exactly
this `(state, inbox)` has reached a local fixpoint, so it votes to halt rather
than paying a cache lookup to rediscover its own answer.

This rests on the vertex program being a function of `(state, inbox)`. A step
that consults the round number, a clock, or a random source is not one, and it
turns a priceable loop back into an unbounded one. The same applies to the
algorithm: `Route` must be deterministic and side-effect free, because what it
returns participates in the next round's cache keys.

## IterateSpec

```go
d.Iterate("explore", pipeline.IterateSpec{
    Step:        pipeline.InferSpec{...},  // the vertex program — one call per active vertex per round
    Algorithm:   walk,                     // required: what each vertex sends, and where
    Halt:        pipeline.HaltWhen{...},   // required: MaxRounds > 0, optional stage Budget
    Grow:        func(id string, msgs []algo.Message) (core.Record, error),  // optional: open world
    MaxInbox:    4,                        // cap messages one vertex reads per round (0 = uncapped)
    MaxFrontier: 6,                        // cap vertices running per round (0 = uncapped)
}, pipeline.WithBroadcast("question"))
```

`Step` is a plain `InferSpec` — bindings, escalation ladders, a cached
`Prefix`, `ParseJSON`, `Validate` all work unchanged. Its template
additionally sees the inbox:

```gotemplate
{{if .Inbox}}What your neighbours told you:
{{range .Inbox}}- {{.}}
{{end}}{{end}}
```

plus `{{.Senders}}`, the sending vertex IDs by index. Those two names are
reserved for the stage's duration (`algo.Reserved()`), and a record already
carrying one is rejected before the stage runs rather than silently
overwritten.

**Record IDs are load-bearing here** in a way they are not elsewhere: a message
names its destination, so duplicate IDs in the input are rejected rather than
merged.

**`Grow` is the open-world switch.** A message can address a vertex the graph
does not contain — a paper citing something outside the corpus. Nil drops such
messages (counted and reported); a `Grow` function materializes the vertex from
the messages that reached it. This is where a program following a reference it
invented stops being the model's decision and becomes yours. Whatever `Grow`
returns still runs under the stage's envelope: same grants, same deny-by-default
egress, same budget. Discovery widens what the computation *reads*, never what
it is allowed to *reach*.

## Choosing the algorithm

### BSP — message passing over a graph

For diffusion, multi-hop traversal, and anything where vertices inform their
neighbours.

```go
walk, err := algo.NewBSP(algo.BSPConfig{
    Edges:       algo.EdgesFromField("cites"),      // required
    Emit:        algo.MessagesFromField("follow"),  // default: field "messages"
    Seeds:       func(r core.Record) bool { return r.ID == "p1" },  // nil = every vertex starts
    MaxMessages: 3,      // per-vertex cap per round (0 = none)
    Directed:    true,   // default; false delivers both ways
})
```

The useful split: **edges come from the records, messages come from the model.**
A paper's own `cites` field carries the graph; `follow` is a field the step
writes, which makes the walk's shape discovered rather than declared.
`EdgesFromTable(map[string][]string)` covers a graph known up front.

`Directed` defaults to true and has to be asked for — undirected is usually
what a "related to" field means and rarely what a "cites" field means.

### Refine — a record that critiques itself

Independent self-loops, each halting when its own critique passes, so the
stage's cost falls as the easy ones finish rather than being set by the
hardest.

```go
loop, err := algo.NewRefine(algo.RefineConfig{
    Accept: func(r core.Record) bool { return r.String("verdict") == "ok" },  // required
    Note:   func(r core.Record) string { return r.String("critique") },  // default
    Carry:  false,   // keep only the newest note
})
```

`Accept` is an ordinary Go predicate running in the engine, not a model call —
deliberately. The thing deciding when to stop spending should not itself cost
money and be talkable out of its answer. Have the step write a score or verdict
field and test *that* here.

`Carry` is off by default because the usual failure of a refine loop is a
prompt that grows until it drowns the draft.

### Beam — search that keeps the best k

```go
search, err := algo.NewBeam(algo.BeamConfig{
    Width:    4,                                        // required — sets the cost
    Expand:   algo.ExpandFromField("next", nil),        // required
    Terminal: func(r core.Record) bool { ... },         // nil = nothing is terminal
})
```

`Width` is the single number that sets the stage's cost: the frontier never
exceeds it, so the search costs at most `Width` calls per round. A beam of 4
over 5 rounds with 3 proposals each is 20 calls against a tree with 243 leaves.
A candidate reached twice by different paths gets the same content-derived ID,
so the second path is a cache hit rather than a second bill.

## Bounding it

```go
Halt: pipeline.HaltWhen{
    MaxRounds: 6,                                   // required, positive
    Budget:    core.Budget{MaxCostUSD: 0.50},       // checked between rounds
}
```

All conditions apply at once, and that is the design rather than an abundance
of options:

- **Quiet alone does not terminate.** Models do not converge monotonically, and
  two vertices can trade messages indefinitely without either being wrong.
- **A round cap alone does not bound cost.** Rounds are not the same size, and
  the expensive one is usually the last.
- **A budget alone does not bound time.**

The stage budget is separate from the run budget rather than a share of it: the
run governor stops the *whole run* and returns partial results, which is right
for a run that overspends and wrong for a loop that simply did not converge.
This stops the loop and lets the pipeline continue with what it reached.

Two more caps, and they do different jobs:

- **`MaxInbox`** caps how many messages one vertex reads, keeping the
  highest-ranked. A high-degree vertex is where an iterative model workload
  dies — its prompt grows with its degree until it exceeds the context window,
  and the failure arrives as a provider error in round four rather than a
  planning problem in round zero. Truncation is reported per round. The sharp
  fix is to tree-reduce the inbox before the vertex sees it, which makes a round
  two stages instead of one.
- **`MaxFrontier`** caps the round itself, and it is the half that bounds the
  bill. A per-vertex message cap does not: a thousand vertices each legally
  emitting two messages is a two-thousand-call round. With it, the stage's worst
  case is `MaxFrontier × MaxRounds` calls — a number `loom.Explain` can multiply
  by a price before anything runs.

## Reading the result

```go
it, ok := res.Iteration("explore")
fmt.Print(it.String())   // per-round shape and the halt reason
```

| Field | Meaning |
|---|---|
| `Rounds` | Supersteps that actually ran |
| `Halt` | `HaltQuiet`, `HaltFixpoint`, `HaltRounds`, `HaltBudget`, `HaltFailed` |
| `Active []int` | Vertices that ran in each round |
| `Delivered []int` | Messages delivered into each round |
| `Quiesced` | Vertices skipped unpaid at a local fixpoint |
| `Grown` | Vertices the computation discovered and created |
| `Dropped` | Messages to vertices that did not exist, with no `Grow` |
| `Truncated` | Messages discarded by `MaxInbox`/`MaxFrontier` |
| `Vertices` | Graph size when the loop stopped |
| `Usage` | What the whole loop cost |

`it.Halt.Converged()` is true for `HaltQuiet` and `HaltFixpoint` only — that is
the assertion a test should make. `HaltRounds` or `HaltBudget` means the loop
was **cut off**, not finished, and the distinction is the point of reporting it.
Non-zero `Dropped` means the algorithm is reaching for an open world inside a
closed one: add `Grow` or fix the edges.

## Writing your own algorithm

Two methods over plain data — no model, no scheduler, so it unit-tests
directly:

```go
type Algorithm interface {
    Name() string
    Seed(g Graph) ([]Message, error)      // round 0's frontier
    Route(r Round) ([]Message, error)     // next round's messages; none = halt
}
```

`Round` gives you `N`, `Steps` (each with `Vertex` after, `Before`, and the
`Inbox` it consumed), and `Graph`. The `Before`/`Vertex` pair is what lets an
algorithm test convergence by *delta* ("this value moved less than epsilon")
rather than only by silence.

Helpers in `algo`: `Sort`, `GroupByTo`, `Bodies`, `Senders`, `Fanout`, `Cap`,
`Strings`, `Number`, `EdgesFromField`, `EdgesFromTable`, `MessagesFromField`,
`Successor`, `ExpandFromField`.

`algo.Sort` is not cosmetic — canonical message ordering is what makes an inbox
a *value* rather than a sequence of arrivals, and therefore what makes it a
stable cache key.

Working example: `examples/multi-hop` walks a citation graph the model chooses
as it goes, with an open world and all three bounds. `docs/ITERATION.md` and
`docs/ALGORITHMS.md` cover the reasoning in depth.
