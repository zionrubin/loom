# The missing dimension: iteration

Loom is Spark's core rebuilt for model calls, and it is complete at what it
set out to do: one pass over a dataset, declaratively, under a budget, with
least privilege and content-addressed reuse. `Map`, `Filter`, `FlatMap`,
`Combine`, `Infer`, `ReduceAI`, two drivers, three providers.

One pass is also the limit. Records enter a source, flow forward through a
DAG, and leave. Nothing in Loom can look at what a stage produced and decide
to go around again.

That is the gap this document is about, and it is not a small feature. It is
the same gap Spark closed when it passed Hadoop: MapReduce could express one
pass efficiently, and everything interesting — PageRank, ALS, k-means,
connected components — was a *loop*. Spark's decisive move was making
iteration cheap, and MLlib and GraphX are what that unlocked. Loom is at
exactly that point, one altitude up.

---

## 1. What one pass cannot express

Four workloads, and they are the four people actually want:

| Workload | Why one pass fails |
|---|---|
| **Deep research** | Reading a document tells you which document to read next. The set of inputs is discovered *by* the computation, not before it. |
| **Entity resolution** | Merging two records changes what the merged record matches. Resolution is a fixpoint, not a join. |
| **Knowledge-graph construction** | Extracting a claim is better with the neighbours' claims in context, which are themselves being extracted. |
| **Refine until good** | Draft, critique, revise. The number of revisions is a property of the draft, not of the pipeline. |

Three of those four are not merely iterative — they are iterative **over a
graph**, where what a vertex computes depends on its neighbourhood. That is
the shape, and it is worth naming precisely, because it tells you what
primitive to build rather than leaving you with "support loops somehow".

## 2. Why this is Loom's opportunity and not a generic one

Anyone can write a `for` loop around a model call. That is a weekend. What
nobody has is a loop that is *safe to run on a hundred thousand vertices with
a company credit card attached*, and the reason is that a loop over paid,
non-deterministic, rate-limited calls fails in four ways at once: it doesn't
terminate, it fans out geometrically, it re-pays for work it already did, and
it discovers its own next targets — which means it reaches parts of the
network nobody authorized.

Loom already has the answer to all four, built for other reasons:

1. **The budget governor** turns "loop until done" into "loop until done or
   $20", with partial results as a first-class outcome rather than a crash.
   An iteration count is the wrong bound; dollars are the real one.
2. **Content-addressed caching** is the mechanism that makes iteration
   affordable, and this is the part worth stopping on. Convergence *means*
   most vertices stop changing. An unchanged vertex with an unchanged inbox
   has an unchanged cache key, so it costs nothing to "run" again. **Cost per
   superstep falls as the computation converges** — the opposite of the usual
   economics of iterative LLM work, and a direct consequence of machinery that
   already exists and is already tested.
3. **Envelopes** matter far more in a loop than in a pass. A vertex program
   that follows a citation it discovered is a program choosing its own egress
   target. Deny-by-default allowlists and per-call broker-resolved secrets are
   the difference between that being a feature and being an incident.
4. **Lineage** is the only way to answer "why does the graph believe this"
   after six hops of model-derived inference. Without it, an iterated
   pipeline's output is unauditable by construction.

That is the vision in one sentence: **Loom is the only place where the loop
can be built safely, because everything a paid loop needs to be safe is
already here and load-bearing.**

## 3. The primitive

Bulk-synchronous message passing — Pregel — where the vertex program is a
model call.

```go
g := p.Graph("citations", pipeline.GraphSpec{
    Vertices: papers,                              // a Dataset
    Edges:    pipeline.EdgesFromField("cites"),    // adjacency from the record

    // The vertex program: one model call per active vertex per superstep.
    // Identical to InferSpec, with the inbox in template scope.
    Step: pipeline.InferSpec{
        Binding: model.Binding{Tier: model.TierFast,
                  Escalation: []string{"claude-sonnet-5"}},
        Prefix: `Research question:\n{{broadcast "question"}}`,
        Prompt: `Paper: {{.title}}
{{if .Inbox}}Findings passed to you by neighbours:
{{range .Inbox}}- {{.}}
{{end}}{{end}}
State what this paper contributes to the question. If judging it requires a
paper it cites, name that paper in "request".`,
        ParseJSON: true,
    },

    // What a vertex sends onward. Model-produced: the graph grows itself.
    Messages: pipeline.MessagesFromField("request"),

    Halt: pipeline.HaltWhen{
        Quiet:         true,                            // no vertex sent anything
        MaxSupersteps: 6,
        Budget:        core.Budget{MaxCostUSD: 20},
    },
})
```

Five semantics carry the design:

**A superstep is a stage.** Every active vertex's call is one task, submitted
through the same `Scheduler.RunTask` both existing drivers use. Admission
control, class-aware retry, the escalation ladder, the governor, the event
bus, the constellation view — all of it applies to supersteps for free, and
cannot drift, for the same reason the barrier and streaming drivers cannot
drift from each other today.

**Vote to halt.** A vertex with an empty inbox does not run. This is the rule
that makes the cost curve bend downward, and it is Pregel's, unmodified.

**The inbox is canonically ordered.** It has to be, or the cache key isn't
stable and the whole economic argument collapses. So message delivery sorts,
and that is a constraint on the API rather than an implementation detail.

**The cache key is (program fingerprint, vertex state, inbox).** Which gives
incremental recomputation with no new machinery: edit one paper and rerun, and
only that vertex recomputes — plus its neighbours, and only if its messages
actually changed. Purpose-built graph engines have subsystems for this.

**Three halt conditions, always all three.** Quiet, a superstep cap, and a
budget. Models do not converge monotonically; a pair of vertices can argue
forever. Any one condition alone is a way to lose money.

## 4. Why bulk-synchronous, and not an agent swarm

This is the deliberate part. Asynchronous agents calling each other freely is
the popular shape and the wrong one here: it is unbudgetable (no point at
which spend is known), unreproducible (no barrier at which state is defined),
and uninspectable (no round to show a user). The superstep barrier buys all
three back — each round has a price, a checkpoint, and a diff.

It is also the same trade Loom already made once and then relaxed on
evidence. The barrier driver came first; `WithStreaming` relaxed it where
occupancy paid for the loss of ordering (see
[INFERENCE.md](./INFERENCE.md#asynchronous-agents-continuous-batching)).
Supersteps should follow that order: barrier first, and an
overlapping-supersteps driver later, if measurements ask for it.

## 5. What is genuinely hard

Stated plainly, because a design that only lists its strengths isn't one:

- **Message explosion.** A vertex emitting 100 messages multiplies the next
  superstep. Needs a per-vertex `MaxMessages` and a per-superstep admission
  cap, and the projection has to show fan-out per round, not just totals.
- **Inbox growth versus context.** A high-degree vertex's prompt grows with
  its degree and eventually exceeds the context window. The fix already
  exists: tree-reduce the inbox with `ReduceAI` before the vertex program sees
  it. That makes a superstep two stages, not one.
- **Oscillation.** Two vertices can trade messages indefinitely without
  converging on anything. Detectable — an inbox equal to the previous
  superstep's is a fixpoint at that vertex — and worth detecting explicitly
  rather than leaving to the superstep cap.
- **Projecting an unknown number of rounds.** The honest answer is that
  `Explain` cannot know how many supersteps a graph will take, so for a graph
  it reports **cost per superstep** and **the ceiling at `MaxSupersteps`**.
  Which is precisely the number `HaltWhen.Budget` needs.

## 6. Where to start

Four steps, in dependency order. All four are now implemented; each section
below records what shipped and where it diverged from the plan.

### Step 0 — `loom.Explain` *(implemented)*

You cannot responsibly ship a loop over paid calls without pre-flight
pricing, and the projection is independently useful today, so it goes first.

```go
proj, err := loom.Explain(p, loom.WithRegistry(reg),
    loom.WithRunBudget(core.Budget{MaxCostUSD: 5}))
fmt.Print(proj)
```

```
projection  ticket-triage  (barrier driver, no calls issued)
stage                  model                      recs  calls   prompt   cached     exp($)     max($)    floor
tickets                                           2000      0        0        0     0.0000     0.0000       0s
classify               claude-haiku-4-5           2000   2000   198000   151924     0.9513     2.6213    3m33s
briefing               claude-opus-5              2000    287   386500      572    21.2017    49.8730   19m29s
TOTAL                                                    2287   584500   152496    22.1530    52.4943    23m2s
expected 967992 tokens for $22.1530; cannot exceed 1684276 tokens / $52.4943 before retries
run budget $5.0000 is below the ceiling: the governor will stop the run and return partial results
```

What makes the projection sharp rather than a guess is a property specific to
this framework: **a pipeline's cheap stages are ordinary Go functions and its
expensive stages are declarative data.** So `Explain` executes the cheap
skeleton for real — `Map`, `Filter`, `FlatMap` and `Combine` actually run —
and models only the paid calls. Record counts are exact rather than
extrapolated from a selectivity guess, and every prompt is measured after
rendering against the record that will produce it, including the prompt-cache
split, because the projection reuses the planner's own break-even rule.

Two numbers per stage, and the distinction between them is the point:

- **Expected** rests on one stated assumption — the share of `MaxTokens` a
  response fills, `WithExpectedOutput`, default 0.35. It is an estimate and is
  labelled as one.
- **Ceiling** rests on nothing. `MaxTokens` is a cap the provider enforces, so
  a first attempt cannot cost more than this. It is the number to hand
  `WithRunBudget`, and the reason the report ends by comparing the two.

`Explain` also inherits the compiler, so it is a free validation pass:
unregistered models, unparseable templates, and broadcasts a stage reads
without declaring all surface before anything is provisioned. Whatever it
cannot compute — a response's length, the fields a `ParseJSON` stage
introduces, the output of a source function it deliberately refuses to invoke
— becomes a named warning rather than a confident wrong number.

One of those deserves singling out, because it is the only way a projection can
be wrong in the direction that costs money. A `ParseJSON` stage's field names
come out of the model, so a `Filter` below it testing one of them drops every
record during projection while keeping them in the real run, and everything
past it projects as no work at all. That is an *under*-count, and an
under-count presented as a ceiling is precisely the failure this tool exists to
prevent — so those stages are marked, `Projection.Partial()` reports the
projection as incomplete, and the report stops using the word ceiling.
`loom.WithStageSample` names the fields and makes it exact again.

The same projection is published on the event bus (`stage.projected`,
`run.projected`), so pointing `Explain` and `Run` at one handler gives the
constellation view both halves: the forecast while the sky is still empty, then
every stage read against it as the run fills in. That pairing is what makes the
next step tractable — a loop's cost is only legible as *per-round projected
versus actual*, and the machinery for that comparison now exists.

The example above earns its keep immediately: the reduce tree costs **22×**
the classification stage that feeds it, which is not where anyone looks first.

### Steps 1 and 2 — the loop and the graph operator *(implemented)*

These arrived together rather than in sequence, and the reason is worth
recording: the fixpoint driver was proposed as a way to settle iteration's
semantics with no graph in the way, and building it revealed that the graph was
never the hard part. Cache keys, per-round charging, halting and round events
are settled the same way whether a message travels along an edge or back to the
vertex that sent it. So instead of a third driver, iteration became a third
*operator* — `pipeline.Iterate` — and the part that varies between a fixpoint
and a graph traversal was factored out into a seam.

That seam is `algo.Algorithm`: two methods over plain data that decide, round
by round, which records run next and what they carry. The engine keeps
everything expensive — task building, scheduling, budget, caching, quiescence,
halting — and the algorithm keeps only the routing. Three ship, covering three
shapes: `BSP` (message passing over a graph, the primitive sketched in §3),
`Refine` (a vertex looping on itself — step 1's fixpoint driver, now a
nine-line algorithm rather than a driver), and `Beam` (frontier search that
grows its own graph).

The full design is in [ALGORITHMS.md](./ALGORITHMS.md). What §3 called
`p.Graph(...)` is `p.Iterate(...)` with `algo.NewBSP(...)`, which is that
sketch with the control flow pulled out of it.

Everything §5 called genuinely hard is handled, and one of them better than
proposed:

- **Message explosion** — `BSPConfig.MaxMessages` per vertex and
  `IterateSpec.MaxFrontier` per round. The second is the one that bounds the
  bill; the first alone does not, since a thousand vertices emitting a legal two
  messages each is a two-thousand-call round.
- **Inbox growth** — `IterateSpec.MaxInbox`, with truncation counted and
  reported. The tree-reduce remains the sharp fix and remains a ReduceAI stage.
- **Oscillation** — better than the superstep cap this document settled for. A
  vertex's (state, inbox) is hashed against *every* input it has run on, so a
  repeat is a local fixpoint and the vertex is not scheduled at all. That costs
  one hash rather than one cache lookup, and catches cycles of any period — a
  two-cycle repeats an input that comparing against the previous round would
  miss. A round in which every vertex quiesces halts as `fixpoint`.
- **Projecting an unknown number of rounds** — exactly as this document
  proposed: `Explain` prices `MaxRounds` rounds, with round 0's frontier
  computed exactly by calling the algorithm's `Seed` (it is a pure function, so
  the projection can just ask). A stage that can grow its graph and caps nothing
  is marked as unbounded rather than given a confident number.

### Step 3 — the proving example *(implemented)*

`examples/multi-hop` walks a citation graph to answer a research question,
with the traversal chosen by the model rather than declared. It reaches a paper
three hops from the seeds that no seed cites, and the synthesis rests on it —
a claim no single pass could have produced. One cited paper is outside the
corpus and the walk creates it. The frontier goes 2 → 4 → 3 → 2 → 1: growing
while discovering, shrinking while converging.

```
$ go run ./examples/multi-hop -state /tmp/hop
actually spent    2164 tokens / $0.0202 over 5 round(s), halted: quiet

$ go run ./examples/multi-hop -state /tmp/hop      # again
actually spent    0 tokens / $0.0000 over 5 round(s), halted: quiet
15 task(s) replayed from the result cache
```

Still open, and still a natural addition: the constellation view drawing
supersteps as concentric orbits, so convergence is something you *watch* — the
outer rings thinning as vertices go quiet. The events it needs now exist
(`round.started`, `round.finished`, `stage.converged`), so this is a view
change rather than an engine one.

## 7. What this is deliberately not

- **Not an agent framework.** No autonomy, no free-form tool loops, no
  agents choosing their own next action. A vertex program is a pure function
  of (state, inbox) that happens to be executed by a model.
- **Not an async swarm.** See §4. The barrier is the feature.
- **Not a graph database.** Edges live in records, or in a broadcast adjacency
  table shared by hash. Loom computes over a graph; it does not store one.
- **Not general recursion.** Bulk-synchronous supersteps over a vertex set,
  which is a restriction, and the restriction is what makes the thing
  priceable.

---

Read next: [ALGORITHMS.md](./ALGORITHMS.md) for the seam this design became and
how to write an algorithm for it, [ARCHITECTURE.md](./ARCHITECTURE.md) for the
components it builds on, and [INFERENCE.md](./INFERENCE.md) for the
serving-engine lineage that produced the caching and batching machinery the loop
depends on.
