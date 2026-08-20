# Model routing: the escalation ladder as policy

Loom already refuses to pay twice for four kinds of work:

```
result cache        same computation?         don't execute it again
findings commons    same question?            don't research it again
prompt prefix       same head of a prompt?    don't prefill it again
delta state         same evolving object?     don't process it again
```

Every one of them is about a call you *made* and would otherwise make again.
There is a fifth kind of waste and it has the opposite shape — a call you have
not made yet, and already know will fail:

```
model routing       a call that cannot succeed?   don't make it at all
```

Nothing above catches it. A failed call produces no result, so there is nothing
to cache; it asks nothing about the world, so the commons cannot serve it; it
is a real call, so the prefix cache dutifully makes it cheaper. The only thing
that avoids it is not issuing it.

---

## 1. The call that was always going to fail

A stage with an escalation ladder already has a recovery story:

```go
Binding: model.Binding{
    Tier:       model.TierFast,
    Escalation: []string{"claude-sonnet-5"},
},
Validate: func(r core.Record) error { ... },
```

The fast model runs, its output fails `Validate`, the scheduler climbs. That
story is correct and it is expensive, because it is *reactive*. Every record
enters at the bottom rung, so every record the fast model cannot handle pays
twice: once for the call that was always going to fail, and once for the call
that answers.

Put numbers on it. A stage where 40% of records escalate spends 1.4 cheap calls
per record to buy 1.0 answers. The 0.4 is not recoverable by anything already
in Loom, and it does not shrink with scale — the ladder is stateless, so record
10,000 enters where record 1 did, having learned nothing from the 9,999
verdicts in between.

Those verdicts are the interesting part.

## 2. The labels already exist

`InferSpec.Validate` is an oracle. It has already been written, it already runs
on every record, and it already answers exactly the question a router needs:
*was the model that ran strong enough for this input?*

- A semantic failure is a labelled negative.
- A success is a labelled positive.

Loom threw both away. Keeping them is the whole feature. Nothing needs to be
trained, annotated, or configured, because the training signal is a by-product
of work the pipeline was already doing — and the oracle is the user's own
definition of correct, not a proxy for it.

```go
res, err := loom.Run(ctx, p,
    loom.WithRegistry(reg),
    loom.WithStateDir("./state"),
    loom.WithRouting(route.Config{Features: route.ByField("kind")}),
)
```

That is the whole API. `route.ByField("kind")` is worth more than every other
knob combined, and §6 says why.

## 3. What a router may and may not do

A `Router` chooses a **starting rung** and nothing else. Validation still runs,
escalation still climbs, the top of the ladder is still the ceiling. Three
consequences follow, and together they are the reason this is safe to leave on:

**A wrong guess costs work, never an answer.** Routing a record too low costs
the failed call a flat ladder would have paid anyway. Routing it too high costs
the price difference between two models. Neither can produce output that would
not have passed the same `Validate`. This is the same guarantee shape as the
delta certificate — a failed fast path costs work and never a different answer
— and it is why the router has no ability to suppress, weaken, or shortcut the
validator.

**Routing cannot exceed the projection.** Starting at rung *k* walks rungs
*k..n*, which is a subset of the rungs *0..n* a flat ladder walks. So the
ceiling `loom.Explain` reported — and the budget handed to `WithRunBudget` on
the strength of it — bounds a routed run without being recomputed.

**A cold router is today's behaviour.** With no evidence for a bucket, the
router returns rung 0 and says so. Switching routing on can begin to save; it
cannot begin to cost.

## 4. The decision

`route.Adaptive` keeps a Beta posterior per (stage, bucket, rung) over "output
at this rung passed validation", and picks the rung minimising the expected
cost of *reaching a valid answer*, by backward induction over the ladder:

```
E[last] = price(last)
E[i]    = price(i) + (1 − p̂(i)) · E[i+1]
```

That is the entire model. It is worth reading once more, because it does not
say what people assume it says. It is not "skip a rung that fails often" — it
is arithmetic over prices, and the prices usually win:

| bottom rung fails | next rung costs | start at |
|---|---|---|
| 70% of the time | 1000× the bottom | the bottom — the gamble is nearly free |
| 70% of the time | 1.1× the bottom | the next — the cheap call is a toll |
| 3% of the time | 15× the bottom | the bottom, obviously |

A rung that fails most of the time is still the right place to start when the
rung above it costs a thousand times more, and a rung that usually works is the
wrong place to start when skipping it saves almost nothing. Only the expected
cost knows which case you are in.

**Determinism.** Decisions are a pure function of the profile and the request
key — nothing reads a clock or a global random source. A task asked twice
routes twice the same way, two workers holding one profile agree, and a report
says the same thing when it is regenerated. That is why `Adaptive` minimises
expected cost under the posterior *mean* rather than Thompson-sampling the
posterior: sampling would explore more smoothly and would make every decision
unreproducible.

## 5. The probe, and why the report never omits it

Exploration lives somewhere else, and it is the most important part of the
design.

Once a bucket is routed upward, the bottom rung stops being sampled — and the
estimate that sent it there can never be contradicted. That is not a
theoretical worry; it is a system that quietly congratulates itself. "Skipped
412 calls" is a statement about calls that were never made, and there is
nothing in the run that could ever show it to be wrong.

So `Adaptive` keeps a **probe**: a deterministic fraction (default 5%) of the
tasks it would have routed up are started at the bottom anyway. Probes cost
real money and they buy the only thing that turns the saving into a
measurement — an unbiased bottom-rung success rate on the population actually
being routed.

The run report therefore prints the two together, always:

```
routing: 174 task(s) started above the bottom rung, skipping 174 call(s)
         worth $0.0099; 6 probe(s) held back, 0 answered at the bottom
         (0% of skips would have been wrong)
```

The gross figure and its correction. Never the first without the second.

**Dollars are measured, not estimated.** The figure above is priced from calls
that actually ran on the skipped models. The scheduler holds only the planner's
`EstTokens`, which reserves each call's *maximum* output for admission control
and so overstates a real call several times over; pricing a saving from it
would put a number in the report that the run's own cost column contradicts.

### Routing and the result cache

A task's cache key is its op fingerprint and its input content. The rung it ran
on is no part of either, so a routed run replays a flat run's results and vice
versa — routing decides how to produce an answer, not what the answer is.

This is already true of escalation today (a record that escalates caches the
stronger model's output under the same key), and routing inherits it rather
than adding a new case. It is worth stating because the obvious "improvement" —
folding the resolved model into the key — would look like a correctness fix and
would in fact halve the cache's hit rate on every stage with a ladder.

## 6. Buckets are the whole game

A bucket is the unit of generalization: records in one bucket are assumed alike
in difficulty, so a verdict about one is evidence about the rest.

The default featurizer, `route.SizeBucket`, buckets by the task's estimated
token count in powers of two. Length is a real and free proxy for difficulty,
and it is almost never the best one available. A pipeline usually knows what
makes its records differ:

```go
route.ByField("tier")      // support tickets: enterprise tickets are harder
route.ByField("language")  // extraction: the model is weaker outside English
route.ByField("doc_kind")  // contracts: dense ones need the bigger model
```

Two failure modes to recognize in the report:

- **One bucket** means the featurizer separates nothing, so routing can only
  move the whole stage or none of it — and a whole stage that should move is a
  binding you should just edit. `LadderProjection.Buckets` reports this.
- **Too many buckets** means no bucket reaches `MinSamples` and the router
  stays cold forever. `Routing.Cold` in the run result reports this.

A featurizer must be cheap and deterministic. It runs before every task on the
scheduler's hot path, and one that called a model to judge difficulty would
cost the thing it is trying to save.

## 7. Where the estimate is biased

Two biases, one benign and one removed. Both are stated because a cost claim
you cannot audit is not a cost claim.

**Upper rungs are pessimistic, which is the safe direction.** Observations at
rung *i > 0* come mostly from tasks that *escalated* into it, and a task that
escalated is by construction one the rung below could not handle — a harder
subpopulation than the rung would see if records arrived directly. So p̂ for
upper rungs runs low, which raises their expected cost, which biases the router
toward the bottom of the ladder — toward the behaviour Loom has without a
router. The bias is real and it points the safe way.

**Bucket weights were biased, and are not any more.** An early version weighted
a stage's buckets by their bottom-rung verdict counts, which is exactly wrong:
a bucket the router has moved stops producing bottom-rung verdicts, so the
weighting shrank precisely the buckets routing was working on — the expensive
ones. On the example pipeline it reported a bucket at 23% of the stage that was
really 67% of it. Rung counts record `Starts` — the tasks that *entered* the
ladder at that rung, as opposed to climbing into it — and a bucket's weight is
their sum, which is its record count exactly.

## 8. Across runs, and across a fleet

A `route.Profile` is plain serializable data: for each stage, each bucket, and
each rung, how often the output passed. Given a state directory Loom appends
what a run learned beside the result cache, so the calibration a run pays for
is what the next one starts with.

The file is append-only and additive, for the same reason the cache index is:
several processes calibrating one pipeline write concurrently, and a
last-writer-wins total would silently discard whichever fleet member finished
first. Summing lines makes the merge associative, so the file means the same
thing whatever order the appends land in.

Persistence writes a **contribution**, not a total — `Adaptive.Learned()` holds
only what this router observed, separately from the profile it was seeded with,
so a run that started from disk does not write its seed back out and count it
twice.

On a fleet the router sits on the host beside the cache and the rate limiter,
because it is the same kind of thing: what a stage's records cost to get right
is a property of the work, not of the pipeline that happened to discover it.
Every agent shares one, so what the first agent pays to learn about a stage
routes the records of the agents that follow.

## 9. What this does to `loom.Explain`

A projection's columns price **one call per record at the base model**. The
ceiling says "before retries" and means it. So a stage with an escalation
ladder has always been under-projected by exactly the escalations, and no
amount of staring at the columns would show it.

A profile is what makes them computable. Given one, `Explain` reports a ladder
line per stage:

```
extract ladder (126 verdicts over 2 bucket(s)): flat $14.9433 → routed $12.4227, saving $2.5206 (17%)
  mock-fast 3000→1000 calls, mock-deep 1938→2010 calls
```

Three numbers that were not available before: what the escalations cost, what
routing them saves, and how the calls divide between the models.

Read the second line carefully, because it says something the headline hides:
routing makes **more** calls on the expensive model, not fewer. It sends every
dense record straight to `mock-deep`, where the flat ladder let the cheap model
occasionally get one right. That is the cost side of a skip, it is real, and it
is exactly what the probes in §5 measure. It comes out ahead here because 2,000
avoided cheap calls outweigh 72 added expensive ones at a 4× ratio — and on a
50× ladder the same trade would go the other way, which is why the decision is
arithmetic over prices rather than a rule about failure rates. The forecast
runs through the same estimator the scheduler routes with — same `choose`, same
`MinSamples` gate — so "what Explain said" and "where the scheduler started
each task" cannot drift into disagreeing.

The existing columns are untouched. `Usage` and `Ceiling` mean exactly what
they meant, and the ceiling remains a bound on a routed run for the reason in
§3.

## 10. What routing is actually worth

Honestly: it depends on the ladder, and it is worth saying which part is which.

**In calls, always the same.** Every skipped call is a call not made. That is
latency the record does not wait for and rate-limit budget the stage does not
consume, whatever the models cost. On a stage bounded by RPM rather than by
dollars this is the whole benefit and it is a large one.

**In dollars, it depends on the price ratio.** Skipping a doomed call saves
that rung's price. On a Haiku→Sonnet ladder (about 4× apart) that is roughly a
fifth of what an escalating record costs. On a ladder 50× apart it is almost
nothing — the expensive rung dominates, and the wasted cheap call was never
where the money was.

The example, offline, on a 4× ladder:

```
$ go run ./examples/router

300 contracts, two thirds of them beyond the fast model.

                                calls      cost($)
flat ladder                       500       0.0675
routed                            326       0.0595
difference                        174       0.0080

300/300 records carry identical output: routing changes where a
record starts, never what it says.
```

35% of the calls, 12% of the dollars, and the same answers. (The probe count
varies between runs — probes are drawn deterministically from task IDs, which
are fresh each run — so the exact split moves a little.)

## 11. Bringing your own

`WithRouter` installs a `Router` directly, for a caller who wants a policy
rather than a learner — a hand-written rule over a field, a model trained
elsewhere, or `route.Off` to keep the flat ladder while leaving the option in
place:

```go
type byLanguage struct{}

func (byLanguage) Route(r route.Request) route.Decision {
    if r.Records[0].String("lang") != "en" {
        return route.Decision{Rung: len(r.Rungs) - 1, Reason: "non-English: the fast model cannot"}
    }
    return route.Decision{Reason: "English: the fast model can"}
}
func (byLanguage) Observe(route.Outcome) {}
```

A router supplied this way is used as given: nothing seeds it from the state
directory, nothing persists what it learns, and `Explain` prints no ladder line
— Loom cannot price a policy it did not write.

## 12. Open

- **Per-rung price learning.** Rungs are priced from the planner's token
  estimate, which is fine for ordering a ladder and useless as an absolute. A
  router that priced rungs from the calls it has watched would decide on
  measured economics, the way the report already reports on them.
- **Cross-stage generalization.** Buckets are per stage, so two stages doing
  similar work over the same records learn separately.
- **Drift.** Nothing ages the counts, so a profile carries a pipeline's whole
  history at equal weight. A stage whose prompt was rewritten keeps the
  verdicts the old prompt earned. The stage fingerprint is the obvious thing to
  key on; it is not keyed on yet.
- **Latency as an objective.** The decision minimises dollars. On a ladder
  whose rungs differ more in speed than in price the interesting objective is a
  different one.
