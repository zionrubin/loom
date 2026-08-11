# Loom as an async inference engine for agents

[INFERENCE.md](./INFERENCE.md) maps the serving engines' mechanisms onto Loom
at the level of the **call**: prefix caching, continuous batching, admission
against a token budget. That mapping treats a run as one big batch of requests,
which is what a run is.

This document is about the level above it. When several pipelines run at once —
a sweep, three specialists, and the synthesis that reads their conclusions —
the unit that matters stops being the call and becomes the **program**: the
whole multi-call trajectory a caller is waiting on. Everything changes at that
altitude. The scarce resources become shared rather than owned, the latency
that matters becomes completion time rather than per-call latency, and the
agents need to reach each other's results.

That is the problem the agentic-serving literature has been working on since
2024, and this document maps it onto Loom the same way INFERENCE.md maps the
request-level literature: what each mechanism actually solves, what Loom's
equivalent is, and — for the ones that aren't here — why not.

## What the literature found

Five systems, and they converge on the same diagnosis from different angles.

**[Parrot](https://www.usenix.org/conference/osdi24/presentation/lin-chaofan)
(OSDI '24)** started it. Its observation is that a serving engine sees a stream
of independent requests and therefore cannot see the *application*: which
requests belong to one workflow, which depend on which, and what the end-to-end
latency target is. Its Semantic Variable annotates prompt regions with their
purpose, from which the system recovers the app-level DAG — and with the DAG it
can parallelize, batch, communicate asynchronously between dependent requests,
deduce per-request objectives from the end-to-end one, and schedule requests of
one application together instead of interleaving them with everyone else's.
Reported end-to-end gains reach 11.7×. The lesson is that *the structure was
always there and the engine simply could not see it.*

**[Autellix](https://arxiv.org/abs/2502.13965)** (published as *Agentix* at
NSDI '26) draws the scheduling conclusion. An agent is a program that issues
many correlated calls with tool work between them, so what a caller experiences
is the program's job completion time, not any call's latency. Under
first-come-first-served, programs suffer head-of-line blocking at two levels —
behind other calls, and behind other *programs* — and the second is invisible
to a request-level scheduler. Its answer is to prioritize a call by the
cumulative service its program has already received: **PLAS** (Program-Level
Attained Service) for single-threaded programs, **ATLAS** (Adaptive
Thread-Level Attained Service) for multi-threaded ones. Long programs are
deprioritized so short ones finish, which is least-attained-service standing in
for shortest-remaining-first. Reported throughput gains are 4–15×.

**[Astraea](https://arxiv.org/abs/2512.14142)** adds the observation that agent
programs alternate model calls with *external* I/O, and that the scheduling
granularity of existing engines does not match that shape — so nothing is
optimizing the end-to-end lifecycle. It classifies requests as I/O- or
compute-intensive, schedules hierarchically on history plus prediction with an
enhanced highest-response-ratio-next policy, and manages agent state adaptively
across I/O waits under memory pressure. Reported JCT reduction is up to 25.5%.

**[Continuum](https://arxiv.org/abs/2511.02230)** takes the same gap and
attacks the cache: because a tool call pauses a program, a normal engine evicts
its KV cache and pays to rebuild it on the next turn. So it pins the cache
across the pause with a time-to-live derived from the expected tool latency, and
lets it expire rather than holding memory indefinitely.

**[Kairos](https://arxiv.org/abs/2508.06948)** looks at multi-agent workflows
specifically, and its finding is the one that most directly concerns a fleet:
the agents *within* one workflow differ in latency sensitivity and resource
profile, and a scheduler that ignores those differences serves all of them
worse. Its answer is a workflow orchestrator feeding a workflow-aware priority
scheduler and a memory-aware dispatcher.

Alongside the serving work, the agent-harness literature converged on a
complementary structure. Anthropic's long-horizon agent architecture separates
the *brain* (model plus harness loop) from the *hands* (ephemeral sandboxed
execution environments), with the session as an **append-only event log** — so
the harness is stateless, sandboxes are disposable, and a crashed brain does not
lose the run. And the NeurIPS 2025 MAST taxonomy of multi-agent failures finds
roughly a third each from specification ambiguity, coordination breakdowns, and
verification gaps — which is to say most multi-agent failures are not model
failures.

## The mapping

| Mechanism | What it actually solves | Loom equivalent | Status |
|---|---|---|---|
| **Program as the scheduling unit** (Autellix) | A scheduler that sees only calls cannot reason about what a caller waits for | A `Fleet` agent is a program; the slot pool admits by program, and the fleet report is keyed on per-agent completion time | ✅ `fleet.go`, `runtime/pool.go` |
| **Least-attained-service priority** (PLAS) | Program-level head-of-line blocking: a short program stuck behind a long one | `runtime.Pool` admits a contended slot to the agent with the least attained service, measured in slot-time including work in flight | ✅ `runtime/pool.go` |
| **Anti-starvation** (Autellix) | LAS alone lets newcomers shut out incumbents forever | Queued tasks earn priority credit at `WithAdmissionAging` × wait, bounding a program's wait by its own attained service | ✅ `runtime/pool.go` |
| **Application-level view** (Parrot) | The engine cannot optimize structure it cannot see | Loom never had to infer it: a pipeline *is* the declared DAG, and stage specs are data the planner reads directly | ✅ by construction |
| **Performance-objective deduction** (Parrot) | Per-request targets derived from the end-to-end goal | Not implemented — Loom's per-agent knob is a budget, not a latency target | ❌ |
| **Scheduling one application's requests together** (Parrot) | Interleaving unrelated work destroys locality | Partly: a stage's tasks share a prefix by construction, but the pool deliberately *interleaves* agents, trading locality for completion time | ◐ |
| **Workflow-aware priority across agents** (Kairos) | Agents in one workflow have different latency sensitivity | Partly: the pool distinguishes agents by attained service, not by declared sensitivity — an agent cannot yet say "I am the interactive one" | ◐ |
| **One quota across concurrent programs** | Two runs in a process each believe they own the provider's whole limit | One `runtime.RateLimiter` per fleet, borrowed by every agent | ✅ `fleet.go` |
| **One ceiling across concurrent programs** | A budget enforced per pipeline is a budget multiplied by pipelines | One `runtime.Governor` per fleet (`WithFleetBudget`) | ✅ `fleet.go` |
| **Cross-program cache reuse** | Programs redo work a sibling already paid for | One CAS and result cache per fleet: an agent replays another agent's completed work at zero cost | ✅ `fleet.go` |
| **Cross-program *research* reuse** | The result cache is blind to the duplication that costs most: one question in three wordings, asked at the same instant | `findings.Gate` keys on the question rather than the bytes, and a single-flight lease collapses concurrent askers onto one call | ✅ `findings`, [FINDINGS.md](./FINDINGS.md) |
| **Append-only session log** (Anthropic) | State that survives a crashed harness | Loom's equivalent already existed for a different reason: the content-addressed result cache *is* the checkpoint, and lineage is the append-only record | ✅ `store` |
| **Inter-agent state** (blackboard architectures) | Coordination breakdown — the largest single MAST failure class | Append-only, versioned, content-addressed topics: `Fleet.Post` / `Await`, read through the broadcast mechanism so grants, audit and cache fingerprints all apply unchanged | ✅ `fleet.go` |
| **KV-cache TTL across tool pauses** (Continuum) | A paused program pays to rebuild its prefix | No analog — Loom does not own a KV cache. The remote-provider equivalent is the provider's own prefix-cache TTL, which Loom cannot pin | n/a |
| **Adaptive state management under memory pressure** (Astraea) | GPU memory contention between paused programs | n/a — Loom holds records, not KV blocks | n/a |
| **I/O- vs compute-intensive classification** (Astraea) | Mismatched scheduling granularity around external calls | Not implemented — Loom's tasks are one shape, and the tool-call gap inside a task is invisible to the scheduler | ❌ |
| **Prefix-affinity routing across replicas** (Autellix) | A program's later calls landing on a replica without its cache | Not implemented, and it becomes relevant only with the remote executor fleet | ❌ |

Three rows in that table were the work: making the program the scheduling
unit, admitting slots by attained service, and giving agents a safe way to read
each other's conclusions.

---

## One engine, many agents

`loom.Run` provisions everything a pipeline needs and releases it afterwards: a
rate limiter, a budget governor, a result cache, a set of execution slots. For
one pipeline that scope is exactly right.

For several it is exactly wrong, and not by a little. None of those things is a
property of a pipeline. A rate limit is a property of an *account*; a dollar
ceiling is a property of a *wallet*; a cache is a property of *work already
done*. Two `Run` calls in one process therefore give you two of each, and every
one of the duplicates is a bug waiting for load:

```go
loom.Run(ctx, sweep,   opts...)   // believes it owns the whole quota
loom.Run(ctx, summary, opts...)   // also believes it owns the whole quota
```

Together they exceed the limit neither of them individually violates. Each
enforces a ceiling, so neither enforces yours. Neither can replay the other's
completed work. And nothing schedules them against each other, so the summary
waits on the sweep for no reason at all.

A `Fleet` holds those things once and lends them out:

```go
fleet, _ := loom.NewFleet(
    loom.WithRegistry(reg),
    loom.WithWorkers(8),                                        // slots for the whole fleet
    loom.WithFleetBudget(core.Budget{MaxCostUSD: 20}),          // one ceiling, every agent
    loom.WithStateDir("./state"),                               // one cache, every agent
    loom.WithEventHandler(v.Handle),                            // one universe in the view
    loom.WithTopic("findings"),                                 // a board they can read
)
defer fleet.Close()

desk := fleet.Go(ctx, wireDesk())        // returns immediately
for _, b := range beats {
    fleet.Go(ctx, beatPipeline(b))       // all running at once
}
fleet.Wait()
fmt.Print(fleet.Report())
```

`Run` is a fleet of one, built through the same path — the same reason the
barrier and streaming drivers cannot drift apart is the reason a run and an
agent cannot.

Agents on a fleet run pipelined, always. The barrier driver's per-stage worker
pool is precisely the thing a fleet replaces, so there is nothing for it to do
here; `run.started` reports the driver as `fleet` so an observer can tell.

### What each agent still owns

Sharing is not merging. Each agent keeps its own plan, envelopes, runners,
stage outputs, report, lineage, and audit trail. Two agents whose pipelines both
name a stage `note` get two separate rows, not one — the fleet demultiplexes
one event stream into per-agent collectors, and attributes the fleet-wide audit
log back to each agent by its own task IDs.

---

## Admitting slots by attained service

`runtime.Pool` is the fleet's slot pool and the policy that goes with it.

With one program there is no policy to apply: its tasks are interchangeable, so
first-come-first-served is both fair and optimal, and the pool behaves exactly
as the FIFO channel of slots it replaced. With several, tasks stop being
interchangeable, and arrival order stops being defensible — a three-call summary
queued behind 10,000 records of sweep has a completion time set by the sweep's
size rather than its own.

So a contended slot goes to the waiting task whose **program has been served
least**, where service is slot-time. Two details carry the whole design:

**Work in flight counts.** Service is not just slot-time already returned; it
includes the elapsed time of everything the program currently holds. Without
that, a program occupying every slot still looks idle to the next admission
decision — which is exactly the case the policy exists to handle, so getting
this wrong makes the policy inert rather than merely imprecise.

**A newcomer starts at zero, deliberately.** That is not an equal share of what
remains; it is priority until it catches up, and it is what lets a short agent
*overtake* a long one rather than merely draw level. When service times vary as
widely as they do here, least-served-first is the best available stand-in for
shortest-remaining-first, which is what minimises mean completion time.

The cost of that is the incumbent, and it needs an explicit answer rather than
a hope: served heavily, it would yield to every newcomer forever. So a queued
task earns priority credit at `WithAdmissionAging` × its wait, which bounds the
wait — **a program is held back by at most its own attained service divided by
the aging rate**, whatever arrives while it waits. A fleet whose agents keep
joining indefinitely wants that rate higher than one whose agents are launched
together and drain.

The measurable claim, from `examples/newsroom` (60-task sweep launched first and
filling every slot; three 7-task agents launched 150ms later):

```
agent                stages  tasks   tokens   cost($)   service      wait       jct
wire-desk                 2     60     5166    0.0070   12.833s    3.913s    4.093s
beat-markets              3      7      426    0.0025    3.434s     1.33s    2.396s
beat-policy               3      7      424    0.0025      3.4s    1.425s    2.379s
beat-tech                 3      7      417    0.0024    3.307s    1.474s    2.346s
front-page                2      1      208    0.0083    1.363s      19ms    1.382s
wire-recheck              2     60        0    0.0000       8ms        0s       2ms
slots 6 occupied 99% of 4.096s · 142 tasks admitted
```

The beats arrive last and finish first, at 99% slot occupancy. `service` is what
each agent was given and `jct` is what a caller waited — printing both is what
makes the trade legible rather than asserted.

---

## The blackboard

Agents that work together have to reach each other's conclusions, and Loom's
existing sharing primitive deliberately cannot express that. A **broadcast** is
read-only for the run's whole life, because shared mutable state would make a
cached result depend on execution order — which is precisely what
content-addressed caching assumes away.

A fleet's **blackboard** is the versioned sibling. Topics are append-only; an
agent reads the snapshot that existed when it launched:

```go
fleet.Post("findings", finding)                   // or PostFrom(agent, …)
posts, _ := fleet.Await(ctx, "findings", 3)       // fan-in: block on a count
fleet.Run(ctx, frontPage())                       // reads findings@3
```

and the reader declares it exactly like a broadcast, because it *is* one:

```go
Infer("write", pipeline.InferSpec{
    Prompt: `{{range broadcast "findings"}}- [{{.value.beat}}] {{.value.finding}}
{{end}}`,
}, pipeline.WithBroadcast("findings"))
```

Routing it through the broadcast mechanism is what makes a mutable shared log
safe in front of a content-addressed cache. A snapshot is serialized into the
CAS and agents carry its 64-byte hash, so:

- **A post cannot disturb a running agent.** Its envelopes already name a hash,
  and the bytes behind a hash never change. A later post writes a new snapshot
  under a new hash and leaves the old one resolvable. The indirection that
  existed to keep envelopes small turns out to be the thing that makes this
  race-free, for free.
- **The cache stays honest.** The snapshot's hash joins the fingerprint of every
  stage that reads the topic, so an agent reading `findings@3` and one reading
  `findings@7` have different cache keys — while a rerun against `findings@3`
  replays for nothing, and a stage that never declared the topic keeps its warm
  cache when someone posts.
- **Reads stay least-privilege.** A topic on the board is not a topic an agent
  can see. It reads only what it declared, checked against its grants and
  audited like any other capability.
- **It survives leaving the process.** Envelopes carry hashes, not bytes, so a
  board of any size stays shippable to a remote or sandboxed executor.

One detail is load-bearing and looks like an omission: **a `Post` carries no
timestamp.** A wall clock in the payload would make identical knowledge hash
differently on every fleet, and a rerun that should have been free would pay for
itself again. `Seq` orders posts; the event stream and audit log carry the
timing.

This is also what closes a gap the README has always named: *loom DAGs fan out
but do not fan back in, so a fan-out and the synthesis that fuses its results
are two runs.* They still are. But the second run can now read what the first
ones concluded, with a content hash pinning what it read.

### What the blackboard does not do

It shares what an agent concluded *after it finished*. It does nothing for the
case that costs more and happens more: two agents running right now, both about
to research the same thing.

That case needs a different instrument, because the sharing has to happen inside
a task rather than at an agent boundary, and it has to be keyed on the *question*
rather than on a topic name someone chose in advance. `findings.Gate` is that
instrument — the commons — and [FINDINGS.md](./FINDINGS.md) is its design: three
lookup tiers ordered so the free ones answer most of it, a single-flight lease so
simultaneous askers become one call, and four properties that make writing from
inside a task safe in front of a content-addressed cache. The two are
complements: a topic is where an agent publishes a conclusion it reached, and a
finding is where it deposits a fact it had to go and buy.

### Why agents post between tasks and not inside them

A task cannot post. That is a restriction, and it is the same restriction
[ITERATION.md](./ITERATION.md) argues for at the level of a superstep.

A task that published mid-run would make its own cached result depend on
execution order, and a replayed task would not re-publish at all — so the
board's contents would depend on how warm the cache happened to be. Worse, an
agent could then read a board that changes underneath its own stages, and
nothing about the run would be reproducible. Coordination happens at agent
boundaries because that is where there is a defined state to coordinate on: a
snapshot, a price, and a diff.

The looser thing — agents calling each other freely, mid-task — is the popular
shape and remains the wrong one here, for the reasons §4 of ITERATION.md sets
out: unbudgetable, unreproducible, uninspectable.

---

## What this is deliberately not

**Not an agent framework.** A Loom agent is a pipeline, not a tool-calling
loop. Nothing here gives a model the autonomy to choose its next action; a
stage is still a declared operation over records. Loom is the engine *under*
agents that work together — it schedules them, prices them, caches them, and
carries their conclusions between them. The harness that decides what an agent
does next is somebody else's, and that is the right seam.

**No latency objectives.** Parrot deduces per-request targets from an
end-to-end one. Loom's per-agent knob is a budget, and attained-service
admission optimizes mean completion time rather than any stated deadline. An
agent cannot yet declare itself the interactive one — which is exactly the
distinction Kairos found mattered, and the most obvious next thing to add.

**No preemption.** A task that has been admitted runs to completion. Serving
engines preempt mid-sequence to bound tail latency; Loom's unit of admission is
a whole model call, and cancelling one wastes what it has already spent.
Fairness is therefore enforced between calls, which bounds waiting rather than
tail latency.

**Nothing about the tool-call gap.** Astraea's and Continuum's central case —
the program paused on external I/O, and what to do with its state meanwhile — has
no analog here, because a Loom task does not pause: it is one model call, and
tool work sits inside a Go stage that holds its slot throughout. A stage that
does slow tool work therefore occupies a slot while doing nothing a model could
use. That is a real inefficiency and the honest description of it is that Loom's
task granularity is too coarse to see it.

**No cross-executor prefix affinity.** Autellix routes a program's later calls
to the replica holding its cache. Loom does not own the cache and does not yet
own a fleet of executors; when the remote executor lands, this is the row of the
table that becomes urgent.

**No cache eviction, still.** The fleet's result cache is unbounded, which was
merely wrong for a long-lived worker fleet and is now wrong for a long-lived
*fleet*, which is a thing that exists. An LRU over content-addressed blocks
remains the obvious answer, and it now applies to the findings ledger too.

**No single-flight on the result cache.** The findings gate has a lease, so
concurrent askers of one question become one call. The result cache does not, so
concurrent *identical tasks* both run and both write. This was invisible while
stage latency spread tasks out; `examples/commons` makes it visible by removing
the latency that was hiding it, and reports the regression rather than only the
column that improved.

---

Read next: [INFERENCE.md](./INFERENCE.md) for the call-level mapping this builds
on, [ITERATION.md](./ITERATION.md) for the loop that a fleet does not yet
provide and why the barrier is a feature, and
[ARCHITECTURE.md](./ARCHITECTURE.md#6-scaling-path-from-local-runtime-to-distributed-system)
for the remote-executor work several rows above are waiting on.
