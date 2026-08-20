# Loom Architecture

Loom is a framework for running AI-powered data processing at scale — the
MapReduce/Spark idea rebuilt from first principles for workloads whose
operators are model calls rather than CPU functions.

This document explains the reasoning behind the design, walks through each
component, and lays out the path from the local runtime to a distributed
deployment.

---

## 1. Why AI workloads need a different framework

Classic data frameworks (MapReduce, Spark, Flink) optimize for a world where
the operator is cheap, deterministic, and CPU-bound, and where the scarce
resources are cores, memory, and shuffle bandwidth. Every one of those
assumptions breaks for AI processing:

| Classic assumption | AI reality | Consequence for design |
|---|---|---|
| Operators are cheap and fast | A single operator call costs real money and hundreds of ms | Caching identical work is a first-order economic feature, not an optimization |
| Operators are deterministic | Model output varies and can be *wrong* while the call "succeeds" | Failure handling needs a **semantic** class, with validation and escalation, not just retry |
| Throughput bounded by cores | Throughput bounded by provider **rate limits** (RPM/TPM) | The scheduler needs admission control in provider units, not a thread pool |
| Cost ≈ cluster time | Cost = tokens × price, per call | Budgets are a runtime enforcement concern (a governor), not a billing report |
| Operators are trusted code | Operators execute prompts, tools, and model-derived actions | Executors need least-privilege sandboxes: explicit grants for models, secrets, tools, egress |
| One binary, one capability | Many models with different cost/quality/limits | Model choice is a *scheduling* decision: tiers, routing, escalation ladders |
| Reduce is associative math | Aggregation is often itself an AI operation (summarize, synthesize) | Hierarchical AI-reduce is a native operator with tree fan-in |

Loom's thesis: **make every one of these differences a first-class concept**
rather than something bolted onto a generic DAG runner.

## 2. Design principles

1. **Explicit provisioning, least privilege.** A task declares everything it
   may use — model, secrets, tools, network hosts, context, budget, sandbox
   profile — in a serializable *envelope*. Executors can do exactly what the
   envelope grants, and every sensitive access is checked and audited at the
   moment of use.
2. **Determinism where it buys something.** Operation specs are fingerprinted
   and results content-addressed, so identical work is never paid for twice
   and any output can be traced to the op, model, and inputs that produced
   it. Caching *is* checkpointing *is* resume.
3. **Failure classes drive recovery.** Transient failures back off and retry;
   semantic failures escalate to a stronger model; permanent failures fail
   fast; budget failures stop the run with partial results. No single retry
   loop can serve all four.
4. **The data plane is declarative; the control plane is pluggable.** AI
   operations are pure data (prompt templates, bindings, schemas), so they
   can execute anywhere. The `Executor` interface is the seam where
   distribution and isolation plug in.
5. **Economics are runtime state.** Token usage and dollar cost flow through
   every result, are aggregated live, and are enforced by a governor — a run
   is bounded by dollars the same way a query is bounded by a timeout.

## 3. System overview

```
        Authoring                Planning                    Execution
  ┌──────────────────┐   ┌─────────────────────┐   ┌──────────────────────────┐
  │ pipeline.Pipeline│   │ plan.Compile        │   │ runtime.Scheduler        │
  │  FromRecords     │   │  validate           │   │  admission (rate limits) │
  │  .Map/.Filter    │──▶│  fuse pure chains   │──▶│  budget governor         │
  │  .Infer          │   │  fingerprint ops    │   │  class-aware retries     │
  │  .ReduceAI       │   │  resolve bindings   │   │  escalation ladder       │
  │  .Combine        │   │  build envelopes    │   └────────────┬─────────────┘
  └──────────────────┘   └─────────────────────┘                │ task + envelope
                                                                ▼
  ┌─────────────────────┐   ┌───────────────────┐   ┌──────────────────────────┐
  │ observe.Bus/Report  │◀──│ store.CAS/Cache/  │◀──│ executor.Local           │
  │  events, metrics,   │   │ Lineage           │   │  cache short-circuit     │
  │  latency, cost      │   │  content-addressed│   │  op runners (ops.*)      │
  └─────────────────────┘   │  artifacts        │   │  ModelClient (grants,    │
                            └───────────────────┘   │   egress, secrets, cost) │
  ┌─────────────────────┐                           │  ToolSet (grant-checked) │
  │ security.Broker/    │◀──────────────────────────│  sandbox profile         │
  │ Audit               │      per-call resolution  └──────────────────────────┘
  └─────────────────────┘
```

The last box is the one that moves. `executor.Local` is the default, and
`worker.Client` is the same interface over a leased queue — so the right-hand
column can be another process, or several, without the two to its left
changing:

```
  scheduler ──task+envelope──▶ worker.Client ──▶ queue (leases, fencing) ──▶ worker
       ▲                             │                                        │
       │                             └──── CAS: inputs, broadcasts, outputs ──┘
       └────────── task.Result ◀── receipt (usage, cost, output hash)
```

A run flows: **author** a pipeline → **compile** it into a plan (validation,
fusion, fingerprints, envelopes) → the **driver** walks stages in
topological order, building tasks and handing each stage's batch to the
**scheduler** → the scheduler admits tasks under rate limits and budget and
drives them through an **executor** with class-aware recovery → results are
cached, lineage-tracked, observed, and flow to downstream stages.

## 4. Component walkthrough

### 4.1 Pipeline (authoring)

`pipeline` is a declarative builder producing a DAG of stages. Two families
of operators:

- **Pure transforms** — `Map`, `Filter`, `FlatMap`, `MapTools`, `Combine`:
  ordinary Go functions. They execute wherever the closure lives; giving
  them a `Version` makes them cacheable.
- **AI operators** — `Infer` (per-record model call with prompt template,
  optional JSON parsing and validation) and `ReduceAI` (hierarchical
  tree-aggregation with configurable fan-in). These are *fully declarative
  and serializable*: they can execute on any worker.

Datasets are handles to stage outputs; deriving twice from one dataset
branches the DAG.

### 4.2 Planner

`plan.Compile` performs:

- **Validation** — unique stage names, templates parse, bindings resolve —
  authoring errors surface before any money is spent.
- **Operator fusion** — maximal runs of adjacent pure stages with
  single-consumer links collapse into one task boundary (the classic
  narrow-dependency optimization; fewer serialization points).
- **Fingerprinting** — each stage's op spec is canonically hashed. The
  fingerprint + input content hash is the task's cache key, and the
  fingerprint is recorded in lineage.
- **Envelope assembly** — for each stage, the *minimal* grant set: the
  binding's candidate models, exactly their secrets, exactly their
  endpoints on the egress allowlist, plus explicitly requested tool grants.
  Least privilege is the *default output of planning*, not a configuration
  chore.

### 4.3 Task envelope (the security & portability boundary)

Every task carries an `Envelope`:

```
Envelope{
  Binding    — model or tier + escalation ladder
  Grants     — capabilities: model:*, secret:*, tool:*, data:read:*
  Egress     — deny-by-default host allowlist
  Context    — the exact context bundle (system + fragments) the task needs
  Broadcasts — run-level shared values, by name → content hash
  Budget     — per-task timeout / attempts / token caps
  Sandbox    — inline | subprocess | container | wasm
}
```

Note the asymmetry between `Context` and `Broadcasts`, which is deliberate.
Context fragments are *copied* into every task: they are small, stage-specific,
and part of the prompt. Broadcasts are *referenced* by content hash: they are
potentially large, shared by the whole run, and fetched from content-addressed
storage at the point of use. Copying a 10 MB taxonomy into 100,000 envelopes
would cost a terabyte of task payload; referencing it costs 64 bytes each.

Envelopes (and tasks) are plain JSON-serializable data — proven by test —
which is what makes remote and sandboxed execution possible without changing
planning or scheduling.

### 4.4 Scheduler

`runtime.Scheduler` executes a batch of tasks with:

- **Bounded concurrency** (global or per-stage worker counts).
- **Admission control** — per-model token buckets for requests/min *and*
  tokens/min; work waits at the scheduler instead of burning provider 429s.
  A third dimension, `MaxConcurrent`, bounds calls in flight rather than
  calls per minute — the ceiling a *local* backend imposes, where the scarce
  resource is a device that decodes some fixed number of sequences at once and
  oversubscription queues invisibly inside the server instead of failing. An
  admission returns the release for what it holds, and the scheduler holds it
  across the call, so a backoff between attempts gives the slot back.
- **Budget governor** — run-level cost/token caps enforced across all
  concurrent tasks; on exhaustion the run stops admitting work and returns
  partial results plus the spend so far.
- **Class-aware recovery** —
  - *transient* → exponential backoff + jitter, same model;
  - *semantic* → climb the binding's escalation ladder (e.g. Haiku → Sonnet)
    and retry — invalid output is evidence the model was too weak, so paying
    the same price for the same failure is wasteful;
  - *permanent* → dead-letter immediately;
  - *budget* → abort the run.
- **Dead letters** — with `ContinueOnError`, failing records are quarantined
  into `Failures` instead of poisoning the run.

### 4.5 Executor

`executor.Executor` is one method: `Execute(ctx, task) (Result, error)`. The
local implementation:

1. rejects sandbox profiles it can't honor (fail closed);
2. short-circuits through the content-addressed cache;
3. dispatches to the stage's op runner with a capability-scoped `Runtime`:
   - `ModelClient` — checks the model grant, checks the endpoint against the
     egress allowlist, scopes secret resolution to the task's grants,
     computes cost from registry pricing, publishes telemetry;
   - `Tools` — grant-checked, audited tool invocation;
4. stores results in the CAS, records lineage.

Because providers resolve credentials **per call through the broker**,
executors and ops never hold raw secrets — the same property as vault-style
egress injection, implemented at the framework boundary.

**The seam, used** (`worker`). `worker.Client` is a second implementation of
that one method: it puts the task on a durable queue and waits. Local stays the
default and remote is an adapter, selected at provisioning
(`loom.WithWorkerService`) and nowhere else, so nothing above the seam learns
that execution moved off-process. The other side, `loom.Serve`, takes the same
options and wraps an ordinary `executor.Local` — so *how* a task runs remains
one implementation rather than two that must be kept in agreement.

Three properties carry it:

- **The queue owns delivery.** Leases with heartbeats, expiry, and **fencing
  tokens** that increase on every claim. A worker that dies loses its claim
  rather than the task; a worker that stalled past its expiry is told apart from
  the live owner by comparing integers rather than by trusting clocks. Delivery
  is at-least-once, and redelivery is bounded so a task that kills every worker
  it touches is eventually declared the problem.
- **The CAS owns payloads.** Inputs above a size threshold, broadcast values,
  and outputs travel by content hash through storage both sides reach. This is
  what makes at-least-once delivery produce exactly-once *work*: two workers
  executing one task write identical bytes to one address, and exactly one of
  their commits becomes the receipt. A commit against a finished task returns
  the winning receipt rather than creating a second one; a commit under a fenced
  lease against an unfinished task is refused.
- **Workers advertise capabilities.** Stages (a runner is code and cannot be
  serialized), providers, tools, sandbox profiles, MCP servers, and concurrency.
  In one process "this executor can run this task" is true by construction;
  across a fleet it is a question, and a claim refused before it is made costs
  nothing where a task failed after a model call does not.

Recovery policy stays with the scheduler. The queue redelivers *silence* — a
lease nobody renewed — while a task that failed is reported to the client with
its `FailureClass` intact, so backoff, the escalation ladder and dead-lettering
continue to happen where the run's budget and the binding's ladder are known.

Two queue backends implement one contract and pass one conformance suite
(`worker/queuetest`): `worker.MemQueue` for a process with several workers in
it, and `worker/filequeue` — an append-only log plus an `O_EXCL` lock
directory — for several processes over a shared directory.

### 4.6 Model layer

`model.Registry` holds every model a deployment may use: provider, pricing,
rate limits, tier, required secret. Stages bind by explicit ID or by
**tier** (`fast` / `balanced` / `deep`), keeping pipelines portable across
model generations. A binding's **escalation ladder** is the ordered list of
increasingly capable models used by semantic recovery.

Four providers ship today: a deterministic `Mock` (tests, offline
development, scripted failures); `providers/anthropic` and `providers/openai`
over the official SDKs (per-call broker-resolved keys, 429/5xx → transient,
4xx → permanent, refusals → semantic); and `providers/llamacpp`, which runs
the model on your own hardware behind the same seam.

Local inference is where the layer's separations earn themselves, because a
pipeline is unaffected by it — a binding names a model, not a machine — while
the envelope around the call simplifies in four ways. Pricing is zero, so the
dollar governor stops being the bound that matters and `MaxConcurrent`
(discovered from the server's own slot count) becomes it. `SecretRef` is
empty, so the planner emits no secret grant at all. `Endpoint` is loopback
rather than empty, so the egress allowlist *states* that the stage's records
cannot reach a vendor and the executor enforces it. And the prompt-prefix
cache the shared-prefix design was written against turns out to be the KV
cache itself, whose writes are free — so the planner's break-even rule, which
exists to earn back a remote write's premium, has nothing left to weigh. See
[INFERENCE.md](./INFERENCE.md#when-the-inference-engine-is-yours).

### 4.7 State: CAS, cache, lineage

- **CAS** — artifacts stored by content hash, memory-first with optional
  disk persistence.
- **Cache** — deterministic key `hash(op fingerprint, input content)` →
  artifact. This is simultaneously the **checkpoint/resume** mechanism: a
  rerun (same code, same inputs) replays completed AI work with zero model
  calls and zero cost, across process restarts when a state dir is
  configured. Partial failures resume from where they left off for free.
- **Broadcasts** — run-level read-only values shared by every task that
  declares them. Registered once before execution, serialized into the CAS,
  and carried through envelopes as content hashes. This is how tasks and
  executors share memory in Loom: not by pointing at the same mutable
  object, which no distributed executor could honor, but by agreeing on a
  hash whose bytes any worker can fetch from shared storage. Two properties
  make them safe to combine with caching: the value's hash participates in
  the reading stage's fingerprint (edit the value, and exactly the stages
  that read it recompute), and the value is immutable for the run's
  lifetime, so replaying a cached result cannot observe a different world
  than the original execution did.
- **Prompt prefixes** — the second layer of sharing, and the one that
  reaches the model rather than the worker. A broadcast makes every task
  reference the same bytes; a stage's `Prefix` makes every task send those
  bytes in the same leading position, so the provider's prompt cache serves
  the prefix instead of reprocessing it per record. The planner enables it
  only when a stage issues more than one call — the point at which a cache
  write can be earned back — and the prefix joins the stage fingerprint on
  the same terms a broadcast hash does. See
  [INFERENCE.md](./INFERENCE.md#shared-memory-prefix-caching).
- **Findings** — the third layer of sharing, and the one that reaches *outside*
  the process. A broadcast shares bytes between tasks and a prefix shares them
  with the provider; a **finding** shares an answer about the world between
  agents that were about to go and get it themselves. The result cache cannot
  do this job, for three structural reasons: its key is the bytes going in, so
  two wordings of one question are two keys; it serves the second asker only
  after the first has finished, so agents launched together all miss and all
  call out; and it is all-or-nothing, so a partial overlap is worth zero.
  `findings.Gate` keys on the *question* instead — an exact key, then a
  topic-and-facets class, then optional embedding similarity — collapses
  concurrent askers onto one call with a single-flight lease, and narrows the
  external request to the fields a partial hit left uncovered. Writing to it
  from inside a task is safe in front of a content-addressed cache because a
  hit is substitutable for the call it replaces, entries are append-only and
  content-addressed, the knowledge hash excludes the clock (so independent
  rediscovery corroborates rather than forks), and every finding carries the
  capabilities its research consumed — the ledger may save a reader a call it
  was allowed to make, never make one it was not. See
  [FINDINGS.md](./FINDINGS.md).
- **Lineage** — every artifact records the run, stage, op fingerprint,
  model, input hashes, and broadcast hashes that produced it: reproducibility
  and audit for outputs whose provenance would otherwise be "a model said
  so".

### 4.8 Observability

Every lifecycle transition — run/stage/task start and finish, every model
call with usage/latency, every retry with its reason, every cache hit, every
budget trip — is a typed event on `observe.Bus` (synchronous handlers for
the built-in collector; non-blocking channels for external consumers). The
collector folds events into a `RunReport`: per-stage task counts, failures,
retries, cache hits, token usage, dollar cost, and latency percentiles.

Every event carries the run ID it belongs to, and run-level events carry the
pipeline's name. That pair is what lets one handler serve several runs: the
constellation view (`viz`) folds the stream into a *universe* — one run state
per run ID, retained after the run ends — instead of a single current run that
`run.started` resets. A process whose work is several pipelines (loom DAGs fan
out but do not fan back in, so a fan-out and its synthesis are two runs) is
therefore watchable as a whole, and pipelines running concurrently on one
handler stay separate rather than interleaving. The universe is bounded by
run count rather than by age: runs are held whole, so the oldest is dropped
when a new one pushes past the limit, and the run still receiving events is
never the one dropped.

### 4.9 Security

Four cooperating mechanisms, all exercised by tests:

1. **Capability grants** (`model:*`, `secret:*`, `tool:*`, `data:read:*`) —
   assembled minimally by the planner, checked at use. Registering a
   broadcast for a run does not expose it: a stage reads only what it
   declared, so shared state does not become ambient state.
2. **Secret broker** — per-call, grant-scoped, audited resolution; no
   ambient credentials.
3. **Egress policy** — deny-by-default allowlist per task; a provider
   endpoint not implied by the stage's binding is unreachable.
4. **Audit log** — append-only record of every allow/deny decision with the
   task that triggered it.

### 4.10 Aggregation

- `Combine` — associative Go fold for classic reductions.
- `ReduceAI` — hierarchical tree reduce: records are grouped `FanIn` at a
  time, each group is aggregated by a model call, and levels repeat until
  one record remains. O(log_FanIn n) sequential depth, each level fully
  parallel and cache-eligible.

### 4.11 Iteration and the algorithm seam (`algo`, `pipeline.Iterate`)

Everything above describes one forward pass. `pipeline.Iterate` is the operator
for computations whose next step depends on the last one's answer — a fixpoint,
usually over a graph — and `algo.Algorithm` is the seam that decides *which*
computation.

The framework had two extension points and neither was this one. `Executor`
decides where a task runs; `OpRunner` decides what one task does. Between them
they cover a pipeline's whole cost and none of its shape, because the shape was
fixed at "walk the DAG once". An `Algorithm` is two methods over plain data —
`Seed` returns the messages that make round 0, `Route` consumes a completed
round and returns the messages that make the next — and returning none halts
the computation. It never schedules, never spends, never calls a model. Three
ship: `BSP` (Pregel over edges), `Refine` (a vertex critiquing itself), and
`Beam` (frontier search that grows its own graph).

**A round is a stage batch.** Every active vertex's call is one task through
the same scheduler both drivers use, so admission control, class-aware retry,
the escalation ladder, the governor, the cache, lineage and the event stream
apply to a superstep without any of them learning what a superstep is. The loop
itself is driver-agnostic: it hands each round's tasks to a runner the driver
supplies, so barrier and streaming cannot drift apart on it any more than they
can on ReduceAI's levels.

Three properties carry the design, and each is a consequence of machinery §4.7
already had:

- **The cache key is (op fingerprint, vertex state, inbox)** — deliberately not
  the round. Convergence means vertices stop changing, so a converged vertex's
  key stops changing and re-running it is free. Cost per round *falls* as the
  computation settles, which is the opposite of the usual economics of
  iterative model work. Rerunning a converged loop costs nothing at all, and
  editing one vertex recomputes what its *messages* reached rather than what
  was touched.
- **Quiescence beats caching.** Before building a round's tasks the engine
  checks whether each vertex has already run on this exact (state, inbox); a
  repeat is a local fixpoint and the task is never built. Checking against every
  input the vertex has seen, not just the last, catches oscillation of any
  period. This rests on the vertex program being a function of (state, inbox) —
  the contract the operator declares, and the reason the round number is not
  offered in template scope.
- **The envelope contains a self-directed loop.** A vertex program that follows
  a reference it invented is a program choosing its own next input. `Grow`
  decides whether that creates a vertex or is dropped and counted, and whatever
  it creates runs under the envelope assembled before round zero: the same
  grants, the same deny-by-default egress allowlist, the same budget. Discovery
  widens what the computation reads, never what it may reach.

Three halt conditions apply at once — quiet, a round cap, a stage budget — and
the stage reports which one stopped it, because convergence and exhaustion
produce identical records. The round cap is required at compile time. Two caps
bound the fan-out: `MaxMessages` per vertex, which is necessary and
insufficient, and `MaxFrontier` per round, which is what makes the stage's worst
case `MaxFrontier × MaxRounds` and therefore priceable.

Full design in [ALGORITHMS.md](./ALGORITHMS.md); the reasoning behind choosing
this primitive is in [ITERATION.md](./ITERATION.md).

### 4.12 Projection (`loom.Explain`)

Everything above is measured after the fact. `Explain` is the same accounting
run forward: it compiles the pipeline exactly as `Run` does, then walks the
plan computing per-stage call counts, rendered prompt sizes, prompt-cache
splits, registry-priced cost, and the wall-clock floor the models' per-minute
limits impose — without issuing a call, resolving a secret, or touching the
state dir.

It can be sharp rather than heuristic because of the design's own asymmetry:
pure stages are ordinary Go functions and AI stages are declarative data
(§4.1), so the projection *executes* the cheap skeleton and models only the
paid calls. Record counts are exact rather than extrapolated from a
selectivity guess. Each stage reports an expected cost under one stated
assumption (response length, the single quantity a plan cannot determine) and a
ceiling that rests on none, since `MaxTokens` is a provider-enforced cap — so
the ceiling is what the governor's budget (§4.4) should be set from. Anything
unknowable becomes a named warning rather than a confident wrong number, which
is the same principle the prefix-cache accounting follows in reporting
negative savings while a write is unamortized: state the truth, including when
it is inconvenient.

The sharpest case of that principle is `ParseJSON`. Its output fields are
chosen by the model, so a `Filter` below it drops every record during
projection while keeping them in the run — an under-count, and the only
direction in which a cost projection is dangerous. Those stages are marked,
`Projection.Partial()` reports the whole projection as incomplete, and the
ceiling stops being described as a bound; `loom.WithStageSample` supplies the
field names and restores exactness.

The projection is published on the event bus (§4.8) as `stage.projected` and
`run.projected` rather than only returned, which is what lets an observer hold
both halves of the comparison. Point `Explain` and `Run` at one handler and the
constellation view (`viz`) shows the forecast before the run exists, then reads
each stage's live cost against it — the projection deliberately survives
`run.started`, because it describes the pipeline and the run that follows is
the thing it predicted.

### 4.13 Routing (`route`)

The escalation ladder in §4.4 is a *recovery* mechanism, and recovery is by
definition reactive: every record enters at the bottom rung, so every record
the cheap model cannot handle pays for the call that was always going to fail
before paying for the one that answers. That first call is unrecoverable by
anything else in this document — a failed call produces no result, so there is
nothing to cache, and it asks nothing about the world, so the commons cannot
serve it. The only way not to pay for it is not to make it.

Turning the ladder into a *policy* is a supervised problem, and the reason it
is tractable here is that the labels already exist and were being discarded.
`InferSpec.Validate` is an oracle: written by the user, already run on every
record, and already answering exactly the question — was the model that ran
strong enough? A semantic failure is a labelled negative, a success a labelled
positive. `route` keeps them.

`runtime.Scheduler` consults a `route.Router` once per task, before the first
attempt, and reports every semantic verdict back. The default `route.Adaptive`
holds a Beta posterior per (stage, feature bucket, rung) and picks the rung
minimising the expected cost of *reaching* a valid answer, by backward
induction over the ladder:

```
E[last] = price(last)
E[i]    = price(i) + (1 − p̂(i)) · E[i+1]
```

That objective is the design's one non-obvious choice, and it is not "route to
the model most likely to succeed". A rung that fails 70% of the time is still
the right place to start when the rung above costs a thousand times more; a
rung that usually works is the wrong place to start when skipping it saves
almost nothing. Only expected cost distinguishes the two.

The containment story is what makes it safe to leave on. A router picks a
**starting rung** and nothing else — validation still runs, escalation still
climbs — so:

- a wrong guess costs a call and never an answer, the same guarantee shape as
  the delta certificate (§4.7);
- rungs *k..n* are a subset of *0..n*, so a routed run cannot exceed the
  ceiling `loom.Explain` already reported, and a budget set on that ceiling
  still holds;
- with no evidence the router returns rung 0, so a cold run is byte-for-byte
  the run Loom does without one.

Decisions are deterministic — a pure function of the profile and the request
key, reading no clock and no global random source — so two workers holding one
profile agree and a regenerated report says the same thing. Exploration
therefore cannot live in the decision rule, and lives in a **probe** instead: a
deterministic slice of would-be-routed tasks held at the bottom rung anyway.
This is the part that is easy to leave out and shouldn't be. Once a bucket is
routed upward the bottom rung stops being sampled, and the estimate that sent
it there can never be contradicted — a system that quietly congratulates
itself. The probes are the only evidence that ever contradicts a skip, which is
what makes the reported saving a measurement rather than an assertion, and the
run report prints the two together always.

A `route.Profile` is plain serializable data, appended to the state directory
beside the result cache and summed rather than overwritten, so several
processes calibrating one pipeline compose. The router sits on the host beside
the cache and the rate limiter, for the reason those do: what a stage's records
cost to get right is a property of the work rather than of the pipeline that
discovered it, so every agent on a fleet shares one.

`loom.Explain` reads the same profile through the same estimator, which gives a
projection the number it could not previously compute at all — its columns
price one call per record at the *base* model, so a stage with a ladder was
under-projected by exactly the escalations. Sharing `route.Adaptive` rather
than reimplementing the arithmetic is what stops the forecast and the
scheduler drifting into disagreement. [ROUTING.md](./ROUTING.md).

## 5. Failure taxonomy (summary)

| Class | Detected by | Recovery |
|---|---|---|
| `transient` | 429/5xx/529, timeouts, network errors | Backoff + jitter, retry same model |
| `semantic` | JSON parse failure, `Validate` rejection, model refusal | Escalate up the binding ladder, retry |
| `permanent` | 4xx, missing grants, template errors, user-code bugs | Dead-letter immediately |
| `budget` | Governor limit crossed | Stop admitting work; return partial results + spend |

Unclassified errors default to *permanent*: a user-code bug should fail
fast, not burn paid retries.

## 6. Scaling path: from local runtime to distributed system

The local runtime is a complete, correct single-node system. Scaling out
does not change the programming model or the planner — it replaces the
executor and the state backends behind existing interfaces.

**Phase 1 — remote executor fleet (implemented).** `Executor` implemented as a
client to a worker service: tasks (already serializable, envelope included) go
onto a queue with **leases**; workers claim, execute, and report. It landed
exactly where the interface said it would — no change to the planner, the
scheduler, the ops, or a single pipeline — and the one thing the design note
underestimated is worth stating: lease expiry gives at-least-once *delivery*,
and turning that into at-least-once *execution without duplicate results* needs
two mechanisms rather than one. Content addressing makes a duplicate
execution's output identical and its write idempotent, as predicted; a
**fencing token** on the commit is what stops the worker that was presumed dead
from overwriting the worker that replaced it when it turns out to have been
merely slow. §4.5 has the details, `examples/worker-fleet` runs it, and
`worker_process_test.go` kills a worker mid-call and checks the run's answers
against a single-process baseline.

Still open in this phase: the scheduler's admission control is still per-client,
so a fleet's collective respect for provider limits rests on how the clients are
configured rather than on a shared token-bucket service; per-call telemetry
stays in the process that made the calls, so a remote run's report is exact in
tokens, cost and cache rate but counts model calls per task; and the queue is a
directory or a map, with a broker-backed implementation of the same contract
left to whoever needs many hosts.

**Phase 2 — shared state.** The CAS maps naturally onto object storage
(S3/GCS) with the same hash keys; the cache index and lineage onto any
KV/OLTP store. Nothing in the interfaces assumes locality. Phase 1 took the
first step of this on its way past: a state directory shared by a fleet is
already shared state, and `store.Cache` now re-reads its append-only index on a
miss so a worker replays what a *sibling process* paid for rather than only what
it paid for itself. What remains is the same trick against storage that is not a
filesystem, behind a `worker.Blobs`-shaped interface the executor already talks
through.

**Phase 3 — sandbox depth.** The envelope's sandbox profiles are implemented
by worker runtimes: subprocess isolation for untrusted pure ops, containers
for tool-running agents, and — the most promising direction — **WASM**
sandboxes whose imports are generated *from the envelope's grant set*, making
the capability model enforceable at the instruction level rather than by
convention.

**Phase 4 — streaming and dynamic scheduling.** Pipelined execution is
implemented: `loom.WithStreaming()` runs every stage concurrently against one
global pool of execution slots, moving each record downstream as its own task
completes rather than at a stage barrier (see
[INFERENCE.md](./INFERENCE.md#asynchronous-agents-continuous-batching)). The
barrier driver remains the default because streaming trades input ordering
for occupancy. Cost-based model routing — the escalation ladder generalized from recovery to
policy — is **implemented**, and landed one step further than this line
proposed. The obstacle it names, "route records to cheaper models and escalate
only the hard ones", is a supervised problem with no obvious labels; what makes
it tractable is that Loom already has the labels and was discarding them.
`InferSpec.Validate` runs on every record and says whether the model that ran
was strong enough, so a router needs no training set, only the verdicts a
validated stage already produces. `route.Adaptive` (§4.13) keeps them, and
picks the rung minimising the expected cost of *reaching* a valid answer rather
than the rung most likely to succeed — which is a different and cheaper
objective. Still open in this phase: speculative re-execution of stragglers —
now worth adding, since a straggler delays only its own record.

**Phase 5 — iteration (implemented).** The dimension the DAG could not express
at all: a stage looking at its own output and deciding to run again. Deep
research, entity resolution, knowledge-graph construction, and refine-until-good
are all fixpoints, and most are fixpoints over a graph. `pipeline.Iterate`
(§4.11) is that primitive, generalized one step further than
[ITERATION.md](./ITERATION.md) proposed: rather than a graph operator, an
operator whose *control flow* is a plug-in, so bulk-synchronous message passing
is one algorithm among several rather than the only shape available. It landed
on the existing scheduler unchanged — a superstep is a stage, a vertex's call is
a task — and inherits the four properties that make a paid loop safe:
dollar-bounded halting, cost per round that *falls* as vertices go quiet and
their cache keys stop changing, envelope containment for a program that
discovers its own targets, and lineage across hops.

The constellation view draws such a stage as concentric orbits — one ring per
superstep, the live ring turning, the outer rings thinning as vertices go quiet
— with the per-round frontier and the halt reason in the stage inspector, in the
colour that says whether the loop converged or was cut off.

Still open in this phase: the inbox tree-reduce for high-degree vertices (today
a cap, which is blunt but reported).

**Also on the roadmap:**
- **Semantic caching** — landed, but one level up rather than where this line
  originally proposed it. Putting embedding similarity in front of the *result*
  cache would have matched near-duplicate task inputs; what actually duplicates
  between concurrent agents is external *research*, and the reuse worth having
  is keyed on the question rather than on the record. `findings` is that
  (§4.7), and its embedding tier is the optional refinement rather than the
  mechanism — the free structural tiers answer most of it. Still open there:
  vCache-style calibrated error rates, bi-temporal validity, and eviction.
- **Ensemble/quorum operators** — N samples + vote or judge as a native op
  (today expressible as FlatMap → Infer → Combine).
- **Data-access capabilities** — `data:read:<name>` exists today for
  broadcasts; extend it to externally-backed datasets with broker-mediated
  loaders, so a stage can be granted a table it streams rather than one
  materialized up front.
- **Agentic operators** — multi-turn tool-using tasks inside the same
  envelope/budget/sandbox machinery.

## 7. What is implemented vs. designed

Implemented and tested in this repository: the pipeline API, planner
(validation, fusion, fingerprints, least-privilege envelopes), scheduler
(admission control, governor, class-aware retries with escalation), local
executor, capability/secret/egress/broadcast/audit security, CAS + persistent
cache + lineage, content-hash-referenced broadcast values, shared prompt
prefixes with provider prompt-cache accounting, streaming (continuously
batched) execution alongside the barrier driver, pre-flight cost projection
(`loom.Explain`), event bus + run reports, tree AI-reduce, iterative execution
with a pluggable algorithm seam (`pipeline.Iterate` + `algo`, with BSP, refine
and beam algorithms, quiescence detection, three-way halting, fan-out caps,
open-world growth, per-round projection and round events), the shared research
layer (`findings`: question-keyed lookup over three tiers, a sufficiency ladder
with memoized adjudication under a break-even rule, single-flight leases over
subjects, negative results, per-topic volatility horizons, append-only revision
and retraction with dependent reporting, and capability containment), mock,
Anthropic,
OpenAI, and llama.cpp providers (the last with device-width admission control,
loopback egress, no-credential envelopes, and KV-cache prefix reuse),
cross-restart and cross-process cache resume, stream mode (`loom.Stream`:
unbounded partitioned sources with resumable positions, per-split watermarks
with bounded lateness, idleness and retirement, watermark holdback across
asynchronous stages, event-time windowing with keyed and sliding assigners and
count/interval triggers, pane-delimited aggregates, sinks with pane-stable
batch identity, quiesce checkpointing tying window state to source positions and
sink commits, restart-and-resume, and file and Kafka sources and sinks — see
[STREAMING.md](./STREAMING.md)), cost-based model routing (`route`: an online per-bucket, per-rung estimate over
the validator's own verdicts, expected-cost selection of a starting rung, a
deterministic probe holdout that keeps the saving measurable, calibration
persisted across runs and shared across a fleet, and a ladder projection in
`loom.Explain` computed by the same estimator the scheduler routes with),
and the remote executor fleet
(`worker`: a durable queue with leases, heartbeats, expiry and fencing tokens;
capability-advertising workers; CAS-referenced inputs and outputs; idempotent
result commit; two queue backends behind one conformance suite; and failure
tests for worker death, late results, lease expiry, network interruption and
duplicate execution, including a multi-process test that SIGKILLs a worker
mid-call).

Designed but not yet implemented: a shared admission-control service so a fleet
respects provider limits collectively rather than per client, a broker-backed
queue for fleets spanning hosts, object-storage state backends,
subprocess/container/WASM sandbox runtimes, ensemble operators,
priority/preemptive scheduling, result-cache eviction, a single-flight lease on
the *result* cache (the findings gate has one; the result cache does not, so
concurrent identical tasks still both run — see `examples/commons`), findings
eviction and bi-temporal validity, the inbox tree-reduce for high-degree
vertices, and the later phases of stream mode (transactional sinks, renewing
rate budgets with backpressure/shed/degrade policies, session windows, split
assignment across a fleet, and windowing in a bounded run as a group-by). The interfaces above are the contract
those implementations plug into.

---

## 8. Design notes

Short rationales that belong to no single component, collected.

### Working notes

- AI operators are pure data and tasks are JSON-serializable (tested), so
  remote/sandboxed executors plug in behind the `Executor` interface without
  touching planning or scheduling.
- Go-function stages run in-process; give them `pipeline.WithVersion("v1")` to
  make them cacheable (bump the version when behavior changes).
- Unclassified errors are treated as permanent so user-code bugs fail fast
  instead of burning paid retries; providers classify their own errors.

### One path, so nothing can drift

- Both drivers execute tasks through the same `Scheduler.RunTask`, so retry,
  escalation, admission control, and the budget governor cannot drift between
  barrier and streaming execution. `loom.Run` is a fleet of one, built through
  the same construction path, so what a run and an agent share cannot drift
  either.
- A fleet's agents coordinate at agent boundaries, not inside a task. A task
  that could publish mid-run would make its own cached result depend on
  execution order, and a task replayed from cache would not publish at all — so
  the board's contents would depend on how warm the cache happened to be.
  [ASYNC.md](./ASYNC.md).
- Findings are written from *inside* a task, which the blackboard deliberately
  forbids — and the exemption is earned rather than assumed. A blackboard post
  changes what a later stage in the same run reads, so it cannot happen
  mid-task. A findings hit is *substitutable* for the call it replaces, so it
  can: the answer is the same either way and only the spend differs, which is
  the trade worth making and the one `examples/commons` asserts rather than
  claims. [FINDINGS.md](./FINDINGS.md).

### Why a declaration beats a closure

- The studio's document is declarative because it has to survive a round trip
  through a browser, and that constraint pays for itself: a step's cache version
  can be the hash of its own declaration, an assistant can propose a diff rather
  than generate code, and the answer shape can be one declaration instead of
  three that agree until someone edits one. What it cannot express is arbitrary
  Go — which is what the Go export is for. [STUDIO.md](./STUDIO.md).
- A continuation is a *reference to something that changes*, which is the one
  thing Loom had no shape for. A broadcast is registered once because every task
  reads the same bytes; a continuation is registered per run because each run
  reads a little more than the last. Getting that distinction into the type
  system is what makes the rest fall out: the envelope holds a revision hash, so
  it stays small; the hash joins the fingerprint, so the cache invalidates
  exactly the round that changed; the key is what a queue scores locality on;
  and the parent hash is what tells an executor which of its own state it may
  build on. [DELTA.md](./DELTA.md).

### Proving only what can be proved cheaply

- The certificate in `delta` deliberately does **not** prove the splice was
  right, and saying so is the point. It proves the splice's arithmetic — that
  the result is the parent's first *n* bytes plus what was re-rendered, that the
  segments just before the boundary came out identical, that the digest follows
  — in time proportional to the change. Proving more would mean reading the
  bytes it did not touch, which is the rebuild it exists to avoid. So the honest
  design is a ladder: a declared bound on how far a change reaches, evidence at
  the seam on every splice, a sampled full recomputation to test the declaration
  itself, and a fallback that is the same code path with the window widened all
  the way. [DELTA.md](./DELTA.md).
- The cheapest tier of the findings gate is also the most certain, and that is
  structural rather than lucky. Matching on topic-and-facets says two questions
  are about one subject; matching on embedding similarity says only that they
  look alike, so it yields candidates that must then be *checked*. The tier that
  needs a model is therefore the last one tried, its verdicts are memoized per
  question-and-finding pair, and it is skipped entirely when the ledger's own
  record of what a topic costs to research says the judgement would cost more
  than the lookup could save. [FINDINGS.md](./FINDINGS.md).

---

## 9. Where the layers came from

The dimension Loom was missing longest was **iteration**: records flowed forward
once, so nothing could look at a stage's output and decide to go around again —
which ruled out the workloads people most want, deep research, entity
resolution, knowledge-graph construction and refine-until-good, all of which are
loops and most of which are loops over a graph.
[ITERATION.md](./ITERATION.md) makes the case that Loom is the only place this
can be built *safely*: the governor bounds a loop in dollars, content-addressed
caching makes cost per round *fall* as it converges, envelopes contain a program
that discovers its own targets, and lineage is the only way to audit six hops of
model-derived inference.

It is now built, and one step further than that document proposed. Rather than a
graph operator, `pipeline.Iterate` is an operator whose *control flow* is a
plug-in — so Pregel is one algorithm among several rather than the only shape
available, and writing a new one is two pure methods that need neither a model
nor a scheduler to test. [ALGORITHMS.md](./ALGORITHMS.md) is the design;
`examples/multi-hop` is it answering a research question by walking a citation
graph the model chooses as it goes, reaching a conclusion three hops from
anything the question named.

Iteration is also what makes the newest layer worth having. A loop's context
grows by a finding, a critique, a turn — a little each round, all of it needed
every round — and until then every round paid for the whole of it, twice over:
once to render it and once to ship it to whichever process was running.
**Stateful delta execution** ([DELTA.md](./DELTA.md)) makes that context an
immutable chain in the CAS, so what an envelope carries is a hash and what an
executor holding the previous revision does is splice. The design worth reading
for is not the splice but the discipline around it, borrowed almost verbatim
from TokTier's guarantee ladder: a declared bound on how far a change can reach,
per-splice evidence at the seam, sampled recomputation against the reference
path, and a fallback that is the same code with the repair window opened all the
way. State makes a round cheap; it is never what makes it right. Kill the worker
carrying a session and the next round rebuilds from the chain and answers
identically, which is the whole claim and the thing `examples/delta-session`
checks rather than asserts.
