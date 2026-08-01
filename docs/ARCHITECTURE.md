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

### 4.6 Model layer

`model.Registry` holds every model a deployment may use: provider, pricing,
rate limits, tier, required secret. Stages bind by explicit ID or by
**tier** (`fast` / `balanced` / `deep`), keeping pipelines portable across
model generations. A binding's **escalation ladder** is the ordered list of
increasingly capable models used by semantic recovery.

Three providers ship today: a deterministic `Mock` (tests, offline
development, scripted failures), and `providers/anthropic` and
`providers/openai` over the official SDKs (per-call broker-resolved keys,
429/5xx → transient, 4xx → permanent, refusals → semantic).

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

### 4.11 Projection (`loom.Explain`)

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

**Phase 1 — remote executor fleet.** Implement `Executor` as a client to a
worker service: tasks (already serializable, envelope included) go onto a
queue with **leases**; workers claim, execute, and report. Lease expiry
gives at-least-once execution; the deterministic cache key makes duplicate
execution harmless (idempotent writes into the CAS). The scheduler's
admission control moves to a shared token-bucket service so the whole fleet
respects provider limits collectively.

**Phase 2 — shared state.** The CAS maps naturally onto object storage
(S3/GCS) with the same hash keys; the cache index and lineage onto any
KV/OLTP store. Nothing in the interfaces assumes locality.

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
for occupancy. Still open in this phase: speculative re-execution of
stragglers — now worth adding, since a straggler delays only its own record —
and cost-based model routing (route records to cheaper models and escalate
only the hard ones, the escalation ladder generalized from recovery to
policy).

**Phase 5 — iteration.** The dimension the DAG cannot express at all: a stage
cannot look at its own output and decide to run again. Deep research, entity
resolution, knowledge-graph construction, and refine-until-good are all
fixpoints, and most are fixpoints over a graph. The proposed primitive is
bulk-synchronous message passing — Pregel supersteps whose vertex program is a
model call — which lands on the existing scheduler unchanged (a superstep is a
stage, a vertex's call is a task) and inherits the four properties that make a
paid loop safe: dollar-bounded halting, cost per round that *falls* as
vertices go quiet and their cache keys stop changing, envelope containment for
a program that discovers its own egress targets, and lineage across hops. The
full design, the hard parts, and the four-step path are in
[ITERATION.md](./ITERATION.md).

**Also on the roadmap:**
- **Semantic caching** — embedding-similarity lookup in front of the exact
  cache for near-duplicate inputs.
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
(`loom.Explain`), event bus + run reports, tree AI-reduce, mock, Anthropic,
and OpenAI providers, and cross-restart cache resume.

Designed but not yet implemented: iterative/graph execution (phase 5, see
[ITERATION.md](./ITERATION.md)), remote executor backends, shared state
stores, subprocess/container/WASM sandbox runtimes, semantic cache, ensemble
operators, priority/preemptive scheduling, and result-cache eviction. The
interfaces above are the contract those implementations plug into.
