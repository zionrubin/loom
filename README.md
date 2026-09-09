# Loom

**Large-scale Orchestration Of Models** — an AI-native data processing
framework in Go.

Loom is MapReduce/Spark rethought for pipelines whose operators are model
calls: declarative dataflow over records, where every task runs under an
explicit least-privilege envelope, the scheduler speaks the language of rate
limits and dollar budgets, invalid model output escalates to stronger models
automatically, and completed AI work is never paid for twice.

```go
p := pipeline.New("ticket-triage")
src := p.FromRecords("tickets", tickets)

classified := src.Infer("classify", pipeline.InferSpec{
    Binding:   model.Binding{Tier: model.TierFast,               // run cheap,
               Escalation: []string{"claude-sonnet-5"}},         // escalate when output is invalid
    System:    "You classify support tickets.",
    Prompt:    "Classify this ticket: {{.subject}}",
    ParseJSON: true,                                             // parse output into the record
    Validate:  func(r core.Record) error { ... },                // semantic gate
})

classified.
    Filter("urgent-only", func(r core.Record) (bool, error) {
        b, _ := r.Data["urgent"].(bool); return b, nil
    }).
    ReduceAI("briefing", pipeline.ReduceAISpec{                  // hierarchical tree reduce
        Binding: model.Binding{Model: "claude-opus-4-8"},
        Prompt:  "Summarize {{.Count}} items:\n{{range .Items}}- {{.}}\n{{end}}",
        FanIn:   8,
    })

res, err := loom.Run(ctx, p,
    loom.WithRegistry(reg),
    loom.WithSecrets(map[security.SecretRef]string{"anthropic_api_key": key}),
    loom.WithRunBudget(core.Budget{MaxCostUSD: 5.00}),           // hard dollar cap
    loom.WithStateDir("./state"),                                // cache = checkpoint = resume
)
fmt.Print(res.Report)                                            // per-stage cost, tokens, retries, p95
```

## Why

Classic data frameworks assume operators are cheap, deterministic, and trusted,
and that throughput is bounded by cores. AI workloads violate all of that: calls
cost real money, outputs can be *wrong* while the call succeeds, throughput is
bounded by provider rate limits, and operators execute model-derived behavior
that shouldn't be ambiently trusted. Loom makes each difference a first-class
concept — [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is the full design.

The closest prior art isn't another data framework — it's the inference serving
engines (vLLM, SGLang), which solve the same problem one level down: many
expensive, variable-latency model calls that share structure, under a hard
resource ceiling. Loom borrows their two central mechanisms, prefix cache
sharing and continuous batching; [docs/INFERENCE.md](docs/INFERENCE.md) maps the
rest of that playbook onto this one, including what is deliberately left out.

That literature has since moved up an altitude, to serving *programs* rather
than requests. Loom follows it there with `loom.Fleet`: many pipelines running
at once as one engine, with one quota, one ceiling, one cache, slots admitted
fairly between them, and a blackboard they use to reach each other's
conclusions — mapped against the agentic-serving literature in
[docs/ASYNC.md](docs/ASYNC.md), which also says which rows are honestly still
empty.

## Quickstart

```sh
go test ./...              # full suite, no network or keys needed
go run ./examples/triage   # a complete pipeline on a mock model, offline

# a desk that reads conversations as they arrive and answers questions about them
go run ./examples/switchboard

# four processes, one provider account: a $0.05 ceiling that costs $0.20 without it
go run ./examples/treasury

# one pipeline, three deployment policies: the refused round makes zero calls
go run ./examples/clearance

# watch a run as a sky of stars: http://localhost:8077
go run ./examples/constellation

# cache = checkpoint: the second run makes zero model calls
LOOM_STATE=/tmp/loom go run ./examples/triage
LOOM_STATE=/tmp/loom go run ./examples/triage

# real models
ANTHROPIC_API_KEY=sk-... go run ./examples/anthropic-review
OPENAI_API_KEY=sk-...    go run ./examples/openai-review
```

Twenty-four examples ship — fleets, streaming, serving, MCP, local inference,
worker processes, iteration, routing, governance, the studio — and all but five
run offline against a deterministic mock provider.
[docs/EXAMPLES.md](docs/EXAMPLES.md) is the catalog.

## What you get

- **Declarative pipelines** — `Map` / `Filter` / `FlatMap` / `Combine` plus
  AI-native `Infer` (templated per-record inference, JSON parsing, validation)
  and `ReduceAI` (parallel tree aggregation). Branching builds DAGs; the planner
  fuses adjacent pure stages.
- **Least-privilege task envelopes** — every task carries an explicit,
  serializable declaration of its model binding, capability grants, secret
  references, egress allowlist, context bundle, budget, and sandbox profile. The
  planner assembles the *minimal* envelope automatically; executors enforce it
  at the moment of use, with an append-only audit log.
- **Governance — the authority the pipeline's author does not hold** — least
  privilege answers "what does this task need?" and never asked the prior
  question, which is *who decides what it may need*. The planner assembles the
  minimal envelope that satisfies **what the stage declared**, so the author is
  also the security officer; in a deployment those are two people.
  `loom.WithPolicy` is the missing one, and it needs no new vocabulary: the
  envelope is already the complete statement of what a task may use, so a
  policy is a **predicate over envelopes** — which models, tools, hosts and
  secrets, under which isolation, for how much money. Data classes are declared
  where data *enters* and propagate to every stage downstream, because that is
  where the data goes, so a clearance table (`pii` → self-hosted models only)
  refuses a pipeline **before a single call** — the whole escalation ladder
  judged, not just the rung it starts on, and every violation reported at once.
  An admitted run is still contained: every envelope is narrowed to what the
  policy permits, under an invariant the tests assert — **narrowing never
  widens**, so on the conforming path it is the identity. A policy is JSON, so
  it is a document reviewed and versioned beside the infrastructure it governs
  rather than a function compiled into the program it constrains.
  [docs/POLICY.md](docs/POLICY.md)
- **An AI-aware scheduler** — per-model token-bucket admission control
  (requests/min and tokens/min), a run-level dollar/token budget governor with
  graceful partial results, and class-aware recovery: transient failures back
  off, semantic failures climb the model escalation ladder, permanent failures
  dead-letter, budget exhaustion stops the run.
- **Cost before you spend it** — `loom.Explain` projects a run without making a
  single model call: per-stage calls, rendered prompt sizes, prompt-cache
  economics, priced cost, and the wall-clock floor rate limits impose. Record
  counts are exact rather than extrapolated, and every stage reports a
  **ceiling** that rests on no assumption — the number to hand `WithRunBudget`.
  [docs/EXPLAIN.md](docs/EXPLAIN.md)
- **A ladder that learns — routing, not just recovery** — an escalation ladder
  is reactive: every record enters at the bottom, so every record the cheap
  model cannot handle pays for the call that was always going to fail *and* the
  one that answers, and nothing caches a call that produced no result. But the
  labels already exist — `Validate` is an oracle that already runs on every
  record — so `loom.WithRouting()` keeps those verdicts and starts each record
  on the rung expected to answer it. It picks a *starting* rung and nothing
  else, so **a wrong guess costs a call and never an answer**, and rungs
  *k..n* being a subset of *0..n* means it can never exceed the ceiling
  `Explain` already reported. A deterministic slice of would-be-routed tasks is
  held at the bottom anyway, because a saving nobody tests is a claim rather
  than a measurement — the report never prints one without the other. With a
  state dir the calibration outlives the run.
  [docs/ROUTING.md](docs/ROUTING.md)
- **Content-addressed caching = checkpointing** — task results are keyed by op
  fingerprint + input content. Reruns and crash recovery replay completed AI
  work with zero model calls and zero cost, across process restarts with a state
  dir, and across the *processes of a fleet* with a shared one. A cache serves
  the second asker only after the first has finished, so the lookup also hands
  out a **single-flight lease**: identical tasks admitted together collapse onto
  one call instead of each paying for the same answer. The lease bounds a saving
  and never an answer — the wait lives inside the task's own budgeted duration
  rather than beside it, and a task that reaches the ceiling simply computes.
  A task settled without a model call also hands its rate-limit admission back,
  so a run that mostly replays stops throttling itself against a quota it never
  spent.
- **Iteration, with a pluggable algorithm** — `pipeline.Iterate` runs a model
  operation over a record set repeatedly, and `algo.Algorithm` decides what
  "repeatedly" means. Three ship: `BSP` (Pregel message passing), `Refine` (a
  record critiquing itself), `Beam` (frontier search). A vertex's cache key is
  its state and its inbox rather than its round, so **cost per round falls as
  the loop converges**. [docs/ALGORITHMS.md](docs/ALGORITHMS.md)
- **Broadcasts and shared prompt prefixes** — register a rubric or taxonomy once
  and every task *references* it by hash instead of copying it; put the
  stage-stable head of a prompt in `InferSpec.Prefix` and the provider's cache
  serves it rather than reprocessing it per record. Both join the stage
  fingerprint, so editing one recomputes exactly what saw it.
  [docs/SHARING.md](docs/SHARING.md)
- **Pipelined execution** — `loom.WithStreaming()` replaces the stage barrier
  with continuous batching: a record moves downstream when its own task
  finishes, not when its whole stage does, and every stage draws from one global
  pool of slots. The trade is ordering, so the barrier driver stays the default.
- **Stream mode — an input that never ends** — `loom.Stream` runs a pipeline
  against a `stream.Source` until stopped. `Dataset.Window` cuts an endless
  input into finite sets; watermarks say when a set is complete; a checkpoint
  ties window state, source positions and sink commits into one recoverable
  point. Delivery is at-least-once and spend is exactly-once. File and Kafka
  sources and sinks ship. [docs/STREAMING.md](docs/STREAMING.md)
- **Fleets — many agents on one engine** — `loom.Fleet` runs any number of
  pipelines at once and holds what was never a property of a pipeline: **one**
  rate limiter, **one** budget governor, **one** cache, **one** set of slots. A
  contended slot goes to the agent whose *program* has been served least, so a
  three-call summary overtakes a 10,000-record sweep. Agents coordinate through
  an append-only **blackboard**. [docs/ASYNC.md](docs/ASYNC.md)
- **One quota, one wallet — across processes** — a fleet gives every agent in a
  process one limiter and one ceiling. A *deployment* is several processes, and
  each of them was getting its own: ten workers each admitting against 4,000
  requests/min collectively admit against 40,000, and
  `WithRunBudget(MaxCostUSD: 100)` is a hard cap in one process and a $1,000 cap
  in ten — silently, because all ten finish inside their budget and say so.
  `loom.WithSharedQuota` puts the per-minute buckets and the ceiling where every
  process can see them: a shared directory for one host, an HTTP service for a
  fleet spanning hosts, both passing one conformance suite. It composes with the
  run budget rather than replacing it, so a run stops at whichever ceiling it
  reaches first — and `Explain` now answers the question an operator actually
  has, which is not what a run costs but whether there is enough left for it.
  The shared quota **errs low, never high**: a draw a dead process never
  returns costs the fleet throughput until the bucket refills, and a store
  nobody can reach refuses admission rather than granting it.
  [docs/QUOTA.md](docs/QUOTA.md)
- **MCP tools, under the envelope** — stages declare what they may call and the
  planner turns that one declaration into a grant per tool, the server's host on
  the egress allowlist, and the digest of the descriptors it was compiled
  against. Connections are made **once per host**, before any task exists, so a
  fleet of ten agents shares one session. [docs/MCP.md](docs/MCP.md)
- **A fleet of worker processes** — `loom.WithWorkerService` puts a run's tasks
  on a durable queue with leases; `loom.Serve` is the other side. A worker killed
  mid-call loses its claim rather than the task, and fencing tokens make
  at-least-once delivery produce exactly-once *work*. Two queue backends pass one
  conformance suite. [docs/WORKERS.md](docs/WORKERS.md)
- **Stateful delta execution** — a context that grows a turn at a time lives in
  the CAS as an immutable chain, so an **envelope carries a couple of hundred
  bytes where it would otherwise carry the transcript** (2981× on the example's
  617 kB session) and an executor holding an earlier revision **splices** rather
  than re-renders — under a guarantee ladder that makes a failed fast path cost
  work and never an answer. [docs/DELTA.md](docs/DELTA.md)
- **A serving layer over a feed that never stops** — `recall.Desk` turns an
  endless stream of conversations into a **context that is maintained rather
  than retrieved**: every message is understood as it arrives, each slice of a
  conversation becomes one line of a `delta` chain, and a question is one model
  call against a couple of kilobytes — the same one call whether the desk has
  read ten messages or ten million. The two sides meet at an immutable published
  revision, so ingestion never blocks a read and the pool's fairness lets a
  question overtake an hour-old ingest job. The revision joins the query's
  fingerprint, so **the same question against an unchanged context costs
  nothing**, and moves the moment a conversation lands.
  [docs/RECALL.md](docs/RECALL.md)
- **A commons for external research** — `loom.WithFindings` gates the tools that
  reach public sources, keying on the *question* rather than the bytes: one
  question in three wordings, asked by agents that started at the same instant,
  becomes one call. PostgreSQL/`pgvector` or a shared directory extends the same
  gate across executor processes. [docs/FINDINGS.md](docs/FINDINGS.md)
- **Lineage & audit** — every artifact traces to the op, model, and inputs that
  produced it; every secret/tool/egress/broadcast decision is audited, as is
  every admission — a deployment asked to prove a run was authorized needs the
  line that says so, not only the absence of one saying it was not.
- **Observability** — a typed event bus and per-stage run reports (tasks,
  failures, retries, cache hits, tokens, dollars, latency percentiles), plus the
  **constellation view**: a live, zero-dependency web UI where every task and
  executor is a star, with per-node prompts and responses, lineage, a run
  summary, forecast-against-actual, and a universe of every run in the process.
  [docs/VIZ.md](docs/VIZ.md)
- **A canvas that prices itself** — `studio` serves **Loom Studio**: the same
  pipeline as a document you edit in the browser, with `loom.Explain` running
  behind every keystroke. ⌘K proposes edits it computed rather than generated,
  and `Doc.Go` exports the canvas as a Go program that compiles, because a
  builder you cannot leave is a trap. [docs/STUDIO.md](docs/STUDIO.md)
- **Providers, hosted or your own** — a deterministic mock (offline dev,
  scripted failures), Anthropic and OpenAI adapters over the official SDKs, and
  **local inference** through `providers/llamacpp`: the model runs on your
  hardware behind the same seam, so nothing in a pipeline changes — what changes
  is the envelope, and every change is a simplification (no credential, loopback
  egress still *on* the allowlist, the device's decode width as the ceiling, the
  KV cache as the prompt cache). [docs/INFERENCE.md](docs/INFERENCE.md)

## Documentation

| Doc | |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | The full design, component by component, plus the scaling path and design notes |
| [EXAMPLES.md](docs/EXAMPLES.md) | Every example, what it demonstrates, and how to run it |
| [EXPLAIN.md](docs/EXPLAIN.md) | Knowing what a run will cost before making a call |
| [ROUTING.md](docs/ROUTING.md) | The escalation ladder as policy: not paying for the call that was going to fail |
| [SHARING.md](docs/SHARING.md) | Broadcasts and shared prompt prefixes |
| [VIZ.md](docs/VIZ.md) | The constellation view and the universe of runs |
| [ALGORITHMS.md](docs/ALGORITHMS.md) | The algorithm seam: BSP, refine, beam |
| [ITERATION.md](docs/ITERATION.md) | Why iteration is the dimension that was missing |
| [ASYNC.md](docs/ASYNC.md) | Fleets, attained-service scheduling, the blackboard |
| [QUOTA.md](docs/QUOTA.md) | One rate limit and one wallet, shared across processes |
| [POLICY.md](docs/POLICY.md) | Governance: the authority a pipeline author does not hold |
| [FINDINGS.md](docs/FINDINGS.md) | The commons: sharing research between concurrent agents |
| [DELTA.md](docs/DELTA.md) | Stateful delta execution, and proving a splice was safe |
| [WORKERS.md](docs/WORKERS.md) | Distributing a run across worker processes |
| [STREAMING.md](docs/STREAMING.md) | Stream mode: windows, watermarks, checkpoints |
| [RECALL.md](docs/RECALL.md) | The serving layer: a context maintained rather than retrieved |
| [MCP.md](docs/MCP.md) | Tools under the envelope |
| [INFERENCE.md](docs/INFERENCE.md) | Loom as an inference engine, and running the model yourself |
| [STUDIO.md](docs/STUDIO.md) | The pipeline as a canvas that prices itself |

## Package map

| Package | Role |
|---|---|
| `core` | Records, usage/cost accounting, budgets, failure taxonomy |
| `pipeline` | Authoring API: datasets, stages, options |
| `algo` | The `Algorithm` interface an iterative stage plugs in, plus BSP, refine and beam |
| `plan` | Validation, fusion, fingerprints, least-privilege envelopes |
| `runtime` | Scheduler, retries, rate-limit admission, budget governor, and the fleet's slot pool |
| `executor` | Executor seam, capability-scoped runtime, model client, tools |
| `ops` | Operation runners (infer, reduce, fused transforms) |
| `model` | Provider abstraction, registry, tiers, escalation bindings, mock |
| `route` | Where on a stage's ladder a task starts: an online estimator over the validator's own verdicts, a probe that keeps the saving measurable, and a profile that outlives the run |
| `quota` | The account's two singletons — per-minute buckets and a ledger — where every process can reach them: the `Store` contract, a shared directory, an HTTP service and client, and the coalescing front end the limiter and governor plug into |
| `quota/quotatest` | The conformance suite both quota backends pass |
| `mcp` | MCP client: server descriptors, stdio/HTTP transports, the host-owned connection catalog, tool adapters |
| `mcp/mcptest` | A scriptable in-process MCP server for tests and offline examples |
| `providers/anthropic` | Official-SDK Anthropic adapter, broker-resolved keys |
| `providers/openai` | Official-SDK OpenAI adapter, broker-resolved keys |
| `providers/llamacpp` | Local inference against a llama.cpp server: loopback egress, no credential, KV cache as prompt cache |
| `providers/llamacpp/llamacpptest` | A scriptable in-process llama.cpp server on a real loopback socket |
| `findings` | The commons: a gate agents pass before reaching a public source. `pgstore` / `filestore` share it between processes |
| `recall` | The serving layer: conversation ingestion, a continuously maintained context, and an agent-like query interface over it |
| `stream` | Sources with resumable positions, watermarks, the windower, sinks, checkpoints — plain data, no model in sight |
| `stream/file` | A directory of JSONL as a stream: a file is a split, a byte offset a position |
| `stream/kafka` | Topics as streams, with Loom's checkpoint as the source of truth for offsets |
| `security` | Grants, secret broker, egress policy, audit log |
| `policy` | The deployment's constraint on what a pipeline may do: rules over models, tools, egress, secrets, sandbox and data classes; the admission gate over a compiled plan and the containment over every envelope it builds |
| `store` | Content-addressed store, persistent cache, lineage |
| `observe` | Event bus, metrics collector, run reports |
| `viz` | Constellation view: tasks and executors as stars, and the universe of every run |
| `studio` | Loom Studio: canvas, live projection, ⌘K proposals, a Go export that compiles |
| `worker` | Durable task queue with leases, heartbeats, expiry and fencing tokens; `filequeue` spans processes, `queuetest` is the conformance suite |
| `task` | Task + envelope types (serializable — the distribution seam) |

## Working notes

- AI operators are pure data and tasks are JSON-serializable (tested), so
  remote/sandboxed executors plug in behind the `Executor` interface without
  touching planning or scheduling.
- Go-function stages run in-process; give them `pipeline.WithVersion("v1")` to
  make them cacheable (bump the version when behavior changes).
- Unclassified errors are treated as permanent so user-code bugs fail fast
  instead of burning paid retries; providers classify their own errors.

The longer rationales — why both drivers share one `RunTask`, why a fleet's
agents coordinate only at agent boundaries, why the delta certificate proves
arithmetic rather than correctness — are collected in
[docs/ARCHITECTURE.md §8](docs/ARCHITECTURE.md#8-design-notes), and the scaling
path in [§6](docs/ARCHITECTURE.md#6-scaling-path-from-local-runtime-to-distributed-system).

## Demo

[▶️ Watch the demo](./assets/demo.mp4)

There is also a static landing page in [`public/`](./public) — what Loom is, a
pipeline end to end, and the docs index, with the Woven Knot as its hero. It
publishes to GitHub Pages at <https://zionrubin.github.io/loom/>; run it
locally with `python3 -m http.server 8099 --directory public`.
