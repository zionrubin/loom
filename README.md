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

Classic data frameworks assume operators are cheap, deterministic, and
trusted, and that throughput is bounded by cores. AI workloads violate all
of that: calls cost real money, outputs can be *wrong* while the call
succeeds, throughput is bounded by provider rate limits, and operators
execute model-derived behavior that shouldn't be ambiently trusted. Loom
makes each difference a first-class concept — see
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full design.

The closest prior art isn't another data framework — it's the inference
serving engines (vLLM, SGLang), which solve the same problem one level down:
many expensive, variable-latency model calls that share structure, under a
hard resource ceiling. Loom borrows their two central mechanisms — prefix
cache sharing and continuous batching — and
[docs/INFERENCE.md](docs/INFERENCE.md) maps the rest of that playbook onto
this one, including what is deliberately left out.

## What you get

- **Declarative pipelines** — `Map` / `Filter` / `FlatMap` / `Combine` plus
  AI-native `Infer` (templated per-record inference, JSON parsing,
  validation) and `ReduceAI` (parallel tree aggregation). Branching builds
  DAGs; the planner fuses adjacent pure stages.
- **Least-privilege task envelopes** — every task carries an explicit,
  serializable declaration of its model binding, capability grants, secret
  references, egress allowlist, context bundle, budget, and sandbox
  profile. The planner assembles the *minimal* envelope automatically;
  executors enforce it at the moment of use, with an append-only audit log.
- **An AI-aware scheduler** — per-model token-bucket admission control
  (requests/min and tokens/min), a run-level dollar/token budget governor
  with graceful partial results, and class-aware recovery: transient
  failures back off, semantic failures (bad output) climb the model
  escalation ladder, permanent failures dead-letter, budget exhaustion
  stops the run.
- **Broadcast values** — register a lookup table, taxonomy, or rubric once
  per run with `loom.WithBroadcast`; stages opt in with
  `pipeline.WithBroadcast`. The value is stored once by content hash and
  *referenced* by every task that reads it, so sharing costs one copy per
  run rather than one per task and the tasks stay small enough to ship to a
  remote executor. Reads are grant-checked and audited like any other
  capability, and the value's hash joins the reading stage's fingerprint —
  edit a broadcast and exactly the results that saw it recompute.
- **Shared prompt prefixes** — the stage-stable head of a prompt (a rubric, a
  taxonomy, few-shot examples) goes in `InferSpec.Prefix`, a template with no
  record data in scope. It renders once per task instead of once per record,
  and providers receive it as a cacheable prefix — an explicit `cache_control`
  breakpoint on Anthropic, stable leading bytes for OpenAI's automatic prefix
  cache. Broadcasts share the *bytes* across tasks; this shares the *work the
  model does on them*. The planner turns it on only when a stage issues more
  than one call, which is exactly when a cache write earns itself back, and
  the run report states what it cost and what it saved.
- **Pipelined execution** — `loom.WithStreaming()` replaces the stage barrier
  with continuous batching: a record moves downstream when its own task
  finishes, not when its whole stage does, and every stage draws from one
  global pool of execution slots. Stages overlap, a straggler no longer idles
  the workers behind it, and aggregates (`Combine`, `ReduceAI`) remain the
  natural barriers they have to be. The trade is ordering — records flow in
  completion order — so the barrier driver stays the default.
- **Content-addressed caching = checkpointing** — task results are keyed by
  op fingerprint + input content. Reruns and crash recovery replay
  completed AI work with zero model calls and zero cost, across process
  restarts with a state dir.
- **Lineage & audit** — every artifact traces to the op, model, and inputs
  that produced it; every secret/tool/egress/broadcast decision is audited.
- **Observability** — a typed event bus and per-stage run reports: tasks,
  failures, retries, cache hits, tokens, dollars, latency percentiles. Plus
  the **constellation view** (`viz`): a live, zero-dependency web UI that
  renders every task and executor as a star — what's running, what's slow,
  what failed, at a glance. Per-node drill-down shows the full input and
  output records, every model call's rendered request and response, runtime,
  tokens, cost, retries, and logs; selecting a star draws its lineage (which
  tasks merged into it and where its output went), and clicking a stage name
  opens an inspector with the stage's spec, prompt template, and live stats.
  When a run completes, a **run summary** overlay (also on `s`) recaps every
  step — tasks, records, retries, cache hits, tokens, cost, p95 per stage —
  what each shared value saved by being referenced instead of copied, and
  per-executor utilization. The header names the driver that ran, stage and
  task inspectors carry the shared prefix's cache economics, and under
  streaming the executors are the engine's slots, so overlapping stages and
  shared occupancy are visible as they happen. Heavy per-node payloads
  (rendered prompts, responses, record JSON) load only for the node you open,
  which is what keeps the view responsive on runs with thousands of tasks.
- **Providers** — a deterministic mock (offline dev, scripted failures) plus
  Anthropic and OpenAI adapters over the official SDKs with per-call
  broker-resolved credentials. The `model.Provider` interface is small; add
  your own.

## Quickstart

```sh
cd loom

go test ./...            # full suite, no network or keys needed
go run ./examples/triage # complete pipeline on a mock model, offline

# live constellation view: watch a run as a sky of task/executor stars —
# pulsing while running, ringed when slow, flashing on completion, red on
# failure; click any star for model, input, tokens, cost, retries, and logs
go run ./examples/constellation   # then open http://localhost:8077

# the constellation view at scale, still offline: a ~50-paper literature
# survey (≈205 tasks, three mock model tiers, a branching DAG with two
# reduce trees) scripted to show every visual state in one run — retries,
# a straggler, escalations, a dead letter; see examples/research/README.md
# for flags (budget squeeze, cache replay) and a recording storyboard
go run ./examples/research        # then open http://localhost:8077

# watch cache-resume: second run makes zero model calls
LOOM_STATE=/tmp/loom go run ./examples/triage
LOOM_STATE=/tmp/loom go run ./examples/triage

# real models (Claude): classification + executive summary, budget-capped
ANTHROPIC_API_KEY=sk-... go run ./examples/anthropic-review

# same pipeline on OpenAI (GPT-5.4 family) + live constellation view
OPENAI_API_KEY=sk-... go run ./examples/openai-review
# then open http://localhost:8077

# the broadcast + multi-task-executor showcase on OpenAI: shared catalog/
# policy/voice-rubric knowledge read by reference across every task, an
# end-of-run summary of what that saved, and selective cache invalidation
# when the shared policy is edited (see examples/support-desk/README.md)
LOOM_STATE=/tmp/loom-desk OPENAI_API_KEY=sk-... go run ./examples/support-desk
LOOM_STATE=/tmp/loom-desk OPENAI_API_KEY=sk-... go run ./examples/support-desk  # all cached, $0
LOOM_STATE=/tmp/loom-desk OPENAI_API_KEY=sk-... go run ./examples/support-desk -policy v2  # only policy readers recompute
```

## Package map

| Package | Role |
|---|---|
| `core` | Records, usage/cost accounting, budgets, failure taxonomy |
| `pipeline` | Authoring API: datasets, stages, options |
| `plan` | Validation, fusion, fingerprints, least-privilege envelopes |
| `runtime` | Scheduler, retries, rate-limit admission, budget governor |
| `executor` | Executor seam, capability-scoped runtime, model client, tools |
| `ops` | Operation runners (infer, reduce, fused transforms) |
| `model` | Provider abstraction, registry, tiers, escalation bindings, mock |
| `providers/anthropic` | Official-SDK Anthropic adapter, broker-resolved keys |
| `providers/openai` | Official-SDK OpenAI adapter, broker-resolved keys |
| `security` | Grants, secret broker, egress policy, audit log |
| `store` | Content-addressed store, persistent cache, lineage |
| `observe` | Event bus, metrics collector, run reports |
| `viz` | Constellation view: live web visualization of a run (tasks and executors as stars) |
| `task` | Task + envelope types (serializable — the distribution seam) |

## Sharing data across tasks

Tasks are isolated by design — each one gets its own records and its own
envelope, which is what lets them run anywhere. When many tasks need the *same*
side data, broadcast it: registered once per run, stored once by content hash,
and referenced (never copied) by the tasks that ask for it.

```go
regions := map[string]string{"t1": "EMEA", "t2": "APAC"} // any JSON-serializable value

src.
    // Go ops read broadcasts through the task's capability-checked session.
    MapTools("region", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
        table, err := core.BroadcastAs[map[string]string](ctx, s, "regions")
        if err != nil {
            return core.Record{}, err
        }
        r.Data["region"] = table[r.ID]
        return r, nil
    }, pipeline.WithBroadcast("regions")).                       // ← stage opts in

    // Prompts read them with the broadcast/broadcastJSON template functions.
    Infer("classify", pipeline.InferSpec{
        Binding: model.Binding{Tier: model.TierFast},
        Prompt: `Rubric: {{broadcast "rubric"}}
Ticket in {{.region}}: {{.subject}}`,
    }, pipeline.WithBroadcast("rubric"))

loom.Run(ctx, p,
    loom.WithBroadcast("regions", regions),                      // ← registered once
    loom.WithBroadcast("rubric", rubricText),
)
```

Three properties come from routing it through the envelope rather than a
package-level variable:

- **It scales past one process.** Envelopes carry a 64-byte hash, not the
  bytes, so tasks stay shippable to a remote or sandboxed executor and each
  worker fetches the value once instead of once per task.
- **It stays least-privilege.** Registering a value for the run doesn't expose
  it; a stage reads only what it declared, and reads are audited like any
  other capability.
- **It keeps the cache honest.** The value's content hash is part of the
  reading stage's fingerprint, so editing a broadcast recomputes exactly the
  stages that could have seen it and leaves the rest of the cache warm.

Broadcasts are read-only for the run's lifetime. For state that accumulates
*across* records, use `Combine` or `ReduceAI` at a stage boundary — shared
mutable state would make cached results depend on execution order, which is
precisely what content-addressed caching assumes away.

[`examples/support-desk`](./examples/support-desk) turns these properties
into numbers on real OpenAI models: how many bytes the run avoided copying,
which stages recompute when a shared value is edited, and a live view of
every broadcast read.

### Sharing the work, not just the bytes

A broadcast shares a value across tasks. It does not stop the *model* from
reprocessing that value on every call: a rubric sent to a thousand tasks is
read by the provider a thousand times. Put it in the stage's `Prefix` and
that stops being true.

```go
Infer("classify", pipeline.InferSpec{
    Binding: model.Binding{Tier: model.TierFast},
    System:  "You classify support tickets.",
    Prefix:  `Rubric:\n{{broadcast "rubric"}}`,    // once per task, cached provider-side
    Prompt:  "Classify this ticket: {{.subject}}", // once per record
}, pipeline.WithBroadcast("rubric"))
```

`Prefix` is a template with no record data in scope, which is the whole
mechanism: a template that cannot see the record cannot vary by record, so
every call in the stage opens with identical bytes and the provider's prompt
cache serves them. The prefix joins the stage fingerprint, so editing the
rubric recomputes exactly the stages that could have seen it — and a stage
without a prefix fingerprints exactly as it did before, leaving existing
caches warm.

## Design notes

- AI operators are pure data and tasks are JSON-serializable (tested), so
  remote/sandboxed executors plug in behind the `Executor` interface without
  touching planning or scheduling.
- Go-function stages run in-process; give them `pipeline.WithVersion("v1")`
  to make them cacheable (bump the version when behavior changes).
- Unclassified errors are treated as permanent so user-code bugs fail fast
  instead of burning paid retries; providers classify their own errors.

- Both drivers execute tasks through the same `Scheduler.RunTask`, so retry,
  escalation, admission control, and the budget governor cannot drift between
  barrier and streaming execution.

The scaling path (remote worker fleets, shared object-store CAS, WASM
sandboxes with grant-derived imports) is laid out in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#6-scaling-path-from-local-runtime-to-distributed-system),
and the inference-engine lineage — what Loom borrows from vLLM/SGLang and
what it deliberately leaves out — in [docs/INFERENCE.md](docs/INFERENCE.md).

## Demo

[▶️ Watch the demo](./assets/demo.mp4)
