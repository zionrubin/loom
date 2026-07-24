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
- **Content-addressed caching = checkpointing** — task results are keyed by
  op fingerprint + input content. Reruns and crash recovery replay
  completed AI work with zero model calls and zero cost, across process
  restarts with a state dir.
- **Lineage & audit** — every artifact traces to the op, model, and inputs
  that produced it; every secret/tool/egress decision is audited.
- **Observability** — a typed event bus and per-stage run reports: tasks,
  failures, retries, cache hits, tokens, dollars, latency percentiles. Plus
  the **constellation view** (`viz`): a live, zero-dependency web UI that
  renders every task and executor as a star — what's running, what's slow,
  what failed, at a glance. Per-node drill-down shows the full input and
  output records, every model call's rendered request and response, runtime,
  tokens, cost, retries, and logs; selecting a star draws its lineage (which
  tasks merged into it and where its output went), and clicking a stage name
  opens an inspector with the stage's spec, prompt template, and live stats.
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

## Design notes

- AI operators are pure data and tasks are JSON-serializable (tested), so
  remote/sandboxed executors plug in behind the `Executor` interface without
  touching planning or scheduling.
- Go-function stages run in-process; give them `pipeline.WithVersion("v1")`
  to make them cacheable (bump the version when behavior changes).
- Unclassified errors are treated as permanent so user-code bugs fail fast
  instead of burning paid retries; providers classify their own errors.

The scaling path (remote worker fleets, shared object-store CAS, WASM
sandboxes with grant-derived imports, streaming execution) is laid out in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#6-scaling-path-from-local-runtime-to-distributed-system).

## Demo

[▶️ Watch the demo](./assets/demo.mp4)
