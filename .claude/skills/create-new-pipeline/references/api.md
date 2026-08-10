# Loom authoring API reference

The complete surface for writing a pipeline. Defaults are what you get from
the zero value.

- [Records](#records)
- [Building the graph](#building-the-graph)
- [InferSpec](#inferspec)
- [ReduceAISpec](#reduceaispec)
- [Stage options](#stage-options)
- [Prompt templates](#prompt-templates)
- [Models and bindings](#models-and-bindings)
- [Run options](#run-options)
- [RunResult](#runresult)
- [Explain](#explain)
- [Failure classes](#failure-classes)
- [Compile-time errors](#compile-time-errors)

## Records

```go
type Record struct {
    ID   string
    Data map[string]any   // the payload — must be JSON-serializable
    Meta map[string]any   // framework/user metadata; ops should preserve it
}

core.NewRecord(id string, data map[string]any) Record
r.Clone() Record          // fresh top-level maps; mutate without aliasing upstream
r.String(key string) string   // "" when absent; non-strings via fmt %v
```

Values must be JSON-serializable so records can cross an executor boundary and
be content-addressed. IDs matter everywhere and are *load-bearing* in
`Iterate`, where messages address vertices by ID and duplicates are rejected.

## Building the graph

```go
p := pipeline.New("name")

// Sources
p.FromRecords(name string, recs []core.Record) Dataset
p.FromFunc(name string, fn func(ctx) ([]core.Record, error)) Dataset

// Pure Go — no model, fused together, cacheable only with WithVersion
d.Map(name, func(core.Record) (core.Record, error), opts...) Dataset
d.Filter(name, func(core.Record) (bool, error), opts...) Dataset
d.FlatMap(name, func(core.Record) ([]core.Record, error), opts...) Dataset
d.Combine(name, func(a, b core.Record) (core.Record, error), opts...) Dataset

// Go with a capability-checked session (tools + broadcasts)
d.MapTools(name, func(ctx, core.Session, core.Record) (core.Record, error), opts...) Dataset

// Model operations
d.Infer(name, pipeline.InferSpec{...}, opts...) Dataset
d.ReduceAI(name, pipeline.ReduceAISpec{...}, opts...) Dataset
d.Iterate(name, pipeline.IterateSpec{...}, opts...) Dataset   // see iterate.md

d.StageID() string
p.Stages() []*Stage
```

A `Dataset` is a handle to a stage's output. Deriving twice from one dataset
branches the DAG. Branches never rejoin — for cross-branch synthesis, run a
second pipeline over the first's `StageOutputs`.

`Combine` and `ReduceAI` are natural barriers: they need the whole dataset, so
they stay barriers even under `WithStreaming`.

## InferSpec

One model call per record.

```go
type InferSpec struct {
    Binding     model.Binding   // required: which model/tier, plus escalation ladder
    System      string          // system prompt
    Prefix      string          // shared prompt head — rendered ONCE PER TASK
    Prompt      string          // text/template over the record's Data
    MaxTokens   int             // default 1024
    Context     []task.Fragment // named documents delivered in the envelope
    ParseJSON   bool            // parse output as a JSON object, merge into Data
    OutputField string          // raw text lands here when !ParseJSON; default "output"
    Validate    func(core.Record) error   // errors are SEMANTIC → escalate
}
```

`Prefix` has no record data in scope — only the broadcast functions. That is
what makes it shared: every call in the stage sends identical leading bytes, so
the provider's prefix cache serves them. The planner enables prefix caching
whenever a stage issues more than one call; `WithoutPrefixCache()` opts out.

`ParseJSON` failures and `Validate` errors are both semantic failures, so they
walk the escalation ladder rather than retrying identically.

## ReduceAISpec

Hierarchical aggregation: records are grouped `FanIn` at a time, each group
summarized by one model call, levels repeating until one record remains.

```go
type ReduceAISpec struct {
    Binding     model.Binding
    System      string
    Prefix      string   // same shared-prefix mechanism; worth it — every level repeats the rubric
    Prompt      string   // text/template over {Items []string, Count int}
    FanIn       int      // group size per call; default 8
    MaxTokens   int      // default 1024
    ItemField   string   // which field feeds aggregation; default "output"
    OutputField string   // where the aggregate text lands; default "output"
}
```

`ItemField` is the common mistake: it reads *one field* from each input record,
not the whole record. Point it at what you actually want aggregated.

## Stage options

Passed variadically to any stage constructor.

| Option | Effect |
|---|---|
| `WithVersion(v)` | Content version for Go-func stages — **required for caching them** |
| `WithNoCache()` | Never cache this stage (use for side-effecting tool calls) |
| `WithoutPrefixCache()` | Opt out of provider prompt-prefix caching |
| `WithParallelism(n)` | Bound concurrent tasks for this stage (0 = run default) |
| `WithBatchSize(n)` | Group n records per task (default 1) |
| `WithBudget(core.Budget)` | Per-task budget: timeout, attempts, tokens, cost |
| `WithBroadcast(names...)` | Declare run-level shared values this stage may read |
| `WithMCP(server, tools...)` | Declare an MCP server (and optionally exact tools) |
| `WithGrants(caps...)` | Extra capabilities, e.g. `security.ToolCap("name")` |
| `WithSandbox(profile)` | Isolation profile: `task.SandboxInline` (default), `Subprocess`, `Container`, `WASM` |

Fusion merges adjacent pure stages into one task boundary. Merged options take
the last/strictest value — but **versions concatenate only if every fused stage
has one**; a single unversioned member makes the whole fused run uncacheable.

## Prompt templates

Standard `text/template`.

- `Infer.Prompt` — scope is the record's `Data`: `{{.subject}}`, `{{.text}}`
- `Infer.Prefix` — no record data; broadcast functions only
- `ReduceAI.Prompt` — scope is `{Items []string, Count int}`:
  `{{range .Items}}- {{.}}\n{{end}}` and `{{.Count}}`
- `Iterate.Step.Prompt` — the record's data plus `{{.Inbox}}` and `{{.Senders}}`

Inside any of them, when the stage declared the broadcast:

```
{{broadcast "name"}}       the shared value (index it for structured data)
{{broadcastJSON "name"}}   the value as indented JSON
```

Templates are parsed at compile time, so a typo fails before provisioning.

## Models and bindings

```go
type Binding struct {
    Model      string    // explicit model ID
    Tier       Tier      // or a tier — one or the other is required
    Escalation []string  // model IDs tried in order on semantic failures
}

model.TierFast | model.TierBalanced | model.TierDeep
```

Registry:

```go
reg := model.NewRegistry()
reg.Register(model.Info{ID, Provider, Pricing, Limits, Tier, SecretRef}) error
reg.SetTier(tier, id) error      // first model registered for a tier is its default
reg.Get(id) (Info, error)
reg.ForTier(t) (Info, error)
reg.All() []Info

// Offline development
mock, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
    model.WithHandler(func(req model.Request) (string, error) { ... }),
    model.WithLatency(d), model.WithFailures(errs...), model.WithEndpoint(host))
mock.Calls() int
```

`model.Request` carries `Model`, `System`, `Prefix`, `Prompt`, `MaxTokens`,
`CachePrefix`; `req.FullPrompt()` is `Prefix + Prompt`. In a mock handler,
branch on `req.Prompt` to serve several stages from one function.

See `providers.md` for real providers.

## Run options

```go
res, err := loom.Run(ctx, p, opts...)
```

| Option | Effect |
|---|---|
| `WithRegistry(reg)` | The model registry — required for any model stage |
| `WithWorkers(n)` | Default concurrency (default 8) |
| `WithRunBudget(core.Budget{MaxCostUSD, MaxTokens, MaxDuration, MaxAttempts})` | Stops admitting work when exhausted; returns partial results **and** an error |
| `WithStateDir(dir)` | Persistent CAS + result cache — reruns replay instead of re-spending |
| `WithSecrets(map[security.SecretRef]string)` | Loads the broker; tasks resolve only what their envelope grants |
| `WithEgress(hosts...)` | Extra allowed hosts beyond provider endpoints (which are auto-allowed per stage) |
| `WithTools(tools...)` | Register `executor.Tool` implementations |
| `WithMCPServer(servers...)` | Register MCP servers (connected once, at provisioning) |
| `WithMCPResource(name, server, uri)` | Read an MCP resource once and register it as a broadcast |
| `WithBroadcast(name, value)` | Register a run-level shared value (stored once by hash) |
| `WithContinueOnError()` | Dead-letter failing tasks instead of aborting the run |
| `WithRetry(runtime.RetryPolicy)` | Override the retry policy |
| `WithEventHandler(func(observe.Event))` | Synchronous observer of all run events |
| `WithStreaming()` | Pipelined execution instead of stage barriers — **reorders records** |
| `WithBatchWait(d)` | How long a streaming batch waits to fill (default 25ms) |

Events worth handling: `RunStarted`, `RunFinished`, `StageStarted`,
`StageFinished`, `TaskStarted`, `TaskCompleted`, `TaskRetried`, `TaskFailed`,
`ModelCalled`, `CacheHit`, `BudgetExceeded`, `RoundStarted`, `RoundFinished`,
`StageConverged`, `MCPConnected`, `MCPCalled`, `BroadcastRead`.

## RunResult

```go
type RunResult struct {
    RunID        string
    Output       []core.Record              // terminal stage, when there is exactly one
    StageOutputs map[string][]core.Record   // every stage, by ID — what tests should assert on
    Report       observe.RunReport          // .String() prints the per-stage table
    Failures     []runtime.Failure
    Lineage      []store.LineageEntry
    Audit        []security.AuditEntry
    Broadcasts   map[string]string          // name → content hash
    MCP          []mcp.Stats                // dials, calls, wait time per server
    Spent        core.Usage
    Iterations   []IterationReport          // per iterative stage
}
res.Iteration("stage") (IterationReport, bool)
```

`core.Usage` splits prompt tokens three ways — `InputTokens` (full price),
`CacheReadTokens`, `CacheWriteTokens` — plus `OutputTokens`, `Requests`,
`CostUSD`. Helpers: `PromptTokens()`, `TotalTokens()`, `CacheHitRate()`.

## Explain

```go
proj, err := loom.Explain(p, opts...)   // same opts as Run
fmt.Print(proj.String())

proj.Expected() core.Usage   // under an assumed response length (35% of MaxTokens)
proj.Ceiling()  core.Usage   // rests on nothing but MaxTokens — hand this to WithRunBudget
proj.Partial()  bool         // some stages estimated → the ceiling is NOT a bound
proj.FitsBudget() bool
proj.AdmissionFloor() time.Duration   // rate-limit floor on wall time
proj.Warnings []string
proj.Stages []StageProjection
```

Explain-only options, ignored by `Run` so one slice serves both:

| Option | Why |
|---|---|
| `WithStageSample(stage, fields)` | `ParseJSON` output shape is unknowable from the plan — name the fields or everything downstream projects as zero work |
| `WithSourceSample(stage, recs)` | Explain never executes a `FromFunc` source (it may touch the network) |
| `WithExpectedOutput(ratio)` | Fraction of `MaxTokens` responses are assumed to fill (default 0.35); affects `Expected()` only |

Retries and escalation are excluded from the projection — both respond to
failures a projection cannot predict, and both spend above the ceiling.

## Failure classes

The taxonomy drives recovery. Wrap errors from your own ops to steer it.

| Class | Meaning | Recovery |
|---|---|---|
| `core.Transient(err)` | Rate limit, 5xx, timeout | Retry the same work with backoff |
| `core.Semantic(err)` | Call succeeded, output unusable | Escalate along the binding's ladder |
| `core.Permanent(err)` | Retrying cannot help — bad request, missing grant, bug | Fail fast |
| `core.BudgetExceeded(err)` | A budget is exhausted | Stop admitting new work |

`core.ClassOf(err)` classifies: deadline expiry is transient; **anything
unclassified is treated as permanent**, so bugs in your ops fail fast instead
of burning retries.

## Compile-time errors

`plan` rejects these before anything is provisioned. Each names the stage.

- `pipeline %q has no stages` / `stage with empty name` / `duplicate stage name`
- `non-source stage without upstream`
- `empty prompt template` / `prompt template: <parse error>` / `prefix template: ...`
- `broadcast %q is not registered for this run (loom.WithBroadcast)`
- `MCP server %q is not registered for this run (loom.WithMCPServer)`
- `MCP server %q offers no tools this stage may call`
- `iterate without an algorithm` / `iterate needs Halt.MaxRounds > 0`
