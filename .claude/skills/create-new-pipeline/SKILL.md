---
name: create-new-pipeline
description: Build a new Loom pipeline (or add/modify stages in an existing one) in this repository — the AI-native dataflow framework whose operators are model calls. Use this whenever the request involves a Loom pipeline, a new example under examples/, an Infer/ReduceAI/Map/Filter/Iterate stage, a model Binding or escalation ladder, prompt templates, broadcasts, MCP stages, budgets and loom.Explain projections, the result cache, or a Fleet. Trigger on direct asks ("write a pipeline that summarizes X", "add a classification stage", "make this run on Claude instead of mocks") and on indirect ones ("process these documents with a model", "fan this out over the API and roll it up", "how much would this cost before I run it") — anything shaped like records flowing through model calls belongs here rather than a hand-rolled loop over an SDK.
---

# Creating a Loom pipeline

Loom is MapReduce where the operators are model calls. You declare a graph of
stages; the framework plans it, fuses the cheap parts, provisions each task
with exactly the model/secret/tool/network/budget it needs, retries by failure
class, escalates on bad output, caches by content hash, and prices the whole
thing before it runs.

The point of this skill is that you should not need to read the framework to
add to it. Everything load-bearing is below; the reference files are for when
you need a specific answer, not for orientation.

## Start from the template, not from scratch

```sh
.claude/skills/create-new-pipeline/scripts/new-pipeline.sh <kebab-name>
go test ./examples/<kebab-name>      # green before you change anything
```

This writes `examples/<name>/{main.go,main_test.go,README.md}`: a complete
offline pipeline (mock models, source → Infer → Filter → ReduceAI), an
`-explain` cost gate, cache-aware reruns, and three tests that already pass.
Then edit `build()` — the stages are the design. Starting from a green
baseline means any breakage you see is yours, which is worth far more than the
few lines the template costs you.

If the user wants a pipeline somewhere other than `examples/`, pass a second
argument for the destination directory.

## The five things that matter

**1. Records are the currency.** A `core.Record` is `{ID, Data map[string]any,
Meta}`, JSON-serializable so it can cross an executor boundary and be
content-addressed. `r.String("field")` reads a field as text. Stages transform
records; nothing else flows.

**2. Pick the stage by what the work actually is.**

| Work | Stage | Notes |
|---|---|---|
| Load the input | `FromRecords` / `FromFunc` | `FromFunc` runs at run time; Explain won't execute it |
| Pure Go transform | `Map` / `Filter` / `FlatMap` | free, fused together, no model |
| Go transform needing tools or broadcasts | `MapTools` | gets a capability-checked `core.Session` |
| One model call per record | `Infer` | the workhorse |
| Many records → one, via the model | `ReduceAI` | hierarchical tree, `FanIn` per level |
| Many records → one, in Go | `Combine` | associative fold, no model, no cost |
| Loop until it converges | `Iterate` | see `references/iterate.md` |

Branching a `Dataset` twice builds a DAG. DAGs fan **out** but do not fan back
**in** — two branches cannot rejoin. Cross-branch synthesis is a second `Run`
over the first one's outputs (that is what `examples/vertical-digest` does).

**3. Bind to a tier, with an escalation ladder.**

```go
Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"claude-sonnet-5"}},
```

`TierFast`/`TierBalanced`/`TierDeep` let the registry decide what "fast" means,
so swapping providers is a registry change rather than a pipeline change. The
ladder is tried only on **semantic** failures — output that failed `ParseJSON`
or `Validate` — because retrying a bad answer on the same model usually
reproduces it. Transient failures (429, 5xx, timeout) retry in place instead.

On a stage where a good fraction of records escalate, add
`loom.WithRouting(route.Config{Features: route.ByField("<field>")})` to the run.
Each record then starts on the rung expected to answer it instead of always at
the bottom, so the ladder stops charging for the cheap call that was going to
fail. It learns from the `Validate` verdicts the stage already produces —
nothing to train — and it picks a *starting* rung only, so a wrong guess costs
a call and never an answer. Name a field that predicts difficulty; the default
featurizer can only guess from size. See `docs/ROUTING.md`.

**4. Put what never varies in `Prefix`, not `Prompt`.** `Prefix` is rendered
once per task and sent as the same leading bytes on every call, so the
provider's prompt cache serves it. `Prompt` is rendered per record. Rubrics,
taxonomies, and few-shot examples belong in `Prefix`; keep `Prompt` down to
what actually changes. Both are `text/template` over the record's `Data`
(`{{.subject}}`); `ReduceAISpec.Prompt` instead sees `{{.Items}}` and
`{{.Count}}`.

**5. Go-func stages need `WithVersion` to be cacheable.** Closures are not
content-addressable, so the version string stands in for the function body:

```go
Filter("urgent-only", fn, pipeline.WithVersion("v1"))
```

Bump it when the logic changes. Without it the stage silently recomputes every
run — and because fused stages share one fingerprint, **one unversioned stage
makes the whole fused run uncacheable**.

## Verify with a projection before you verify with a run

`loom.Explain` compiles the pipeline exactly as `Run` does — same validation,
fusion, fingerprints, envelopes — then executes only the cheap Go stages and
*models* the paid calls. No calls issued, no secrets resolved, no sockets
opened. It catches template typos, unregistered broadcasts, and unbound models
before anything is provisioned, and its ceiling is the number to hand
`WithRunBudget`.

One trap is worth knowing before it bites you: **`ParseJSON` hides the record
shape from the projection.** The field names come out of the model, so a
downstream `Filter` testing one sees it missing, drops every record, and
everything past it projects as zero work — an under-count, the one direction a
ceiling must not be wrong in. Explain marks those stages `~` and warns. Fix it
by naming the fields once:

```go
loom.WithStageSample("classify", map[string]any{"severity": "high", "topic": "billing"})
```

Choose the values that make the *most* downstream work, so the ceiling stays a
bound. Explain-only options are ignored by `Run`, so one option slice both
describes the run and prices it. Same idea for `FromFunc` sources, which
Explain refuses to execute: `loom.WithSourceSample("load", recs)`.

## Develop offline, then swap the registry

Mocks are deterministic, free, and model the provider prompt cache, so the
cache accounting you see offline is what you get in production. Keep model
setup in its own `registry()` function; going live is then a one-function
change:

```go
reg := model.NewRegistry()
anthropic.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 50})
// ...
loom.WithSecrets(map[security.SecretRef]string{anthropic.DefaultSecretRef: key})
```

Details and the other providers (OpenAI, llama.cpp for local inference) are in
`references/providers.md`.

## Test what would actually break

The template ships all three; keep them as you edit.

- **Shape** — assert per stage via `res.StageOutputs["stage-id"]`, not just
  `res.Output`. A stage that silently went empty is the real failure mode, and
  a filter that drops everything looks identical to a filter that works until
  you look at the stage between them.
- **Cache replay** — run twice against one `t.TempDir()`; the second run should
  make zero model calls (`mock.Calls() == 0`). This catches a
  non-deterministic prompt or a stage missing `WithVersion`.
- **Projection** — `proj.Partial()` false and `proj.FitsBudget()` true, so the
  ceiling stays meaningful as the pipeline grows.

Run `go test ./examples/<name>` and `go vet ./examples/<name>`. Everything is
offline, so tests need no key and cost nothing. This repo has no CI — the
tests you write are the only check there is.

## Things that will bite you

- **Stage IDs are identifiers.** They must be unique and non-empty; they are
  the cache key, the event stream's label, and how you fetch `StageOutputs`.
- **`WithStreaming` reorders records.** It pipelines stages instead of using a
  barrier, so outputs arrive in completion order, not input order. Use the
  default when output order is part of the contract.
- **A cached stage does not repeat side effects.** Tool and MCP calls are
  side effects. A lookup is fine to replay; a write is not — give those
  `pipeline.WithNoCache()`.
- **Budget aborts are not total failures.** `Run` returns partial results
  *and* an error when the governor stops it. `res` is still populated; report
  `res.Spent.CostUSD`.
- **Broadcast and MCP typos fail at compile time, not at the first record** —
  that is deliberate. If `plan` rejects your pipeline, read the stage name in
  the error; it is telling you exactly which declaration is unbacked.
- **`ReduceAI` reads `ItemField`** (default `"output"`) from each record, not
  the whole record. Point it at the field you actually want aggregated.
- **`Validate` failures are semantic**, so they escalate. Use
  `core.Permanent(err)` from a `Map` when retrying genuinely cannot help. A
  good `Validate` is worth writing twice over: it is the semantic gate *and*
  the only training signal `loom.WithRouting` has.
- **A run's `Spent` excludes failed calls.** The governor is charged from a
  task's result, and a task whose call failed validation returns none. Report
  `res.Report.Totals().CostUSD` when you want what the run actually spent, and
  `res.Spent` when you want what the governor counted against the budget.

## Reference files

Read the one you need; they are written to be opened alone.

| File | Read it when |
|---|---|
| `references/api.md` | You need an exact signature, field, or default — the complete authoring surface |
| `references/providers.md` | Going from mocks to Anthropic, OpenAI, or local llama.cpp; tiers, limits, secrets, cost |
| `references/context.md` | The stage needs more than the record: broadcasts, tools, MCP servers, secrets, egress, sandboxing |
| `references/iterate.md` | The work loops — research that finds its own next document, refine-until-good, graph algorithms, beam search |
| `references/scale.md` | Several pipelines at once (Fleet, blackboard), streaming, batching, parallelism, the constellation view |

Repo docs go deeper on the reasoning behind all of this:
`docs/ARCHITECTURE.md`, `docs/INFERENCE.md`, `docs/ITERATION.md`,
`docs/ALGORITHMS.md`, `docs/ASYNC.md`, `docs/MCP.md`, `docs/STUDIO.md`.
Working examples worth copying from: `examples/triage` (smallest complete
pipeline), `examples/support-desk` (broadcasts), `examples/mcp-desk` (MCP),
`examples/multi-hop` (iteration), `examples/newsroom` (fleet).
