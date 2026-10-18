# Development reference

[Documentation](README.md) · [Project overview](../README.md)

For contributors working on Loom itself. To try a pipeline first, follow the
[root quickstart](../README.md#quickstart).

## Run the tests

From the repository root, with Go 1.24 or later:

```sh
go test ./...
```

The suite uses mocks and local test services; no provider API keys are required.
Go may need network access to download dependencies on the first run.

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
| `telemetry` | The seam onto a deployment's monitoring: Prometheus exposition, OTLP spans with derived identifiers, outcome-aware sampling, and the `/metrics` `/healthz` `/readyz` surface |
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
[docs/ARCHITECTURE.md §8](ARCHITECTURE.md#8-design-notes), and the scaling
path in [§6](ARCHITECTURE.md#6-scaling-path-from-local-runtime-to-distributed-system).

## Preview the landing page

The static site lives in [`public/`](../public) and publishes to
[GitHub Pages](https://zionrubin.github.io/loom/). From the repository root:

```sh
python3 -m http.server 8099 --directory public
```

Open <http://localhost:8099>.
