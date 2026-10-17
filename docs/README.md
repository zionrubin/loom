# Loom documentation

[Project overview](../README.md) · [Quickstart](../README.md#quickstart)

New to Loom? Run the quickstart, read the
[triage source](../examples/triage/main.go), then choose a workflow from the
[examples catalog](EXAMPLES.md). The guides below cover capabilities you can
add as you need them.

## Build a pipeline

| Guide | What you'll learn |
|---|---|
| [Examples](EXAMPLES.md) | Run complete workflows, from offline mocks to real providers |
| [Cost estimation](EXPLAIN.md) | Project calls, tokens, and cost before running a pipeline |
| [Model routing](ROUTING.md) | Choose a starting model using previous validation results |
| [Shared context](SHARING.md) | Reuse broadcasts and stable prompt prefixes |
| [Iteration algorithms](ALGORITHMS.md) | Use message passing, refinement, and beam search |
| [Tools with MCP](MCP.md) | Connect tools with explicit task permissions |
| [Local inference](INFERENCE.md) | Run llama.cpp-compatible models and understand inference-engine parallels |
| [Loom Studio](STUDIO.md) | Edit a pipeline visually, estimate its cost, and export Go code |

## Run agents and continuous workloads

| Guide | What you'll learn |
|---|---|
| [Fleets](ASYNC.md) | Run concurrent pipelines with shared capacity, budgets, and a blackboard |
| [Streaming](STREAMING.md) | Process unbounded inputs with windows, watermarks, and checkpoints |
| [Conversation serving](RECALL.md) | Maintain context as conversations arrive and answer questions against it |
| [Shared findings](FINDINGS.md) | Reuse external research across agents and processes |
| [Delta execution](DELTA.md) | Carry growing context by reference and reuse earlier revisions |

## Deploy and observe

| Guide | What you'll learn |
|---|---|
| [Deployment policy](POLICY.md) | Constrain models, tools, secrets, egress, isolation, and data classes |
| [Shared quotas](QUOTA.md) | Coordinate rate limits and spending across processes |
| [Worker processes](WORKERS.md) | Distribute tasks through a durable queue |
| [Live visualization](VIZ.md) | Inspect tasks, prompts, results, lineage, and run summaries |
| [Telemetry](TELEMETRY.md) | Export Prometheus metrics and OpenTelemetry traces |

## Understand and contribute

- [Architecture](ARCHITECTURE.md): components, execution boundaries, caching,
  recovery, design decisions, and the scaling roadmap.
- [Why iteration](ITERATION.md): the rationale behind iterative AI processing.
- [Development reference](DEVELOPMENT.md): tests, package map, working notes,
  and the local website preview.
