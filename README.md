# Loom

**Large-scale Orchestration Of Models** — a Go framework for data pipelines
powered by AI models.

Define how records are classified, filtered, and summarized. Loom handles
concurrency, provider rate limits, retries, model escalation, and caching,
with explicit budgets and permissions for each task.

Use it for workflows such as triaging support tickets, summarizing document
collections, or running several agents that share model capacity and results.

[Website](https://zionrubin.github.io/loom/) · [Watch the demo](assets/demo.mp4) · [Documentation](docs/README.md)

## Quickstart

You'll need **Go 1.24 or later** and Git.

```sh
git clone https://github.com/zionrubin/loom.git
cd loom
go run ./examples/triage
```

This example uses a deterministic mock model: **no API key or paid model calls**.
Go may download dependencies on the first run; the example itself runs offline.

It classifies five support tickets, keeps the three urgent ones, and produces a
briefing. The terminal shows model-call events, the briefing, and a per-stage
report of tasks, tokens, cost, and cache hits.

To see completed work reused, run it twice with the same state directory:

```sh
LOOM_STATE=./state go run ./examples/triage
LOOM_STATE=./state go run ./examples/triage
```

The second run replays cached results with zero model calls. Read the
[complete example](examples/triage/main.go) to see the pipeline and model setup.

## How it works

A pipeline starts with **records** and connects **stages** such as `Infer`,
`Filter`, and `ReduceAI`. Loom turns those stages into tasks, schedules their
execution, and returns the output with a run report.

- **Control spending:** estimate calls and cost before a run, then apply token
  and dollar budgets during execution.
- **Handle model failures:** retry transient errors and escalate invalid output
  to stronger models using your validation rules.
- **Reuse completed work:** persist task results so unchanged work can be
  replayed across runs and process restarts.
- **Declare access:** task envelopes specify permitted models, tools, secrets,
  network hosts, and sandbox profiles; deployment policies constrain them.
- **Scale when needed:** run concurrent pipelines in a fleet, process streams,
  or distribute tasks across worker processes.

Loom is a Go library. The default executor runs locally; an envelope is an
access declaration, not an OS sandbox. See the
[architecture and isolation model](docs/ARCHITECTURE.md) for execution boundaries.

## Where to go next

| I want to… | Start here |
|---|---|
| Try another workflow or a real model | [Runnable examples](docs/EXAMPLES.md) |
| Watch tasks execute in a browser | [Live visualization](docs/VIZ.md) |
| Build a pipeline visually | [Loom Studio](docs/STUDIO.md) |
| Find a specific capability | [Documentation guide](docs/README.md) |
| Understand or contribute to the internals | [Architecture](docs/ARCHITECTURE.md) · [Development reference](docs/DEVELOPMENT.md) |
