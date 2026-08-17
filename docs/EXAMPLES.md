# The examples

Most of these run **offline** — a deterministic mock provider stands in for a
model, so they need no key, no network, and cost nothing. The handful that talk
to a real provider say so.

| Example | What it shows | Needs |
|---|---|---|
| [`triage`](../examples/triage) | A complete pipeline end to end; cache resume | — |
| [`constellation`](../examples/constellation) | The live view, and the projection→run loop | — |
| [`research`](../examples/research) | The view at scale: ~205 tasks, every visual state | — |
| [`multi-hop`](../examples/multi-hop) | Iteration: a research question answered by walking a graph | — |
| [`newsroom`](../examples/newsroom) | A fleet: six agents, one engine, a blackboard | — |
| [`game-forge`](../examples/game-forge) | Three runs, one universe — a playable game at the end | — |
| [`agent-forge`](../examples/agent-forge) | Reads a chat corpus and designs the agents to build | — |
| [`mcp-desk`](../examples/mcp-desk) | MCP tools against a real child-process server | — |
| [`on-device`](../examples/on-device) | Local inference on llama.cpp-compatible servers | — |
| [`worker-fleet`](../examples/worker-fleet) | One pipeline across worker processes, one SIGKILLed | — |
| [`delta-session`](../examples/delta-session) | A growing context across processes, its holder killed | — |
| [`watchtower`](../examples/watchtower) | Stream mode: windows, watermarks, a priced interruption | — |
| [`commons`](../examples/commons) | The findings gate: four desks, one question each | — |
| [`commons-shared`](../examples/commons-shared) | The same commons across four executor *processes* | — |
| [`studio`](../examples/studio) | Loom Studio on an invented archive | — (`-openai` optional) |
| [`anthropic-review`](../examples/anthropic-review) | Classification + summary, budget-capped | `ANTHROPIC_API_KEY` |
| [`openai-review`](../examples/openai-review) | The same on the GPT-5.4 family, with the live view | `OPENAI_API_KEY` |
| [`support-desk`](../examples/support-desk) | Broadcasts on real models, and selective invalidation | `OPENAI_API_KEY` |
| [`vertical-digest`](../examples/vertical-digest) | Two runs over a chat archive, fan-out then synthesis | `OPENAI_API_KEY` |
| [`partner-atlas`](../examples/partner-atlas) | Three runs: roster, per-partner history, portfolio | `OPENAI_API_KEY` |

Several carry their own README with flags and a fuller walkthrough.

## Offline

```sh
go test ./...            # full suite, no network or keys needed
go run ./examples/triage # complete pipeline on a mock model, offline

# watch cache-resume: second run makes zero model calls
LOOM_STATE=/tmp/loom go run ./examples/triage
LOOM_STATE=/tmp/loom go run ./examples/triage
```

### Iteration

```sh
# a research question answered by walking a citation graph the model chooses as
# it goes. The frontier grows while discovering and shrinks while converging
# (2 → 4 → 3 → 2 → 1); one cited paper is outside the corpus and the walk
# creates it; the conclusion is three hops from anything the question named.
# Prints the projection first, then what the run actually spent
go run ./examples/multi-hop

# the same, twice, to see a converged loop replay for nothing
go run ./examples/multi-hop -state /tmp/loom-hop
go run ./examples/multi-hop -state /tmp/loom-hop   # 0 tokens, $0.0000

# watch it converge: the constellation view draws an iterative stage as
# concentric orbits, one ring per superstep, the live ring turning — the outer
# rings thinning as vertices go quiet. Click the stage for the per-round table
# and the halt reason; rerun with -rounds 2 to see the same panel say the loop
# was cut off rather than finished
go run ./examples/multi-hop -view localhost:8077 -slow 900ms
```

### The constellation view

```sh
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
```

### Fleets and multi-run

```sh
# a fleet: six agents at once on one engine, sharing slots, quota, ceiling and
# cache, coordinating through a blackboard. Three short agents launched after a
# 60-task sweep still finish in half its time; a seventh run of the sweep's own
# input costs zero calls. Ends with the fleet report
go run ./examples/newsroom        # then open http://localhost:8077, press `u`

# a playable web game, planned, written, and shipped by three pipelines —
# still offline, still free: one task per module, a shared engine contract as
# the cached prompt prefix, a module cut for needing network the contract
# doesn't grant, and one self-contained HTML file at the end. The three runs
# land in one universe; the finished game shows its own build provenance
go run ./examples/game-forge      # forge on :8077, the game it built on :8078

# read a folder of exported chat and answer the question that comes before
# building anything: how many agents, split along which axis, and what does
# each one have to remember
go run ./examples/agent-forge     # :8077 the three runs, :8078 the blueprint
```

### Distribution

```sh
# one pipeline across several worker *processes*, with one of them killed by
# SIGKILL while it holds a paid model call. Prints what the kill cost (the
# calls the dead worker had started, re-executed — and nothing else) and
# checks every record's answer against a single-process run of the same
# pipeline
go run ./examples/worker-fleet
go run ./examples/worker-fleet -workers 5 -docs 40
go run ./examples/worker-fleet -kill=false   # the undisturbed fleet: same cost as local

# one long agent session across worker processes, appending a turn per round,
# with the worker holding that session's state killed halfway through. Prints
# what crossed the queue (a 212-byte reference against a 617 kB transcript),
# how much of each prompt was certified unchanged since the previous round,
# and the one rebuild the kill cost — then checks every answer against a
# single process doing the whole session by itself
go run ./examples/delta-session
go run ./examples/delta-session -turns 400 -rounds 12   # a bigger context to carry
go run ./examples/delta-session -kill=false             # the undisturbed session

# four executor processes over six overlapping subjects, counting the calls in
# the source's own log: 24 asked, 6 researched
go run ./examples/commons-shared
```

### Streaming

```sh
# an input that never ends: an incident feed graded per event and digested per
# minute. Panes fire on event time rather than wall clock, so the same feed
# always produces the same windows; the report prints panes next to model calls,
# because a pane is the unit that costs money. -crash stops the job a third of
# the way through and starts it again: it resumes at the checkpointed offsets
# with its half-filled windows intact, and the replay costs zero model calls
go run ./examples/watchtower
go run ./examples/watchtower -live       # tail the feed as a writer appends
go run ./examples/watchtower -crash      # price the interruption
go run ./examples/watchtower -window 30s # twice the panes, the same gradings
```

### Tools, local models, and the canvas

```sh
# MCP: an inventory desk whose tools live behind a Model Context Protocol
# server — a real child process over real pipes, still offline. One connection
# serves every record; the model picks a follow-up tool and the next stage runs
# it; a document on the server becomes a broadcast. Run it twice and the second
# run makes zero tool calls, because a tool call is work and Loom does not pay
# for work twice
go run ./examples/mcp-desk
go run ./examples/mcp-desk -state /tmp/loom-mcp
go run ./examples/mcp-desk -state /tmp/loom-mcp   # 0 tool calls, 0 tokens

# four analyst desks over six companies, each asking in its own phrasing, run
# both ways: with the findings gate and without it
go run ./examples/commons

# local inference: an on-call desk whose models run on this machine — two
# llama.cpp servers, no key, $0, and customer records that provably cannot
# leave the box. Still offline: with no -fast/-deep address it starts its own
# llama.cpp-compatible servers on real loopback sockets, so nothing is
# installed and no weights are downloaded. Ends by printing what running
# locally changed, including what the same pipeline would have cost billed by
# the token
go run ./examples/on-device

# against real servers (llama-server -m small.gguf --port 8080 --parallel 2,
# and a larger one on 8081 as the escalation rung)
go run ./examples/on-device -fast http://127.0.0.1:8080 -deep http://127.0.0.1:8081

# watch admission control do the thing local inference makes necessary: eight
# workers against two decode slots, admitted two at a time
go run ./examples/on-device -view localhost:8077 -slow 400ms

# Loom Studio: the pipeline as a canvas that prices itself — invented archive,
# mock models, real arithmetic
go run ./examples/studio
```

## Against real providers

```sh
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

# two runs over a real chat archive: per-vertical digests, then the synthesis
# that reads them (a second run, because DAGs fan out but do not fan back in)
OPENAI_API_KEY=sk-... go run ./examples/vertical-digest -messages /path/to/messages

# three runs over the same archive: partner roster, per-partner history, and a
# per-vertical portfolio
OPENAI_API_KEY=sk-... go run ./examples/partner-atlas -messages /path/to/messages
```
