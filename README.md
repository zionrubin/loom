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

That literature has since moved up an altitude, to serving *programs* rather
than requests — where the unit a caller waits on is a whole multi-call
trajectory and the resources it needs are shared rather than owned. Loom follows
it there with `loom.Fleet`: many pipelines running at once as one engine, with
one quota, one ceiling, one cache, slots admitted fairly between them, and a
blackboard they use to reach each other's conclusions.
[docs/ASYNC.md](docs/ASYNC.md) maps that half of the playbook — Autellix's
attained-service scheduling, Parrot's application-level view, Kairos, Astraea,
Continuum — and says which rows are honestly still empty.

## What you get

- **Declarative pipelines** — `Map` / `Filter` / `FlatMap` / `Combine` plus
  AI-native `Infer` (templated per-record inference, JSON parsing,
  validation) and `ReduceAI` (parallel tree aggregation). Branching builds
  DAGs; the planner fuses adjacent pure stages.
- **Iteration, with a pluggable algorithm** — `pipeline.Iterate` runs a model
  operation over a record set repeatedly, and `algo.Algorithm` decides what
  "repeatedly" means: two methods over plain data that return the messages
  making up the next round, and no messages when it has converged. Three ship —
  `BSP` (Pregel message passing over a graph), `Refine` (a record critiquing
  itself), `Beam` (frontier search that grows its own graph). A round is an
  ordinary stage batch through the ordinary scheduler, so retries, escalation,
  admission control and the governor apply to a superstep for free. The economics
  invert the usual ones: a vertex's cache key is its state and its inbox rather
  than the round it is in, so **cost per round falls as the loop converges**, and
  a vertex that has already seen its input is not scheduled at all. Three bounds
  always apply — quiet, a round cap, a stage budget — and the stage reports which
  one stopped it, because converging and running out of money produce identical
  records.
  The constellation view draws such a stage as concentric orbits — one ring per
  superstep, the outer rings thinning as vertices go quiet — with the per-round
  frontier and the halt reason in the stage inspector.
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
- **Cost before you spend it** — `loom.Explain` projects a run without making
  a single model call: per-stage calls, rendered prompt sizes, prompt-cache
  economics, registry-priced cost, and the wall-clock floor provider rate
  limits impose. It works because a pipeline's cheap stages are ordinary Go
  functions and its expensive ones are declarative data, so the projection
  *executes* the cheap skeleton and models only the paid calls — record counts
  are exact, not extrapolated. Every stage reports an expected cost under one
  stated assumption and a **ceiling** that rests on none, because `MaxTokens`
  is a cap the provider enforces; the ceiling is the number to hand
  `WithRunBudget`, and the report says outright whether your budget covers it.
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
- **Stream mode — an input that never ends** — `loom.Stream` runs a pipeline
  against a `stream.Source` until it is stopped. A Loom pipeline is bounded
  because its aggregates are, so the one thing stream mode adds is a **window**:
  `Dataset.Window` cuts an endless input into finite sets, and everything
  downstream of it runs once per pane. Watermarks say when a set is complete —
  the minimum across the source's splits, held back per stage by the model calls
  still in flight — and a **checkpoint** ties window state, source positions and
  sink commits into one recoverable point, taken by briefly quiescing the job
  rather than by flowing barriers through it (a pause of one task latency, on a
  workload whose tasks are model calls, costs nothing worth defending against).
  Delivery is at-least-once and spend is exactly-once: a replayed record hashes
  to the same cache key, so an interrupted job costs wall clock rather than
  tokens. File and Kafka sources and sinks ship. See
  [docs/STREAMING.md](docs/STREAMING.md).
- **Fleets — many agents on one engine** — `loom.Fleet` runs any number of
  pipelines at once and holds the things that were never properties of a
  pipeline in the first place: **one** rate limiter (a quota belongs to an
  account), **one** budget governor (`WithFleetBudget` — a ceiling enforced per
  pipeline is a ceiling multiplied by pipelines), **one** content-addressed
  cache (an agent replays a sibling's completed work for free), and **one** set
  of execution slots. Slots are not first-come-first-served: a contended slot
  goes to the agent whose *program* has been served least, so a three-call
  summary overtakes a 10,000-record sweep instead of queueing behind it —
  Autellix's attained-service scheduling, one altitude up — with an aging rule
  bounding how long a heavily served agent can be held back. Agents coordinate
  through a **blackboard**: append-only, versioned topics that `Fleet.Post`
  appends to and later agents read, snapshotted by content hash so a post cannot
  disturb a running agent, and so the reader's cache key changes when the board
  does. `loom.Run` is a fleet of one, built through the same path. The fleet
  report gives each agent its completion time next to the slot-time it was
  given. See [docs/ASYNC.md](docs/ASYNC.md).
- **MCP tools, under the envelope** — register Model Context Protocol servers
  with `loom.WithMCPServer`; stages declare what they may call with
  `pipeline.WithMCP`, and the planner turns that one declaration into a grant
  per tool, the server's host on the egress allowlist, and the digest of the
  tool descriptors the stage was compiled against. The connections are the
  interesting part: they are made **once per host**, before any task exists —
  next to the rate limiter and the budget governor, because a connection belongs
  to a server process and an account rather than to a pipeline, so a fleet of
  ten agents shares one session and one bound on how hard they may push it. MCP
  is JSON-RPC, so one session serves any number of concurrent calls; what a task
  leases is a **call slot**, not a connection, from a per-server semaphore that
  is the tool-side analogue of token-bucket admission control. Envelopes name
  servers and carry no socket, so a task stays shippable — and the digest makes
  the name a contract: a server that comes back from a reconnect offering
  different tools fails the tasks planned against the old ones instead of
  silently substituting. That digest also joins the fingerprint, so **upgrading a
  server recomputes exactly the stages that could have called the tools that
  changed**. A model that *chooses* a tool is two stages (`Infer` → `mcp.Dispatch`)
  rather than a loop hidden inside a task, so the choice is data in the record
  and the call is an ordinary scheduled task. In the constellation view a server
  is a **ring** in its own band below the stage clusters — the mirror of the
  shared-value band above them — whose filled arc is the peak calls in flight
  against the ceiling, with the sessions as dots at its centre; press `m` for
  the inspector's per-tool breakdown, queue time, and callers. See
  [docs/MCP.md](docs/MCP.md).
- **Content-addressed caching = checkpointing** — task results are keyed by
  op fingerprint + input content. Reruns and crash recovery replay
  completed AI work with zero model calls and zero cost, across process
  restarts with a state dir, and across the *processes of a fleet* with a
  shared one.
- **A fleet of worker processes** — `loom.WithWorkerService` puts a run's tasks
  on a durable queue with leases and lets worker processes claim them;
  `loom.Serve` is the other side, and takes the same options. Nothing above the
  executor seam changes, because `Executor` is one method over serializable
  data: the planner, the admission control, the escalation ladder, the governor,
  the cache and the event stream cannot tell which process a task ran in. What
  distribution costs is a lease, and the lease is what makes it survivable — a
  worker killed mid-call loses its claim rather than the task, and the task is
  redelivered. Delivery is therefore at-least-once, and it produces
  exactly-once *work*: results are written to an address derived from their own
  bytes and committed under a **fencing token**, so a duplicate execution writes
  the same blob to the same place and exactly one of the two commits becomes the
  result — the other is told which one won, and a worker that stalled past its
  expiry cannot overwrite the worker that replaced it. Workers advertise what
  they can serve (stages, providers, tools, sandbox profiles, MCP servers,
  concurrency) and claim nothing else, because across a fleet "this executor can
  run this task" stops being true by construction. Two queues ship behind one
  contract and one conformance suite: in-memory for a process with several
  workers in it, and `worker/filequeue` for several processes over a shared
  directory.
- **Stateful delta execution** — a context that grows a turn at a time is
  Loom's fourth kind of repeated work, and the first three caches cannot see it:
  the result cache asks "same computation?", the commons asks "same question?",
  the prompt prefix asks "same head?", and none of them asks *"same evolving
  object?"*. `pipeline.WithContinuation` declares that a stage reads one;
  `loom.WithContinuation` supplies the revision. The context lives in the CAS as
  an immutable chain — each revision holds what it *added* and the hash of what
  it extends — so an **envelope carries a couple of hundred bytes where it would
  otherwise carry the transcript** (2981× on the example's 617 kB session), and
  an executor that already materialized an earlier revision **splices**: keep its
  bytes, render the change and a small repair window, and check that the window
  came out byte-identical where it overlaps what the parent held. Rendering is
  not compositional — a closing wrapper moves when you append, a numbered header
  rewrites its front — so nothing here trusts the shortcut. A `Renderer` declares
  how far a change reaches back and one that cannot bound it is never spliced; a
  certificate re-derives every splice's arithmetic in time proportional to the
  *change* (a seam-chain hash, not a 400 kB memcmp), so it runs on all of them
  rather than a sample; a sampled fraction are recomputed from scratch and
  compared, and a divergence quarantines that renderer for the life of the
  process; and a state miss, an oversized append, and an unprovable seam all
  route to the same full render — widening the repair window far enough *is* that
  render, so a failed fast path costs work and never an answer. Each request
  tells its provider how many leading bytes are **certified** identical to the
  previous revision's rendering (99.7% after appending a turn), which is what a
  KV cache can act on rather than guess at. On a fleet, workers advertise what
  they hold and the queue prefers them — *softly*: `Affinity` is a preference, not
  a `Requirement`, because a task that only its state-holder could claim would be
  unclaimable the moment that worker died. SIGKILL the process carrying a session
  and the next round lands on one that has never seen it, rebuilds from the chain,
  and answers identically — one round of latency, no lost work. See
  [docs/DELTA.md](docs/DELTA.md).
- **A commons for external research** — `loom.WithFindings` gates the tools that
  reach public sources, keying on the *question* rather than on the bytes,
  because that is the duplication a result cache cannot see: one question in
  three wordings, asked by agents that started at the same instant. An exact
  key, then a topic-and-facets class, then optional embedding similarity that
  yields candidates rather than hits; a single-flight lease so simultaneous
  askers become one call; a coverage check that narrows the external request to
  the fields it is actually missing; negative results stored so nobody
  re-searches a dead end; and per-topic volatility horizons instead of
  per-answer TTL guesses. Findings carry the capabilities their research
  consumed, so the commons can save a reader a call it was allowed to make and
  never make one it was not. Corrections append revisions and retractions report
  every task that rested on the withdrawn claim. With a shared backend the same
  gate spans executors — PostgreSQL and `pgvector` for the findings and the
  similarity search, a fenced lease for the single flight — so ten processes
  research a subject once between them and a local hit still costs no I/O. See
  [docs/FINDINGS.md](docs/FINDINGS.md).
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
  shared occupancy are visible as they happen. Feed it a projection
  (`loom.Explain` on the same handler) and it shows the forecast before the run
  starts, then reads every stage against it live — spend versus projection in
  the header, and a projected-versus-actual reconciliation in the summary. A
  process that runs several pipelines gets a **universe** (`u`): every run it
  has produced, side by side, each one still whole and enterable — so a
  pipeline that finishes while the next one starts is still there to read.
  Heavy per-node payloads
  (rendered prompts, responses, record JSON) load only for the node you open,
  which is what keeps the view responsive on runs with thousands of tasks.
- **A canvas that prices itself** — `studio` serves **Loom Studio**: the same
  pipeline as a document you edit in the browser, with `loom.Explain` running
  behind every keystroke. It compiles to an ordinary `pipeline.Pipeline`, so
  nothing built here is weaker than something written by hand; it reads your
  sources locally to price against the real records rather than an assumed row
  count; its ⌘K assistant answers from the projection and returns *edits* that
  nothing applies until you accept them; and `Doc.Go` exports the canvas as a Go
  program that compiles, because a builder you cannot leave is a trap.
- **Providers, hosted or your own** — a deterministic mock (offline dev,
  scripted failures), Anthropic and OpenAI adapters over the official SDKs
  with per-call broker-resolved credentials, and **local inference** through
  `providers/llamacpp`: point it at a `llama-server` and the model runs on
  your hardware behind the same seam, so nothing in a pipeline changes. What
  changes is the envelope, and every change is a simplification — cost is
  zero, so the ceiling that binds becomes the *device's* decode width
  (`Limits.MaxConcurrent`, discovered from the server's own slot count rather
  than guessed); no credential exists, so the stage is planned with no secret
  grant at all; egress is loopback and still *on* the allowlist rather than
  exempt from it, so the envelope states — and the executor enforces — that
  those records cannot reach a vendor. The shared prompt prefix stops being a
  metaphor: locally the prompt cache **is** the KV cache, and because a local
  cache write costs nothing to earn back, reuse is asked for on every call
  instead of only on stages that would amortize it. Two servers on two ports
  make an escalation ladder whose rungs are both local. The `model.Provider`
  interface is small; add your own.

## Quickstart

```sh
cd loom

go test ./...            # full suite, no network or keys needed
go run ./examples/triage # complete pipeline on a mock model, offline

# iteration: a research question answered by walking a citation graph the
# model chooses as it goes. The frontier grows while discovering and shrinks
# while converging (2 → 4 → 3 → 2 → 1); one cited paper is outside the corpus
# and the walk creates it; the conclusion is three hops from anything the
# question named. Prints the projection first, then what the run actually spent
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

# MCP: an inventory desk whose tools live behind a Model Context Protocol
# server — a real child process over real pipes, still offline. One connection
# serves every record; the model picks a follow-up tool and the next stage runs
# it; a document on the server becomes a broadcast. Run it twice and the second
# run makes zero tool calls, because a tool call is work and Loom does not pay
# for work twice
go run ./examples/mcp-desk
go run ./examples/mcp-desk -state /tmp/loom-mcp
go run ./examples/mcp-desk -state /tmp/loom-mcp   # 0 tool calls, 0 tokens

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

# an input that never ends: an incident feed graded per event and digested per
# minute. Panes fire on event time rather than wall clock, so the same feed
# always produces the same windows; the report prints panes next to model calls,
# because a pane is the unit that costs money. -crash stops the job a third of
# the way through and starts it again: it resumes at the checkpointed offsets
# with its half-filled windows intact, and the replay costs zero model calls
go run ./examples/watchtower
go run ./examples/watchtower -live      # tail the feed as a writer appends
go run ./examples/watchtower -crash     # price the interruption
go run ./examples/watchtower -window 30s # twice the panes, the same gradings

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
| `algo` | The algorithm seam: the `Algorithm` interface an iterative stage plugs in, plus BSP, refine and beam |
| `plan` | Validation, fusion, fingerprints, least-privilege envelopes |
| `runtime` | Scheduler, retries, rate-limit admission, budget governor, and the slot pool that admits a fleet's agents by attained service |
| `executor` | Executor seam, capability-scoped runtime, model client, tools |
| `ops` | Operation runners (infer, reduce, fused transforms) |
| `model` | Provider abstraction, registry, tiers, escalation bindings, mock |
| `mcp` | Model Context Protocol client: server descriptors, stdio and HTTP transports, the host-owned connection catalog, and the tool adapters stages call |
| `mcp/mcptest` | A scriptable in-process MCP server for tests and offline examples — what `model.Mock` is to a provider |
| `providers/anthropic` | Official-SDK Anthropic adapter, broker-resolved keys |
| `providers/openai` | Official-SDK OpenAI adapter, broker-resolved keys |
| `providers/llamacpp` | Local inference against a llama.cpp server: loopback egress, no credential, the KV cache as the prompt prefix cache, and the device's slot count as the admission ceiling |
| `providers/llamacpp/llamacpptest` | A scriptable in-process llama.cpp server for tests and offline examples — what `mcp/mcptest` is to a server, on a real loopback socket |
| `findings` | The commons: a gate agents pass before reaching a public source, so research one agent paid for is served to the next instead of repeated. `findings/pgstore` and `findings/filestore` share it between executor processes |
| `stream` | Stream mode's vocabulary: sources with resumable positions, watermarks, the windower that turns an endless input into finite sets, sinks, and checkpoints. Plain data and single-goroutine logic — no model, scheduler or run in sight |
| `stream/file` | A directory of JSONL as a stream: a file is a split, a byte offset is a position. The source you develop against, the one that makes a test deterministic, and the one a backfill uses |
| `stream/kafka` | Topics as streams: a partition is a split, an offset is a position, and offsets advance only after the checkpoint that covers them. Narrow `Consumer`/`Producer` seams with a franz-go binding supplied |
| `security` | Grants, secret broker, egress policy, audit log |
| `store` | Content-addressed store, persistent cache, lineage |
| `observe` | Event bus, metrics collector, run reports |
| `viz` | Constellation view: live web visualization of a run (tasks and executors as stars), and the universe of every run in the process |
| `studio` | Loom Studio: the pipeline as an editable document — canvas, live projection, ⌘K proposals, and a Go export that compiles |
| `worker` | The executor seam used: a durable task queue with leases, heartbeats, expiry and fencing tokens; a client that implements `Executor` over it; and the worker that claims, executes and commits. `worker/filequeue` spans processes over a shared directory; `worker/queuetest` is the conformance suite both queues pass |
| `task` | Task + envelope types (serializable — the distribution seam) |

## Knowing what a run will cost

`loom.Explain` answers the question you want answered *before* the run: is
this $12 or $12,000, and will it take four minutes or four hours? It takes the
same options as `Run`, so one config describes a run and can be asked what
that run would cost.

```go
proj, err := loom.Explain(p,
    loom.WithRegistry(reg),
    loom.WithRunBudget(core.Budget{MaxCostUSD: 5.00}),
)
fmt.Print(proj)
```

```
projection  ticket-triage  (barrier driver, no calls issued)
stage                  model                      recs  calls   prompt   cached     exp($)     max($)    floor
tickets                                           2000      0        0        0     0.0000     0.0000       0s
classify               claude-haiku-4-5           2000   2000   198000   151924     0.9513     2.6213    3m33s
briefing               claude-opus-5              2000    287   386500      572    21.2017    49.8730   19m29s
TOTAL                                                    2287   584500   152496    22.1530    52.4943    23m2s
expected 967992 tokens for $22.1530; cannot exceed 1684276 tokens / $52.4943 before retries
run budget $5.0000 is below the ceiling: the governor will stop the run and return partial results
```

No model calls, no secrets resolved, no sockets opened, nothing written to the
state dir — safe to point at a production config. The projection is sharp
rather than a guess because of a property specific to this framework: the
cheap stages are ordinary Go functions and the expensive ones are declarative
data, so `Explain` *executes* the cheap skeleton (`Map`, `Filter`, `FlatMap`,
`Combine` really run) and models only the paid calls. Record counts are
therefore exact, and every prompt is measured after rendering against the
record that will produce it — including which tokens the provider's prefix
cache will serve, since the projection reuses the planner's own break-even
rule. `TestExplainMatchesRun` pins the prompt side to a real run token for
token.

Two numbers per stage, and the difference between them is the whole point.
**Expected** carries one stated assumption — the share of `MaxTokens` a
response fills (`loom.WithExpectedOutput`, default 0.35). **Ceiling** carries
none: `MaxTokens` is a cap the provider enforces, so a first attempt cannot
cost more. Hand the ceiling to `WithRunBudget`.

Because it compiles the pipeline exactly as `Run` does, it doubles as a free
validation pass — unregistered models, unparseable templates, and a prefix
reading a broadcast its stage never declared all surface before anything is
provisioned. Anything it genuinely cannot compute becomes a named warning
instead of a confident wrong number, and the report stops calling the total a
ceiling when it no longer is:

- **Response length** is the one assumption, above.
- **A source function** is never invoked — a cost projection that reads your
  database has defeated its own purpose. Supply records with
  `loom.WithSourceSample`.
- **A `ParseJSON` stage's fields come out of the model**, so a downstream
  `Filter` testing one of them would drop every record during projection while
  keeping them in the real run — an *under*-count, the only way this can be
  wrong in the dangerous direction. Those stages are marked `~`, the run is
  reported as incomplete, and `Projection.Partial()` says so. Name the fields
  with `loom.WithStageSample("classify", map[string]any{"urgent": true})` and
  it becomes exact again. A sample is one scenario applied to every record, so
  pick the values that make the most downstream work and the result is a
  conservative bound.

### In the constellation view

Point `Explain` and `Run` at the same event handler and the
[constellation view](#what-you-get) holds both halves of the comparison:

```go
opts := []loom.Option{loom.WithRegistry(reg), loom.WithEventHandler(v.Handle)}

proj, _ := loom.Explain(p, append(opts, loom.WithStageSample(...))...)  // before
res, _ := loom.Run(ctx, p, opts...)                                     // after
```

The projection is published on the same bus as everything else
(`stage.projected`, `run.projected`), so it arrives while the sky is still
empty — the empty state shows the forecast and the budget verdict instead of
"waiting for a run". Once the run starts, each stage's inspector reads its live
cost against what it was projected to cost, the header carries spend against
forecast and flags going past the ceiling, and the run summary gains a
projected column plus a projected-versus-actual reconciliation. The projection
deliberately survives `run.started`: it describes the pipeline, and the run
that follows is the thing it predicted. `go run ./examples/constellation`
demonstrates the whole loop.

## Building it on a canvas: Loom Studio

`loom.Explain` prices a pipeline before it runs. **Loom Studio** (`studio`) is
what happens when that projection stops being something a program prints at the
end and becomes the thing you edit against: a browser canvas where each step is
a card, the header carries what the run will cost, and the number moves as you
change a model, a batch size, or a branch.

```go
doc, _ := studio.Load("vertical-digest.json")   // or build one in Go
s := studio.New(doc,
    studio.Models(reg),                          // what the steps bind to
    studio.File("vertical-digest.json"),         // autosave every edit
    studio.Constellation(vizURL),                // where the RUN tab goes
    studio.Runner(run),                          // what the Run button does
)
url, _ := s.Start("localhost:8078")
```

It is not a second runtime. The studio edits a *document* — a flat, serializable
list of steps — and `Doc.Build` compiles it into an ordinary
`pipeline.Pipeline`, so a pipeline built here is planned, fused, fingerprinted,
priced, admitted, retried, escalated, cached and audited by the same code as one
written by hand. What the document holds instead of closures is declarations: a
filter is a field, an operator and a value; a derived field is a template; a
source is a spec; a loop is a condition and a round cap. That constraint is what
makes it editable, diffable, and proposable-against.

**The price is computed, not estimated.** The studio reads your sources itself —
locally, off the disk, no model call, no network — and hands the records to
`Explain`, which then executes every filter and derived field against them. So
"148 of 412 days reach the payments rollup" is a count, and the branch
multiplier, where projections usually go wrong, is exact. It also closes
`Explain`'s one blind spot: a `ParseJSON` stage's fields normally come out of
the model, but here the step *declares* its answer shape, so the projection is
told exactly what the next step will read. And because `Explain` runs the pure
Go stages for real, the studio compiles a dry form for pricing in which a write
step computes the same path without writing the file — asking what a run would
cost must not perform any of it.

**One declaration, three consequences.** Saying a step returns
`{summary, topics, signals}` with `summary` required generates the JSON
instruction appended to the prompt, parses the response into the record, and
builds the validation gate whose failure escalates that record to the next model
on the ladder. Hand-written, those are three places that agree until someone
edits one of them.

**⌘K proposes; it never edits.** The built-in assistant answers from the
projection rather than from a model: which step dominates the spend and why,
what a cheaper shape would actually reprice to (computed by pricing it), whether
a $8 cap still fits and what the run does when it does not, the shortest wall
clock the rate limits allow. It returns *edits* — drawn on the canvas as a
dashed ghost card and a diff — and the document changes only when you accept
them. Anything it cannot compute it says so plainly, and `studio.Assist` takes a
model-backed assistant in its place.

**Fan-in is drawn as what it is.** Loom's DAGs fan out but never fan back in, so
a fold over three branches is a *second run* seeded from the first one's output.
The canvas puts it inside a dashed SECOND RUN · FAN-IN band, `Doc.BuildSecond`
compiles it from `RunResult.StageOutputs`, and it is priced with its own stated
assumption instead of being quietly folded into the first pass.

**And there is a way out.** `Doc.Go` renders the canvas as a Go program that
compiles — same operators, same prompts, same versions, same validators, with
`build()`, `buildSecond()` and a `main` that runs both. A visual builder you
cannot leave is a trap; the way out of this one is a file you can review, diff
and run in CI.

```sh
go run ./examples/studio    # offline: invented archive, mock models, real arithmetic
```

Design notes and what is deliberately absent: [docs/STUDIO.md](docs/STUDIO.md).

## Running more than one pipeline: fleets

Most real programs run more than one pipeline. Loom DAGs fan out but do not fan
back in, so a fan-out and the synthesis that fuses its results are two runs —
and a retry, an A/B, or a nightly loop are more.

`loom.Run` provisions everything a pipeline needs and releases it afterwards,
which is right for one pipeline and wrong for several, because none of what it
provisions is a property of a pipeline. A rate limit belongs to an *account*, a
dollar ceiling to a *wallet*, a cache to *work already done*. Two `Run` calls
give you two of each, and every duplicate is a bug waiting for load: together
they exceed a limit neither individually violates, each enforces a ceiling so
neither enforces yours, neither can replay the other's work, and nothing
schedules them against each other.

A **fleet** holds those once and lends them to every agent on it:

```go
fleet, _ := loom.NewFleet(
    loom.WithRegistry(reg),
    loom.WithWorkers(8),                               // slots for the whole fleet
    loom.WithFleetBudget(core.Budget{MaxCostUSD: 20}), // one ceiling, every agent
    loom.WithStateDir("./state"),                      // one cache, every agent
    loom.WithEventHandler(v.Handle),                   // one universe in the view
    loom.WithTopic("findings"),                        // a board they can read
)
defer fleet.Close()

desk := fleet.Go(ctx, wireDesk())          // returns immediately
for _, b := range beats {
    fleet.Go(ctx, beatPipeline(b))         // all running at once
}

// Fan-in, which a single DAG cannot express: each beat posts as it lands, and
// the synthesis reads the snapshot — pinned by content hash, so its cache key
// changes when the board does.
fleet.Await(ctx, "findings", len(beats))
page, _ := fleet.Run(ctx, frontPage())

fmt.Print(fleet.Report())
```

Slots go to the agent whose program has been served least rather than the one
that queued first, so a short agent overtakes a long one instead of inheriting
its completion time. The report shows both halves of that — `service` is the
slot-time an agent was given, `jct` what a caller waited:

```
fleet  6 agents · 6 slots · 4.096s
agent                stages  tasks   tokens   cost($)   service      wait       jct
wire-desk                 2     60     5166    0.0070   12.833s    3.913s    4.093s
beat-markets              3      7      426    0.0025    3.434s     1.33s    2.396s
beat-policy               3      7      424    0.0025      3.4s    1.425s    2.379s
beat-tech                 3      7      417    0.0024    3.307s    1.474s    2.346s
front-page                2      1      208    0.0083    1.363s      19ms    1.382s
wire-recheck              2     60        0    0.0000       8ms        0s       2ms
slots 6 occupied 99% of 4.096s · 142 tasks admitted
fleet budget $5.0000, spent $0.0226 (0%) across every agent
blackboard: 1 topic(s), 3 post(s), read by reference
```

The beats were launched *after* the 60-task wire desk had claimed every slot and
still finished in roughly half its time; `wire-recheck` reran the desk's whole
input for zero model calls, because the cache belongs to the fleet rather than
to the run that filled it. [`examples/newsroom`](./examples/newsroom) is that
run end to end, offline, and [docs/ASYNC.md](docs/ASYNC.md) is the design.

### Watching them

Point every agent at one handler and the constellation view keeps a
**universe**: one sky per run, retained whole, rather than the latest run
overwriting the last. A fleet does this by construction, since it publishes
every agent onto one bus.

```go
v := viz.New()                    // viz.New(viz.Retain(30)) to hold more
url, _ := v.Start("localhost:8077")

loom.Run(ctx, digest,   loom.WithEventHandler(v.Handle), ...)  // run 1
loom.Run(ctx, overview, loom.WithEventHandler(v.Handle), ...)  // run 2
```

Press `u` for the overview: every run in the process, named by its pipeline,
with how it ended, what it cost, and the shape of its stages — click one to
enter it. `,` and `.` step between runs, `l` jumps to the one still live, and
each run keeps its own stages, tasks, executors, shared values, prompts, and
responses, so a finished pipeline stays as inspectable as the running one.

The live view follows new runs as they start, but never out from under you: if
you are reading a run — its summary open, or a task's prompts on screen — the
new run waits in the header (`◉ <pipeline> live →`) instead of replacing what
you were looking at. Events are routed by run ID, so pipelines running
*concurrently* on one handler land in their own skies rather than interleaving
into one. The universe is bounded (12 runs by default, `viz.Retain(n)` to
change it) — runs are held whole, so the oldest is dropped when a new one
pushes past the limit.

`go run ./examples/vertical-digest` is the two-run shape end to end;
[`examples/game-forge`](./examples/game-forge) is the three-run shape, where
each run consumes what the last one produced — plan the modules, write them,
link them into a playable game.

## Input that never ends: stream mode

A Loom pipeline is bounded because its aggregates are. `ReduceAI` folds a set,
`Combine` folds a set, `Iterate` cannot begin a superstep before every vertex's
mail has arrived — and on a finite dataset the end of the input is what tells
them the set is closed. A stream has no end, so stream mode answers one
question, **when is a set complete?**, and everything else follows from the
answer.

The answer is a window. `loom.Stream` runs the same pipeline, planner,
envelopes, scheduler, cache and budget as `loom.Run`; the only new line in the
pipeline is the one that makes an endless input finite:

```go
p := pipeline.New("incident-desk")
events := p.FromStream("incidents")            // unbounded, bound at run time

events.
    Infer("grade", pipeline.InferSpec{...}).   // per record, the moment it arrives
    Window("per-minute", stream.WindowSpec{    // the finite set
        Assigner: stream.Tumbling(time.Minute),
        Key:      func(r core.Record) string { return r.String("service") },
        Time:     stream.EventTime("at"),
        Lateness: 30 * time.Second,
    }).
    ReduceAI("digest", pipeline.ReduceAISpec{...}) // once per pane, over its records

res, err := loom.Stream(ctx, p,
    loom.WithSource("incidents", kafkaSrc),
    loom.WithSink("digest", sink),
    loom.WithStateDir("./state"),              // result cache *and* checkpoints
    loom.WithJobID("incident-desk"),           // what a restart resumes
)
```

Everything **upstream** of the window runs per record, fully pipelined — a
grading call starts the moment an event lands. Everything **downstream** is
scoped to the pane. That one rule is the whole semantics: *a window replaces
"the end of the input" with "the end of the pane"*.

Four things follow, and each is a place where being an AI framework changes the
answer a stream engine would give:

- **A window fires once.** Lateness *delays* the firing rather than causing
  another one, and speculative panes are opt-in (`stream.AtInterval`,
  `stream.AtCount`). A classic engine re-fires a whole window on every
  straggler; when the operator is a model call that is not a latency choice, it
  is a bill multiplied by the stragglers.
- **A pane is the unit that costs money.** Halving the interval doubles the
  aggregations; a keyed window multiplies them by the key's cardinality;
  `Sliding` multiplies them by `size/slide`. `loom.Explain` prices a window as
  one pane for exactly this reason.
- **Checkpoints quiesce.** Ingestion stops, the graph finishes what it is
  holding, and window buffers, source positions and sink commits are recorded
  together — then positions advance last. Barrier-based checkpointing exists
  because stopping a stream engine costs throughput measured against
  microsecond operators; a pause of one task latency every thirty seconds, on a
  workload whose tasks are model calls, buys a snapshot with no in-flight state
  to serialize and no alignment to get wrong.
- **At-least-once delivery, exactly-once spend.** A crash loses the work since
  the last checkpoint. On restart those records are re-read, re-planned into the
  same tasks, hashed to the same cache keys — and served. Loom does not need a
  transactional source to avoid paying twice; it needs deterministic record IDs
  and event-time-ordered panes, and it has both.

`stream/file` reads a directory of JSONL — a file is a split, a byte offset is a
position — and without `Follow` a fully-read file *ends*, which is what makes the
same job a backfill. `stream/kafka` reads topics with Loom's checkpoint as the
source of truth for offsets, writing group offsets afterwards for whatever
watches consumer lag.

`go run ./examples/watchtower` is the whole shape end to end, offline, and
[docs/STREAMING.md](docs/STREAMING.md) is the design — including the phases not
yet built: transactional sinks, renewing rate budgets with backpressure /
shedding / model-ladder degradation, session windows, and split assignment
across a fleet.

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

### Sharing the research, not just the results

A broadcast shares bytes between tasks; a prefix shares them with the provider;
the result cache shares *completed work* between agents. None of them stops two
agents researching the same thing, because the result cache keys on the bytes
going in — and research does not arrive as identical bytes:

- **One question, three wordings.** Two desks wanting the same company's revenue
  write two query strings. Same subject, two cache keys.
- **Same instant, no prior.** Agents launched together all miss a cold key at
  once, all call out, and all write the same entry. Concurrency is precisely
  what defeats a write-then-read cache.
- **Enough, not identical.** A finding gathered for one purpose often carries
  four of the five fields another agent needs — worth zero to an all-or-nothing
  cache.

`loom.WithFindings` puts a **gate** in front of the tools that reach public
sources. It keys on the question rather than on the bytes:

```go
fleet, _ := loom.NewFleet(
    loom.WithMCPServer(web),
    loom.WithFindings(findings.Config{
        Gate: []string{"mcp/web/search"},                 // ← the public source
        Policy: findings.Policy{
            Topics: map[string]findings.TopicPolicy{
                "mcp/web/search": {Volatility: findings.Slow},
            },
        },
    }),
)
```

Nothing in the pipeline changes — the stage declares the tool it always
declared, and whether the call reaches the source or the ledger is decided
beneath it. `examples/commons` runs four analyst desks over six companies, each
desk asking in its own phrasing, and prints the same fleet both ways:

```
                           no commons   with commons
calls to the source                24              6
wall clock                      415ms          171ms
spent at the source           $0.0960        $0.0240

findings  24 asked · 18 reused (75%) · 6 researched
  exact 0 · class 12 · near 0 · coalesced 6 · topped-up 0
  avoided $0.0720 and 2.198s of research, spent $0.0240
  gate overhead 1.122ms total, 47µs per question
```

No embedder was configured for that run: every reuse came from the free tiers —
a topic-and-facets index, and a single-flight lease that collapses simultaneous
askers onto one call. The gate costs about 1/2500 of the call it decides about,
which is the argument for gating every task rather than the ones somebody
guessed would collide.

Four properties make writing to a shared ledger *from inside a task* safe in
front of a content-addressed cache:

- **A hit is substitutable for the call it replaces.** The example asserts it:
  every brief is byte-identical in both runs. The ledger makes spend
  order-dependent and answers order-independent.
- **Entries are append-only and content-addressed.** A correction appends a
  revision; a retraction writes a retracted head and *returns every task that
  was served the claim*, so a withdrawn fact can find what rests on it.
- **The knowledge hash excludes the clock**, so two agents that independently
  learn the same thing corroborate one finding instead of filing two.
- **The commons may save you a call you were allowed to make, never make one you
  were not.** Every finding carries the capabilities and hosts its research
  consumed, and a reader is served only if its own envelope holds them —
  otherwise a shared cache is just a way around an egress allowlist.

### Across executors

That commons is one process's. A fleet worth running usually is not one process
— a worker per machine, ten pods of one deployment — and each of those holds a
ledger the others cannot see, so the duplication the gate removed between agents
comes back at the process boundary. One field connects them:

```go
backend, _ := pgstore.Open(ctx, dsn, pgstore.Options{Dimensions: 1536})
loom.WithFindings(findings.Config{
    Gate:   []string{"mcp/web/search"},
    Shared: findings.NewShared(findings.SharedConfig{Backend: backend}),   // ← every executor
})
```

It adds a rung rather than replacing one — the in-process ledger is still
consulted first and still answers with no I/O — and a shared candidate is
adopted into that ledger and then checked by the same sufficiency ladder as a
local one, containment included. The single-flight lease grows a second scope:
a row with an expiry, a heartbeat and a fencing token, so executors that miss
the same question at the same instant make one call between them and a crashed
lease owner costs one TTL rather than blocking the question. PostgreSQL with
`pgvector` is the production backend (`findings/pgstore`); a shared directory is
the one for fleets that are several processes on one machine
(`findings/filestore`); `Store`, `VectorStore` and `Leases` are the whole seam
between them and the gate. An unavailable backend degrades to ordinary research
unless strict mode is asked for.

`examples/commons-shared` runs four executor *processes* over six overlapping
subjects and counts the calls in the source's own log:

```
                                 no commons shared commons
calls to the source                      24              6

findings  24 asked · 18 reused (75%) · 6 researched
  local  exact 0 · class 0 · near 0 · coalesced 0 · topped-up 0
  shared exact 0 · class 7 · near 0 · coalesced 11  →  18 external call(s) another executor had already made
```

The local row is zero because no executor asked anything twice itself: all 18
avoided calls crossed a process boundary, and 11 of them were collapsed by the
distributed lease rather than served from the store.

[docs/FINDINGS.md](docs/FINDINGS.md) is the design, and the literature it maps:
semantic caching and its threshold problem, per-entry learned boundaries,
memcache leases, negative caching, temporal invalidation, and truth maintenance.

## Calling tools: MCP servers

Register the servers a run may use; declare, per stage, what it may call.

```go
inventory := mcp.Stdio("inventory", "npx", "-y", "@example/inventory-mcp").
    WithTools("lookup_sku", "stock_level")            // least privilege, at the deployment

src.MapTools("enrich", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
    out, err := s.Invoke(ctx, mcp.ToolName("inventory", "lookup_sku"),
        map[string]any{"sku": r.String("sku")})
    if err != nil {
        return core.Record{}, err
    }
    r.Data["product"] = mcp.Text(out)
    return r, nil
}, pipeline.WithMCP("inventory", "lookup_sku"), pipeline.WithVersion("v1"))

loom.Run(ctx, p,
    loom.WithMCPServer(inventory),
    loom.WithMCPResource("voice", "inventory", "mem://voice"),  // a doc → a broadcast
)
```

That single `WithMCP` declaration produces the grant
(`tool:mcp/inventory/lookup_sku` — an ordinary capability; MCP needed no second
permission mechanism), the server's host on the stage's egress allowlist, and
the digest of the tool descriptors the stage was compiled against. A server
nobody registered fails compilation, not the first record.

**Where the connections come from** is the part worth stating plainly, because
the obvious answers are all wrong at pipeline scale. A connection per record
spawns a process per record. A connection per task is Spark's `mapPartitions`
answer, which is per record again when the batch is one. A connection per run
gives a fleet of ten agents ten of them, and together they exceed a limit none
of them individually violates.

Loom connects **once per host** — during provisioning, before any task exists,
in the same structure that holds the one rate limiter and the one budget
governor, and for the same reason: a connection is a property of a server
process and an account, not of a pipeline. Because MCP is JSON-RPC and every
request carries an id, one session serves any number of concurrent calls, so
what a task leases is a **call slot** rather than a connection — bounded per
server by a semaphore that is the tool-side analogue of the scheduler's
token-bucket admission control. Ten thousand records, one connection; a fleet of
agents, still one.

Everything else follows from treating the tool set as data:

- **Envelopes name servers and carry no socket**, so tasks stay shippable to a
  remote executor — the same indirection that makes a broadcast a hash rather
  than a copy. The name is also a contract: a server that comes back from a
  reconnect advertising different tools fails the tasks planned against the old
  ones instead of silently invoking something else.
- **The descriptors join the fingerprint**, so upgrading a server recomputes
  exactly the stages that could have called the tools that changed, and leaves
  the rest of the cache warm. Whether to cache at all stays the author's call:
  a cacheable stage asserts that replaying the recorded result is as good as
  calling again, which for a lookup is usually true and for a write never is.
- **Credentials are named, never held.** `AuthSecret` / `EnvSecrets` are
  resolved through the run's broker at provisioning and reach the transport
  only, so no task needs a secret grant for a server it calls — it gets a lease
  on an already-authenticated session.
- **A model that chooses a tool is two stages**, not a loop inside a task:
  `Infer` emits `{"tool": ..., "args": {...}}` and `mcp.Dispatch` runs it, so the
  choice is visible in the record and the lineage and the call is scheduled,
  retried, and budgeted like everything else. A hidden loop would have unbounded
  cost under a per-task budget, a cache key that describes only its input, and
  failures the scheduler cannot classify. When a loop is genuinely wanted it
  already exists one altitude up, in `pipeline.Iterate`.

[`examples/mcp-desk`](./examples/mcp-desk) is all of it running offline against
a real child-process server, and [docs/MCP.md](docs/MCP.md) is the design.

![The constellation view of a run that calls MCP tools: twelve completed tasks
in four stage clusters, the shared `voice` value feeding down from above, and
the `inventory` MCP server as a ring below, its dashed feeds running up to the
two stages that call it.](./assets/mcp-constellation.png)

The ring is the server. Its circumference is the concurrency ceiling and the
bright arc is the most calls ever in flight at once; the dot at its centre is
the session — one, shared by every call in the run. Press `m` for the
inspector's per-tool timings, queue time, and callers.

## Running the model yourself

Point `providers/llamacpp` at a `llama-server` and the model runs on your
hardware behind the same `model.Provider` seam as an API.

```go
reg := model.NewRegistry()
props, _ := llamacpp.Register(ctx, reg,                    // asks the server what it is
    llamacpp.New("http://127.0.0.1:8080"), "local-fast", model.TierFast)
llamacpp.Register(ctx, reg,
    llamacpp.New("http://127.0.0.1:8081"), "local-deep", model.TierDeep)

src.Infer("triage", pipeline.InferSpec{
    Binding: model.Binding{Tier: model.TierFast,            // both rungs local
             Escalation: []string{"local-deep"}},
    Prefix:  severityRubric,                                // served from the KV cache
    Prompt:  "Incident: {{.text}}",
    ParseJSON: true, Validate: ...,
})
```

**Nothing in the pipeline changes** — a binding names a model, not a machine.
What changes is the envelope around the call, and each change is a
simplification rather than a special case:

- **The ceiling stops being a rate and becomes a width.** A hosted model
  meters how fast you may ask per minute; a model on your device decodes some
  fixed number of sequences at once, and asking faster does not fail — the
  excess queues *inside* the server, where the scheduler can neither see it
  nor schedule around it. So `model.Limits` gained `MaxConcurrent`, admission
  holds it across the call rather than merely before it, and `Register` reads
  the number from the server's own `/props` instead of a config file that goes
  stale the first time somebody changes `--parallel`.
- **Cost is zero, so the dollar governor stops being the bound that matters.**
  Pricing is left at zero because zero is the true marginal cost of a token
  you generate yourself. Tokens are still counted — free is not the same as
  unmeasured.
- **No credential exists to leak.** `SecretRef` is empty, so the stage is
  planned with model grants and *no secret grant at all*. It is not a stage
  trusted with a key it happens not to use.
- **Egress is loopback, and still on the allowlist rather than exempt from
  it.** A local provider could report no endpoint — the in-process,
  always-allowed answer — and naming `127.0.0.1` instead is what puts it in
  the envelope, where the executor checks it before every call. So a stage
  bound to a local model *provably* cannot send its records to a vendor, which
  is what makes the mixed deployment expressible in the ordinary way: personal
  data triaged locally, only the redacted aggregate escalating to a frontier
  model, the boundary a binding and the envelope the proof.
- **The prompt cache stops being a metaphor.** `InferSpec.Prefix` exists so a
  provider's cache can serve the stage-stable head; locally that cache *is*
  the KV cache. The planner's break-even rule — enable prefix caching only for
  stages issuing more than one call — exists to earn back the premium a remote
  cache *write* costs, and a local KV write costs nothing, so the adapter asks
  for reuse on every call and `CacheWriteTokens` stays zero because there is
  nothing to amortize.

[`examples/on-device`](./examples/on-device) is an on-call incident desk
running entirely on local models, offline and with nothing installed — it
starts its own llama.cpp-compatible servers on real loopback sockets when no
address is given. It ends by printing each property above as a number the run
produced, including what the identical pipeline would have cost billed by the
token (`loom.Explain` against hosted rates: no key, no socket, no call).
[docs/INFERENCE.md](docs/INFERENCE.md) is the design.

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
  barrier and streaming execution. `loom.Run` is a fleet of one, built through
  the same construction path, so what a run and an agent share cannot drift
  either.
- A fleet's agents coordinate at agent boundaries, not inside a task. A task
  that could publish mid-run would make its own cached result depend on
  execution order, and a task replayed from cache would not publish at all — so
  the board's contents would depend on how warm the cache happened to be.
- The studio's document is declarative because it has to survive a round trip
  through a browser, and that constraint pays for itself: a step's cache version
  can be the hash of its own declaration, an assistant can propose a diff rather
  than generate code, and the answer shape can be one declaration instead of
  three that agree until someone edits one. What it cannot express is arbitrary
  Go — which is what the Go export is for. [docs/STUDIO.md](docs/STUDIO.md).

- Findings are written from *inside* a task, which the blackboard deliberately
  forbids — and the exemption is earned rather than assumed. A blackboard post
  changes what a later stage in the same run reads, so it cannot happen
  mid-task. A findings hit is *substitutable* for the call it replaces, so it
  can: the answer is the same either way and only the spend differs, which is
  the trade worth making and the one `examples/commons` asserts rather than
  claims. [docs/FINDINGS.md](docs/FINDINGS.md).
- A continuation is a *reference to something that changes*, which is the one
  thing Loom had no shape for. A broadcast is registered once because every task
  reads the same bytes; a continuation is registered per run because each run
  reads a little more than the last. Getting that distinction into the type
  system is what makes the rest fall out: the envelope holds a revision hash, so
  it stays small; the hash joins the fingerprint, so the cache invalidates
  exactly the round that changed; the key is what a queue scores locality on;
  and the parent hash is what tells an executor which of its own state it may
  build on. [docs/DELTA.md](docs/DELTA.md).
- The certificate in `delta` deliberately does **not** prove the splice was
  right, and saying so is the point. It proves the splice's arithmetic — that
  the result is the parent's first *n* bytes plus what was re-rendered, that the
  segments just before the boundary came out identical, that the digest follows
  — in time proportional to the change. Proving more would mean reading the
  bytes it did not touch, which is the rebuild it exists to avoid. So the honest
  design is a ladder: a declared bound on how far a change reaches, evidence at
  the seam on every splice, a sampled full recomputation to test the declaration
  itself, and a fallback that is the same code path with the window widened all
  the way. [docs/DELTA.md](docs/DELTA.md).
- The cheapest tier of the findings gate is also the most certain, and that is
  structural rather than lucky. Matching on topic-and-facets says two questions
  are about one subject; matching on embedding similarity says only that they
  look alike, so it yields candidates that must then be *checked*. The tier that
  needs a model is therefore the last one tried, its verdicts are memoized per
  question-and-finding pair, and it is skipped entirely when the ledger's own
  record of what a topic costs to research says the judgement would cost more
  than the lookup could save.

The scaling path (remote worker fleets, shared object-store CAS, WASM
sandboxes with grant-derived imports) is laid out in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#6-scaling-path-from-local-runtime-to-distributed-system),
and the inference-engine lineage — what Loom borrows from vLLM/SGLang and
what it deliberately leaves out — in [docs/INFERENCE.md](docs/INFERENCE.md) for
the call and [docs/ASYNC.md](docs/ASYNC.md) for the program.

The dimension Loom was missing longest was **iteration**: records flowed
forward once, so nothing could look at a stage's output and decide to go around
again — which ruled out the workloads people most want, deep research, entity
resolution, knowledge-graph construction and refine-until-good, all of which are
loops and most of which are loops over a graph.
[docs/ITERATION.md](docs/ITERATION.md) makes the case that Loom is the only
place this can be built *safely*: the governor bounds a loop in dollars,
content-addressed caching makes cost per round *fall* as it converges, envelopes
contain a program that discovers its own targets, and lineage is the only way to
audit six hops of model-derived inference.

It is now built, and one step further than that document proposed. Rather than a
graph operator, `pipeline.Iterate` is an operator whose *control flow* is a
plug-in — so Pregel is one algorithm among several rather than the only shape
available, and writing a new one is two pure methods that need neither a model
nor a scheduler to test. [docs/ALGORITHMS.md](docs/ALGORITHMS.md) is the design;
`examples/multi-hop` is it answering a research question by walking a citation
graph the model chooses as it goes, reaching a conclusion three hops from
anything the question named.

Iteration is also what makes the newest layer worth having. A loop's context
grows by a finding, a critique, a turn — a little each round, all of it needed
every round — and until now every round paid for the whole of it, twice over:
once to render it and once to ship it to whichever process was running. **Stateful
delta execution** ([docs/DELTA.md](docs/DELTA.md)) makes that context an
immutable chain in the CAS, so what an envelope carries is a hash and what an
executor holding the previous revision does is splice. The design worth reading
for is not the splice but the discipline around it, borrowed almost verbatim
from TokTier's guarantee ladder: a declared bound on how far a change can reach,
per-splice evidence at the seam, sampled recomputation against the reference
path, and a fallback that is the same code with the repair window opened all the
way. State makes a round cheap; it is never what makes it right. Kill the worker
carrying a session and the next round rebuilds from the chain and answers
identically, which is the whole claim and the thing `examples/delta-session`
checks rather than asserts.

## Demo

[▶️ Watch the demo](./assets/demo.mp4)
