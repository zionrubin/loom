# Stream mode: an unbounded input for a framework built on finite ones

A Loom pipeline is bounded because its aggregates are. `ReduceAI` folds a set,
`Combine` folds a set, `Iterate` cannot begin a superstep before every vertex's
mail has arrived. Each of them has to know that the input is complete, and on a
finite dataset the end of the input is what tells them.

A stream has no end. So stream mode is not a new execution engine, a second
scheduler, or a parallel set of operators. It is the answer to one question —

> **When is a set complete?**

— plus the machinery that answer needs to be trustworthy. The answer is a
**window**; the evidence for it is a **watermark**; and what makes both
survivable is a **checkpoint** that ties them to where the source was read to
and what the sink has been told.

Everything else about a Loom run is unchanged. Same authoring API, same
planner, same task envelopes, same least-privilege grants, same class-aware
retry and escalation ladder, same content-addressed result cache, same budget
governor, same fleet, same worker queue, same `loom.Explain`. A stage cannot
tell whether the records reaching it came from a slice or from Kafka.

```
                 the only three new things

  stream.Source ──► records with an event time and a resumable position
  pipeline.Window ─► turns an endless stream into a sequence of finite sets
  stream.Sink   ──► where a finished set goes, and what a position is safe against
```

---

## 1. The shape of a stream job

```go
p := pipeline.New("incident-desk")
events := p.FromStream("incidents")          // unbounded, bound at run time

events.
    Infer("grade", pipeline.InferSpec{       // per record, the moment it arrives
        Binding:   model.Binding{Tier: model.TierFast},
        Prompt:    "Grade this incident on {{.service}}: {{.text}}",
        ParseJSON: true,
    }).
    Window("per-minute", stream.WindowSpec{  // the finite set
        Assigner: stream.Tumbling(time.Minute),
        Key:      func(r core.Record) string { return r.String("service") },
        Time:     stream.EventTime("at"),
        Lateness: 30 * time.Second,
    }).
    ReduceAI("digest", pipeline.ReduceAISpec{ // once per pane, over its records
        Binding: model.Binding{Tier: model.TierDeep},
        Prompt:  "Summarize {{.Count}} incidents:\n{{range .Items}}- {{.}}\n{{end}}",
    })

res, err := loom.Stream(ctx, p,
    loom.WithSource("incidents", src),
    loom.WithSink("digest", sink),
    loom.WithStateDir("./state"),            // result cache *and* checkpoints
    loom.WithJobID("incident-desk"),         // what a restart resumes
    loom.WithLateness(5*time.Second),        // the source's out-of-orderness
)
```

The window is the only line that is new to the pipeline, and it is where the
whole design lives:

| position relative to the window | what it means |
| --- | --- |
| **upstream** | runs per record, as records arrive, fully pipelined. This is where per-event `Infer` belongs when latency matters. |
| **the window** | buffers records into panes; fires a pane when event time says it is complete. |
| **downstream** | scoped to the pane. An aggregate folds the pane, not the stream. |

That last row is the whole rule: *a `Window` stage replaces "the end of the
input" with "the end of the pane"*.

`loom.Run` refuses a pipeline with a stream source or a window stage, and
`loom.Stream` refuses one without a stream source, or one whose aggregate has no
window between it and the source. All three failures happen at start-up, with
the fix in the message.

---

## 2. Ingestion

```go
type Source interface {
    Splits(ctx context.Context) ([]Split, error)
    Open(ctx context.Context, sp Split, from Position) (Reader, error)
    Close() error
}

type Reader interface {
    Read(ctx context.Context, max int, wait time.Duration) ([]Event, error)
    Commit(ctx context.Context, pos Position) error
    Close() error
}

type Event struct {
    Record core.Record   // what the pipeline sees
    Time   time.Time     // when the thing happened
    Pos    Position      // where the reader is *after* this event
}
```

Three decisions are worth defending.

**It is pulled, not pushed.** A push source has to be told when to stop, and
every source has to implement that correctly. A pulled one is backpressured by
construction: nothing arrives that the job did not ask for. It is the same
reason a stage takes records off a pipe rather than being handed them, and it is
what lets a checkpoint pause ingestion by simply not asking.

**Splits are first class.** A split is a topic partition, a file, a shard: the
unit that is independently readable and independently resumable. Splits are what
make parallel ingestion possible and what make watermarks honest — see below.

**Positions are opaque and JSON-shaped.** Loom never interprets a `Position`; it
stores it in a checkpoint and hands it back to `Source.Open` on restart. The
`Offset` field exists so that a human reading a checkpoint file can see what it
means: *offset 4096 of `events-03.jsonl`*, and that is exactly what it says.

### 2.1 Watermarks

A watermark is a claim: *no record older than this will arrive again.* It is
tracked per split and reduced to one number for the job:

```
job watermark = min over splits still holding the line of (max event time seen − Lateness)
```

**The minimum, not the maximum.** A job reading four partitions is only as
caught up as its furthest-behind partition. Taking a maximum would produce a
watermark that races ahead of the slowest reader and declares complete a window
that is still filling — which does not fail loudly, it quietly drops data.

Two rules keep that minimum from being useless:

- **Idleness.** A split silent for `IdleTimeout` (default 30s) stops holding the
  line. Without it, one quiet partition holds every window in the job open
  forever, because a silent split's claim never improves.
- **Retirement.** A split that reports `ErrSplitDone` is excluded outright.

Both are reversible, and the watermark is monotonic, so a split rejoining can
never move event time backwards.

`Lateness` here is the *source's* bounded out-of-orderness — how far behind its
own clock the source may deliver. A window's own `Lateness` is a separate,
later grace period stacked on top of it.

### 2.2 Watermarks across asynchronous stages

Loom has a problem a classic stream engine does not: its operators are
asynchronous and take seconds. A stage with model calls in flight cannot
honestly repeat a watermark it was handed — its own outputs are still to come.

Blocking at every watermark would destroy pipelining. Instead each stage
forwards

```
min(watermark received, earliest event time still in flight)
```

recomputing it whenever a task completes. The stage keeps pipelining, and the
watermark it emits is never a claim it cannot back. A task's outputs inherit the
earliest event time among its inputs — conservative, so batching across a window
boundary can only hold a watermark back, never let one run ahead.

Pane boundaries are different: a pane boundary *is* an ordering claim, so it is
not forwarded until the records before it have been. That is a barrier, and it
costs one stage-depth per pane. Panes are rare; watermarks are not; and the two
get the treatment each deserves.

### 2.3 What ships today

| source | split | position | notes |
| --- | --- | --- | --- |
| `stream/file` | a file matching a glob | byte offset | JSONL or a custom decoder; `Follow` tails and discovers new files; without it, a fully-read file ends — which is what makes the same job a backfill |
| `stream/kafka` | a topic partition | offset | direct partition assignment, offsets resumed from Loom's checkpoint, group offsets written after each checkpoint for external lag monitoring |

Both decode into `core.Record` with a **deterministic ID** — the payload's `id`
field, the message key, or `file:offset` / `topic/partition/offset`. That is not
cosmetic. A deterministic ID is what makes a replayed record hash to the same
cache key, which is what makes replay free (§6).

Kafka is implemented against narrow `Consumer` and `Producer` interfaces with a
franz-go binding supplied. A real deployment already has a Kafka client
configured — TLS, SASL, a schema registry, metrics — and a second one inside
Loom would be a second set of all of that and a second share of the broker's
quota.

---

## 3. Windowing

```go
type WindowSpec struct {
    Assigner   Assigner                      // Tumbling / Sliding / GlobalWindow
    Key        func(core.Record) string      // keyed windows
    Time       func(core.Record) time.Time   // override the ingested event time
    Lateness   time.Duration                 // grace before firing
    Trigger    Trigger                       // AtWatermark / AtCount / AtInterval / Any
    MaxRecords int                           // safety valve per window
    MaxWindows int                           // safety valve per stage
    Late       LatePolicy                    // DropLate (default) / FailLate
}
```

### 3.1 A window fires once

This is the deliberate divergence from Flink, and the reason for it is that
Loom's operators cost money.

- **Lateness delays the firing rather than causing another one.** A window fires
  when the watermark passes `End + Lateness`, once, carrying everything that
  arrived in the meantime — including the stragglers. A classic engine re-fires
  the whole window on each late record, which for a text aggregation is not a
  latency choice, it is a bill multiplied by the number of stragglers.
- **Speculative firings are opt-in.** `AtInterval(d)` emits early panes;
  `AtCount(n)` emits every *n* records. Both are things you ask for.
- **Records arriving after the window is gone are late**: counted, and dropped
  or fatal per `LatePolicy`. A growing late count is a fact about your source,
  not about Loom, and hiding it would hide the one number that says your
  lateness bound is wrong.

### 3.2 Panes are event-time ordered

A pane's records are sorted by event time, ties broken on record ID, before it
fires. This is not presentation. Records reach a window in *completion* order,
because the stage before it runs several model calls at once — so a pane
assembled in arrival order would differ between two runs over the same input,
and the aggregate's prompt would differ, and the cache would miss. Sorting makes
a pane a function of its contents.

### 3.3 The shape of the window is the shape of the bill

Everything downstream of a window runs once per pane, so:

- halving the interval doubles the aggregations;
- a keyed window multiplies them by the key's cardinality;
- `Sliding(size, slide)` puts each record in `size/slide` windows and multiplies
  the downstream cost by the same factor.

`loom.Explain` projects a window as a pass-through, which prices **one pane** —
the right unit. Multiply by expected panes per hour for the hourly bill.

`MaxRecords` and `MaxWindows` are the safety valves: past them a window fires
early and purges, which bounds what a hot key or a stalled watermark can consume.
Both are counted as `Evicted` in the report, because a firing caused by a bound
rather than by completeness is a different kind of answer.

---

## 4. Output

```go
type Sink interface {
    Write(ctx context.Context, b Batch) error   // once per pane per terminal stage
    Commit(ctx context.Context, epoch int64) error
    Close() error
}
```

`Write` is called as panes fire. `Commit` is called after a checkpoint has been
durably recorded, and means: *everything written since the last Commit is now
covered, and the source positions behind it are about to advance.*

That split gives sinks a choice:

- **Write directly, ignore `Commit`** → at-least-once. A crash between a write
  and a checkpoint replays the pane.
- **Stage writes, publish in `Commit`** → exactly-once, at the cost of holding
  output for a checkpoint interval.

Every batch carries a stable identity, `Batch.Key() = stage + "/" + pane.ID()`,
derived from the window's start, end and key plus the firing number — the same
string on any machine, before and after a restart. A sink that overwrites by key
therefore sees each pane exactly once however many times the job restarts, with
no transaction anywhere.

| sink | delivery | how the identity is used |
| --- | --- | --- |
| `stream/file` (`PanePerFile`, default) | effectively-once | the batch key is the filename; a replay rewrites the same path with the same bytes |
| `stream/file` (`AppendJSONL`) | at-least-once | duplicates appear, distinguishable by the pane each line carries |
| `stream/kafka` | at-least-once | `loom-pane`, `loom-stage`, `loom-window`, `loom-epoch` headers, so a consumer can deduplicate without knowing anything about the job |

A sink bound to a stage with no window upstream has no pane boundaries to batch
on; its unit of delivery is the checkpoint epoch instead.

---

## 5. Checkpointing: Loom quiesces

Every `CheckpointEvery` (default 30s):

1. **Stop pulling.** The ingest gate closes; readers park at it within one poll.
2. **Let the graph finish what it is holding.** Wait until no reader is between
   the gate and its push, no task is in flight, and every pipe is empty with
   nothing checked out of it.
3. **Snapshot** source positions, every window stage's buffered records and
   watermark, and the running progress counters.
4. **Save** the checkpoint durably.
5. **Commit sinks**, then **commit sources** — in that order, always.
6. **Resume.**

The classic alternative is Chandy-Lamport: barriers flow through the graph and
each operator snapshots as they pass, so nothing ever stops. That design exists
because stopping a stream engine costs throughput measured against
microsecond-scale operators.

**Loom's operators are model calls.** A pause of one task latency, once every
thirty seconds, costs a fraction of a percent of a job's capacity — and buys a
snapshot with no in-flight state to serialize, no barrier alignment to get
wrong, and no partially-processed pane to reason about. It is the trade this
workload makes obvious, and it is why checkpointing here is fifty lines rather
than a subsystem.

A checkpoint that cannot quiesce inside its timeout is **skipped**, not forced:
the previous one still stands, and the report says what was still moving. A job
that keeps skipping is a job that cannot be restarted cheaply, which is worth
knowing.

### 5.1 Ordering is the guarantee

Source positions advance **last**, after the checkpoint naming them is durable
and after every sink covering them has committed. The three cannot be separate
records: positions saved without window buffers would resume a job that has
forgotten its half-filled windows; window buffers saved without positions would
replay records into windows that already hold them.

### 5.2 What a restart resumes

`loom.Stream` with the same `JobID` and `StateDir` loads the last checkpoint,
restores each window stage's buffers and watermark, and opens each split at the
stored position. A job with no `JobID` gets a random one and therefore never
resumes — right for an experiment, wrong for anything deployed.

### 5.3 Stopping

A cancelled context stops the job and returns the report, not an error: a stream
job that was asked to stop has succeeded.

What happens to windows still open depends on **why** it stopped, because the
right answer does:

- **sources exhausted** → drain. There is no more evidence coming, so every open
  window is as complete as it will ever be. This is what makes a file-backed job
  a backfill.
- **cancelled, or a limit reached** → do not drain. Those windows are half full;
  firing them would publish a partial answer as if it were the real one. They go
  into the final checkpoint and the next start picks them up.

`loom.WithDrainOnStop(bool)` overrides it.

---

## 6. At-least-once delivery, exactly-once spend

This is the property that makes the whole design cheap, and Loom already had it:
**the result cache is keyed on content, not on time.**

A crash loses the work since the last checkpoint. On restart those records are
re-read, re-planned into the same tasks, hashed to the same cache keys — and
served. The replay costs wall-clock and no tokens.

So Loom does not need transactional sources to avoid paying twice. It needs
deterministic record IDs (§2.3) and event-time-ordered panes (§3.2), and it has
both. What remains for a transaction to solve is *duplicate output*, which is a
property of the sink and is handled by batch identity (§4).

```
delivery       at-least-once      records may be re-read after a crash
spend          exactly-once       replayed tasks hit the result cache
output         effectively-once   a pane's identity is stable, so a sink can overwrite
```

---

## 7. Observability

New events on the same bus, in the same envelope, so existing consumers keep
working:

| event | carries |
| --- | --- |
| `split.opened` / `split.retired` | split, and where reading resumed from |
| `watermark.advanced` | the new watermark |
| `pane.fired` | pane identity, records, watermark, and whether it was final, early or late |
| `records.late` | how many were dropped and by which window |
| `sink.wrote` | stage, pane, records |
| `checkpoint.committed` | epoch, quiesce duration, window state size, watermark |
| `checkpoint.skipped` | what was still moving |

`StreamResult.Stream` is the printable report: uptime, stop reason, records in,
panes fired, late count, checkpoints taken and skipped, watermark, per-split lag,
and per-window-stage statistics. `StreamResult.Report` is the ordinary
`observe.RunReport` — per-stage tasks, tokens, cost, retries, p95 — covering the
job's whole life.

The number to watch is **panes**, because a pane is what costs money.
The number to watch second is **per-split lag**, because it names the partition
holding your windows open.

---

## 8. Phases

Phase 1 and 2 are implemented. The rest is specified here so it can be built
without redesigning anything above.

### Phase 1 — the substrate ✅

The `stream` package: `Source`, `Reader`, `Split`, `Position`, `Event`, `Sink`,
`Batch`; watermark tracking with lateness, idleness and retirement; the
`Windower` (assigners, triggers, panes, lateness, bounds, snapshot/restore);
`Checkpoint` with a file-backed and an in-memory store. `stream/file` source and
sink. All of it plain data and single-goroutine logic, tested without a model, a
scheduler, or a run.

### Phase 2 — the job ✅

`pipeline.FromStream` and `Dataset.Window`; planner support and start-up
validation; `loom.Stream` — ingestion, watermark propagation through
asynchronous stages, pane-delimited aggregates, sinks, quiesce checkpointing,
restart; `stream/kafka` source and sink over franz-go. Marks on the existing
runtime pipe, so one implementation serves both drivers.

### Phase 3 — semantics hardening

- **Transactional sinks.** A two-phase `Write`/`Commit` contract already exists;
  what is missing are implementations: staged directory published by rename,
  Kafka transactions with `loom-epoch` as the transactional ID.
- **Rate budgets.** `core.Budget` depletes, which on an endless job means a job
  that stops. An unbounded run needs a budget that *renews*:
  ```go
  stream.Rate{CostUSD: 12.00, Window: time.Hour, OnExceed: stream.Backpressure}
  ```
  with three overload policies — `Backpressure` (stop admitting; lag grows;
  nothing is lost if the source retains), `Shed` (drop at the ingestor, counted,
  keeping latency at the cost of completeness), `Degrade` (rebind AI stages down
  their escalation ladder, keeping completeness at the cost of quality). Degrade
  is the interesting one and only Loom can offer it, because only Loom knows the
  ladder.
- **Session windows.** A merging assigner: assign `[t, t+gap)`, merge
  overlapping windows on arrival. The `Assigner` interface is already shaped to
  take a `Merging` sibling. Sessions are the natural window for conversation
  streams, and pair with `delta` continuations.
- **Side outputs for late records.** `LatePolicy` currently drops or fails;
  routing them to a named stage or a dead-letter sink is the third option.
- **Dead-letter routing for undecodable input.** The count exists; the topic
  does not.

### Phase 4 — scale

- **Split assignment across a fleet.** Today one process reads every split.
  Sharding them across `loom.Fleet` agents or worker processes needs a
  coordinator and a per-shard checkpoint, plus Kafka consumer-group mode (whose
  rebalances are exactly the negotiation this avoids today).
- **Nested and re-keyed windows.** A window downstream of a window: the pipe
  marks already nest, the windower does not yet.
- **Bounded windowing in `loom.Run`.** A `Window` in a batch run is a group-by:
  panes fire at the end of input and the aggregate runs per pane. It would make
  the window stage useful outside stream mode and make backfill expressible two
  ways.
- **Watermark alignment.** Holding back a split that has run far ahead of its
  siblings, to bound how much window state a skewed source can accumulate.

### Phase 5 — operations

- **The constellation view for a live job**: panes as they fire, watermark as a
  moving front, per-split lag, checkpoint pulses.
- **Loom Studio**: a window as a canvas node, priced per pane per hour.
- **Barrier checkpointing** as an option for record rates where quiescing stops
  being free — which is not the rate Loom is built for, but is the honest place
  to put the alternative.

---

## 9. What this is deliberately not

**Not a low-latency stream processor.** Loom's unit of work is a model call of
seconds and cents. Every trade here — quiesce instead of barriers, fire-once
windows, unbounded pipes, a checkpoint interval in seconds — is made against
that unit. A framework processing a million events a second should make the
opposite choice on every one of them.

**Not a replacement for Flink or Kafka Streams.** If the operators are cheap and
deterministic, those engines are better at this than Loom will ever be. Loom's
claim is narrower: when the operator is a model call, the interesting problems
move from throughput to cost, correctness of output, and provenance — and those
are the problems the rest of this framework already solves. Stream mode exists
so that they stay solved when the input never ends.

**Not exactly-once delivery.** It is exactly-once *spend* with effectively-once
*output*, which for this workload is the property that actually matters and is
reached without a distributed transaction.
