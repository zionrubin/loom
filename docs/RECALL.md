# The serving layer: a context that is maintained rather than retrieved

[STREAMING.md](./STREAMING.md) is about an input that never ends.
[DELTA.md](./DELTA.md) is about a context that grows a piece at a time and the
cost of re-reading it. This document is about what you get when you put those
two together and point them at the same body of text: a stream of conversations
on one side, a question on the other, and between them a context that is
**maintained** — never rebuilt, never retrieved, always current, always small.

`recall.Desk` is that arrangement, and it is a serving layer in the database
sense of the phrase. The expensive work happens on write. A read is a read.

---

## 1. The shape of the problem

A growing body of conversation is easy to store and expensive to use. The
standard answer is to do the work at query time:

```
question → embed → retrieve k chunks → rerank → stuff a prompt → answer
```

Three things are wrong with that as a *serving* architecture, and none of them
is about retrieval quality:

- **The cost of understanding is on the critical path.** Every question pays the
  latency of finding, ranking and reading. The user waits for work that has
  nothing to do with their question being hard.
- **It is paid again every time.** A thousand questions over the same corpus do
  the same reading a thousand times. Nothing accumulates.
- **The answer is bounded by one retrieval.** What was not in the top *k* did
  not exist. A question spanning forty conversations gets an answer spanning
  whichever eight scored highest.

Invert it. Do the reading when the message arrives — once, per message, forever
— and keep the result as a compact standing context. Then a question is one
model call against a couple of kilobytes, and it is the same one call whether
the desk has ingested ten messages or ten million.

```
conversations → ingest → read · slice · digest → living context → answers
                └──────── continuous, expensive ────────┘ └── cheap ──┘
```

That trade is not free and the package does not pretend it is. It buys a cheap,
bounded, complete-by-construction read path with a per-message write cost and a
lossy context. §6 is the honest accounting.

---

## 2. Neither half is new machinery

The whole of a desk is two things Loom already had, joined at one seam.

**The ingest side is `loom.Stream`.** An ordinary pipeline over an unbounded
source:

| stage | |
|---|---|
| `messages` | `FromStream`, bound to any `stream.Source` |
| `live` | `Filter`: drops the feed's idle heartbeats (§5) |
| `read` | `Infer`, per message, the moment it arrives |
| `keep` | `FlatMap`: drops what is not worth remembering, stamps what is |
| `slice` | `Window`: one pane per conversation per interval |
| `digest` | `ReduceAI` over one slice → one line of the context |

Same planner, same envelopes, same scheduler, same budget, same checkpoints. A
desk gets exactly-once *spend* and resumable positions because a stream job
already has them.

**The living context is a `delta.Chain`.** Each digested slice appends one
revision:

```
Root() ──▶ +entry ──▶ +entry ──▶ +entry
                                    │
                                    ▼
                        Ref{Hash: h, Parent: h′}   ← what a query carries
```

**The seam is the continuation.** The query stage declares
`pipeline.WithContinuation(key)` and each question is run with
`loom.WithContinuation(key, view.Ref)`. That one line is where the design pays
off, because `ops.sharedPrefix` puts a materialized continuation at the *front*
of the prompt, ahead of everything else:

```go
prefix := rt.Continuation.Text + contextPrefix(rt.Env) + renderedPrefixTemplate
```

Four properties follow from that, none of them implemented by this package:

- The envelope carries a revision hash, not the context. A query task is a few
  hundred bytes whatever the desk knows.
- A process that already materialized the previous revision **splices**: it
  keeps the bytes it has, re-renders a repair window, and certifies the seam.
  Appending a line to a context does not cost a pass over the context.
- The revision joins the stage fingerprint, so **the same question against an
  unchanged context is a cache hit** — no model call at all — and the moment a
  conversation lands, the revision moves and the next answer is computed.
- The context is the longest run of identical leading bytes across every query,
  which is exactly what a provider's prompt-prefix cache is for.

---

## 3. The handoff, and why it is a value

Ingestion and querying meet at one place: a published `View`.

```go
type View struct {
    Version  int64      // monotonic, one per published revision
    Ref      delta.Ref  // what a query carries
    Text     string     // the living prompt, as the model receives it
    Entries  int
    Bytes    int
    Messages int64
    ...
}
```

A `View` is immutable. The curator publishes a new one by swapping an
`atomic.Pointer`; a query loads it and holds it for the duration of an answer.

That is the whole synchronization between the two halves of a desk. There is no
lock a query can wait on, so **ingestion cannot slow a read down**; and there is
no lock ingestion can wait on, so **a slow model call on the read side cannot
stall the feed**. A query either sees a revision or the one before it, never a
context halfway through an update — which is what an atomic swap of an immutable
value gets you and what a mutex held across a model call would not.

The write side is single-owner for the same reason. One **curator** goroutine
owns the chain, fed by a channel from the sink. Appending and compacting are two
operations that must not interleave, and one writer needs no lock to guarantee
that.

### Queries do not queue behind ingestion

A desk is a `loom.Fleet`. The ingest job, every compaction, and every question
are agents on it — one budget, one cache, one rate limiter, one slot pool.

The slot pool admits by **attained service**, so a query that has been served
nothing wins the next contended slot from an ingest job that has been running
for an hour. That is the property that makes a fleet usable as a serving layer,
and it is why `Fleet.Stream` exists at all: a stream job with its own private
pool would be a program the fairness policy could not see, occupying a ceiling
of its own, forever.

---

## 4. Staleness is reported, not hidden

A decoupled read path is answering from a snapshot. The only honest thing to do
is say which one:

```go
type Answer struct {
    Version int64         // the revision this was computed against
    Behind  int64         // ingested messages that revision does not cover
    Stale   time.Duration // how long ago it was published
    Cached  bool          // served without a model call
    ...
}
```

A caller who needs the coupling back asks for it explicitly:

```go
n, _ := desk.Post(ctx, msgs...)        // returns a cumulative message count
desk.Await(ctx, n)                     // returns when all n are accounted for
desk.Ask(ctx, recall.Question{Text: q, MinVersion: v, Wait: 30 * time.Second})
```

`Await` waits for messages to be **accounted for**, which means each one either
reached the context or was dropped as not worth remembering. Counting the drops
is not bookkeeping for its own sake: a dropped message reaches no pane, so
nothing downstream could ever account for it, and a caller waiting on a greeting
would wait forever.

`MinVersion` refuses to answer against anything older than a version. A
deadline that expires while waiting for *freshness* is not an error — the caller
asked for the best available and gets it, with `Stale` saying what that turned
out to mean. A `MinVersion` that will never arrive **is** an error, because
answering against an older revision is the thing it ruled out.

---

## 5. Two things that are less obvious than they look

### Compaction is a new root, not an append

A context that only grows is not maintained, it is accumulated. Past
`Config.MaxBytes`, the oldest entries are folded by a `ReduceAI` agent into a
standing **brief** and the newest `Keep` stay verbatim:

```
<brief>   everything older, folded — rewritten rarely, by a model
<entry>   the last Keep slices, quoted — appended constantly, spliced
```

The brief sits at the front, so installing one changes bytes that every later
segment sits behind. That cannot be an append: the chain gets a **new root**,
which costs the next query a full render, once, and every query after it a
splice again. That is the reason the budget is a byte count rather than a count
of entries, and the reason compaction is rare by construction.

Compaction runs *beside* the curator rather than inside it. Entries keep
arriving during a fold, and the fold that lands is spliced in front of whatever
arrived meanwhile — so `Keep` is how many entries survive a fold, not a ceiling
the context is held under between folds. The invariant is the budget.

If a fold cannot help — every entry is within `Keep` and the context is still
over budget — the desk says so once and carries on oversized. A compaction that
cannot shrink is a model call spent to learn that again.

### A quiet feed has to say so

A window closes when event time advances past its end, and event time advances
because messages arrive. So the last slice of a conversation stays open until
somebody says something else: on a busy feed, no time at all; on a feed that has
gone quiet, forever.

`recall.Feed` resolves it by making the claim a live feed is entitled to make.
After `Config.Quiet` with nothing posted, each split emits a **heartbeat**
carrying the present as its event time, which advances the watermark and closes
every window whose end has passed. The heartbeat is dropped by the `live` stage
before it reaches the inference, so it costs one pure task and no model call.

It is worth being precise about what that claim is and is not. *Nothing further
will arrive stamped earlier than now* is true of a live feed and false of a
backfill: replaying an archive through a `Feed` with a long pause in the middle
will have the pause read as "that was all of it", and everything after it as
late. Replay through a durable source instead, where positions and event times
mean what they say. A `Feed` is also memory: a desk that restarts finds it
empty, which is why anything deployed should point `Config.Source` at a
directory of JSONL or a topic and let the durable write happen before the desk
ever sees it.

---

## 6. What it costs, honestly

Per unit of work, in model calls:

| | calls | when |
|---|---|---|
| `read` | one per message | as it arrives |
| `digest` | one per conversation slice | per pane |
| `compact` | one per budget's worth of entries | rarely |
| `answer` | one per question | on the read path |

The first row is the bill, and it is the deliberate one — it is what buys a
query that does not pay for the history. Three things follow:

- **A desk over a noisy feed should say so in `Spec.Read`.** The cheapest way to
  keep a context small is to keep things out of it, and `keep` drops what Read
  judged not worth remembering before it costs anything downstream.
- **The window is the shape of the bill.** Each conversation active in each
  interval is one aggregation and one entry. Halving `Every` doubles both.
  `loom.Explain` prices a pipeline per pane for exactly this reason.
- **The context is lossy, on purpose.** What survives is what `Spec.Digest`
  wrote down and, past the budget, what `Spec.Compact` kept. A desk answers from
  its context and nothing else — it will say a thing is not in there rather than
  infer it — so the prompts that decide what gets written down are where a
  desk is taught its domain.

Where a desk is the wrong shape: a corpus that is queried rarely (the ingest
cost never amortizes), questions that need verbatim detail from arbitrary points
in history (the digest is a summary), and anything where the answer must cite
an exact source span. Retrieval is the right tool for those, and nothing here
stops you putting one behind an MCP tool the answer stage may call.

---

## 7. Restarting

The chain is in the content-addressed store; the desk's pointer is a small JSON
file beside it, written and renamed over so a process killed mid-write resumes
from the previous pointer rather than half of this one. On restart the entries
come back from the file and the brief from the chain — resolving the revision is
also how a desk checks that the store it was pointed at is the store its chain
was written to.

A pointer into a store that no longer holds the revision is a desk that was
moved without its state directory. It starts cold and says so, rather than
starting cold quietly.

Panes are delivered at least once, so a pane can arrive twice — after a restart,
or after a checkpoint taken between a write and its commit. `stream.Batch.Key`
is stable across both, so recognizing it is all the idempotence the context
needs.

---

## 8. The interface

```go
desk, _ := recall.Open(recall.Config{Name: "support", Every: time.Minute},
    loom.WithRegistry(reg),
    loom.WithStateDir("./state"),
    loom.WithFleetBudget(core.Budget{MaxCostUSD: 20}))
defer desk.Close()

desk.Start(ctx)                                   // ingestion runs from here
n, _ := desk.Post(ctx, messages...)               // or bind any stream.Source
ans, _ := desk.Ask(ctx, recall.Question{Session: "sam", Text: "what is Acme blocked on?"})
```

`Desk.Handler` serves the same thing over HTTP — `POST /ingest`, `POST /ask`,
`GET /context`, `GET /stats`, `GET /updates` (long poll), and a console at `/`
with the living context beside the thread.

`GET /context` returns the living prompt as the bytes a model receives, with the
revision in the headers. A serving layer whose product is a prompt should be
able to show you the prompt.

---

## 9. What is honestly still missing

- **Per-session contexts.** Every query session shares one context. A desk that
  should show different people different subsets of the same conversations needs
  a chain per audience, which the package does not do.
- **Retrieval as a fallback.** When the compact context does not contain the
  answer, a desk says so. It cannot currently go and look at the original
  messages — the digests are lossy and the transcript is not kept.
- **A remote curator.** The curator is a goroutine in one process. Two processes
  serving one desk would both write the chain, and nothing arbitrates that. The
  read side already scales — a query resolves a revision by hash from any
  process sharing the store.
- **Deletion.** Nothing removes a conversation from a context once digested,
  short of starting a new chain.

---

`examples/switchboard` is the whole of this running offline against a
deterministic mock, in about a second.
