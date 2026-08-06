# Long-term memory

Loom's third and longest-lived sharing mechanism: a durable knowledge base that
outlives the run that wrote it, is retrieved by meaning rather than by name, and
is shared between the pipelines of an application.

This document explains why the two mechanisms that already existed do not cover
it, the one hard problem a mutable store creates for a framework built on
content-addressed replay, and the two ideas that solve it.

---

## 1. What was missing

Loom could already share state two ways, and both are short-lived:

| | Broadcast | Blackboard topic | Memory |
|---|---|---|---|
| Lifetime | one run | one process (a `Fleet`) | indefinite |
| Addressed by | name | name | **similarity** |
| Written by | the caller, before the run | an agent, between runs | **a stage, during a run** |
| Read as | the whole value | the whole topic | **the top-k** |
| Size | fits in a prompt | fits in a prompt | **does not** |

The gaps compound. A broadcast is registered before the run and dies with it, so
nothing a run concludes can inform the next one. A blackboard reaches across
agents but not across processes, so an application restarted on Monday knows
nothing it learned on Friday. And both are read *whole*, which is only viable
while the shared thing is small — a taxonomy, a rubric, a lookup table. Months
of accumulated conclusions are not small, and the reader does not know which
part of them it needs until it has the record in front of it.

That last point is the one that forces the shape. Retrieval by meaning is not a
convenience on top of a bigger broadcast; it is the only way to read something
larger than the context window.

## 2. The hard part

Loom's cache is also its checkpoint. A task's result is keyed by
`(op fingerprint, input content)`, and replaying that result must be
indistinguishable from re-running it. `ARCHITECTURE.md` §4.7 spells out why
broadcasts are safe to combine with it:

> Two properties make them safe to combine with caching: the value's hash
> participates in the reading stage's fingerprint … and the value is immutable
> for the run's lifetime, so replaying a cached result cannot observe a
> different world than the original execution did.

A knowledge base breaks both. It is mutable by construction — the point is that
it grows — and it is far too large to hash into a fingerprint on every read.
`fleet.go` states the second half of the same constraint from the other
direction:

> a task that could publish mid-run would make its own cached result depend on
> execution order, which is exactly what content-addressed replay assumes away.

So the design has to answer: **how does a mutable, durable, semantically
retrieved store coexist with deterministic replay, least privilege, lineage, and
remote execution?**

Two mechanisms, and the second is the one that makes the first affordable.

## 3. Epochs

A memory **space** carries a monotonic **epoch**.

- A run **pins** each space's epoch before its first task. Every read is served
  *as of* that epoch, so a commit landing mid-run — from another process
  sharing the store, or from a sibling agent on the same fleet — is invisible
  to it, however long it runs.
- Writes are **staged**. `Remember` stages an item; `Commit` publishes the
  staged set as a new epoch. Nothing a run writes is visible to the run that
  wrote it.

Immutability-for-the-run is restored without freezing the store forever, and the
staging rule is the blackboard's rule — publish *between* units of work, never
inside one — generalized from a process to a durable store.

The pinned epoch joins the fingerprint of every stage that reads the space, so
the epoch is a correctness boundary and not just bookkeeping.

```
run A pins kb@4 ─────────── reads kb@4 ─────────── reads kb@4 ─── commits kb@5
                                  ▲
       run B commits kb@5 ────────┘  (invisible to run A)
```

## 4. Recall-keyed caching

Pinning alone would be correct and useless.

The epoch is in the fingerprint of every stage that reads the space, so a single
commit would cold-start every reading stage in the application. A knowledge base
that grows daily would never see a cache hit again — and the whole economic
argument for caching AI work is that identical work is never paid for twice.

So **retrieval is its own operation**:

```go
src.
    Recall("history", pipeline.RecallSpec{Space: "kb", Query: "{{.subject}}", K: 5}).
    Infer("answer", pipeline.InferSpec{
        Binding: model.Binding{Tier: model.TierBalanced},
        Prompt:  "context:\n{{.memory}}\n\nquestion: {{.subject}}",
    })
```

`Recall` writes what it found into the record: the rendered items into `memory`,
and **their content-addressed IDs into `memory_ids`**. That second field is the
whole trick. The `Infer` stage below is keyed, as every stage is, by its own
fingerprint plus its input record content — and that content now names exactly
what was retrieved.

The result:

| What changed | Recall stage | Infer stage |
|---|---|---|
| Nothing | cached | cached |
| A commit that does not move a record's top-k | recomputes (cheap) | **cached** |
| A commit that moves a record's top-k | recomputes | recomputes, **for that record only** |
| The query template, K, filter, or embedder | recomputes | recomputes |

Commit ten thousand items and the queries whose neighbourhoods did not move
replay for free. The expensive half — the model call — is invalidated at
**record granularity**, by content, and the cheap half — one embedding and one
index lookup — absorbs the epoch.

None of this is new machinery. It is the existing content-addressed key doing
its job on a record that now says what it read. That is the argument for
splitting recall from inference, and it is why memory is a *stage* rather than
a tool the model may call: a tool call's result never reaches a cache key, its
cost never reaches a projection, its access is never checked against an
envelope, and what it returned is never recorded in lineage.

`TestRecallKeyedCacheInvalidation` in `memory_test.go` is this table, executed.

## 5. Provenance

Every item records the run, stage, task, model, and op fingerprint that produced
it, stamped by the executor rather than by the op — so it cannot be forged by
one and cannot be forgotten by another.

This is not an audit nicety. A knowledge base whose entries are model outputs is
a laundering channel for hallucination unless a later reader can tell where each
entry came from: the second run cannot otherwise distinguish a fact it retrieved
from a fact the first run invented. It is the same principle
`store.LineageEntry` applies to artifacts — *provenance for outputs whose
provenance would otherwise be "a model said so"* — asked about a fact instead of
a file.

The loop closes because `Recall` puts item IDs in the record: lineage already
links a produced artifact to the memory it saw, and each item back to the run
that wrote it.

Items are content-addressed on `(space, text, metadata)` — and on nothing else,
so `Created` and `Source` stay outside the hash. Writing the same fact twice is
idempotent and free, which matters more here than anywhere else in Loom: a
knowledge base is fed by every run of every pipeline pointed at it, and the same
conclusion will be reached again and again.

## 6. Least privilege

Spaces are named partitions, and reads and writes are separate capabilities:

```
memory:read:support-history
memory:write:support-history
```

In a knowledge base shared across an application that separation is the point:
nearly every stage should be able to consult what the organization knows, and
very few should be able to add to it. A single `memory:<space>` grant would make
every reader an author.

The planner assembles the grants as the default output of planning, the way it
already does for a stage's binding models:

- a `Recall` or `Remember` stage grants **its own space**, because the spec
  already names it;
- `pipeline.WithMemory` / `WithMemoryWrite` are the escape hatch for stages that
  reach memory through the session (`MapTools`);
- a stage that touches memory earns the **embedder's secret and endpoint**, and
  only such a stage does — the rest of the pipeline cannot reach the knowledge
  base or the embedding API at all.

The envelope carries `Memory map[string]uint64` — space to pinned epoch. Like
broadcasts, that is a *reference*: a knowledge base cannot travel in an
envelope, but the one number that says which version to read can, which is what
keeps a task reading months of accumulated knowledge as shippable to a remote
worker as one reading nothing.

## 7. Backends

`memory.Store` is the seam, exactly as `model.Provider` is for completions.

| Backend | What it is | Use it when |
|---|---|---|
| `memory.InMemory` | Exact cosine over a scan, zero dependencies, JSONL journal | Tests, development, single-node, up to ~10⁵ items |
| `memory/chromem` | [chromem-go](https://github.com/philippgille/chromem-go) — embedded pure-Go vector DB, no CGO, no server | The default for a real application: a `go get` and a directory, into the hundreds of thousands of documents |
| *(your adapter)* | Qdrant (`qdrant/go-client`), pgvector (`pgx` + `pgvector-go`), Milvus, Weaviate | The corpus outgrows one process, or you want filtering/quantization/replication you do not want to own |

**chromem-go is the recommended starting point**, for the same reason
`store.CAS` is memory-first with optional persistence: it is a library, not a
server. No daemon, no container, no operational surface. If you already run
Postgres, pgvector is the pragmatic alternative and costs you nothing new to
operate; if the corpus is genuinely large, Qdrant has the best Go client of the
hosted engines and maps onto this interface directly.

Two things the adapter adds that chromem-go does not have, and that any new
backend must also provide: **epochs** (each document carries the epoch it became
visible at, with the current epoch in a sidecar) and **staging** (writes held
until `Commit`).

> The chromem-go adapter creates its collection with an embedding function that
> **refuses**. Reaching it would mean a document arrived without a vector, and
> chromem-go's default embedding function calls OpenAI — outside the task's
> budget, egress allowlist, and audit log. Failing loudly is the only acceptable
> behaviour there.

### Embedders

`memory.Embedder` resolves its credential per call through the task's secret
broker and declares its endpoint for the egress allowlist — the same contract
`model.Provider` follows, for the same reason.

- **`memory.HashEmbedder`** — deterministic, offline, zero dependencies. The
  memory package's `model.Mock`: it is what makes the mechanism testable and
  developable without a network, a key, or a bill. It measures *lexical
  overlap, not meaning* — "car" and "automobile" are orthogonal to it — so use
  it for tests and examples and nothing whose recall quality matters.
- **`providers/openai.NewEmbedder`** — `text-embedding-3-small` by default, with
  the `dimensions` parameter exposed so an index can be narrowed to trade a
  little recall quality for a lot of store size. Its usage flows into the
  governor and the run report like a completion's does: embedding a large corpus
  is real money, and a budget that ignored it would not be a budget.

Anthropic publishes no first-party embeddings endpoint; its documented
recommendation is a third-party embedding provider, so an Anthropic-model
pipeline pairs a Claude binding with an OpenAI (or other) embedder. Implementing
`memory.Embedder` for one is about forty lines — see
`providers/openai/embed.go`.

The embedder's **name is in every recall stage's fingerprint**: the same query
under a different embedder has different neighbours, so results computed under
one must not be replayed for another.

## 8. Using it

```go
store, err := chromem.Open("./state/kb", false)   // or memory.NewInMemory(dir)
if err != nil { ... }
defer store.Close()

p := pipeline.New("support")
tickets := p.FromRecords("tickets", incoming)

answered := tickets.
    Recall("similar", pipeline.RecallSpec{
        Space:    "resolutions",
        Query:    "{{.subject}}\n{{.body}}",
        K:        5,
        Filter:   map[string]string{"product": "{{.product}}"},
        MinScore: 0.35,
    }).
    Infer("draft", pipeline.InferSpec{
        Binding: model.Binding{Tier: model.TierBalanced, Escalation: []string{"deep"}},
        Prefix:  "You answer support tickets. Cite the [n] you used.",
        Prompt:  "past resolutions:\n{{.memory}}\n\nticket: {{.subject}}\n{{.body}}",
    })

// What this run concludes, tomorrow's run recalls.
answered.Remember("learn", pipeline.RememberSpec{
    Space: "resolutions",
    Text:  "{{.subject}} → {{.output}}",
    Meta:  map[string]string{"product": "{{.product}}"},
})

res, err := loom.Run(ctx, p,
    loom.WithRegistry(reg),
    loom.WithMemory(store, openai.NewEmbedder("", 512, "")),
    loom.WithSecrets(map[security.SecretRef]string{"openai_api_key": key}),
    loom.WithStateDir("./state"),
)
// res.Memory    — the epoch each space was read at
// res.Committed — the epoch each space reached
```

Note what is *not* in that pipeline: no vector-store client, no epoch handling,
no cache-key management, no grant plumbing. The store is configuration; the
retrieval is a stage.

### Commit discipline

- **`loom.Run`** commits what it staged when it finishes. It is the only writer
  its store can see, so there is no ambiguity.
- **`Fleet`** agents do not. Several agents share one store and one staging
  area, so a commit fired when one agent happens to finish would publish
  another's work in progress and split a single run's output across two epochs.
  The fleet's owner calls `Fleet.CommitMemory` between waves, exactly as it
  calls `Post`.
- **`loom.WithoutMemoryCommit`** leaves a run's writes staged: for the run that
  may read the knowledge base without being trusted to extend it — an
  evaluation, a dry run, a backfill pending review.

A `Remember` stage is cacheable, which means a rerun over unchanged inputs does
**not** re-stage what a previous run already committed. That is deliberate — a
nightly pipeline over an unchanged corpus should not re-stage its whole output
every night — and it is safe because items are content-addressed and the write
is idempotent: the knowledge base converges to the state it would have reached
anyway. Use `pipeline.WithNoCache()` on a stage whose writes must be attempted
on every run regardless.

## 9. What `Explain` can and cannot tell you

`loom.Explain` prices a run before it happens, and long-term memory adds two
things it cannot know, both understating cost — the one direction a projection
must never be quietly wrong in. Both are reported rather than papered over,
which is the same principle `ParseJSON` follows:

1. **Embedding calls are counted but not priced.** An embedder is not a registry
   model, so there is no entry to price it from.
2. **The retrieved text is unknowable without issuing the queries**, so the
   prompts of stages below a `Recall` are projected without the context it would
   have supplied.

The stages *below* a `Recall` are therefore marked estimated — the recall's own
counts are exact, one embedding per record, and it is the records it hands
downstream whose shape is not, which is the same treatment and the same
reasoning `ParseJSON` gets. The run's ceiling stops claiming to be a bound, and
`loom.WithStageSample("recall-stage", map[string]any{"memory": "…"})` restores
exactness by naming what a typical recall returns.

## 10. Limits, and what comes next

- **`memory.InMemory` scans.** Exact, untunable, dependency-free, and wrong
  above ~10⁵ items. Move to `memory/chromem` or a hosted index; nothing above
  the `Store` interface changes.
- **The chromem-go adapter's stale-pin path over-fetches.** chromem-go's
  metadata filter is string equality and visibility is an inequality, so a query
  whose pin has fallen behind the current epoch widens its fetch until it has
  `K` survivors. Correct, and slower — but only on the rare path, since a run
  pins at launch and usually reads the epoch that was current then.
- **No forgetting.** Items are never deleted or superseded. A long-running
  knowledge base needs both — a retention policy, and a way to mark a fact
  obsolete when a later run contradicts it — and neither exists yet. Today the
  honest workaround is metadata (`{"valid_until": …}`) plus a filter.
- **No reranking.** Retrieval is a single vector search. A cross-encoder rerank
  over an over-fetched candidate set is the standard next quality step and fits
  the `Recall` stage without changing anything around it.
- **No hybrid search.** Dense vectors only; BM25 alongside them measurably helps
  on keyword-ish queries, and is a `Store` implementation detail.
- **`ARCHITECTURE.md` §6 lists semantic caching** — embedding-similarity lookup
  in front of the exact result cache — as a roadmap item. It is now one step
  away: the embedder and store are wired, and what remains is a similarity
  lookup keyed on task input rather than on a user query.
