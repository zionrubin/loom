# longterm — a support desk that remembers

The proving example for [long-term memory](../../docs/MEMORY.md): a support
desk that is measurably better on Tuesday because of what it worked out on
Monday.

```
go run ./examples/longterm                       # offline, no key, no network
go run ./examples/longterm -backend chromem      # the embedded vector DB
go run ./examples/longterm -state /tmp/loom-kb   # keep the knowledge base
```

## Why this workload

Loom's other sharing mechanisms cannot express it. A **broadcast** is fixed
before the run and dies with it, so nothing a run concludes can inform the next
one. A fleet's **blackboard** reaches across agents but not across processes.
What a support desk needs is neither: a knowledge base that outlives every run,
is too large to put in a prompt, and is therefore read by similarity rather than
by name.

## The pipeline

```
tickets    the day's incoming tickets
similar    Recall: the k nearest past resolutions, as of the pinned epoch
draft      Infer: answer the ticket using what was recalled
learn      Remember: stage today's resolution for tomorrow's epoch
```

Three days run against one store, then day 3 is answered three more times
without writing anything back.

## What to watch

**Day 1 recalls nothing; day 3 recalls from both.** The knowledge base starts
empty and the answers say so. The runs in between wrote what they concluded.

**Nothing a run writes is visible to the run that wrote it.** Each day reads at
the epoch pinned before its first task and its own writes land in the next one.
The epoch moves *between* days and never *within* one — which is what stops a
task's cached result from depending on when it happened to execute.

**Provenance.** Every item names the run, stage, and task that produced it. A
knowledge base of model outputs is a hallucination laundering channel without
it: the next run cannot otherwise tell a fact it retrieved from a fact the last
run invented.

**The last three sections are the design's central claim, executed:**

| Pass | Model calls | Cache hits |
|---|---|---|
| Cold (read-only shape, new cache keys) | 3 | 0 |
| Again, knowledge base unchanged | **0** | 6 |
| Again, after committing one billing fact | **1** | 2 |

That last row is the whole argument for making retrieval a stage of its own.
The commit moves the epoch, so **all three** recalls recompute — the pinned
epoch is in their fingerprints. But `Recall` writes the retrieved item IDs into
the record, so the `Infer` below inherits them in its ordinary cache key, and
only the one ticket whose retrieved set actually changed pays for a model call.
The new fact is in the `billing` product and the stage filters by product, so
which ticket that is depends on nothing about how the embedder ranks anything.

Add ten thousand items to a knowledge base and the queries whose neighbourhoods
did not move replay for free. Without the split, every commit would cold-start
every inference ever computed from the store.

## Caveats

The model and the embedder are both deterministic and offline. `memory.HashEmbedder`
measures **lexical overlap, not meaning** — "car" and "automobile" are
orthogonal to it — which is enough to demonstrate the mechanism and not enough
to judge recall quality. Use `providers/openai.NewEmbedder` for that.

The tickets and resolutions are fixtures invented for this example.
