# Loom as an inference engine

Loom orchestrates model calls over a dataset. Systems like **vLLM** and
**SGLang** orchestrate model calls over a batch of requests. The two look
unrelated — one is a dataflow framework, the other a GPU serving runtime —
but they are solving the same problem at different altitudes: *many
expensive, variable-latency model invocations that share structure, under a
hard resource ceiling.*

That makes the serving literature directly useful here. This document maps
each of its mechanisms onto Loom, says which ones are implemented, and — for
the ones that aren't — says why not.

## The mapping

| Serving mechanism | What it actually solves | Loom equivalent | Status |
|---|---|---|---|
| **PagedAttention** — KV cache in fixed blocks, deduplicated by content | Memory fragmentation; identical blocks stored once | Content-addressed store: artifacts keyed by hash, stored once, referenced everywhere | ✅ `store` |
| **RadixAttention / automatic prefix caching** — requests sharing a prompt prefix share its KV cache | Recomputing the same prefix for every request in a batch | **Shared prompt prefixes** — `InferSpec.Prefix` rendered once per task and cached provider-side | ✅ `pipeline`, `providers` |
| **Prefix-aware scheduling** — order requests to maximize cache hits | Cache thrash from arbitrary ordering | Tasks of a stage share one prefix by construction, so ordering is already optimal within a stage | ✅ by construction |
| **Continuous batching** — a finished sequence leaves the batch immediately; a waiting one takes its slot | Head-of-line blocking; idle capacity behind a straggler | **Streaming driver** — records flow downstream on completion, stages overlap, one global slot pool | ✅ `loom.WithStreaming` |
| **Chunked prefill / scheduling budget** — admit work against a token budget | Overrunning provider or device limits | Per-model token-bucket admission control (requests/min and tokens/min) | ✅ `runtime` |
| **Speculative decoding** — a cheap draft model proposes, a strong one verifies | Paying frontier-model prices for easy tokens | Escalation ladder: run cheap, escalate on *semantically* invalid output | ✅ `runtime`, `model` |
| **Preemption / priority scheduling** | Fairness and tail latency across tenants | Not implemented — FIFO within the slot pool | ❌ |
| **KV cache eviction (LRU)** | Bounded cache memory | Not implemented — the result cache is unbounded | ❌ |
| **Disaggregated prefill/decode** | Different hardware profiles per phase | No analog — Loom does not own the decode loop | n/a |

Two entries in that table were the gaps this work closed, and they are the
two the serving world talks about most: sharing the prefix, and never
waiting on a barrier.

---

## Shared memory: prefix caching

Loom already had one form of sharing. A **broadcast** registers a value once
per run, stores it by content hash, and hands every task a 64-byte reference
instead of a copy. That solves sharing at the *data* layer: the bytes cross
the process boundary once.

It does nothing for the model. If a rubric is broadcast to a thousand tasks
and each renders it into its prompt, the provider processes that rubric a
thousand times. The bytes were shared; the *computation over them* was not.
This is exactly the gap RadixAttention closes inside a serving engine, and
the remote-provider equivalent is prompt-prefix caching.

### How it works

`InferSpec.Prefix` is a template with **no record data in scope** — only the
broadcast functions. That restriction is the whole mechanism: a template
that cannot see the record cannot vary by record, so every call the stage
issues begins with the same bytes.

```go
Infer("classify", pipeline.InferSpec{
    Binding: model.Binding{Tier: model.TierFast},
    System:  "You classify support tickets.",
    Prefix:  `Rubric:\n{{broadcast "rubric"}}`,   // rendered once per task
    Prompt:  "Classify this ticket: {{.subject}}", // rendered once per record
}, pipeline.WithBroadcast("rubric"))
```

The request carries the two parts separately, and providers send
`System + Prefix + Prompt` in that order:

- **Anthropic** needs an explicit marker: a `cache_control` breakpoint is
  placed at the end of the prefix block. Since the render order is
  tools → system → messages, that one breakpoint caches the entire stable
  head.
- **OpenAI** caches prefixes automatically and needs no marker — only the
  stable leading bytes, which the split guarantees.
- The **mock** provider simulates the same cache, so offline development
  sees the same accounting and the same savings as production.

### Paying for it honestly

A cache entry is not free: writing one costs a premium over a plain input
token, and reads cost a fraction of one. Two calls is roughly break-even;
three or more is a clear win.

So the planner does not enable prefix caching unconditionally. It enables it
when the stage will issue **more than one model call** — the exact condition
under which the write can be earned back:

```go
calls := (len(input) + batch - 1) / batch
env.CachePrefix = calls > 1
```

A single-record stage pays full price and writes nothing. `core.Usage` keeps
the three prompt-token classes disjoint (`InputTokens`, `CacheReadTokens`,
`CacheWriteTokens`) so the run report can state what caching actually cost
and actually saved, including while a fresh write is still unamortized —
`Pricing.Saved` returns a negative number in that window, which is the
truthful reading.

### Keeping the result cache honest

The prefix joins the stage fingerprint, so editing a rubric recomputes
exactly the stages that could have seen it — the same rule broadcasts
follow. And a stage that declares *no* prefix fingerprints exactly as it did
before this feature existed, so adopting prefixes in one stage does not
cold-start every warm cache in the pipeline.

---

## Asynchronous agents: continuous batching

The original driver ran stage barriers: every task of a stage completed
before the next stage began. It is the MapReduce execution model, and it has
MapReduce's failure mode — one straggler idles every worker behind it, and
the p99 of a stage sets the pace for the whole run.

Serving engines abandoned this years ago. **Static batching** meant a batch
ran until its slowest sequence finished; **continuous batching** lets a
finished sequence leave immediately and a waiting one take its slot. The
same change applies here, one level up.

```go
res, err := loom.Run(ctx, p,
    loom.WithStreaming(),          // pipelined execution
    loom.WithBatchWait(25*time.Millisecond),
)
```

### How it works

Every stage gets a goroutine and an input pipe, and they all start at once.
A stage pulls records as they arrive, forms a task, submits it to a **shared
engine**, and forwards each result downstream from the completion callback —
so a record can be three stages deep while its neighbours are still on the
first.

Three design decisions carry the weight:

**One global slot pool, not one per stage.** Concurrency is bounded once,
by the engine. A stage with work waiting can use capacity that a draining
stage isn't — which is the entire point of continuous batching, and
impossible when each stage owns a fixed worker pool.

**Unbounded pipes between stages.** This is deliberate. A bounded pipe would
let a full downstream queue block an upstream task that is *holding an
execution slot*, while the downstream stage waits for a slot to free — a
deadlock that only appears under load. Unbounded buffering makes forwarding
total: a stage can always hand off a record and release its slot. The memory
ceiling is the records in flight, which the barrier driver materializes in
full anyway.

**A batch deadline.** A stage with `WithBatchSize(8)` takes whatever records
are queued, but waits at most `BatchWait` for the group to fill. Without a
deadline, the tail of a stream would wait forever for peers that will never
arrive — batching would have reintroduced the barrier it was meant to remove.

### What stays a barrier

`Combine` and `ReduceAI` buffer their input until upstream closes. That is
not a limitation of the driver but of the operators: an aggregate over a set
cannot begin before the set is known. Inside a `ReduceAI`, each tree level is
a barrier while the tasks within it run concurrently against the shared
engine.

### What it costs

Records flow in **completion order**, not input order. A stage's outputs no
longer line up positionally with its inputs. This is the same trade serving
engines make — completion order is not arrival order — and it is why the
barrier driver remains the default. `StageOutputs` is still sorted by
submission order for reproducibility, but downstream stages observe records
as they finish.

Everything else is identical: same planner, same envelopes, same cache keys,
same recovery. Both drivers execute tasks through the same
`Scheduler.RunTask`, so retry, escalation, admission control, and the budget
governor cannot drift between them.

---

## What is deliberately not here

**Preemption and priority.** Serving engines preempt low-priority sequences
to bound tail latency across tenants. Loom's slot pool is FIFO. This matters
for a multi-tenant deployment and not much for a single run, so it belongs
with the remote-executor work, where a shared admission service is the right
place to enforce it.

**Cache eviction.** The result cache is unbounded, which is correct for a
run and wrong for a long-lived worker fleet. Bounding it means an eviction
policy, and an LRU over content-addressed blocks is the obvious one.

**Semantic caching.** Prefix caching is exact-match by construction.
Embedding-similarity lookup in front of the exact cache would catch
near-duplicate inputs — genuinely useful, and genuinely a different
correctness argument, since a near-miss is not a hit.

**Speculative execution of stragglers.** Serving engines re-issue slow
requests. Loom has the retry machinery to do this but no straggler
detector; the streaming driver makes it worth adding, since a straggler now
delays only its own record rather than its whole stage.

See [ARCHITECTURE.md](./ARCHITECTURE.md#6-scaling-path-from-local-runtime-to-distributed-system)
for how these fit the broader scaling path.
