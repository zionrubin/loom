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
| **Priority scheduling** — order work so short jobs are not stuck behind long ones | Fairness and completion time across tenants | **Program-fair admission** — `runtime.Pool` admits a contended slot to the agent with the least attained service | ✅ `runtime/pool.go` |
| **Preemption** — evict work already running | Tail latency under contention | Not implemented — a task that has been admitted runs to completion | ❌ |
| **KV cache eviction (LRU)** | Bounded cache memory | Not implemented — the result cache is unbounded | ❌ |
| **Running the engine yourself** — the model on your own device | Paying by the token; sending records to a vendor | **Local providers** — `providers/llamacpp` over a llama.cpp server, with the device's decode width as the admission ceiling | ✅ `providers/llamacpp` |
| **Disaggregated prefill/decode** | Different hardware profiles per phase | No analog — Loom does not own the decode loop | n/a |

Two entries in that table were the gaps the prefix/streaming work closed, and
they are the two the serving world talks about most: sharing the prefix, and
never waiting on a barrier.

A third — priority scheduling — was closed later, and it belongs to a different
altitude. It only means anything once more than one pipeline is in flight, and
[ASYNC.md](./ASYNC.md) is about that level: the program rather than the call as
the unit of scheduling, one quota and one ceiling across concurrent agents, and
the blackboard they use to reach each other's conclusions.

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

## When the inference engine is yours

Everything above treats the serving engine as somebody else's: Loom issues a
call, a vendor's fleet decodes it, and a bill arrives. `providers/llamacpp`
removes the vendor. Point it at a `llama-server` and the model runs on your
hardware, behind the same `model.Provider` seam as an API — which is the
interesting part, because *nothing in a pipeline changes*. A binding names a
model, not a machine.

```go
reg := model.NewRegistry()
props, err := llamacpp.Register(ctx, reg,
    llamacpp.New("http://127.0.0.1:8080"), "local-fast", model.TierFast)
```

What changes is the envelope around the call, and each change is a
simplification rather than a special case.

### The ceiling stops being a rate and becomes a width

A hosted model meters how fast you may ask over a minute. A model on your own
device has some fixed number of sequences it can decode at once — llama.cpp's
slots, a serving engine's batch width — and asking faster than that does not
fail, which is the problem. The excess queues *inside* the server, where the
scheduler can neither see it nor schedule around it: latency inflates, the
run report attributes the wait to the model call, and admission control is
quietly bypassed.

So `model.Limits` gained a third dimension next to the two token buckets:

```go
Limits{MaxConcurrent: props.Slots}   // discovered, not guessed
```

`RateLimiter.Acquire` now returns the release for what an admission holds, and
the scheduler holds it across the call — so the bound is on calls actually in
flight rather than calls dispatched, and a backoff sleep between attempts
gives the slot back instead of sitting on it. The rate buckets are drawn on
*before* the slot, because a request holding a scarce device slot while
waiting on a per-minute quota idles the device.

`llamacpp.Register` reads the number from `/props` rather than accepting one
from a config file: the server knows its own decode width, and it is the kind
of number that goes stale the first time somebody changes `--parallel`.

### Cost is zero, which changes which bound matters

The dollar governor is the ceiling that matters for hosted work, and against a
local model it is inert — `Pricing` is left zero because zero is the true
marginal cost of a token you generate yourself. That is not a gap to paper
over with an invented rate; it is the point. What still binds is the device
(above), the token budget, and wall-clock.

Tokens are still counted. Free is not the same as unmeasured, and a run report
that went silent because nothing was billable would be less useful than the
bill it replaced.

### The prompt cache stops being a metaphor

`InferSpec.Prefix` exists so a provider's prompt cache can serve the
stage-stable head instead of reprocessing it per record — RadixAttention's
benefit, reached from outside. Against a llama.cpp server there is no reaching
from outside: the cache **is** the KV cache, reused across requests whose
prompts share a prefix.

That collapses the break-even rule. The planner enables `CachePrefix` only for
stages issuing more than one call, because a remote cache *write* costs a
premium over a plain input token and needs a second call to earn it back. A
local KV cache write costs nothing — it is a byproduct of the forward pass the
model was making anyway — so the adapter asks for reuse unconditionally, and
`CacheWriteTokens` is always zero because there is no write to amortize.
Reused tokens still come back as `CacheReadTokens`, so the report reads the
same as it does against a hosted model, priced at the zero it actually cost.

One consequence for authors: the adapter joins `System` and `Prefix` into a
single system message, where the hosted adapters send two. A GGUF ships
whatever chat template its author wrote and plenty of them accept only one
leading system turn. Both halves are stage-stable, so their concatenation is
too — and identical leading bytes across a stage's calls is the entire
requirement.

### Two rungs, both local

A llama.cpp server loads one model, so a deployment wanting a fast model and a
strong one runs two servers on two ports. That makes an escalation ladder
ordinary rather than special:

```go
Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"local-deep"}}
```

This table's speculative-decoding row said escalation is the program-level
form of "cheap model proposes, strong model verifies". With both rungs local,
it is that shape with the vendor removed entirely — and the verification is
`Validate`, a semantic gate the author wrote, rather than token agreement.

### Least privilege gets easier, not harder

Two properties fall out, and both are worth stating because the naive
implementation loses them:

**Endpoint is loopback, not empty.** A local provider could report `""` — the
in-process, always-allowed answer — and the temptation is real, since nothing
is leaving the machine. Reporting `127.0.0.1` instead is what puts it on the
stage's egress allowlist, where the executor checks it before every call. The
envelope then *states* that this stage's records cannot reach a vendor, and
the statement is enforced rather than asserted.

**No secret exists to leak.** `SecretRef` is empty for a plain local server,
so the planner grants no secret capability at all. A stage bound to a local
model is not a stage trusted with a key it happens not to use; it is a stage
with no key in its envelope. (A server started with `--api-key` is covered by
`llamacpp.WithAuth` and is then a broker-resolved secret like any other.)

Together those make the mixed deployment expressible in the ordinary way:
stages that touch personal data bind to a local model and are planned unable
to egress, while a downstream stage over redacted or aggregated records binds
to a frontier model and carries the key. The boundary is a binding, and the
envelope is the proof.

[`examples/on-device`](../examples/on-device) is all of it running offline
against real loopback servers, and prints each of these as a number the run
produced.

---

## What is deliberately not here

**Preemption.** Serving engines preempt low-priority sequences mid-flight to
bound tail latency. Loom's unit of admission is a whole model call, and
cancelling one wastes what it has already spent — so fairness is enforced
*between* calls instead, which bounds waiting rather than tail latency. Priority
between concurrent pipelines is implemented (see
[ASYNC.md](./ASYNC.md#admitting-slots-by-attained-service)); preemption within
one is not.

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
