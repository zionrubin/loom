# Stateful delta execution: processing what changed, and proving it was safe

[FINDINGS.md](./FINDINGS.md) is about two agents wanting the same fact.
[ITERATION.md](./ITERATION.md) is about one agent going round a loop until it
stops learning. This document is about the thing they both leave on the floor:
an agent going round a loop while carrying a context that grows a little each
time, where every round pays for the whole of it.

Loom already refuses to redo three kinds of work:

```
result cache        same computation?         don't execute it again
findings commons    same question?            don't research it again
prompt prefix       same head of a prompt?    don't prefill it again
```

There is a fourth, and nothing was avoiding it:

```
delta state         same evolving object?     don't PROCESS it again
```

*Processing* here means everything between deciding to use a context and
handing it to a provider: getting it out of storage, rendering it into bytes,
shipping those bytes to whichever process will make the call. All three are
linear in the whole context. The change is two per cent of it.

---

## 1. Where the idea comes from

TokTier (arXiv 2607.29678) makes this argument about tokenization specifically.
Its measurements of agent traces are the shape of the problem: a median call
appends about 1.4K characters to a context of 86K–123K tokens, and only 1–3.6%
of calls start or rebuild a session. With a warm KV cache the model may barely
process the old prefix — and the tokenizer still scans the entire transcript
every time.

Its answer is not "tokenize faster". It is to keep the previous token state,
tokenize only the changed region plus a small repair window, and **splice** —
after checking. The check matters because tokenization is not compositional:
`tok(A) + tok(B)` need not equal `tok(A+B)`, since appending text can move token
boundaries in the old suffix. So TokTier stores token IDs with their source
spans, retokenizes around the seam, and accepts the splice only once it has
found a stable boundary. If it cannot find one it widens the repair region, and
in the limit it does a full reference tokenization. A failed fast path costs
more work and never a different answer.

Around that sits a guarantee ladder worth copying verbatim:

```
family-level argument  →  per-request certificate  →  runtime shadow
                                                      verification  →  exact fallback
```

Plus version pinning by content hash, session state kept cheaper and longer than
KV state, and worker affinity used as an optimization rather than a requirement.

Two things are worth saying plainly before any of this is mapped onto Loom.

**The headline number is not transferable.** TokTier's 1,821 req/s against 40
req/s mixes hardware and establishes system capacity, not equal-resource
efficiency; the authors say so. The defensible signal is the engine-in-loop
result — median TTFT down 16–34% in loaded vLLM regimes — and the fact that the
optimization gets better as the context grows relative to the append.

**The GPU tokenizer is not the interesting part for Loom.** It is
tokenizer-family-specific, carries a real certification burden, and pays off
most when you control the inference engine. Loom mostly does not. What
transfers is the *pattern*, and the pattern is worth more than the kernel.

---

## 2. What Loom does with it

The pattern, stated generally:

> When a computation repeatedly processes a large state that changed only a
> little, keep the previous state, compute the delta, certify that reuse was
> safe, and fall back to full recomputation whenever you cannot.

The first thing in Loom shaped like that is not tokenization. It is **context
materialization** — the step where a stage's context becomes the bytes of a
prompt — and it has the same non-compositionality for the same kind of reason.
Rendering is not a concatenation: a format with a closing wrapper moves that
wrapper when you append, a format with a numbered header rewrites its front, a
truncation policy drops the head when the tail grows. `render(A) + render(B)`
need not equal `render(A+B)` any more than tokenization composes.

So the mechanism is the same mechanism, one level up:

```
                 immutable logical state in the CAS
                              S₀
                              │
                       Δ1 ────┤
                              ▼
                              S₁
                              │
                       Δ2 ────┤
                              ▼
                              S₂

executing S₂:

     state for S₁ resident in this process?
                 /                    \
               yes                     no
                │                       │
        change small enough?        rebuild: render
           /          \              every segment
        yes            no                 │
         │              │                 │
      splice        rebuild               │
         │                                │
   certificate ────────────────────────▶ exact bytes
         │
   sampled shadow check against a full render
```

Every path ends at the same bytes. That is the property everything else rests
on, and it is why a killed worker costs latency rather than an answer.

---

## 3. The five pieces

### Continuation identity

`delta.Ref` names one revision of one evolving object: a key, a content hash, a
parent hash, and counts. `task.ContextBundle` grew a `Chain delta.Ref`, so an
envelope carries a couple of hundred bytes where it would otherwise carry the
transcript. It is the same indirection `store.Broadcasts` uses, applied to a
value that changes.

`model.Request` grew a `Continuation delta.Hint`. It is strictly an optimization
channel: the prompt on a request is always the whole prompt, because a request
that only made sense next to state somebody else was keeping would stop being
replayable, cacheable, or safe to retry against a different provider. What the
hint adds is `Stable` — the count of leading bytes certified identical to the
parent revision's rendering, which is exactly what a KV cache needs to know how
much of its work it may keep.

### The state store

`delta.Chain` writes revisions into the CAS; each holds the segments it *added*
and the hash of the revision it added them to. Writing is constant; reading from
cold is linear. That asymmetry is the design.

`delta.State` is a rendered context plus the evidence needed to extend it
without re-reading it: the segments and their identities, the span of each in
the output, and a chained hash where `Seams[i]` covers everything before segment
`i`. The seam chain is the load-bearing part — without it, a certificate
claiming 400 KB was retained unchanged could only be checked by comparing 400 KB,
which would cost more than the rebuild it was avoiding.

A resident state costs roughly twice its context in memory: the bytes a model
will see and the segments they were rendered from. That is the price of
extending it without a trip to storage, it is bounded by a byte ceiling, and
exceeding the ceiling costs a rebuild rather than an error.

### Soft worker affinity

`worker.Submission` grew an `Affinity{Key, Grace}`; `worker.Claim` grew
`Resident []string`. The distinction from `Requirements` is the whole point:

```
Requirements   HARD.  A worker that cannot serve them must not claim the task,
                      because claiming it means failing it.
Affinity       SOFT.  A worker that does not hold the state runs the task
                      correctly and more slowly.
```

Encoding "prefers worker A" as a requirement would make a task unclaimable the
moment A died — the exact failure locality exists to survive. So the queue does
two passes over pending work: state it holds first, everything else second. A
grace window can hold keyed work back from workers without its state, briefly
and optionally; after it, the task goes to whoever asks. Never waiting forever is
a property, not a policy.

Both queue implementations do this identically, and the conformance suite in
`worker/queuetest` asserts all three halves: that affinity is preferred, that it
never blocks, and that the grace expires.

### Delta-aware routing

`delta.Policy` decides splice or rebuild from the change size, the change-to-
context ratio, and whether any ancestor is resident. Incremental is not
universally better — TokTier finds a tail above roughly 30K appended characters
where full GPU recomputation wins — so the defaults (32 KiB, 50%) are a starting
point drawn from someone else's measurements and want re-measuring on yours. The
one thing a bad policy cannot get wrong is the answer.

Every route reports its reason in words, because a report that cannot
distinguish "this worker has never seen this session" from "this append was half
the context" cannot say whether the fleet is misrouting work or the policy is
doing its job.

### Certified fast paths

This is the part worth having independently of contexts.

A `delta.Certificate` records what was assumed, what was checked, and what it
cost. What it establishes, checkably and in time proportional to the change:

> the resulting bytes are the parent's first `Retained` bytes followed by
> `Repaired` bytes freshly rendered — and the `Agreed` segments immediately
> before that boundary, re-rendered from scratch under the new list, came out
> byte-identical to what the parent already held.

What it does *not* establish is that a full render would have produced these
bytes. Nothing proportional to the change could: reaching the segments outside
the window means reading them, and reading them is the rebuild being avoided.
Saying so precisely is what makes the next rung necessary rather than
decorative.

---

## 4. The guarantee ladder, as implemented

| rung | what it is | what it catches | what it costs |
|---|---|---|---|
| family argument | `Renderer.Lookahead()` declares how far a change reaches back; `-1` means never splice | formats whose output is not local — headers with counts, tables of contents | nothing; they are simply always rebuilt |
| per-splice evidence | the repair window is re-rendered and must agree with the parent across the overlap | a renderer that understates its lookahead — the window widens and reports it | the window, on every splice |
| certificate | `Verify` re-derives the splice's arithmetic against the states it names | tampering, spans that do not tile, a digest from elsewhere, a parent it did not splice from | O(change), so it runs on **every** splice, not a sample |
| shadow verification | a sampled fraction of accepted splices are resolved from the chain and rendered in full, then compared | the family argument being wrong — a renderer changing bytes outside any window | one full render per sample |
| exact fallback | a state miss, an oversized change, a quarantined renderer and an unprovable seam all route to the same full render | everything else | the render that was being avoided |

A divergence is not recoverable and is not treated as one. The renderer is
quarantined for the life of the process, every later context is rendered in
full, the caller is handed the reference result, and an event says so once.

Widening the repair window far enough *is* the full render — there is one code
path here and two regimes of it — which is the strongest available statement
that a failed fast path cannot cost correctness.

The version pin is the same discipline TokTier applies to tokenizers: a
renderer's version seeds the seam chain and is stored in every revision, so a
chain written by one renderer cannot be materialized by another, and a cached
state from one cannot be spliced onto by another. Two renderers reporting the
same version must produce identical bytes for identical input. Changing the
output means changing the version.

---

## 5. Using it

```go
// Write the session. This is the only place the transcript exists.
cas, _ := store.NewCAS(stateDir + "/cas")
chain, _ := delta.NewChain(cas, delta.Tags{}, "session/abc")
ref, _ := chain.Root(delta.Segment{Name: "brief", Body: brief})

// Each round appends and runs. Nothing else about the pipeline changes.
for _, turn := range turns {
    ref, _ = chain.Append(ref, delta.Segment{Name: "turn", Body: turn})
    loom.Run(ctx, p,
        loom.WithStateDir(stateDir),
        loom.WithContinuation("session", ref))
}
```

and the stage declares it:

```go
src.Infer("reply", pipeline.InferSpec{...}, pipeline.WithContinuation("session"))
```

On a fleet, add `loom.WithAffinity(200*time.Millisecond)` and start workers with
`loom.Serve`. A worker is handed the pipeline and no revision: revisions arrive
on envelopes, one per round, and a worker told one at startup would be holding a
fact that is stale before its first claim.

The revision's hash joins the fingerprint of every stage that reads it, so the
result cache invalidates exactly the round that changed and leaves every earlier
round warm — the same treatment a changed broadcast gets.

---

## 6. What it is worth

From `examples/delta-session`, a 400-turn support thread growing by one message
per round, across two worker processes, with the state-holder killed halfway:

```
round   appended    context   worker   route     stable     prompt
    1      974 B   612.4 kB   w1       rebuild     0.0%   619.7 kB
    2      974 B   613.4 kB   w1       splice     99.7%   620.6 kB
    3      974 B   614.3 kB   w1       splice     99.7%   621.6 kB
    4      974 B   615.3 kB   w1       splice     99.7%   622.6 kB
        ── w1 killed (SIGKILL) while holding this session's state ──
    5      974 B   616.2 kB   w2       rebuild     0.0%   623.5 kB
    6      974 B   617.2 kB   w2       splice     99.7%   624.5 kB

what crossed the queue, per round
  envelope referencing the session      212 B
  the session itself                 617.2 kB
  ratio                                  2981×

every round matches a single process doing the whole session by itself.
```

Three numbers, and none of them is a speedup claim:

- **2981×** less carried per task, which is exact arithmetic on what the queue
  actually holds.
- **99.7%** of each prompt certified byte-identical to the previous round's,
  which is what a provider's prefix cache can act on.
- **one round** of extra latency for a SIGKILL to the process holding the
  session — a rebuild, taken automatically, reported as one.

What is deliberately not claimed: end-to-end throughput. Assembling a
contiguous prompt is still one memcpy over the whole context, because
`model.Request.Prompt` is a string, and the model call dominates everything here
anyway. What this removes is the render, the transport, and the storage walk.

---

## 7. What this is deliberately not

**Not a tokenizer.** No GPU BPE kernels, no tokenizer-family certification, no
span-export path. If a Loom deployment ever owns its inference engine, the
seam for that already exists: `model.Request.Continuation` is what a
TokTier-compatible frontend would read, and `Stable` is the number it would act
on. Building the abstraction first is what keeps Loom from becoming a tokenizer
project.

**Not a diff.** The fast path reuses a common *prefix*, not a general edit
script. A general diff would find more to reuse when a segment changes in the
middle, and would make every certificate a claim about several disjoint ranges
instead of one — more machinery, more to verify, aimed at a case that barely
happens. Contexts grow at the end. A middle edit shortens the prefix and
re-renders the rest, which is correct and no worse than what happened before.

**Not a session manager.** Loom does not know what a session is, when one starts,
or when one ends. `delta.Chain` turns "here is what changed" into a resolvable
reference; what that reference means is the application's business.

**Not authoritative state.** Nothing a worker holds is the state. The chain in
shared storage is the state, and residency is a cache of renderings of it.
Losing a worker loses nothing, which is what makes locality safe to want.

---

## 8. Where this goes next

The obvious second client is the findings commons, and the mapping is close to
exact. A finding today carries volatility at the topic level, while a single
answer usually mixes facts with radically different change rates:

```
Company = Stripe
  revenue       learned yesterday      slow
  CEO           learned 2 months ago   slow
  share_price   learned 20 min ago     live
  founded       learned a year ago     static
```

Today a stale finding is re-researched. The delta pattern says: ask which facets
need repair, reuse the rest under a splice certificate — provenance still
reachable, grants still contained, facet within its own volatility horizon, not
retracted, parent revision matching — and publish a new immutable revision that
is certified old facets plus a fresh delta. That would move the commons from a
semantic cache to an incrementally maintained knowledge layer, and the
certificate discipline carries over unchanged.

The other direction is what the router could eventually decide. Right now it
picks between splice and rebuild. The same cost surface — base size, delta size,
state residency, backend, hardware, latency objective — is what would decide
worker placement, prompt transport, KV residency, and whether a context is worth
materializing on this machine at all. The routing decision is already in the
right place for that; it is just answering a smaller question than it could.

---

The principle underneath all of it, which is the thing actually worth keeping:

> **State is allowed to make execution faster. It is never allowed to be
> necessary to make execution correct. Every incremental optimization must be
> able to fail toward recomputation.**
