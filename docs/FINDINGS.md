# The commons: sharing research between concurrent agents

[ASYNC.md](./ASYNC.md) maps the agentic-serving literature onto Loom at the
level of the **program**: what a caller waits for, how slots are admitted
between agents, how agents reach each other's conclusions at their boundaries.
It closes with a blackboard — append-only topics an agent posts to and later
agents read.

This document is about the gap that leaves. A blackboard lets an agent publish
what it concluded *after it finishes*. It does nothing for the far more common
and far more expensive case: two agents, running right now, about to research
the same thing.

That case is not hypothetical. Anthropic's write-up of its multi-agent research
system names it directly — an early version had one subagent investigating the
2021 automotive chip crisis while two others duplicated each other on 2025
supply chains — and the fix reported there is better task decomposition by the
lead agent. That works, and it is upstream of the problem. This document is
about the downstream half: what the *engine* can do when two agents want the
same fact, whether or not whoever spawned them managed to carve their briefs
apart perfectly.

---

## 1. Why the result cache cannot do this job

Loom already refuses to pay twice for identical work. `store.Cache` keys a task
on `hash(op fingerprint, input content)`, and on a fleet that cache is shared,
so one agent replays another's completed work for nothing. It is a good cache
and it is the wrong instrument here, in three specific ways.

**It keys on the bytes, and research questions are not byte-identical.** Two
desks wanting the same company's revenue write two different prompts and call a
search tool with two different query strings. Same subject, same facts wanted,
two keys. The cache is not *wrong* about this — those really are different
inputs — it simply cannot see that they are one question.

**It serves the second asker only after the first has finished.** A cache is a
record of completed work. Agents launched together all miss a cold key at the
same instant, all call out, and all write the same entry. The more agents you
run concurrently — which is the entire point of a fleet — the worse this gets,
because concurrency is exactly what defeats a write-then-read cache.

**It is all-or-nothing.** A cached result either matches or does not. It cannot
say "I have four of the five fields you need, go and get the fifth", so a
partial overlap is worth precisely zero.

None of these is a defect to fix in `store.Cache`. They follow from what a
result cache *is*: a memo on a deterministic function of its inputs. Research is
not that. It is a query against a nondeterministic, time-varying oracle, and it
needs an instrument shaped for that.

So the unit of sharing becomes the **question**, and the unit of reuse becomes
the **finding**: a sourced, durable answer about the world that outlives the
agent that learned it.

---

## 2. What the literature found

Four bodies of work, converging from different directions.

### Semantic caching for LLMs

**[GPTCache](https://openreview.net/pdf?id=ivwM8NwM4Z)** established the shape:
embed the prompt, store it in a vector database beside its response, and serve a
cached answer when a new prompt's nearest neighbour is similar enough. The
mechanism is right and the failure mode is well documented — a single global
cosine threshold trades false hits against false misses with no good setting.
Too low and the cache confidently answers a question nobody asked; too high and
it never fires. **[MeanCache](https://arxiv.org/pdf/2403.02694)** quantifies the
cost: over 700 queries it reports 89 false hits against GPTCache's 233.

**[vCache](https://arxiv.org/abs/2502.03771)** (Schroeder et al.) is the direct
answer, and the one this design borrows from. Its observation is that the static
threshold is the problem: correctness is a per-entry property, so the threshold
should be too. It learns a decision boundary *per cached prompt* online, which
gives a cache with a stated error rate instead of a tuned constant and a hope.

### Agent memory and temporal invalidation

**[Zep / Graphiti](https://arxiv.org/pdf/2501.13956)** contributes the
invalidation discipline. Its knowledge graph is bi-temporal — it tracks both
when a fact was true (`t_valid`, `t_invalid`) and when the system learned it
(`t_created`, `t_expired`) — and, crucially, when new information contradicts an
old fact it *writes an invalidation timestamp rather than deleting the edge*. A
memory that forgets cannot answer what it believed when it produced a
conclusion, which is exactly the question a correction makes urgent.

### Stampede control

**[Scaling Memcache at Facebook](https://www.usenix.org/system/files/conference/nsdi13/nsdi13-final170_update.pdf)**
(Nishtala et al., NSDI '13) is the canonical treatment of the concurrency case.
Their **leases** hand exactly one client a token to recompute a missed key and
make everyone else wait; regulating recomputation this way dropped peak database
load from 17,000 to 1,300 queries per second. The same structure appears
everywhere under different names — request coalescing, single-flight — and it is
the mechanism that addresses the cache's second blind spot above.

Two smaller pieces of standards work matter as much. **[RFC 2308](https://www.rfc-editor.org/rfc/rfc2308)**
made DNS cache *negative* answers, because a name that does not resolve is a
fact worth remembering and re-asking is pure waste. **[RFC 5861](https://www.rfc-editor.org/rfc/rfc5861)**
separates "expired" from "unusable" with `stale-while-revalidate` — freshness is
a spectrum with a policy attached, not a boolean.

### Belief revision

**[Doyle's Truth Maintenance System](https://www.sciencedirect.com/science/article/abs/pii/0004370279900080)**
(1979) is the oldest citation here and the most directly applicable. A JTMS
records, for every derived belief, the beliefs it was derived *from* — so
withdrawing a premise can find everything that has to be reconsidered rather
than leaving the system quietly inconsistent. That relation is the reason a
retraction can be more than a delete.

### Multi-agent coordination

The NeurIPS 2025 **MAST** taxonomy finds that roughly a third of multi-agent
failures are coordination breakdowns rather than model failures, and
[Anthropic's multi-agent research system](https://www.anthropic.com/engineering/built-multi-agent-research-system)
reports duplicated subagent work as a concrete instance. **Parrot** (OSDI '24),
already discussed in ASYNC.md, contributes the framing: the engine cannot
optimize structure it cannot see. Here the invisible structure is that two
requests are the same question.

---

## 3. The mapping

| Mechanism | What it actually solves | Loom equivalent | Status |
|---|---|---|---|
| **Question as the cache key** (semantic caching) | Two wordings of one question are two cache keys | `Question{Topic, Facets, Text}` with a canonical key, a subject class, and an optional vector | ✅ `findings/findings.go` |
| **Per-entry learned thresholds** (vCache) | One global similarity threshold trades false hits against false misses with no good setting | Each `Entry` carries its own boundary, seeded from topic policy and moved by adjudication verdicts | ◐ `findings/ledger.go` — moved by verdicts, not by a calibrated error rate |
| **Structural match before semantic match** | An embedding call to decide something the arguments already settle | Exact key, then topic+facets class, then vectors — the near tier is reached only on a miss | ✅ `findings/gate.go` |
| **Verified, not assumed, similarity** (vCache) | A similar question is not the same question | The near tier yields *candidates*; every one passes the sufficiency ladder before it is served | ✅ `findings/gate.go` |
| **Leases / single flight** (memcache) | Concurrent askers all miss a cold key and all call out | One asker leads, the rest wait on its flight, keyed on the subject rather than the sentence | ✅ `findings/gate.go` |
| **Negative caching** (RFC 2308) | A dead end is re-searched forever, at full price | `Result.NoEvidence` is stored and served like any other finding | ✅ |
| **Volatility classes** (RFC 5861) | A per-answer TTL is a guess about something the topic's author knows | `Volatility` declared per topic: static / slow / daily / hourly / live | ✅ |
| **Invalidate, don't delete** (Graphiti) | A memory that forgets cannot say what it believed when it concluded something | Append-only revisions; retraction writes a retracted head and leaves every prior hash resolvable | ✅ `findings/ledger.go` |
| **Justification edges** (Doyle's JTMS) | Withdrawing a premise leaves derived conclusions quietly wrong | Every serve is recorded against the finding; `Retract` returns the run/stage/task list | ✅ |
| **Contradiction detection** (Graphiti) | Two answers to one question, both served depending on index order | `Ledger.Conflicts` reports live claims in a class whose knowledge hashes differ and whose coverage overlaps | ◐ reported, not resolved |
| **Corroboration** | A single-sourced claim treated like a well-attested one | Independent rediscovery converges on one knowledge hash and increments corroborations; `MinSources` gates serving | ✅ |
| **Partial reuse** | A partial overlap is worth zero to an all-or-nothing cache | Coverage gaps narrow the external request to the missing fields | ✅ |
| **Capability-scoped memory** | A shared cache is a way around an egress allowlist | A finding carries the capabilities and hosts its research consumed; a reader is served only if it holds them | ✅ — no prior art found; see §7 |
| **Bi-temporal validity** (Graphiti) | "True then" and "recorded then" are different questions | Not implemented: entries carry one timestamp, the moment the world was consulted | ❌ |
| **Calibrated error rate** (vCache) | A cache should state its false-hit rate, not its threshold | Not implemented: boundaries move on verdicts but no error rate is estimated or guaranteed | ❌ |
| **Cross-process commons** | A fleet per machine is a commons per machine | Partly: the ledger persists to disk and reloads, but there is no shared backend | ◐ |

Four rows were the work: making the question the key, ordering the tiers so the
cheap ones answer most of it, collapsing concurrent askers onto one call, and
containing the whole thing inside the capability model.

---

## 4. What is worth storing

The distinction that decides everything is whether an answer is about the
**world** or about the **record**.

> "Northwind's 2024 revenue was $4.2bn" is a finding. Any agent that asks wants
> that answer, and asking again costs money to learn the same thing.
>
> "This support ticket is angry" is not. It is a judgment about one record, it
> is what the result cache already keys on, and putting it in the commons fills
> it with entries nobody can reuse.

Applying that test:

**Worth storing.** Resolved facts with sources. Retrieved documents, by content
hash. **Negative results** — searched, found nothing — which are the single
highest-value entry class and the one most often left out; without them a
question with no answer is re-researched by every agent that ever asks it.
Expensive derivations over public inputs (a parsed filing, a normalized entity)
where the derivation, not the judgment, is the cost.

**Not worth storing.** Per-record judgments. Anything whose question embeds the
record's private payload — the key itself would be the leak. Model opinions with
no external grounding: they are cheap to regenerate and expensive to be wrong
about, and they are what the escalation ladder exists for. Anything under a
`Live` topic, which is how a caller says "always fetch this" *inside* the
mechanism rather than by routing around it.

---

## 5. Is this question already covered?

Three tiers, ordered so that the cheapest test is also the most certain — which
is not a coincidence but the reason the ordering works.

```
exact   canonical question key → O(1) map hit, no I/O, no model
class   same topic and same facets → a small candidate set, still free
near    embedding similarity within the class → candidates, never hits
```

**The exact key is deliberately conservative.** It normalizes case, whitespace
and trailing punctuation — transformations that cannot change what is being
asked — and nothing else. Stemming, stop-word removal and paraphrase folding all
*can* change it, so they belong in the near tier where a candidate is checked
before it is served, not in the exact key where a false merge is served
silently.

**The class tier is the one that earns its keep**, and it needs no model at all.
Two questions with the same topic and the same facets are about the same
subject, however they are worded. In `examples/commons` this tier alone answers
about half the questions, with no embedder configured. Its warrant is entirely
in the facets: **with no facets the tier is skipped**, because "same class"
would then mean only "same topic", which would serve any answer filed under
`web-search` to any question asked of it. A tier that is free and certain
becomes free and wrong the moment it has no structure to be certain about.

**The near tier produces candidates, never hits.** It is reached only on a miss,
scored only over the class's own entries, and each entry is compared against
*its own* boundary rather than a global constant — the vCache observation,
implemented cheaply. Every candidate then goes through §6 before anything is
served.

---

## 6. Is what exists sufficient?

A second ladder, and again the expensive rung is last and rarely reached.

1. **Reachable.** Does the reader hold every capability and host the research
   consumed? Checked first because it is a map lookup and because its failure is
   not a cache miss but a denial (§7).
2. **Visible.** Is the topic fleet-scoped, or private to whoever learned it?
3. **Fresh.** Is the entry inside its topic's volatility horizon? Freshness is a
   property of the *question* — a founding year does not go stale because the
   crawler was slow, and a share price is stale in minutes however carefully it
   was gathered — so volatility is declared per topic, not guessed per finding.
4. **Covering.** Does it answer every field this caller declared in `Needs`? A
   gap here is not a rejection but a **narrowing**: the external request is
   reissued for the missing fields only, and the two halves are merged.
5. **Corroborated and confident.** Enough independent support, above the topic's
   minimum confidence.
6. **Adjudicated**, optionally: a model asked whether this candidate actually
   answers this question.

The last rung is the only one that can cost anything, and three things keep it
affordable. It is never reached by an exact hit. Its verdicts are **memoized per
(question, finding) pair**, so a fleet of a hundred agents hitting one near-miss
pays for one judgement rather than a hundred. And it is subject to a break-even
rule:

> **The gate may not spend more looking than the lookup could save.**

The ledger records what every finding cost to learn, so a topic's mean research
cost is a *measurement*, not an estimate. When it falls below the adjudication
cost times a stated factor, the gate declines to judge — and declining means
serving the structural verdict it already reached, not rejecting the candidate.
This is the same shape as the planner's prefix-cache rule, which writes a cache
entry only when a second call exists to read it.

---

## 7. Containment: the invariant that makes a shared commons safe

> **The ledger may save you a call you were allowed to make. It may never make a
> call you were not.**

Every finding records the capabilities and egress hosts its research consumed,
and a reader is served only if its own envelope holds all of them. Without this,
a shared research cache is a capability-laundering channel, and the cheapest way
around an egress allowlist is to wait for someone who has it.

This is the one row of §3's table with no prior art behind it, and the reason is
structural: semantic caches are built for one application with one identity, so
the question does not arise. It arises immediately for a fleet whose agents are
deliberately given *different* least-privilege envelopes, and it is checked
first in the ladder rather than last.

Scope is the second half. Containment protects the *answer*; `ScopePrivate`
protects the *question*, for topics whose facets carry something that should not
be answerable by asking whether anyone has asked it.

---

## 8. Indexing, revision, invalidation

The ledger is append-only and content-addressed, which is what makes writing to
it *from inside a task* safe in front of a content-addressed result cache.

**Three indices**, all maintained on append: question key → entries, class →
entries, and knowledge hash → entries. Plus `id` → revisions, with a head
pointer, and finding hash → dependents.

**The knowledge hash excludes the clock.** A finding's semantic body — the
claim, its fields, whether it is a negative result — hashes without wall-clock
time, provenance, or cost; those live on the ledger *entry* around it. So two
agents that independently learn the same thing converge on one hash, and the
second is recorded as **corroboration** rather than as a rival claim. This is
exactly the rule `Fleet.Post` follows in carrying no timestamp, applied one level
down, and it buys corroboration counting for free.

**Revision, not mutation.** Correcting a claim appends a revision naming the
hash it supersedes. The head becomes servable; every prior hash stays
resolvable, because lineage entries name them.

**Four ways a finding stops being served:**

| Trigger | Mechanism |
|---|---|
| Time | The entry ages past its topic's volatility horizon |
| Correction | A revision supersedes it; the old head stops being a candidate |
| Retraction | `Retract` writes a retracted head **and returns every task that was served it** |
| Contradiction | `Conflicts` reports live claims in one class that disagree on overlapping coverage |

Retraction is where the JTMS relation pays. The ledger reports the dependents; it
does **not** re-run them, because whether stale conclusions are worth recomputing
is a question about the caller's budget, not about the ledger.

---

## 9. Does it pay for itself?

The claim is "reduces duplicated work without adding meaningful latency", which
is two claims, so `examples/commons` measures both. Four analyst desks, six
companies, each desk asking in its own house phrasing — 24 questions about 6
subjects, against a source that takes 120ms and bills per query:

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

Three things in that output are worth reading closely.

**No embedder was configured.** Every one of those 18 reuses came from the free
tiers — the class index and the single-flight lease. The near tier is the
optional refinement, not the mechanism.

**The gate costs about 1/2500 of the call it decides about.** That ratio, not
the absolute number, is the argument for gating every task rather than only the
ones somebody guessed would collide.

**The model column does not improve, and here it gets slightly worse.** The
layer removes duplicate calls to the *source*; by removing that source's latency
it makes the desks arrive at the next stage together, so more identical tasks are
in flight at once and the result cache — which has no single-flight lease of its
own — serves fewer of them. That is the same thundering herd one level up. It is
a real finding, it belongs in `store`, and the example prints it rather than
quietly reporting the good column.

---

## 10. Why writing inside a task is safe here

ASYNC.md argues that a task cannot post to the blackboard, and that argument
still holds. A findings write is different in four specific ways, and all four
are load-bearing.

1. **A hit is substitutable for the call it replaces.** The gate serves a
   finding only when it would answer the question the caller was about to ask a
   public source, so whether a task hit the ledger or called out is not
   observable in its output — the same claim the result cache makes. What *is*
   observable is cost. **The ledger makes spend order-dependent and answers
   order-independent**, which is the right way round, and `examples/commons`
   asserts the second half rather than claiming it: every brief is byte-identical
   in both runs.
2. **Append-only and content-addressed.** A contribution never mutates a finding
   another task is holding. The bytes behind a hash never change.
3. **The knowledge hash excludes the clock** (§8), so rediscovery converges
   instead of forking.
4. **Containment travels with the finding** (§7), so shared state does not
   become ambient authority.

A blackboard post fails test 1 and that is why it happens at agent boundaries. A
finding passes it, and that is why it can happen inside a task.

---

## 11. Across executors

Everything above happens inside one process. A fleet worth running usually is
not one process — it is a worker per machine, a batch job beside a long-lived
service, ten pods of one deployment — and each of those holds a ledger the
others cannot see. The duplication the gate removed between agents comes
straight back at the process boundary: *n* executors research one subject *n*
times, and *n* executors call one source at the same instant because none of
them can see the others' flights.

The distributed layer closes that gap by adding one rung to the ladder rather
than replacing it:

```
L1   the in-process ledger      map lookups, no I/O, unchanged
L2   the shared backend         one round trip, only after an L1 miss
     the source                 what both exist to avoid
```

### The rung, and why it is a rung

L2 is not a write-through cache in front of L1, and L1 is not a buffer in front
of L2. A local hit answers without touching the network — which is the property
that lets the gate stand in front of *every* task rather than the ones somebody
guessed would collide — and L2 is consulted only when L1 had nothing.

What L2 returns is *adopted*: copied into the local ledger, and then run through
the ordinary sufficiency ladder. That is the whole of the design's safety
argument. There is no second implementation of "is this finding good enough",
so a remotely stored finding gets no privilege a local one lacks: the same
reachability check, the same freshness horizon, the same coverage test, the same
corroboration floor, the same adjudication. `Reachable` in particular is what
stops a shared database from becoming a capability-laundering channel with a
larger radius — the cheapest way around an egress allowlist would otherwise be
to wait for a *machine* that has it.

Adoption also warms L1, so the next agent in that process never leaves it, and
it carries across the finding's *decided* history — the adjudications other
executors already paid a model for, so no pairing is judged twice anywhere.

### The lease

Deduplication between processes needs mutual exclusion between processes. The
key is the one the in-process flight already uses — subject class plus required
coverage, falling back to the exact question key when there are no facts to be
certain about the subject with — and the mechanism is a lease rather than a
lock, because the holder is a process that can die:

- an **owner ID** and an **expiry**, so a crashed executor costs one TTL rather
  than blocking a question forever;
- **renewal**, so the TTL bounds how long a *crash* stalls a question rather
  than how long research is allowed to take;
- a **fencing token**, incremented every time the lease changes hands, so an
  owner that stalled past its expiry and woke to find itself replaced cannot
  release the new owner's lease — which would wake its followers onto a finding
  nobody has contributed yet;
- **bounded waiting** with backoff, cancellable, after which a follower
  researches the question itself: correctness preserved, deduplication lost,
  which is the right way round for a bound that exists to stop a stuck leader
  from stalling a fleet.

Only the process-level leader takes a distributed lease, so an executor
contributes one waiter however many of its own agents are asking. Waiting is
polling with bounded exponential backoff against an indexed primary key — no
pub/sub, no broker, no connection anybody has to hold open.

### The seam

Three interfaces, and the gate knows nothing else about the backend:

| | |
|---|---|
| `Store` | put a revision, fetch the candidates for a question, cite, retract, memoize verdicts and thresholds, summarize topics |
| `VectorStore` | upsert by finding hash, top-K cosine search filtered by topic and subject class, deactivate on retraction |
| `Leases` | acquire, renew, release, peek — with owner, expiry and fencing token |

`findings/pgstore` implements all three over PostgreSQL with `pgvector`, which
is the intended production backend and the reason leases need no Redis and
vectors need no second database: contributing a finding and indexing it is one
transaction, and releasing a lease after the contribution lands is an ordering
one connection can guarantee. Where the extension is unavailable it stores
embeddings as JSON and scores them in Go, and says which mode it is in.

`findings/filestore` implements the same three over a shared directory, for the
many fleets that are several processes on one machine — and, just as usefully,
as the second implementation that keeps the interfaces honest.
`findings/backendtest` is the conformance suite both of them pass, which is what
"replaceable" means operationally.

### What a copy costs

An adopted finding is a copy, and a retraction on another executor cannot reach
into this process's memory. Re-validating on every local hit would put the
network back on the path this layer exists to keep it off, so a copy instead
carries a **refresh window** (`SharedConfig.Refresh`, 60s by default): past it,
the local hit misses, L2 is consulted, and whatever the commons holds now — a
revision, a retraction, nothing — is what gets served. Locally learned entries
have no such window, so a fleet that shares nothing behaves exactly as it did.
That window is the staleness bound for cross-executor invalidation, and it is
stated rather than hidden because it is a real cost of a network-free L1.

### When the commons is down

A layer whose job is avoiding calls should cost money when it breaks, not
correctness. A backend failure is counted, reported, and otherwise ignored: the
gate researches the question as though no backend were configured. `Strict`
inverts that for the deployment where an uncoordinated executor is worse than a
stalled one — a metered source with a hard quota — and it is deliberately
explicit, because turning it on converts an optimization into a dependency.

One rule outranks both: **a shared-backend failure never fails research that
succeeded.** The answer is in hand and paid for; all that is lost is another
executor's chance to reuse it, which is counted as such.

### What it measures

`examples/commons-shared` runs four executor *processes* over six overlapping
subjects, twice — once with the shared commons and once without — and counts the
calls in the source's own log rather than in the layer's counters:

```
                                 no commons shared commons
questions asked                          24             24
calls to the source                      24              6
spent at the source                 $0.0960        $0.0240

findings  24 asked · 18 reused (75%) · 6 researched
  local  exact 0 · class 0 · near 0 · coalesced 0 · topped-up 0
  shared exact 0 · class 7 · near 0 · coalesced 11  →  18 external call(s) another executor had already made
  backend  18 adopted · 6 published · 6 led · 11 followed
```

The split between the two shared lines is the argument: 11 of the 18 avoided
calls were collapsed by the distributed lease — executors that missed at the
same instant — and 7 by findings already in the commons. A layer with only the
second half would have saved 7.

## 12. What this is deliberately not

**Not a knowledge base.** Findings are cached research with a horizon, not a
curated corpus. Nothing here does entity resolution across claims, and
`Conflicts` reports contradictions rather than resolving them — resolving them
is a modelling decision, and the ledger is not the right place to make it.

**Not calibrated.** vCache's contribution is a cache with a *stated* error rate.
Entry thresholds here move on adjudication verdicts, which is the cheap half of
that idea; nothing estimates or guarantees a false-hit rate. A topic where a
false hit would be expensive should set `Adjudicate` and a high `Near`, and know
that it is buying care rather than a bound.

**Not bi-temporal.** An entry records when the world was consulted, not the
interval over which the fact held. Graphiti's `(t_valid, t_invalid)` is the right
model for "what was true in March" and this is not it.

**No eviction.** The ledger grows without bound, which is the same gap the result
cache has and now has in a second place.

**Not a replicated store.** The commons is distributed in the sense that every
executor reads and writes one backend (§11); it is not replicated, partitioned,
or multi-region, and a backend outage degrades every executor to local-only
research at once. Nor is L1 coherent: an adopted copy is trusted for its refresh
window, so a retraction reaches other executors within that window rather than
immediately.

**No contradiction resolution across executors.** `Conflicts` still reports
disagreement rather than settling it, and a shared backend means it can now
report disagreement between machines.

**Not a substitute for good task decomposition.** Anthropic's fix for duplicated
subagent work — tell each subagent precisely what it owns — is upstream of this
and better, because work never dispatched costs nothing at all. This is what
catches the overlap that survives it, and on a fleet whose agents are written
independently, most of it does.

---

Read next: [ASYNC.md](./ASYNC.md) for the fleet this sits on and the blackboard
it complements, [ARCHITECTURE.md](./ARCHITECTURE.md#47-state-cas-cache-lineage)
for the state layer it extends, `examples/commons` for the single-process
numbers and `examples/commons-shared` for the cross-executor ones — both
runnable offline.
