# One quota, one wallet — across processes

[ASYNC.md](./ASYNC.md) makes an argument and then acts on it. `loom.Run`
provisions a rate limiter, a budget governor, a result cache and a set of
execution slots, and releases them afterwards. For one pipeline that scope is
exactly right. For several it is exactly wrong, because none of those things is
a property of a pipeline:

> A rate limit is a property of an **account**; a dollar ceiling is a property
> of a **wallet**; a cache is a property of **work already done**.

`loom.Fleet` is what follows from that: one limiter, one governor, one cache,
one set of slots, borrowed by every agent in the process.

This document is about the same sentence one level out, where the answer was
missing. A `Fleet` fixes the duplication *inside* a process. It does nothing
about the duplication *between* them — and between them is where most
deployments live.

---

## 1. The bug, stated plainly

Every one of these is several processes against one provider account:

- a worker fleet — `loom.Serve` on four boxes, claiming from one queue;
- a batch of jobs started by a systemd timer, or a cron entry per dataset;
- a service that runs a pipeline per request, behind a load balancer;
- two teams whose pipelines happen to use the same API key.

Each process calls `loom.Run`. Each gets a fresh `runtime.RateLimiter` and a
fresh `runtime.Governor`. So:

**The rate limit is multiplied by processes.** Ten workers each admitting
against 4,000 requests/min collectively admit against 40,000. The provider
answers the difference with the 429s the scheduler exists to avoid, and the
scheduler — which is doing its job correctly, against a limit it was told it
owned — retries them as transient failures, which is exactly the wrong response
to a limit you are already over.

**The ceiling is multiplied by processes.** This one is worse, because it is
not a throughput problem:

```go
loom.WithRunBudget(core.Budget{MaxCostUSD: 100})
```

is a hard cap in one process and a $1,000 cap in ten. Nothing reports it.
Every one of the ten finishes inside its budget and says so. The framework whose
first premise is that model calls cost real money has a ceiling with a hole
under it, and the hole is shaped exactly like a deployment.

`examples/treasury` runs that: the same four processes, the same `$0.05`
ceiling, twice.

```
               alone($)    calls    shared($)    calls
TOTAL            0.2018      424       0.0536      112

  alone:  4 processes × $0.0500 = $0.2018 spent — 4.0× the ceiling
  shared: $0.0536 spent, 107% of the ceiling
```

---

## 2. In practice

```go
q, err := quota.Open("/var/lib/loom/quota", quota.Options{})
defer q.Close()

loom.Run(ctx, p,
    loom.WithSharedQuota(q, core.Budget{MaxCostUSD: 500}, 24*time.Hour),
    loom.WithRunBudget(core.Budget{MaxCostUSD: 5}),
)
```

`WithSharedQuota` replaces this process's per-minute buckets and adds a second
ceiling. The two ceilings compose the obvious way — **a run stops at whichever
it reaches first** — and dropping either would leave one failure unguarded: a
runaway pipeline that empties the shared wallet, or a fleet of well-behaved
pipelines that collectively empty the account.

A window of zero is a pot that does not refill. `24*time.Hour` is a daily
budget. A budget of zero is no ceiling at all, which still records spend,
because a shared ledger with no ceiling is a fleet-wide cost report and worth
having on its own.

For a fleet spanning hosts, a directory is not shared. One process holds it and
the rest ask:

```go
// the process that holds the quota
http.ListenAndServe(":9095", quota.Handler(q))

// every process that spends it
loom.WithSharedQuota(quota.Dial("http://quota:9095", quota.ClientOptions{}),
    core.Budget{MaxCostUSD: 500}, 24*time.Hour)
```

Nothing else changes. The two backends pass one conformance suite
(`quota/quotatest`), which is the only honest test of "the store is
replaceable".

---

## 3. The store is bookkeeping. The ceiling is not.

A `quota.Store` records what a fleet has drawn and what it has spent. It never
records what that spend is *allowed* to reach.

That looks like an omission and is the design. A ceiling is a policy decision,
and policy belongs to the process that was configured with it. Writing it into
the store would create a question nobody wants to answer — what happens when a
process opens a store whose stored ceiling disagrees with its own config? Fail
to start? Silently adopt? Silently *raise* the account's ceiling because
somebody deployed a stale config?

Leaving it out dissolves the question:

> Each process compares the shared spend against the budget it was given. A
> fleet whose processes disagree needs no reconciling — each stops at its own
> ceiling, and the strictest stops first.

The same reasoning applies to the window. A window is how a caller wants the
spend summed, so the store keeps spend at hourly resolution for
`quota.Retention` (seven days) plus an all-time total, and each process sums the
span it cares about. One process can hold the fleet to $500/day while another
holds it to $50/hour, and both are answered from the same numbers.

---

## 4. Two ways to be wrong, and only one is allowed

Coordination fails. A process dies holding a draw it never used; a directory is
on an NFS mount having a bad minute; a quota service is restarting. Every one of
those resolves in the same direction:

> **The shared quota errs low. Never high.**

- **A draw a dead process never returns** is a request the fleet was entitled to
  make and did not. The bucket refills toward its cap regardless, so the loss
  costs throughput until it refills and can never let the fleet exceed the
  limit.
- **A store that cannot be reached cannot admit anything**, so `Acquire` fails —
  as a *transient* failure, which is the scheduler's cue to back off and retry
  rather than to dead-letter a task, and, if the outage lasts, to run out of
  attempts and stop the run. A quota you cannot reach is a quota you cannot
  respect.
- **A wallet that cannot be read** is treated as spent once the outage outlasts
  `Config.Grace`. A blip is absorbed; an outage stops the fleet.

The one thing that is *not* bounded this way is the overrun a post-hoc charge
allows. The governor charges after a call returns, because until then nobody
knows what it cost — so a ceiling can be crossed by whatever was already in
flight across the fleet. That is a real number and the report prints it rather
than rounding it away: `107% of the ceiling` above is 112 calls where 105 fit,
and the seven were in flight when the 105th landed.

---

## 5. Why a batch, and why it gets cheaper as the fleet gets busier

A shared bucket is a bucket behind a round trip, and the naive integration —
one round trip per request — is wrong in the same way a chat client's MCP
integration is wrong for a pipeline. A stage with a hundred concurrent tasks
against one model would make a hundred calls to ask one question, and when the
bucket is empty it would make them *again* on every retry. The moment the fleet
is genuinely contended is the moment coordination costs the most.

So a process does not ask per request. One goroutine per model takes the round
trip on behalf of every task waiting in this process, hands out what came back
in arrival order, and waits once for the rest.

> The lock is taken by one goroutine on behalf of every task waiting here, so
> the cost of coordination per call **falls** as the fleet gets busier.

`Store.Draw` therefore takes a batch and answers with a count: it walks the
batch from the front, drawing while both buckets allow, and stops at the first
request that does not fit — reporting how long until it would. A partial grant
is normal and is the point: the front of the queue goes now and the rest waits,
rather than everyone waiting for the whole batch to fit at once.

What the leader does **not** do is hold quota. It draws exactly what its
followers are waiting for and hands all of it out; nothing is reserved for
later, and nothing is lost when the process dies mid-round. That is what keeps
the shared bucket exact rather than approximately fair. The alternative —
leasing a slice of the minute and spending it locally — is the standard
distributed rate-limiting trick and it buys fewer round trips at the cost of a
bucket that is only right on average. Batching buys the same round trips with
none of that, because the waiters are already in one place.

`TestWaitingTasksShareOneRoundTrip` pins it: 64 concurrent acquisitions,
measurably fewer than 64 round trips, and `Stats.Coalesced` reports how many
rode on somebody else's.

---

## 6. What a refund is for

A task settled from the result cache reached no provider, so the admission it
was granted goes back rather than throttling the calls that still have to be
made. The in-process limiter has always done this; across processes it matters
more, because the quota it would otherwise sit on is somebody else's.

Refunds ride on the next leader's round when there is a leader, so a run that
mostly replays from cache pays no round trips of its own to say it spent
nothing. When there is nobody to carry them, the refunding goroutine takes them
itself — off the hot path, since the task it belongs to has already settled —
and `host.close` flushes whatever is left, so a batch of short-lived processes
does not each exit holding a handful of draws.

A refund mirrors its draw's arithmetic exactly, clamp included, because a refund
that does not match its draw is a leak in one direction or a licence to overrun
in the other. `AReturnIsTheInverseOfItsDraw` and `AReturnNeverOverfillsTheBucket`
are both in the conformance suite, so both backends owe it.

---

## 7. Explain, against the money that is actually left

`loom.Explain` projects what a run will cost without making a call. With a
shared quota configured it also reads the wallet, and the projection gains the
line an operator actually wants:

```
shared wallet $500.0000: the fleet has spent $496.2000 in the last 24h0m0s
over 8213 charge(s), leaving $3.8000
  which does NOT cover this run's ceiling: it will stop part-way and return
  partial results, or another process will
```

Knowing that costs one read. Not knowing it costs half a job, and a report
whose "partial results" nobody can explain.

This is the one thing a projection reaches outside the process for, and it is a
read: `Explain` still issues no model call, resolves no secret, and writes
nothing — not to the state directory and not to the quota. `TestExplainReadsThe
WalletsHeadroom` checks the ledger is unchanged after a projection, because a
tool that charged you for asking what something costs would be the exact
opposite of the point.

---

## 8. Which ceiling stopped the run

Two ceilings means an operator reading a stopped run has a question: which one
do I raise? So the governor remembers, and the failure says:

```
shared wallet exhausted: run budget exhausted
```

`runtime.ErrWalletExhausted` wraps `runtime.ErrBudgetExhausted`, so everything
that already handles an exhausted budget handles this unchanged, and a caller
that only cares that money ran out can keep ignoring the distinction.

---

## 9. What the run report says

```
shared quota: $0.0487 of $0.0500 spent by the fleet in the last 24h0m0s over 200
              charge(s); $0.0013 left
  admission: 200 request(s) drawn from the shared buckets, 0 given back unissued,
             in 114 round trip(s) (86 coalesced onto a round somebody else was
             already making)
  contention: 0 wait(s), 0s spent waiting for room the fleet had already taken
  triage-fast: 200 admitted, 0 deferred, 0 returned (fleet-wide)
  store: 315 round trip(s) in all — admission, charges and balance reads
```

Three kinds of number, deliberately kept apart:

- **the fleet's**, read from the store: what everyone has spent, and what the
  buckets did;
- **this process's**, counted here: what it drew, what it gave back, and how
  many round trips that took;
- **what sharing cost**: waits, time spent waiting, and round trips. The waits
  are reported even when they are zero, because "we waited nothing" is the
  answer that says the rate limit is not what is slowing this fleet down — and
  that is the number an operator is looking for. The 200 admissions above took
  114 round trips because 86 of them rode on a round another task was already
  making; the other 200 round trips are charges, one per call, which is the
  throughput bound of the whole mechanism: a directory serializes them, so a
  fleet issuing thousands of calls a second wants the service rather than the
  directory, and a fleet issuing millions wants a store this package does not
  ship.

`FleetReport` prints the same block under its own budget line, because the fleet
budget is this process's ceiling and the wallet is the account's, and showing
only the first would be the exact mistake this feature exists to correct.

---

## 10. The directory, and what it is not

`quota.Open` is a directory: one small state file and a lock beside it.

The file is rewritten whole under the lock rather than appended to, which is a
different choice from `store.Cache`'s index or `worker/filequeue`'s log, and it
is the right one here. A bucket's level is a *number*, not a history; nothing
downstream replays a draw; and what the fleet spent last Tuesday at 3pm is not a
number anybody needs to the cent. So there is no log to compact, no offset to
fold, and no partial read to defend against — the write goes through a temporary
file and a rename, so a reader sees the old state or the new one and never half
of either.

The lock is `worker/filequeue`'s, mechanism for mechanism: a directory created
with `O_EXCL` as the portable atomic test-and-set, a token stamped inside it, a
breaker that may only remove a lock it has watched unchanged for the whole stale
window, and a release that only removes the lock if the token there is still its
own. What remains is a window in which a suspended holder and its replacement
both believe they hold it — and here that is cheaper than it is for a queue: the
worst a torn write can do to a bucket is set its level to one of two values the
fleet was entitled to be at within the same second.

Reads do not take the lock. A reader that did would serialize the run report and
every budget check behind every draw in the fleet, for an answer that is a
snapshot the moment it is returned anyway.

What a directory is not is distributed. A shared directory is a machine (or an
NFS mount, whose locking guarantees are exactly as good as its administrator's
claims). Many processes on one host: the directory. Many hosts: `quota.Handler`
and `quota.Dial`.

---

## 11. What is deliberately not shared

**`model.Limits.MaxConcurrent`** stays in the process. It is the ceiling a model
running on *your* hardware imposes — llama.cpp's slots, a serving engine's batch
width — which is a property of a device rather than of an account, and two
processes pointed at two GPUs would be wrong to share one. A single local server
behind several processes does want a shared bound, but a semaphore with
heartbeats and expiry is a different mechanism from a bucket that refills on its
own, and it is not built.

**The result cache's single-flight lease.** `store.Cache` collapses identical
tasks admitted together onto one call, and that lease lives in the process. Two
workers on different hosts still both compute, because the lease is beside the
in-memory flight table rather than beside the shared index. The mechanism this
document adds — a lock and a small shared state file — is most of what fixing
that needs, and it is the obvious next step.

**Per-owner attribution.** The ledger records what the fleet spent, not which
process spent it. A run's own spend is already in its report; joining the two is
an observability question rather than a quota one, and putting an unbounded map
of process names in a file rewritten on every charge would be paying for it in
the wrong place.

**The worker side of a worker fleet.** A worker never schedules — it claims a
task and runs it through `executor.Local`, and admission and charging both
happened in the process that submitted it. So `loom.Serve` and `loom.NewWorker`
have nothing to draw with, and configuring a worker with `WithSharedQuota` is
harmless and does nothing. The place to put it is the client, which is also the
place a run's budget already goes. Where a shared quota earns its keep in that
topology is several *clients*: four submitting processes against one queue are
four schedulers, and those are exactly the four that used to hold four ceilings.

**Reservations.** Nothing here holds money in advance. A process cannot say "I
am about to spend $40, keep it for me", so two fleets racing for the last $10
both start and both stop. Reserving would need a lease with an expiry and a
process to renew it, and it would trade a bounded overrun for a bounded
*under*-spend — which is the worse of the two when what you are protecting is a
ceiling.

---

## 12. Map of the code

| File | What it holds |
|---|---|
| `quota/quota.go` | The contract: `Store`, `Draw`/`Grant`, `Charge`/`Ledger`, `ModelState`, and `Shared` — the process's view, its cached ledger, its grace window, its `Stats`. |
| `quota/admit.go` | Admission: the per-model queue, the leader that takes one round trip for everybody waiting, refunds and their lazy flush. |
| `quota/dir.go` | The directory backend: the state file, the `O_EXCL` lock, bucket refill and draw arithmetic, the hourly ledger. |
| `quota/service.go` | The HTTP backend: five routes over any `Store`, and the client that speaks them. |
| `quota/quotatest` | The conformance suite both backends pass. |
| `runtime/runtime.go` | `Buckets` and `Wallet` — the two seams — plus `NewSharedRateLimiter`, `NewSharedGovernor`, and `ErrWalletExhausted`. |
| `loom.go` / `fleet.go` | `WithSharedQuota`, `WithQuotaConfig`, the host's `quota.Shared`, `RunResult.Quota`, `FleetReport.Quota`. |
| `explain.go` | `Projection.Wallet` and `FitsWallet`. |
| `quota_process_test.go` | Two real processes, one directory, and the control that makes the claim a measurement. |
| `examples/treasury` | Four processes, one wallet, both rounds printed side by side. |

The seams are two methods each, and that is the whole of the integration:
nothing in planning, execution or recovery learns that the bucket it drew from
was in another process.
