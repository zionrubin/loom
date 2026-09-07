# treasury — four processes, one wallet

Four independent processes — separate programs, separate address spaces,
separate schedulers, the same thing four pods of one deployment are — triage
support tickets against one provider account. The same fleet runs twice against
the same `$0.05` ceiling: once with each process holding it alone, and once with
all four sharing it.

```
go run ./examples/treasury
go run ./examples/treasury -processes 8
go run ./examples/treasury -service       # the quota behind an HTTP service
go run ./examples/treasury -wallet 0.25
go run ./examples/treasury -rpm 60        # a rate bucket tight enough to bind (slow)
```

Offline by default: mock models with realistic pricing, a quota directory in a
temporary path, no keys, no network, zero real cost. With `-service` the same
run goes through an HTTP quota service instead, and nothing else changes —
which is what the store being an interface is for.

## What it shows

A `Fleet` already fixed this once, one level down: `loom.Run` provisions a rate
limiter, a budget governor and a result cache per *pipeline*, and none of those
is a property of a pipeline, so a fleet holds one of each for every agent in the
process.

The same sentence is true of a process, and there the answer was missing. A
worker fleet, a job per pod, a service running a pipeline per request: each of
them gets a fresh limiter and a fresh governor, and each of them therefore
believes it owns the whole account.

```
4 processes · 200 tickets each · $0.05 ceiling · one provider account
quota: a shared directory (/tmp/loom-treasury-…)
explain: one process's ceiling is $0.4130, so 4 of them are $1.6522 against a $0.05 wallet

               alone($)    calls    shared($)    calls
proc-1           0.0504      106       0.0158       33
proc-2           0.0504      106       0.0158       33
proc-3           0.0504      106       0.0106       22
proc-4           0.0504      106       0.0115       24
TOTAL            0.2018      424       0.0536      112

the ceiling was $0.0500 in both rounds.
  alone:  4 processes × $0.0500 = $0.2018 spent — 4.0× the ceiling
  shared: $0.0536 spent, 107% of the ceiling, overrun bounded by the calls in
          flight when it was crossed
  triage-fast: 120 request(s) admitted through one shared bucket, 0 deferred
          for want of room, 8 returned unissued
  proc-1 stopped: shared wallet exhausted: run budget exhausted
  …

and asked again, against the wallet the fleet just spent:
  shared wallet $0.0500: the fleet has spent $0.0536 in all time over 112
  charge(s), leaving $0.0000
  which does NOT cover this run's ceiling: it will stop part-way and return
  partial results, or another process will
```

Both columns are the same program with the same ceiling. The left one is a
`$0.05` cap that cost `$0.20`, and nothing in it is broken or misconfigured —
it is four correct processes each enforcing a number that was never theirs
alone.

## What to look for

- **`4.0× the ceiling`.** That is the bug, measured. It scales with processes
  and it is silent: every one of the four reports a run that finished inside
  its budget.
- **`107% of the ceiling`.** That is the fix, and the 7% is not a rounding
  error — it is the calls that were already in flight when the ceiling was
  crossed. Charging is post-hoc, so the overrun is bounded by concurrency
  rather than by nothing, and the report says so instead of claiming a
  precision it does not have.
- **who stopped.** All four processes are stopped by a wallet that three of
  them did not empty, and the failure says `shared wallet exhausted` rather
  than `run budget exhausted`, because those are two different ceilings and an
  operator needs to know which one to raise.
- **the bucket.** The model is metered, so admission goes through the shared
  buckets too, and the per-model line is the *fleet's*. At the default limits
  it reports zero deferrals, which is the useful answer: the rate limit is not
  what is slowing this fleet down, the wallet is. `-rpm 60` makes the bucket
  bind instead.
- **the projection, twice.** `loom.Explain` prints the run's ceiling before
  anything is spent, and again against the wallet the fleet has since emptied.
  The second one is the question an operator actually has: not what this run
  costs, but whether there is enough left for it.

## In code

```go
q, err := quota.Open("/var/lib/loom/quota", quota.Options{})
defer q.Close()

loom.Run(ctx, p,
    loom.WithSharedQuota(q, core.Budget{MaxCostUSD: 500}, 24*time.Hour),
    loom.WithRunBudget(core.Budget{MaxCostUSD: 5}),
)
```

Both ceilings apply, and the run stops at whichever it reaches first: a runaway
pipeline still cannot outspend its own budget, and a fleet of well-behaved
pipelines still cannot outspend the account.

For a fleet spanning hosts a directory is not shared, so one process holds it
and the rest ask:

```go
http.ListenAndServe(":9095", quota.Handler(q))                       // one process
loom.WithSharedQuota(quota.Dial("http://quota:9095", quota.ClientOptions{}),
    core.Budget{MaxCostUSD: 500}, 24*time.Hour)                      // every other
```

[docs/QUOTA.md](../../docs/QUOTA.md) is the design.
