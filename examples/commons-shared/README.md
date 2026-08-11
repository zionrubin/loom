# commons-shared — four processes, one set of facts

Four independent executor processes — separate programs, separate address
spaces, separate ledgers, the same thing four pods of one deployment are —
research six overlapping companies against one shared commons. The public source
gets called six times instead of twenty-four.

```
go run ./examples/commons-shared
go run ./examples/commons-shared -executors 8
go run ./examples/commons-shared -dsn "$FINDINGS_DSN"   # PostgreSQL + pgvector
```

Offline by default: a scripted source tool with realistic latency and per-query
pricing, mock models, a commons in a temporary directory, no keys, no network,
zero real cost. With `-dsn` the same run goes through PostgreSQL instead, and
nothing else changes — which is what the backend being an interface is for.

## What it shows

`examples/commons` shows four analyst desks *inside one process* sharing their
research. This shows the case that one cannot help with at all: the desks are in
different processes, so each has its own ledger and none can see the others'
work in flight.

The same fleet of executors runs twice, distinguished by one field, and the two
are printed side by side.

```
4 executor processes · 6 subjects · 4 phrasings
the public source takes 120ms per call

                                 no commons shared commons
questions asked                          24             24
calls to the source                      24              6
spent at the source                 $0.0960        $0.0240

18 external call(s) avoided across executors — 75% of the calls the
same fleet made with every process holding its own ledger.

executor        source   local  shared     led followed adopted
executor-1           3       0       3       3       1       3
executor-2           1       0       5       1       3       5
executor-3           2       0       4       2       3       4
executor-4           0       0       6       0       4       6

findings  24 asked · 18 reused (75%) · 6 researched
  local  exact 0 · class 0 · near 0 · coalesced 0 · topped-up 0
  shared exact 0 · class 7 · near 0 · coalesced 11  →  18 external call(s) another executor had already made
  backend  18 adopted · 6 published · 6 led · 11 followed
  avoided $0.0720 and 742ms of research, spent $0.0240
  gate overhead 4.523ms total, 188µs per question (52ms in the shared backend)
```

**The `local` column is zero and that is the finding.** Every one of these
executors is asking about subjects the others are asking about, and none of them
is asking twice itself — so there is nothing for a per-process ledger to reuse.
All 18 avoided calls crossed a process boundary.

**Eleven of the eighteen came from the lease, not the store.** The executors are
started together, so most of them miss a cold commons at the same instant: the
`coalesced` count is the distributed single-flight collapsing simultaneous
askers onto one call, and the `class` count is findings that had already landed.
A layer that only shared *completed* research would have saved 7 of 18.

**The per-executor rows are uneven, on purpose.** `executor-4` paid for nothing
and was served everything; `executor-1` paid for three subjects. That asymmetry
is what sharing looks like when nobody coordinates who researches what — the
saving has beneficiaries, and reporting a fleet total would hide who bought it.

**Gate overhead is microseconds per question against a 120ms source**, and the
line says how much of it was the backend. With `-dsn` that number is larger and
still an order of magnitude under the call it decides about; printing them
separately is what lets you tell "the layer is slow" from "the database is far
away".

**The briefs are byte-identical in both runs.** An answer served out of another
machine's research is the answer the call would have returned, or this is not a
cache, it is a bug — so the example checks it rather than claiming it.

## How it is wired

Nothing below one field changes:

```go
loom.WithFindings(findings.Config{
    Gate:   []string{"dd-search"},
    Policy: findings.Policy{ ... },
    Shared: findings.NewShared(findings.SharedConfig{
        Backend: backend,          // pgstore.Open(...) or filestore.Open(...)
        LeaseTTL: 5 * time.Second,
    }),
})
```

The stage still declares the tool it always declared, the planner still grants
exactly that name, and the executor still checks the capability and the egress
allowlist before the guard is reached. Whether a call reaches the source, this
process's ledger, or another machine's research is decided beneath all of it.

Drop the `Shared` field and this is the single-process layer, unchanged, with no
network on any path — which is what the "no commons" column is running.

## See also

- `docs/FINDINGS.md` §11 for the design: the L1/L2 ladder, the lease and its
  fencing token, what an adopted copy costs, and what happens when the backend
  is down.
- `findings/distributed_test.go` for the same claims as tests, run against real
  executor processes and both backends.
- `examples/commons` for the single-process version and the tiers that work
  without any of this.
