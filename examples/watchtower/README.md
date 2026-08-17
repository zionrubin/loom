# watchtower

An incident feed that never ends, graded per event and digested per minute.

```sh
go run ./examples/watchtower            # backfill six minutes of feed
go run ./examples/watchtower -live      # tail it as a writer appends
go run ./examples/watchtower -crash     # stop mid-stream, resume, price the interruption
```

Offline: the models are deterministic mocks, so there is no key, no network, and
no cost. The incidents are synthetic — the services, messages and timestamps are
fixtures invented for this example.

## What it is demonstrating

`ReduceAI` folds a set. On a stream there is no end of input to say the set is
closed, so a **window** supplies one and the **watermark** is the evidence. That
is stream mode in one sentence, and this example is that sentence with numbers
attached.

Four stages, and only one of them is new:

| stage | |
|---|---|
| `incidents` | `FromStream`: a directory of JSONL, read as it grows |
| `grade` | `Infer`, per record, the moment it arrives |
| `per-minute` | `Window`: turns an endless stream into finite sets |
| `digest` | `ReduceAI` over one window's incidents, once per pane |

Everything **upstream** of the window runs per record with full pipelining — a
grading call starts the moment an incident lands. Everything **downstream** is
scoped to the pane.

## What to look at in the output

**Panes fire on event time, not wall clock.** The feed spans six minutes of
timestamps and is delivered up to 8 seconds out of order; the panes still come
out one per minute per service, each holding exactly the incidents that happened
in it.

```
  pane  2026-03-01T09:00:00Z..2026-03-01T09:01:00Z[api]        3 incidents  (final)
  pane  2026-03-01T09:00:00Z..2026-03-01T09:01:00Z[ledger]     4 incidents  (final)
  pane  2026-03-01T09:01:00Z..2026-03-01T09:02:00Z[checkout]   4 incidents  (final)
```

Run it twice with the same seed and the panes are identical, because a pane's
records are sorted by event time before it fires. That is what makes the
aggregate's prompt a function of its contents rather than of which model call
happened to return first.

**A pane is the unit that costs money.**

```
model calls: 48 grading (one per incident), 21 digesting (one per pane)
```

`-window 30s` roughly doubles the second number and leaves the first unchanged.
`-key ""` collapses the four services into one digest per minute and cuts the
second number by about four. The shape of the window is the shape of the bill,
which is why `loom.Explain` prices a windowed pipeline per pane.

**Lateness is a bound you have to know.** The generator delivers at most 8
seconds behind event time, and the job is configured to allow for exactly that.
Lower it — `loom.WithLateness` in the source, or `-lateness` for the window's
own grace period — and the report starts counting late records:

```
  ingested   48 records, 6 late
```

That count is a fact about the feed, not about Loom. Hiding it would hide the
one number that says your lateness bound is wrong.

**An interrupted job resumes, and the replay is free.** `-crash` stops the job a
third of the way through and starts it again under the same job ID:

```
what the interruption cost
  resumed from epoch      1
  incidents after resume  28
  model calls, first run  28
  model calls, both runs  69
```

69 is 48 gradings plus 21 digests — exactly what one uninterrupted run costs.
The second start resumed at the checkpointed offsets with its half-filled
windows intact, and every task it did re-execute was served from the result
cache. An interrupted stream job costs wall clock, not tokens.

**A job cancelled mid-window does not publish a partial answer.** In `-live` the
job stops on a duration limit with six windows still filling:

```
  window     per-minute    15 panes from 48 records, 6 still open
```

Those went into the final checkpoint rather than being fired half full. The
backfill, whose source genuinely ran out, drains instead — because there really
is nothing more coming. The `stream:` line in the report says which happened.

## Flags

| flag | |
|---|---|
| `-minutes`, `-rate` | size of the generated feed |
| `-window` | window size (default 1m) |
| `-lateness` | the window's grace period before firing (default 20s) |
| `-key` | window key field (default `service`; empty for one window per interval) |
| `-live` | follow the directory while a writer appends to it |
| `-crash` | stop mid-stream, then resume under the same job ID |
| `-state` | keep the workspace instead of using a temporary one |
| `-seed` | feed generator seed |

See [docs/STREAMING.md](../../docs/STREAMING.md) for the design.
