# A fleet of worker processes

`loom.WithWorkerService` puts a run's tasks on a durable queue with leases and
lets worker processes claim them; `loom.Serve` is the other side, and takes the
same options.

```go
// every worker process
loom.Serve(ctx, buildPipeline(), opts(q)...)

// the process driving the run
loom.Run(ctx, buildPipeline(), append(opts(q), loom.WithWorkerService(q))...)
```

Nothing above the executor seam changes, because `Executor` is one method over
serializable data: the planner, the admission control, the escalation ladder, the
governor, the cache and the event stream cannot tell which process a task ran in.
A pipeline does not know it has been distributed.

## What distribution costs, and what the cost buys

What it costs is a **lease**, and the lease is what makes it survivable — a
worker killed mid-call loses its claim rather than the task, and the task is
redelivered.

Delivery is therefore at-least-once, and it produces exactly-once *work*: results
are written to an address derived from their own bytes and committed under a
**fencing token**, so a duplicate execution writes the same blob to the same
place and exactly one of the two commits becomes the result — the other is told
which one won, and a worker that stalled past its expiry cannot overwrite the
worker that replaced it.

A stopped worker finishes what it is holding before returning; a killed one loses
its leases, which the queue redelivers. Both are safe. The difference is the
tasks in flight.

## Two things must be true of the fleet

Both fail loudly rather than quietly.

- **Every worker needs the pipeline compiled into it**, because an op is code and
  a runner cannot be serialized. A worker does not receive a pipeline, it *is*
  one — which is why both sides take the same options: the two processes compile
  the same plan, register the same tools, connect to the same servers, and agree
  on every stage fingerprint without exchanging any of it.
- **Every worker must share the driving process's content-addressed storage**,
  because inputs, broadcast values and outputs travel by hash. Use `WithStateDir`
  pointing at shared storage; a task whose blob cannot be resolved fails as the
  deployment error it is.

Workers advertise what they can serve — stages, providers, tools, sandbox
profiles, MCP servers, concurrency — and claim nothing else, because across a
fleet "this executor can run this task" stops being true by construction.
`Worker.Advertises` reports what a process told the queue.

## Queues

Two ship behind one contract and one conformance suite (`worker/queuetest`):

| | |
|---|---|
| `worker` (in-memory) | one process with several workers in it |
| `worker/filequeue` | several processes over a shared directory |

## Locality, softly

A stage carrying delta state has a worker that already holds it. The queue
prefers that worker — *softly*: `Affinity` is a preference, not a `Requirement`,
because a task that only its state-holder could claim would be unclaimable the
moment that worker died. SIGKILL the process carrying a session and the next
round lands on one that has never seen it, rebuilds from the chain, and answers
identically. See [DELTA.md](./DELTA.md).

## Examples

```sh
# one pipeline across several worker *processes*, with one of them killed by
# SIGKILL while it holds a paid model call. Prints what the kill cost (the calls
# the dead worker had started, re-executed — and nothing else) and checks every
# record's answer against a single-process run of the same pipeline
go run ./examples/worker-fleet
go run ./examples/worker-fleet -workers 5 -docs 40
go run ./examples/worker-fleet -kill=false   # the undisturbed fleet: same cost as local

# one long agent session across worker processes, with the worker holding that
# session's state killed halfway through
go run ./examples/delta-session
```

## Related

- [ARCHITECTURE.md §6](./ARCHITECTURE.md#6-scaling-path-from-local-runtime-to-distributed-system)
  — the scaling path this is the first stage of.
- [ASYNC.md](./ASYNC.md) — many pipelines on one engine, which is the other axis.
- [DELTA.md](./DELTA.md) — state that makes a worker worth preferring.
