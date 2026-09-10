# A run you can see from outside the process

Loom observes itself thoroughly. Every lifecycle transition publishes a typed
event; a collector folds that stream into a `RunReport` with per-stage cost,
tokens, retries, cache hit rate and latency percentiles; the constellation view
([VIZ.md](./VIZ.md)) draws every task as a star while it runs.

All of it lives in the process, and all of it ends when the process does.

That is the right shape for building a pipeline and the wrong one for running a
deployment. The report answers *what did this run cost*, and it answers it
after the fact, to whoever was holding the `RunResult`. The questions an
operator actually has are different, and none of them can be answered from
inside a run:

- What is being spent, **right now**, across every process on this account?
- Will the wallet last the hour?
- Which stage is throttled, and since when?
- Did the escalation ladder start climbing at 03:00, and is it still climbing?
- This run took forty minutes. **Which** forty minutes, and where?

Those are questions for the monitoring system the team already pages off, and
`telemetry` is the seam onto it: Prometheus metrics on an HTTP endpoint, and
OpenTelemetry spans to a collector.

```go
tel := telemetry.New(telemetry.Options{
    Service:  "ticket-triage",
    Endpoint: "http://otel-collector:4318",   // or $OTEL_EXPORTER_OTLP_ENDPOINT
})
go tel.Serve(ctx, ":9464")                    // /metrics, /healthz, /readyz

res, err := loom.Run(ctx, p, loom.WithTelemetry(tel), ...)
```

One exporter serves any number of runs, fleets, stream jobs and worker
processes in one program, and should: its counters are cumulative for the life
of the process, which is what a scrape expects.

---

## 1. What is exported is chosen by what is scarce

A classic data framework exports records per second, queue depth and CPU,
because cores and seconds are what it runs out of. Loom runs out of other
things first, and they come first here:

| Family | Why it is the first one |
|---|---|
| `loom_cost_usd_total{pipeline,stage,model}` | The invoice, broken down by where it came from |
| `loom_budget_headroom_usd{pipeline}` | What is left of the wallet in flight |
| `loom_tokens_total{…,kind}` | input / output / cache_read / cache_write |
| `loom_task_queue_seconds` | How long tasks wait for rate-limit admission |
| `loom_escalations_total` | The ladder climbing, which is money |
| `loom_cache_hits_total{…,kind}` | The work that was not paid for twice |

`loom_cache_hits_total` is a **subset** of `loom_tasks_total{outcome="completed"}`
rather than a sibling of it — a replayed task is a completed task that made no
call, and a dashboard that added the two would double-count every run that
resumed.

Throughput and latency are here too — `loom_task_duration_seconds`,
`loom_model_call_duration_seconds`, `loom_stage_duration_seconds` — but they
are not the headline, because a Loom deployment that goes wrong usually goes
wrong on money or on admission long before it goes wrong on wall-clock.

The full surface is one table in
[`telemetry.go`](../telemetry/telemetry.go), declared rather than created on
first use, so a change to it is reviewable the way a schema change is.

---

## 2. Cost is counted at the call, not at the task

This is the one design decision in the package worth arguing about, so here is
the argument.

A task that climbs an escalation ladder makes more than one call. The cheap
model answers, the validator rejects the answer, the task climbs, and the
stronger model answers. The provider billed for both. But the task's completion
event carries the usage of the call that *answered* — the rejected one is a
fact about a failed attempt, not about the result.

So a cost metric fed from task completions under-reports a run by exactly what
the escalations cost, which is the money an operator most wants to see. Loom's
own scheduler already knows this: it charges the budget governor for every
attempt, including the ones whose answers were thrown away.

`telemetry` counts the same way. `loom_cost_usd_total` and `loom_tokens_total`
are fed from `model.called`, one increment per call, and the root-level test
asserts the totals against `RunResult.Spent` — because an exporter that
disagrees with the run report is worse than no exporter, since two dashboards
will be built on it.

A corollary worth knowing when reading a deployment's graphs: in a worker fleet
the *cost* series appear in the worker processes, because that is where the
calls are made. The driver's process reports the tasks, the queueing and the
run. They are one trace either way (§5).

### The prefix cache's two halves

`model.called` reports what the provider's prompt-prefix cache was worth as one
signed number: positive when a read served tokens cheaply, negative while a
freshly written entry is still unamortized. That is the honest reading, and it
is unusable as a counter — a series that can fall is one every `rate()` over it
misreads.

It is exported as the two monotonic halves it is made of:

```
loom_prefix_cache_saved_usd_total     # what reads saved
loom_prefix_cache_premium_usd_total   # what writes cost
```

The net is their difference, and a dashboard can take it.

---

## 3. The wallet

`loom_budget_headroom_usd{pipeline}` is the series to page on, and it needs no
`Explain` call to exist: the run budget rides on the run's own header event, so
the exporter knows the ceiling from the moment a run starts and subtracts what
the calls report.

Two semantics are worth stating because they are choices:

- It is the **tightest** wallet in flight. Several runs of one pipeline at once
  report the smallest remaining balance among them, because the one about to
  stop is the one worth knowing about.
- It is **restored** when nothing is in flight. An idle pipeline has spent
  nothing, so its headroom is its whole ceiling. A stale "almost empty" left
  behind by a finished run would page somebody at 04:00 about a run that ended
  at midnight.

A run with no ceiling reports **no series at all**. There is no honest number
for how much is left of a budget nobody set, and a zero would read as an
emergency.

Beside it, `loom_projected_cost_usd` and `loom_projected_ceiling_usd` carry
what [`loom.Explain`](./EXPLAIN.md) last said a run would cost — so forecast
and actual sit on one dashboard, and a stage whose real spend leaves its
projection behind is visible as a divergence rather than as a surprise on the
invoice.

---

## 4. Cardinality is bounded, and the dollars survive the bound

A label whose values come from the workload rather than from the program is how
monitoring systems are brought down, and Loom has one: a stage ID is whatever
the pipeline's author named it, and a *generated* pipeline generates names.
`agent-forge` writes a stage per agent; `partner-atlas` writes a branch per
partner.

Every family therefore has a ceiling on its series count (`Options.MaxSeries`,
500 by default). Past it, new label sets are **folded into one overflow member**
whose label values read `__overflow__` — they are not dropped:

```
loom_cost_usd_total{pipeline="atlas",stage="__overflow__",model="__overflow__"} 41.2216
loom_telemetry_overflowed_metrics 1
```

The number stays true and only the attribution collapses. That is the right way
around. An operator whose dollar counter silently stopped counting has been lied
to; one whose dollars arrive under a label that says *there were too many
stages to break this down* has been told something useful, and told where to
look.

The exporter's own failures are exported for the same reason:
`loom_telemetry_spans_dropped_total`, `loom_telemetry_export_failures_total`,
`loom_telemetry_undeclared_writes_total`, `loom_telemetry_overflowed_metrics`.
A monitoring surface that hides its own faults is worse than one that has them.

---

## 5. The run ID is the trace ID

Nothing in the trace side is randomly generated. A trace ID is a hash of the
run ID; a stage's span ID is a hash of the run ID and the stage name. Two
things follow, and they are the reason for the choice.

**A run ID is enough to find the trace.** The string in `RunResult.RunID`, in a
log line, or under a star in the constellation view is all an operator needs —
`telemetry.TraceIDFor(runID)` is the same function the exporter used, and it is
exported so a support tool can call it. There is no correlation field that
somebody had to remember to log.

**A fleet assembles one trace with no propagation.** A worker process never saw
the driver's context; all it has is the envelope, which already carries the run
ID and the stage. It derives the same trace ID and the same *stage* span ID, so
its spans hang in the right place in a tree it never saw. Task and call spans
mix in the process instance, so the driver's view of a task — dispatch, queue,
wait — and the worker's view of it — the call — are two spans under one stage
rather than two processes writing one ID.

The shape:

```
loom.run <pipeline>                     $ spent, budget, driver
  loom.stage <id>                       stage kind
    loom.task <stage>                   attempts, rung, model, records
      model.call <model>                cost, tokens, latency   [client span]
      • retry / cache.hit               span events
```

### Sampling decides at the end

A run of ten thousand records is ten thousand task spans and nobody wants them.
But the interesting ones are exactly the ones a head sampler cannot recognize
yet: the task that failed, the one that climbed the ladder, the one that took
four minutes.

So spans are held until their task settles, and the decision is made with the
outcome in hand:

```go
Sample: telemetry.Sampling{
    Ratio: 0.02,             // 2% of ordinary tasks
    Slow:  30 * time.Second, // and every task slower than this
}                            // and every failure, retry and escalation
```

Run and stage spans are always kept — they are few, and they are the skeleton
the sampled tasks hang from.

The ratio is applied to a hash of the span's own derived ID, so the decision is
deterministic: a replayed event stream keeps the same tasks, and two processes
tracing one run do not disagree about which ones are interesting.

---

## 6. It cannot stall a run

Telemetry attached to a pipeline must never be able to stop it. Three
properties, all tested:

1. `Handle` runs inline on the event bus and does map work under a mutex — no
   I/O, ever.
2. Spans go onto a **bounded** queue with a non-blocking send. A collector that
   has gone away costs `loom_telemetry_spans_dropped_total`, not a stalled
   pipeline. The package's test drives a thousand events through an exporter
   that never returns and asserts the bus keeps moving.
3. Export happens on one background goroutine, in batches, with a timeout and a
   bounded retry — and a batch is retried only when the collector's answer says
   retrying could help. A `400` means the payload was rejected; offering it
   again is a way to fail three times.

---

## 7. Liveness and readiness are different questions

```go
tel.Ready("queue", func(ctx context.Context) error { return q.Ping(ctx) })
tel.Ready("state-dir", func(context.Context) error { return checkWritable(dir) })
```

`/healthz` answers 200 while the process is alive. `/readyz` runs the
registered checks and answers 503 with the names that failed. The distinction
matters to exactly the systems that will run this: a worker that is alive but
cannot reach its queue should be **left running and sent nothing**, and an
orchestrator can only make that call if the two endpoints say different things.

`telemetry.Serve(ctx, addr)` is the one line a long-lived process — a worker
(`loom.Serve`), a stream job (`loom.Stream`), a serving desk (`recall.Desk`) —
needs in order to be operable.

---

## 8. Why OTLP is written by hand

Going through the official OpenTelemetry Go SDK would be the obvious choice,
and it is the wrong one here.

Loom's dependency list is provider SDKs and nothing else, on purpose: it is a
framework other programs import, and every module it drags in is a module its
callers must reconcile with their own — a version, a transitive graph, a
release cadence. The OTLP wire format is a stable, versioned *specification*
with a documented JSON encoding, and what this package needs of it — one
request shape, spans with attributes and events — is about a hundred lines.

So the exporter speaks OTLP/HTTP with `Content-Type: application/json`,
following the protobuf-to-JSON rules the specification pins: trace and span IDs
as lowercase hex rather than base64, 64-bit values as strings, enums as
numbers. Any OTLP-compatible collector accepts it — the OpenTelemetry
Collector's own receiver, Jaeger, Tempo, a vendor endpoint — on the same
`:4318` a deployment already points everything else at, and
`OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_SERVICE_NAME` are read when the fields
are left empty, so a deployment that already sets them gets Loom's traces
without a code change.

That trade looks different for an *application*, which should just use the SDK.
`Options.Traces` takes any `Exporter`, so an application that has one already
can hand it over and skip all of this.

The `examples/beacon` program stands up an OTLP endpoint on loopback and posts
to it over a socket, so the encoding is exercised offline rather than described.

---

## 9. What to put on call

```promql
# the wallet is about to stop the work
loom_budget_headroom_usd < 1

# dollars per hour, by stage — the graph to leave on a wall
sum by (stage) (rate(loom_cost_usd_total[5m])) * 3600

# the ladder started climbing: a model regression, or the inputs changed
  rate(loom_escalations_total[15m])
/ rate(loom_tasks_total{outcome="completed"}[15m]) > 0.2

# throttled against a rate limit, rather than slow
histogram_quantile(0.95, sum by (le, stage) (rate(loom_task_queue_seconds_bucket[5m]))) > 30

# a stream job whose event time has stopped advancing
time() - loom_watermark_seconds > 300

# the exporter has stopped telling the truth
loom_telemetry_overflowed_metrics > 0 or rate(loom_telemetry_spans_dropped_total[5m]) > 0
```

The watermark is exported as a **timestamp** rather than as a lag, because a
lag computed in the process is as stale as the last event — which for an idle
stream is exactly when the number matters most. `time() -
loom_watermark_seconds` is computed against the scrape's own clock and keeps
rising while nothing happens, which is the alert.

---

## 10. Map of the code

| File | |
|---|---|
| [`telemetry/telemetry.go`](../telemetry/telemetry.go) | Options, the declared metric surface, the event folding, the ops handler |
| [`telemetry/metrics.go`](../telemetry/metrics.go) | The registry, the cardinality bound, Prometheus text exposition |
| [`telemetry/trace.go`](../telemetry/trace.go) | Span model, derived identifiers, the sampler, the batching tracer |
| [`telemetry/otlp.go`](../telemetry/otlp.go) | OTLP/HTTP JSON encoding, retries, and `Capture` for tests |
| [`telemetry_test.go`](../telemetry_test.go) | The exposition against `RunResult`, on real runs |
| [`examples/beacon`](../examples/beacon) | A run, its scrape, and its trace off the wire — offline |

---

## 11. Try it

```sh
go run ./examples/beacon          # run, scrape, and print the trace
go run ./examples/beacon -serve   # leave the endpoint up and curl it

curl -s localhost:9464/metrics | grep loom_cost
curl -s localhost:9464/readyz
```

Against a real collector:

```sh
docker run -p 4318:4318 otel/opentelemetry-collector:latest
go run ./examples/beacon -otlp http://localhost:4318
```
