# beacon — the run, as a monitoring system sees it

Four support tickets through a stage bound to a cheap model with a rung above
it, and a validator that rejects what the cheap model returns for two of them.
The run climbs, pays twice for those records, and finishes inside its budget.
That is the ordinary shape of an AI workload — and this example is about what a
deployment can *see* of it from the outside.

```
go run ./examples/beacon              # run, scrape, print the trace
go run ./examples/beacon -serve       # leave the ops endpoint up to curl
go run ./examples/beacon -otlp URL    # post spans to a real collector
```

Entirely offline: mock models, no keys, no network. The OTLP collector the
spans are posted to is an HTTP endpoint this program stands up on loopback, so
what gets printed came back off a socket in the format a real collector reads,
rather than out of a mock.

## What it shows

Loom already observes itself — an event bus, a run report, the constellation
view. All of it is in-process and all of it ends when the process does, which
is the right shape for building a pipeline and the wrong one for running a
deployment. This is the other half.

```
── the run ─────────────────────────────────────────────
   run run_56d77c6dd28f92a3
   4 tickets classified, 6 model calls, $0.0007 of a $0.50 budget

── the scrape (http://127.0.0.1:9464/metrics) ───────────
   loom_budget_headroom_usd{pipeline="beacon"} 0.5
   loom_cost_usd_total{model="mock-deep",pipeline="beacon",stage="classify"} 0.00047145
   loom_cost_usd_total{model="mock-fast",pipeline="beacon",stage="classify"} 0.00023276
   loom_escalations_total{pipeline="beacon",stage="classify"} 2
   loom_model_calls_total{model="mock-deep",outcome="ok",…} 2
   loom_model_calls_total{model="mock-fast",outcome="ok",…} 4
   loom_task_retries_total{class="semantic",…} 2
   loom_tasks_total{outcome="completed",…} 4
   …

── the trace, off the wire ─────────────────────────────
   loom.run beacon                 187ms  $0.0007
     loom.stage tickets                 0s
     loom.stage classify             187ms
       loom.task classify              187ms  attempts=2 rung=1 mock-deep •retry
         model.call mock-fast              4ms  $0.0001
         model.call mock-deep             12ms  $0.0002
       loom.task classify              131ms  attempts=2 rung=1 mock-deep •retry
         model.call mock-fast              4ms  $0.0001
         model.call mock-deep             12ms  $0.0003
```

**Four tasks, six calls.** The counters say six because cost is counted at the
call, where the provider counts it. Two records were rejected on the cheap
model and answered on the rung above, and the invoice has both. A cost metric
fed from task *completions* would report this run as two-thirds of its real
price — which is the money an operator most wants to see.

**The unit is the dollar.** `loom_budget_headroom_usd` is the series to page
on, and nothing had to call `Explain` for it to exist: the run's ceiling rides
on its own header event. It reads whole in the output above because the run has
finished — headroom is what is left of the tightest ceiling *in flight*, and
nothing is.

**The run ID is the trace ID.** The trace at the bottom is found by pasting
`res.RunID` into a tracing backend; `telemetry.TraceIDFor` is the same function
the exporter used. Nothing was propagated, logged or correlated — which is also
how a worker process's spans join a trace it never saw the start of.

**Sampling keeps what somebody would open.** The ratio here is **zero**, and
both escalated tasks are traced anyway. The decision is made when a task
settles rather than when it starts, so it can see the failure a head sampler
could not. The two tasks that answered on the first rung are the ones that got
dropped.

**Liveness and readiness are different questions.** `/healthz` says the process
is alive; `/readyz` runs the registered checks and answers 503 with the names
that failed. A worker that is alive but cannot reach its queue should be left
running and sent nothing, and an orchestrator can only make that call if the
two endpoints disagree.

## Curl it

```sh
go run ./examples/beacon -serve &
curl -s localhost:9464/metrics | grep loom_cost
curl -s localhost:9464/readyz
```

## Against a real collector

```sh
docker run -p 4318:4318 otel/opentelemetry-collector:latest
go run ./examples/beacon -otlp http://localhost:4318
```

Or set nothing at all in code: `OTEL_EXPORTER_OTLP_ENDPOINT` and
`OTEL_SERVICE_NAME` are read when the corresponding options are empty, so a
deployment that already exports telemetry from its other services gets Loom's
without a code change.

The full metric surface, the trace shape, the cardinality bound and what to
alert on are in [docs/TELEMETRY.md](../../docs/TELEMETRY.md).
