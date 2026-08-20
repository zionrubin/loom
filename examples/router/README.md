# router — not paying for the call that was going to fail

```sh
go run ./examples/router
```

Offline, against a deterministic mock model. No key, no network, no cost.

## What it shows

300 contracts go through one `Infer` stage bound to a fast model with a
stronger one behind it. Two thirds of them are "dense", and the fast model
cannot do those — its output fails `Validate` and the task escalates.

A flat ladder pays for that doomed fast call on **every dense contract, every
time, forever**. Nothing caches a call that produced no result, and the ladder
is stateless, so contract 300 enters where contract 1 did.

The example runs the pipeline twice — once flat, once with
`loom.WithRouting(...)` — and prints what each one paid:

```
                                calls      cost($)
flat ladder                       500       0.0675
routed                            326       0.0595
difference                        174       0.0080

300/300 records carry identical output.
```

Then it hands the profile the routed run wrote to `loom.Explain`, which prices
the escalations for a run ten times the size — a number a projection cannot
compute without one, because its columns price one call per record at the
*base* model.

## The three things worth noticing

**Nothing was trained.** `route.ByField("kind")` is the entire configuration.
The training signal is the `Validate` function already in the pipeline: it runs
on every record and already says whether the model that ran was strong enough.

**The answers do not move.** The router picks a *starting* rung. Validation
still runs and escalation still climbs, so a wrong guess costs a call and never
an answer — which is why the run prints 300/300 identical.

**The saving is measured, not claimed.** A slice of the tasks the router wanted
to move are started at the bottom rung anyway. They cost real money and they
are the only evidence that ever contradicts a skip, so the report never prints
the saving without them:

```
routing: 174 task(s) started above the bottom rung, skipping 174 call(s)
         worth $0.0099; 6 probe(s) held back, 0 answered at the bottom
         (0% of skips would have been wrong)
```

## The ladder ratio decides what this is worth

The mock ladder here is priced about four times apart — the Haiku-to-Sonnet
shape. Skipping a doomed call saves that rung's price, so:

- **In calls it is always the same**: 35% fewer here, which is latency and
  rate-limit budget whatever the models cost.
- **In dollars it depends on the ratio**: 12% here. On a ladder 50× apart it
  would be almost nothing, because the expensive rung dominates and the wasted
  cheap call was never where the money was.

Change the prices in `registry()` and watch the decision change — at a wide
enough ratio the router correctly stops routing at all.

[docs/ROUTING.md](../../docs/ROUTING.md) is the design.
