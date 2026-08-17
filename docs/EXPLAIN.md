# Knowing what a run will cost

`loom.Explain` answers the question you want answered *before* the run: is this
$12 or $12,000, and will it take four minutes or four hours? It takes the same
options as `Run`, so one config describes a run and can be asked what that run
would cost.

```go
proj, err := loom.Explain(p,
    loom.WithRegistry(reg),
    loom.WithRunBudget(core.Budget{MaxCostUSD: 5.00}),
)
fmt.Print(proj)
```

```
projection  ticket-triage  (barrier driver, no calls issued)
stage                  model                      recs  calls   prompt   cached     exp($)     max($)    floor
tickets                                           2000      0        0        0     0.0000     0.0000       0s
classify               claude-haiku-4-5           2000   2000   198000   151924     0.9513     2.6213    3m33s
briefing               claude-opus-5              2000    287   386500      572    21.2017    49.8730   19m29s
TOTAL                                                    2287   584500   152496    22.1530    52.4943    23m2s
expected 967992 tokens for $22.1530; cannot exceed 1684276 tokens / $52.4943 before retries
run budget $5.0000 is below the ceiling: the governor will stop the run and return partial results
```

No model calls, no secrets resolved, no sockets opened, nothing written to the
state dir — safe to point at a production config.

## Why the number is sharp rather than a guess

Because of a property specific to this framework: the cheap stages are ordinary
Go functions and the expensive ones are declarative data, so `Explain`
*executes* the cheap skeleton (`Map`, `Filter`, `FlatMap`, `Combine` really run)
and models only the paid calls. Record counts are therefore exact, and every
prompt is measured after rendering against the record that will produce it —
including which tokens the provider's prefix cache will serve, since the
projection reuses the planner's own break-even rule.
`TestExplainMatchesRun` pins the prompt side to a real run token for token.

## Two numbers per stage

The difference between them is the whole point. **Expected** carries one stated
assumption — the share of `MaxTokens` a response fills
(`loom.WithExpectedOutput`, default 0.35). **Ceiling** carries none: `MaxTokens`
is a cap the provider enforces, so a first attempt cannot cost more. Hand the
ceiling to `WithRunBudget`.

## A free validation pass

Because it compiles the pipeline exactly as `Run` does, `Explain` doubles as a
validation pass — unregistered models, unparseable templates, and a prefix
reading a broadcast its stage never declared all surface before anything is
provisioned.

Anything it genuinely cannot compute becomes a named warning instead of a
confident wrong number, and the report stops calling the total a ceiling when it
no longer is:

- **Response length** is the one assumption, above.
- **A source function** is never invoked — a cost projection that reads your
  database has defeated its own purpose. Supply records with
  `loom.WithSourceSample`.
- **A `ParseJSON` stage's fields come out of the model**, so a downstream
  `Filter` testing one of them would drop every record during projection while
  keeping them in the real run — an *under*-count, the only way this can be
  wrong in the dangerous direction. Those stages are marked `~`, the run is
  reported as incomplete, and `Projection.Partial()` says so. Name the fields
  with `loom.WithStageSample("classify", map[string]any{"urgent": true})` and it
  becomes exact again. A sample is one scenario applied to every record, so pick
  the values that make the most downstream work and the result is a conservative
  bound.

## In the constellation view

Point `Explain` and `Run` at the same event handler and the
[constellation view](./VIZ.md) holds both halves of the comparison:

```go
opts := []loom.Option{loom.WithRegistry(reg), loom.WithEventHandler(v.Handle)}

proj, _ := loom.Explain(p, append(opts, loom.WithStageSample(...))...)  // before
res, _ := loom.Run(ctx, p, opts...)                                     // after
```

The projection is published on the same bus as everything else
(`stage.projected`, `run.projected`), so it arrives while the sky is still empty
— the empty state shows the forecast and the budget verdict instead of "waiting
for a run". Once the run starts, each stage's inspector reads its live cost
against what it was projected to cost, the header carries spend against forecast
and flags going past the ceiling, and the run summary gains a projected column
plus a projected-versus-actual reconciliation. The projection deliberately
survives `run.started`: it describes the pipeline, and the run that follows is
the thing it predicted.

`go run ./examples/constellation` demonstrates the whole loop.

## Related

- [ARCHITECTURE.md §4.12](./ARCHITECTURE.md#412-projection-loomexplain) — where
  projection sits in the system.
- [STUDIO.md](./STUDIO.md) — the projection as something you edit against rather
  than print at the end.
- [STREAMING.md](./STREAMING.md) — how a window is priced (one pane).
- [ALGORITHMS.md](./ALGORITHMS.md) — how an iterative stage is projected.
