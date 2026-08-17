# Sharing data across tasks: broadcasts and prompt prefixes

Tasks are isolated by design — each one gets its own records and its own
envelope, which is what lets them run anywhere. That isolation is what the two
mechanisms here work around, at two different levels:

```
broadcast       shares the bytes with every task that reads them
prompt prefix   shares the work the model does on those bytes
findings        shares the research that produced them   (see FINDINGS.md)
```

## Broadcast values

When many tasks need the same side data, broadcast it: registered once per run,
stored once by content hash, and referenced (never copied) by the tasks that ask
for it.

```go
regions := map[string]string{"t1": "EMEA", "t2": "APAC"} // any JSON-serializable value

src.
    // Go ops read broadcasts through the task's capability-checked session.
    MapTools("region", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
        table, err := core.BroadcastAs[map[string]string](ctx, s, "regions")
        if err != nil {
            return core.Record{}, err
        }
        r.Data["region"] = table[r.ID]
        return r, nil
    }, pipeline.WithBroadcast("regions")).                       // ← stage opts in

    // Prompts read them with the broadcast/broadcastJSON template functions.
    Infer("classify", pipeline.InferSpec{
        Binding: model.Binding{Tier: model.TierFast},
        Prompt: `Rubric: {{broadcast "rubric"}}
Ticket in {{.region}}: {{.subject}}`,
    }, pipeline.WithBroadcast("rubric"))

loom.Run(ctx, p,
    loom.WithBroadcast("regions", regions),                      // ← registered once
    loom.WithBroadcast("rubric", rubricText),
)
```

Three properties come from routing it through the envelope rather than a
package-level variable:

- **It scales past one process.** Envelopes carry a 64-byte hash, not the bytes,
  so tasks stay shippable to a remote or sandboxed executor and each worker
  fetches the value once instead of once per task.
- **It stays least-privilege.** Registering a value for the run doesn't expose
  it; a stage reads only what it declared, and reads are audited like any other
  capability.
- **It keeps the cache honest.** The value's content hash is part of the reading
  stage's fingerprint, so editing a broadcast recomputes exactly the stages that
  could have seen it and leaves the rest of the cache warm.

Broadcasts are read-only for the run's lifetime. For state that accumulates
*across* records, use `Combine` or `ReduceAI` at a stage boundary — shared
mutable state would make cached results depend on execution order, which is
precisely what content-addressed caching assumes away.

[`examples/support-desk`](../examples/support-desk) turns these properties into
numbers on real OpenAI models: how many bytes the run avoided copying, which
stages recompute when a shared value is edited, and a live view of every
broadcast read.

## Sharing the work, not just the bytes

A broadcast shares a value across tasks. It does not stop the *model* from
reprocessing that value on every call: a rubric sent to a thousand tasks is read
by the provider a thousand times. Put it in the stage's `Prefix` and that stops
being true.

```go
Infer("classify", pipeline.InferSpec{
    Binding: model.Binding{Tier: model.TierFast},
    System:  "You classify support tickets.",
    Prefix:  `Rubric:\n{{broadcast "rubric"}}`,    // once per task, cached provider-side
    Prompt:  "Classify this ticket: {{.subject}}", // once per record
}, pipeline.WithBroadcast("rubric"))
```

`Prefix` is a template with no record data in scope, which is the whole
mechanism: a template that cannot see the record cannot vary by record, so every
call in the stage opens with identical bytes and the provider's prompt cache
serves them — an explicit `cache_control` breakpoint on Anthropic, stable
leading bytes for OpenAI's automatic prefix cache.

The planner turns it on only when a stage issues more than one call, which is
exactly when a cache write earns itself back, and the run report states what it
cost and what it saved. The prefix joins the stage fingerprint, so editing the
rubric recomputes exactly the stages that could have seen it — and a stage
without a prefix fingerprints exactly as it did before, leaving existing caches
warm.

Locally the metaphor collapses into the thing itself: against
`providers/llamacpp` the prompt cache **is** the KV cache, a cache write costs
nothing to earn back, and reuse is asked for on every call. See
[INFERENCE.md](./INFERENCE.md).

## Sharing the research, not just the results

A broadcast shares bytes between tasks; a prefix shares them with the provider;
the result cache shares *completed work* between agents. None of them stops two
agents researching the same thing, because the result cache keys on the bytes
going in — and research does not arrive as identical bytes:

- **One question, three wordings.** Two desks wanting the same company's revenue
  write two query strings. Same subject, two cache keys.
- **Same instant, no prior.** Agents launched together all miss a cold key at
  once, all call out, and all write the same entry. Concurrency is precisely
  what defeats a write-then-read cache.
- **Enough, not identical.** A finding gathered for one purpose often carries
  four of the five fields another agent needs — worth zero to an all-or-nothing
  cache.

`loom.WithFindings` puts a **gate** in front of the tools that reach public
sources, keying on the question rather than on the bytes.
[FINDINGS.md](./FINDINGS.md) is that design in full, including the shared
backends (`findings/pgstore`, `findings/filestore`) that extend the same gate
across executor processes.
