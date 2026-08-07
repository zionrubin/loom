# on-device — a pipeline whose models run on your own hardware

An on-call incident desk. Reports arrive carrying customer names, addresses
and account numbers; a small model triages every one of them; a larger model
writes the brief the on-call engineer reads first. Both models are llama.cpp
servers on this machine.

```sh
go run ./examples/on-device                       # offline, nothing installed
go run ./examples/on-device -state /tmp/loom-dev  # then again: replayed free
go run ./examples/on-device -view localhost:8077  # watch it
```

Without `-fast` or `-deep` the example starts its own llama.cpp-compatible
servers in-process ([`llamacpptest`](../../providers/llamacpp/llamacpptest)),
on real loopback sockets, so it runs with no GPU, no model file, and nothing
downloaded. Against real servers:

```sh
llama-server -m qwen3-4b-instruct-q4_k_m.gguf  --port 8080 --parallel 2 &
llama-server -m qwen3-14b-instruct-q4_k_m.gguf --port 8081 &

go run ./examples/on-device -fast http://127.0.0.1:8080 -deep http://127.0.0.1:8081
```

The pipeline is unchanged between the two. A binding names a model, not a
machine.

## The pipeline

```
incidents ─ triage (local-fast → local-deep, shared rubric prefix)
              └─ pageworthy (Go filter) ─ brief (local-deep, tree reduce)
```

- `triage` — the small model reads one incident and answers with JSON,
  validated against the four severities. The severity rubric is the stage's
  `Prefix`, identical on all eight calls.
- `pageworthy` — pure Go, keeps the incidents that page.
- `brief` — the large model folds them into one paragraph, `FanIn` 4.

## Two servers, because a server is one model

`llama-server` loads one model, so a deployment that wants a fast model and a
strong one runs two of them on two ports. That is what makes the escalation
ladder here ordinary rather than special:

```go
Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"local-deep"}}
```

`inc-6` is the incident whose evidence contradicts itself. The small model
declines to place it, `Validate` rejects the answer as semantically bad, and
the scheduler retries the record one rung up — on the larger local model,
which calls it a sev1. Speculative decoding's shape (cheap model proposes,
strong model settles it), one altitude up, on hardware you own.

## What the run prints, and why each number is there

```
--- what running on your own hardware changed ---
cost         $0.0000 over 12 model call(s) — the run budget never came near binding
             the same pipeline at hosted rates: $0.0137 expected, $0.0314 ceiling (projected, no calls made)
ceiling      local-fast 2 slot(s), peak 2 in flight · local-deep 1 slot(s), peak 1 in flight
egress       [127.0.0.1] — the executor checks this before every call
credentials  0 secret grant(s): model:local-deep model:local-fast
kv cache     1915 of 3554 prompt tokens served from the shared rubric, recomputed for nobody
```

**cost** — the zero is not the interesting part; the line under it is. That
figure is `loom.Explain` over the identical pipeline against a registry where
the two local IDs carry first-party rates instead of nothing, so it needs no
key, opens no socket, and issues no call. It is the bill this run did not get.

**ceiling** — the run is given eight workers and the two servers have three
slots between them, on purpose. A local model's scarce resource is the device,
not a per-minute quota, so `llamacpp.Register` asks each server how many
sequences it decodes at once and registers that as `Limits.MaxConcurrent`; the
scheduler admits exactly that many. The peaks are what the servers *counted*,
and they do not enforce their own slot counts — so a peak above the ceiling
would be a real failure rather than something the fake absorbed. Without
admission control the same run peaks at 8 against 2 slots, and the excess
queues inside `llama-server` where Loom can neither see it nor schedule
around it.

**egress** and **credentials** — both read off the envelope
`plan.Compile` produces for the `triage` stage, not asserted about it. The
allowlist is loopback and nothing else, and the executor checks it before
every call, so the customer records above cannot reach a vendor even by
mistake. There are no secret grants at all, because a local server needs no
credential — there is nothing to leak. (A server started with `--api-key` is
covered by `llamacpp.WithAuth`, and is then a broker-resolved secret like any
other.)

**kv cache** — Loom's shared prompt prefix and llama.cpp's KV cache are the
same mechanism here. Against a hosted provider the planner weighs a cache
write against the calls that will read it, because a write costs a premium;
locally there is nothing to weigh, since the KV cache is a byproduct of the
forward pass the model was making anyway. So the adapter asks for reuse on
every call, and the report counts what it got.

## Running it twice

```sh
go run ./examples/on-device -state /tmp/loom-dev
go run ./examples/on-device -state /tmp/loom-dev   # 0 calls, 0 tokens
```

Content-addressed caching does not care that the work was free: a token you
generated yourself still cost you a GPU-second, and Loom does not pay for work
twice.

## In the constellation view

```sh
go run ./examples/on-device -view localhost:8077 -slow 400ms
```

`-slow` adds latency to the in-process servers, which is what makes the
scheduling visible: eight tasks queued against two slots, admitted two at a
time, and one record climbing to the second model. Click a task for its
rendered prompt — the shared rubric appears once as the prefix, and the
incident once as the body.

## Tests

`go test ./examples/on-device` runs the whole thing offline and pins the
claims above: the ladder escalates exactly one record, admission never
exceeds either server's slots (and does use both of the wider one's), the
envelope carries loopback egress and no secret, the run costs nothing while
still counting its tokens, and `Explain` produces the hosted comparison
without making a call.
