# Providers: mocks, hosted APIs, local inference

Model setup is the one part of a pipeline that should change when you move
from development to production. Keep it in its own `registry()` function and
that move stays a one-function change — nothing in `build()` mentions a
provider.

- [Mocks (develop here)](#mocks-develop-here)
- [Anthropic](#anthropic)
- [OpenAI](#openai)
- [llama.cpp (local)](#llamacpp-local)
- [Pricing and limits](#pricing-and-limits)
- [Mixing providers](#mixing-providers)

## Mocks (develop here)

Deterministic, free, offline — and they model the provider prompt cache, so
the cache accounting you see offline is the accounting you get in production.
Every example in this repo runs green with no API key for this reason.

```go
func registry() (*model.Registry, error) {
    reg := model.NewRegistry()
    if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
        model.WithHandler(answer)); err != nil {
        return nil, err
    }
    if _, err := model.RegisterMock(reg, "mock-deep", model.TierDeep,
        model.WithHandler(answer)); err != nil {
        return nil, err
    }
    return reg, nil
}

// One handler can serve every stage: branch on the prompt.
func answer(req model.Request) (string, error) {
    if strings.Contains(req.Prompt, "Summarize these") {
        return "…the reduce answer…", nil
    }
    return `{"severity": "high", "topic": "billing"}`, nil
}
```

Other mock options, useful for exercising recovery paths:

| Option | Use |
|---|---|
| `model.WithFailures(errs...)` | Script errors on successive calls — test retry and escalation |
| `model.WithLatency(d)` | Simulate per-call latency — test stragglers and admission control |
| `model.WithEndpoint(host)` | Give the mock a fake endpoint so egress policy applies |

`mock.Calls()` returns the call count — the assertion behind every cache-replay
test. To reach the mock from a test given only the registry:

```go
info, _ := reg.Get("mock-fast")
calls := info.Provider.(*model.Mock).Calls()
```

Script a semantic failure to prove the ladder works: have the handler return
invalid JSON on the first call and valid JSON after, then assert the run
succeeded and the deep model was used.

## Anthropic

```go
import "github.com/zionrubin/loom/providers/anthropic"

reg := model.NewRegistry()
if err := anthropic.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 50}); err != nil {
    return nil, err
}
```

Registers three models sharing one provider:

| Model | Tier | $/MTok in | $/MTok out |
|---|---|---|---|
| `claude-opus-4-8` | deep | 5 | 25 |
| `claude-sonnet-5` | balanced | 3 | 15 |
| `claude-haiku-4-5` | fast | 1 | 5 |

The key goes through the broker, never into a task:

```go
loom.WithSecrets(map[security.SecretRef]string{
    anthropic.DefaultSecretRef: os.Getenv("ANTHROPIC_API_KEY"),
})
```

`anthropic.Endpoint` is `api.anthropic.com`, auto-allowed on the egress list of
stages bound to these models — and only those stages. `anthropic.New(secretRef)`
builds a provider against a non-default secret reference.

## OpenAI

Identical shape:

```go
import "github.com/zionrubin/loom/providers/openai"

openai.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 50})
loom.WithSecrets(map[security.SecretRef]string{
    openai.DefaultSecretRef: os.Getenv("OPENAI_API_KEY"),
})
```

| Model | Tier | $/MTok in | $/MTok out |
|---|---|---|---|
| `gpt-5.4` | deep | 2.50 | 15 |
| `gpt-5.4-mini` | balanced | 0.75 | 4.50 |
| `gpt-5.4-nano` | fast | 0.20 | 1.25 |

Endpoint `api.openai.com`.

## llama.cpp (local)

One `Provider` is one server, because one server is one loaded model. A fast
model and a strong one are two servers on two ports — which makes a local
escalation ladder an ordinary binding.

```go
import "github.com/zionrubin/loom/providers/llamacpp"

fast := llamacpp.New("http://127.0.0.1:8080", llamacpp.WithName("local-fast"))
props, err := llamacpp.Register(ctx, reg, fast, "local-fast", model.TierFast)
// props.Model, props.Slots, props.ContextSize — what the server says about itself

deep := llamacpp.New("http://127.0.0.1:8081", llamacpp.WithName("local-deep"))
llamacpp.Register(ctx, reg, deep, "local-deep", model.TierDeep)
```

Three things differ from a hosted provider and all three are the point:

- **Registering contacts the server**, so a server that is not up fails while
  you are still wiring the pipeline, not on the first record.
- **The device sets the ceiling.** The server's slot count becomes
  `Limits.MaxConcurrent`, so the scheduler admits exactly as many calls as the
  hardware decodes at once instead of letting them queue invisibly inside the
  server. Eight workers against two slots is admitted two at a time.
- **Pricing is zero and means it.** A local run reports `$0.0000`. Reused
  prefix tokens still come back as `CacheReadTokens` — on llama.cpp the prompt
  cache *is* the KV cache — so the report reads the same as a hosted run.

Egress is loopback and still on the allowlist rather than exempt from it: a
stage bound to a local model carries `127.0.0.1` and nothing else, so the
envelope states plainly that those records cannot reach a vendor. That is the
enforceable version of "this data stays on the box".

For a server behind `--api-key`, pass `llamacpp.StaticKey(key)` to `Register`
and use `llamacpp.WithAuth(ref)` on the provider.

`examples/on-device` runs this whole path offline against real loopback
sockets — no weights downloaded, nothing installed.

## Pricing and limits

Registering a model by hand:

```go
reg.Register(model.Info{
    ID:       "my-model",
    Provider: p,
    Tier:     model.TierBalanced,
    Pricing:  model.Pricing{InputPerMTok: 3, OutputPerMTok: 15},
    Limits:   model.Limits{RequestsPerMinute: 50, TokensPerMinute: 100_000},
    SecretRef: "my_api_key",
})
```

Cache rates default from `InputPerMTok`: reads at ×0.10, writes at ×1.25. So a
prefix pays a 25% premium once and returns 90% on every reuse — which is why
the planner enables prefix caching only on stages issuing more than one call.
`Pricing.Saved(usage)` reports what the prefix cache was worth, and is
*negative* while a write is still unamortized. That is honest, not a bug.

`Limits` feeds admission control, and `RequestsPerMinute` is what makes
`proj.AdmissionFloor()` meaningful — the wall-clock floor no amount of
concurrency gets under. Zero means unlimited.

The first model registered for a tier becomes that tier's default;
`reg.SetTier(tier, id)` overrides.

## Mixing providers

Nothing stops a registry from holding models from several providers, and a
binding does not care where its model came from — so a fast local model
escalating to a hosted one is just:

```go
Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"claude-sonnet-5"}}
```

Each stage's envelope carries only the endpoints its own models need, so the
local stages still cannot reach the network.
