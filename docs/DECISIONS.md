# Decisions: typed questions, a System One model, and an LLM above it

Most of what a pipeline asks a model is not "write something." It is *which
queue*, *how angry*, *is this an attack*, *does this need a human* — closed
questions whose answers are read by code, not by a person. Loom has always
answered them the only way a text model can: ask for JSON, parse it back into
the type the code wanted all along, `Validate` it, and escalate when the parse
or the check fails. Every step of that round trip is a way to be wrong while
the call succeeds.

`pipeline.Decide` answers them the way the question is shaped. You declare the
questions and their types; a **decision model** answers each one with a value
of that type, a probability distribution, and a calibrated confidence; and an
LLM above it on the escalation ladder answers the ones the decision model says
it is unsure of — under the same schema.

```go
triaged := inbox.Decide("triage", pipeline.DecideSpec{
    Binding: model.Binding{Model: typesafe.Jev,                 // System One
        Escalation: []string{"claude-sonnet-5"}},               // System Two
    Prefix: policy,                                             // stage-wide context
    State:  "Plan: {{.plan}}\nSubject: {{.subject}}\n\n{{.body}}",
    Questions: model.Questions{
        "queue":  model.Choice("Which team should own this?", queues),
        "urgent": model.Noul("Does this need a human within the hour?"),
        "anger":  model.Score("How frustrated is the customer?",
                      "calm", "annoyed", "frustrated", "furious"),
    },
    MinConfidence: 0.65,   // the weakest answer below this climbs the ladder
})
// each record now has .queue (string), .urgent (probability), .anger (score),
// and .decision — every distribution, every confidence, and who answered
```

`go run ./examples/reflex` runs this on a support inbox, offline, and
`-serve :8079` opens a board that shows every answer's distribution and why
each escalated message escalated.

---

## 1. The model

[TypeSafe](https://typesafe.ai/)'s **Jev** (early access, September 2026) is
the first of what TypeSafe calls *System One models*, after Kahneman's fast,
intuitive System 1. It does not generate text. `POST /v1/systemone` takes a
**state** — the material to judge, a string or any JSON — and a map of named,
typed **questions**:

| Type | Asks | Returns |
|---|---|---|
| `noul` | a yes/no question | `noul`: the probability of yes |
| `choice` | pick one of the declared options (name → description) | `choice`, `probabilities` over every option, `confidence` |
| `score` | rate on an ordered rubric of 2–10 levels | `score` (probability-weighted position), `legend`, `probabilities`, `confidence` |

A Choice or Score confidence comes from the shape of the distribution —
concentrated is high, spread is low. A Noul needs none: its value already is a
probability. Answers come back in roughly 70–500 ms. Billing is input-only at
$0.042 per million tokens; **output is free**. Every call also bills a fixed
preamble of about 270 input tokens on top of the state and the questions;
the provider registers it as `model.Info.InputOverheadTokens`, so `Explain`
counts it per call and its ceiling stays a bound.

Two properties follow that shape how it belongs in a pipeline:

- **It cannot answer outside the schema.** A Choice is one of the options you
  declared, not a near-synonym, not prose around one. Nothing to parse means
  nothing to fail parsing.
- **More questions cost almost nothing.** Input is billed once per call, output
  is free, so asking every question you *might* need about a record in one call
  — TypeSafe's "speculative fan-out" — costs about what one question does, and
  the code decides afterwards which answers it reads.

The published use cases are the shapes this doc's examples take: guardrails
(screening prompts, replies and tool calls before an agent acts), intent and
model routing, ticket triage, moderation, bulk labelling, fraud screening,
agent verification, and real-time loops in games, robots and browsers where a
decision has to land in tens of milliseconds.

## 2. Where it lives in Loom

**A capability of a provider, not a new kind of pipeline.** `model.Request`
gains `Questions`; `model.Response` gains `Answers`. A provider that answers
questions natively declares it:

```go
func (p *Provider) Capabilities() model.Capabilities {
    return model.Capabilities{Decisions: true}   // and Text: false
}
```

Every provider that says nothing is a text model, which is what every provider
was before this. `providers/typesafe` is the adapter: one HTTP call, the key
resolved per call through the task's secret broker, 429/5xx transient, other
4xx permanent, and an answer set that does not match the schema — which Jev
cannot produce, and which is checked anyway — semantic.

**`Decide` compiles to an `Infer`.** A decision is a model call per record
whose answer type is declared up front, so it is an `InferSpec` carrying
`Questions`. That is the whole trick: least-privilege envelopes, egress
allowlists, admission against rate limits, the content-addressed cache,
budgets, the escalation ladder, routing, telemetry, the constellation view and
`Explain` all apply unchanged, because none of them had to learn a new
operator.

The fingerprint appends the questions and the gate — *appends*, so an ordinary
`Infer` stage keeps the fingerprint, and the warm cache, it always had.

## 3. The ladder: System One answers, System Two reviews

The runner does one of two things per record, depending on what the rung it is
on can do:

- **A decision model** is asked the questions directly. Its answers are checked
  against the schema and its confidences are *calibrated* — probabilities, not
  opinions.
- **A text model** is asked the same questions through a prompt `model.Generative`
  renders (options with their descriptions, the rubric with its indices, the
  exact JSON shape), and `model.ParseDecision` holds the reply to the same
  schema. An LLM that answers `"sales"` when the options were `billing | bug`
  has produced a **semantic failure, which escalates** — the invention never
  reaches the record.

So whichever rung answers, the record has the same fields of the same types.
The latency and the price differ; the shape does not.

**The gate.** `MinConfidence` is compared with the certainty of the *weakest*
answer — acting on five judgments is only as safe as the least sure of them.
Certainty is one scale for all three types: a Choice or Score's confidence, and
for a Noul its distance from a coin flip, `|2p − 1|`. Below the gate the
answers are a semantic failure, and the scheduler does what it already does
with one: climbs the ladder.

Two rules keep the gate honest:

- **It gates calibrated confidences only.** An LLM's stated confidence is its
  opinion of itself. It is recorded in the decision — the board shows it,
  labelled as stated — and never gated on.
- **The top rung is never failed for being unsure.** There is nothing above it
  to ask. A low-confidence decision there is *kept* and marked
  `"low_confidence": true`, so a Jev-only stage with a gate is a triage into
  "confident" and "needs a person" rather than a stage that dead-letters its
  hardest records. `examples/reflex`'s guard treats unsure as not clean.

Because the gate produces ordinary semantic verdicts, `loom.WithRouting()`
learns from it with no extra wiring: a bucket of records Jev is reliably unsure
about starts on the LLM, and stops paying for the call whose answer was always
going to be sent up.

## 4. What the record holds

For each question, a field named for its ID: the chosen option (string), the
probability of yes (float), or the weighted score (float). Filters and
templates below read `{{.queue}}` like any field.

The full decision goes in `OutputField` (default `"decision"`):

```json
{"model": "jev-1.13.0", "calibrated": true, "rung": 0,
 "confidence": 0.69, "weakest": "queue",
 "answers": {"queue": {"type": "choice", "choice": "bug",
                       "probabilities": {"bug": 0.89, "billing": 0.03, ...},
                       "confidence": 0.69}, ...}}
```

It is written as the plain map a record holds after a trip through JSON, so a
fresh record and one replayed from the cache are the same shape.
`model.ReadDecision` turns it back into a type.

## 5. What the projection can and cannot know

A `ParseJSON` stage hides its output shape from `Explain`: the fields come out
of the model. A decision's fields are **declared**, so every template below a
Decide renders in the projection with the right fields of the right types.

What no plan can know is the *answers*, and a filter on an answer keeps or
drops records by them. No placeholder is safe for every filter — "yes" is the
most work for an urgency filter and none at all for a guard — so an unsampled
decision marks the stages below it **estimated**, exactly as ParseJSON does,
and says which questions to name:

```go
loom.WithStageSample("guard",  map[string]any{"injection": 0.0, "data_request": 0.0}),
loom.WithStageSample("triage", map[string]any{"queue": "bug", "urgent": 1.0, ...}),
```

The studio does this for you: its filters are declarations too, so for each
question it picks the answer the most filters below it keep, and states the
assumption beside the price.

A gate escalates by design, so its cost belongs in the plan. `Explain` cannot
know the fraction that will fall below the gate, so it warns with the worst
case — every record climbing to the top — next to the ceiling:

```
stage "triage" sends records whose weakest answer is below 0.65 confidence up its
ladder: if every record escalated to claude-sonnet-5 the stage would cost up to $X more
```

## 6. Compile-time refusals

- A text stage (`Infer` without questions, `ReduceAI`, an `Iterate` step)
  bound to a decision-only model is refused when the pipeline compiles — the
  whole ladder is checked, so the refusal comes before the rungs below it are
  paid for.
- Questions are validated at compile time: identifier IDs (they become record
  fields), non-empty instructions, at least two options per Choice, 2–10 levels
  per Score — the API's own limit, which would otherwise be an HTTP 400 on the
  first record.

## 7. UI

- **Loom Studio** has a *Typed questions* step. Its inspector edits the
  decision model, the ladder, the confidence gate, the state template and the
  questions themselves — yes/no, pick-one with described options, rate on a
  rubric — and it prices, validates and exports to Go like every other step.
  `go run ./examples/reflex -studio localhost:8078` opens the reflex pipeline
  on the canvas.
- **The decision board** (`go run ./examples/reflex -serve :8079`) shows each
  message's typed answers as distributions, meters and rubric positions against
  the gate; for an escalated message, what Jev said first and which answer was
  too unsure; and the money — Jev's calls, the LLM's, and the projection of the
  same triage with the LLM alone.
- **The constellation view** needs nothing new: a decision is a model call, and
  its drill-down shows the rendered questions beside the state.

## 8. Using the real API

```go
reg := model.NewRegistry()
typesafe.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 600})  // registers "jev-latest"
anthropic.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 50})

loom.WithSecrets(map[security.SecretRef]string{
    typesafe.DefaultSecretRef:  os.Getenv("TYPESAFE_API_KEY"),
    anthropic.DefaultSecretRef: os.Getenv("ANTHROPIC_API_KEY"),
})
```

Jev is registered **without a tier** on purpose: a tier means "whatever model
does this kind of work," and a text stage asking for `TierFast` must never
resolve to a model that cannot write. Decision stages name it.

`typesafe.WithBaseURL` points the provider at a gateway or proxy;
`typesafetest.Server` is an in-process System One server speaking the real wire
protocol, which is how the tests and the example run offline.

## 9. Not done, deliberately

- **Structured state.** Jev accepts JSON state; Loom sends the rendered
  template as a string, because a template is how every other stage says what
  the model sees, and `{{json .}}` already renders a record as JSON. A
  `StateJSON` mode is a small addition when a use case needs it.
- **Tool selection as a decision.** Function calling maps onto one Choice for
  the tool (with an explicit "none") plus speculative Choices per argument.
  `examples/mcp-desk`'s dispatch could run on it; it doesn't yet.
- **A decision ReduceAI.** Aggregation is generation. It stays on text models.

## Sources

- [TypeSafe AI](https://typesafe.ai/) · [Introducing System One models & Jev](https://typesafe.ai/blog/introducing-system-one-models-and-jev)
- [TypeSafe API reference](https://docs.typesafe.ai/api)
- [Pydantic AI: TypeSafe (Jev)](https://pydantic.dev/docs/ai/models/typesafe/)
- [MarkTechPost: A coding guide to TypeSafe AI Jev](https://www.marktechpost.com/2026/09/23/a-coding-guide-to-typesafe-ai-jev/)
- [Top use cases of Jev (CloudRaft)](https://www.cloudraft.io/blog/top-use-cases-of-jev-typesafe-ai-model)
- [awesome-jev-by-typesafe](https://github.com/Anil-matcha/awesome-jev-by-typesafe)
