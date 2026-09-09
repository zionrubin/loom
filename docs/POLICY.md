# Governance: the authority a pipeline author does not hold

Loom's README lists least-privilege task envelopes second, right after the
pipeline API, and the list is accurate. Every task carries an explicit,
serializable declaration of its model binding, capability grants, secret
references, egress allowlist, context bundle, budget and sandbox profile. The
planner assembles the *minimal* envelope automatically. Executors enforce it at
the moment of use. Every decision lands in an append-only audit log.

All of that is true, and none of it is governance, because of one word in the
middle of it: **automatically**. The planner assembles the minimal envelope
that satisfies *what the stage declared*. So:

```go
src.Infer("classify", pipeline.InferSpec{
    Binding: model.Binding{Model: "vendor/frontier-1"},   // I'll use this model
    Prompt:  "Classify this ticket from {{.customer}}: {{.subject}}",
}, pipeline.WithGrants(security.ToolCap("shell.exec")))   // and this tool
```

gets a grant for `vendor/frontier-1`, a grant for the vendor's API key, the
vendor's endpoint on the egress allowlist, and a grant for `shell.exec` — all of
it minimal, all of it audited, and all of it decided by the person who wrote the
line. Least privilege answers "what does this task need?" It never asked the
prior question: **who decides what it may need?**

In a deployment those are two different people. A platform team decides which
models may see customer data, which tools are reachable from production, which
hosts a job may talk to, and what a run may spend. An analyst writes the
pipeline. Nothing in Loom expressed the first of those, and no amount of care in
the second substitutes for it — an author cannot constrain themselves.

`policy.Policy` is that missing authority.

---

## 1. A policy is a predicate over envelopes

The design falls out of something already true. The envelope is *the complete
statement of what a task may use* — that is its entire job, and the reason it is
plain serializable data. So a governing document needs no new vocabulary. It is
a predicate over envelopes:

```go
pol := policy.Policy{
    Name:   "prod-eu",
    Models: policy.Rule{Allow: []string{"local/*"}},
    Egress: policy.Rule{Allow: []string{"*.internal"}},
    Data:   map[string]policy.Rule{"pii": {Allow: []string{"local/*"}}},
    MaxCostUSD: 25,
}

res, err := loom.Run(ctx, p, loom.WithRegistry(reg), loom.WithPolicy(pol))
```

Nothing in the pipeline changes. Nothing in the pipeline *can* change to evade
it, because the thing being judged is the envelope the planner built, not the
source that asked for it.

A policy has two faces, and the difference between them is the whole design:

| | when | what it does | failure mode it closes |
|---|---|---|---|
| **Check** | once, against the compiled plan | reports every violation | a pipeline that should never have run |
| **Bound** | on every envelope built | narrows grants and egress to what is permitted | a capability reappearing *after* the check |

They are related by an invariant the tests assert directly:

> **`Bound` never widens.** If `Check` admitted an envelope, `Bound` returns it
> unchanged. If it did not, `Bound` is what makes the refusal true of the tasks
> as well as of the plan.

That is why turning a policy on cannot quietly change what a conforming
pipeline does, and why a non-conforming one cannot get its capability back by
being assembled later.

---

## 2. The gate costs nothing

Admission runs against the **compiled plan**, before a scheduler exists:

```
── governed, uncleared model ──────────────────────
   pii may only reach a self-hosted model

   self-hosted-pii: refused 2 of 3 stages checked
     - stage "briefing": data "pii on vendor/frontier-1" model not cleared for "pii": not on the allowlist
     - stage "briefing": egress "api.frontier.example" not on the allowlist
     - stage "classify": data "pii on vendor/frontier-1" model not cleared for "pii": not on the allowlist
     - stage "classify": egress "api.frontier.example" not on the allowlist
   run refused. model calls this round: 0
```

Three properties of that output are deliberate.

**Zero calls.** A pipeline the deployment will not permit is refused rather than
discovered halfway through a bill. This is the same instinct as `loom.Explain` —
answer the expensive question before paying it — applied to a different
question. Explain asks *what will this cost*; a policy asks *may this happen*.
Both are answered from the plan, so `loom.Explain` refuses the same plan a run
would, for the same reasons, and a pipeline is never priced and then found
inadmissible.

**Every violation, at once.** A plan refused for four reasons should be fixed
once, not four times. The report is sorted deterministically, so the same plan
produces the same text and a CI job can diff it.

**The rule that refused it.** `not on the allowlist` and `denied by "shell.*"`
are different facts, and an operator debugging a refusal needs to know which.

The error is a `*policy.Denied` carrying the whole `Report`, so a caller can
render the violations instead of scraping a message:

```go
var d *policy.Denied
if errors.As(err, &d) {
    for _, v := range d.Report.Violations { ... }
}
```

---

## 3. The rule language is one thing, five times

Every axis is a `Rule`, so learning it once is learning all of them:

```go
type Rule struct {
    Allow []string   // glob patterns; empty means unconstrained
    Deny  []string   // glob patterns; deny always wins
}
```

- **Deny wins.** A carve-out cannot be widened by a later broadening.
- **A non-empty `Allow` is an allowlist.** Only matches pass.
- **An empty `Allow` leaves the axis unconstrained.**

The last one is the only choice worth defending. Deny-by-default per axis would
mean a policy that governs models must enumerate every egress host to avoid
bricking the run — and every allowlist an operator *does* write is by
construction already deny-by-default for its own axis. Denying an axis outright
is `Deny: ["*"]`, so no state is unreachable.

Matching is glob over `*` and nothing else. `path.Match` was deliberately not
used: a policy names model IDs, hosts and tool names, where `/` and `.` are
ordinary characters and a separator rule would surprise whoever wrote the
pattern.

The five axes:

| Axis | Subject | Where it comes from |
|---|---|---|
| `Models` | model ID | every `model:` grant, so the whole escalation ladder |
| `Tools` | qualified tool name (`server.tool`) | every `tool:` grant |
| `Secrets` | secret reference | every `secret:` grant |
| `Egress` | host | the envelope's allowlist *and* `loom.WithEgress` |
| `Sandbox` | profile name | the stage's `WithSandbox` |

Provider-level rules are written the same way. A deployment that registers its
self-hosted models as `local/...` says "nothing leaves the building" with
`Allow: ["local/*"]` — allowlists over naming conventions, the way image
registries and IAM ARNs already work.

`Sandbox` is a `Rule` rather than a minimum on purpose: "stronger" is not a
total order anyone agrees on — a container and a WASM sandbox are *differently*
strong, not comparably so — so a deployment writes the profiles it accepts
rather than trusting a ranking somebody invented.

---

## 4. Data classes: declared once, propagated everywhere

The question a regulated deployment asks first is not what a pipeline costs. It
is what it may show to whom.

```go
src := p.FromRecords("queue", tickets, pipeline.WithDataClass("pii"))
```

That is the whole of the author's obligation. The class is declared where data
**enters**, and the planner propagates it to every stage downstream, because
that is where the data goes:

```
queue ──► normalize ──► classify ──► briefing
[pii]      [pii]         [pii]        [pii]
```

Only the source says `pii`. The stage that calls a model is two hops down and
never mentions it. That asymmetry is the point: a scheme that required the label
to be repeated at each stage would be a scheme whose failure mode is forgetting
to repeat it — which is exactly the mistake classification exists to prevent.
Propagation is a single forward pass, which suffices because the plan's order is
topological, and it runs after fusion so a class declared on an absorbed stage
survives into the stage that absorbed it.

The policy side is a clearance table:

```go
Data: map[string]policy.Rule{
    "pii": {Allow: []string{"local/*"}},
    "phi": {Allow: []string{"local/*"}, Deny: []string{"local/experimental-*"}},
},
```

Two rules govern the table itself:

- **An empty table does not govern data.** A deployment not doing data
  governance pays nothing for the machinery.
- **A non-empty table governs every class.** A stage carrying a class the table
  does not list is *refused*, not waved through, because an unrecognized
  classification is exactly the case where silence is most expensive.

And clearance is checked against **every model the task can reach**, not just
the one it starts on:

```go
Binding: model.Binding{
    Model:      "local/llama-3.1-70b",   // cleared
    Escalation: []string{"vendor/gpt-5"}, // not
}
```

is refused. A ladder that escalates to an uncleared model leaks on the second
call, and finding that out then is finding it out too late.

**Classes do not join the stage fingerprint.** A class says who may see a
result, not what the result is, so relabelling data must not throw away answers
already paid for — and the model a policy steers work to is already in the
fingerprint through the binding.

---

## 5. What the envelope carries, and what the trail keeps

The class rides in the envelope, next to the grants:

```json
{ "stage": "classify",
  "grants": ["model:local/llama-3.1-70b", "secret:local_key"],
  "egress": {"hosts": ["inference.internal"]},
  "data_classes": ["pii"] }
```

for the same reason the grants do. A worker in another process is handed a task
and nothing else; "what am I holding?" is a question it can only answer if the
answer travelled with the work. Nothing about execution branches on it — a class
constrains which envelopes may *exist*, not what an executor holding one does.

The audit log gains both halves of what a governance review asks for:

```
policy.admit  self-hosted-pii       allowed=true  3 stages checked
data.access   local/llama-3.1-70b   classes: pii  ×8
```

The second line is the one containment cannot supply on its own. Containment
already guarantees the property — an uncleared model has no grant, so the
executor refuses the call — but a deployment asked to prove that no PII reached
an uncleared model cannot do it from an *absence of denials*. `data.access` is
the affirmative record, and it is written only for classified tasks, so a
pipeline handling nothing sensitive pays nothing for the guarantee.

The first line is the same argument one level up: a deployment that has to show
a run was authorized needs the line saying so, not only the absence of a line
saying it was not. It carries the run ID, because a decision made before any
task existed has nothing else to name — which is why `security.AuditEntry` grew
a `RunID` field alongside its `TaskID`.

---

## 6. The ceiling composes

`MaxCostUSD` behaves like the shared quota's ceiling — the run stops at
whichever limit it reaches first — with one deliberate difference:

| the run says | what happens |
|---|---|
| nothing | it inherits the policy's ceiling |
| less than the ceiling | its own, tighter figure stands |
| more than the ceiling | **refused**, not clamped |

The last row is the interesting one. A run silently given a tenth of the money
it asked for would die at ten percent and report budget exhaustion, which tells
an operator nothing about why. A refusal names the number and the ceiling.

`loom.Explain` prices the run against the ceiling it will actually be held to,
not the one the caller forgot to name.

---

## 7. Where it binds

Everything that compiles a plan admits it, which is four entry points and one
line each:

| entry point | what it admits |
|---|---|
| `loom.Run` | the run's plan, plus its budget and `WithEgress` hosts |
| `loom.Stream` | the same, for a job that never ends |
| `loom.Explain` | the same plan, without running it |
| `loom.Serve` | the worker's own compilation of the client's pipeline |

`Serve` matters more than it looks. A worker process is exactly where an
unadmitted envelope would otherwise be executed, and a worker holds the same
policy document rather than trusting the client that sent it the task.

Run-level configuration — the budget, `WithEgress` — is judged where it is
configured and *held* rather than raised, so it joins the plan's own violations
in one report. An operator fixing a policy failure should not fix the budget,
re-run, and be refused again for the model.

---

## 8. A policy is a document, not a function

`policy.Policy` is plain data with JSON tags, and that is the point:

```json
{
  "name": "prod-eu",
  "models": {"allow": ["local/*"]},
  "egress": {"allow": ["*.internal"]},
  "data":   {"pii": {"allow": ["local/*"]}},
  "max_cost_usd": 25
}
```

```go
pol, err := policy.Parse(mustRead("policy.json"))
```

A security control compiled into the program it is supposed to constrain is a
security control the constrained party can edit. As a document it is reviewed,
versioned, and deployed beside the infrastructure it governs — by the people
who own that infrastructure.

`Parse` rejects unknown fields. A misspelled rule is a rule that does not
apply, which is the worst possible failure mode for a document like this: it
must not parse rather than silently governing nothing.

---

## 9. What this is not

Honest limits, in the spirit of the rest of these docs.

**It is not a sandbox.** `Sandbox` constrains which profile a stage may
*declare*; the profiles above `inline` are still the seam described in
[ARCHITECTURE.md §4.5](./ARCHITECTURE.md), not implementations. A policy can
require `container` today and will be enforced the day a container runtime
lands — which is a reason to write the rule now, not a reason to believe it
isolates anything yet.

**It does not classify data.** It propagates classifications an author
declared. There is no scanner deciding a field is PII, and a deployment that
wants one puts it upstream of the label.

**It does not read prompts.** A policy governs what a task may *reach*, not
what a model is told or says. Prompt-level controls are a different mechanism
at a different layer, and pretending an allowlist covers them would be worse
than not having one.

**`Bound` narrows sets, not scalars.** Grants and egress are narrowed because
those are the fields something downstream of admission can still add to. The
sandbox and the binding are fixed when the stage is written, so `Check` is the
whole of their enforcement — quietly rewriting either would change what a
pipeline *does* rather than what it *may do*.

**An ungoverned capability kind passes through `Bound`.** Stripping what a
policy does not recognize would make every new capability kind silently
unusable under every policy written before it. `Bound` is defence in depth
behind an explicit gate, not the gate.

---

## 10. Try it

```sh
go run ./examples/clearance              # three rounds, one pipeline
go run ./examples/clearance -show-audit  # the trail each round leaves
```

One pipeline, three deployment policies, printed side by side: ungoverned, a
policy the pipeline violates, and the same policy against a pipeline that
conforms. The middle round makes zero model calls, and the example counts them
so the claim is a measurement rather than an assertion.
