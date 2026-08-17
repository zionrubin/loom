# Loom Studio: the pipeline before you pay for it

Loom's authoring API is a Go program. That is the right primary form — a
pipeline is code, it belongs in a repository, it should be reviewed and
versioned like everything else that spends money. But it has one property that
matters here: you find out what a pipeline costs by running it.

`loom.Explain` already fixed that for the Go form. It compiles the pipeline
exactly as `Run` does, executes the cheap Go stages against the real records,
models only the paid calls, and produces two numbers — an expected cost under a
stated assumption, and a ceiling that rests on none. The studio is what happens
when that projection stops being something you print at the end of a program
and becomes the thing you are editing against.

```
┌ canvas ─────────────────────────────┐   ┌ inspector ──────────────┐
│  Load days → Daily digest → line ─┬→ │   │  PRICED  ≤ $11.40       │
│                                   ├→ │   │  Daily digest   $7.94   │
│                                   └→ │   │  3 rollups      $2.18   │
└─────────────────────────────────────┘   └─────────────────────────┘
        ↓ Doc.Build()                             ↑ loom.Explain
   pipeline.Pipeline  ────────────────────────────┘
        ↓ Doc.Go()
   the same pipeline, as a Go program you can leave with
```

```go
doc, _ := studio.Load("vertical-digest.json")   // or build one in Go
s := studio.New(doc,
    studio.Models(reg),                          // what the steps bind to
    studio.File("vertical-digest.json"),         // autosave every edit
    studio.Constellation(vizURL),                // where the RUN tab goes
    studio.Runner(run),                          // what the Run button does
)
url, _ := s.Start("localhost:8078")
```

```sh
go run ./examples/studio    # offline: invented archive, mock models, real arithmetic
```

---

## 1. What the studio is not

It is not a runtime. Nothing in `studio` executes a model call, schedules a
task, or holds a result. It edits a document, compiles that document into a
`pipeline.Pipeline`, and hands it to the framework that already knows how to do
the rest. A pipeline built here is planned, fused, fingerprinted, priced,
admitted, retried, escalated, cached and audited by exactly the same code as one
written by hand, because by the time any of that happens it *is* one.

It is also not the run view. Watching a run — every call as a star, the cost
climbing, the retries, the dead letters — is the constellation view's job and
`viz` already does it well. The studio's RUN tab is a link.

## 2. Why the document is data

A pipeline written in Go holds closures. A filter is a `func(core.Record)
(bool, error)`; a source is a `func(ctx) ([]core.Record, error)`; a validator is
a func. None of them survives a round trip through a browser, a JSON file, or an
undo stack. So the studio's document holds declarations instead:

| Closure in Go | Declaration in the document |
|---|---|
| `Filter(func(r) (bool, error))` | `Cond{Field, Op, Value}` |
| `Map(func(r) (Record, error))` | `FieldSpec{Name, Template}` |
| `FromFunc(func(ctx) ([]Record, error))` | `SourceSpec{From, Root, Line, Scrub…}` |
| `InferSpec.Validate` | `Answer{Name, Kind, Required}` |
| `algo.NewRefine(Accept, Note)` | `LoopSpec{Until, Note, Rounds, CapUSD}` |

`Doc.Build` turns each one back into the closure the authoring API wants. The
constraint costs some expressiveness — a condition is a field, an operator and a
value, not arbitrary Go — and buys three things the closure form cannot offer.

**It can be proposed against.** The ⌘K assistant returns *edits*, not a new
document, and the canvas draws them before anything moves. A closure-shaped
pipeline can only be proposed against by generating code, and reviewing
generated code is a slower loop than accepting a diff whose new price is already
computed.

**Cache versions stop being the author's problem.** A Go-function stage is
cacheable only if it declares a `WithVersion`, and keeping that version in sync
with the function's behavior is the bookkeeping everyone forgets. Here a derived
field's version *is* the hash of its template, so there is no way to change what
a step does without invalidating exactly the records that saw the old behavior.

**The answer shape is one declaration with three consequences.** Declaring that
a step returns `{summary, topics, signals}` with `summary` required generates the
JSON instruction appended to the prompt, sets `ParseJSON`, and builds the
`Validate` gate whose failure escalates the record to the next model on the
ladder. Written by hand those are three places to keep in agreement, and they
agree until someone edits one of them.

## 3. What makes the price real

Three things, and each one closes a specific way an estimate can lie.

**The records are counted, not assumed.** The studio reads the source itself —
locally, off the disk, no model call, no network — and hands the records to
`Explain` as samples. Explain then *executes* every filter and every derived
field against them, so "148 of 412 days reach the payments rollup" is a count.
Builders that estimate from "roughly how many rows do you have?" cannot produce
that number, and the branch multiplier is exactly where a projection goes wrong.

**The blind spot closes itself.** `Explain` has one: a `ParseJSON` stage adds
fields the model invents, so a filter below one drops every record and the rest
of the run projects as no work at all — an undercount, in the dangerous
direction. It warns, and tells you to name the fields with `WithStageSample`.
In the studio the fields are not invented; the step declares them. `Price`
passes the declared answer shape as the sample, sized so the fields add up to
the response length the step is priced at, which is also what the *next* step
reads — the rollups price against a realistic digest line rather than an empty
one.

**Pricing performs nothing.** `Explain` runs the pure Go stages for real, which
is what makes the counts exact — and a write step is a pure Go stage that writes
files. So the studio compiles a *dry* form for pricing, in which every step that
touches the world computes the same thing without the touch. Asking what a run
would cost must not perform any part of it.

## 4. The seam: fan-out without fan-in

Loom's graphs fan out but never fan back in. Branching is a second consumer of a
dataset; there is no operator that reads two. A step that has to read three
branches — the one-page overview over three per-vertical rollups — is therefore
not a stage. It is a second run, seeded from the records the first one produced.

The studio does not hide that. A fold that merges branches is drawn inside a
dashed band labelled SECOND RUN · FAN-IN, and everything downstream of it is
inside the band too. `Doc.Build` returns the first pass; `Doc.BuildSecond(out)`
compiles the second from the first's stage outputs. Both are priced, and the
second one carries its own warning, because its input does not exist yet: each
merged fold contributes exactly one record, and the most that record can hold is
that fold's own token cap.

Hiding the seam would have been easy and wrong. It changes when the step starts,
what it costs, and which run's cache it lives in.

## 5. The assistant answers from the projection

`Insight`, the assistant that ships with the studio, is not a language model. It
answers what it can compute:

- **Where is the money going?** — the dominant step, its share, its call count,
  its average prompt size, and why its shape makes it dominant.
- **Make this cheaper.** — concrete edits (cheapest tier with escalation on
  failure, tighter answer caps, larger fold batches), and the price of the
  edited document, computed by pricing it.
- **Cap this run at $8.** — whether the ceiling still fits, and what the run
  does when it does not: halt on the budget, keep the finished work cached, and
  resume for free later.
- **How long, at best?** — the admission floor: the shortest wall clock the
  models' rate limits allow, however many workers are free.

For anything else it says so plainly rather than improvising, and
`studio.Assist` takes a model-backed `Assistant` in its place. The interface is
narrow on purpose: an assistant returns a `Proposal` carrying `[]Edit`, never a
modified document. Nothing changes until a human accepts it.

## 6. The way out

`Doc.Go` renders the document as a Go program, and the program compiles. Not a
sketch of one — the same operators, the same prompts, the same versions, the
same validators, with `build()`, `buildSecond()` and a `main` that runs both.
Two things in it are library calls rather than generated code, because they are
library code in the studio too: a source reads through `studio.LoadRecords` with
the spec the canvas holds, and a derived field renders through the same
`text/template` with the same `studio.TemplateFuncs`.

This is the property that makes the studio safe to start in. A visual builder
you cannot leave is a trap: the pipeline exists only inside the tool, and every
question about it — diff it, review it, test it, run it in CI — has to be
answered by the tool or not at all. The way out of this one is a file.

## 7. What is deliberately not here

- **Multi-input steps.** The document has one `From` edge per step, because the
  framework has one upstream per stage. The fan-in case is the second pass, and
  it is a fold over folds precisely so the second run's size is knowable before
  the first one runs.
- **Arbitrary Go in the canvas.** A condition is a field, an operator and a
  value; a derived field is a template with two helpers. Anything that needs
  more is a Go stage wearing a disguise, and the honest move is to export and
  write it.
- **A model in the assistant.** Not because one would not help — for "are the
  Hebrew days being handled correctly?" it is the only thing that would — but
  because the default should be the one that cannot be wrong about a number.
- **Collaboration.** One document, one server, one editor. The document is a
  JSON file; the tool that merges two of them is called git.
- **A run view.** That is `viz`, and duplicating it would produce two skies that
  disagree.

## 8. Shape of the package

| File | What it holds |
|---|---|
| `doc.go` | `Doc`, `Step`, the specs, validation, ordering, layout, `Edit`/`Apply`, load/save |
| `build.go` | `Doc.Build` / `BuildSecond`: declarations → pipeline; source readers, redactions, condition evaluation |
| `price.go` | `Price`: read the sources, call `loom.Explain`, join the projection back onto the steps |
| `assist.go` | `Assistant`, `Proposal`, `Edit`, and `Insight` — the projection-backed default |
| `export.go` | `Doc.Go`: the document as a compilable Go program |
| `server.go` | the HTTP surface: the UI, the state, the edits, the export, the run handoff |
| `ui.html` | the canvas, the inspector, the ⌘K palette — one file, no dependencies |
