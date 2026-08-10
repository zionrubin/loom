# agent-forge — read the conversations, design the agents

Point it at a folder of exported chat and it answers the question you actually
have before you build anything: **how many agents, split along which axis, and
what does each one have to remember?**

One agent per vertical that remembers partners, or one PPC agent that remembers
verticals, or bizdev that remembers partners first and verticals second — that
is a decision about *memory keys*, and this reads it off the work rather than
off a whiteboard.

```sh
go run ./examples/agent-forge
# constellation view: http://localhost:8077   ← watch the three runs
# ▶ the blueprint:    http://localhost:8078   ← the thing it produced
```

Offline by default: three scripted models stand in for a provider, so this needs
**no key, no network, and costs nothing**. It reads the 12-day corpus bundled
beside it and finishes in under a second.

## The three runs

```
work-census    load-days ─ day-census ─ census-line ─┬─ only-renters       ─ profile-renters
                                                     ├─ only-travel        ─ profile-travel
                                                     └─ only-pet-insurance ─ profile-pet-insurance
                  ↓ 58 job observations, one prose profile per space

agent-roster   label-catalog ─ capability-map ─ score ─ roster
                  ↓ 11 canonical capabilities, a shape, and a roster

agent-design   agents ─ agent-spec ─ spec-json ─ agent-charter
                  ↓ DESIGN.md, agents/<id>.md, index.html
```

Three runs rather than one because a loom DAG fans out and never fans back in,
and "what shape should this org be" is a question about all of it at once. They
publish to one constellation view, so all three skies stay in a single
**universe** — press `u` and every run is still there, whole and enterable.

| run | what it does | why it is interesting |
|---|---|---|
| **work-census** | one cheap call per day file extracts the *jobs* people did — label, function, entities touched, what had to be recalled across days, trigger, cadence, handoff, one quote | the map half: every day is independent, so a 3,000-day corpus is 3,000 parallel fast-tier calls, not one enormous prompt |
| **agent-roster** | exact-dedupe in Go collapses observations to distinct labels; one reduce folds them into a canonical capability taxonomy; **plain Go then counts everything that can be counted**; a deep call names and defends a shape | the model is asked to *choose and justify*, never to invent numbers — the spread, coupling and memory rankings are already computed when it is called |
| **agent-design** | each agent gets an operating spec (memory primary key, three horizons, tools, triggers, guardrails, evals) and then a charter (a concrete memory schema with worked example rows, and its system prompt) | one task per agent, fanned out; the partition-axis exclusion is re-applied in Go after the model answers, so it cannot be talked out of the invariant |

## The decision it makes

Everything reduces to two numbers, and the four candidate org shapes are the
four corners of the square they land in.

```
S  spread    volume-weighted mean of how evenly each capability recurs across
             spaces (normalised Shannon entropy). High: the same craft, done
             five times in five places.
C  coupling  volume-weighted mean of how tangled each space is — half the
             entropy of its job-family mix, half its cross-family handoff rate.
             High: one space runs many kinds of work that pass to each other.
```

| shape | score | what it would mean |
|---|---|---|
| function | `S·(1−C)` | one agent per job family, shared across every space |
| vertical | `C·(1−S)` | one agent per space, owning every family inside it |
| hybrid | `min(S,C)` | shared function agents *plus* a per-space owner, over shared memory |
| single | `(1−S)·(1−C)` | one generalist |

Because they are corners of the same square, a shape's score is simultaneously
the argument against the other three — which is why the report can print what it
rejected and why.

On the bundled corpus: **S = 0.77, C = 0.84 → hybrid (0.769)**, four agents.

### Partition is not memory

The distinction the whole example is built around:

- **partition** — what separates one agent *instance* from another
- **remembers** — what one instance accumulates knowledge *about*, ranked

An agent partitioned by vertical must never list `vertical` as memory: inside
one instance that axis is a constant, and "remembering" it is a no-op that
crowds out the key it actually needs. `rankAxes` drops the partition axis before
ranking, and `normaliseMemory` re-applies the rule to whatever the model
returned. That is what turns *"an agent per vertical that remembers partners"*
from a slogan into a computed ranking:

| agent | instances | remembers, in order |
|---|---|---|
| **Bizdev** | one shared instance | **partner** 77% → vertical 23% |
| **PPC** | one shared instance | **campaign** 63% → channel 31% → account 6% |
| **Vertical Owner** | one instance per vertical (3) | **campaign** 33% → account 27% → partner 27% → geo 12% |
| **Analytics** | one shared instance | **campaign** 50% → channel 50% |

Any axis two or more agents key on becomes a **shared ledger** rather than three
private copies that drift.

## Input

The same layout `examples/vertical-digest` reads — one folder per conversation,
one JSONL file per day, Google Chat export shape:

```
messages/
  renters/2026-03-02.jsonl
  renters/2026-03-03.jsonl
  travel/2026-03-02.jsonl
```

```json
{"sender":{"name":"...","type":"HUMAN"},"createTime":"2026-03-02T09:15:00Z","text":"..."}
```

A flat folder of `<space>.jsonl` files also works — each file becomes one space,
undated.

## Output

Written to `-out` (default `blueprint/`):

| file | what it is |
|---|---|
| `DESIGN.md` | the deliverable: verdict, shared memory, what each space talks about, the measured capability and space tables, the score table, then a full section per agent — memory design, schema with example rows, tools, triggers, guardrails, evals, first week, system prompt, evidence |
| `index.html` | the same thing as an interactive page — score bars with their formulas, a spread × coupling quadrant with the corpus plotted on it, agent cards with ranked memory bars, a capability × space heatmap, click any agent for its full charter in a drawer |
| `agents/<id>.md` | one charter per agent, standalone |
| `blueprint.json` | everything above as data |
| `roster.json` | the decided roster, and the one file that reads back in — edit it and re-run with `-roster` |
| `capabilities.json` | the canonical taxonomy the labels folded onto, for inspection and diffing between corpora |
| `run-report.txt` | per-stage tasks, calls, cache hits, retries, cost |

`index.html` is self-contained: no CDN, no fonts, no fetches. Open it from disk
or let the example serve it on `:8078`.

## Privacy

Sender names and `@mentions` become stable salted pseudonyms (`TM-1a2b`), and
e-mail addresses, phone numbers, long identifiers and URLs become placeholders —
**inside `loadDay`, before a record exists**, so there is no path by which raw
text reaches a prompt. The salt defaults to something derived from the corpus
path; pass `-salt` to make pseudonyms stable across runs, or to make them
deliberately unjoinable between corpora.

Order matters and is tested: the mention rule matches `@` followed by letters,
which is also the shape of a mail domain, so addresses are redacted *first* —
otherwise `dana.levi@example.com` becomes `dana.levi TM-1a2b` and the redaction
keeps the domain while publishing the name.

## Running it on a real corpus

```sh
go run ./examples/agent-forge \
  -messages ~/exports/chat \
  -provider anthropic \
  -state .loom \
  -since 2026-01-01 \
  -budget 25 -workers 12 -rpm 300
```

- **`-state`** turns on caching and resume, keyed on the prompt. The census is
  the expensive half and it is one call per day file, so a prompt change
  downstream re-reads nothing. On the bundled corpus the three runs are 78 tasks
  / 25 model calls cold — 12 of them the per-day census — and the same three runs
  against a warm state dir are 78 tasks, **78 cache hits, 0 calls, 0 tokens**,
  with `DESIGN.md` and all four charters byte-identical. Worth setting on any
  corpus you will read twice.
- **`-since` / `-until` / `-last N`** bound the read. `-last 30` keeps the most
  recent 30 days *of each space*, which is the cheapest way to sanity-check
  prompts against real data before committing to the whole archive.
- **`-budget`** is a hard cap for each run. Combined with `-provider mock` for
  a dry run, you can see the stage graph and the record counts before spending.
- **`-roster roster.json`** overrides the decided roster with one you edited by
  hand — drop an agent, merge two, rename one — and designs against that
  instead. The census still runs, because the design reads it; with `-state` it
  comes back from cache for nothing. This is the loop for "the computed answer
  is close, but merge these two agents".
- **`-workers` / `-rpm`** are the throughput knobs; the census stage is where
  they matter.

Cost scales with day files, not messages: the taxonomy reduce sees distinct job
*labels* (11 on the bundled corpus, a few hundred on a large one), never the
tens of thousands of raw observations behind them.

## Tests

```sh
go test ./examples/agent-forge
```

Everything runs offline. The scoring tests pin the maths that picks the shape —
including all four corners of the quadrant — because a silent drift there still
produces a plausible-looking document. The privacy tests pin the scrubber. The
pipeline tests run all three runs end to end and assert the invariant that ties
them together: no agent remembers its own partition key.
