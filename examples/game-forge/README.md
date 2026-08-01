# game-forge — a web game, built as pipelines

A playable browser game, planned, written, reviewed, linked, and shipped by
three Loom runs. One task per module, one least-privilege envelope per task, a
dollar cap per run, a cost projection before each one — and at the end a single
self-contained HTML file you can open and play.

```sh
go run ./examples/game-forge
# constellation view: http://localhost:8077   ← watch it being built
# ▶ play it:          http://localhost:8078   ← the thing it built
```

Offline by default: three scripted mock models stand in for a provider, so this
needs **no key, no network, and costs nothing** — and the game at the end is
real. It takes about 15 seconds.

## The three runs

```
game-design   brief ─ concept ─ modules+feasible (fused) ─ spec ─ seal ─ design-doc (tree)
                 ↓ 12 module specifications
game-build    specs ─ implement ─ lint ─┬─ review
                                        └─ build-notes (tree)
                 ↓ 12 modules of JavaScript
game-ship     modules ─ link+banner (fused) ─ collect ─ weave ─ title-card ─ emit
                 ↓ index.html
```

Three runs rather than one because loom DAGs fan out and do not fan back in,
and because each run answers the question the next one starts from: *what are
the modules*, *what is in them*, *what does the bundle look like*. They publish
to one constellation view, which keeps them as one **universe** — press `u` and
all three skies are there, each still whole and enterable after the next has
started.

| run | what it does | why it is interesting |
|---|---|---|
| **game-design** | one deep-model call turns a one-line pitch into a module breakdown; a pure stage explodes it into one record per module; each module gets its own specification task; a reduce tree writes `DESIGN.md` | the fan-out point: everything downstream is per-module and parallel |
| **game-build** | one task writes each module, in isolation, against a contract it shares with every other; a Go stage lints the result; a cheap review reads each module against its own acceptance criteria | the contract rides in as the stage's **prompt prefix**, so 12 calls send it once and read it 11 times from the provider's cache |
| **game-ship** | order the modules against the engine's dependency table, fold them into one bundle, write the title card, emit the page | `Combine` + pure Go: shipping is substitution once every decision has been made upstream |

## What the demo is actually showing

**Twelve modules written by twelve tasks that never see each other's code.**
That only works if they agree on something, and what they agree on is the
`engine-contract` broadcast: the namespace form, the shared `world` object, the
forbidden APIs, the language level. It is registered once for the run, stored
once by content hash, and *referenced* — not copied — by every task that
declares it.

**One shared value, two very different readers.** The same contract is the
implement stage's prompt prefix *and* the rule set the `lint` stage enforces in
Go, through the task's capability-checked session. Edit it and exactly the
stages that declared it recompute.

**Scope is cut by least privilege, not by taste.** The design proposes a
thirteenth module — an online leaderboard — and honestly declares that it needs
`network`. The engine contract grants `canvas2d`, `keyboard`, `webaudio`, and
`requestAnimationFrame`. A pure Go filter drops it *before a single token is
spent specifying or implementing it*.

**A bad module is a semantic failure, not a transport failure.** The
`implement` stage validates what came back: it must assign its namespace key
and its brackets must balance. When the `shards` module comes back truncated,
retrying the same model is a coin flip — so the task climbs the escalation
ladder to the deep model instead, which is visible in the finished game's own
provenance screen (`studio-master ↑`).

**The artifact carries its build.** Press `P` in the game: every module, the
model that wrote it, its size, tokens, and cost, plus the three runs and their
totals — all read from a manifest the `emit` stage stamped into the page.

<!-- the game's provenance screen: module · written by · bytes · tokens · cost -->

## Scripted moments

The mocks are written so one build shows every recovery path the framework has:

| scripted moment | where it shows up |
|---|---|
| `shards` comes back truncated | semantic failure → escalation ladder → `studio-master` writes it; amber `↑` in the game's provenance panel |
| `audio` comes back wrapped in a markdown fence, with prose either side | the `lint` stage strips it, as it must for real chat models |
| `game` takes ~5s to write | a growing star with a rotating activity ring while its cluster finishes around it |
| two 429s and a 503 | retry orbits in two different runs |
| `motes`' review is refused by a content filter | a dead letter — and the build ships all 12 modules anyway, because review is off the critical path |
| one module needs network | cut by the feasibility filter, inside a fused pure stage |

Everything is keyed to the module ID, so reruns are cache-stable and takes are
repeatable; only latency jitter varies.

## Flags & modes

```sh
go run ./examples/game-forge -addr localhost:8077 -play localhost:8078
go run ./examples/game-forge -out dist            # where the artifact lands
go run ./examples/game-forge -pitch "a one-line brief of your own"

# real models: the same prompts, the same contract, the same validation
ANTHROPIC_API_KEY=sk-... go run ./examples/game-forge -provider anthropic
OPENAI_API_KEY=sk-...    go run ./examples/game-forge -provider openai

# cache = checkpoint: the second forge re-derives the same bundle for $0
LOOM_STATE=/tmp/loom-forge go run ./examples/game-forge
LOOM_STATE=/tmp/loom-forge go run ./examples/game-forge

# squeeze the governor: the build stops partway and says what it got. With a
# state dir a partial build is still progress — run it again and it replays
# what completed for $0 and spends the budget on what is left
go run ./examples/game-forge -budget 0.03 -state /tmp/loom-squeeze
go run ./examples/game-forge -budget 0.03 -state /tmp/loom-squeeze
```

Each run is projected before it happens (`loom.Explain`, same options, no calls
issued), so the terminal shows what the build will cost *before* it starts and
the constellation view reads every stage against that forecast as it fills in.
The projection also says what it could not compute — the `lint` stage is a
`MapTools`, which needs a provisioned session, so Explain treats it as identity
and marks what comes after it as estimated rather than quietly guessing.

## The artifact

```
dist/index.html      the game: one file, no requests, no assets, no build step
dist/DESIGN.md       the design document, written by the design run's reduce tree
dist/BUILD.md        the build log, plus a table of every module and what it cost
dist/run-report.txt  all three run reports: per-stage tasks, tokens, cost, p95
```

The game is *Constellation Drift*: turn with `A`/`D`, thrust with `W`, fire with
`Space`, and spend a full meter on the weave pulse with `Z`. `P` shows how it
was built; `M` toggles sound.

## Where the code comes from

Offline, the scripted studio returns the module implementations from
`sources.go` — the same way every offline example in this repo scripts its mock
responses. Nothing downstream knows the difference: the build run validates,
lints, reviews, orders, links, and pays for those strings exactly as it would
for tokens off a real provider, and the answer key is never consulted anywhere
else. Point it at `-provider anthropic` or `-provider openai` and a real model
writes the modules against the same contract, with the same gates deciding
whether what came back is a module.

| file | what is in it |
|---|---|
| `main.go` | flags, provider selection, the universe view, three runs, the artifact |
| `forge.go` | the three pipelines |
| `shared.go` | the broadcasts (contract, art direction, module graph) and the HTML shell |
| `studio.go` | the scripted models, and every failure they are scripted to produce |
| `sources.go` | the module implementations the offline studio returns |
