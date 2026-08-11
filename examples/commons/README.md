# commons — four desks, one set of facts

Four analyst desks research the same six companies at the same time, each in its
own house phrasing. The public source gets called six times instead of
twenty-four.

```
go run ./examples/commons
```

Offline: a scripted source tool with realistic latency and per-query pricing,
mock models, no keys, no network, zero real cost.

## What it shows

The same fleet is run twice, distinguished by one option, and the two are
printed side by side — because "reduces duplicated work without adding
meaningful latency" is two claims and both should be measurable.

```
4 desks × 6 companies = 24 questions about 6 subjects
the public source takes 120ms per call

                           no commons   with commons
calls to the source                24              6
wall clock                      415ms          171ms
spent at the source           $0.0960        $0.0240
result-cache hits                  11              5
spent on models               $0.0015        $0.0021

findings  24 asked · 18 reused (75%) · 6 researched
  exact 0 · class 12 · near 0 · coalesced 6 · topped-up 0
  avoided $0.0720 and 2.198s of research, spent $0.0240
  gate overhead 1.122ms total, 47µs per question
```

**No embedder is configured.** All 18 reuses come from the two free tiers: the
topic-and-facets class index, and the single-flight lease that collapses
simultaneous askers onto one call. The embedding tier is the optional
refinement, not the mechanism.

**The exact tier never fires**, and that is the point. Four desks asking four
different sentences about one company produce four different exact keys — which
is precisely why a cache keyed on the request cannot help them, and why the
class tier exists.

**`coalesced` is the number a cache can never produce.** Those are questions
that arrived while another task was already fetching the answer. A cache serves
the second asker only after the first has finished; a lease serves it while the
first is still in flight.

**Gate overhead is ~1/2500 of the call it decides about.** That ratio is the
argument for gating every task rather than the ones somebody guessed would
collide.

## The two things it refuses to hide

**The model column gets slightly worse.** Removing the source's latency makes
the desks arrive at the note stage together, so more identical tasks are in
flight at once and the result cache — which has no single-flight lease of its
own — serves fewer of them. Same thundering herd, one level up. The example
prints it.

**The answers must be identical.** After the comparison it diffs every brief
from both runs and exits non-zero if any differ. A served finding that is not
substitutable for the call it replaced would mean the layer is changing answers
to save money, which is not a trade anyone agreed to.

## Retraction

The run ends by withdrawing one claim and printing the tasks that had already
been served it — the reason serves are recorded against findings at all. The
withdrawn revision stays resolvable by hash, because lineage names it: a ledger
that forgets cannot say what it believed when it produced a conclusion, which is
exactly what a correction makes urgent.

## Flags

```
-addr localhost:8077        serve the constellation view
-source-latency 120ms       how slow the public source is
```

The design is [docs/FINDINGS.md](../../docs/FINDINGS.md).
