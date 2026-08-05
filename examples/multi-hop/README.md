# multi-hop

A research question answered by walking a citation graph, where which paper
gets read next is decided by the model that just read the previous one.

```sh
go run ./examples/multi-hop
```

Offline: the model is a deterministic mock, so there is no key and no network.
The corpus is synthetic — the papers, abstracts and citations are fixtures
invented for this example, not a real bibliography.

## What it is demonstrating

This is the workload one forward pass cannot express. A pipeline can read every
paper once. It cannot read a paper, learn that the claim it needs is in a paper
that one cites, and go get it — that is a fixpoint over a graph, and it is what
`pipeline.Iterate` plus an `algo.Algorithm` are for.

Three stages, and only the middle one is new:

| stage | |
|---|---|
| `seed` | the corpus |
| `explore` | `Iterate` with `algo.BSP`: each active paper reports what it contributes and names what to follow; those become the next round |
| `synthesize` | `ReduceAI` over everything the walk reached |

The edges come from the records — each paper's own `cites` field — while the
messages come from the model, out of a `follow` field the step writes. The
author declares what *can* be reached; the vertex program decides what *is*.

## What to look at in the output

**The frontier per round.** It grows while the walk is discovering and shrinks
as papers run out of anywhere new to send:

```
explore  (bsp, 5 rounds, halted: quiet)
round      active   messages
0               2          0
1               4          8
2               3          6
3               2          2
4               1          1
9 vertices, 0 quiesced, 1 grown, 0 dropped, 0 truncated
```

That shrinking is convergence, and it is why the last round is the cheapest
rather than the most expensive — the inversion of the usual economics of
iterative model work.

**The projection, printed before the run.** A loop's cost is not knowable; the
round count is a property of the data. So `Explain` prices the *round cap*
instead, which is the number `HaltWhen.Budget` should be set from:

```
projected ceiling 11558 tokens / $0.1603 at the round cap
actually spent    2164 tokens / $0.0202 over 5 round(s), halted: quiet
```

An over-estimate is the only safe direction for a number a budget is set from.

**The graph growing.** `p7` cites `p9`, which the corpus does not contain. With
`Grow` set the walk creates that vertex rather than dropping the edge; without
it, the message would be dropped *and counted*, because "found nothing" and
"was not allowed to look" produce identical output.

**Selectivity.** `p6` and `p8` are in the corpus and are never activated —
nothing cites them and they are not seeds. A walk that reads them is not
selecting, it is scanning.

**That the depth is load-bearing.** `p7` is three hops out — `p3 → p4 → p7` —
and it is the only paper that closes the question. Run with `-rounds 2` and the
same pipeline over the same corpus gathers four papers' findings and reports
that the question is not settled. That gap is the argument for the operator.

**The rerun.**

```sh
go run ./examples/multi-hop -state /tmp/loom-hop
go run ./examples/multi-hop -state /tmp/loom-hop
```

```
actually spent    0 tokens / $0.0000 over 5 round(s), halted: quiet
15 task(s) replayed from the result cache: rerunning a converged loop costs nothing
```

Five rounds replay for free, because a vertex's cache key is its state and its
inbox rather than the round it is in.

## Flags

| flag | |
|---|---|
| `-state DIR` | persistent cache directory; run twice to see the replay |
| `-rounds N` | the superstep cap (default 5). Set it to 2 and the walk halts on `rounds` instead of `quiet`, never reaches `p7`, and the synthesis says outright that the question is not settled — the same pipeline over the same corpus, unable to answer because it was stopped one hop short |
| `-budget USD` | the stage budget (default 2.00). Set it low and the loop halts on `budget` — and the pipeline continues with what the loop reached, which is the distinction from the run governor |

Both of those last two are worth trying: the halt reason is the only thing
distinguishing a walk that finished from one that was cut off, since they
return the same records.

See [docs/ALGORITHMS.md](../../docs/ALGORITHMS.md) for the interface this is
built on.
