# newsroom — a fleet of agents that work together

Six agents on one engine, sharing its slots, its provider quota, its dollar
ceiling and its cache, and handing conclusions to each other through a
blackboard. Entirely on scripted mock models: no keys, no network, no cost.

```sh
go run ./examples/newsroom        # then open http://localhost:8077
```

| Agent | Work | Model tier | Role in the demo |
|---|---|---|---|
| `wire-desk` | 60 wire reports → classify | fast | The long agent. Launched first, fills every slot. |
| `beat-markets` | 6 briefs → summarize → one finding | balanced | Short agent. Launched 150ms later. |
| `beat-policy` | ditto | balanced | Short agent. |
| `beat-tech` | ditto | balanced | Short agent. |
| `front-page` | reads the board → writes the page | deep | The expensive agent a person waits on. |
| `wire-recheck` | the same 60 reports again | fast | Costs nothing: the fleet's cache has them. |

## What to look for

**The short agents overtake the long one.** `wire-desk` is launched first and
immediately claims every slot; the beats are launched afterwards and still
finish in roughly half the time, because a contended slot goes to the agent that
has been served *least* rather than the one that queued first. On separate
`loom.Run` calls with a shared concurrency ceiling, the beats would queue behind
all 60 of the desk's tasks.

The report prints both halves of that trade — `service` is the slot-time an
agent was given, `jct` is what a caller actually waited:

```
fleet  6 agents · 6 slots · 4.096s
agent                stages  tasks   tokens   cost($)   service      wait       jct
wire-desk                 2     60     5166    0.0070   12.833s    3.913s    4.093s
beat-markets              3      7      426    0.0025    3.434s     1.33s    2.396s
beat-policy               3      7      424    0.0025      3.4s    1.425s    2.379s
beat-tech                 3      7      417    0.0024    3.307s    1.474s    2.346s
front-page                2      1      208    0.0083    1.363s      19ms    1.382s
wire-recheck              2     60        0    0.0000       8ms        0s       2ms
TOTAL                    15    142     6641    0.0226   24.346s    8.162s    4.096s
slots 6 occupied 99% of 4.096s · 142 tasks admitted
fleet budget $5.0000, spent $0.0226 (0%) across every agent
blackboard: 1 topic(s), 3 post(s), read by reference
```

**Fan-in that a loom DAG cannot express.** Loom DAGs fan out but do not fan back
in, so the synthesis of three parallel branches has always had to be a separate
run. Here it still is — and it can now *read* what the branches concluded. Each
beat posts its finding as it lands, `front-page` is launched on the count rather
than on a list of agents, and it reads `findings@3` pinned to a content hash:

```
posted   beat-tech      → findings@1
posted   beat-policy    → findings@2
posted   beat-markets   → findings@3

board has 3 findings — launching front-page

--- front page ---
EVENING EDITION
  [policy] Carrier cancels regional routes — 3 sources agree (over 5 others)
  [markets] Central bank holds rates, signals two cuts — 3 sources agree (over 5 others)
  [tech] Data-centre buildout strains water permits — 2 sources agree (over 5 others)
(3 findings, read off the fleet's blackboard by reference)
```

**One ceiling, one quota, one cache.** The budget line covers all six agents.
`wire-recheck` runs the identical stage over the identical records and makes
zero model calls, because the cache belongs to the fleet rather than to the run
that filled it — which is why a fleet beats a sequence of runs even when nothing
overlaps.

**Six skies in one universe.** Every agent publishes to the same event handler,
so the constellation view holds each as its own sky. Press `u` for the
overview: the wire desk stays whole and inspectable while the front page is
still being written, and `l` jumps to whichever agent is still live.

## Flags

```sh
# squeeze the ceiling and watch one governor stop the whole newsroom
go run ./examples/newsroom -budget 0.02

# fewer slots: the same work, more queueing, and a longer wire desk
go run ./examples/newsroom -slots 2

# tune the fairness bound — a higher aging rate gives an agent that has already
# been served heavily more protection from agents that keep arriving fresh
go run ./examples/newsroom -aging 4

# cache as checkpoint, across the fleet and across processes: the second run
# makes no model calls at all
LOOM_STATE=/tmp/loom-newsroom go run ./examples/newsroom
LOOM_STATE=/tmp/loom-newsroom go run ./examples/newsroom

# exit when the newsroom finishes instead of holding the view open
go run ./examples/newsroom -serve=false
```

## Notes on the mocks

The scripted models dispatch on the **system prompt**, not the user prompt. A
later stage's user prompt contains the *output* of an earlier one, so a mock
that pattern-matches on text a model produced eventually matches the wrong
branch — which it did, on the first run of this example. Latency jitter is
derived from a hash of the prompt rather than a clock, so a rerun is
byte-identical and the cache behaves as it would in production.

See [docs/ASYNC.md](../../docs/ASYNC.md) for the design and the serving
literature it comes from.
