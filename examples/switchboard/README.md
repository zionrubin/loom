# switchboard

A support desk whose conversations never stop arriving, and which can be asked
about them at any moment.

```sh
go run ./examples/switchboard                 # ingest a shift of support traffic, then ask it questions
go run ./examples/switchboard -live           # deliver it at a conversational pace, and ask mid-stream
go run ./examples/switchboard -serve :8099    # keep it running, with the console at that address
go run ./examples/switchboard -budget 1200    # a tighter context, so it compacts
go run ./examples/switchboard -ask "what is Northwind waiting on?"
```

Offline: the models are deterministic mocks, so there is no key, no network, and
no cost. The conversations are synthetic — the companies, the people and their
problems are fixtures invented for this example.

## What it is demonstrating

Retrieval-at-query-time puts the cost of understanding on the critical path of
every question, pays it again for every question, and makes the answer only as
good as what one retrieval happened to surface. A **desk** inverts that: the
understanding happens once, as each message arrives, and what it produces is a
compact standing context that a question reads as it stands.

```
conversations → ingest → read · slice · digest → living context → answers
                └──────── continuous, expensive ────────┘ └── cheap ──┘
```

Five stages on the ingest side, and only one of them is stream-specific:

| stage | |
|---|---|
| `messages` | `FromStream`: the conversation feed |
| `live` | `Filter`: drops the feed's idle heartbeats before they cost anything |
| `read` | `Infer`, per message, the moment it arrives — the bill, and the point |
| `keep` | `FlatMap`: drops what is not worth remembering, stamps the rest |
| `slice` | `Window`: one pane per conversation per interval |
| `digest` | `ReduceAI` over one slice, once per pane → one line of the context |

The query side is one `Infer` stage whose continuation is the maintained
context. That is not a detail: the context reaches the model as a revision hash
rather than as bytes, and the revision joins the stage fingerprint.

## What to look for

- **The cost is on the ingest side.** The report prints model calls per stage.
  Reading messages is the bill; answering is one call against a couple of
  kilobytes, and it is the same one call whether the desk has ingested ten
  messages or ten thousand.

- **The context is maintained, not rebuilt.** One line per revision as the
  example runs, each a conversation slice appended to a chain — the hash moves,
  the byte count grows by a line, and nothing already in it is re-read.

- **A repeated question is free.** Ask the same thing twice against an unchanged
  context and the second answer is a cache hit, marked `cached: no model call`.
  Then a conversation lands, the revision moves, and the next answer is computed
  against it.

- **Compaction is a fold, not a truncation.** With `-budget 1200` the oldest
  entries become a standing brief while the newest stay verbatim, and the
  summary line says how many folds it took.

- **The two sides do not wait for each other.** With `-live` a question is asked
  while messages are still arriving. It reports the revision it used and how
  many messages that revision does not yet cover — the honest price of
  decoupling, printed rather than hidden.

## The console

`-serve` keeps the desk running and serves a page you can talk to it in, with
the living context beside the thread so you can watch what the model is being
told change as conversations arrive.

```sh
go run ./examples/switchboard -serve :8099
curl -s localhost:8099/context      # the living prompt, as the model receives it
curl -s localhost:8099/stats        # what the desk holds and what it cost
curl -s localhost:8099/ingest -d '{"conversation":"acme","messages":[{"role":"user","text":"the export is failing again"}]}'
```

[docs/RECALL.md](../../docs/RECALL.md) is the full design.
