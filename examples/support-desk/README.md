# support-desk — broadcasts and multi-task executors, on real OpenAI models

A support-ticket pipeline that exists to make two Loom features *measurable*
instead of theoretical:

- **Broadcast values** — the product catalog, the support policy, and the
  brand-voice rubric are registered once per run and read by reference from
  every task that declared them. The run prints exactly how many bytes never
  had to be copied into task envelopes, and the constellation view draws each
  shared value as a ⬡ node wired to its readers.
- **Multi-task executors** — 6 executors (◇ diamonds) each run many tasks,
  and every task is provisioned with only what its stage declared: the
  `enrich` tasks can read the catalog but never touch the API key; `classify`
  tasks can read the policy but not the voice rubric; `draft-reply` tasks can
  read both.

```sh
OPENAI_API_KEY=sk-... go run ./examples/support-desk
# then open http://localhost:8077
```

The run is capped at $0.50 by the budget governor (a fresh run costs a few
cents). No key handy? The pipeline itself is exercised offline by the tests:
`go test ./examples/support-desk`.

## The pipeline

```
tickets ─ enrich (Go + catalog⬡) ─ classify (nano→mini, policy⬡) ─ route (fused Go)
                                                                    ├─ needs-reply ─ draft-reply (mini, voice⬡ + policy⬡)
                                                                    └─ ops-digest (gpt-5.4 tree reduce)
```

- `enrich` — pure Go, joins each ticket to the shared catalog through the
  task's capability-checked session (`core.BroadcastAs`). No model call.
- `classify` — GPT-5.4 nano with `{{broadcast "policy"}}` in the prompt,
  JSON-parsed and validated; invalid output escalates to mini automatically.
- `route` — pure Go queue assignment, fused and cached like everything else.
- `draft-reply` — GPT-5.4 mini writes replies in the brand voice, citing the
  same policy the classifier applied.
- `ops-digest` — GPT-5.4 synthesizes an operations digest from every ticket.

## The three-run demo

This is the sequence that shows the advantage clearly:

```sh
# 1. fresh run: real model calls, watch the sky fill
LOOM_STATE=/tmp/loom-desk OPENAI_API_KEY=sk-... go run ./examples/support-desk

# 2. identical rerun: every task settles instantly in the ✧ cache hue,
#    zero model calls, $0 — completed AI work is never paid for twice
LOOM_STATE=/tmp/loom-desk OPENAI_API_KEY=sk-... go run ./examples/support-desk

# 3. edit the shared policy (refund window 30→60 days, new goodwill credits):
#    ONLY the stages that read the policy broadcast recompute — classify and
#    draft-reply go live again while enrich replays from cache untouched,
#    because the policy's content hash is part of its readers' fingerprints
LOOM_STATE=/tmp/loom-desk OPENAI_API_KEY=sk-... go run ./examples/support-desk -policy v2
```

Run 3 is the point: a knowledge-base edit invalidates *exactly* the work that
depended on it, and the v2 results actually change (ticket t11's 41-day-old
refund request is denied under v1's 30-day window and approved under v2's 60).

## What to look at in the constellation view

- The three ⬡ **shared-value nodes** along the top; click one for its size,
  content hash, and exactly which stages and tasks read it — plus how much
  payload was never copied.
- Press **`s`** (or click **☰ summary** in the header) for the **run summary
  overlay**: every step of the pipeline with tasks/records/retries/cache/
  tokens/cost/p95 per stage, per-shared-value savings, and per-executor
  utilization. It opens by itself when the run completes.
- Click any `classify` star, open its **model calls**: the rendered request
  shows the policy text injected by `{{broadcast "policy"}}`, and its
  **shared values** row links back to the ⬡ node.
- Click a ◇ **executor**: tasks done, busy time — a handful of executors
  running dozens of tasks, each task under its own least-privilege envelope.

Console output ends with the same story in numbers: bytes stored once vs.
bytes that would have shipped if every envelope carried its own copy, tasks
per executor, and cache replays.
