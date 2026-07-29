# vertical-digest

Analyzes exported Google Chat messages — one JSONL file per vertical per day —
and produces:

- `reports/<vertical>.md` — a rollup per vertical: status, themes, recurring
  patterns, subjects that deserve attention.
- `reports/business-overview.md` — a one-page executive summary across all
  verticals.
- `reports/run-report.txt` — the loom run report (tasks, cache hits, tokens, cost).

## How it works

Run 1 (`vertical-digest`):

```
load-days → daily-digest (gpt-5.4-nano ↗ mini) → digest-line ─┬→ only-<v> → rollup-<v> (gpt-5.4-mini)
                                                              └→ ... one branch per vertical
```

Each day-file becomes one record; a fast-tier model digests it into
`{summary, topics, signals}` (with escalation to mini on semantic failures),
and a hierarchical `ReduceAI` rolls each vertical up chronologically.

Run 2 (`business-overview`) fuses the per-vertical rollups into the one-pager
with `gpt-5.4` — loom DAGs fan out but do not fan back in, so the
cross-vertical synthesis is a separate (single-call) run.

Privacy: sender IDs are anonymized to per-day labels (S1, S2, ...) and email
addresses are redacted before any text is sent to the provider.

## Run

```sh
go test ./examples/vertical-digest          # offline, mock models

OPENAI_API_KEY=sk-... LOOM_STATE=~/.loom-state \
  go run ./examples/vertical-digest \
  -messages /path/to/messages \
  -out reports -budget 15
```

Flags: `-messages` (root dir of `<vertical>/<date>.jsonl`), `-out`, `-budget`
(hard USD cap for the digest run), `-workers`, `-rpm`, `-since`/`-until`
(YYYY-MM-DD clamp), `-last` (N most recent days per vertical, 0 = all),
`-state` (defaults to `$LOOM_STATE`), `-addr`
(constellation view address, default `localhost:8077`; empty disables it).

The constellation view streams both runs to the same page. The process waits
up to 30s for a viewer before starting, and keeps serving after the run
finishes (Ctrl-C to exit). Pass `-addr ""` for headless/cron runs.

Tips for the full ~3,000-file corpus:

- Set `-state` (or `LOOM_STATE`) so an interrupted or budget-capped run
  resumes at zero cost for completed digests.
- Try `-since` first (e.g. one month) to validate prompts cheaply before the
  full run.
- `-rpm` should match your OpenAI tier; 200 is the default.
