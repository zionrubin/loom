package main

// Run 1 — the map half. Every day of every space is read once, independently,
// and turned into a list of the JOBS people did that day. This is the only
// stage that touches conversation text, and it is deliberately the cheapest
// model in the lineup: the work is extraction, not judgement, and there are
// thousands of days.
//
//	load-days ─ day-census ─ census-line ─┬─ only-<space> ─ profile-<space>
//	                                      ├─ only-<space> ─ profile-<space>
//	                                      └─ …
//
// The two outputs serve different readers. profile-<space> is prose — "what do
// they actually talk about in here", the first question to answer. day-census
// carries the structured job list, which Go reads out of StageOutputs and
// counts; no model is asked to do arithmetic on it.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

const censusPrefix = `You are cataloguing the WORK a team does, by reading one day of their internal chat.

You are not summarising the conversation. You are answering: what jobs were done or asked for
today, what each job is about, and what a person would have had to remember from earlier days to
do it well. That last part decides how an agent covering this job would have to be built, so it
is the part to get right.

A JOB is a recurring unit of work with an owner and an outcome: "pause underperforming keywords",
"chase a partner for a missing payout report", "explain a CPA spike". Not a job: small talk,
scheduling, one-off links, acknowledgements. Most messages are not jobs. Five to ten jobs on a
busy day is normal; zero is a correct answer for a quiet one.

function must be exactly one of:
  ppc        media buying — bids, budgets, keywords, campaign structure, platform settings
  partners   bizdev — external counterparties, commercials, integrations, payouts, escalations
  creative   ads, landing pages, copy, design iteration
  analytics  reporting, dashboards, attribution, pulling and reading numbers
  product    site, funnel and product changes
  quality    lead quality, fraud, compliance, disputes
  ops        process, access, scheduling, vendor administration
  finance    invoicing, payouts, budget approval
  other      anything the list above does not fit — use it rather than forcing a bad fit

entities are the kinds of thing the job operates on. memory is the narrower list: the kinds of
thing the person must RECALL ACROSS DAYS to do the job — what was agreed with this partner last
month, how this campaign behaved last quarter. A job with no cross-day recall has an empty memory
list, and that is a meaningful answer: it says the job needs no long-term store.

Both are drawn from exactly this vocabulary:
  vertical  the business line this channel runs
  partner   an external commercial counterparty
  channel   an ad platform: google, meta, bing, tiktok
  campaign  a campaign, ad group or creative set
  account   an ad account, property or integration endpoint
  geo       a market or region
  person    an internal owner

Messages are Hebrew, English, or both. Always answer in English. People appear as pseudonyms
(TM-xxxx); use those exact strings and never invent one.

Respond with a single JSON object and nothing else:
{"themes": ["<3-6 words each: what this channel spent the day on>"],
 "jobs": [{"label": "<the job in 2-5 words, verb first, no vertical or partner name in it>",
           "function": "<from the list above>",
           "entities": ["<from the vocabulary>"],
           "memory": ["<from the vocabulary, or empty>"],
           "trigger": "<what set it off: a schedule, an alert, a person, a partner>",
           "cadence": "daily|weekly|monthly|ad_hoc|continuous",
           "decision": "<the judgement call it needs, or 'none' if mechanical>",
           "tools": ["<systems touched: Google Ads, BI, Salesforce, sheets…>"],
           "handoff_to": "<the function it hands off to, or empty>",
           "quote": "<one short verbatim line that shows the job happening>"}],
 "systems": ["<tools and data sources named today>"]}`

// buildCensusPipeline is run 1: one extraction per day, then one prose profile
// per space.
func buildCensusPipeline(files []dayFile, spaces []string, lu lineup, scrub *scrubber, workers int, onLoad func(int, int)) *pipeline.Pipeline {
	p := pipeline.New("work-census")

	src := p.FromFunc("load-days", func(ctx context.Context) ([]core.Record, error) {
		recs := make([]core.Record, 0, len(files))
		for i, f := range files {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			rec, ok, err := loadDay(f, scrub)
			if err != nil {
				return nil, fmt.Errorf("load %s: %w", f.Path, err)
			}
			if ok {
				recs = append(recs, rec)
			}
			if onLoad != nil {
				onLoad(i+1, len(files))
			}
		}
		return recs, nil
	})

	days := src.Infer("day-census", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast, Escalation: []string{lu.balanced}},
		System:  "You catalogue work from chat transcripts. You answer only with JSON.",
		Prefix:  censusPrefix,
		Prompt: `Space: "{{.space}}"   Date: {{.date}}   Messages: {{.count}}

{{.messages}}`,
		MaxTokens: 1500,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			if _, ok := r.Data["jobs"].([]any); !ok {
				return fmt.Errorf("missing jobs array")
			}
			return nil
		},
	}, pipeline.WithParallelism(workers))

	// One line per day for the prose rollup. The structured jobs stay on the
	// record for Go to count; this is only what the reduce tree reads.
	lines := days.Map("census-line", func(r core.Record) (core.Record, error) {
		out := r.Clone()
		obs := jobsOf(r)
		var parts []string
		for _, o := range obs {
			part := fmt.Sprintf("%s [%s]", o.Label, o.Function)
			if len(o.Memory) > 0 {
				part += " remembers:" + strings.Join(o.Memory, "/")
			}
			parts = append(parts, part)
		}
		themes := stringsOf(r.Data["themes"])
		line := fmt.Sprintf("%s — themes: %s | jobs: %s", r.String("date"),
			orNone(strings.Join(themes, "; ")), orNone(strings.Join(parts, "; ")))
		out.Data["profile_line"] = line
		return out, nil
	}, pipeline.WithVersion("v1"))

	for _, space := range spaces {
		space := space
		only := lines.Filter("only-"+space, func(r core.Record) (bool, error) {
			return r.String("space") == space, nil
		}, pipeline.WithVersion("v1"))

		only.ReduceAI("profile-"+space, pipeline.ReduceAISpec{
			Binding: model.Binding{Tier: model.TierBalanced},
			System:  "You describe how a team works, from a catalogue of their daily jobs.",
			Prefix: `You are given day-by-day job catalogues from one space (or partial rollups of them), in date
order. Describe the working life of this space so someone deciding what to automate can see it.

Cover, in this order and nothing else:
  1. What this space is for, in one sentence.
  2. The recurring jobs, most frequent first — what triggers each and how often it runs.
  3. What the people here have to remember across days, and about what: partners, campaigns,
     channels, accounts. Be specific about which, because this is what an agent would have to store.
  4. Where work leaves this space — the handoffs, and to whom.
  5. What is judgement and what is mechanical.

Under 400 words, plain prose, no headings, no bullet characters. Do not invent anything that is
not in the catalogues.`,
			Prompt: fmt.Sprintf(`Space %q — {{.Count}} day catalogues:

{{range .Items}}{{.}}
{{end}}`, space),
			FanIn:     14,
			MaxTokens: 900,
			ItemField: "profile_line",
		})
	}
	return p
}

// jobsOf pulls the structured job list off a census record. Model output is
// permissive by nature, so every field is coerced rather than asserted: one
// malformed job should cost one job, not a day.
func jobsOf(r core.Record) []jobObs {
	raw, _ := r.Data["jobs"].([]any)
	space, date := r.String("space"), r.String("date")
	out := make([]jobObs, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		label := strings.TrimSpace(str(m["label"]))
		if label == "" {
			continue
		}
		out = append(out, jobObs{
			Space:     space,
			Date:      date,
			Label:     label,
			Function:  knownFunction(str(m["function"])),
			Entities:  stringsOf(m["entities"]),
			Memory:    stringsOf(m["memory"]),
			Trigger:   str(m["trigger"]),
			Cadence:   str(m["cadence"]),
			Decision:  str(m["decision"]),
			Tools:     stringsOf(m["tools"]),
			HandoffTo: str(m["handoff_to"]),
			Quote:     str(m["quote"]),
		})
	}
	return out
}

// collectJobs flattens every day's jobs into the observation list the scoring
// runs on, and gathers the systems named along the way.
func collectJobs(recs []core.Record) ([]jobObs, []string) {
	var obs []jobObs
	systems := map[string]int{}
	for _, r := range recs {
		obs = append(obs, jobsOf(r)...)
		for _, s := range stringsOf(r.Data["systems"]) {
			if s = strings.TrimSpace(s); s != "" {
				systems[s]++
			}
		}
	}
	names := make([]string, 0, len(systems))
	for s := range systems {
		names = append(names, s)
	}
	sort.Slice(names, func(i, j int) bool {
		if systems[names[i]] != systems[names[j]] {
			return systems[names[i]] > systems[names[j]]
		}
		return names[i] < names[j]
	})
	return obs, names
}

func str(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case float64:
		return fmt.Sprintf("%g", t)
	case nil:
		return ""
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

func stringsOf(v any) []string {
	items, ok := v.([]any)
	if !ok {
		if s := str(v); s != "" && !strings.HasPrefix(s, "{") {
			return []string{s}
		}
		return nil
	}
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s := str(it); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func orNone(s string) string {
	if strings.TrimSpace(s) == "" {
		return "none"
	}
	return s
}
