package main

// The offline studio: three deterministic models that stand in for a provider
// so the example runs, and its tests pass, with no API key and no network.
//
// These are not stubs that echo "ok". Each one answers the prompt it is given
// the way the real stage expects — the census reads the space it was handed and
// returns the jobs that space does, and the roster stage adopts the computed
// baseline it was shown, which is exactly what its prompt tells a real model to
// do by default. That is what makes the offline run produce a real design
// document rather than a shaped placeholder, and what makes a test failure mean
// the pipeline is wrong rather than the mock is thin.

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/providers/anthropic"
	"github.com/zionrubin/loom/providers/openai"
	"github.com/zionrubin/loom/security"
)

// lineup is the concrete model id per tier. Tiers cover the base binding, but
// an escalation ladder needs ids, so the pipeline builders take these.
type lineup struct{ fast, balanced, deep string }

func buildRegistry(provider string, rpm int) (*model.Registry, map[security.SecretRef]string, error) {
	switch provider {
	case "mock", "":
		reg := model.NewRegistry()
		for id, tier := range map[string]model.Tier{
			"mock-fast":     model.TierFast,
			"mock-balanced": model.TierBalanced,
			"mock-deep":     model.TierDeep,
		} {
			if _, err := model.RegisterMock(reg, id, tier, model.WithHandler(respond)); err != nil {
				return nil, nil, err
			}
		}
		return reg, nil, nil
	case "anthropic":
		key := os.Getenv("ANTHROPIC_API_KEY")
		if key == "" {
			return nil, nil, fmt.Errorf("set ANTHROPIC_API_KEY for -provider anthropic")
		}
		reg := model.NewRegistry()
		if err := anthropic.RegisterDefaults(reg, model.Limits{RequestsPerMinute: rpm}); err != nil {
			return nil, nil, err
		}
		return reg, map[security.SecretRef]string{anthropic.DefaultSecretRef: key}, nil
	case "openai":
		key := os.Getenv("OPENAI_API_KEY")
		if key == "" {
			return nil, nil, fmt.Errorf("set OPENAI_API_KEY for -provider openai")
		}
		reg := model.NewRegistry()
		if err := openai.RegisterDefaults(reg, model.Limits{RequestsPerMinute: rpm}); err != nil {
			return nil, nil, err
		}
		return reg, map[security.SecretRef]string{openai.DefaultSecretRef: key}, nil
	}
	return nil, nil, fmt.Errorf("unknown provider %q (want mock, anthropic, or openai)", provider)
}

func lineupOf(reg *model.Registry) (lineup, error) {
	var lu lineup
	for _, t := range []struct {
		tier model.Tier
		dst  *string
	}{{model.TierFast, &lu.fast}, {model.TierBalanced, &lu.balanced}, {model.TierDeep, &lu.deep}} {
		info, err := reg.ForTier(t.tier)
		if err != nil {
			return lu, fmt.Errorf("tier %q: %w", t.tier, err)
		}
		*t.dst = info.ID
	}
	return lu, nil
}

// respond routes on the stage's shared prefix, which is stable per stage and
// never collides.
func respond(req model.Request) (string, error) {
	switch {
	case strings.HasPrefix(req.Prefix, "You are cataloguing the WORK"):
		return mockCensus(req.Prompt)
	case strings.HasPrefix(req.Prefix, "You are given day-by-day job catalogues"):
		return mockProfile(req.Prompt), nil
	case strings.HasPrefix(req.Prefix, "You are folding a list of job labels"):
		return mockTaxonomy(req.Prompt)
	case strings.HasPrefix(req.Prefix, "You are deciding the shape"):
		return mockRoster(req.Prompt)
	case strings.HasPrefix(req.Prefix, "You are writing the operating specification"):
		return mockSpec(req.Prompt)
	case strings.HasPrefix(req.Prefix, "You are turning an agent's operating"):
		return mockCharter(req.Prompt)
	}
	return "", fmt.Errorf("offline studio has no answer for this prompt")
}

// ---------------------------------------------------------------------------
// the answer key: what each space in the bundled corpus actually does
// ---------------------------------------------------------------------------

type scriptJob struct {
	Label    string   `json:"label"`
	Function string   `json:"function"`
	Entities []string `json:"entities"`
	Memory   []string `json:"memory"`
	Trigger  string   `json:"trigger"`
	Cadence  string   `json:"cadence"`
	Decision string   `json:"decision"`
	Tools    []string `json:"tools"`
	Handoff  string   `json:"handoff_to"`
	Quote    string   `json:"quote"`
}

// Jobs every space does. These carry the spread: the same craft, repeated in
// each vertical, is the case for one shared agent serving all of them.
var sharedJobs = []scriptJob{
	{"pause underperforming keywords", "ppc", []string{"campaign", "channel"}, []string{"campaign"},
		"weekly spend review", "weekly", "which losers are noise and which are seasonal",
		[]string{"Google Ads", "BI"}, "analytics", "pausing the 12 kws that burned budget with no leads again this week"},
	{"shift budget between campaigns", "ppc", []string{"campaign", "channel", "account"}, []string{"campaign", "channel"},
		"pacing alert", "daily", "how much to move before the model relearns",
		[]string{"Google Ads", "Meta Ads"}, "", "moved 30% from the generic campaign to brand, pacing was way off"},
	{"explain CPA spike", "analytics", []string{"campaign", "channel"}, []string{"campaign", "channel"},
		"CPA alert", "ad_hoc", "whether it is auction pressure or a tracking break",
		[]string{"BI", "Google Ads"}, "product", "CPA doubled overnight — checking if it is the tracking or the auction"},
	{"chase missing payout report", "partners", []string{"partner"}, []string{"partner"},
		"month end", "monthly", "when to escalate versus wait",
		[]string{"email", "Salesforce"}, "finance", "still no report from them, third time I am asking this month"},
	{"renegotiate CPA with partner", "partners", []string{"partner", "vertical"}, []string{"partner", "vertical"},
		"volume shift", "ad_hoc", "what floor to hold given last quarter",
		[]string{"Salesforce"}, "finance", "they want 15% off — last time we agreed a floor, holding it"},
}

// Jobs only one space does. These carry the coupling: work that lives in one
// place and tangles with everything else there is the case for an owner.
var localJobs = map[string][]scriptJob{
	"renters": {
		{"review flagged fraudulent leads", "quality", []string{"partner", "geo"}, []string{"partner", "geo"},
			"fraud flag", "daily", "whether the source is bad or the filter is",
			[]string{"BI", "internal dashboard"}, "partners", "same source again, 40% of yesterday's leads bounced"},
		{"update policy eligibility copy", "product", []string{"vertical"}, nil,
			"compliance request", "ad_hoc", "none", []string{"CMS"}, "", "legal wants the eligibility wording changed on the form"},
	},
	"travel": {
		{"fix booking funnel step", "product", []string{"account"}, []string{"account"},
			"drop in conversion", "ad_hoc", "whether to roll back or patch forward",
			[]string{"CMS", "BI"}, "analytics", "step 3 conversion fell off a cliff after yesterday's deploy"},
		{"refresh seasonal landing pages", "creative", []string{"campaign", "geo"}, []string{"campaign"},
			"season change", "monthly", "which creative angle to lead with",
			[]string{"CMS", "Figma"}, "ppc", "swapping the summer set in before the weekend push"},
	},
	"pet-insurance": {
		{"refresh ad creative set", "creative", []string{"campaign"}, []string{"campaign"},
			"creative fatigue", "weekly", "which angle is fatigued versus mispriced",
			[]string{"Meta Ads", "Figma"}, "ppc", "the puppy set is fatigued, CTR down three weeks running"},
		{"reconcile partner invoice", "finance", []string{"partner"}, []string{"partner"},
			"invoice received", "monthly", "whether the delta is ours or theirs",
			[]string{"NetSuite", "BI"}, "partners", "invoice is 8% over what BI says we owe them"},
	},
}

var spaceRe = regexp.MustCompile(`Space:\s*"([^"]*)"\s+Date:\s*(\S+)`)

func mockCensus(prompt string) (string, error) {
	m := spaceRe.FindStringSubmatch(prompt)
	space, date := "unknown", "undated"
	if m != nil {
		space, date = m[1], m[2]
	}

	pool := append([]scriptJob(nil), sharedJobs...)
	pool = append(pool, localJobs[space]...)

	// Rotate by day so volumes differ across days the way a real week does,
	// without any of it being random: the same day always yields the same jobs.
	seed := 0
	for _, r := range space + date {
		seed = seed*31 + int(r)
	}
	if seed < 0 {
		seed = -seed
	}
	n := 3 + seed%(len(pool)-2)
	jobs := make([]scriptJob, 0, n)
	for i := 0; i < n; i++ {
		jobs = append(jobs, pool[(seed+i)%len(pool)])
	}

	themes := []string{"spend pacing", "partner follow-ups"}
	if len(localJobs[space]) > 0 {
		themes = append(themes, localJobs[space][0].Function+" work")
	}
	systems := map[string]bool{}
	for _, j := range jobs {
		for _, t := range j.Tools {
			systems[t] = true
		}
	}
	names := make([]string, 0, len(systems))
	for s := range systems {
		names = append(names, s)
	}
	sort.Strings(names)

	return marshal(map[string]any{"themes": themes, "jobs": jobs, "systems": names})
}

// mockProfile writes the space narrative from the day catalogues it was actually
// handed, so two spaces with different work read differently offline. Everything
// it says is derived from the prompt — nothing about the corpus is hard-coded.
func mockProfile(prompt string) string {
	space := "this"
	if m := regexp.MustCompile(`Space "([^"]*)"`).FindStringSubmatch(prompt); m != nil {
		space = m[1]
	}
	days := 0
	if m := regexp.MustCompile(`(\d+) day catalogues`).FindStringSubmatch(prompt); m != nil {
		days, _ = strconv.Atoi(m[1])
	}

	themes := map[string]int{}
	for _, m := range regexp.MustCompile(`themes: ([^|]*)`).FindAllStringSubmatch(prompt, -1) {
		for _, t := range strings.Split(m[1], ";") {
			if t = strings.TrimSpace(t); t != "" {
				themes[t]++
			}
		}
	}
	jobs, byFunction := map[string]int{}, map[string]int{}
	for _, m := range jobCiteRe.FindAllStringSubmatch(prompt, -1) {
		jobs[jobLabel(m[1])]++
		byFunction[m[2]]++
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Over %d day catalogues the %s channel reads as %s work first — %s of the jobs logged here sit "+
		"in that function.", days, space, titleFunction(topKey(byFunction)), sharePhrase(byFunction))
	if t := topN(themes, 3); len(t) > 0 {
		fmt.Fprintf(&b, " What comes up day after day: %s.", strings.Join(t, ", "))
	}
	if j := topN(jobs, 4); len(j) > 0 {
		fmt.Fprintf(&b, " The recurring jobs are %s — the same shapes, on different objects, most days.",
			strings.Join(j, "; "))
	}
	fmt.Fprintf(&b, " Across the whole catalogue %d distinct job shapes appear over %d functions, which is what "+
		"decides whether one agent can hold this space or whether it has to be split.", len(jobs), len(byFunction))
	if len(byFunction) > 1 {
		fmt.Fprintf(&b, " The second strand is %s work, and it moves on its own clock: it arrives from a counterparty "+
			"or a calendar rather than from an alert, so the state it needs is the last thing agreed, not the last "+
			"thing measured.", titleFunction(secondKey(byFunction)))
	}
	b.WriteString(" What people carry from one day to the next is narrow and specific: which objects were already " +
		"touched and why, and what was decided about each the last time it came up.")
	return b.String()
}

var jobCiteRe = regexp.MustCompile(`([^;|]+?) \[(\w+)\]`)

// jobLabel cleans one capture from jobCiteRe. The first job on a day line sits
// directly after "jobs: ", and the separator class cannot exclude a space, so
// that prefix lands inside the capture — leave it and every day file
// contributes a phantom "jobs: <label>" beside the real one, inflating both the
// distinct-shape count and the per-function tallies.
func jobLabel(raw string) string {
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "jobs:"))
}

func topKey(m map[string]int) string {
	if k := topN(m, 1); len(k) > 0 {
		return k[0]
	}
	return "other"
}

func secondKey(m map[string]int) string {
	if k := topN(m, 2); len(k) > 1 {
		return k[1]
	}
	return topKey(m)
}

// sharePhrase is the leading function's share of all jobs, in words.
func sharePhrase(m map[string]int) string {
	total := 0
	for _, n := range m {
		total += n
	}
	if total == 0 {
		return "none"
	}
	switch pct := 100 * m[topKey(m)] / total; {
	case pct >= 75:
		return "most"
	case pct >= 50:
		return "over half"
	default:
		return fmt.Sprintf("about %d%%", pct)
	}
}

// topN ranks by count, breaking ties on the key so the output is deterministic.
func topN(m map[string]int, n int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}
		return keys[i] < keys[j]
	})
	return keys[:min(n, len(keys))]
}

func mockTaxonomy(prompt string) (string, error) {
	// Fold whatever labels arrived onto the answer key, so the taxonomy stays
	// honest to the corpus it was shown rather than to a hard-coded list.
	seen := map[string]bool{}
	for _, line := range strings.Split(prompt, "\n") {
		if i := strings.Index(line, " | function="); i > 0 {
			seen[normalizeLabel(line[:i])] = true
		}
	}
	var caps []capability
	add := func(j scriptJob) {
		id := j.Function + "-" + slug(j.Label)
		for _, c := range caps {
			if c.ID == id {
				return
			}
		}
		caps = append(caps, capability{
			ID: id, Name: j.Label, Function: j.Function,
			Aliases: []string{j.Label}, Summary: j.Decision,
		})
	}
	for _, j := range sharedJobs {
		if seen[normalizeLabel(j.Label)] {
			add(j)
		}
	}
	for _, js := range localJobs {
		for _, j := range js {
			if seen[normalizeLabel(j.Label)] {
				add(j)
			}
		}
	}
	if len(caps) == 0 {
		caps = append(caps, capability{ID: "other-unclassified", Name: "unclassified work", Function: "other"})
	}
	return marshal(map[string]any{"capabilities": caps})
}

var (
	agentLineRe  = regexp.MustCompile(`^ {2}(\S+)\s+(function|vertical|generalist)\s+(.+?)\s+(\d+\.\d)% of work\s+remembers: (.*)$`)
	perAxisRe    = regexp.MustCompile(`one instance per (\w+) \((\d+)\)`)
	sharedLineRe = regexp.MustCompile(`^ {2}(\w+)\s+read by (.+)$`)
	recallRe     = regexp.MustCompile(`(\w+) (\d\.\d+)`)
)

// mockRoster does what the prompt asks a real model to do when the evidence
// does not contradict the computed baseline: adopt it, and say so. It reads the
// baseline out of the brief it was handed rather than carrying its own copy, so
// changing the corpus changes the roster.
func mockRoster(prompt string) (string, error) {
	d := rosterDecision{Topology: "hybrid"}
	if m := regexp.MustCompile(`computed recommendation: (\w+) — (.*)`).FindStringSubmatch(prompt); m != nil {
		d.Topology = strings.ToLower(m[1])
		d.Verdict = "Adopting the computed shape: " + m[2] + "."
	}
	var spaces []string
	if m := regexp.MustCompile(`Spaces: (.*)`).FindStringSubmatch(prompt); m != nil {
		for _, s := range strings.Split(m[1], ",") {
			spaces = append(spaces, strings.TrimSpace(s))
		}
	}

	lines := strings.Split(prompt, "\n")
	inRoster, inShared := false, false
	for i, line := range lines {
		switch {
		case strings.HasPrefix(line, "COMPUTED ROSTER"):
			inRoster, inShared = true, false
			continue
		case strings.HasPrefix(line, "SHARED MEMORY IMPLIED"):
			inRoster, inShared = false, true
			continue
		case line != "" && !strings.HasPrefix(line, " "):
			inRoster, inShared = false, false
		}
		if inShared {
			if m := sharedLineRe.FindStringSubmatch(line); m != nil {
				var readers []string
				for _, r := range strings.Split(m[2], ",") {
					readers = append(readers, strings.TrimSpace(r))
				}
				d.Shared = append(d.Shared, sharedMemory{Axis: m[1], Readers: readers,
					Why: fmt.Sprintf("%d agents key on %s; one ledger keeps their versions from drifting apart",
						len(readers), m[1])})
			}
			continue
		}
		if !inRoster {
			continue
		}
		m := agentLineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		a := agentDecl{ID: m[1], Name: agentTitle(m[1]), Partition: partition{Axis: "global", Instances: 1}}
		if pm := perAxisRe.FindStringSubmatch(m[3]); pm != nil {
			n, _ := strconv.Atoi(pm[2])
			a.Partition = partition{Axis: pm[1], Instances: n, Keys: spaces}
		}
		for _, rm := range recallRe.FindAllStringSubmatch(m[5], -1) {
			w, _ := strconv.ParseFloat(rm[2], 64)
			a.Remembers = append(a.Remembers, memoryAxis{Axis: rm[1], Weight: w,
				Why: "recall keyed on " + rm[1] + " is what the measured job mix asks for"})
		}
		for j := i + 1; j < len(lines) && j <= i+3; j++ {
			switch {
			case strings.HasPrefix(lines[j], "      owns: "):
				for _, id := range strings.Split(strings.TrimPrefix(lines[j], "      owns: "), ",") {
					if id = strings.TrimSpace(strings.TrimSuffix(id, "…")); id != "" {
						a.Capabilities = append(a.Capabilities, id)
					}
				}
			case strings.HasPrefix(lines[j], "      why:  "):
				a.Why = strings.TrimSpace(strings.TrimPrefix(lines[j], "      why:  "))
			}
		}
		a.Mission = fmt.Sprintf("Own %s work across its scope and keep the recall that work depends on.", m[2])
		d.Agents = append(d.Agents, a)
	}

	if len(d.Agents) == 0 {
		d.Agents = []agentDecl{{ID: "operator", Name: "Operations agent",
			Mission:   "Own the work the corpus shows, end to end.",
			Partition: partition{Axis: "global", Instances: 1}}}
	}
	// The brief carries a score and an argument against each alternative; pair
	// them rather than restating the number as if it were a reason.
	against := map[string]string{}
	for _, r := range regexp.MustCompile(`(?m)^  against (\w+)\s+(.*)$`).FindAllStringSubmatch(prompt, -1) {
		against[r[1]] = strings.TrimSpace(r[2])
	}
	for _, r := range regexp.MustCompile(`(?m)^  (function|vertical|hybrid|single)\s+([\d.]+) = `).FindAllStringSubmatch(prompt, -1) {
		if r[1] == d.Topology {
			continue
		}
		why := against[r[1]]
		if why == "" {
			why = "scores below " + d.Topology
		}
		d.Rejected = append(d.Rejected, rejection{Shape: r[1],
			WhyNot: fmt.Sprintf("%s (scores %s against %s for %s)", why, r[2], scoreOf(prompt, d.Topology), d.Topology)})
	}
	return marshal(d)
}

func mockSpec(prompt string) (string, error) {
	name := field(prompt, "name: ")
	recall := field(prompt, "measured recall ranking: ")
	axesRanked := recallRe.FindAllStringSubmatch(recall, -1)
	primary := "partner"
	if len(axesRanked) > 0 {
		primary = axesRanked[0][1]
	}
	var secondary []string
	for _, m := range axesRanked[min(1, len(axesRanked)):] {
		secondary = append(secondary, m[1])
	}
	var tools []any
	for _, t := range strings.Split(field(prompt, "SYSTEMS NAMED IN THIS WORK: "), ",") {
		if t = strings.TrimSpace(t); t != "" {
			tools = append(tools, map[string]any{"name": t, "use": "read the current state before deciding", "access": "read"})
		}
	}
	if len(tools) == 0 {
		tools = append(tools, map[string]any{"name": "BI", "use": "pull the numbers behind a decision", "access": "read"})
	}

	return marshal(map[string]any{
		"mission": fmt.Sprintf("%s owns its capabilities end to end and keeps a durable record keyed on %s.", orNone(name), primary),
		"scope": map[string]any{
			"owns":     linesUnder(prompt, "CAPABILITIES IT OWNS"),
			"excluded": []string{"anything another agent on the roster already owns"},
		},
		"memory": map[string]any{
			"primary_key":    primary,
			"secondary_keys": secondary,
			"working":        "the current request, the rows it pulled for it, and the draft decision",
			"episodic": map[string]any{
				"record":    fmt.Sprintf("one row per event: %s, date, what changed, what was decided, who decided", primary),
				"keyed_by":  primary + " + date",
				"retention": "18 months — a year of seasonality plus the quarter being compared to it",
			},
			"semantic": map[string]any{
				"record":       fmt.Sprintf("one profile per %s: current state, agreed terms, open threads, owner", primary),
				"keyed_by":     primary,
				"updated_when": "an episodic event contradicts the profile",
			},
			"why_this_key": fmt.Sprintf("every task arrives naming a %s, so recall that is not keyed on it is a scan", primary),
		},
		"tools": tools,
		"triggers": []any{
			map[string]any{"when": "the scheduled review for its cadence", "does": "re-read state and propose the changes it would make"},
			map[string]any{"when": "an alert on a metric it owns", "does": "reconstruct the history and explain the move"},
		},
		"outputs": []any{
			map[string]any{"what": "a proposed change with the evidence behind it", "to": "the human owner"},
			map[string]any{"what": "an updated profile row", "to": "its own semantic store"},
		},
		"handoffs": []any{
			map[string]any{"to": "the owning human", "when": "money or a commitment to a counterparty changes", "payload": "the proposal, the history, and the alternative"},
		},
		"guardrails": []string{
			"never change spend or send anything outward without an explicit approval",
			"never write a profile field it cannot point at an episodic row for",
			fmt.Sprintf("never answer from a %s profile older than its last event", primary),
		},
		"evals": []any{
			map[string]any{"name": "recall precision", "checks": "the state it reports for a " + primary + " matches the source system"},
			map[string]any{"name": "no invented history", "checks": "every claim resolves to a stored event"},
		},
		"external_lookups": []any{
			map[string]any{"what": "spend and conversion actuals", "where": "the BI warehouse — chat only ever paraphrases them"},
		},
		"risks": []any{
			map[string]any{"what": "the profile drifts from reality between events", "mitigation": "reconcile against the source system on the cadence it runs"},
		},
	})
}

func mockCharter(prompt string) (string, error) {
	name, id := "the agent", "agent"
	if m := regexp.MustCompile(`Agent "([^"]*)" \(([^)]*)\)`).FindStringSubmatch(prompt); m != nil {
		name, id = m[1], m[2]
	}
	primary := "partner"
	if m := regexp.MustCompile(`"primary_key":\s*"([^"]*)"`).FindStringSubmatch(prompt); m != nil {
		primary = m[1]
	}
	col := primary + "_id"

	return marshal(map[string]any{
		"memory_schema": []any{
			map[string]any{
				"store": "episodic", "name": id + "_events", "key": col + " + occurred_on",
				"fields": []any{
					map[string]any{"name": col, "type": "string", "note": "the " + primary + " this event is about"},
					map[string]any{"name": "occurred_on", "type": "date", "note": "when it happened, not when it was written"},
					map[string]any{"name": "kind", "type": "enum", "note": "decision | change | escalation | observation"},
					map[string]any{"name": "what", "type": "text", "note": "what happened, in one line"},
					map[string]any{"name": "evidence", "type": "text", "note": "the source line or query behind it"},
				},
				"example": map[string]any{col: "acme-leads", "occurred_on": "2026-03-04", "kind": "decision",
					"what": "held the CPA floor after a request for 15% off", "evidence": "chat, renters, 2026-03-04"},
			},
			map[string]any{
				"store": "semantic", "name": id + "_profiles", "key": col,
				"fields": []any{
					map[string]any{"name": col, "type": "string", "note": "primary key"},
					map[string]any{"name": "state", "type": "enum", "note": "current standing in one word"},
					map[string]any{"name": "agreed_terms", "type": "json", "note": "what is currently committed"},
					map[string]any{"name": "open_threads", "type": "json", "note": "what is unresolved, with an owner"},
					map[string]any{"name": "last_event_on", "type": "date", "note": "staleness guard for every read"},
				},
				"example": map[string]any{col: "acme-leads", "state": "steady",
					"agreed_terms": map[string]any{"cpa_floor": 42}, "open_threads": []any{"missing March report"},
					"last_event_on": "2026-03-04"},
			},
		},
		"system_prompt": fmt.Sprintf(`You are %s. You own a narrow set of jobs and you are judged on whether your `+
			`recall is right, not on how much you say.

Before you act, read. Every task you get names a %s: load its profile from %s_profiles and its `+
			`recent rows from %s_events. If the profile's last_event_on is older than the newest event, `+
			`reconcile before you answer — a stale profile is worse than no profile.

When you answer, every factual claim must resolve to a stored event or a live query you ran. If you `+
			`cannot point at one, say what you would need and where it lives. Never fill a gap with something `+
			`plausible.

After you act, write. One episodic row per thing that happened, dated when it happened. Update the `+
			`profile only where an event contradicts it, and never write a profile field you have no event for.

You propose; you do not commit. Anything that moves money, changes spend, or reaches a counterparty `+
			`goes to your human owner with the history and the alternative you rejected.`, name, primary, id, id),
		"first_week": []string{
			fmt.Sprintf("backfill %s_events from the last 90 days and check the profiles it produces against what the owner believes", id),
			"run read-only alongside the current owner for five days and diff every proposal against what they did",
			"turn on writes to the episodic store only; keep profiles human-approved until precision holds",
		},
	})
}

// ---------------------------------------------------------------------------

// scoreOf reads one shape's score back out of the brief.
func scoreOf(prompt, shape string) string {
	re := regexp.MustCompile(`(?m)^  ` + regexp.QuoteMeta(shape) + `\s+([\d.]+) = `)
	if m := re.FindStringSubmatch(prompt); m != nil {
		return m[1]
	}
	return "?"
}

func field(prompt, label string) string {
	i := strings.Index(prompt, label)
	if i < 0 {
		return ""
	}
	rest := prompt[i+len(label):]
	if j := strings.IndexByte(rest, '\n'); j >= 0 {
		rest = rest[:j]
	}
	return strings.TrimSpace(rest)
}

func linesUnder(prompt, header string) []string {
	i := strings.Index(prompt, header)
	if i < 0 {
		return nil
	}
	var out []string
	for _, line := range strings.Split(prompt[i:], "\n")[1:] {
		if !strings.HasPrefix(line, "  ") {
			break
		}
		line = strings.TrimSpace(line)
		if j := strings.Index(line, " — "); j > 0 {
			line = line[:j]
		}
		if line != "" && !strings.HasPrefix(line, "(") {
			out = append(out, line)
		}
		if len(out) >= 8 {
			break
		}
	}
	return out
}

func marshal(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
