package main

// Everything here runs offline against the scripted studio in studio.go. No key,
// no network, no fixtures outside t.TempDir() and the bundled corpus.
//
// The tests are split three ways on purpose. The scoring tests pin the maths
// that decides the org shape, because that decision is the whole point of the
// example and a silent drift in it would still produce a plausible-looking
// document. The privacy tests pin the scrubber, because a regression there
// leaks. The pipeline tests run all three Loom runs end to end and assert the
// invariant that ties them together: an agent never remembers its own partition
// key.

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
)

// ---------------------------------------------------------------------------
// corpus discovery
// ---------------------------------------------------------------------------

func fixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	files := map[string][]string{
		"renters/2026-03-02.jsonl": {
			`{"sender":{"name":"Dana Levi","type":"HUMAN"},"createTime":"2026-03-02T09:15:00Z","text":"CPA doubled overnight, mailing dana.levi@example.com the sheet"}`,
			`{"sender":{"name":"Omer Katz","type":"HUMAN"},"createTime":"2026-03-02T09:16:00Z","text":"@Dana Levi call me on +972-52-555-0134, id 987654321"}`,
		},
		"renters/2026-03-03.jsonl": {
			`{"sender":{"name":"Dana Levi","type":"HUMAN"},"createTime":"2026-03-03T10:00:00Z","text":"paused the losing keywords, see https://ads.example.com/x"}`,
		},
		"renters/2026-03-09.jsonl": {
			`{"sender":{"name":"Omer Katz","type":"HUMAN"},"createTime":"2026-03-09T10:00:00Z","text":"partner still owes us the March report"}`,
		},
		"travel/2026-03-02.jsonl": {
			`{"sender":{"name":"Maya Bar","type":"HUMAN"},"createTime":"2026-03-02T11:00:00Z","text":"booking funnel step 3 fell off a cliff"}`,
		},
		"renters/notes.txt":   {"ignored"},
		"renters/draft.jsonl": {`{"sender":{"name":"x","type":"HUMAN"},"createTime":"","text":"undated, must be skipped"}`},
	}
	for rel, lines := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestDiscoverNestedLayout(t *testing.T) {
	root := fixture(t)

	files, spaces, err := discover(root, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(spaces, ","); got != "renters,travel" {
		t.Fatalf("spaces = %q, want renters,travel", got)
	}
	if len(files) != 4 {
		t.Fatalf("got %d day files, want 4 (draft.jsonl has no date and must be skipped)", len(files))
	}
	for _, f := range files {
		if strings.Contains(f.Path, "draft") || strings.Contains(f.Path, "notes") {
			t.Fatalf("undated/non-jsonl file leaked into the corpus: %s", f.Path)
		}
	}

	// -since / -until clamp on the date in the filename.
	files, _, err = discover(root, "2026-03-03", "2026-03-03", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Date != "2026-03-03" {
		t.Fatalf("date window gave %+v, want the single 2026-03-03 day", files)
	}

	// -last trims per space, not globally: renters keeps its newest day and
	// travel keeps its only one.
	files, _, err = discover(root, "", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("-last 1 gave %d files, want 1 per space", len(files))
	}
	for _, f := range files {
		if f.Space == "renters" && f.Date != "2026-03-09" {
			t.Fatalf("-last kept %s for renters, want the newest day", f.Date)
		}
	}
}

func TestDiscoverFlatLayout(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"ppc-israel.jsonl", "bizdev.jsonl"} {
		line := `{"sender":{"name":"A","type":"HUMAN"},"createTime":"2026-03-02T09:00:00Z","text":"hi"}`
		if err := os.WriteFile(filepath.Join(root, name), []byte(line+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	files, spaces, err := discover(root, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || len(spaces) != 2 {
		t.Fatalf("flat layout gave %d files / %d spaces, want 2 / 2", len(files), len(spaces))
	}
	if spaces[0] != "bizdev" {
		t.Fatalf("space name %q should come from the filename", spaces[0])
	}
}

func TestDiscoverEmptyRoot(t *testing.T) {
	if _, _, err := discover(t.TempDir(), "", "", 0); err == nil {
		t.Fatal("an empty corpus should be an error, not an empty run")
	}
}

// ---------------------------------------------------------------------------
// privacy — this is the test that matters most; a regression here leaks
// ---------------------------------------------------------------------------

func TestLoadDayScrubsIdentifiers(t *testing.T) {
	root := fixture(t)
	files, _, err := discover(root, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	s := newScrubber("test-salt")

	var all strings.Builder
	perDay := map[string]string{} // day -> the messages block for that day
	for _, f := range files {
		rec, ok, err := loadDay(f, s)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			continue
		}
		body := rec.String("messages")
		all.WriteString(body + "\n")
		if f.Space == "renters" {
			perDay[f.Date] = body
		}
	}

	corpus := strings.ToLower(all.String())
	for _, forbidden := range []string{
		"dana", "levi", "omer", "katz", "maya", // real names, in any casing
		"dana.levi@example.com", "@example.com", // addresses
		"+972", "555-0134", // phone
		"987654321",               // long id
		"https://ads.example.com", // url
	} {
		if strings.Contains(corpus, strings.ToLower(forbidden)) {
			t.Errorf("scrubbed corpus still contains %q", forbidden)
		}
	}
	for _, want := range []string{"TM-", "<email>", "<phone>", "<id>", "<url>"} {
		if !strings.Contains(all.String(), want) {
			t.Errorf("scrubbed corpus is missing the %q placeholder — check the redaction rules", want)
		}
	}

	// The same person must get the same pseudonym on every day, or a handoff
	// between two days looks like a handoff between two people.
	who := regexp.MustCompile(`TM-[0-9a-f]{4}`)
	dana := map[string]bool{}
	for date, body := range perDay {
		found := who.FindAllString(body, -1)
		if len(found) == 0 {
			t.Errorf("no pseudonyms on renters/%s", date)
		}
		for _, p := range found {
			dana[p] = true
		}
	}
	// renters has exactly two speakers across three days.
	if len(dana) != 2 {
		t.Fatalf("renters resolved to %d distinct people (%v), want 2 — pseudonyms are not stable across days",
			len(dana), dana)
	}

	// A different salt must produce a different pseudonym for the same name,
	// so two corpora cannot be joined on the hash.
	if newScrubber("other-salt").pseudonym("Dana Levi") == newScrubber("test-salt").pseudonym("Dana Levi") {
		t.Fatal("pseudonyms are not salted — the same name hashes identically across corpora")
	}
}

// The mention rule matches @ followed by letters, which is also the shape of a
// mail domain. If it runs before the address rule the domain is consumed as a
// mention and the local part — usually someone's actual name — survives.
func TestScrubberDoesNotSplitEmailAddresses(t *testing.T) {
	s := newScrubber("salt")
	cases := map[string]string{
		"mail dana.levi@example.com the sheet":  "mail <email> the sheet",
		"cc: first.last+tag@sub.example.co.il":  "cc: <email>",
		"ask @Dana Levi about it":               "", // pseudonym, asserted below
		"ping @Omer and dana@example.com again": "", // both rules on one line
	}
	for in, want := range cases {
		got := s.text(in)
		if want != "" && got != want {
			t.Errorf("text(%q) = %q, want %q", in, got, want)
		}
		for _, leak := range []string{"dana", "levi", "omer", "first.last", "example.com"} {
			if strings.Contains(strings.ToLower(got), leak) {
				t.Errorf("text(%q) = %q — leaks %q", in, got, leak)
			}
		}
	}
	if got := s.text("ask @Dana Levi about it"); !strings.Contains(got, "TM-") {
		t.Errorf("a mention should become a pseudonym, got %q", got)
	}

	// A different salt must produce a different pseudonym for the same name,
	// so two corpora cannot be joined on the hash.
	if newScrubber("other-salt").pseudonym("Dana Levi") == newScrubber("test-salt").pseudonym("Dana Levi") {
		t.Fatal("pseudonyms are not salted — the same name hashes identically across corpora")
	}
}

func TestLoadDaySkipsEmpty(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "empty.jsonl")
	if err := os.WriteFile(path, []byte("\n{ not json }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, ok, err := loadDay(dayFile{Space: "empty", Path: path}, newScrubber("s"))
	if err != nil {
		t.Fatalf("a malformed line should be skipped, not fatal: %v", err)
	}
	if ok {
		t.Fatal("a day with no usable messages should not produce a record")
	}
}

// ---------------------------------------------------------------------------
// the scoring maths
// ---------------------------------------------------------------------------

func TestEntropyNorm(t *testing.T) {
	cases := []struct {
		name    string
		counts  map[string]int
		buckets int
		want    float64
	}{
		{"one bucket is never spread", map[string]int{"a": 10}, 1, 0},
		{"all in one of three", map[string]int{"a": 9}, 3, 0},
		{"perfectly even over three", map[string]int{"a": 3, "b": 3, "c": 3}, 3, 1},
		{"even over two of four", map[string]int{"a": 5, "b": 5}, 4, 0.5},
		{"nothing", map[string]int{}, 3, 0},
	}
	for _, tc := range cases {
		if got := entropyNorm(tc.counts, tc.buckets); math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("%s: entropyNorm = %.4f, want %.4f", tc.name, got, tc.want)
		}
	}
}

func TestRankAxesDropsPartitionKey(t *testing.T) {
	mix := map[string]float64{"vertical": 10, "partner": 6, "campaign": 3, "geo": 1, "person": 0.05}

	ranked := rankAxes(mix, "vertical")
	if len(ranked) == 0 {
		t.Fatal("no axes ranked")
	}
	for _, a := range ranked {
		if a.Axis == "vertical" {
			t.Fatal("an agent partitioned by vertical must not remember vertical — inside one instance it is a constant")
		}
	}
	if ranked[0].Axis != "partner" {
		t.Fatalf("top axis = %q, want partner (the heaviest remaining)", ranked[0].Axis)
	}
	var sum float64
	for _, a := range ranked {
		sum += a.Weight
	}
	if math.Abs(sum-1) > 1e-6 {
		t.Fatalf("weights sum to %.4f, want 1 after the partition axis is removed", sum)
	}
	for _, a := range ranked {
		if a.Axis == "person" {
			t.Fatal("a 0.5%% axis is noise and should be dropped, not carried into a memory design")
		}
	}
	if len(ranked) > 4 {
		t.Fatalf("ranked %d axes; a memory design with more than 4 keys is not a design", len(ranked))
	}

	// With no partition axis to exclude, the heaviest axis leads.
	if top := rankAxes(mix, "global"); top[0].Axis != "vertical" {
		t.Fatalf("unpartitioned top axis = %q, want vertical", top[0].Axis)
	}
}

// corner builds a census that lands exactly on one corner of the spread ×
// coupling square, which is the only way to assert the shape choice without
// asserting the whole corpus.
func corner(spread, coupling float64) census {
	return census{
		Observations: 100,
		Capabilities: []capStats{{Cap: capability{ID: "c", Function: "ppc"}, Count: 100, Spread: spread}},
		SpaceStats:   []spaceStats{{Space: "s", Count: 100, Coupling: coupling}},
	}
}

func TestScoreTopologyCorners(t *testing.T) {
	cases := []struct {
		spread, coupling float64
		want             string
		because          string
	}{
		{0.95, 0.05, "function", "the same craft everywhere, untangled — one agent per craft"},
		{0.05, 0.95, "vertical", "tangled in place, not repeated — one owner per place"},
		{0.90, 0.90, "hybrid", "both forces live — shared craft plus a local owner"},
		{0.05, 0.05, "single", "neither force — do not split at all"},
	}
	for _, tc := range cases {
		got := scoreTopology(corner(tc.spread, tc.coupling))
		if got.Recommended != tc.want {
			t.Errorf("spread %.2f coupling %.2f → %q, want %q (%s); scores %v",
				tc.spread, tc.coupling, got.Recommended, tc.want, tc.because, got.Scores)
		}
		if len(got.Rejected) != 3 {
			t.Errorf("expected the other three shapes to be recorded as rejected, got %d", len(got.Rejected))
		}
		for i := 1; i < len(got.Rejected); i++ {
			if got.Rejected[i].Score > got.Rejected[i-1].Score {
				t.Error("rejected shapes should be ordered by score, closest runner-up first")
			}
		}
		if got.Rationale == "" {
			t.Error("every verdict needs a rationale citing the two numbers")
		}
	}
}

func TestSharedLedgersNeedTwoReaders(t *testing.T) {
	agents := []agentProposal{
		{ID: "ppc", Remembers: []memoryAxis{{Axis: "campaign", Weight: 0.7}, {Axis: "channel", Weight: 0.3}}},
		{ID: "analytics", Remembers: []memoryAxis{{Axis: "campaign", Weight: 0.6}, {Axis: "geo", Weight: 0.4}}},
		{ID: "partners", Remembers: []memoryAxis{{Axis: "partner", Weight: 0.9}, {Axis: "campaign", Weight: 0.02}}},
	}
	shared := sharedLedgers(agents)

	byAxis := map[string][]string{}
	for _, s := range shared {
		byAxis[s.Axis] = s.Readers
	}
	if len(byAxis["campaign"]) != 2 {
		t.Fatalf("campaign readers = %v, want the two agents above the weight floor (partners at 0.02 is not a reader)",
			byAxis["campaign"])
	}
	if _, ok := byAxis["partner"]; ok {
		t.Error("an axis only one agent keys on is that agent's private memory, not a shared ledger")
	}
	if _, ok := byAxis["geo"]; ok {
		t.Error("geo has a single reader and should not be a ledger")
	}
}

func TestProposeAgentsExcludesOwnPartitionAxis(t *testing.T) {
	cen := buildCensus(sampleObs(), nil, []string{"renters", "travel", "pet-insurance"})
	for _, a := range proposeAgents(cen) {
		if a.Partition.Axis == "global" {
			continue
		}
		for _, m := range a.Remembers {
			if m.Axis == a.Partition.Axis {
				t.Errorf("agent %s is partitioned by %s and also claims to remember it", a.ID, a.Partition.Axis)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// folding raw labels onto a taxonomy
// ---------------------------------------------------------------------------

func TestMatcherFoldsAliases(t *testing.T) {
	caps := []capability{
		{ID: "ppc-pause-keywords", Name: "pause underperforming keywords", Function: "ppc",
			Aliases: []string{"pause losing keywords", "prune keywords"}},
		{ID: "partners-chase-report", Name: "chase missing payout report", Function: "partners",
			Aliases: []string{"chase partner report"}},
	}
	m := newMatcher(caps)

	cases := []struct {
		label, fn, want string
	}{
		{"pause underperforming keywords", "ppc", "ppc-pause-keywords"}, // exact
		{"prune keywords", "ppc", "ppc-pause-keywords"},                 // alias
		{"pause losing keywords again", "ppc", "ppc-pause-keywords"},    // superset of an alias
		{"chase partner report", "partners", "partners-chase-report"},   // other family
	}
	for _, tc := range cases {
		got, ok := m.match(normalizeLabel(tc.label), tc.fn)
		if !ok || got.ID != tc.want {
			t.Errorf("match(%q) = %q ok=%v, want %q", tc.label, got.ID, ok, tc.want)
		}
	}
	if _, ok := m.match(normalizeLabel("write the quarterly board deck"), "other"); ok {
		t.Error("an unrelated label should not be forced onto the taxonomy")
	}

	// Unmatched labels are kept, flagged, and stable across calls — dropping
	// them would silently shrink the corpus the design is scored on.
	a := m.synthesize("write the quarterly board deck", "other")
	b := m.synthesize("write the quarterly board deck", "other")
	if a.ID != b.ID {
		t.Fatalf("synthesized ids are not stable: %q vs %q", a.ID, b.ID)
	}
	if !a.Synthesized {
		t.Error("a synthesized capability must be marked, so the report can show the match rate honestly")
	}
}

func TestDedupeLabelsCollapsesTheCorpus(t *testing.T) {
	obs := []jobObs{
		{Space: "renters", Label: "Pause underperforming keywords", Function: "ppc"},
		{Space: "renters", Label: "pause  underperforming keywords!", Function: "ppc"},
		{Space: "travel", Label: "pause underperforming keywords", Function: "ppc"},
		{Space: "travel", Label: "chase missing payout report", Function: "partners"},
	}
	got := dedupeLabels(obs)
	if len(got) != 2 {
		t.Fatalf("deduped to %d labels, want 2: %+v", len(got), got)
	}
	for _, l := range got {
		if strings.Contains(l.Label, "keywords") {
			if l.Count != 3 {
				t.Errorf("keyword label count = %d, want 3", l.Count)
			}
			if len(l.Spaces) != 2 {
				t.Errorf("keyword label spaces = %v, want both", l.Spaces)
			}
		}
	}
}

func TestNormalizeLabelKeepsNonLatin(t *testing.T) {
	// The corpus this was built for is bilingual; stripping non-ASCII would
	// erase whole job families rather than normalising them.
	if got := normalizeLabel("עצירת מילות מפתח!"); !strings.Contains(got, "מילות") {
		t.Fatalf("normalizeLabel dropped non-Latin text: %q", got)
	}
}

func TestMockProfileCountsRealJobsOnly(t *testing.T) {
	// The scripted profile parses the day lines it is handed. The first job on
	// a line sits right after "jobs: ", so a naive capture picks the prefix up
	// and every day file donates a phantom shape — the count in the narrative
	// then reads high and the function shares are computed off it.
	// The same two jobs lead on different days, so a prefix left in the capture
	// splits each of them into two shapes: 3 real, 5 apparent.
	prompt := `Space "renters" — 2 day catalogues.
2026-03-02 — themes: payouts | jobs: pause underperforming keywords [ppc]; chase payout report [partners]
2026-03-03 — themes: payouts | jobs: chase payout report [partners]; pause underperforming keywords [ppc]; explain CPA spike [ppc]`

	got := mockProfile(prompt)
	if strings.Contains(got, "jobs:") {
		t.Errorf("profile leaked the line prefix as a job name:\n%s", got)
	}
	if !strings.Contains(got, "3 distinct job shapes") {
		t.Errorf("want 3 distinct shapes across the two days, got:\n%s", got)
	}
	if !strings.Contains(got, "over 2 functions") {
		t.Errorf("want ppc and partners counted once each, got:\n%s", got)
	}
	if !strings.Contains(got, "PPC work first") {
		t.Errorf("want PPC as the leading function, got:\n%s", got)
	}
}

// ---------------------------------------------------------------------------
// the three runs, end to end, against the scripted studio
// ---------------------------------------------------------------------------

func offlineRun(t *testing.T) (*loom.RunResult, *loom.RunResult, *loom.RunResult, census, rosterDecision) {
	t.Helper()
	reg, _, err := buildRegistry("mock", 0)
	if err != nil {
		t.Fatal(err)
	}
	lu, err := lineupOf(reg)
	if err != nil {
		t.Fatal(err)
	}
	files, spaces, err := discover("corpus", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	opts := []loom.Option{loom.WithRegistry(reg), loom.WithWorkers(4)}

	censusRun, err := loom.Run(ctx, buildCensusPipeline(files, spaces, lu, newScrubber("t"), 4, nil), opts...)
	if err != nil {
		t.Fatalf("census run: %v", err)
	}
	obs, systems := collectJobs(censusRun.StageOutputs["day-census"])
	if len(obs) == 0 {
		t.Fatal("no job observations came out of the census run")
	}

	rosterRun, err := loom.Run(ctx, buildRosterPipeline(obs, spaces, systems, lu), opts...)
	if err != nil {
		t.Fatalf("roster run: %v", err)
	}
	var cen census
	scored := rosterRun.StageOutputs["score"]
	if len(scored) == 0 {
		t.Fatal("the score stage produced nothing")
	}
	if err := json.Unmarshal([]byte(scored[0].String("census_json")), &cen); err != nil {
		t.Fatalf("decode census: %v", err)
	}
	decision := rosterFrom(rosterRun.StageOutputs["roster"][0], cen)

	profiles := map[string]string{}
	for _, s := range spaces {
		if recs := censusRun.StageOutputs["profile-"+s]; len(recs) > 0 {
			profiles[s] = recs[0].String("output")
		}
	}
	designRun, err := loom.Run(ctx, buildDesignPipeline(decision, cen, profiles, lu, 4), opts...)
	if err != nil {
		t.Fatalf("design run: %v", err)
	}
	return censusRun, rosterRun, designRun, cen, decision
}

func TestPipelinesEndToEnd(t *testing.T) {
	censusRun, rosterRun, designRun, cen, decision := offlineRun(t)

	if got := len(censusRun.StageOutputs["day-census"]); got != 12 {
		t.Errorf("day-census produced %d records, want one per bundled day file (12)", got)
	}
	for _, s := range []string{"renters", "travel", "pet-insurance"} {
		if len(censusRun.StageOutputs["profile-"+s]) != 1 {
			t.Errorf("space %s has no prose profile", s)
		}
	}
	if len(rosterRun.StageOutputs["capability-map"]) == 0 {
		t.Error("capability-map produced nothing")
	}

	if cen.Observations == 0 || len(cen.Capabilities) == 0 {
		t.Fatalf("census is empty: %d observations, %d capabilities", cen.Observations, len(cen.Capabilities))
	}
	if cen.MatchRate < 0.5 {
		t.Errorf("match rate %.2f — most labels failed to fold onto the taxonomy", cen.MatchRate)
	}
	if cen.Topology.Recommended == "" {
		t.Error("no topology was recommended")
	}
	if cen.Topology.Spread <= 0 || cen.Topology.Coupling <= 0 {
		t.Errorf("spread %.2f / coupling %.2f — the bundled corpus has both by construction",
			cen.Topology.Spread, cen.Topology.Coupling)
	}

	if len(decision.Agents) == 0 {
		t.Fatal("the roster has no agents")
	}
	seen := map[string]bool{}
	for _, a := range decision.Agents {
		if seen[a.ID] {
			t.Errorf("duplicate agent id %q", a.ID)
		}
		seen[a.ID] = true
		if a.Name == "" || a.Mission == "" {
			t.Errorf("agent %q is missing a name or mission", a.ID)
		}
		if a.Partition.Instances < 1 {
			t.Errorf("agent %q has %d instances", a.ID, a.Partition.Instances)
		}
		// The invariant the whole example is built around.
		for _, m := range a.Remembers {
			if m.Axis == a.Partition.Axis {
				t.Errorf("agent %q is partitioned by %s and still lists it as memory", a.ID, m.Axis)
			}
		}
	}

	specs := designRun.StageOutputs["agent-spec"]
	charters := designRun.StageOutputs["agent-charter"]
	if len(specs) != len(decision.Agents) || len(charters) != len(decision.Agents) {
		t.Fatalf("designed %d specs / %d charters for %d agents", len(specs), len(charters), len(decision.Agents))
	}
	for _, r := range charters {
		if r.String("system_prompt") == "" {
			t.Errorf("agent %s has an empty system prompt", r.String("agent_id"))
		}
	}
	for _, r := range specs {
		mem, _ := r.Data["memory"].(map[string]any)
		if mem == nil || mem["primary_key"] == "" {
			t.Errorf("agent %s has no memory primary key — the point of the spec", r.String("agent_id"))
		}
	}
}

// The end-to-end determinism check below only catches an unsorted prompt line
// when Go's map order actually differs between the two runs, which for a small
// set is a coin flip. This pins the same thing outright: the systems line is
// built from a map, and it has to come out ordered every time.
func TestDossierSystemsLineIsOrdered(t *testing.T) {
	c := census{Capabilities: []capStats{
		{Cap: capability{ID: "payout-chase", Function: "partners"}, Tools: []string{"looker", "affise", "bigquery"}},
		{Cap: capability{ID: "cpa-triage", Function: "ppc"}, Tools: []string{"google-ads", "bigquery", "asana"}},
	}}
	a := agentDecl{
		ID:           "bizdev",
		Name:         "Bizdev",
		Capabilities: []string{"payout-chase", "cpa-triage"},
		Partition:    partition{Axis: "global"},
	}
	d := rosterDecision{Topology: "hybrid", Agents: []agentDecl{a}}

	const want = "\nSYSTEMS NAMED IN THIS WORK: affise, asana, bigquery, google-ads, looker\n"
	// Every call re-ranges the same map, so a missing sort shows up as soon as
	// the runtime picks a different order — hammer it rather than trusting one.
	for i := 0; i < 64; i++ {
		if got := agentDossier(a, d, c, nil); !strings.Contains(got, want) {
			line := "«no systems line»"
			for _, l := range strings.Split(got, "\n") {
				if strings.HasPrefix(l, "SYSTEMS NAMED IN THIS WORK:") {
					line = l
				}
			}
			t.Fatalf("call %d built the systems line out of map order: %s", i, line)
		}
	}
}

func TestOfflineRunIsDeterministic(t *testing.T) {
	_, _, designA, a, da := offlineRun(t)
	_, _, designB, b, db := offlineRun(t)

	if a.Topology.Recommended != b.Topology.Recommended {
		t.Fatalf("two runs of the same corpus disagree on the shape: %s vs %s",
			a.Topology.Recommended, b.Topology.Recommended)
	}
	if math.Abs(a.Topology.Spread-b.Topology.Spread) > 1e-9 {
		t.Fatalf("spread is not stable: %.6f vs %.6f", a.Topology.Spread, b.Topology.Spread)
	}
	if len(da.Agents) != len(db.Agents) {
		t.Fatalf("roster size is not stable: %d vs %d", len(da.Agents), len(db.Agents))
	}
	for i := range da.Agents {
		if da.Agents[i].ID != db.Agents[i].ID {
			t.Fatalf("roster order is not stable at %d: %s vs %s", i, da.Agents[i].ID, db.Agents[i].ID)
		}
	}

	// The shape and the roster being stable is not enough: the design run builds
	// one prompt per agent, and anything assembled there out of a Go map lands in
	// the prompt in iteration order. That is invisible in the roster and loud in
	// the output — a shuffled prompt is a different cache key, so on a real
	// provider that agent re-bills on every run and answers differently each time.
	// Compare the per-agent specs, which is where such a line surfaces.
	specsOf := func(r *loom.RunResult) map[string]string {
		out := map[string]string{}
		for _, rec := range r.StageOutputs["spec-json"] {
			out[rec.String("agent_id")] = rec.String("spec_json")
		}
		return out
	}
	sa, sb := specsOf(designA), specsOf(designB)
	if len(sa) != len(da.Agents) {
		t.Fatalf("spec-json emitted %d specs for %d agents", len(sa), len(da.Agents))
	}
	for id, spec := range sa {
		if sb[id] != spec {
			t.Errorf("agent %q designs differently on a second identical run:\n--- run A ---\n%s\n--- run B ---\n%s",
				id, spec, sb[id])
		}
	}
}

// The offline studio must read the corpus rather than recite an answer: change
// what the corpus contains and the design has to change with it.
func TestDesignFollowsTheCorpus(t *testing.T) {
	reg, _, err := buildRegistry("mock", 0)
	if err != nil {
		t.Fatal(err)
	}
	lu, _ := lineupOf(reg)

	// One space, one function's worth of work: neither spread nor coupling.
	root := t.TempDir()
	day := filepath.Join(root, "solo", "2026-03-02.jsonl")
	if err := os.MkdirAll(filepath.Dir(day), 0o755); err != nil {
		t.Fatal(err)
	}
	line := `{"sender":{"name":"A","type":"HUMAN"},"createTime":"2026-03-02T09:00:00Z","text":"pacing is off again"}`
	if err := os.WriteFile(day, []byte(line+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	files, spaces, err := discover(root, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	opts := []loom.Option{loom.WithRegistry(reg), loom.WithWorkers(2)}
	censusRun, err := loom.Run(ctx, buildCensusPipeline(files, spaces, lu, newScrubber("t"), 2, nil), opts...)
	if err != nil {
		t.Fatal(err)
	}
	obs, systems := collectJobs(censusRun.StageOutputs["day-census"])
	rosterRun, err := loom.Run(ctx, buildRosterPipeline(obs, spaces, systems, lu), opts...)
	if err != nil {
		t.Fatal(err)
	}
	var cen census
	if err := json.Unmarshal([]byte(rosterRun.StageOutputs["score"][0].String("census_json")), &cen); err != nil {
		t.Fatal(err)
	}
	if cen.Topology.Spread != 0 {
		t.Errorf("spread = %.2f over a single space, want 0 — nothing can recur across one place", cen.Topology.Spread)
	}
	if cen.Topology.Recommended == "hybrid" {
		t.Errorf("a one-space corpus was given a hybrid org: %v", cen.Topology.Scores)
	}
}

// ---------------------------------------------------------------------------
// rendering
// ---------------------------------------------------------------------------

func TestRenderDesignAndAgents(t *testing.T) {
	_, _, designRun, cen, decision := offlineRun(t)

	docs := make([]agentDoc, 0, len(decision.Agents))
	byID := map[string]core.Record{}
	for _, r := range designRun.StageOutputs["agent-charter"] {
		byID[r.String("agent_id")] = r
	}
	for _, a := range decision.Agents {
		doc := agentDoc{agentDecl: a}
		if r, ok := byID[a.ID]; ok {
			doc.Charter = pick(r.Data, "memory_schema", "system_prompt", "first_week")
		}
		docs = append(docs, doc)
	}
	bp := blueprint{Generated: "2026-03-10", Source: "corpus", Provider: "mock",
		Census: cen, Roster: decision, Agents: docs}

	md := renderDesign(bp)
	for _, want := range []string{"spread", "coupling", strings.ToUpper(cen.Topology.Recommended)} {
		if !strings.Contains(strings.ToLower(md), strings.ToLower(want)) {
			t.Errorf("DESIGN.md never mentions %q", want)
		}
	}
	for _, a := range decision.Agents {
		if !strings.Contains(md, a.Name) {
			t.Errorf("DESIGN.md omits agent %q", a.Name)
		}
		if body := renderAgent(docs[0], cen); body == "" {
			t.Fatal("renderAgent produced nothing")
		}
	}
	if strings.Contains(md, "Ppc") {
		t.Error(`function-scoped agents should render as "PPC", not "Ppc"`)
	}
}

// A missing model field must render as an omission, never a panic — the whole
// document is built from optional JSON a provider may or may not have returned.
func TestRenderAgentSurvivesEmptyModelOutput(t *testing.T) {
	doc := agentDoc{agentDecl: agentDecl{ID: "x", Name: "X", Partition: partition{Axis: "global", Instances: 1}}}
	if got := renderAgent(doc, census{}); got == "" {
		t.Fatal("renderAgent returned nothing for an agent with no model output")
	}
	doc.Spec = map[string]any{"memory": "not an object", "tools": 42}
	doc.Charter = map[string]any{"memory_schema": []any{"not an object"}}
	if got := renderAgent(doc, census{}); got == "" {
		t.Fatal("renderAgent returned nothing for malformed model output")
	}
}

func TestRenderUIEmbedsAndEscapes(t *testing.T) {
	bp := blueprint{Generated: "2026-03-10", Provider: "mock",
		Roster: rosterDecision{Topology: "hybrid",
			Agents: []agentDecl{{ID: "ppc", Name: "PPC", Mission: "</script><script>alert(1)</script>"}}}}

	page := renderUI(bp)
	if strings.Contains(page, "__BLUEPRINT__") {
		t.Fatal("the blueprint placeholder was never substituted")
	}
	if strings.Contains(page, "</script><script>alert(1)") {
		t.Fatal("model text was injected into the page unescaped")
	}
	if !strings.Contains(page, `id="bp"`) {
		t.Error("the page is missing its embedded blueprint")
	}
	// The page must be self-contained: no CDN, no remote fonts, no fetches.
	for _, forbidden := range []string{"http://", "https://", "//cdn", "fetch("} {
		if strings.Contains(page, forbidden) {
			t.Errorf("index.html reaches out to the network (%q) — it must render from a file:// URL", forbidden)
		}
	}

	// And the embedded JSON must survive a round trip.
	start := strings.Index(page, `id="bp" type="application/json">`)
	if start < 0 {
		t.Fatal("cannot locate the embedded blueprint")
	}
	start += len(`id="bp" type="application/json">`)
	end := strings.Index(page[start:], "</script>")
	if end < 0 {
		t.Fatal("the embedded blueprint is not terminated")
	}
	var back map[string]any
	if err := json.Unmarshal([]byte(page[start:start+end]), &back); err != nil {
		t.Fatalf("the embedded blueprint is not valid JSON: %v", err)
	}
}

// ---------------------------------------------------------------------------
// the whole command, including everything it writes to disk
// ---------------------------------------------------------------------------

func TestRunWritesTheFullBlueprint(t *testing.T) {
	out := t.TempDir()
	cfg := runConfig{messages: "corpus", out: out, provider: "mock", budget: 5, workers: 4, salt: "test"}
	if err := run(cfg); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"DESIGN.md", "blueprint.json", "capabilities.json", "roster.json",
		"index.html", "run-report.txt"} {
		info, err := os.Stat(filepath.Join(out, name))
		if err != nil {
			t.Fatalf("%s was not written: %v", name, err)
		}
		if info.Size() < 200 {
			t.Errorf("%s is %d bytes — suspiciously empty", name, info.Size())
		}
	}

	var bp blueprint
	if err := loadJSON(filepath.Join(out, "blueprint.json"), &bp); err != nil {
		t.Fatal(err)
	}
	if len(bp.Agents) == 0 {
		t.Fatal("the blueprint has no agents")
	}
	for _, a := range bp.Agents {
		if _, err := os.Stat(filepath.Join(out, "agents", a.ID+".md")); err != nil {
			t.Errorf("no charter written for agent %s", a.ID)
		}
	}
	if len(bp.Runs) != 3 {
		t.Errorf("recorded %d runs, want 3", len(bp.Runs))
	}

	// roster.json must round-trip: that is what -roster reloads.
	var frozen rosterDecision
	if err := loadJSON(filepath.Join(out, "roster.json"), &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen.Agents) != len(bp.Agents) {
		t.Errorf("frozen roster has %d agents, blueprint has %d", len(frozen.Agents), len(bp.Agents))
	}

	// Nothing that identifies a person may reach any artifact. The bundled
	// corpus seeds real-looking names, an address and a phone number so this
	// assertion has something to catch.
	for _, forbidden := range []string{"Dana", "Omer", "Maya", "Tomer", "Shaked", "Katz",
		"@example.com", "+972", "555-0134"} {
		if hits := grepDir(t, out, forbidden); len(hits) > 0 {
			t.Errorf("%q leaked into %v", forbidden, hits)
		}
	}
}

func TestRosterReloadSteersTheDesign(t *testing.T) {
	// The edit loop the README describes: run once, hand-edit roster.json, re-run
	// with -roster. Asserting the frozen file unmarshals is not enough — it has to
	// survive recordOf/rosterFrom and reach the charters.
	first := t.TempDir()
	if err := run(runConfig{messages: "corpus", out: first, provider: "mock", budget: 5, workers: 4, salt: "test"}); err != nil {
		t.Fatal(err)
	}
	var frozen rosterDecision
	if err := loadJSON(filepath.Join(first, "roster.json"), &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen.Agents) < 2 {
		t.Fatalf("need at least two agents to test a merge, got %d", len(frozen.Agents))
	}

	// Drop the last agent and rename the first — the two edits a human makes.
	dropped := frozen.Agents[len(frozen.Agents)-1].ID
	frozen.Agents = frozen.Agents[:len(frozen.Agents)-1]
	frozen.Agents[0].Name = "Hand Edited"
	kept := frozen.Agents[0].ID
	blob, err := json.MarshalIndent(frozen, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	edited := filepath.Join(t.TempDir(), "roster.json")
	if err := os.WriteFile(edited, blob, 0o644); err != nil {
		t.Fatal(err)
	}

	second := t.TempDir()
	cfg := runConfig{messages: "corpus", out: second, provider: "mock", budget: 5, workers: 4,
		salt: "test", rosterPath: edited}
	if err := run(cfg); err != nil {
		t.Fatal(err)
	}

	var bp blueprint
	if err := loadJSON(filepath.Join(second, "blueprint.json"), &bp); err != nil {
		t.Fatal(err)
	}
	if len(bp.Agents) != len(frozen.Agents) {
		t.Fatalf("designed %d agents, the edited roster asked for %d", len(bp.Agents), len(frozen.Agents))
	}
	for _, a := range bp.Agents {
		if a.ID == dropped {
			t.Errorf("agent %q was removed from the roster but still got designed", dropped)
		}
		if a.ID == kept && a.Name != "Hand Edited" {
			t.Errorf("hand-edited name was overwritten: %q", a.Name)
		}
		// Mission and capabilities have to survive the round-trip too, or the
		// second run quietly designs thinner agents than the first.
		if strings.TrimSpace(a.Mission) == "" {
			t.Errorf("agent %s lost its mission through the reload", a.ID)
		}
		if len(a.Remembers) == 0 {
			t.Errorf("agent %s lost its memory ranking through the reload", a.ID)
		}
	}
	if _, err := os.Stat(filepath.Join(second, "agents", dropped+".md")); err == nil {
		t.Errorf("a charter was written for the dropped agent %q", dropped)
	}
}

func TestRunRejectsUnknownProvider(t *testing.T) {
	err := run(runConfig{messages: "corpus", out: t.TempDir(), provider: "gemini"})
	if err == nil || !strings.Contains(err.Error(), "unknown provider") {
		t.Fatalf("err = %v, want an unknown-provider error", err)
	}
}

func grepDir(t *testing.T, root, needle string) []string {
	t.Helper()
	var hits []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(body), needle) {
			hits = append(hits, strings.TrimPrefix(path, root+string(filepath.Separator)))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hits
}

// sampleObs is a hand-built corpus with a known shape: three spaces, shared PPC
// and partner work, one local family each. It is the smallest input that
// exercises both spread and coupling.
func sampleObs() []jobObs {
	var obs []jobObs
	for _, space := range []string{"renters", "travel", "pet-insurance"} {
		for _, day := range []string{"2026-03-02", "2026-03-03"} {
			for _, j := range sharedJobs {
				obs = append(obs, jobObs{Space: space, Date: day, Label: j.Label, Function: j.Function,
					Entities: j.Entities, Memory: j.Memory, HandoffTo: j.Handoff})
			}
			for _, j := range localJobs[space] {
				obs = append(obs, jobObs{Space: space, Date: day, Label: j.Label, Function: j.Function,
					Entities: j.Entities, Memory: j.Memory, HandoffTo: j.Handoff})
			}
		}
	}
	return obs
}
