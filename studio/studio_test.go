package studio

import (
	"context"
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

// --- fixtures --------------------------------------------------------------

// digestDoc is the document the design was drawn from: a folder of daily chat
// files, a digest per day, one line each, a branch per vertical, a rollup per
// branch, and a second pass folding the rollups into one page.
func digestDoc(root string) *Doc {
	d := &Doc{
		Name: "vertical-digest", CapUSD: 15, Workers: 8, KeepGoing: true,
		Steps: []Step{
			{ID: "load-days", Kind: KindSource, Title: "Load days", Source: &SourceSpec{
				From: "folder", Root: root, Match: "*.jsonl",
				Line:   "{{clock .createTime}} {{.sender}}: {{.text}}",
				Scrub:  []string{"emails", "speakers"},
				Fields: map[string]string{"group": "vertical", "name": "date", "text": "messages"},
			}},
			{ID: "daily-digest", Kind: KindInfer, Title: "Daily digest", From: "load-days", Infer: &InferSpec{
				Tier: "cheapest", Escalate: []string{"mock-deep"},
				System:    "You are a business analyst.",
				Prompt:    "One day of chat from {{.vertical}} on {{.date}}:\n\n{{.messages}}",
				MaxTokens: 400, Workers: 8,
				Answer: []Answer{
					{Name: "summary", Note: "3-5 sentences", Required: true},
					{Name: "topics", Kind: "list", Note: "up to 5 labels"},
				},
			}},
			{ID: "digest-line", Kind: KindDerive, Title: "One line each", From: "daily-digest", Field: &FieldSpec{
				Name: "digest_line", Template: "[{{.date}}] {{.summary}} | topics: {{join .topics}}",
			}},
			{ID: "only-payments", Kind: KindFilter, From: "digest-line",
				Keep: &Cond{Field: "vertical", Op: "is", Value: "payments"}},
			{ID: "only-retail", Kind: KindFilter, From: "digest-line",
				Keep: &Cond{Field: "vertical", Op: "is", Value: "retail"}},
			{ID: "rollup-payments", Kind: KindReduce, From: "only-payments", Reduce: &ReduceSpec{
				Model: "mock-mid", Cover: []string{"Status", "Themes"}, Words: 400,
				FanIn: 12, MaxTokens: 900, ItemField: "digest_line",
			}},
			{ID: "rollup-retail", Kind: KindReduce, From: "only-retail", Reduce: &ReduceSpec{
				Model: "mock-mid", Cover: []string{"Status", "Themes"}, Words: 400,
				FanIn: 12, MaxTokens: 900, ItemField: "digest_line",
			}},
			{ID: "business-overview", Kind: KindReduce, Title: "Business overview",
				Merge: []string{"rollup-payments", "rollup-retail"}, Reduce: &ReduceSpec{
					Model: "mock-deep", Cover: []string{"Headline status", "Status by vertical"},
					MaxTokens: 2000,
				}},
		},
	}
	d.Layout()
	return d
}

// chatRoot writes a small folder archive: two verticals, two days each.
func chatRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	days := map[string][]string{
		"payments/2026-07-30": {
			`{"createTime":"2026-07-30T09:12:00Z","sender":"Dana","text":"chargebacks up again, ping dana@example.com"}`,
			`{"createTime":"2026-07-30T09:14:00Z","sender":"Roi","text":"opening a ticket with the PSP"}`,
			`{"createTime":"2026-07-30T09:20:00Z","sender":"Dana","text":"agreed"}`,
		},
		"payments/2026-07-31": {
			`{"createTime":"2026-07-31T10:00:00Z","sender":"Roi","text":"PSP answered, fix lands tomorrow"}`,
		},
		"retail/2026-07-30": {
			`{"createTime":"2026-07-30T08:00:00Z","sender":"Maya","text":"store 12 inventory drift"}`,
			`{"createTime":"2026-07-30T08:30:00Z","sender":"Noa","text":"recount scheduled"}`,
		},
		"retail/2026-07-31": {
			`{"createTime":"2026-07-31T08:00:00Z","sender":"Maya","text":"recount clean"}`,
		},
	}
	for name, lines := range days {
		path := filepath.Join(root, name+".jsonl")
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// testRegistry registers three priced mock models, one per tier. The prices
// are made up but the ratios are not: a run's shape is only visible in a
// projection when the tiers cost different amounts.
func testRegistry(t *testing.T) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	for _, m := range []struct {
		id      string
		tier    model.Tier
		in, out float64
	}{
		{"mock-fast", model.TierFast, 0.10, 0.40},
		{"mock-mid", model.TierBalanced, 0.60, 2.40},
		{"mock-deep", model.TierDeep, 3.00, 12.00},
	} {
		err := reg.Register(model.Info{
			ID: m.id, Provider: model.NewMock(m.id), Tier: m.tier,
			Pricing: model.Pricing{InputPerMTok: m.in, OutputPerMTok: m.out},
			Limits:  model.Limits{RequestsPerMinute: 200},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

// --- document --------------------------------------------------------------

func TestValidateRejectsBrokenDocuments(t *testing.T) {
	root := chatRoot(t)
	cases := []struct {
		name string
		edit func(*Doc)
		want string
	}{
		{"unknown upstream", func(d *Doc) { d.Find("daily-digest").From = "nope" }, "not a step"},
		{"self reference", func(d *Doc) { d.Find("daily-digest").From = "daily-digest" }, "reads from itself"},
		{"duplicate id", func(d *Doc) { d.Steps = append(d.Steps, d.Steps[1]) }, "duplicate step id"},
		{"bad id", func(d *Doc) { d.Steps[1].ID = "Daily Digest" }, "invalid id"},
		{"source with upstream", func(d *Doc) { d.Steps[0].From = "daily-digest" }, "sources have no upstream"},
		{"unknown operator", func(d *Doc) { d.Find("only-retail").Keep.Op = "kinda" }, "unknown operator"},
		{"operator needs a value", func(d *Doc) { d.Find("only-retail").Keep.Value = "" }, "needs a value"},
		{"merge of a non-fold", func(d *Doc) {
			d.Find("business-overview").Merge = []string{"digest-line"}
		}, "a merge reads folds"},
		{"merge and upstream", func(d *Doc) { d.Find("business-overview").From = "rollup-retail" }, "second pass"},
		{"field mismatch across the seam", func(d *Doc) {
			d.Find("business-overview").Reduce.ItemField = "item"
		}, "writes its result to"},
		{"no prompt", func(d *Doc) { d.Find("daily-digest").Infer.Prompt = " " }, "no prompt"},
		{"tier and model", func(d *Doc) { d.Find("daily-digest").Infer.Model = "mock-deep" }, "not both"},
		{"unknown redaction", func(d *Doc) { d.Steps[0].Source.Scrub = []string{"faces"} }, "unknown redaction"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := digestDoc(root)
			tc.edit(d)
			err := d.Validate()
			if err == nil {
				t.Fatalf("expected an error mentioning %q, got none", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateAcceptsTheDesignDocument(t *testing.T) {
	if err := digestDoc(chatRoot(t)).Validate(); err != nil {
		t.Fatalf("the reference document should validate: %v", err)
	}
}

func TestOrderIsTopologicalAndCyclesAreCaught(t *testing.T) {
	d := digestDoc(chatRoot(t))
	order, err := d.Order()
	if err != nil {
		t.Fatal(err)
	}
	at := map[string]int{}
	for i, s := range order {
		at[s.ID] = i
	}
	if len(order) != len(d.Steps) {
		t.Fatalf("order has %d steps, document has %d", len(order), len(d.Steps))
	}
	for _, s := range d.Steps {
		for _, up := range s.upstreams() {
			if at[up] >= at[s.ID] {
				t.Fatalf("%s runs at %d, after its upstream %s at %d", s.ID, at[s.ID], up, at[up])
			}
		}
	}

	d.Find("load-days").Kind = KindFilter
	d.Find("load-days").From = "rollup-retail"
	d.Find("load-days").Keep = &Cond{Field: "x", Op: "exists"}
	if _, err := d.Order(); err == nil {
		t.Fatal("expected a cycle to be reported")
	}
}

func TestLayoutPlacesEveryStepAndKeepsHandPlacedOnes(t *testing.T) {
	d := digestDoc(chatRoot(t))
	d.Find("daily-digest").X, d.Find("daily-digest").Y = 999, 777
	for i := range d.Steps {
		if d.Steps[i].ID != "daily-digest" {
			d.Steps[i].X, d.Steps[i].Y = 0, 0
		}
	}
	d.Layout()
	if got := d.Find("daily-digest"); got.X != 999 || got.Y != 777 {
		t.Fatalf("hand-placed card moved to %d,%d", got.X, got.Y)
	}
	depth := d.Depth()
	if depth["business-overview"] <= depth["rollup-retail"] {
		t.Fatalf("the second pass should sit right of the folds it merges: %v", depth)
	}
	for _, s := range d.Steps {
		if s.ID == "load-days" {
			continue // column 0, row 0 is legitimately (originX, originY)
		}
		if s.X == 0 && s.Y == 0 {
			t.Fatalf("step %q was never placed", s.ID)
		}
	}
}

func TestApplyDoesNotTouchTheOriginal(t *testing.T) {
	d := digestDoc(chatRoot(t))
	before, _ := json.Marshal(d)

	next, err := d.Apply([]Edit{
		{Op: SetCap, Value: 8},
		{Op: UpdateStep, ID: "daily-digest", Step: &Step{Infer: &InferSpec{
			Tier: "balanced", Prompt: "shorter", Answer: d.Find("daily-digest").Infer.Answer,
		}}},
		{Op: AddStep, Step: &Step{ID: "flag", Kind: KindInfer, From: "daily-digest",
			Infer: &InferSpec{Tier: "cheapest", Prompt: "flag risk in {{.summary}}"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(d)
	if string(before) != string(after) {
		t.Fatal("Apply modified the document it was called on")
	}
	if next.CapUSD != 8 {
		t.Fatalf("cap is %v, want 8", next.CapUSD)
	}
	if got := next.Find("daily-digest").Infer.Tier; got != "balanced" {
		t.Fatalf("tier is %q, want balanced", got)
	}
	if got := next.Find("daily-digest").Title; got != "Daily digest" {
		t.Fatalf("update overwrote the title with %q; an update carries only what it changes", got)
	}
	if next.Find("flag") == nil {
		t.Fatal("added step is missing")
	}
	if next.Find("flag").X == 0 && next.Find("flag").Y == 0 {
		t.Fatal("added step was never laid out")
	}
}

func TestRemoveReparentsChildren(t *testing.T) {
	d := digestDoc(chatRoot(t))
	next, err := d.Apply([]Edit{{Op: RemoveStep, ID: "digest-line"}})
	if err != nil {
		t.Fatal(err)
	}
	if next.Find("digest-line") != nil {
		t.Fatal("step still present")
	}
	if got := next.Find("only-payments").From; got != "daily-digest" {
		t.Fatalf("child was re-parented onto %q, want daily-digest", got)
	}

	if _, err := d.Apply([]Edit{{Op: RemoveStep, ID: "load-days"}}); err == nil {
		t.Fatal("removing a source with children should be refused")
	}

	next, err = d.Apply([]Edit{{Op: RemoveStep, ID: "rollup-retail"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := next.Find("business-overview").Merge; len(got) != 1 || got[0] != "rollup-payments" {
		t.Fatalf("merge list is %v; a removed fold should leave it", got)
	}
}

func TestSaveAndLoadRoundTrip(t *testing.T) {
	d := digestDoc(chatRoot(t))
	path := filepath.Join(t.TempDir(), "nested", "doc.json")
	if err := d.Save(path); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(d)
	b, _ := json.Marshal(got)
	if string(a) != string(b) {
		t.Fatalf("round trip changed the document:\n%s\n%s", a, b)
	}
}

// --- sources ---------------------------------------------------------------

func TestFolderSourceReadsRenamesAndRedacts(t *testing.T) {
	root := chatRoot(t)
	spec := digestDoc(root).Steps[0].Source
	recs, err := LoadRecords(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 4 {
		t.Fatalf("read %d records, want 4", len(recs))
	}
	first := recs[0]
	if first.ID != "payments/2026-07-30" {
		t.Fatalf("first record is %q; records should be ordered", first.ID)
	}
	if got := first.String("vertical"); got != "payments" {
		t.Fatalf("group field is %q, want the renamed \"payments\"", got)
	}
	if _, ok := first.Data["group"]; ok {
		t.Fatal("canonical field survived the rename")
	}
	if got := first.Data["count"]; got != 3 {
		t.Fatalf("count is %v, want 3", got)
	}
	text := first.String("messages")
	if strings.Contains(text, "dana@example.com") {
		t.Fatalf("email was not redacted:\n%s", text)
	}
	if !strings.Contains(text, "<email>") {
		t.Fatalf("redaction marker missing:\n%s", text)
	}
	for _, want := range []string{"09:12 S1:", "09:14 S2:", "09:20 S1:"} {
		if !strings.Contains(text, want) {
			t.Fatalf("speaker labels are not stable within the record (%q missing):\n%s", want, text)
		}
	}
	if strings.Contains(text, "Dana") || strings.Contains(text, "Roi") {
		t.Fatalf("a real name survived pseudonymization:\n%s", text)
	}
	// Stable within a record, deliberately not across records.
	if !strings.Contains(recs[2].String("messages"), "S1:") {
		t.Fatalf("second record was not labelled from scratch:\n%s", recs[2].String("messages"))
	}
}

func TestFolderSourceClampsAndLimits(t *testing.T) {
	root := chatRoot(t)
	spec := digestDoc(root).Steps[0].Source
	spec.Since, spec.Until = "2026-07-31", "2026-07-31"
	recs, err := LoadRecords(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("date range kept %d records, want 2", len(recs))
	}
	spec.Since, spec.Until, spec.Limit = "", "", 3
	recs, _ = LoadRecords(context.Background(), spec)
	if len(recs) != 3 {
		t.Fatalf("limit kept %d records, want 3", len(recs))
	}
}

func TestTableSource(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "tickets.csv")
	if err := os.WriteFile(csvPath, []byte("id,subject\n1,late delivery\n2,double charge\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	recs, err := LoadRecords(context.Background(), &SourceSpec{From: "table", Path: csvPath})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].ID != "1" || recs[1].String("subject") != "double charge" {
		t.Fatalf("csv read wrong: %+v", recs)
	}

	jsonlPath := filepath.Join(dir, "tickets.jsonl")
	if err := os.WriteFile(jsonlPath, []byte(`{"id":"a","subject":"x"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	recs, err = LoadRecords(context.Background(), &SourceSpec{From: "table", Path: jsonlPath})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].ID != "a" {
		t.Fatalf("jsonl read wrong: %+v", recs)
	}
}

func TestCondMatch(t *testing.T) {
	r := core.NewRecord("r", map[string]any{
		"vertical": "payments", "count": 214, "topics": []any{"a"}, "empty": []any{}, "blank": " ",
	})
	cases := []struct {
		c    Cond
		want bool
	}{
		{Cond{"vertical", "is", "payments"}, true},
		{Cond{"vertical", "is-not", "retail"}, true},
		{Cond{"vertical", "contains", "pay"}, true},
		{Cond{"vertical", "not-contains", "pay"}, false},
		{Cond{"topics", "non-empty", ""}, true},
		{Cond{"empty", "non-empty", ""}, false},
		{Cond{"empty", "empty", ""}, true},
		{Cond{"blank", "empty", ""}, true},
		{Cond{"vertical", "exists", ""}, true},
		{Cond{"nope", "exists", ""}, false},
		{Cond{"nope", "missing", ""}, true},
		{Cond{"count", "gt", "200"}, true},
		{Cond{"count", "gt", "1000"}, false},
		{Cond{"count", "lte", "214"}, true},
		// Not numbers on either side: lexical, which is what comparing dates
		// has to mean.
		{Cond{"vertical", "gt", "logistics"}, true},
	}
	for _, tc := range cases {
		got, err := tc.c.Match(r)
		if err != nil {
			t.Fatalf("%v: %v", tc.c, err)
		}
		if got != tc.want {
			t.Fatalf("%s = %v, want %v", tc.c, got, tc.want)
		}
	}
}

// --- build -----------------------------------------------------------------

func TestBuildProducesTheRightPipelineShape(t *testing.T) {
	d := digestDoc(chatRoot(t))
	p, err := d.Build()
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "vertical-digest" {
		t.Fatalf("pipeline name is %q", p.Name)
	}
	stages := map[string]string{}
	for _, s := range p.Stages() {
		stages[s.ID] = string(s.Kind)
	}
	want := map[string]string{
		"load-days": "source", "daily-digest": "infer", "digest-line": "map",
		"only-payments": "filter", "only-retail": "filter",
		"rollup-payments": "reduce_ai", "rollup-retail": "reduce_ai",
	}
	for id, kind := range want {
		if stages[id] != kind {
			t.Fatalf("stage %q is %q, want %q", id, stages[id], kind)
		}
	}
	if _, ok := stages["business-overview"]; ok {
		t.Fatal("the merging fold belongs to the second pass, not the first")
	}

	// The answer shape has to reach the spec as all three of its consequences.
	for _, s := range p.Stages() {
		if s.ID != "daily-digest" {
			continue
		}
		if !s.Infer.ParseJSON {
			t.Fatal("declaring an answer shape should parse the response as JSON")
		}
		if !strings.Contains(s.Infer.Prompt, `"summary": "<3-5 sentences>"`) {
			t.Fatalf("the JSON instruction is missing from the prompt:\n%s", s.Infer.Prompt)
		}
		if s.Infer.Validate == nil {
			t.Fatal("a required answer field should produce a validator")
		}
		if err := s.Infer.Validate(core.NewRecord("x", map[string]any{"summary": " "})); err == nil {
			t.Fatal("an empty required field should fail validation")
		}
		if err := s.Infer.Validate(core.NewRecord("x", map[string]any{"summary": "ok"})); err != nil {
			t.Fatalf("a filled required field should pass: %v", err)
		}
	}

	second, err := d.BuildSecond(map[string][]core.Record{
		"rollup-payments": {core.NewRecord("a", map[string]any{"output": "payments report"})},
		"rollup-retail":   {core.NewRecord("b", map[string]any{"output": "retail report"})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Name != "business-overview" {
		t.Fatalf("second pass is named %q", second.Name)
	}
	if n := len(second.Stages()); n != 2 {
		t.Fatalf("second pass has %d stages, want a source and a fold", n)
	}
	if _, err := d.BuildSecond(map[string][]core.Record{}); err == nil {
		t.Fatal("a second pass over nothing should be an error, not an empty run")
	}
}

func TestBuiltStagesRunTheirGoFunctions(t *testing.T) {
	d := digestDoc(chatRoot(t))
	p, err := d.Build()
	if err != nil {
		t.Fatal(err)
	}
	rec := core.NewRecord("payments/2026-07-30", map[string]any{
		"vertical": "payments", "date": "2026-07-30",
		"summary": "chargebacks", "topics": []any{"psp", "chargebacks"},
	})
	for _, s := range p.Stages() {
		switch s.ID {
		case "digest-line":
			out, err := s.MapFn(rec)
			if err != nil {
				t.Fatal(err)
			}
			want := "[2026-07-30] chargebacks | topics: psp, chargebacks"
			if got := out.String("digest_line"); got != want {
				t.Fatalf("derived field is %q, want %q", got, want)
			}
			if s.Opts.Version == "" {
				t.Fatal("a derived field must carry a version, or its results cannot be cached")
			}
		case "only-payments":
			keep, err := s.FilterFn(rec)
			if err != nil || !keep {
				t.Fatalf("filter dropped a matching record (%v, %v)", keep, err)
			}
		case "only-retail":
			keep, _ := s.FilterFn(rec)
			if keep {
				t.Fatal("filter kept a record from another branch")
			}
		}
	}
}

func TestDeriveVersionTracksTheTemplate(t *testing.T) {
	root := chatRoot(t)
	a, err := digestDoc(root).Build()
	if err != nil {
		t.Fatal(err)
	}
	d2 := digestDoc(root)
	d2.Find("digest-line").Field.Template = "[{{.date}}] {{.summary}}"
	b, err := d2.Build()
	if err != nil {
		t.Fatal(err)
	}
	va, vb := stageVersion(a, "digest-line"), stageVersion(b, "digest-line")
	if va == "" || va == vb {
		t.Fatalf("editing the template left the version at %q; cached records would survive the edit", va)
	}
}

// stageVersion is the content version the planner fingerprints a Go-function
// stage with.
func stageVersion(p *pipeline.Pipeline, id string) string {
	for _, s := range p.Stages() {
		if s.ID == id {
			return s.Opts.Version
		}
	}
	return ""
}

func TestWriteStepWritesFiles(t *testing.T) {
	dir := t.TempDir()
	d := &Doc{Name: "w", Steps: []Step{
		{ID: "src", Kind: KindSource, Source: &SourceSpec{From: "records", Records: []core.Record{
			core.NewRecord("r1", map[string]any{"name": "alpha", "output": "body one"}),
		}}},
		{ID: "save", Kind: KindWrite, From: "src", Write: &WriteSpec{
			Dir: dir, Name: "{{.name}}.md", Body: "# {{.name}}\n\n{{.output}}",
		}},
	}}
	p, err := d.Build()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range p.Stages() {
		if s.ID != "save" {
			continue
		}
		if !s.Opts.NoCache {
			t.Fatal("a write must not be cacheable: a replayed write writes nothing")
		}
		out, err := s.MapFn(core.NewRecord("r1", map[string]any{"name": "alpha", "output": "body one"}))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(dir, "alpha.md"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "# alpha\n\nbody one" {
			t.Fatalf("file contents %q", got)
		}
		if out.String("path") == "" {
			t.Fatal("the written path should come back on the record")
		}
	}
}

// --- pricing ---------------------------------------------------------------

func TestPriceCountsRealRecords(t *testing.T) {
	root := chatRoot(t)
	d := digestDoc(root)
	est := Price(d, WithRegistry(testRegistry(t)))
	if est.Error != "" {
		t.Fatalf("price failed: %s", est.Error)
	}
	if est.Records != 4 {
		t.Fatalf("priced against %d records, want the 4 on disk", est.Records)
	}
	digest := est.Step("daily-digest")
	if digest.Calls != 4 {
		t.Fatalf("daily digest projects %d calls, want one per day", digest.Calls)
	}
	if digest.Model != "mock-fast" {
		t.Fatalf("digest priced on %q, want the cheapest tier", digest.Model)
	}
	// Branches are executed, not extrapolated: two payments days, two retail.
	if got := est.Step("rollup-payments").Records; got != 2 {
		t.Fatalf("payments branch carries %d records, want 2", got)
	}
	if est.ExpectedUSD <= 0 || est.CeilingUSD < est.ExpectedUSD {
		t.Fatalf("expected %v / ceiling %v", est.ExpectedUSD, est.CeilingUSD)
	}
	if !est.FitsCap {
		t.Fatalf("a $15 cap should cover %v", est.CeilingUSD)
	}
	// The second pass is priced too, and says so.
	ov := est.Step("business-overview")
	if ov.Calls == 0 {
		t.Fatal("the second pass was not priced")
	}
	joined := strings.Join(est.Warnings, " ")
	if !strings.Contains(joined, "second pass") {
		t.Fatalf("the second pass's assumption should be stated; warnings: %v", est.Warnings)
	}
	p := est.Priciest()
	if p == nil {
		t.Fatal("no step carries any cost")
	}
	for _, s := range est.Steps {
		if s.ExpectedUSD > p.ExpectedUSD {
			t.Fatalf("priciest step is %q at %v, but %q costs %v", p.ID, p.ExpectedUSD, s.ID, s.ExpectedUSD)
		}
	}
	if sum := est.Step("daily-digest").Share + est.Step("business-overview").Share; sum <= 0 || sum > 1.0001 {
		t.Fatalf("shares do not read as fractions of the run: %v", sum)
	}
	if len(est.Paid()) != 4 {
		t.Fatalf("%d paid steps, want 4", len(est.Paid()))
	}
	if line := est.Step("digest-line"); !line.Free {
		t.Fatal("a derived field costs nothing and should say so")
	}
}

func TestPriceReportsWhyItCannotPrice(t *testing.T) {
	d := digestDoc(chatRoot(t))
	if est := Price(d); est.Error == "" || !strings.Contains(est.Error, "registry") {
		t.Fatalf("without a registry the estimate should explain itself, got %q", est.Error)
	}
	d.Find("daily-digest").Infer.Model = ""
	d.Find("daily-digest").Infer.Tier = "cheapest"
	d.Find("rollup-retail").Reduce.Model = "not-registered"
	est := Price(d, WithRegistry(testRegistry(t)))
	if est.Error == "" || !strings.Contains(est.Error, "not-registered") {
		t.Fatalf("an unregistered model should surface as an error, got %q", est.Error)
	}
	if est.ExpectedUSD != 0 {
		t.Fatal("a pipeline that cannot be compiled has no price, not a zero one")
	}
}

func TestPriceRespondsToTheKnobsTheInspectorOffers(t *testing.T) {
	root := chatRoot(t)
	base := Price(digestDoc(root), WithRegistry(testRegistry(t)))

	cheaper := digestDoc(root)
	cheaper.Find("daily-digest").Infer.MaxTokens = 100
	got := Price(cheaper, WithRegistry(testRegistry(t)))
	if got.CeilingUSD >= base.CeilingUSD {
		t.Fatalf("a smaller token cap should lower the ceiling: %v vs %v", got.CeilingUSD, base.CeilingUSD)
	}

	capped := digestDoc(root)
	capped.CapUSD = 0.0000001
	if Price(capped, WithRegistry(testRegistry(t))).FitsCap {
		t.Fatal("a cap below the ceiling should report that it does not fit")
	}
}

// --- export ----------------------------------------------------------------

func TestGoExportParsesAndCarriesTheDocument(t *testing.T) {
	d := digestDoc(chatRoot(t))
	src, err := d.Go()
	if err != nil {
		t.Fatalf("export failed: %v\n%s", err, src)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "export.go", src, parser.AllErrors); err != nil {
		t.Fatalf("generated Go does not parse: %v\n%s", err, src)
	}
	for _, want := range []string{
		`p := pipeline.New("vertical-digest")`,
		`loadDays := p.FromFunc("load-days"`,
		`studio.LoadRecords(ctx, &studio.SourceSpec{`,
		`dailyDigest := loadDays.Infer("daily-digest", pipeline.InferSpec{`,
		`Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"mock-deep"}}`,
		`ParseJSON: true`,
		`return r.String("vertical") == "payments", nil`,
		`pipeline.WithParallelism(8)`,
		`func buildSecond(out map[string][]core.Record) (*pipeline.Pipeline, error)`,
		`businessOverviewIn = append(businessOverviewIn, out["rollup-payments"]...)`,
		`loom.WithRunBudget(core.Budget{MaxCostUSD: 15})`,
		`loom.WithContinueOnError()`,
	} {
		if !strings.Contains(src, want) {
			t.Fatalf("generated program is missing %q:\n%s", want, src)
		}
	}
	// A step nothing reads from is a statement, not an assignment: an unused
	// variable would not compile.
	if strings.Contains(src, "rollupRetail := ") {
		t.Fatalf("leaf step was assigned to an unused variable:\n%s", src)
	}
}

func TestGoExportCoversEveryStepKind(t *testing.T) {
	dir := t.TempDir()
	d := &Doc{Name: "all-kinds", CapUSD: 2, Workers: 2, Steps: []Step{
		{ID: "src", Kind: KindSource, Source: &SourceSpec{From: "records", Records: []core.Record{
			core.NewRecord("r1", map[string]any{"draft": "hello", "score": 3}),
		}}},
		{ID: "polish", Kind: KindLoop, From: "src", Loop: &LoopSpec{
			Step: InferSpec{Tier: "balanced", Prompt: "improve {{.draft}}{{if .Inbox}}\n{{range .Inbox}}- {{.}}\n{{end}}{{end}}",
				Answer: []Answer{{Name: "draft", Required: true}, {Name: "score"}, {Name: "critique"}}},
			Until: Cond{Field: "score", Op: "gte", Value: "8"}, Rounds: 4, CapUSD: 1,
		}},
		{ID: "good", Kind: KindFilter, From: "polish", Keep: &Cond{Field: "score", Op: "gte", Value: "8"}},
		{ID: "headline", Kind: KindDerive, From: "good", Field: &FieldSpec{
			Name: "headline", Template: "{{.draft}} ({{.score}})"}},
		{ID: "save", Kind: KindWrite, From: "headline", Write: &WriteSpec{
			Dir: dir, Name: "{{.id}}.md", Body: "{{.headline}}"}},
	}}
	src, err := d.Go()
	if err != nil {
		t.Fatalf("export failed: %v\n%s", err, src)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "export.go", src, parser.AllErrors); err != nil {
		t.Fatalf("generated Go does not parse: %v\n%s", err, src)
	}
	for _, want := range []string{
		"algo.NewRefine(algo.RefineConfig{",
		"MaxRounds: 4",
		"core.Budget{MaxCostUSD: 1}",
		`(studio.Cond{Field: "score", Op: "gte", Value: "8"}).Match(r)`,
		"template.Must(template.New(",
		"studio.TemplateFuncs()",
		"pipeline.WithNoCache()",
		"mustRecords(",
	} {
		if !strings.Contains(src, want) {
			t.Fatalf("generated program is missing %q:\n%s", want, src)
		}
	}
}
