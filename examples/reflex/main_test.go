package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/providers/typesafe"
	"github.com/zionrubin/loom/studio"
)

// offline returns the offline setup whatever keys the environment holds, so
// the tests never spend money.
func offline(t *testing.T) *setup {
	t.Helper()
	for _, k := range []string{"TYPESAFE_API_KEY", "ANTHROPIC_API_KEY"} {
		if v, ok := os.LookupEnv(k); ok {
			os.Unsetenv(k)
			t.Cleanup(func() { os.Setenv(k, v) })
		}
	}
	s, err := models(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.stop)
	return s
}

func run(t *testing.T, s *setup, stateDir string) (*loom.RunResult, *ledger) {
	t.Helper()
	calls := &ledger{}
	opts := append(s.opts(), loom.WithRunBudget(core.Budget{MaxCostUSD: 1}),
		loom.WithEventHandler(calls.observe))
	if stateDir != "" {
		opts = append(opts, loom.WithStateDir(stateDir))
	}
	p := build(inbox(), model.Binding{Model: typesafe.Jev, Escalation: []string{s.fallback}}, s.brief)
	res, err := loom.Run(context.Background(), p, opts...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Failures) > 0 {
		t.Fatalf("run dead-lettered %d records: %v", len(res.Failures), res.Failures[0].Err)
	}
	return res, calls
}

func ids(recs []core.Record) string {
	var out []string
	for _, r := range recs {
		out = append(out, r.ID)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func TestGuardQuarantinesTheAttacks(t *testing.T) {
	res, _ := run(t, offline(t), "")
	if got := ids(res.StageOutputs["quarantine"]); got != "m03,m07,m14" {
		t.Errorf("quarantined %s, want the injection and data-grab attempts m03,m07,m14", got)
	}
	if got := len(res.StageOutputs["triage"]); got != 15 {
		t.Errorf("triaged %d, want the 15 clean messages", got)
	}
}

func TestJevAnswersAndTheLLMReviewsOnlyTheUnsure(t *testing.T) {
	s := offline(t)
	res, _ := run(t, s, "")

	var escalated []core.Record
	for _, r := range res.StageOutputs["route"] {
		dec, ok := model.ReadDecision(r.Data["decision"])
		if !ok {
			t.Fatalf("%s: no decision", r.ID)
		}
		// The schema holds whichever rung answered.
		if _, ok := queues[r.String("queue")]; !ok {
			t.Errorf("%s: queue %q is not a declared option", r.ID, r.String("queue"))
		}
		if f, _ := r.Data["frustration"].(float64); f < 0 || f > 3 {
			t.Errorf("%s: frustration %v off the rubric", r.ID, f)
		}
		if r.String("lane") == "" {
			t.Errorf("%s: not routed", r.ID)
		}
		if !dec.Calibrated {
			escalated = append(escalated, r)
			continue
		}
		if c := dec.Confidence(); c < gate {
			t.Errorf("%s: accepted from Jev at confidence %.2f, below the %.2f gate", r.ID, c, gate)
		}
	}
	// The tied queue, the message with no signal Jev knows, the hedged churn.
	if got := ids(escalated); got != "m08,m11,m12" {
		t.Errorf("escalated %s, want m08,m11,m12", got)
	}
	if s.llm.Calls() != 4 { // three reviews and the brief
		t.Errorf("LLM calls = %d, want 4", s.llm.Calls())
	}
	byID := map[string]core.Record{}
	for _, r := range res.StageOutputs["route"] {
		byID[r.ID] = r
	}
	for id, lane := range map[string]string{"m02": "page-oncall", "m17": "page-oncall", "m01": "refunds",
		"m06": "retention", "m12": "retention", "m11": "bug"} {
		if got := byID[id].String("lane"); got != lane {
			t.Errorf("%s routed to %q, want %q", id, got, lane)
		}
	}
	if brief := res.StageOutputs["shift-brief"]; len(brief) != 1 || brief[0].String("output") == "" {
		t.Errorf("shift brief = %v, want one non-empty record", brief)
	}
	if n := len(res.StageOutputs["hot"]); n != 4 {
		t.Errorf("hot kept %d, want 4", n)
	}
}

func TestSecondRunAsksNobodyAnything(t *testing.T) {
	s := offline(t)
	dir := t.TempDir()
	run(t, s, dir)
	jev, llm := s.jev.Calls(), s.llm.Calls()
	if jev == 0 || llm == 0 {
		t.Fatalf("first run made %d Jev and %d LLM calls", jev, llm)
	}
	run(t, s, dir)
	if s.jev.Calls() != jev || s.llm.Calls() != llm {
		t.Errorf("rerun made %d Jev and %d LLM calls, want none", s.jev.Calls()-jev, s.llm.Calls()-llm)
	}
}

func TestProjectionBoundsTheRunAndPricesTheAlternative(t *testing.T) {
	s := offline(t)
	samples := []loom.Option{
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1}),
		loom.WithStageSample("guard", map[string]any{"injection": 0.0, "data_request": 0.0}),
		loom.WithStageSample("triage", map[string]any{
			"queue": "bug", "urgent": 1.0, "refund": 1.0, "churn_risk": 1.0, "frustration": 3.0,
		}),
	}
	jevFirst, err := loom.Explain(build(inbox(), model.Binding{Model: typesafe.Jev, Escalation: []string{s.fallback}}, s.brief),
		append(s.opts(), samples...)...)
	if err != nil {
		t.Fatal(err)
	}
	if jevFirst.Partial() || !jevFirst.FitsBudget() {
		t.Errorf("projection partial=%v fits=%v:\n%s", jevFirst.Partial(), jevFirst.FitsBudget(), jevFirst)
	}
	llmOnly, err := loom.Explain(build(inbox(), model.Binding{Model: s.fallback}, s.brief), append(s.opts(), samples...)...)
	if err != nil {
		t.Fatal(err)
	}
	if j, l := stageCost(jevFirst, "triage"), stageCost(llmOnly, "triage"); j*20 > l {
		t.Errorf("Jev-first triage projects $%.5f against $%.5f on the LLM alone; want at least 20x cheaper", j, l)
	}
	if s.jev.Calls() != 0 || s.llm.Calls() != 0 {
		t.Error("Explain made calls")
	}
}

func TestBoardServesTheRun(t *testing.T) {
	s := offline(t)
	res, calls := run(t, s, "")
	b := newBoard(res, calls, 0.05, s, time.Second)
	srv := httptest.NewServer(b.handler())
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/api/board")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got board
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 18 || got.Triaged != 15 || got.Escalated != 3 {
		t.Errorf("board has %d messages, %d triaged, %d escalated", len(got.Messages), got.Triaged, got.Escalated)
	}
	for _, m := range got.Messages {
		if m.Triage == nil || m.Triage["calibrated"] == true {
			continue
		}
		// An escalated message shows what Jev said before the gate sent it up.
		if m.First == nil || m.First.Confidence >= gate {
			t.Errorf("%s: first try %+v, want Jev's answer below the gate", m.ID, m.First)
		}
	}
	if len(got.Lanes) == 0 || got.Brief == "" || len(got.Models) < 2 {
		t.Errorf("board incomplete: lanes %v, brief %q, models %d", got.Lanes, got.Brief, len(got.Models))
	}

	page, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page.Body.Close()
	if !strings.HasPrefix(page.Header.Get("Content-Type"), "text/html") {
		t.Errorf("board page content type %q", page.Header.Get("Content-Type"))
	}
}

func TestStudioDocumentCompilesAndPrices(t *testing.T) {
	s := offline(t)
	doc := studioDoc(s)
	if err := doc.Validate(); err != nil {
		t.Fatalf("document invalid: %v", err)
	}
	est := studio.Price(doc, studio.WithRegistry(s.reg))
	if est.Error != "" {
		t.Fatalf("price: %s", est.Error)
	}
	var triage *studio.StepPrice
	for i := range est.Steps {
		if est.Steps[i].ID == "triage" {
			triage = &est.Steps[i]
		}
	}
	if triage == nil || triage.Free || triage.ExpectedUSD <= 0 || triage.Records != 18 {
		t.Errorf("triage priced as %+v, want a paid step over all 18 records", triage)
	}
	src, err := doc.Go()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	src = strings.Join(strings.Fields(src), " ") // gofmt aligns; compare tokens
	for _, want := range []string{`.Decide("triage", pipeline.DecideSpec{`, `model.Choice(`, `MinConfidence: 0.65`, `model.Score("How frustrated`} {
		if !strings.Contains(src, want) {
			t.Errorf("exported Go missing %q", want)
		}
	}
	if s.jev.Calls() != 0 {
		t.Error("pricing made calls")
	}
}
