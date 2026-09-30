package loom_test

import (
	"context"
	"strings"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/providers/typesafe"
	"github.com/zionrubin/loom/providers/typesafe/typesafetest"
	"github.com/zionrubin/loom/security"
)

// jevJudge is sure about everything except a ticket that says "ambiguous",
// where it splits the queue evenly — the case a confidence gate exists for.
func jevJudge(state string, qs model.Questions) (map[string]model.Answer, error) {
	queue := map[string]float64{"billing": 9, "bug": 0.5}
	if strings.Contains(state, "crash") {
		queue = map[string]float64{"billing": 0.5, "bug": 9}
	}
	if strings.Contains(state, "ambiguous") {
		queue = map[string]float64{"billing": 1, "bug": 1}
	}
	urgent := 0.1
	if strings.Contains(state, "URGENT") {
		urgent = 0.95
	}
	return map[string]model.Answer{
		"queue":  typesafetest.Choice(queue),
		"urgent": typesafetest.Noul(urgent),
	}, nil
}

func decisionQuestions() model.Questions {
	return model.Questions{
		"queue": model.Choice("Which team owns this ticket?", map[string]string{
			"billing": "charges, refunds, invoices",
			"bug":     "something in the product is broken",
		}),
		"urgent": model.Noul("Does this need a human within the hour?"),
	}
}

func decisionTickets() []core.Record {
	return []core.Record{
		core.NewRecord("t1", map[string]any{"subject": "URGENT charged twice, refund please"}),
		core.NewRecord("t2", map[string]any{"subject": "app crash on login"}),
		core.NewRecord("t3", map[string]any{"subject": "ambiguous: something is off with my account"}),
	}
}

// llmAnswers is a text model answering the rendered questions.
func llmAnswers(choice string) func(model.Request) (string, error) {
	return func(req model.Request) (string, error) {
		return `{"queue": {"choice": "` + choice + `", "confidence": 0.8}, "urgent": {"noul": 0.2}}`, nil
	}
}

type decideRig struct {
	reg *model.Registry
	jev *typesafetest.Server
	llm *model.Mock
}

func newDecideRig(t *testing.T) *decideRig {
	t.Helper()
	jev := &typesafetest.Server{APIKey: "sk-ts", Judge: jevJudge}
	url, stop := jev.Start()
	t.Cleanup(stop)
	reg := model.NewRegistry()
	if err := typesafe.Register(reg, typesafe.New(typesafe.WithBaseURL(url)), typesafe.Jev, model.Limits{}); err != nil {
		t.Fatal(err)
	}
	llm, err := model.RegisterMock(reg, "llm", model.TierDeep, model.WithHandler(llmAnswers("billing")))
	if err != nil {
		t.Fatal(err)
	}
	return &decideRig{reg: reg, jev: jev, llm: llm}
}

func (r *decideRig) opts(extra ...loom.Option) []loom.Option {
	return append([]loom.Option{
		loom.WithRegistry(r.reg), loom.WithRetry(quickRetry()),
		loom.WithSecrets(map[security.SecretRef]string{typesafe.DefaultSecretRef: "sk-ts"}),
	}, extra...)
}

func decidePipeline(ladder ...string) *pipeline.Pipeline {
	p := pipeline.New("decide")
	p.FromRecords("tickets", decisionTickets()).
		Decide("triage", pipeline.DecideSpec{
			Binding:       model.Binding{Model: typesafe.Jev, Escalation: ladder},
			State:         "Ticket: {{.subject}}",
			Questions:     decisionQuestions(),
			MinConfidence: 0.6,
		}).
		Filter("billing-only", func(r core.Record) (bool, error) {
			return r.String("queue") == "billing", nil
		}, pipeline.WithVersion("v1"))
	return p
}

func TestDecideEscalatesOnlyWhatJevIsUnsureOf(t *testing.T) {
	rig := newDecideRig(t)
	dir := t.TempDir()
	p := decidePipeline("llm")

	res, err := loom.Run(context.Background(), p, rig.opts(loom.WithStateDir(dir))...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	byID := map[string]core.Record{}
	for _, r := range res.StageOutputs["triage"] {
		byID[r.ID] = r
	}
	if len(byID) != 3 {
		t.Fatalf("triage produced %d records, want 3", len(byID))
	}

	for id, want := range map[string]struct {
		queue      string
		calibrated bool
		rung       float64
	}{
		"t1": {"billing", true, 0},
		"t2": {"bug", true, 0},
		"t3": {"billing", false, 1}, // Jev split 50/50, so the LLM answered
	} {
		rec := byID[id]
		dec, ok := model.ReadDecision(rec.Data["decision"])
		if !ok {
			t.Fatalf("%s: no decision field: %+v", id, rec.Data)
		}
		if rec.String("queue") != want.queue || dec.Calibrated != want.calibrated {
			t.Errorf("%s: queue %q calibrated %v, want %q %v", id, rec.String("queue"), dec.Calibrated, want.queue, want.calibrated)
		}
		full := rec.Data["decision"].(map[string]any)
		if full["rung"] != want.rung {
			t.Errorf("%s: answered on rung %v, want %v", id, full["rung"], want.rung)
		}
	}
	if u, _ := byID["t1"].Data["urgent"].(float64); u < 0.9 {
		t.Errorf("t1 urgent = %v, want the probability Jev returned", byID["t1"].Data["urgent"])
	}
	if got := len(res.StageOutputs["billing-only"]); got != 2 {
		t.Errorf("billing-only kept %d, want 2 (t1, and t3 via the LLM)", got)
	}
	// t3 was asked of Jev and escalated, so Jev saw all three; the LLM saw one.
	if rig.jev.Calls() != 3 || rig.llm.Calls() != 1 {
		t.Errorf("jev calls %d, llm calls %d; want 3 and 1", rig.jev.Calls(), rig.llm.Calls())
	}

	// Cache = checkpoint: the rerun asks nobody anything.
	jevBefore, llmBefore := rig.jev.Calls(), rig.llm.Calls()
	again, err := loom.Run(context.Background(), p, rig.opts(loom.WithStateDir(dir))...)
	if err != nil {
		t.Fatalf("rerun: %v", err)
	}
	if rig.jev.Calls() != jevBefore || rig.llm.Calls() != llmBefore {
		t.Errorf("rerun made %d Jev and %d LLM calls, want none",
			rig.jev.Calls()-jevBefore, rig.llm.Calls()-llmBefore)
	}
	if len(again.StageOutputs["billing-only"]) != 2 {
		t.Errorf("replayed run kept %d records, want 2", len(again.StageOutputs["billing-only"]))
	}
}

func TestDecideHoldsGenerativeRungsToTheSchema(t *testing.T) {
	rig := newDecideRig(t)
	// A rung that invents an option: its reply is a semantic failure and the
	// record climbs past it rather than carrying "sales" downstream.
	inventive, err := model.RegisterMock(rig.reg, "inventive", "", model.WithHandler(llmAnswers("sales")))
	if err != nil {
		t.Fatal(err)
	}
	res, err := loom.Run(context.Background(), decidePipeline("inventive", "llm"), rig.opts()...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, r := range res.StageOutputs["triage"] {
		if q := r.String("queue"); q != "billing" && q != "bug" {
			t.Errorf("%s carries undeclared queue %q", r.ID, q)
		}
	}
	if inventive.Calls() != 1 || rig.llm.Calls() != 1 {
		t.Errorf("inventive %d calls, llm %d; want the ambiguous ticket to pass through both once",
			inventive.Calls(), rig.llm.Calls())
	}
}

func TestDecideWithoutFallbackFlagsLowConfidence(t *testing.T) {
	rig := newDecideRig(t)
	res, err := loom.Run(context.Background(), decidePipeline(), rig.opts()...)
	if err != nil {
		t.Fatalf("a Jev-only stage must keep low-confidence answers, not fail them: %v", err)
	}
	flagged := 0
	for _, r := range res.StageOutputs["triage"] {
		full := r.Data["decision"].(map[string]any)
		if full["low_confidence"] == true {
			flagged++
			if r.ID != "t3" {
				t.Errorf("%s flagged low confidence", r.ID)
			}
		}
	}
	if flagged != 1 {
		t.Errorf("flagged %d records, want exactly the ambiguous one", flagged)
	}
}

func TestDecisionModelRefusedOnTextStage(t *testing.T) {
	rig := newDecideRig(t)
	p := pipeline.New("misbound")
	p.FromRecords("tickets", decisionTickets()).
		Infer("summarize", pipeline.InferSpec{
			Binding: model.Binding{Model: typesafe.Jev},
			Prompt:  "Summarize: {{.subject}}",
		})
	_, err := loom.Run(context.Background(), p, rig.opts()...)
	if err == nil || !strings.Contains(err.Error(), "typed questions only") {
		t.Fatalf("binding a text stage to Jev should fail compilation, got %v", err)
	}
	if rig.jev.Calls() != 0 {
		t.Errorf("a refused plan made %d calls", rig.jev.Calls())
	}
}

func TestExplainKnowsWhatADecisionAdds(t *testing.T) {
	rig := newDecideRig(t)
	// Unsampled, the fields are known but the answers are not: a filter on
	// one is a guess, and the projection says so rather than bounding by it.
	blind, err := loom.Explain(decidePipeline("llm"), rig.opts()...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !blind.Partial() {
		t.Errorf("a filter on unsampled answers must mark the projection partial:\n%s", blind)
	}

	proj, err := loom.Explain(decidePipeline("llm"), append(rig.opts(),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1}),
		loom.WithStageSample("triage", map[string]any{"queue": "billing", "urgent": 1.0}))...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if proj.Partial() {
		t.Errorf("with every answer sampled nothing below the decision should be estimated:\n%s", proj)
	}

	if !proj.FitsBudget() {
		t.Errorf("projection over budget:\n%s", proj)
	}
	var sawLadder bool
	for _, w := range proj.Warnings {
		if strings.Contains(w, "escalated to llm") {
			sawLadder = true
		}
		if strings.Contains(w, "parses its output as JSON") {
			t.Errorf("decision stage warned like a ParseJSON stage: %s", w)
		}
	}
	if !sawLadder {
		t.Errorf("projection should price the confidence gate's worst case; warnings: %v", proj.Warnings)
	}
	if rig.jev.Calls() != 0 || rig.llm.Calls() != 0 {
		t.Error("Explain must make no calls")
	}
}
