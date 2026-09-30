package model_test

import (
	"math"
	"strings"
	"testing"

	"github.com/zionrubin/loom/model"
)

func questions() model.Questions {
	return model.Questions{
		"queue": model.Choice("Which team owns this?", map[string]string{
			"billing": "charges, refunds, invoices",
			"bug":     "something is broken",
		}),
		"urgent": model.Noul("Does this need a human within the hour?"),
		"anger":  model.Score("How frustrated is the customer?", "calm", "annoyed", "furious"),
	}
}

func TestQuestionsValidate(t *testing.T) {
	if err := questions().Validate(); err != nil {
		t.Fatalf("valid questions rejected: %v", err)
	}
	levels := make([]string, model.MaxScoreLevels+1)
	for i := range levels {
		levels[i] = "l"
	}
	bad := map[string]model.Questions{
		"empty":        {},
		"bad id":       {"has space": model.Noul("x")},
		"no text":      {"q": model.Noul(" ")},
		"one option":   {"q": model.Choice("x", map[string]string{"a": ""})},
		"one level":    {"q": model.Score("x", "only")},
		"eleven level": {"q": model.Score("x", levels...)},
		"unknown type": {"q": {Type: "essay", Instructions: "x"}},
	}
	for name, qs := range bad {
		if err := qs.Validate(); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
}

func TestCheckHoldsAnswersToSchema(t *testing.T) {
	qs := questions()
	good := map[string]model.Answer{
		"queue":  {Type: model.QuestionChoice, Choice: "billing", Confidence: 0.9},
		"urgent": {Type: model.QuestionNoul, Noul: 0.8},
		"anger":  {Type: model.QuestionScore, Score: 1.4, Confidence: 0.7},
	}
	if err := qs.Check(good); err != nil {
		t.Fatalf("good answers rejected: %v", err)
	}
	cases := map[string]func(map[string]model.Answer){
		"undeclared option": func(a map[string]model.Answer) {
			a["queue"] = model.Answer{Type: model.QuestionChoice, Choice: "sales"}
		},
		"missing answer":  func(a map[string]model.Answer) { delete(a, "urgent") },
		"wrong type":      func(a map[string]model.Answer) { a["urgent"] = model.Answer{Type: model.QuestionChoice} },
		"off the rubric":  func(a map[string]model.Answer) { a["anger"] = model.Answer{Type: model.QuestionScore, Score: 3} },
		"probability > 1": func(a map[string]model.Answer) { a["urgent"] = model.Answer{Type: model.QuestionNoul, Noul: 1.2} },
	}
	for name, mutate := range cases {
		a := map[string]model.Answer{}
		for k, v := range good {
			a[k] = v
		}
		mutate(a)
		if err := qs.Check(a); err == nil {
			t.Errorf("%s: expected Check to fail", name)
		}
	}
}

func TestCertaintyAndWeakest(t *testing.T) {
	d := model.Decision{Answers: map[string]model.Answer{
		"queue":  {Type: model.QuestionChoice, Choice: "bug", Confidence: 0.92},
		"urgent": {Type: model.QuestionNoul, Noul: 0.55}, // a coin flip, nearly
		"anger":  {Type: model.QuestionScore, Score: 0.2, Confidence: 0.8},
	}}
	if got := d.Answers["urgent"].Certainty(); math.Abs(got-0.1) > 1e-9 {
		t.Errorf("noul 0.55 certainty = %v, want 0.1", got)
	}
	if got := (model.Answer{Type: model.QuestionNoul, Noul: 0.05}).Certainty(); math.Abs(got-0.9) > 1e-9 {
		t.Errorf("noul 0.05 certainty = %v, want 0.9", got)
	}
	if d.Weakest() != "urgent" || math.Abs(d.Confidence()-0.1) > 1e-9 {
		t.Errorf("weakest = %q at %v, want urgent at 0.1", d.Weakest(), d.Confidence())
	}
}

func TestGenerativeRoundTrip(t *testing.T) {
	qs := questions()
	req := model.Generative(model.Request{System: "Be terse.", Prompt: "I was charged twice!!", Questions: qs})
	if req.Questions != nil {
		t.Error("a generative request must not carry questions to a text provider")
	}
	if !strings.HasPrefix(req.System, "Be terse.") || !strings.Contains(req.Prompt, "I was charged twice!!") {
		t.Errorf("system or state lost: %+v", req)
	}
	for _, want := range []string{"queue (choice)", "billing: charges", "2 = furious", `"urgent": {"noul": 0.0}`} {
		if !strings.Contains(req.Prompt, want) {
			t.Errorf("rendered prompt missing %q:\n%s", want, req.Prompt)
		}
	}

	reply := "Sure.\n```json\n" + `{"queue": {"choice": "billing", "confidence": 0.9},
		"urgent": {"noul": 0.7}, "anger": {"score": 2, "confidence": 0.6}}` + "\n```"
	got, err := model.ParseDecision(reply, qs)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got["queue"].Choice != "billing" || got["urgent"].Noul != 0.7 || got["anger"].Score != 2 {
		t.Errorf("parsed answers wrong: %+v", got)
	}
	if got["anger"].Legend["2"] != "furious" {
		t.Errorf("score legend not filled: %+v", got["anger"].Legend)
	}

	if _, err := model.ParseDecision(`{"queue": {"choice": "sales"}, "urgent": {"noul": 1}, "anger": {"score": 0}}`, qs); err == nil {
		t.Error("an option nobody declared must be refused")
	}
	if _, err := model.ParseDecision(`{"queue": {"choice": "bug"}}`, qs); err == nil {
		t.Error("a missing answer must be refused")
	}
}

func TestReadDecisionFromRecordField(t *testing.T) {
	field := map[string]any{
		"model": "jev-latest", "calibrated": true,
		"answers": map[string]any{
			"urgent": map[string]any{"type": "noul", "noul": 0.9},
		},
	}
	d, ok := model.ReadDecision(field)
	if !ok || d.Model != "jev-latest" || !d.Calibrated || d.Answers["urgent"].Noul != 0.9 {
		t.Fatalf("ReadDecision = %+v, %v", d, ok)
	}
	if _, ok := model.ReadDecision("not a decision"); ok {
		t.Error("a string is not a decision")
	}
}

func TestCapabilitiesDefaultToText(t *testing.T) {
	if c := model.CapabilitiesOf(model.NewMock("m")); !c.Text || c.Decisions {
		t.Errorf("mock capabilities = %+v, want text only", c)
	}
}
