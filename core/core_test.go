package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestClassOf(t *testing.T) {
	base := errors.New("boom")
	cases := []struct {
		err  error
		want FailureClass
	}{
		{Transient(base), FailTransient},
		{Semantic(base), FailSemantic},
		{Permanent(base), FailPermanent},
		{BudgetExceeded(base), FailBudget},
		{fmt.Errorf("wrap: %w", Semantic(base)), FailSemantic},
		{context.DeadlineExceeded, FailTransient},
		{base, FailPermanent}, // unclassified fails fast
	}
	for _, c := range cases {
		if got := ClassOf(c.err); got != c.want {
			t.Errorf("ClassOf(%v) = %v, want %v", c.err, got, c.want)
		}
	}
	if !errors.Is(Transient(base), base) {
		t.Error("TaskError should unwrap to the base error")
	}
}

func TestUsageAdd(t *testing.T) {
	var u Usage
	u.Add(Usage{InputTokens: 10, OutputTokens: 5, Requests: 1, CostUSD: 0.01})
	u.Add(Usage{InputTokens: 2, OutputTokens: 3, Requests: 1, CostUSD: 0.02})
	if u.InputTokens != 12 || u.OutputTokens != 8 || u.Requests != 2 {
		t.Errorf("unexpected usage: %+v", u)
	}
	if u.TotalTokens() != 20 {
		t.Errorf("TotalTokens = %d, want 20", u.TotalTokens())
	}
}

func TestRecordClone(t *testing.T) {
	r := NewRecord("a", map[string]any{"x": 1})
	c := r.Clone()
	c.Data["x"] = 2
	c.Data["y"] = 3
	if r.Data["x"] != 1 {
		t.Error("clone mutated the original")
	}
	if _, ok := r.Data["y"]; ok {
		t.Error("clone additions leaked into the original")
	}
	if r.String("x") != "1" {
		t.Errorf("String(x) = %q", r.String("x"))
	}
}
