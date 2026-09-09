package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/policy"
)

func governing() policy.Policy {
	return policy.Policy{
		Name:       "self-hosted-pii",
		Egress:     policy.Rule{Allow: []string{"*.internal"}},
		Sandbox:    policy.Rule{Allow: []string{"inline"}},
		Data:       map[string]policy.Rule{"pii": {Allow: []string{"local/*"}}},
		MaxCostUSD: 5,
	}
}

// TestTheRefusalCostsNothing is the example's central claim, measured rather
// than asserted: the uncleared round makes zero model calls.
func TestTheRefusalCostsNothing(t *testing.T) {
	reg, calls := registry()

	_, err := loom.Run(context.Background(),
		supportDesk(model.Binding{Model: "vendor/frontier-1"}),
		loom.WithRegistry(reg), loom.WithPolicy(governing()))
	if !denied(err) {
		t.Fatalf("want a policy refusal, got %v", err)
	}
	if n := calls(); n != 0 {
		t.Fatalf("a refused run made %d model calls", n)
	}

	var d *policy.Denied
	errors.As(err, &d)
	if !strings.Contains(d.Report.String(), "classify") {
		t.Fatalf("the refusal should name the stage:\n%s", d.Report)
	}
}

// TestTheClearedRoundRuns: the same policy against a conforming pipeline
// leaves the work unchanged.
func TestTheClearedRoundRuns(t *testing.T) {
	reg, calls := registry()

	res, err := loom.Run(context.Background(),
		supportDesk(model.Binding{Model: "local/llama-3.1-70b"}),
		loom.WithRegistry(reg), loom.WithPolicy(governing()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.StageOutputs["classify"]) != len(tickets()) {
		t.Fatalf("classified %d of %d tickets",
			len(res.StageOutputs["classify"]), len(tickets()))
	}
	if calls() == 0 {
		t.Fatal("the cleared round should actually call the model")
	}
	if !res.Policy.Admitted() || res.Policy.Policy != "self-hosted-pii" {
		t.Fatalf("the result should carry the verdict: %+v", res.Policy)
	}

	// Both halves of the trail: what authorized the run, and every call that
	// carried classified data.
	var admitted, access int
	for _, e := range res.Audit {
		switch {
		case e.Action == "policy.admit" && e.Allowed:
			admitted++
		case e.Action == "data.access":
			access++
			if !strings.Contains(e.Reason, "pii") {
				t.Errorf("access entry does not name the class: %+v", e)
			}
			if !strings.HasPrefix(e.Subject, "local/") {
				t.Errorf("classified data reached %q", e.Subject)
			}
		}
	}
	if admitted != 1 {
		t.Errorf("want one admission entry, got %d", admitted)
	}
	if access != calls() {
		t.Errorf("want an access entry per call: %d entries, %d calls", access, calls())
	}
}

// TestTheLadderIsJudgedWhole: the commented-out escalation in main.go is a
// real refusal, not a rhetorical one.
func TestTheLadderIsJudgedWhole(t *testing.T) {
	reg, calls := registry()

	_, err := loom.Run(context.Background(), supportDesk(model.Binding{
		Model:      "local/llama-3.1-70b",
		Escalation: []string{"vendor/frontier-1"},
	}), loom.WithRegistry(reg), loom.WithPolicy(governing()))

	if !denied(err) {
		t.Fatalf("a cleared starting rung must not clear the ladder: %v", err)
	}
	if n := calls(); n != 0 {
		t.Fatalf("refused before any call, got %d", n)
	}
}

// TestUngovernedIsUnchanged: the framework without a policy behaves as it
// always did, classified source or not.
func TestUngovernedIsUnchanged(t *testing.T) {
	reg, calls := registry()

	res, err := loom.Run(context.Background(),
		supportDesk(model.Binding{Model: "vendor/frontier-1"}),
		loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.Output) != 1 || calls() == 0 {
		t.Fatalf("outputs=%d calls=%d", len(res.Output), calls())
	}
	if res.Policy.Checked != 0 {
		t.Errorf("no policy means nothing was looked at, got %+v", res.Policy)
	}
}
