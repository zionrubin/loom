package loom_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/policy"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/task"
)

// governedRegistry offers two models: one the deployment hosts itself, one it
// does not. Every test below turns on which of them a stage may reach.
func governedRegistry(t *testing.T) (*model.Registry, *model.Mock, *model.Mock) {
	t.Helper()
	reg := model.NewRegistry()
	local, err := model.RegisterMock(reg, "local/llama-3.1-70b", model.TierFast,
		model.WithHandler(classifyMock))
	if err != nil {
		t.Fatal(err)
	}
	hosted, err := model.RegisterMock(reg, "vendor/gpt-5", model.TierDeep,
		model.WithHandler(classifyMock))
	if err != nil {
		t.Fatal(err)
	}
	return reg, local, hosted
}

// classifiedPipeline labels its source and never mentions the label again:
// propagation is what carries it to the stage that calls a model.
func classifiedPipeline(binding model.Binding, classes ...string) *pipeline.Pipeline {
	p := pipeline.New("support")
	src := p.FromRecords("tickets", tickets(),
		pipeline.WithDataClass(classes...))
	src.
		Map("tag", func(r core.Record) (core.Record, error) {
			r.Data["source"] = "test"
			return r, nil
		}, pipeline.WithVersion("v1")).
		Infer("classify", pipeline.InferSpec{
			Binding:   binding,
			System:    "You are a support ticket classifier.",
			Prompt:    "Classify this ticket: {{.subject}}",
			ParseJSON: true,
		})
	return p
}

func denial(t *testing.T, err error) policy.Report {
	t.Helper()
	if err == nil {
		t.Fatal("want a policy refusal, got success")
	}
	var d *policy.Denied
	if !errors.As(err, &d) {
		t.Fatalf("want *policy.Denied, got %T: %v", err, err)
	}
	return d.Report
}

// TestARefusedRunMakesNoCalls is the property the whole feature rests on: a
// plan the deployment does not permit costs nothing to refuse, because the
// refusal happens before a scheduler exists.
func TestARefusedRunMakesNoCalls(t *testing.T) {
	reg, local, hosted := governedRegistry(t)

	pol := policy.Policy{
		Name: "self-hosted-only",
		Data: map[string]policy.Rule{"pii": {Allow: []string{"local/*"}}},
	}
	p := classifiedPipeline(model.Binding{Model: "vendor/gpt-5"}, "pii")

	_, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithPolicy(pol))

	rep := denial(t, err)
	if local.Calls() != 0 || hosted.Calls() != 0 {
		t.Fatalf("a refused run made calls: local=%d hosted=%d", local.Calls(), hosted.Calls())
	}
	if len(rep.Violations) != 1 || rep.Violations[0].Axis != policy.AxisData {
		t.Fatalf("want one data-clearance violation, got %v", rep.Violations)
	}
	if v := rep.Violations[0]; v.Stage != "classify" ||
		!strings.Contains(v.Subject, "vendor/gpt-5") {
		t.Fatalf("violation should name the stage and the model: %+v", v)
	}
	if !strings.Contains(err.Error(), "self-hosted-only") {
		t.Errorf("the refusal should name the policy: %s", err)
	}
}

// TestAClassReachesAStageThatNeverDeclaredIt covers the propagation itself:
// only the source is labelled, and the label is what refuses the model call
// two stages downstream.
func TestAClassReachesAStageThatNeverDeclaredIt(t *testing.T) {
	reg, _, _ := governedRegistry(t)
	pol := policy.Policy{Data: map[string]policy.Rule{"pii": {Allow: []string{"local/*"}}}}

	// Same pipeline, same binding, label removed: it runs.
	clean := classifiedPipeline(model.Binding{Model: "vendor/gpt-5"})
	if _, err := loom.Run(context.Background(), clean,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()), loom.WithPolicy(pol)); err != nil {
		t.Fatalf("an unclassified pipeline is unaffected: %v", err)
	}

	// Label the source and nothing else changes — but now it is refused.
	tainted := classifiedPipeline(model.Binding{Model: "vendor/gpt-5"}, "pii")
	_, err := loom.Run(context.Background(), tainted,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()), loom.WithPolicy(pol))
	rep := denial(t, err)
	if rep.Violations[0].Stage != "classify" {
		t.Fatalf("the class should reach the inferring stage, got %+v", rep.Violations)
	}
}

// TestAnAdmittedRunIsUnchanged: containment on the ordinary path is the
// identity, so turning a policy on does not quietly change what a conforming
// pipeline does.
func TestAnAdmittedRunIsUnchanged(t *testing.T) {
	reg, local, _ := governedRegistry(t)
	pol := policy.Policy{
		Name:   "self-hosted-only",
		Models: policy.Rule{Allow: []string{"local/*"}},
		Data:   map[string]policy.Rule{"pii": {Allow: []string{"local/*"}}},
	}
	p := classifiedPipeline(model.Binding{Model: "local/llama-3.1-70b"}, "pii")

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()), loom.WithPolicy(pol))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.StageOutputs["classify"]) != 4 || local.Calls() != 4 {
		t.Fatalf("outputs=%d calls=%d, want 4/4",
			len(res.StageOutputs["classify"]), local.Calls())
	}
	if !res.Policy.Admitted() || res.Policy.Policy != "self-hosted-only" {
		t.Fatalf("the result should carry the verdict: %+v", res.Policy)
	}
	if res.Policy.Checked == 0 {
		t.Fatal("the verdict should say how many stages it covered")
	}
}

// TestTheEscalationLadderIsJudgedWhole: a stage that starts on a cleared model
// and escalates to an uncleared one leaks on the second call, and finding that
// out then is finding it out too late.
func TestTheEscalationLadderIsJudgedWhole(t *testing.T) {
	reg, _, _ := governedRegistry(t)
	pol := policy.Policy{Data: map[string]policy.Rule{"pii": {Allow: []string{"local/*"}}}}

	p := classifiedPipeline(model.Binding{
		Model:      "local/llama-3.1-70b",
		Escalation: []string{"vendor/gpt-5"},
	}, "pii")

	_, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()), loom.WithPolicy(pol))
	rep := denial(t, err)
	if len(rep.Violations) != 1 || !strings.Contains(rep.Violations[0].Subject, "vendor/gpt-5") {
		t.Fatalf("the rung, not just the starting model, must be judged: %v", rep.Violations)
	}
}

// TestContainmentSurvivesRunLevelEgress: a host opened for the whole run is
// judged, and stripped from the envelopes even so.
func TestRunLevelEgressIsJudgedToo(t *testing.T) {
	reg, _, _ := governedRegistry(t)
	pol := policy.Policy{
		Name:   "prod",
		Egress: policy.Rule{Allow: []string{"*.internal"}},
	}
	p := classifiedPipeline(model.Binding{Model: "local/llama-3.1-70b"})

	_, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithEgress("db.internal", "exfil.example.com"),
		loom.WithPolicy(pol))

	rep := denial(t, err)
	if len(rep.Violations) != 1 || rep.Violations[0].Subject != "exfil.example.com" {
		t.Fatalf("want the one bad host, got %v", rep.Violations)
	}
	if rep.Violations[0].Stage != "" {
		t.Errorf("a run-level violation belongs to no stage: %+v", rep.Violations[0])
	}
}

// TestEveryViolationArrivesAtOnce: a plan refused for four reasons should be
// fixed once rather than four times, and the run-level ones travel with the
// per-stage ones.
func TestEveryViolationArrivesAtOnce(t *testing.T) {
	reg, _, _ := governedRegistry(t)
	pol := policy.Policy{
		Name:       "prod",
		Models:     policy.Rule{Allow: []string{"local/*"}},
		Egress:     policy.Rule{Allow: []string{"*.internal"}},
		MaxCostUSD: 1,
	}
	p := classifiedPipeline(model.Binding{Model: "vendor/gpt-5"})

	_, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithEgress("exfil.example.com"),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 500}),
		loom.WithPolicy(pol))

	rep := denial(t, err)
	axes := map[policy.Axis]int{}
	for _, v := range rep.Violations {
		axes[v.Axis]++
	}
	for _, want := range []policy.Axis{policy.AxisModel, policy.AxisEgress, policy.AxisBudget} {
		if axes[want] == 0 {
			t.Errorf("no %s violation in %v", want, rep.Violations)
		}
	}
}

// TestAPolicyCeilingIsInherited: a run naming no budget of its own gets the
// policy's, which is what makes a policy a spending control rather than only
// a spending check. The model is priced so the governor has something to stop
// against — a free model can be capped at any figure and never reach it.
func TestAPolicyCeilingIsInherited(t *testing.T) {
	reg := model.NewRegistry()
	mock := model.NewMock("priced/model", model.WithHandler(classifyMock))
	if err := reg.Register(model.Info{
		ID: "priced/model", Provider: mock, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 1000, OutputPerMTok: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	p := classifiedPipeline(model.Binding{Model: "priced/model"})

	// Uncapped, the run finishes and spends something.
	free, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("baseline run: %v", err)
	}
	if free.Spent.CostUSD <= 0 {
		t.Fatalf("baseline should have cost something, got %v", free.Spent.CostUSD)
	}

	// Under a ceiling below that, and with no run budget of its own, the run
	// stops against the policy's.
	pol := policy.Policy{Name: "cheap", MaxCostUSD: free.Spent.CostUSD / 4}
	capped, err := loom.Run(context.Background(), classifiedPipeline(
		model.Binding{Model: "priced/model"}),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithWorkers(1), loom.WithPolicy(pol))
	if err == nil && len(capped.Failures) == 0 {
		t.Fatalf("a run under an inherited ceiling should stop against it; spent %v of %v",
			capped.Spent.CostUSD, pol.MaxCostUSD)
	}

	// And the projection prices the run against the ceiling it will actually
	// be held to, not the one the caller forgot to name.
	proj, err := loom.Explain(classifiedPipeline(model.Binding{Model: "priced/model"}),
		loom.WithRegistry(reg), loom.WithPolicy(pol))
	if err != nil {
		t.Fatal(err)
	}
	if proj.Budget.MaxCostUSD != pol.MaxCostUSD {
		t.Fatalf("projection budget = %v, want the policy ceiling %v",
			proj.Budget.MaxCostUSD, pol.MaxCostUSD)
	}
}

// TestExplainAnswersMayThisRun: the projection refuses the same plan a run
// would, so a pipeline is never priced and then found inadmissible.
func TestExplainAnswersMayThisRun(t *testing.T) {
	reg, _, _ := governedRegistry(t)
	pol := policy.Policy{
		Name: "self-hosted-only",
		Data: map[string]policy.Rule{"pii": {Allow: []string{"local/*"}}},
	}

	_, err := loom.Explain(classifiedPipeline(model.Binding{Model: "vendor/gpt-5"}, "pii"),
		loom.WithRegistry(reg), loom.WithPolicy(pol))
	rep := denial(t, err)
	if rep.Violations[0].Axis != policy.AxisData {
		t.Fatalf("Explain should refuse for the same reason a run does: %v", rep.Violations)
	}

	// And it refuses for everything at once, exactly as a run does: the
	// run-level budget joins the plan's own violations rather than waiting for
	// a second attempt.
	_, err = loom.Explain(classifiedPipeline(model.Binding{Model: "vendor/gpt-5"}, "pii"),
		loom.WithRegistry(reg), loom.WithPolicy(policy.Policy{
			Name: "capped", MaxCostUSD: 1,
			Data: map[string]policy.Rule{"pii": {Allow: []string{"local/*"}}},
		}),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 500}))
	both := denial(t, err)
	axes := map[policy.Axis]bool{}
	for _, v := range both.Violations {
		axes[v.Axis] = true
	}
	if !axes[policy.AxisData] || !axes[policy.AxisBudget] {
		t.Fatalf("want both a stage and a run-level violation, got %v", both.Violations)
	}

	proj, err := loom.Explain(classifiedPipeline(model.Binding{Model: "local/llama-3.1-70b"}, "pii"),
		loom.WithRegistry(reg), loom.WithPolicy(pol))
	if err != nil {
		t.Fatalf("a conforming plan should project: %v", err)
	}
	if !proj.Policy.Admitted() || proj.Policy.Checked == 0 {
		t.Fatalf("the projection should carry the verdict: %+v", proj.Policy)
	}
	if !strings.Contains(proj.String(), "self-hosted-only") {
		t.Errorf("the projection should print the verdict:\n%s", proj)
	}
}

// TestTheEnvelopeCarriesTheClassAndTheAuditRecordsIt: a worker in another
// process is handed a task and nothing else, so the classification has to
// travel with the work — and the affirmative audit line is what a deployment
// shows when asked to prove where classified data went.
func TestTheEnvelopeCarriesTheClassAndTheAuditRecordsIt(t *testing.T) {
	reg, _, _ := governedRegistry(t)
	p := classifiedPipeline(model.Binding{Model: "local/llama-3.1-70b"}, "pii")

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("a classified run without a policy is unchanged: %v", err)
	}

	var access int
	for _, e := range res.Audit {
		if e.Action == "data.access" {
			access++
			if !e.Allowed || !strings.Contains(e.Reason, "pii") ||
				e.Subject != "local/llama-3.1-70b" {
				t.Fatalf("unexpected access entry: %+v", e)
			}
		}
	}
	if access != 4 {
		t.Fatalf("want one data.access entry per classified call, got %d", access)
	}
}

// TestAdmissionIsAudited: the line that says a run was authorized is the one a
// deployment needs, not only the absence of a line saying it was not.
func TestAdmissionIsAudited(t *testing.T) {
	reg, _, _ := governedRegistry(t)
	pol := policy.Policy{Name: "prod-eu", Models: policy.Rule{Allow: []string{"local/*"}}}
	p := classifiedPipeline(model.Binding{Model: "local/llama-3.1-70b"})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()), loom.WithPolicy(pol))
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range res.Audit {
		if e.Action == "policy.admit" && e.Allowed && e.Subject == "prod-eu" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no admission entry in the audit log: %+v", res.Audit)
	}
}

// TestGrantsAreNarrowedNotJustChecked: containment is what makes the refusal
// true of the tasks as well as of the plan, so an envelope built after
// admission cannot carry what the policy denied.
func TestGrantsAreNarrowedNotJustChecked(t *testing.T) {
	reg, _, _ := governedRegistry(t)
	pol := policy.Policy{
		Name:   "no-tools",
		Models: policy.Rule{Allow: []string{"local/*"}},
		Tools:  policy.Rule{Deny: []string{"shell.*"}},
	}

	p := pipeline.New("contained")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding:   model.Binding{Model: "local/llama-3.1-70b"},
			Prompt:    "Classify this ticket: {{.subject}}",
			ParseJSON: true,
		}, pipeline.WithGrants(security.ToolCap("shell.exec")))

	// The gate refuses it, naming the tool.
	_, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()), loom.WithPolicy(pol))
	rep := denial(t, err)
	if len(rep.Violations) != 1 || rep.Violations[0].Subject != "shell.exec" {
		t.Fatalf("want the tool named, got %v", rep.Violations)
	}

	// And the envelopes the same plan would build carry no such grant, which
	// is the half of the guarantee the gate cannot provide on its own.
	env := pol.Bound(taskEnvelopeWith(security.ToolCap("shell.exec"),
		security.ModelCap("local/llama-3.1-70b")))
	if env.Grants.Has(security.ToolCap("shell.exec")) {
		t.Fatal("containment left the denied tool in the envelope")
	}
	if !env.Grants.Has(security.ModelCap("local/llama-3.1-70b")) {
		t.Fatal("containment removed a permitted capability")
	}
}

func taskEnvelopeWith(caps ...security.Capability) task.Envelope {
	return task.Envelope{Stage: "classify", Grants: security.NewGrantSet(caps...)}
}

// TestAnAgentCannotSwapTheFleetsPolicy: a governing document an agent can
// replace governs nothing. Everything here is one program, so this is not a
// privilege boundary — but the surprising reading is the dangerous one.
func TestAnAgentCannotSwapTheFleetsPolicy(t *testing.T) {
	reg, _, hosted := governedRegistry(t)
	fleetPolicy := policy.Policy{
		Name: "fleet-wide", Models: policy.Rule{Allow: []string{"local/*"}},
	}

	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithPolicy(fleetPolicy))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// The agent brings a policy of its own that permits everything.
	_, err = f.Run(context.Background(),
		classifiedPipeline(model.Binding{Model: "vendor/gpt-5"}),
		loom.WithPolicy(policy.Policy{
			Name: "anything-goes", Models: policy.Rule{Allow: []string{"*"}}}))

	rep := denial(t, err)
	if rep.Policy != "fleet-wide" {
		t.Fatalf("the fleet's document must bind, got %q", rep.Policy)
	}
	if hosted.Calls() != 0 {
		t.Fatalf("the agent got %d calls past the fleet's policy", hosted.Calls())
	}
}
