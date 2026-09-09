package policy_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/policy"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/task"
)

func TestRuleSemantics(t *testing.T) {
	cases := []struct {
		name    string
		rule    policy.Rule
		subject string
		want    bool
	}{
		{"empty rule is unconstrained", policy.Rule{}, "anything", true},
		{"allowlist admits a match",
			policy.Rule{Allow: []string{"claude-*"}}, "claude-opus-4-8", true},
		{"allowlist refuses a non-match",
			policy.Rule{Allow: []string{"claude-*"}}, "gpt-5", false},
		{"deny wins over allow",
			policy.Rule{Allow: []string{"*"}, Deny: []string{"gpt-5"}}, "gpt-5", false},
		{"deny star refuses everything",
			policy.Rule{Deny: []string{"*"}}, "whatever", false},
		{"a star matches an empty remainder",
			policy.Rule{Allow: []string{"local/*"}}, "local/", true},
		{"a suffix pattern matches a host",
			policy.Rule{Allow: []string{"*.anthropic.com"}}, "api.anthropic.com", true},
		{"a suffix pattern does not match a lookalike",
			policy.Rule{Allow: []string{"*.anthropic.com"}}, "api.anthropic.com.evil.net", false},
		{"dots are ordinary characters",
			policy.Rule{Allow: []string{"a.b.c"}}, "axbxc", false},
		{"slashes are ordinary characters",
			policy.Rule{Allow: []string{"local/*"}}, "local/llama-3.1-70b", true},
		{"an infix pattern matches",
			policy.Rule{Allow: []string{"*-review-*"}}, "stage-review-2", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, why := c.rule.Permits(c.subject)
			if got != c.want {
				t.Fatalf("Permits(%q) = %v (%s), want %v", c.subject, got, why, c.want)
			}
			if !got && why == "" {
				t.Fatal("a refusal must say why")
			}
		})
	}
}

// env builds an envelope the way the planner would, so the tests judge the
// same shape production does.
func env(stage string, caps []security.Capability, hosts []string, classes ...string) task.Envelope {
	return task.Envelope{
		Stage:       stage,
		Grants:      security.NewGrantSet(caps...),
		Egress:      security.EgressPolicy{}.With(hosts...),
		Sandbox:     task.SandboxInline,
		DataClasses: classes,
	}
}

func TestCheckReportsEveryViolationAtOnce(t *testing.T) {
	p := policy.Policy{
		Name:    "prod",
		Models:  policy.Rule{Allow: []string{"claude-*"}},
		Tools:   policy.Rule{Deny: []string{"shell.*"}},
		Egress:  policy.Rule{Allow: []string{"api.anthropic.com"}},
		Secrets: policy.Rule{Allow: []string{"anthropic_api_key"}},
	}
	e := env("triage",
		[]security.Capability{
			security.ModelCap("gpt-5"),
			security.ToolCap("shell.exec"),
			security.SecretCap("openai_api_key"),
			security.DataCap("rubric"), // ungoverned kind: must not be flagged
		},
		[]string{"api.openai.com"})

	vs := p.Check(e)
	if len(vs) != 4 {
		t.Fatalf("want 4 violations (model, tool, secret, egress), got %d: %v", len(vs), vs)
	}
	axes := map[policy.Axis]bool{}
	for _, v := range vs {
		axes[v.Axis] = true
		if v.Stage != "triage" {
			t.Errorf("violation %v does not name its stage", v)
		}
	}
	for _, want := range []policy.Axis{policy.AxisModel, policy.AxisTool,
		policy.AxisSecret, policy.AxisEgress} {
		if !axes[want] {
			t.Errorf("no violation on axis %q", want)
		}
	}
}

func TestCheckAdmitsAConformingEnvelope(t *testing.T) {
	p := policy.Policy{
		Models:  policy.Rule{Allow: []string{"claude-*"}},
		Egress:  policy.Rule{Allow: []string{"api.anthropic.com"}},
		Sandbox: policy.Rule{Allow: []string{"inline"}},
	}
	e := env("ok", []security.Capability{security.ModelCap("claude-opus-4-8")},
		[]string{"api.anthropic.com"})
	if vs := p.Check(e); len(vs) != 0 {
		t.Fatalf("want admission, got %v", vs)
	}
}

func TestSandboxIsAnAllowlistNotAnOrdering(t *testing.T) {
	p := policy.Policy{Sandbox: policy.Rule{Allow: []string{"container", "wasm"}}}

	e := env("inline-stage", nil, nil)
	vs := p.Check(e)
	if len(vs) != 1 || vs[0].Axis != policy.AxisSandbox {
		t.Fatalf("want one sandbox violation, got %v", vs)
	}

	e.Sandbox = task.SandboxContainer
	if vs := p.Check(e); len(vs) != 0 {
		t.Fatalf("container is on the allowlist, got %v", vs)
	}
}

func TestDataClearanceCoversEveryReachableModel(t *testing.T) {
	// The ladder is the point: a stage that starts on a cleared model and
	// escalates to an uncleared one leaks on the second call.
	p := policy.Policy{
		Data: map[string]policy.Rule{"pii": {Allow: []string{"local/*"}}},
	}
	e := env("triage", []security.Capability{
		security.ModelCap("local/llama-3.1-70b"),
		security.ModelCap("gpt-5"),
	}, nil, "pii")

	vs := p.Check(e)
	if len(vs) != 1 {
		t.Fatalf("want one clearance violation, got %v", vs)
	}
	if !strings.Contains(vs[0].Subject, "gpt-5") {
		t.Fatalf("violation should name the uncleared model: %v", vs[0])
	}
}

func TestAnUnlistedClassIsRefusedNotWavedThrough(t *testing.T) {
	p := policy.Policy{Data: map[string]policy.Rule{"pii": {Allow: []string{"*"}}}}
	e := env("s", []security.Capability{security.ModelCap("m")}, nil, "phi")

	vs := p.Check(e)
	if len(vs) != 1 || vs[0].Axis != policy.AxisData || vs[0].Subject != "phi" {
		t.Fatalf("an unrecognized class must be refused, got %v", vs)
	}
}

func TestAPolicyWithNoDataRulesDoesNotGovernData(t *testing.T) {
	p := policy.Policy{Models: policy.Rule{Allow: []string{"*"}}}
	e := env("s", []security.Capability{security.ModelCap("m")}, nil, "pii", "phi")
	if vs := p.Check(e); len(vs) != 0 {
		t.Fatalf("classes must be inert without a clearance table, got %v", vs)
	}
}

// TestBoundNeverWidens is the invariant that ties the two faces of a policy
// together: containment can only remove. Anything Bound produces that Check
// would have refused is a hole, and anything it removes that Check admitted is
// a run doing less than its plan said.
func TestBoundNeverWidens(t *testing.T) {
	p := policy.Policy{
		Models:  policy.Rule{Allow: []string{"claude-*"}},
		Tools:   policy.Rule{Deny: []string{"shell.*"}},
		Egress:  policy.Rule{Allow: []string{"api.anthropic.com"}},
		Secrets: policy.Rule{Allow: []string{"anthropic_api_key"}},
	}
	e := env("mixed", []security.Capability{
		security.ModelCap("claude-opus-4-8"),
		security.ModelCap("gpt-5"),
		security.ToolCap("shell.exec"),
		security.ToolCap("web.fetch"),
		security.SecretCap("anthropic_api_key"),
		security.SecretCap("openai_api_key"),
		security.DataCap("rubric"),
	}, []string{"api.anthropic.com", "api.openai.com"})

	bounded := p.Bound(e)

	if vs := p.Check(bounded); len(vs) != 0 {
		t.Fatalf("a bounded envelope must be admissible, got %v", vs)
	}
	for _, c := range bounded.Grants.List() {
		if !e.Grants.Has(c) {
			t.Fatalf("Bound introduced capability %q", c)
		}
	}
	for _, h := range bounded.Egress.Hosts {
		if !e.Egress.Allowed(h) {
			t.Fatalf("Bound introduced host %q", h)
		}
	}
	// What survives is exactly what the policy permits — and the ungoverned
	// capability kind passes through rather than being silently stripped.
	want := []security.Capability{
		security.DataCap("rubric"),
		security.ModelCap("claude-opus-4-8"),
		security.SecretCap("anthropic_api_key"),
		security.ToolCap("web.fetch"),
	}
	got := bounded.Grants.List()
	if len(got) != len(want) {
		t.Fatalf("bounded grants = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("bounded grants = %v, want %v", got, want)
		}
	}
	if len(bounded.Egress.Hosts) != 1 || bounded.Egress.Hosts[0] != "api.anthropic.com" {
		t.Fatalf("bounded egress = %v", bounded.Egress.Hosts)
	}
}

func TestBoundIsTheIdentityOnAnAdmittedEnvelope(t *testing.T) {
	p := policy.Policy{Models: policy.Rule{Allow: []string{"claude-*"}}}
	e := env("ok", []security.Capability{security.ModelCap("claude-opus-4-8")},
		[]string{"api.anthropic.com"})
	if vs := p.Check(e); len(vs) != 0 {
		t.Fatalf("precondition: %v", vs)
	}
	b := p.Bound(e)
	if len(b.Grants.List()) != len(e.Grants.List()) ||
		len(b.Egress.Hosts) != len(e.Egress.Hosts) {
		t.Fatalf("Bound changed an admitted envelope: %v / %v", b.Grants.List(), b.Egress.Hosts)
	}
}

func TestBoundLeavesTheScalarFieldsAlone(t *testing.T) {
	// Sandbox and binding are fixed when the stage is written, so Check is the
	// whole of their enforcement; rewriting them would change what a pipeline
	// does rather than what it may do.
	p := policy.Policy{Sandbox: policy.Rule{Allow: []string{"container"}}}
	e := env("s", nil, nil)
	if got := p.Bound(e).Sandbox; got != task.SandboxInline {
		t.Fatalf("Bound rewrote the sandbox to %q", got)
	}
}

func TestBudgetComposesAsACeiling(t *testing.T) {
	p := policy.Policy{MaxCostUSD: 10}

	if vs := p.CheckRun(core.Budget{MaxCostUSD: 100}, nil); len(vs) != 1 ||
		vs[0].Axis != policy.AxisBudget {
		t.Fatalf("a run budget above the ceiling must be refused, got %v", vs)
	}
	if vs := p.CheckRun(core.Budget{MaxCostUSD: 5}, nil); len(vs) != 0 {
		t.Fatalf("a run budget below the ceiling is fine, got %v", vs)
	}
	if got := p.BoundBudget(core.Budget{}).MaxCostUSD; got != 10 {
		t.Fatalf("a run naming no budget inherits the ceiling, got %v", got)
	}
	if got := p.BoundBudget(core.Budget{MaxCostUSD: 5}).MaxCostUSD; got != 5 {
		t.Fatalf("a tighter run budget survives, got %v", got)
	}
	if got := p.Bound(env("s", nil, nil)).Budget.MaxCostUSD; got != 10 {
		t.Fatalf("a stage budget inherits the ceiling, got %v", got)
	}
}

func TestCheckRunJudgesRunLevelEgress(t *testing.T) {
	p := policy.Policy{Egress: policy.Rule{Allow: []string{"*.internal"}}}
	vs := p.CheckRun(core.Budget{}, []string{"db.internal", "evil.example"})
	if len(vs) != 1 || vs[0].Subject != "evil.example" {
		t.Fatalf("want one egress violation for evil.example, got %v", vs)
	}
}

func TestReportRendersAndCarriesTheViolations(t *testing.T) {
	var rep policy.Report
	rep.Policy, rep.Checked = "prod-eu", 3
	if !rep.Admitted() || rep.Err() != nil {
		t.Fatal("an empty report admits")
	}
	if !strings.Contains(rep.String(), "admitted") {
		t.Fatalf("admission should say so: %q", rep.String())
	}

	rep.Add(
		policy.Violation{Stage: "b", Axis: policy.AxisModel, Subject: "gpt-5", Reason: "no"},
		policy.Violation{Stage: "a", Axis: policy.AxisEgress, Subject: "h", Reason: "no"},
	)
	if rep.Admitted() {
		t.Fatal("a report with violations does not admit")
	}
	if rep.Violations[0].Stage != "a" {
		t.Fatalf("Add must keep the report ordered: %v", rep.Violations)
	}

	err := rep.Err()
	var denied *policy.Denied
	if !errors.As(err, &denied) {
		t.Fatalf("Err must return a *Denied, got %T", err)
	}
	if len(denied.Report.Violations) != 2 {
		t.Fatal("the error carries the whole report")
	}
	for _, want := range []string{"prod-eu", "gpt-5", "refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal message missing %q: %s", want, err)
		}
	}
}

func TestZeroPolicyGovernsNothing(t *testing.T) {
	var p policy.Policy
	if !p.IsZero() {
		t.Fatal("the zero policy governs nothing")
	}
	e := env("s", []security.Capability{security.ModelCap("anything")},
		[]string{"anywhere"}, "pii")
	if vs := p.Check(e); len(vs) != 0 {
		t.Fatalf("the zero policy admits everything, got %v", vs)
	}
	if (policy.Policy{MaxCostUSD: 1}).IsZero() {
		t.Fatal("a ceiling alone makes a policy non-zero")
	}
}

func TestParseRoundTripsAndRejectsTypos(t *testing.T) {
	// The document form is the point: a policy is reviewed and versioned
	// beside the infrastructure it governs, not compiled into the program it
	// constrains.
	src := []byte(`{
	  "name": "prod-eu",
	  "models": {"allow": ["claude-*"]},
	  "egress": {"allow": ["api.anthropic.com"]},
	  "data": {"pii": {"allow": ["local/*"]}},
	  "max_cost_usd": 25
	}`)
	p, err := policy.Parse(src)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if p.Name != "prod-eu" || p.MaxCostUSD != 25 {
		t.Fatalf("parsed wrong: %+v", p)
	}
	if ok, _ := p.Data["pii"].Permits("gpt-5"); ok {
		t.Fatal("clearance did not survive the round trip")
	}

	out, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	again, err := policy.Parse(out)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if again.Name != p.Name || again.MaxCostUSD != p.MaxCostUSD {
		t.Fatal("policy does not round-trip through JSON")
	}

	// A misspelled rule is a rule that does not apply, which is the worst
	// possible failure mode for a security document: it must not parse.
	if _, err := policy.Parse([]byte(`{"modles": {"allow": ["x"]}}`)); err == nil {
		t.Fatal("a misspelled field must be refused, not ignored")
	}
}
