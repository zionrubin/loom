package plan

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/policy"
	"github.com/zionrubin/loom/security"
)

func classesOf(t *testing.T, pl *Plan, stage string) []string {
	t.Helper()
	sp, ok := pl.ByID[stage]
	if !ok {
		t.Fatalf("no stage %q in plan (have %v)", stage, stageIDs(pl))
	}
	return sp.DataClasses
}

func stageIDs(pl *Plan) []string {
	var out []string
	for _, sp := range pl.Order {
		out = append(out, sp.Stage.ID)
	}
	return out
}

func identity(r core.Record) (core.Record, error) { return r, nil }

// TestClassesFlowDownstream: a class is declared where data enters and reaches
// everything the data reaches, which is the only way a classification scheme
// survives contact with a pipeline nobody re-reads.
func TestClassesFlowDownstream(t *testing.T) {
	p := pipeline.New("flow")
	src := p.FromRecords("tickets", []core.Record{core.NewRecord("t1", nil)},
		pipeline.WithDataClass("pii"))
	inferred := src.
		Map("tag", identity, pipeline.WithVersion("v1")).
		Infer("classify", pipeline.InferSpec{
			Binding: model.Binding{Model: "small"}, Prompt: "{{.id}}"})
	inferred.ReduceAI("brief", pipeline.ReduceAISpec{
		Binding: model.Binding{Model: "small"}, Prompt: "{{.Count}}"})

	pl, err := Compile(p, reg(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"tickets", "tag", "classify", "brief"} {
		got := classesOf(t, pl, stage)
		if len(got) != 1 || got[0] != "pii" {
			t.Errorf("stage %q classes = %v, want [pii]", stage, got)
		}
	}
	// And it reaches the envelope, which is what a worker in another process
	// is handed.
	if env := pl.ByID["classify"].Envelope("run", nil); len(env.DataClasses) != 1 {
		t.Fatalf("envelope classes = %v", env.DataClasses)
	}
}

// TestClassesMergeAndDoNotFlowUpstream: a label added mid-pipeline joins what
// it inherited, and nothing above it is retroactively classified.
func TestClassesMergeAndDoNotFlowUpstream(t *testing.T) {
	p := pipeline.New("merge")
	src := p.FromRecords("in", []core.Record{core.NewRecord("t1", nil)},
		pipeline.WithDataClass("confidential"))
	src.
		Infer("enrich", pipeline.InferSpec{
			Binding: model.Binding{Model: "small"}, Prompt: "{{.id}}"},
			pipeline.WithDataClass("pii")).
		Infer("summarize", pipeline.InferSpec{
			Binding: model.Binding{Model: "small"}, Prompt: "{{.id}}"})

	pl, err := Compile(p, reg(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := classesOf(t, pl, "in"); len(got) != 1 || got[0] != "confidential" {
		t.Fatalf("the source keeps its own class only, got %v", got)
	}
	for _, stage := range []string{"enrich", "summarize"} {
		got := classesOf(t, pl, stage)
		if strings.Join(got, ",") != "confidential,pii" {
			t.Errorf("stage %q classes = %v, want both, sorted", stage, got)
		}
	}
}

// TestFusionKeepsClasses: pure stages are fused into one task boundary before
// classification runs, so a class declared on an absorbed stage must survive
// into the stage that absorbed it.
func TestFusionKeepsClasses(t *testing.T) {
	p := pipeline.New("fused")
	p.FromRecords("in", []core.Record{core.NewRecord("t1", nil)}).
		Map("a", identity, pipeline.WithVersion("v1"), pipeline.WithDataClass("pii")).
		Map("b", identity, pipeline.WithVersion("v1")).
		Infer("classify", pipeline.InferSpec{
			Binding: model.Binding{Model: "small"}, Prompt: "{{.id}}"})

	pl, err := Compile(p, reg(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := classesOf(t, pl, "classify"); len(got) != 1 || got[0] != "pii" {
		t.Fatalf("fusion lost the class: %v (stages %v)", got, stageIDs(pl))
	}
}

// TestClassesDoNotMoveTheFingerprint: a class says who may see a result, not
// what the result is, so relabelling must not throw away answers already paid
// for.
func TestClassesDoNotMoveTheFingerprint(t *testing.T) {
	build := func(classes ...string) string {
		p := pipeline.New("fp")
		p.FromRecords("in", []core.Record{core.NewRecord("t1", nil)},
			pipeline.WithDataClass(classes...)).
			Infer("classify", pipeline.InferSpec{
				Binding: model.Binding{Model: "small"}, Prompt: "{{.id}}"})
		pl, err := Compile(p, reg(t))
		if err != nil {
			t.Fatal(err)
		}
		return pl.ByID["classify"].Fingerprint
	}
	if build() != build("pii") {
		t.Fatal("labelling data changed the cache key of the work done on it")
	}
}

// TestCompileRefusesAnInadmissiblePlan: the gate is loud, complete, and costs
// nothing — it runs before a scheduler exists.
func TestCompileRefusesAnInadmissiblePlan(t *testing.T) {
	p := pipeline.New("refused")
	p.FromRecords("in", []core.Record{core.NewRecord("t1", nil)}).
		Infer("classify", pipeline.InferSpec{
			Binding: model.Binding{Model: "big"}, Prompt: "{{.id}}"},
			pipeline.WithGrants(security.ToolCap("shell.exec")))

	pol := policy.Policy{
		Name:    "prod",
		Models:  policy.Rule{Allow: []string{"small"}},
		Tools:   policy.Rule{Deny: []string{"shell.*"}},
		Egress:  policy.Rule{Allow: []string{"api.small.example"}},
		Secrets: policy.Rule{Allow: []string{"small_key"}},
	}
	pl, err := Compile(p, reg(t), WithPolicy(pol))
	if pl != nil {
		t.Fatal("an inadmissible plan must not be returned")
	}
	var denied *policy.Denied
	if !errors.As(err, &denied) {
		t.Fatalf("want *policy.Denied, got %T: %v", err, err)
	}
	// Model, secret, tool and egress: the whole list, at once.
	axes := map[policy.Axis]bool{}
	for _, v := range denied.Report.Violations {
		axes[v.Axis] = true
	}
	for _, want := range []policy.Axis{policy.AxisModel, policy.AxisSecret,
		policy.AxisTool, policy.AxisEgress} {
		if !axes[want] {
			t.Errorf("no %s violation in %v", want, denied.Report.Violations)
		}
	}
	if denied.Report.Checked != 1 {
		t.Errorf("checked = %d, want 1 (the source reaches nothing)", denied.Report.Checked)
	}
}

// TestAdmittedEnvelopesAreUnchanged: on the ordinary path containment is the
// identity, so turning a policy on does not quietly change a conforming plan.
func TestAdmittedEnvelopesAreUnchanged(t *testing.T) {
	build := func(opts ...Option) *Plan {
		p := pipeline.New("ok")
		p.FromRecords("in", []core.Record{core.NewRecord("t1", nil)}).
			Infer("classify", pipeline.InferSpec{
				Binding: model.Binding{Model: "small"}, Prompt: "{{.id}}"})
		pl, err := Compile(p, reg(t), opts...)
		if err != nil {
			t.Fatal(err)
		}
		return pl
	}
	pol := policy.Policy{Name: "permissive", Models: policy.Rule{Allow: []string{"*"}}}

	bare := build().ByID["classify"].Envelope("run", nil)
	under := build(WithPolicy(pol)).ByID["classify"].Envelope("run", nil)

	if len(bare.Grants.List()) != len(under.Grants.List()) {
		t.Fatalf("grants changed under a permissive policy: %v vs %v",
			bare.Grants.List(), under.Grants.List())
	}
	a, err := json.Marshal(bare)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(under)
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("the envelope changed under a permissive policy:\n%s\n%s", a, b)
	}
}

// TestContainmentCatchesRunLevelEgress: the compile-time gate never sees a
// host opened for the whole run, so the narrowing has to.
func TestContainmentCatchesRunLevelEgress(t *testing.T) {
	p := pipeline.New("contained")
	p.FromRecords("in", []core.Record{core.NewRecord("t1", nil)}).
		Infer("classify", pipeline.InferSpec{
			Binding: model.Binding{Model: "small"}, Prompt: "{{.id}}"})

	pol := policy.Policy{Egress: policy.Rule{Allow: []string{"*.internal"}}}
	pl, err := Compile(p, reg(t), WithPolicy(pol))
	if err != nil {
		t.Fatal(err)
	}
	env := pl.ByID["classify"].Envelope("run", []string{"db.internal", "exfil.example"})
	if env.Egress.Allowed("exfil.example") {
		t.Fatalf("containment left a denied host on the allowlist: %v", env.Egress.Hosts)
	}
	if !env.Egress.Allowed("db.internal") {
		t.Fatalf("containment removed a permitted host: %v", env.Egress.Hosts)
	}
}

// TestClassesFollowBranches: propagation is one forward pass over the plan's
// order, which is only correct because that order is topological. A DAG that
// branches is where a mistake there would show.
func TestClassesFollowBranches(t *testing.T) {
	p := pipeline.New("branch")
	src := p.FromRecords("in", []core.Record{core.NewRecord("t1", nil)})
	classified := src.Infer("classify", pipeline.InferSpec{
		Binding: model.Binding{Model: "small"}, Prompt: "{{.id}}"},
		pipeline.WithDataClass("pii"))

	// Two consumers of the same classified stage, and a third branch off the
	// unclassified source that must stay clean.
	classified.Map("left", identity, pipeline.WithVersion("v1"))
	classified.ReduceAI("right", pipeline.ReduceAISpec{
		Binding: model.Binding{Model: "small"}, Prompt: "{{.Count}}"})
	src.Map("sibling", identity, pipeline.WithVersion("v1"))

	pl, err := Compile(p, reg(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"classify", "left", "right"} {
		if got := classesOf(t, pl, stage); len(got) != 1 || got[0] != "pii" {
			t.Errorf("stage %q classes = %v, want [pii]", stage, got)
		}
	}
	for _, stage := range []string{"in", "sibling"} {
		if got := classesOf(t, pl, stage); len(got) != 0 {
			t.Errorf("stage %q should be unclassified, got %v", stage, got)
		}
	}
}
