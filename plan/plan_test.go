package plan

import (
	"testing"

	"github.com/zionrubin/brian-ai/loom/core"
	"github.com/zionrubin/brian-ai/loom/model"
	"github.com/zionrubin/brian-ai/loom/pipeline"
	"github.com/zionrubin/brian-ai/loom/security"
)

func reg(t *testing.T) *model.Registry {
	t.Helper()
	r := model.NewRegistry()
	if _, err := model.RegisterMock(r, "small", model.TierFast); err != nil {
		t.Fatal(err)
	}
	m := model.NewMock("big", model.WithEndpoint("api.big.example"))
	if err := r.Register(model.Info{
		ID: "big", Provider: m, Tier: model.TierDeep, SecretRef: "big_key",
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCompileFusesPureChains(t *testing.T) {
	p := pipeline.New("t")
	src := p.FromRecords("src", nil)
	cleaned := src.
		Map("trim", func(r core.Record) (core.Record, error) { return r, nil }).
		Filter("nonempty", func(r core.Record) (bool, error) { return true, nil })
	cleaned.Infer("classify", pipeline.InferSpec{
		Binding: model.Binding{Model: "small"},
		Prompt:  "Classify {{.text}}",
	})

	pl, err := Compile(p, reg(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(pl.Order) != 3 {
		t.Fatalf("want 3 stages after fusion (src, fused, classify), got %d", len(pl.Order))
	}
	fused := pl.Order[1].Stage
	if fused.Kind != pipeline.KindFused || len(fused.Fused) != 2 {
		t.Fatalf("map+filter should fuse: %+v", fused)
	}
	if fused.ID != "nonempty" {
		t.Errorf("fused stage keeps last member's name, got %q", fused.ID)
	}
	classify := pl.Order[2].Stage
	if classify.Upstream.ID != "nonempty" {
		t.Errorf("downstream must rewire to the fused stage, got %q", classify.Upstream.ID)
	}
	if term := pl.Terminal(); len(term) != 1 || term[0] != "classify" {
		t.Errorf("Terminal = %v", term)
	}
}

func TestCompileValidation(t *testing.T) {
	p := pipeline.New("t")
	src := p.FromRecords("src", nil)
	src.Infer("bad", pipeline.InferSpec{
		Binding: model.Binding{Model: "nope"},
		Prompt:  "x {{.f}}",
	})
	if _, err := Compile(p, reg(t)); err == nil {
		t.Fatal("unknown model should fail compilation")
	}

	p2 := pipeline.New("t2")
	s2 := p2.FromRecords("src", nil)
	s2.Infer("dup", pipeline.InferSpec{Binding: model.Binding{Model: "small"}, Prompt: "a"})
	s2.Infer("dup", pipeline.InferSpec{Binding: model.Binding{Model: "small"}, Prompt: "b"})
	if _, err := Compile(p2, reg(t)); err == nil {
		t.Fatal("duplicate stage names should fail compilation")
	}
}

func TestEnvelopeLeastPrivilege(t *testing.T) {
	p := pipeline.New("t")
	src := p.FromRecords("src", nil)
	src.Infer("classify", pipeline.InferSpec{
		Binding: model.Binding{Model: "small", Escalation: []string{"big"}},
		Prompt:  "Classify {{.text}}",
	}, pipeline.WithGrants(security.ToolCap("lookup")))

	pl, err := Compile(p, reg(t))
	if err != nil {
		t.Fatal(err)
	}
	env := pl.ByID["classify"].Envelope("run1", []string{"tools.example"})

	for _, want := range []security.Capability{
		security.ModelCap("small"),
		security.ModelCap("big"),
		security.SecretCap("big_key"),
		security.ToolCap("lookup"),
	} {
		if !env.Grants.Has(want) {
			t.Errorf("envelope missing %s", want)
		}
	}
	if env.Grants.Has(security.ModelCap("other")) {
		t.Error("envelope must not grant unrelated models")
	}
	if !env.Egress.Allowed("api.big.example") || !env.Egress.Allowed("tools.example") {
		t.Errorf("egress allowlist wrong: %v", env.Egress.Hosts)
	}
	if env.Egress.Allowed("evil.example") {
		t.Error("egress must deny unlisted hosts")
	}
}

func TestBuildTasksBatchingAndCacheKeys(t *testing.T) {
	p := pipeline.New("t")
	src := p.FromRecords("src", nil)
	src.Infer("classify", pipeline.InferSpec{
		Binding: model.Binding{Model: "small"}, Prompt: "{{.text}}",
	}, pipeline.WithBatchSize(2))

	pl, err := Compile(p, reg(t))
	if err != nil {
		t.Fatal(err)
	}
	recs := []core.Record{
		core.NewRecord("a", map[string]any{"text": "1"}),
		core.NewRecord("b", map[string]any{"text": "2"}),
		core.NewRecord("c", map[string]any{"text": "3"}),
	}
	sp := pl.ByID["classify"]
	tasks, err := sp.BuildTasks("run1", recs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("batch size 2 over 3 records should yield 2 tasks, got %d", len(tasks))
	}
	if len(tasks[0].Input) != 2 || len(tasks[1].Input) != 1 {
		t.Errorf("unexpected batching: %d, %d", len(tasks[0].Input), len(tasks[1].Input))
	}
	if tasks[0].CacheKey == "" || tasks[0].CacheKey == tasks[1].CacheKey {
		t.Error("cache keys must be set and input-dependent")
	}

	// Identical inputs on a fresh build → identical keys (determinism).
	tasks2, _ := sp.BuildTasks("run2", recs, nil)
	if tasks2[0].CacheKey != tasks[0].CacheKey {
		t.Error("cache keys must be stable across runs (run ID must not leak in)")
	}
}
