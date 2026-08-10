package main

import (
	"context"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
)

// run executes the pipeline offline. Passing a stateDir turns on the
// persistent cache so a second run can be compared against the first.
func run(t *testing.T, stateDir string) (*loom.RunResult, *model.Registry) {
	t.Helper()
	reg, err := registry()
	if err != nil {
		t.Fatal(err)
	}
	opts := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1}),
	}
	if stateDir != "" {
		opts = append(opts, loom.WithStateDir(stateDir))
	}
	res, err := loom.Run(context.Background(), build(items()), opts...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return res, reg
}

// The shape test: assert what the pipeline produces, per stage, not just at
// the end. A stage output that silently went empty is the failure mode worth
// catching, and res.StageOutputs makes every stage observable.
func TestPipelineProducesADigest(t *testing.T) {
	res, _ := run(t, "")

	classified := res.StageOutputs["classify"]
	if len(classified) != len(items()) {
		t.Fatalf("classify produced %d records, want %d", len(classified), len(items()))
	}
	for _, r := range classified {
		switch r.String("severity") {
		case "high", "medium", "low":
		default:
			t.Errorf("%s: severity = %q, want one of high/medium/low", r.ID, r.String("severity"))
		}
	}

	if got := len(res.StageOutputs["severe-only"]); got != 3 {
		t.Errorf("severe-only kept %d records, want 3", got)
	}
	if len(res.Output) != 1 {
		t.Fatalf("digest produced %d records, want 1", len(res.Output))
	}
	if res.Output[0].String("output") == "" {
		t.Error("digest is empty")
	}
	if len(res.Failures) != 0 {
		t.Errorf("unexpected failures: %+v", res.Failures)
	}
}

// The cache test: a rerun against the same state directory should replay
// completed work rather than pay for it again. This is the test that catches
// an accidentally non-deterministic prompt or a stage missing WithVersion.
func TestRerunReplaysFromCache(t *testing.T) {
	dir := t.TempDir()

	first, firstReg := run(t, dir)
	fast, err := firstReg.Get("mock-fast")
	if err != nil {
		t.Fatal(err)
	}
	if fast.Provider.(*model.Mock).Calls() == 0 {
		t.Fatal("the first run should have called the model")
	}

	second, secondReg := run(t, dir)
	fast2, err := secondReg.Get("mock-fast")
	if err != nil {
		t.Fatal(err)
	}
	if n := fast2.Provider.(*model.Mock).Calls(); n != 0 {
		t.Errorf("the cached run made %d model calls, want 0", n)
	}
	if first.Output[0].String("output") != second.Output[0].String("output") {
		t.Error("the replayed run produced a different digest")
	}
}

// The projection test: Explain compiles the pipeline exactly as Run does, so
// it catches template typos, unregistered broadcasts, and unbound models
// before anything is provisioned. A projection with warnings is not
// necessarily wrong, but it is worth reading.
func TestProjectionIsComplete(t *testing.T) {
	reg, err := registry()
	if err != nil {
		t.Fatal(err)
	}
	proj, err := loom.Explain(build(items()),
		loom.WithRegistry(reg),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1}),
		loom.WithStageSample("classify", map[string]any{
			"severity": "high", "topic": "billing",
		}))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if proj.Partial() {
		t.Errorf("projection is partial, so its ceiling is not a bound:\n%s", proj.String())
	}
	if !proj.FitsBudget() {
		t.Errorf("the run budget is below the projected ceiling:\n%s", proj.String())
	}
}
