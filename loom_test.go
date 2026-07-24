package loom_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/brian-ai/loom"
	"github.com/zionrubin/brian-ai/loom/core"
	"github.com/zionrubin/brian-ai/loom/executor"
	"github.com/zionrubin/brian-ai/loom/model"
	"github.com/zionrubin/brian-ai/loom/pipeline"
	"github.com/zionrubin/brian-ai/loom/runtime"
	"github.com/zionrubin/brian-ai/loom/security"
	"github.com/zionrubin/brian-ai/loom/task"
)

func quickRetry() runtime.RetryPolicy {
	return runtime.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
}

func tickets() []core.Record {
	return []core.Record{
		core.NewRecord("t1", map[string]any{"subject": "URGENT refund not processed"}),
		core.NewRecord("t2", map[string]any{"subject": "URGENT app crash on login"}),
		core.NewRecord("t3", map[string]any{"subject": "question about pricing"}),
		core.NewRecord("t4", map[string]any{"subject": "URGENT charged twice refund"}),
	}
}

// classifyMock emulates a classification model with deterministic output.
func classifyMock(req model.Request) (string, error) {
	urgent := strings.Contains(req.Prompt, "URGENT")
	category := "general"
	if strings.Contains(req.Prompt, "refund") {
		category = "billing"
	} else if strings.Contains(req.Prompt, "crash") {
		category = "bug"
	}
	return fmt.Sprintf(`{"category": %q, "urgent": %v}`, category, urgent), nil
}

func summarizeMock(req model.Request) (string, error) {
	n := strings.Count(req.Prompt, "- ")
	return fmt.Sprintf("SUM[%d]", n), nil
}

func triagePipeline(t *testing.T) *pipeline.Pipeline {
	t.Helper()
	p := pipeline.New("triage")
	src := p.FromRecords("tickets", tickets())
	classified := src.
		Map("tag", func(r core.Record) (core.Record, error) {
			r.Data["source"] = "test"
			return r, nil
		}).
		Infer("classify", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			System:    "You are a support ticket classifier.",
			Prompt:    "Classify this ticket: {{.subject}}",
			ParseJSON: true,
		})
	classified.
		Filter("urgent-only", func(r core.Record) (bool, error) {
			b, _ := r.Data["urgent"].(bool)
			return b, nil
		}).
		ReduceAI("brief", pipeline.ReduceAISpec{
			Binding:   model.Binding{Tier: model.TierFast},
			Prompt:    "Summarize {{.Count}} items:\n{{range .Items}}- {{.}}\n{{end}}",
			FanIn:     2,
			ItemField: "category",
		})
	return p
}

func TestEndToEndWithCacheResume(t *testing.T) {
	reg := model.NewRegistry()
	fast, err := model.RegisterMock(reg, "mock-fast", model.TierFast, model.WithHandler(func(req model.Request) (string, error) {
		if strings.Contains(req.Prompt, "Summarize") {
			return summarizeMock(req)
		}
		return classifyMock(req)
	}))
	if err != nil {
		t.Fatal(err)
	}

	stateDir := t.TempDir()
	run := func() *loom.RunResult {
		res, err := loom.Run(context.Background(), triagePipeline(t),
			loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
			loom.WithStateDir(stateDir), loom.WithWorkers(4))
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return res
	}

	res1 := run()

	// 4 classified, 3 urgent, tree-reduced (2+1 → 2 → 1).
	if len(res1.StageOutputs["classify"]) != 4 {
		t.Fatalf("classify outputs = %d", len(res1.StageOutputs["classify"]))
	}
	if len(res1.StageOutputs["urgent-only"]) != 3 {
		t.Fatalf("filter outputs = %d", len(res1.StageOutputs["urgent-only"]))
	}
	if len(res1.Output) != 1 {
		t.Fatalf("terminal output = %d records", len(res1.Output))
	}
	if !strings.HasPrefix(res1.Output[0].String("output"), "SUM[") {
		t.Fatalf("unexpected aggregate: %q", res1.Output[0].String("output"))
	}
	callsAfterRun1 := fast.Calls()
	if callsAfterRun1 != 4+3 { // 4 classify + 3 reduce calls (2 level-1 + 1 level-2)
		t.Fatalf("model calls = %d, want 7", callsAfterRun1)
	}
	if res1.Spent.Requests != 7 {
		t.Errorf("governor recorded %d requests, want 7", res1.Spent.Requests)
	}
	if res1.Report.Totals().Requests != 7 {
		t.Errorf("report recorded %d requests, want 7", res1.Report.Totals().Requests)
	}
	if len(res1.Lineage) == 0 {
		t.Error("lineage should record produced artifacts")
	}

	// Second run over the same state dir: all AI work replays from the
	// content-addressed cache — zero new model calls, zero new cost.
	res2 := run()
	if fast.Calls() != callsAfterRun1 {
		t.Fatalf("second run made %d new model calls, want 0", fast.Calls()-callsAfterRun1)
	}
	if res2.Output[0].String("output") != res1.Output[0].String("output") {
		t.Error("cached replay must reproduce identical output")
	}
	var cacheHits int
	for _, s := range res2.Report.Stages {
		cacheHits += s.CacheHits
	}
	if cacheHits != 7 {
		t.Errorf("second run cache hits = %d, want 7", cacheHits)
	}
}

func TestSemanticEscalationEndToEnd(t *testing.T) {
	reg := model.NewRegistry()
	small, err := model.RegisterMock(reg, "small", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			return "sorry, I cannot produce JSON today", nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	big, err := model.RegisterMock(reg, "big", model.TierDeep,
		model.WithHandler(classifyMock))
	if err != nil {
		t.Fatal(err)
	}

	p := pipeline.New("escalate")
	src := p.FromRecords("in", tickets()[:1])
	src.Infer("classify", pipeline.InferSpec{
		Binding:   model.Binding{Model: "small", Escalation: []string{"big"}},
		Prompt:    "Classify: {{.subject}}",
		ParseJSON: true,
	})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	out := res.Output
	if len(out) != 1 || out[0].String("category") == "" {
		t.Fatalf("escalated output missing category: %+v", out)
	}
	if small.Calls() == 0 {
		t.Error("base model should have been tried first")
	}
	if big.Calls() == 0 {
		t.Error("semantic failure should escalate to the stronger model")
	}
}

func TestToolCapabilityEnforcement(t *testing.T) {
	lookup := executor.FuncTool("lookup", func(ctx context.Context, args map[string]any) (any, error) {
		return "resolved:" + fmt.Sprint(args["id"]), nil
	})

	build := func(granted bool) *pipeline.Pipeline {
		var opts []pipeline.Option
		if granted {
			opts = append(opts, pipeline.WithGrants(security.ToolCap("lookup")))
		}
		p := pipeline.New("tools")
		src := p.FromRecords("in", []core.Record{core.NewRecord("r", map[string]any{"id": "42"})})
		src.MapTools("enrich", func(ctx context.Context, tools core.Tools, r core.Record) (core.Record, error) {
			v, err := tools.Invoke(ctx, "lookup", map[string]any{"id": r.String("id")})
			if err != nil {
				return core.Record{}, err
			}
			r.Data["resolved"] = v
			return r, nil
		}, opts...)
		return p
	}

	// Granted: succeeds and is audited as allowed.
	res, err := loom.Run(context.Background(), build(true),
		loom.WithRetry(quickRetry()), loom.WithTools(lookup))
	if err != nil {
		t.Fatalf("granted run: %v", err)
	}
	if res.Output[0].String("resolved") != "resolved:42" {
		t.Fatalf("tool output missing: %+v", res.Output[0].Data)
	}

	// Not granted: the run fails and the denial is audited.
	res, err = loom.Run(context.Background(), build(false),
		loom.WithRetry(quickRetry()), loom.WithTools(lookup))
	if err == nil {
		t.Fatal("ungranted tool use must fail the run")
	}
	var denied bool
	for _, e := range res.Audit {
		if e.Action == "tool.invoke" && !e.Allowed {
			denied = true
		}
	}
	if !denied {
		t.Error("denial must appear in the audit log")
	}
}

func TestRunBudgetAbort(t *testing.T) {
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "m", model.TierFast,
		model.WithHandler(classifyMock)); err != nil {
		t.Fatal(err)
	}

	p := pipeline.New("budget")
	src := p.FromRecords("in", tickets())
	src.Infer("classify", pipeline.InferSpec{
		Binding: model.Binding{Model: "m"}, Prompt: "Classify: {{.subject}}", ParseJSON: true,
	})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithWorkers(1),
		loom.WithRunBudget(core.Budget{MaxTokens: 10}))
	if !errors.Is(err, runtime.ErrBudgetExhausted) {
		t.Fatalf("expected budget exhaustion, got %v", err)
	}
	if res == nil {
		t.Fatal("partial results must be returned on budget abort")
	}
	if got := len(res.StageOutputs["classify"]); got == 0 || got >= 4 {
		t.Errorf("expected partial classify outputs, got %d", got)
	}
}

func TestEgressDenied(t *testing.T) {
	// A provider with a network endpoint that is not on the stage's egress
	// allowlist must be blocked. Endpoints of bound models are auto-allowed,
	// so simulate a compromised registry entry pointing elsewhere: register
	// a second model whose endpoint is not in the binding's candidates.
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "good", model.TierFast,
		model.WithHandler(classifyMock)); err != nil {
		t.Fatal(err)
	}
	rogue := model.NewMock("rogue", model.WithEndpoint("exfil.example"))
	if err := reg.Register(model.Info{ID: "rogue", Provider: rogue}); err != nil {
		t.Fatal(err)
	}

	// Build a task envelope for "good" and try to call "rogue" through the
	// model client directly: both the grant check and egress check refuse.
	audit := &security.AuditLog{}
	client := &executor.ModelClient{
		Registry: reg,
		Broker:   security.NewStaticBroker(nil, audit),
		Audit:    audit,
	}
	env := task.Envelope{
		RunID: "run1", Stage: "s",
		Binding: model.Binding{Model: "good"},
		Grants:  security.NewGrantSet(security.ModelCap("good")),
		// Egress allowlist covers only the bound model (which is
		// in-process); "exfil.example" is not on it.
	}

	// Ungranted model: denied and audited.
	_, err := client.Call(context.Background(), env, "task1", "rogue", model.Request{Prompt: "x"})
	if err == nil {
		t.Fatal("call to ungranted model must fail")
	}
	if core.ClassOf(err) != core.FailPermanent {
		t.Errorf("security denial should be permanent, got %v", err)
	}

	// Granted model but endpoint off the egress allowlist: still denied.
	env.Grants = env.Grants.With(security.ModelCap("rogue"))
	_, err = client.Call(context.Background(), env, "task1", "rogue", model.Request{Prompt: "x"})
	if err == nil {
		t.Fatal("egress to unlisted host must fail")
	}

	denials := audit.Denials()
	if len(denials) != 2 {
		t.Fatalf("want 2 audited denials, got %d: %+v", len(denials), denials)
	}
	if denials[1].Action != "egress" || denials[1].Subject != "exfil.example" {
		t.Errorf("second denial should be the egress block: %+v", denials[1])
	}
}
