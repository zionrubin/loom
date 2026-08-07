package main

import (
	"context"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/providers/llamacpp"
	"github.com/zionrubin/loom/providers/llamacpp/llamacpptest"
	"github.com/zionrubin/loom/security"
)

// wire starts the two in-process servers and registers them exactly as main
// does, returning the registry and the servers so a test can read what they
// saw. Latency is added so calls genuinely overlap: concurrency that never
// happens proves nothing about the ceiling that bounds it.
func wire(t *testing.T, delay time.Duration) (*model.Registry, *llamacpptest.Server, *llamacpptest.Server) {
	t.Helper()

	fastURL, fastSrv, closeFast := server("", triageModel(), 2, delay)
	t.Cleanup(closeFast)
	deepURL, deepSrv, closeDeep := server("", briefModel(), 1, delay)
	t.Cleanup(closeDeep)

	reg := model.NewRegistry()
	ctx := context.Background()
	if _, err := llamacpp.Register(ctx, reg, llamacpp.New(fastURL), "local-fast", model.TierFast); err != nil {
		t.Fatalf("registering local-fast: %v", err)
	}
	if _, err := llamacpp.Register(ctx, reg, llamacpp.New(deepURL), "local-deep", model.TierDeep); err != nil {
		t.Fatalf("registering local-deep: %v", err)
	}
	return reg, fastSrv, deepSrv
}

// TestPipelineOffline runs the exact pipeline main() builds against real HTTP
// servers on loopback — no GPU, no model file, no key. It guards the parts a
// compile cannot: the templates render, the JSON parses, validation gates the
// ladder, and every stage produces output.
func TestPipelineOffline(t *testing.T) {
	reg, _, _ := wire(t, 0)

	res, err := loom.Run(context.Background(), desk(),
		loom.WithRegistry(reg),
		loom.WithWorkers(8),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1.00}),
	)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	triaged := res.StageOutputs["triage"]
	if len(triaged) != len(incidents) {
		t.Fatalf("triage produced %d records, want %d", len(triaged), len(incidents))
	}
	for _, r := range triaged {
		switch r.String("severity") {
		case "sev1", "sev2", "sev3", "sev4":
		default:
			t.Errorf("%s: severity %q survived validation", r.ID, r.String("severity"))
		}
		if r.String("component") == "" {
			t.Errorf("%s: no component", r.ID)
		}
	}

	// Five incidents page; the reduce tree folds them to one brief.
	if got := len(res.StageOutputs["pageworthy"]); got != 5 {
		t.Errorf("pageworthy kept %d records, want 5", got)
	}
	if len(res.Output) != 1 {
		t.Fatalf("brief produced %d records, want 1", len(res.Output))
	}
	if !strings.Contains(res.Output[0].String("output"), "Payments") {
		t.Errorf("the brief should lead with what costs money, got %q", res.Output[0].String("output"))
	}
}

// TestEscalationClimbsToTheLargerLocalModel checks the ladder both rungs of
// which are on this machine: the small model declines to place one incident,
// validation rejects that, and the retry runs on the large one.
func TestEscalationClimbsToTheLargerLocalModel(t *testing.T) {
	reg, fastSrv, deepSrv := wire(t, 0)

	answered := newLedger()
	res, err := loom.Run(context.Background(), desk(),
		loom.WithRegistry(reg),
		loom.WithWorkers(8),
		loom.WithEventHandler(answered.Handle),
	)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// inc-6 is the contradictory report. The small model tried it and failed
	// validation, so both servers saw it.
	if got := answered.model("inc-6"); got != "local-deep" {
		t.Errorf("inc-6 was answered by %q, want local-deep after escalation", got)
	}
	for _, id := range []string{"inc-1", "inc-2", "inc-3"} {
		if got := answered.model(id); got != "local-fast" {
			t.Errorf("%s was answered by %q, want local-fast — only failures should climb", id, got)
		}
	}
	if fastSrv.Calls() != len(incidents) {
		t.Errorf("the small model saw %d calls, want one per incident (%d)", fastSrv.Calls(), len(incidents))
	}
	// One escalated triage plus the reduce tree over five pageable records
	// (FanIn 4 → two groups, then one).
	if deepSrv.Calls() != 4 {
		t.Errorf("the large model saw %d calls, want 4", deepSrv.Calls())
	}

	var sev string
	for _, r := range res.StageOutputs["triage"] {
		if r.ID == "inc-6" {
			sev = r.String("severity")
		}
	}
	if sev != "sev1" {
		t.Errorf("inc-6 severity = %q, want the escalated sev1", sev)
	}
}

// TestAdmissionRespectsTheDevicesCeiling is the property local inference adds
// to the scheduler: with more workers than the hardware has slots, the excess
// waits in admission control rather than queueing invisibly inside the server.
//
// The servers do not enforce their own slot counts — they only count what
// arrived — so a peak above the ceiling would be a real failure of admission
// rather than something the fake absorbed.
func TestAdmissionRespectsTheDevicesCeiling(t *testing.T) {
	reg, fastSrv, deepSrv := wire(t, 5*time.Millisecond)

	if _, err := loom.Run(context.Background(), desk(),
		loom.WithRegistry(reg),
		// Eight workers against three slots: deliberately oversubscribed.
		loom.WithWorkers(8),
	); err != nil {
		t.Fatalf("run: %v", err)
	}

	if peak := fastSrv.Peak(); peak > 2 {
		t.Errorf("local-fast saw %d calls in flight, past its 2 slots", peak)
	}
	if peak := deepSrv.Peak(); peak > 1 {
		t.Errorf("local-deep saw %d calls in flight, past its 1 slot", peak)
	}
	// And the ceiling is a ceiling, not a serialization: the wider server
	// should actually have used both slots.
	if peak := fastSrv.Peak(); peak < 2 {
		t.Errorf("local-fast peaked at %d in flight; its 2 slots went unused", peak)
	}
}

// TestLocalRunNeedsNoCredentialAndCannotEgress pins the two security
// properties the example claims, read off the envelope the planner actually
// produced rather than asserted about it.
func TestLocalRunNeedsNoCredentialAndCannotEgress(t *testing.T) {
	reg, _, _ := wire(t, 0)
	p := desk()

	env, ok := triageEnvelope(p, reg, "run-test")
	if !ok {
		t.Fatal("triage stage did not compile")
	}

	if len(env.Egress.Hosts) != 1 || env.Egress.Hosts[0] != "127.0.0.1" {
		t.Errorf("egress = %v, want loopback only", env.Egress.Hosts)
	}
	if env.Egress.Allowed("api.anthropic.com") || env.Egress.Allowed("api.openai.com") {
		t.Error("a stage bound to local models must not reach a vendor")
	}

	for _, c := range env.Grants.List() {
		if strings.HasPrefix(string(c), "secret:") {
			t.Errorf("unexpected secret grant %q: a local server needs no credential", c)
		}
	}
	for _, want := range []security.Capability{
		security.ModelCap("local-fast"), security.ModelCap("local-deep"),
	} {
		if !env.Grants.Has(want) {
			t.Errorf("missing %q; the ladder's models must both be granted", want)
		}
	}
}

// TestRunCostsNothing checks the number that makes the local case different,
// and that the tokens behind it are still counted — a run report that said
// nothing at all would be no more useful than a bill.
func TestRunCostsNothing(t *testing.T) {
	reg, _, _ := wire(t, 0)

	res, err := loom.Run(context.Background(), desk(),
		loom.WithRegistry(reg),
		loom.WithWorkers(8),
		// A budget so small that any nonzero cost would stop the run.
		loom.WithRunBudget(core.Budget{MaxCostUSD: 0.0001}),
	)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	total := res.Report.Totals()
	if total.CostUSD != 0 {
		t.Errorf("cost = $%.6f, want zero", total.CostUSD)
	}
	if total.TotalTokens() == 0 {
		t.Error("tokens went uncounted; free is not the same as unmeasured")
	}
	if total.CacheReadTokens == 0 {
		t.Error("the shared rubric should have been served from the KV cache")
	}
	if total.CacheWriteTokens != 0 {
		t.Errorf("CacheWriteTokens = %d; a local KV cache is never a write to pay for", total.CacheWriteTokens)
	}
}

// TestHostedProjectionCosts checks the comparison the summary prints: the
// same pipeline priced as though somebody else owned the hardware, computed
// without a key, a socket, or a call.
func TestHostedProjectionCosts(t *testing.T) {
	reg, fastSrv, _ := wire(t, 0)

	proj, ok := hostedProjection(desk(), reg)
	if !ok {
		t.Fatal("projecting hosted cost failed")
	}
	if proj.Expected().CostUSD <= 0 {
		t.Errorf("expected cost = $%.4f, want a positive number to compare against", proj.Expected().CostUSD)
	}
	if proj.Ceiling().CostUSD < proj.Expected().CostUSD {
		t.Error("the ceiling must not sit below the expectation")
	}
	if proj.Partial() {
		t.Errorf("the stage sample should make the projection exact: %v", proj.Warnings)
	}
	if fastSrv.Calls() != 0 {
		t.Errorf("Explain made %d model call(s); it must make none", fastSrv.Calls())
	}
}
