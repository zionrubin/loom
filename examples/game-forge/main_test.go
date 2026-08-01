package main

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
)

// forgeOffline runs the three pipelines main() runs, on the scripted studio
// with the simulated latency removed. It is the whole demo minus the browser.
type forged struct {
	design, build, ship *loom.RunResult
	state               *forge
	specs, modules      []core.Record
	html                string
}

func runForge(t *testing.T, opts ...loom.Option) forged {
	t.Helper()
	reg, err := mockStudioPaced(0)
	if err != nil {
		t.Fatal(err)
	}
	lu, err := lineupOf(reg)
	if err != nil {
		t.Fatal(err)
	}

	st := &forge{modules: map[string]*moduleBuild{}, taskOf: map[string]string{}}
	base := append([]loom.Option{
		loom.WithRegistry(reg),
		loom.WithWorkers(6),
		loom.WithContinueOnError(),
		loom.WithEventHandler(st.handle),
		loom.WithBroadcast("engine-contract", engineContract),
		loom.WithBroadcast("art-direction", artDirection),
		loom.WithBroadcast("module-graph", moduleGraph),
	}, opts...)

	ctx := context.Background()
	design, err := loom.Run(ctx, buildDesign(defaultPitch, lu), base...)
	if err != nil {
		t.Fatalf("design run: %v", err)
	}
	specs := design.StageOutputs["seal"]
	if len(specs) == 0 {
		t.Fatal("design run produced no specifications")
	}

	build, err := loom.Run(ctx, buildBuild(specs, lu), base...)
	if err != nil {
		t.Fatalf("build run: %v", err)
	}
	modules := st.attribute(build.StageOutputs["lint"], build.StageOutputs["review"])

	manifest := func() map[string]any {
		return st.manifest("mock", []map[string]any{
			runStats("game-design", design.Report),
			runStats("game-build", build.Report),
		})
	}
	ship, err := loom.Run(ctx, buildShip(modules, lu, manifest), base...)
	if err != nil {
		t.Fatalf("ship run: %v", err)
	}
	if len(ship.Output) != 1 {
		t.Fatalf("ship run produced %d artifacts, want 1", len(ship.Output))
	}
	return forged{design: design, build: build, ship: ship, state: st,
		specs: specs, modules: modules, html: ship.Output[0].String("html")}
}

// TestForgeProducesAPlayableBundle is the claim the example makes: twelve
// modules written by twelve isolated tasks, linked by a table none of them
// carried, come out as one self-contained page that boots.
func TestForgeProducesAPlayableBundle(t *testing.T) {
	f := runForge(t)

	if got, want := len(f.modules), len(coreModuleIDs()); got != want {
		t.Fatalf("built %d modules, want %d", got, want)
	}
	for _, id := range coreModuleIDs() {
		if !strings.Contains(f.html, "G."+id+" =") {
			t.Errorf("bundle never assigns G.%s", id)
		}
	}
	if !strings.Contains(f.html, "window.LOOM.game.boot(") {
		t.Error("bundle has nothing to boot")
	}
	if i := strings.Index(f.html, "__LOOM_"); i >= 0 {
		t.Errorf("shell placeholder left unsubstituted: %.40s", f.html[i:])
	}
	// main stamps the ship run's own totals in through this sentinel; if the
	// text drifts, the artifact silently loses a third of its provenance.
	if !strings.Contains(f.html, "var ship = null; /* loom:ship */") {
		t.Error("the post-run stamp sentinel is missing from the emitted page")
	}

	// Link order is a property of the engine, not of any module: the bundle
	// must be concatenated the way the module graph says.
	last := -1
	for _, m := range coreModules() {
		at := strings.Index(f.html, "G."+m.ID+" =")
		if at < last {
			t.Errorf("module %s is linked out of dependency order", m.ID)
		}
		last = at
	}

	// The contract's own forbidden list, enforced against the shipped bundle.
	bundle := f.ship.StageOutputs["weave"][0].String("code")
	for _, bad := range forbiddenAPIs() {
		if strings.Contains(bundle, bad) {
			t.Errorf("bundle uses a forbidden API: %s", bad)
		}
	}
	if !balanced(bundle) {
		t.Error("bundle has unbalanced brackets")
	}
}

// TestForgeCutsInfeasibleModules covers the cheapest decision in the forge: a
// module that needs a capability the engine contract does not grant is dropped
// by a pure Go filter, before any token is spent specifying or implementing it.
func TestForgeCutsInfeasibleModules(t *testing.T) {
	f := runForge(t)

	proposed := proposedModules(f.design)
	if proposed != len(coreModuleIDs())+1 {
		t.Fatalf("the design proposed %d modules, want %d (the core set plus one extra)",
			proposed, len(coreModuleIDs())+1)
	}
	if got := len(f.design.StageOutputs["feasible"]); got != len(coreModuleIDs()) {
		t.Errorf("%d modules survived the feasibility filter, want %d", got, len(coreModuleIDs()))
	}
	for _, r := range f.design.StageOutputs["spec"] {
		if r.String("id") == "netplay" {
			t.Error("netplay needs network egress the contract does not grant, but was specified anyway")
		}
	}
}

// TestForgeEscalatesTruncatedModule pins the recovery path that matters most
// for code generation: a response that parses as text but is not a module is a
// semantic failure, so the task climbs the ladder instead of retrying a model
// that already failed at it.
func TestForgeEscalatesTruncatedModule(t *testing.T) {
	f := runForge(t)

	got := f.state.modules["mod-shards"]
	if got == nil {
		t.Fatal("no build record for the shards module")
	}
	if !got.Escalated {
		t.Errorf("shards came back truncated first; expected an escalation, got %d attempt(s) on %s",
			got.Attempts, got.Model)
	}
	if got.Model != "studio-master" {
		t.Errorf("shards was written by %s, want the deep model after escalation", got.Model)
	}
	for _, r := range f.modules {
		if r.String("lint") != "clean" {
			t.Errorf("module %s shipped with lint findings: %s", r.String("id"), r.String("lint"))
		}
	}
}

// TestForgeIsolatesADeadLetter checks that a failure off the critical path
// costs the build nothing: the review of one module is scripted to be refused,
// and the artifact ships anyway with every module in it.
func TestForgeIsolatesADeadLetter(t *testing.T) {
	f := runForge(t)

	if len(f.build.Failures) != 1 {
		t.Fatalf("expected exactly one dead letter in the build run, got %d", len(f.build.Failures))
	}
	if got := len(f.build.StageOutputs["review"]); got != len(coreModuleIDs())-1 {
		t.Errorf("%d reviews completed, want %d", got, len(coreModuleIDs())-1)
	}
	if got := len(f.modules); got != len(coreModuleIDs()) {
		t.Errorf("a failed review cost the build %d module(s)", len(coreModuleIDs())-got)
	}
}

// TestForgeManifestDescribesTheBuild checks the artifact carries its own
// provenance: the game's [P] screen reads this manifest, so every module has to
// name the model that wrote it and what that cost.
func TestForgeManifestDescribesTheBuild(t *testing.T) {
	f := runForge(t)

	blob := between(f.html, "window.LOOM_MANIFEST = ", "\nwindow.LOOM_CARD")
	var man struct {
		Runs    []map[string]any `json:"runs"`
		Tasks   int              `json:"tasks"`
		Calls   int              `json:"calls"`
		Cost    float64          `json:"cost"`
		Modules []moduleBuild    `json:"modules"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(strings.TrimSpace(blob), ";")), &man); err != nil {
		t.Fatalf("manifest is not valid JSON: %v", err)
	}
	if len(man.Runs) != 2 {
		t.Errorf("manifest carries %d runs; the ship run stamps itself in later, so want 2", len(man.Runs))
	}
	if man.Tasks == 0 || man.Calls == 0 || man.Cost <= 0 {
		t.Errorf("manifest reports no work: %d tasks, %d calls, $%f", man.Tasks, man.Calls, man.Cost)
	}
	if len(man.Modules) != len(coreModuleIDs()) {
		t.Fatalf("manifest describes %d modules, want %d", len(man.Modules), len(coreModuleIDs()))
	}
	for _, m := range man.Modules {
		if m.Model == "" || m.Bytes == 0 || m.Tokens == 0 {
			t.Errorf("module %s has no provenance: %+v", m.ID, m)
		}
	}
}

// TestForgeReplaysFromCache is the cache-as-checkpoint claim applied to a
// build: rerunning the same forge against the same state dir re-derives the
// same artifact without paying for any work that already succeeded.
//
// The one call that does repeat is the scripted refusal — a failure is not a
// result, so nothing is cached for it and the task is attempted again. That is
// also why nothing run-specific may reach a cached stage's input: the ship
// run's bundle has to come out byte-identical for its own cache to hold.
func TestForgeReplaysFromCache(t *testing.T) {
	dir := t.TempDir()

	// Event handlers run on worker goroutines, so the counter is guarded.
	var mu sync.Mutex
	byStage := map[string]int{}
	count := func(e observe.Event) {
		if e.Type != observe.ModelCalled {
			return
		}
		mu.Lock()
		byStage[e.Stage]++
		mu.Unlock()
	}
	first := runForge(t, loom.WithStateDir(dir), loom.WithEventHandler(count))
	if len(byStage) == 0 {
		t.Fatal("the first forge made no model calls")
	}

	byStage = map[string]int{}
	second := runForge(t, loom.WithStateDir(dir), loom.WithEventHandler(count))
	for stage, n := range byStage {
		if stage == "review" && n == 1 {
			continue // the dead letter, retried because it was never cached
		}
		t.Errorf("the second forge spent %d model call(s) in %q on work already done", n, stage)
	}
	firstBundle := first.ship.StageOutputs["weave"][0].String("code")
	secondBundle := second.ship.StageOutputs["weave"][0].String("code")
	if firstBundle != secondBundle {
		t.Error("the replayed forge produced a different bundle")
	}
	// Records make a JSON round trip through the store, so a replayed record's
	// numbers come back as float64. Reading them as int would silently report a
	// fully cached build as zero bytes.
	if got, want := totalBytes(second.modules), totalBytes(first.modules); got != want {
		t.Errorf("replayed build measures %d bytes of JavaScript, want %d", got, want)
	}
}

func between(s, after, until string) string {
	if i := strings.Index(s, after); i >= 0 {
		s = s[i+len(after):]
	}
	if i := strings.Index(s, until); i >= 0 {
		s = s[:i]
	}
	return s
}
