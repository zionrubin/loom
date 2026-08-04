// Command game-forge builds a playable web game the way you would build any
// other AI workload in Loom: as pipelines. Three runs — plan, build, ship — a
// separate task per module, a least-privilege envelope per task, a dollar cap
// per run, and one self-contained HTML file at the end that you can open and
// play.
//
//	go run ./examples/game-forge     # constellation view on :8077, game on :8078
//
// Offline by default: three scripted mock models stand in for a provider, so
// this costs nothing and needs no key, and the game at the end is real. Point
// it at a real provider and the same prompts, the same contract, and the same
// validation produce a different game:
//
//	ANTHROPIC_API_KEY=sk-... go run ./examples/game-forge -provider anthropic
//	OPENAI_API_KEY=sk-...    go run ./examples/game-forge -provider openai
//
//	# cache = checkpoint: rerun and the whole forge replays for $0
//	LOOM_STATE=/tmp/loom-forge go run ./examples/game-forge
//	LOOM_STATE=/tmp/loom-forge go run ./examples/game-forge
//
// The three runs publish to one constellation view, which holds them as one
// universe: press `u` for the overview, `,`/`.` to step between the design,
// build, and ship skies — each stays whole and inspectable after the next has
// started.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/providers/anthropic"
	"github.com/zionrubin/loom/providers/openai"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/viz"
)

const defaultPitch = "A neon vector arcade game in one canvas: fly a shuttle through drifting " +
	"shards, cut them into smaller ones, collect the motes they shed, and spend a full charge on " +
	"a pulse that clears the field. Sixty seconds to learn, no assets, no network."

func main() {
	addr := flag.String("addr", "localhost:8077", "constellation view address (empty to disable)")
	play := flag.String("play", "localhost:8078", "address to serve the finished game on (empty to disable)")
	outDir := flag.String("out", "dist", "directory for the emitted game and reports")
	provider := flag.String("provider", "mock", "mock | anthropic | openai")
	pitch := flag.String("pitch", defaultPitch, "the brief the forge starts from")
	budget := flag.Float64("budget", 5.00, "hard cost cap per run, in USD")
	workers := flag.Int("workers", 6, "concurrent workers")
	rpm := flag.Int("rpm", 120, "per-model requests-per-minute admission limit (real providers)")
	wait := flag.Duration("wait", 45*time.Second, "how long to wait for a browser before starting anyway")
	state := flag.String("state", os.Getenv("LOOM_STATE"), "state dir: cache = checkpoint = resume")
	flag.Parse()

	reg, secrets, err := buildRegistry(*provider, *rpm)
	if err != nil {
		log.Fatal(err)
	}
	lu, err := lineupOf(reg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("studio: %s (%s → %s → %s)\n", *provider, lu.fast, lu.balanced, lu.deep)

	// One view, three runs, one universe.
	var v *viz.Server
	var vizURL string
	if *addr != "" {
		v = viz.New()
		if vizURL, err = v.Start(*addr); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("constellation view: %s\n", vizURL)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	st := &forge{modules: map[string]*moduleBuild{}, taskOf: map[string]string{}}
	handle := st.handle
	if v != nil {
		handle = func(e observe.Event) {
			v.Handle(e)
			st.handle(e)
		}
	}

	base := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithWorkers(*workers),
		loom.WithContinueOnError(),
		loom.WithRunBudget(core.Budget{MaxCostUSD: *budget}),
		loom.WithEventHandler(handle),
		// Registered once for the whole forge; every stage that reads one
		// declares it, and reads are grant-checked and audited.
		loom.WithBroadcast("engine-contract", engineContract),
		loom.WithBroadcast("art-direction", artDirection),
		loom.WithBroadcast("module-graph", moduleGraph),
	}
	if len(secrets) > 0 {
		base = append(base, loom.WithSecrets(secrets))
	}
	if *state != "" {
		base = append(base, loom.WithStateDir(*state))
		fmt.Printf("state dir: %s (completed work replays instead of re-spending)\n", *state)
	}

	// --- run 1: design ------------------------------------------------------
	design := buildDesign(*pitch, lu)

	// Project first, then wait for a browser. The forecast goes out on the same
	// handler the run will use, so it is already there when the page connects:
	// the empty sky opens with what the forge is about to cost instead of
	// "waiting for a run", which is the only window where knowing the price is
	// still actionable.
	explain(design, "design", append(base,
		// The fields the model introduces, named up front: the module
		// breakdown is what the next stage fans out over, so without this the
		// projection would show the whole forge as one task.
		loom.WithStageSample("concept", conceptSample()),
		loom.WithStageSample("spec", map[string]any{
			"api":    "update(w), draw(ctx,w)",
			"notes":  strings.Repeat("implementation guidance. ", 12),
			"accept": []any{"defines one namespace key", "no forbidden API"},
		}),
	)...)

	if v != nil {
		fmt.Printf("waiting up to %s for a browser (Ctrl-C to abort)…\n", *wait)
		waitCtx, cancel := context.WithTimeout(context.Background(), *wait)
		if v.AwaitViewer(waitCtx) {
			fmt.Println("viewer connected — opening the forge")
			time.Sleep(1500 * time.Millisecond) // a beat, so the forecast is read first
		} else {
			fmt.Println("no viewer yet — running anyway (the page replays state on connect)")
		}
		cancel()
	}

	res1, err := loom.Run(ctx, design, base...)
	if res1 == nil {
		log.Fatalf("design run failed before producing anything: %v", err)
	}
	if err != nil {
		fmt.Printf("design run ended early: %v (spent $%.4f)\n", err, res1.Spent.CostUSD)
	}
	specs := res1.StageOutputs["seal"]
	if len(specs) == 0 {
		log.Fatal("design run produced no module specifications; nothing to build")
	}
	// "modules" and "feasible" fuse into one task boundary, so the cut is the
	// difference between what the design proposed and what survived it.
	cut := proposedModules(res1) - len(res1.StageOutputs["feasible"])
	fmt.Printf("\ndesign: %d modules specified, %d cut as infeasible — %s\n",
		len(specs), cut, summarize(res1.Report))

	// --- run 2: build -------------------------------------------------------
	build := buildBuild(specs, lu)
	explain(build, "build", base...)

	res2, err := loom.Run(ctx, build, base...)
	if res2 == nil {
		log.Fatalf("build run failed before producing anything: %v", err)
	}
	if err != nil {
		fmt.Printf("build run ended early: %v (spent $%.4f)\n", err, res2.Spent.CostUSD)
	}
	built := st.attribute(res2.StageOutputs["lint"], res2.StageOutputs["review"])
	// A partial build is still progress when there is a state dir: what
	// completed is cached, so the next run replays it for free and spends the
	// budget on what is left.
	resume := "raise -budget"
	if *state != "" {
		resume = "run it again — everything already built replays from " + *state + " for $0"
	} else {
		resume += ", or set -state so a second run resumes instead of restarting"
	}
	if len(built) == 0 {
		log.Fatalf("the build run wrote no modules under a $%.4f budget; %s", *budget, resume)
	}
	if !hasModule(built, "game") {
		log.Fatalf("%d of %d modules built, but not %q, so the shell has nothing to boot; %s",
			len(built), len(specs), "game", resume)
	}
	fmt.Printf("build: %d modules, %d bytes of JavaScript — %s\n",
		len(built), totalBytes(built), summarize(res2.Report))
	for _, f := range res2.Failures {
		fmt.Printf("  dead letter: %s (%s): %v\n", f.Task.ID, f.Class, f.Err)
	}

	// --- run 3: ship --------------------------------------------------------
	manifest := func() map[string]any {
		return st.manifest(*provider, []map[string]any{
			runStats("game-design", res1.Report),
			runStats("game-build", res2.Report),
		})
	}
	ship := buildShip(built, lu, manifest)
	explain(ship, "ship", append(base, loom.WithStageSample("title-card", map[string]any{
		"title": "CONSTELLATION DRIFT", "tagline": "cut the dark, weave the light",
		"howto": []any{"turn, thrust, fire", "Z for the pulse", "collect motes"},
	}))...)

	res3, err := loom.Run(ctx, ship, base...)
	if res3 == nil || len(res3.Output) == 0 {
		log.Fatalf("ship run produced no artifact: %v", err)
	}
	html := res3.Output[0].String("html")
	// The one thing the ship run could not know: its own totals. Stamp them in.
	if shipStats, err := json.Marshal(runStats("game-ship", res3.Report)); err == nil {
		html = strings.Replace(html,
			"var ship = null; /* loom:ship */",
			"var ship = "+string(shipStats)+"; /* loom:ship */", 1)
	}
	fmt.Printf("ship: %s — %s\n", res3.Output[0].String("title"), summarize(res3.Report))

	// --- the artifact -------------------------------------------------------
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	gamePath := filepath.Join(*outDir, "index.html")
	files := map[string]string{
		gamePath: html,
		filepath.Join(*outDir, "DESIGN.md"): "# " + res3.Output[0].String("title") + " — design\n\n" +
			outputOf(res1, "design-doc") + "\n",
		filepath.Join(*outDir, "BUILD.md"): "# build log\n\n" + outputOf(res2, "build-notes") + "\n\n" +
			reviewTable(st) + "\n",
		filepath.Join(*outDir, "run-report.txt"): res1.Report.String() + "\n" +
			res2.Report.String() + "\n" + res3.Report.String(),
	}
	for path, body := range files {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			log.Fatal(err)
		}
	}

	total := res1.Spent
	total.Add(res2.Spent)
	total.Add(res3.Spent)
	fmt.Printf("\n--- forged ---\n%s (%d bytes, %d modules, no assets, no requests)\n",
		gamePath, len(html), len(built))
	for name := range files {
		if name != gamePath {
			fmt.Printf("%s\n", name)
		}
	}
	fmt.Printf("3 runs · %d model calls · %d tokens · $%.4f total\n",
		total.Requests, total.TotalTokens(), total.CostUSD)

	// --- play it ------------------------------------------------------------
	if *play != "" {
		srv := &http.Server{Addr: *play, Handler: http.FileServer(http.Dir(*outDir))}
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Printf("game server: %v\n", err)
			}
		}()
		fmt.Printf("\n▶ play it: http://%s\n", *play)
		defer func() { _ = srv.Close() }()
	}
	if v != nil {
		fmt.Printf("✦ the forge that built it: %s\n"+
			"  press `u` for the universe — the design, build, and ship runs side by side\n", vizURL)
	}
	if *play == "" && v == nil {
		return // nothing left to serve: the artifact is on disk
	}
	fmt.Println("  (Ctrl-C to exit)")
	<-ctx.Done()
	if v != nil {
		_ = v.Close()
	}
}

// buildRegistry wires the studio: three scripted mock models, or a real
// provider's fast/balanced/deep tiers.
func buildRegistry(provider string, rpm int) (*model.Registry, map[security.SecretRef]string, error) {
	switch provider {
	case "mock", "":
		reg, err := mockStudio()
		return reg, nil, err
	case "anthropic":
		key := os.Getenv("ANTHROPIC_API_KEY")
		if key == "" {
			return nil, nil, fmt.Errorf("set ANTHROPIC_API_KEY for -provider anthropic")
		}
		reg := model.NewRegistry()
		if err := anthropic.RegisterDefaults(reg, model.Limits{RequestsPerMinute: rpm}); err != nil {
			return nil, nil, err
		}
		return reg, map[security.SecretRef]string{anthropic.DefaultSecretRef: key}, nil
	case "openai":
		key := os.Getenv("OPENAI_API_KEY")
		if key == "" {
			return nil, nil, fmt.Errorf("set OPENAI_API_KEY for -provider openai")
		}
		reg := model.NewRegistry()
		if err := openai.RegisterDefaults(reg, model.Limits{RequestsPerMinute: rpm}); err != nil {
			return nil, nil, err
		}
		return reg, map[security.SecretRef]string{openai.DefaultSecretRef: key}, nil
	}
	return nil, nil, fmt.Errorf("unknown provider %q (want mock, anthropic, or openai)", provider)
}

// explain projects a run before it happens, on the same event handler the run
// will use — so the constellation view holds the forecast while that sky is
// still empty and reads every stage against it as the run fills in.
func explain(p *pipeline.Pipeline, label string, opts ...loom.Option) {
	proj, err := loom.Explain(p, opts...)
	if err != nil {
		fmt.Printf("projection for the %s run failed: %v\n", label, err)
		return
	}
	fmt.Printf("\n%s\n", proj)
}

// --- build attribution ------------------------------------------------------

// moduleBuild is what the event stream says about one module: which model
// actually wrote it, what that cost, and whether it took an escalation to get
// there. It is the data the finished game shows on its own provenance screen.
type moduleBuild struct {
	ID        string  `json:"id"`
	Role      string  `json:"role"`
	Model     string  `json:"model"`
	Escalated bool    `json:"escalated"`
	Attempts  int     `json:"attempts"`
	Bytes     int     `json:"bytes"`
	Tokens    int     `json:"tokens"`
	Cost      float64 `json:"cost"`
	Lint      string  `json:"lint"`
	Verdict   string  `json:"verdict,omitempty"`
	Note      string  `json:"note,omitempty"`
}

// forge folds the event stream into per-module provenance. Handlers run on
// worker goroutines, so everything here is guarded.
type forge struct {
	mu      sync.Mutex
	taskOf  map[string]string // task ID → the module record it is writing
	modules map[string]*moduleBuild
}

func (f *forge) handle(e observe.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch e.Type {
	case observe.TaskScheduled:
		if e.Stage == "implement" && len(e.InputIDs) > 0 {
			f.taskOf[e.TaskID] = e.InputIDs[0]
		}
	case observe.ModelCalled:
		id, ok := f.taskOf[e.TaskID]
		if !ok {
			return
		}
		m := f.modules[id]
		if m == nil {
			m = &moduleBuild{ID: strings.TrimPrefix(id, "mod-")}
			f.modules[id] = m
		}
		if m.Model != "" && m.Model != e.Model {
			m.Escalated = true // the ladder was climbed to get this module
		}
		m.Model = e.Model
		m.Attempts++
		m.Tokens += e.Usage.TotalTokens()
		m.Cost += e.Usage.CostUSD
	}
}

// attribute joins what the build run produced with what the event stream saw,
// and returns the module records the ship run links.
func (f *forge) attribute(linted, reviewed []core.Record) []core.Record {
	f.mu.Lock()
	defer f.mu.Unlock()

	reviews := map[string]core.Record{}
	for _, r := range reviewed {
		reviews[r.ID] = r
	}

	out := make([]core.Record, 0, len(linted))
	for _, r := range linted {
		if strings.TrimSpace(r.String("code")) == "" {
			continue
		}
		m := f.modules[r.ID]
		if m == nil {
			// No model call for this module in this run: it replayed from the
			// cache, and the cache stores results, not the metadata of the call
			// that produced them. Saying so is more honest than guessing.
			m = &moduleBuild{ID: r.String("id"), Model: "replayed from cache"}
			f.modules[r.ID] = m
		}
		m.ID = r.String("id")
		m.Role = r.String("role")
		m.Lint = r.String("lint")
		m.Bytes = num(r.Data["bytes"])
		if rev, ok := reviews[r.ID]; ok {
			m.Verdict, m.Note = rev.String("verdict"), rev.String("note")
		}
		// Deliberately not stamped onto the record: which model wrote a module
		// is a fact about this run, and the ship run's stages are cached on
		// their inputs. It travels in the manifest instead.
		out = append(out, r.Clone())
	}
	return out
}

// manifest is the build record the artifact carries: what produced it, at what
// cost, module by module. The game draws it on its provenance screen.
func (f *forge) manifest(provider string, runs []map[string]any) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()

	mods := make([]*moduleBuild, 0, len(f.modules))
	for _, m := range f.modules {
		mods = append(mods, m)
	}
	sort.Slice(mods, func(i, j int) bool { return mods[i].ID < mods[j].ID })

	var tasks, calls, tokens int
	var cost float64
	for _, r := range runs {
		tasks += r["tasks"].(int)
		calls += r["calls"].(int)
		tokens += r["tokens"].(int)
		cost += r["cost"].(float64)
	}
	return map[string]any{
		"forged_at":  time.Now().UTC().Format(time.RFC3339),
		"forged_by":  "loom · github.com/zionrubin/loom",
		"studio":     provider,
		"runs":       runs,
		"runs_count": len(runs) + 1, // the ship run stamps itself in afterwards
		"tasks":      tasks,
		"calls":      calls,
		"tokens":     tokens,
		"cost":       cost,
		"modules":    mods,
	}
}

// --- reporting helpers ------------------------------------------------------

func runStats(name string, r observe.RunReport) map[string]any {
	var tasks, calls, retries, cached int
	for _, s := range r.Stages {
		tasks += s.Tasks
		calls += s.ModelCalls
		retries += s.Retries
		cached += s.CacheHits
	}
	u := r.Totals()
	return map[string]any{
		"name": name, "tasks": tasks, "calls": calls, "retries": retries,
		"cached": cached, "tokens": u.TotalTokens(), "cost": u.CostUSD,
		"seconds": float64(int(r.Duration().Seconds()*10)) / 10,
	}
}

func summarize(r observe.RunReport) string {
	s := runStats("", r)
	return fmt.Sprintf("%d tasks, %d calls, %d retries, %d cached, %d tokens, $%.4f in %.1fs",
		s["tasks"], s["calls"], s["retries"], s["cached"], s["tokens"], s["cost"], s["seconds"])
}

func outputOf(res *loom.RunResult, stage string) string {
	recs := res.StageOutputs[stage]
	if len(recs) == 0 {
		return "_(stage produced no output)_"
	}
	return recs[0].String("output")
}

func reviewTable(f *forge) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	mods := make([]*moduleBuild, 0, len(f.modules))
	for _, m := range f.modules {
		mods = append(mods, m)
	}
	sort.Slice(mods, func(i, j int) bool { return mods[i].ID < mods[j].ID })

	var b strings.Builder
	b.WriteString("## modules\n\n| module | written by | bytes | tokens | cost | lint | review |\n")
	b.WriteString("|---|---|---:|---:|---:|---|---|\n")
	for _, m := range mods {
		who := m.Model
		if m.Escalated {
			who += " (escalated)"
		}
		verdict := m.Verdict
		if verdict == "" {
			verdict = "—"
		} else if m.Note != "" {
			verdict += ": " + m.Note
		}
		fmt.Fprintf(&b, "| `%s` | %s | %d | %d | $%.4f | %s | %s |\n",
			m.ID, who, m.Bytes, m.Tokens, m.Cost, m.Lint, verdict)
	}
	return b.String()
}

// proposedModules is how many modules the design run's concept stage asked for,
// before the feasibility filter had its say.
func proposedModules(res *loom.RunResult) int {
	recs := res.StageOutputs["concept"]
	if len(recs) == 0 {
		return 0
	}
	mods, _ := recs[0].Data["modules"].([]any)
	return len(mods)
}

func hasModule(recs []core.Record, id string) bool {
	for _, r := range recs {
		if r.String("id") == id {
			return true
		}
	}
	return false
}

func totalBytes(recs []core.Record) int {
	var n int
	for _, r := range recs {
		n += num(r.Data["bytes"])
	}
	return n
}

// conceptSample names the fields the concept stage's model introduces, so the
// projection can fan out over them instead of guessing. It is the design's own
// module list — including the one that gets cut, which is the point: a
// projection that ignored the cut would over-count the build by a module.
func conceptSample() map[string]any {
	mods := make([]any, 0, len(moduleGraph)+1)
	for _, m := range coreModules() {
		mods = append(mods, map[string]any{
			"id": m.ID, "role": m.Role, "uses": toAny(m.Uses), "needs": []any{"canvas2d"},
		})
	}
	mods = append(mods, map[string]any{
		"id": "netplay", "role": "global leaderboard", "uses": []any{"hud"},
		"needs": []any{"network"},
	})
	return map[string]any{
		"title": "CONSTELLATION DRIFT", "tagline": "cut the dark, weave the light",
		"loop": "fly, cut, collect, pulse", "modules": mods,
	}
}

func toAny(xs []string) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}
