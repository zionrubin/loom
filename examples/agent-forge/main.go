// Command agent-forge reads a folder of conversations and designs the agentic
// system that work implies — how many agents, split along which axis, and what
// each one has to remember.
//
// Three runs, one constellation:
//
//	work-census    load-days ─ day-census ─ census-line ─┬─ only-<space> ─ profile-<space>
//	                                                     └─ …
//	agent-roster   label-catalog ─ capability-map ─ score ─ roster
//	agent-design   agents ─ agent-spec ─ spec-json ─ agent-charter
//
// The map half reads every day independently and cheaply. The reduce half is a
// second run because a loom DAG fans out and never fans back in, and the shape
// of an org is a question about all of it at once. Between the two, plain Go
// counts what can be counted — how far each capability spreads across spaces,
// how tangled each space is, what each job needs to recall — so the model is
// asked to name and defend a shape rather than to invent one from vibes.
//
// Offline by default: three scripted models stand in for a provider, so this
// runs with no API key against the small corpus bundled beside it.
//
//	go run ./examples/agent-forge
//	go run ./examples/agent-forge -messages ~/exports/chat -provider openai -budget 20
//
// Nothing identifying leaves the machine. Senders and @mentions become stable
// pseudonyms and contact details become placeholders at load, before any record
// exists.
package main

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/viz"
)

func main() {
	messages := flag.String("messages", defaultCorpus(), "folder of conversations: <root>/<space>/<YYYY-MM-DD>.jsonl")
	out := flag.String("out", "blueprint", "output directory")
	provider := flag.String("provider", "mock", "mock | anthropic | openai")
	budget := flag.Float64("budget", 12, "run budget in USD")
	workers := flag.Int("workers", 8, "concurrent model calls")
	rpm := flag.Int("rpm", 200, "per-model requests-per-minute limit (real providers)")
	since := flag.String("since", "", "only days on or after YYYY-MM-DD")
	until := flag.String("until", "", "only days on or before YYYY-MM-DD")
	last := flag.Int("last", 0, "keep only the last N days of each space")
	state := flag.String("state", os.Getenv("LOOM_STATE"), "state directory for cache and resume")
	rosterPath := flag.String("roster", "", "reload a frozen roster.json instead of deciding again")
	addr := flag.String("addr", "localhost:8077", "constellation view address (empty disables)")
	ui := flag.String("ui", "localhost:8078", "blueprint UI address (empty disables)")
	salt := flag.String("salt", "", "pseudonym salt (default: derived from the corpus path)")
	flag.Parse()

	if err := run(runConfig{
		messages: *messages, out: *out, provider: *provider, budget: *budget,
		workers: *workers, rpm: *rpm, since: *since, until: *until, last: *last,
		state: *state, rosterPath: *rosterPath, addr: *addr, ui: *ui, salt: *salt,
	}); err != nil {
		log.Fatal(err)
	}
}

type runConfig struct {
	messages, out, provider         string
	budget                          float64
	workers, rpm, last              int
	since, until, state, rosterPath string
	addr, ui, salt                  string
}

func defaultCorpus() string {
	if _, err := os.Stat("examples/agent-forge/corpus"); err == nil {
		return "examples/agent-forge/corpus"
	}
	if exe, err := os.Executable(); err == nil {
		if p := filepath.Join(filepath.Dir(exe), "corpus"); dirExists(p) {
			return p
		}
	}
	return "corpus"
}

func dirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

func run(cfg runConfig) error {
	reg, secrets, err := buildRegistry(cfg.provider, cfg.rpm)
	if err != nil {
		return err
	}
	lu, err := lineupOf(reg)
	if err != nil {
		return err
	}

	files, spaces, err := discover(cfg.messages, cfg.since, cfg.until, cfg.last)
	if err != nil {
		return err
	}
	fmt.Printf("corpus:  %s — %d spaces, %d day files\n", cfg.messages, len(spaces), len(files))
	fmt.Printf("studio:  %s (%s → %s → %s)\n", cfg.provider, lu.fast, lu.balanced, lu.deep)

	var v *viz.Server
	if cfg.addr != "" {
		v = viz.New()
		url, err := v.Start(cfg.addr)
		if err != nil {
			return err
		}
		defer v.Close()
		fmt.Printf("live:    %s\n", url)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if v != nil {
		v.AwaitViewer(ctx)
	}

	tr := &tracker{}
	opts := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithWorkers(cfg.workers),
		loom.WithContinueOnError(),
		loom.WithRunBudget(core.Budget{MaxCostUSD: cfg.budget}),
		loom.WithEventHandler(tr.handler(v)),
	}
	if len(secrets) > 0 {
		opts = append(opts, loom.WithSecrets(secrets))
	}
	if cfg.state != "" {
		opts = append(opts, loom.WithStateDir(cfg.state))
	}

	salt := cfg.salt
	if salt == "" {
		salt = "agent-forge|" + cfg.messages
	}
	scrub := newScrubber(salt)

	// ---- run 1: what work happens, day by day ----------------------------
	fmt.Printf("\n[1/3] reading %d days …\n", len(files))
	censusRun, err := loom.Run(ctx, buildCensusPipeline(files, spaces, lu, scrub, cfg.workers, tr.loaded), opts...)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("census run: %w", err)
	}
	if censusRun == nil {
		return fmt.Errorf("census run produced nothing")
	}
	obs, systems := collectJobs(censusRun.StageOutputs["day-census"])
	profiles := map[string]string{}
	for _, s := range spaces {
		if recs := censusRun.StageOutputs["profile-"+s]; len(recs) > 0 {
			profiles[s] = recs[0].String("output")
		}
	}
	fmt.Printf("      %d job observations, %d spaces profiled\n", len(obs), len(profiles))
	if len(obs) == 0 {
		return fmt.Errorf("no jobs extracted — check the corpus format and the model output")
	}

	// ---- run 2: fold, count, decide --------------------------------------
	fmt.Printf("[2/3] consolidating %d distinct job labels and scoring …\n", len(dedupeLabels(obs)))
	rosterRun, err := loom.Run(ctx, buildRosterPipeline(obs, spaces, systems, lu), opts...)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("roster run: %w", err)
	}

	var cen census
	if rosterRun != nil {
		if recs := rosterRun.StageOutputs["score"]; len(recs) > 0 {
			if err := json.Unmarshal([]byte(recs[0].String("census_json")), &cen); err != nil {
				return fmt.Errorf("decode census: %w", err)
			}
		}
	}
	if cen.Observations == 0 {
		// The taxonomy stage did not survive; score the raw labels instead so
		// the run still produces a design rather than an error.
		fmt.Println("      taxonomy unavailable — scoring raw labels")
		cen = buildCensus(obs, nil, spaces)
	}

	decision := rosterDecision{}
	if cfg.rosterPath != "" {
		if err := loadJSON(cfg.rosterPath, &decision); err != nil {
			return fmt.Errorf("load roster: %w", err)
		}
		fmt.Printf("      roster reloaded from %s (%d agents)\n", cfg.rosterPath, len(decision.Agents))
		decision = rosterFrom(recordOf(decision), cen)
	} else if rosterRun != nil && len(rosterRun.StageOutputs["roster"]) > 0 {
		decision = rosterFrom(rosterRun.StageOutputs["roster"][0], cen)
	} else {
		decision = rosterFrom(core.Record{}, cen)
	}
	fmt.Printf("      shape: %s — %d agents\n", strings.ToUpper(decision.Topology), len(decision.Agents))
	for _, a := range decision.Agents {
		fmt.Printf("        %-18s %-26s remembers %s\n", a.ID, a.Partition.String(), axisList(a.Remembers))
	}

	// ---- run 3: design each agent ----------------------------------------
	fmt.Printf("[3/3] specifying %d agents …\n", len(decision.Agents))
	designRun, err := loom.Run(ctx, buildDesignPipeline(decision, cen, profiles, lu, cfg.workers), opts...)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("design run: %w", err)
	}

	specs := map[string]core.Record{}
	charters := map[string]core.Record{}
	if designRun != nil {
		for _, r := range designRun.StageOutputs["agent-spec"] {
			specs[r.String("agent_id")] = r
		}
		for _, r := range designRun.StageOutputs["agent-charter"] {
			charters[r.String("agent_id")] = r
		}
	}
	docs := make([]agentDoc, 0, len(decision.Agents))
	for _, a := range decision.Agents {
		doc := agentDoc{agentDecl: a}
		if r, ok := specs[a.ID]; ok {
			doc.Spec = pick(r.Data, "mission", "scope", "memory", "tools", "triggers", "outputs",
				"handoffs", "guardrails", "evals", "external_lookups", "risks")
		}
		if r, ok := charters[a.ID]; ok {
			doc.Charter = pick(r.Data, "memory_schema", "system_prompt", "first_week")
		}
		docs = append(docs, doc)
	}

	// ---- write ------------------------------------------------------------
	all := []*loom.RunResult{censusRun, rosterRun, designRun}
	rows, spent := make([]map[string]any, 0, 3), 0.0
	for i, name := range []string{"work-census", "agent-roster", "agent-design"} {
		if all[i] == nil {
			continue
		}
		rows = append(rows, runStats(name, all[i].Report))
		spent += all[i].Report.Totals().CostUSD
	}
	bp := blueprint{
		Generated: nowStamp(), Source: cfg.messages, Provider: cfg.provider,
		Census: cen, Roster: decision, Agents: docs, Profiles: profiles,
		Systems: systems, Runs: rows, CostUSD: spent,
	}
	if err := writeOutputs(cfg.out, bp, cen, all); err != nil {
		return err
	}

	fmt.Printf("\nspent:   $%.4f\n", bp.CostUSD)
	fmt.Printf("written: %s/DESIGN.md, %s/blueprint.json, %s/agents/, %s/index.html\n", cfg.out, cfg.out, cfg.out, cfg.out)

	if cfg.ui != "" {
		srv := &http.Server{Addr: cfg.ui, Handler: http.FileServer(http.Dir(cfg.out))}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("ui: %v", err)
			}
		}()
		fmt.Printf("\nblueprint: http://%s   (ctrl-c to stop)\n", cfg.ui)
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}
	return nil
}

func writeOutputs(dir string, bp blueprint, cen census, runs []*loom.RunResult) error {
	if err := os.MkdirAll(filepath.Join(dir, "agents"), 0o755); err != nil {
		return err
	}
	write := func(name, body string) error {
		return os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644)
	}

	if err := write("DESIGN.md", renderDesign(bp)); err != nil {
		return err
	}
	for _, a := range bp.Agents {
		body := fmt.Sprintf("# %s\n\n_from %s, %s_\n\n%s", a.Name, bp.Source, bp.Generated, renderAgent(a, cen))
		if err := write(filepath.Join("agents", a.ID+".md"), body); err != nil {
			return err
		}
	}
	if err := writeJSON(filepath.Join(dir, "blueprint.json"), bp); err != nil {
		return err
	}
	// Frozen intermediates: both are hand-editable and reloadable, which is how
	// you correct a taxonomy or a roster without paying for the whole run again.
	if err := writeJSON(filepath.Join(dir, "capabilities.json"), capsOf(cen)); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "roster.json"), bp.Roster); err != nil {
		return err
	}
	if err := write("index.html", renderUI(bp)); err != nil {
		return err
	}
	return write("run-report.txt", runReport(runs))
}

func capsOf(c census) map[string]any {
	caps := make([]capability, 0, len(c.Capabilities))
	for _, cs := range c.Capabilities {
		caps = append(caps, cs.Cap)
	}
	return map[string]any{"capabilities": caps}
}

func writeJSON(path string, v any) error {
	blob, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(blob, '\n'), 0o644)
}

func loadJSON(path string, v any) error {
	blob, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(blob, v)
}

// recordOf round-trips a decision through a record so a hand-edited roster.json
// goes through exactly the same normalisation as model output.
func recordOf(d rosterDecision) core.Record {
	blob, err := json.Marshal(d)
	if err != nil {
		return core.Record{}
	}
	data := map[string]any{}
	if err := json.Unmarshal(blob, &data); err != nil {
		return core.Record{}
	}
	return core.NewRecord("roster", data)
}

func pick(m map[string]any, keys ...string) map[string]any {
	out := map[string]any{}
	for _, k := range keys {
		if v, ok := m[k]; ok {
			out[k] = v
		}
	}
	return out
}

func runReport(runs []*loom.RunResult) string {
	var w strings.Builder
	for _, r := range runs {
		if r == nil {
			continue
		}
		u := r.Report.Totals()
		fmt.Fprintf(&w, "== %s  %s  $%.4f  %d in / %d out tokens\n",
			r.Report.RunID, r.Report.Finished.Sub(r.Report.Started).Round(time.Millisecond),
			u.CostUSD, u.InputTokens, u.OutputTokens)
		stages := append([]*observe.StageStats(nil), r.Report.Stages...)
		sort.Slice(stages, func(i, j int) bool { return stages[i].Stage < stages[j].Stage })
		for _, s := range stages {
			fmt.Fprintf(&w, "   %-28s tasks=%-4d calls=%-4d cache=%-4d retries=%-3d $%.4f\n",
				s.Stage, s.Tasks, s.ModelCalls, s.CacheHits, s.Retries, s.Usage.CostUSD)
		}
		if len(r.Failures) > 0 {
			fmt.Fprintf(&w, "   %d failures\n", len(r.Failures))
			for i, f := range r.Failures {
				if i >= 10 {
					fmt.Fprintf(&w, "   … and %d more\n", len(r.Failures)-i)
					break
				}
				fmt.Fprintf(&w, "   ! %v\n", f)
			}
		}
		w.WriteString("\n")
	}
	return w.String()
}

// tracker keeps the console line honest and totals the spend. Event handlers
// run on worker goroutines, so everything it touches is behind the mutex.
type tracker struct {
	mu       sync.Mutex
	spent    float64
	calls    int
	hits     int
	lastLine time.Time
}

func (t *tracker) handler(v *viz.Server) func(observe.Event) {
	return func(e observe.Event) {
		t.mu.Lock()
		switch e.Type {
		case observe.ModelCalled:
			t.calls++
			t.spent += e.Usage.CostUSD
		case observe.CacheHit:
			t.hits++
		}
		show := time.Since(t.lastLine) > 400*time.Millisecond
		if show {
			t.lastLine = time.Now()
		}
		calls, hits, spent := t.calls, t.hits, t.spent
		t.mu.Unlock()

		if show {
			fmt.Printf("\r      %d calls, %d cache hits, $%.4f          ", calls, hits, spent)
		}
		if v != nil {
			v.Handle(e)
		}
	}
}

func (t *tracker) loaded(n, total int) {
	if n%25 != 0 && n != total {
		return
	}
	fmt.Printf("\r      loaded %d/%d days          ", n, total)
}

func (t *tracker) cost() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.spent
}
