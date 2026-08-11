// Command commons-shared runs the shared research layer across *processes*:
// several independent executors, each a full Loom fleet with its own ledger,
// researching overlapping subjects against one commons.
//
// It is the distributed half of examples/commons. That one shows four analyst
// desks inside one process hitting a public source once per company instead of
// once per desk; this one shows four *executors* — separate programs, separate
// address spaces, the same thing a deployment of four pods is — doing the same,
// and reports how many external calls were avoided across the process boundary
// that the in-process layer structurally cannot see.
//
//	go run ./examples/commons-shared                     # 4 executors, file backend
//	go run ./examples/commons-shared -executors 8        # more of them
//	go run ./examples/commons-shared -dsn "$FINDINGS_DSN"   # PostgreSQL + pgvector
//
// The whole thing runs offline: a scripted "public source" with realistic
// latency, mock models, and a commons in a temporary directory. With -dsn it
// runs against a real PostgreSQL instead, and nothing else changes — which is
// the point of the backend being an interface.
//
// What to look for:
//
//   - **calls to the source**: N per subject without the shared commons — one
//     per executor, because each has its own ledger and none can see the others
//     — against one per subject with it.
//   - **who paid**: exactly one executor researches each subject and the rest
//     are served, so the ledger of avoided spend has an owner and a set of
//     beneficiaries rather than a total nobody can attribute.
//   - **leases**: the executors are started together, so most of the saving
//     comes from the distributed single-flight rather than from a warm
//     commons. Look at the leader/follower counts.
//   - **the answers**: byte-identical in both runs. A hit is substitutable for
//     the call it replaced or this is not a cache, it is a bug.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/findings"
	"github.com/zionrubin/loom/findings/filestore"
	"github.com/zionrubin/loom/findings/pgstore"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/security"
)

// subjects are the companies the executors research. Each executor covers a
// rotated window of them, so every subject is wanted by more than one executor
// and no executor's list is the same as another's — which is what a fleet of
// workers pulling from one queue actually looks like.
var subjects = []string{
	"northwind", "contoso", "fabrikam", "tailwind", "adventure-works", "litware",
}

// phrasings are the house styles. The same subject asked four ways is the case
// a content-addressed cache cannot help with, at any scale.
var phrasings = []string{
	"what are %s's revenue, headcount and outstanding litigation",
	"%s: earnings, staff count, legal exposure",
	"summarize legal and financial exposure for %s",
	"%s company profile including workforce size and revenue",
}

func main() {
	var (
		executor  = flag.Bool("executor", false, "run as one executor process (set by the parent)")
		name      = flag.String("name", "", "this executor's name")
		commons   = flag.String("commons", "", "shared commons directory")
		dsn       = flag.String("dsn", "", "PostgreSQL connection string (default: the file backend)")
		callLog   = flag.String("calls", "", "where the scripted source records its calls")
		out       = flag.String("out", "", "where to write this executor's report")
		offset    = flag.Int("offset", 0, "which slice of the subjects this executor covers")
		shared    = flag.Bool("shared", true, "join the distributed commons")
		executors = flag.Int("executors", 4, "how many executor processes to run")
		latency   = flag.Duration("source-latency", 120*time.Millisecond, "how slow the public source is")
	)
	flag.Parse()

	if *executor {
		if err := runExecutor(context.Background(), config{
			Name: *name, Commons: *commons, DSN: *dsn, CallLog: *callLog,
			Out: *out, Offset: *offset, Shared: *shared, Latency: *latency,
		}); err != nil {
			log.Fatalf("%s: %v", *name, err)
		}
		return
	}

	if err := runFleet(*executors, *dsn, *latency); err != nil {
		log.Fatal(err)
	}
}

// --- the parent ---------------------------------------------------------

// runFleet starts the executors twice — once with the shared commons and once
// without — and prints the two side by side.
func runFleet(executors int, dsn string, latency time.Duration) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "loom-commons-shared-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	fmt.Printf("%d executor processes · %d subjects · %d phrasings\n",
		executors, len(subjects), len(phrasings))
	fmt.Printf("the public source takes %s per call\n", latency)
	if dsn != "" {
		fmt.Printf("commons: PostgreSQL\n\n")
	} else {
		fmt.Printf("commons: a shared directory (%s)\n\n", dir)
	}

	alone, err := round(self, dir, "alone", dsn, executors, latency, false)
	if err != nil {
		return err
	}
	together, err := round(self, dir, "together", dsn, executors, latency, true)
	if err != nil {
		return err
	}

	fmt.Print(compare(alone, together, executors))
	if diff := firstDifference(alone.briefs, together.briefs); diff != "" {
		fmt.Printf("\n!! the answers differ between the two runs: %s\n", diff)
		os.Exit(1)
	}
	fmt.Printf("\nevery brief is byte-identical in both runs: an answer served out of\n" +
		"another executor's research is the answer the call would have returned.\n")
	return nil
}

// outcome is one round of executors.
type outcome struct {
	calls   int
	elapsed time.Duration
	reports []report
	briefs  map[string]string
}

// round starts every executor at once and waits for all of them.
func round(self, root, label, dsn string, executors int, latency time.Duration, shared bool) (*outcome, error) {
	dir := filepath.Join(root, label)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	calls := filepath.Join(dir, "calls.log")
	prefix := ""
	if dsn != "" {
		prefix = "commons_shared_" + label
		if err := reset(dsn, prefix); err != nil {
			return nil, err
		}
	}

	type running struct {
		cmd *exec.Cmd
		out string
	}
	started := time.Now()
	procs := make([]running, 0, executors)
	for i := range executors {
		name := fmt.Sprintf("executor-%d", i+1)
		outPath := filepath.Join(dir, name+".json")
		args := []string{
			"-executor", "-name", name,
			"-commons", filepath.Join(dir, "commons"),
			"-calls", calls, "-out", outPath,
			"-offset", fmt.Sprint(i),
			"-source-latency", latency.String(),
			fmt.Sprintf("-shared=%t", shared),
		}
		if dsn != "" {
			args = append(args, "-dsn", dsn+dsnPrefix(dsn, prefix))
		}
		cmd := exec.Command(self, args...)
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("start %s: %w", name, err)
		}
		procs = append(procs, running{cmd: cmd, out: outPath})
	}

	out := &outcome{briefs: map[string]string{}}
	for _, p := range procs {
		if err := p.cmd.Wait(); err != nil {
			return nil, fmt.Errorf("executor: %w", err)
		}
		blob, err := os.ReadFile(p.out)
		if err != nil {
			return nil, err
		}
		var r report
		if err := json.Unmarshal(blob, &r); err != nil {
			return nil, err
		}
		out.reports = append(out.reports, r)
		for k, v := range r.Briefs {
			out.briefs[k] = v
		}
	}
	out.elapsed = time.Since(started)
	out.calls = countLines(calls)
	sort.Slice(out.reports, func(i, j int) bool { return out.reports[i].Name < out.reports[j].Name })
	return out, nil
}

// dsnPrefix carries the table prefix to the children as a connection parameter
// nobody else reads, so the two rounds do not share a commons.
func dsnPrefix(dsn, prefix string) string {
	if prefix == "" {
		return ""
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return sep + "application_name=" + prefix
}

// reset drops the previous round's tables so each round starts cold.
func reset(dsn, prefix string) error {
	ctx := context.Background()
	s, err := pgstore.Open(ctx, dsn, pgstore.Options{Prefix: prefix, SkipMigrate: true})
	if err != nil {
		return err
	}
	defer s.Close()
	for _, table := range []string{"alias", "vector", "dependent", "verdict", "lease", "revision"} {
		if _, err := s.DB().ExecContext(ctx, `drop table if exists `+prefix+`_`+table+` cascade`); err != nil {
			return err
		}
	}
	return nil
}

// --- one executor -------------------------------------------------------

type config struct {
	Name    string
	Commons string
	DSN     string
	CallLog string
	Out     string
	Offset  int
	Shared  bool
	Latency time.Duration
}

// report is what an executor tells the parent.
type report struct {
	Name    string            `json:"name"`
	Calls   int               `json:"calls"`
	Stats   findings.Stats    `json:"stats"`
	Briefs  map[string]string `json:"briefs"`
	Elapsed time.Duration     `json:"elapsed"`
}

// runExecutor is one process: a Loom fleet with a shared commons in front of the
// one tool that reaches the outside world.
//
// Nothing below the WithFindings line knows the commons exists. The stage
// declares the tool it always declared, the planner grants exactly that name,
// and whether the call reaches the source, this process's ledger or another
// machine's research is decided beneath it.
func runExecutor(ctx context.Context, cfg config) error {
	src := &source{latency: cfg.Latency, log: cfg.CallLog}

	opts := []loom.Option{
		loom.WithRegistry(registry()),
		loom.WithWorkers(4),
		loom.WithTools(src),
		loom.WithEgress(sourceHost),
		loom.WithFleetBudget(core.Budget{MaxCostUSD: 5}),
	}

	fcfg := findings.Config{
		Gate:  []string{sourceTool},
		Specs: map[string]findings.GuardSpec{sourceTool: {CostUSD: sourceCostUSD}},
		Policy: findings.Policy{
			Topics: map[string]findings.TopicPolicy{
				// Company fundamentals move over months, not minutes.
				sourceTool: {Volatility: findings.Slow},
			},
		},
	}
	if cfg.Shared {
		backend, err := openBackend(ctx, cfg)
		if err != nil {
			return err
		}
		fcfg.Shared = findings.NewShared(findings.SharedConfig{
			Backend:  backend,
			Executor: cfg.Name,
			// Short, because the source is fast and the executors were started
			// together: a follower that waits 30s for a 120ms call has turned a
			// saving into a stall.
			LeaseTTL: 5 * time.Second,
			Poll:     10 * time.Millisecond,
		})
		fcfg.Policy.MaxWait = 10 * time.Second
	}
	opts = append(opts, loom.WithFindings(fcfg))

	fleet, err := loom.NewFleet(opts...)
	if err != nil {
		return err
	}
	defer fleet.Close()

	started := time.Now()
	agent := fleet.Go(ctx, deskPipeline(cfg.Name, cfg.Offset))
	res, err := agent.Wait()
	if err != nil {
		return err
	}

	rep := report{
		Name: cfg.Name, Calls: src.Count(), Elapsed: time.Since(started),
		Stats: fleet.Report().Findings, Briefs: map[string]string{},
	}
	for _, r := range res.StageOutputs["research"] {
		rep.Briefs[r.ID] = r.String("brief")
	}
	blob, err := json.Marshal(rep)
	if err != nil {
		return err
	}
	return os.WriteFile(cfg.Out, blob, 0o644)
}

func openBackend(ctx context.Context, cfg config) (findings.Backend, error) {
	if cfg.DSN != "" {
		prefix := "findings"
		if i := strings.Index(cfg.DSN, "application_name="); i >= 0 {
			prefix = cfg.DSN[i+len("application_name="):]
		}
		return pgstore.Open(ctx, cfg.DSN, pgstore.Options{Prefix: prefix})
	}
	return filestore.Open(cfg.Commons, filestore.Options{})
}

// deskPipeline is one executor's work: the subjects it was given, in its own
// house style, and a one-line note about each.
func deskPipeline(name string, offset int) *pipeline.Pipeline {
	// Every executor covers a rotated window of the subjects, so the overlap
	// between any two of them is real but partial.
	recs := make([]core.Record, 0, len(subjects))
	for i := range subjects {
		s := subjects[(i+offset)%len(subjects)]
		recs = append(recs, core.NewRecord(s, map[string]any{"company": s}))
	}
	phrasing := phrasings[offset%len(phrasings)]

	p := pipeline.New(name)
	p.FromRecords("subjects", recs).
		MapTools("research", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
			out, err := s.Invoke(ctx, sourceTool, map[string]any{
				"query":   fmt.Sprintf(phrasing, r.String("company")),
				"company": r.String("company"),
			})
			if err != nil {
				return core.Record{}, err
			}
			nr := r.Clone()
			m, _ := out.(map[string]any)
			nr.Data["brief"], _ = m["text"].(string)
			// The provenance the guard attaches. With a shared commons it can
			// say which *executor* the answer came from, which is the fact this
			// example exists to make visible.
			if prov, ok := m["findings"].(map[string]any); ok {
				nr.Data["origin"] = prov["origin"]
				if who, ok := prov["executor"].(string); ok {
					nr.Data["researched_by"] = who
				}
			}
			return nr, nil
		},
			pipeline.WithGrants(security.ToolCap(sourceTool)),
			pipeline.WithNoCache()).
		Infer("note", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			System:  "You write one-line analyst notes.",
			Prompt:  "Company: {{.company}}\nBrief: {{.brief}}\nWrite one line.",
		})
	return p
}

// --- the scripted public source -----------------------------------------

const (
	sourceTool    = "dd-search"
	sourceHost    = "api.diligence.example"
	sourceCostUSD = 0.004
)

// source stands in for whatever reaches the outside world. It records every
// call to a file, because a claim about calls avoided has to be countable by
// something other than the layer making the claim.
type source struct {
	calls   int32
	latency time.Duration
	log     string
}

func (s *source) Name() string     { return sourceTool }
func (s *source) Endpoint() string { return sourceHost }
func (s *source) Count() int       { return int(atomic.LoadInt32(&s.calls)) }

func (s *source) Invoke(ctx context.Context, args map[string]any) (any, error) {
	atomic.AddInt32(&s.calls, 1)
	company, _ := args["company"].(string)
	if s.log != "" {
		if f, err := os.OpenFile(s.log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			fmt.Fprintf(f, "%s\n", company)
			_ = f.Close()
		}
	}
	select {
	case <-time.After(s.latency):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	f, ok := facts[company]
	if !ok {
		return map[string]any{"text": ""}, nil
	}
	return map[string]any{
		"text": fmt.Sprintf("%s — revenue %s, headcount %d, litigation: %s.",
			company, f.revenue, f.headcount, f.litigation),
		"structured": map[string]any{
			"revenue": f.revenue, "headcount": f.headcount, "litigation": f.litigation,
		},
	}, nil
}

type fact struct {
	revenue    string
	headcount  int
	litigation string
}

var facts = map[string]fact{
	"northwind":       {"$4.2bn", 12000, "two open matters"},
	"contoso":         {"$880m", 3100, "none disclosed"},
	"fabrikam":        {"$12.6bn", 44000, "one antitrust review"},
	"tailwind":        {"$310m", 900, "none disclosed"},
	"adventure-works": {"$2.1bn", 7400, "three consumer claims"},
	"litware":         {"$95m", 260, "none disclosed"},
}

func registry() *model.Registry {
	reg := model.NewRegistry()
	m := model.NewMock("mock-fast",
		model.WithLatency(10*time.Millisecond),
		model.WithHandler(func(req model.Request) (string, error) {
			for _, line := range strings.Split(req.Prompt, "\n") {
				if rest, ok := strings.CutPrefix(line, "Brief: "); ok {
					return "Note: " + rest, nil
				}
			}
			return "Note: (no brief)", nil
		}))
	_ = reg.Register(model.Info{
		ID: "mock-fast", Provider: m, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 0.80, OutputPerMTok: 4.00},
	})
	return reg
}

// --- reporting ----------------------------------------------------------

func compare(alone, together *outcome, executors int) string {
	var b strings.Builder
	questions := executors * len(subjects)

	fmt.Fprintf(&b, "%-28s %14s %14s\n", "", "no commons", "shared commons")
	fmt.Fprintf(&b, "%-28s %14d %14d\n", "questions asked", questions, questions)
	fmt.Fprintf(&b, "%-28s %14d %14d\n", "calls to the source", alone.calls, together.calls)
	fmt.Fprintf(&b, "%-28s %14s %14s\n", "spent at the source",
		fmt.Sprintf("$%.4f", float64(alone.calls)*sourceCostUSD),
		fmt.Sprintf("$%.4f", float64(together.calls)*sourceCostUSD))
	fmt.Fprintf(&b, "%-28s %14s %14s\n", "wall clock (all executors)",
		alone.elapsed.Round(time.Millisecond), together.elapsed.Round(time.Millisecond))

	avoided := alone.calls - together.calls
	fmt.Fprintf(&b, "\n%d external call(s) avoided across executors — %.0f%% of the calls the\n",
		avoided, 100*float64(avoided)/float64(max(alone.calls, 1)))
	fmt.Fprintf(&b, "same fleet made with every process holding its own ledger.\n\n")

	fmt.Fprintf(&b, "%-14s %7s %7s %7s %7s %7s %7s\n",
		"executor", "source", "local", "shared", "led", "followed", "adopted")
	var totals findings.Stats
	for _, r := range together.reports {
		fmt.Fprintf(&b, "%-14s %7d %7d %7d %7d %7d %7d\n",
			r.Name, r.Calls, r.Stats.LocalReuse(), r.Stats.SharedReuse(),
			r.Stats.Leader, r.Stats.Follower, r.Stats.Adopted)
		totals.Asked += r.Stats.Asked
		totals.Exact += r.Stats.Exact
		totals.Class += r.Stats.Class
		totals.Near += r.Stats.Near
		totals.Coalesced += r.Stats.Coalesced
		totals.RemoteExact += r.Stats.RemoteExact
		totals.RemoteClass += r.Stats.RemoteClass
		totals.RemoteNear += r.Stats.RemoteNear
		totals.RemoteCoalesced += r.Stats.RemoteCoalesced
		totals.Fresh += r.Stats.Fresh
		totals.Adopted += r.Stats.Adopted
		totals.Published += r.Stats.Published
		totals.Leader += r.Stats.Leader
		totals.Follower += r.Stats.Follower
		totals.LeaseTimeouts += r.Stats.LeaseTimeouts
		totals.LeaseTakeovers += r.Stats.LeaseTakeovers
		totals.BackendFailures += r.Stats.BackendFailures
		totals.Avoided.Add(r.Stats.Avoided)
		totals.Spent.Add(r.Stats.Spent)
		totals.AvoidedTime += r.Stats.AvoidedTime
		totals.Overhead += r.Stats.Overhead
		totals.RemoteLatency += r.Stats.RemoteLatency
	}

	fmt.Fprintf(&b, "\nthe fleet, summed across every executor:\n\n%s", totals.String())

	// The claim worth checking rather than asserting: one call per subject, not
	// one per executor per subject.
	if together.calls == len(subjects) {
		fmt.Fprintf(&b, "\neach subject was researched exactly once, by one executor, and served\n")
		fmt.Fprintf(&b, "to the other %d — which is what the source's own call log says, not what\n",
			executors-1)
		fmt.Fprintf(&b, "the layer's counters claim.\n")
	} else {
		fmt.Fprintf(&b, "\n%d calls for %d subjects: some executors overtook a slow leader or\n",
			together.calls, len(subjects))
		fmt.Fprintf(&b, "missed a window. Correctness is unaffected; the deduplication is not.\n")
	}
	return b.String()
}

func countLines(path string) int {
	blob, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(string(blob), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

func firstDifference(a, b map[string]string) string {
	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if a[k] != b[k] {
			return fmt.Sprintf("%s: %q vs %q", k, a[k], b[k])
		}
	}
	if len(a) != len(b) {
		return fmt.Sprintf("record counts differ: %d vs %d", len(a), len(b))
	}
	return ""
}
