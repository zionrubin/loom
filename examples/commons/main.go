// Command commons shows the shared research layer doing the thing it exists
// for: four analyst desks researching the same companies at the same time, in
// their own words, hitting the public source once per company instead of once
// per desk.
//
// It runs entirely offline — a scripted "public source" tool with realistic
// latency, and mock models — so the numbers are reproducible and cost nothing.
// The same fleet is run twice, once with the commons and once without, and the
// two are printed side by side, because a layer that claims to reduce
// duplicated work without adding meaningful latency is making two measurable
// claims and should be made to show both.
//
//	go run ./examples/commons
//
//	# watch it in the constellation view alongside everything else
//	go run ./examples/commons -addr localhost:8077
//
// What to look for in the output:
//
//   - **calls to the source**: 24 without the commons, 6 with it — one per
//     subject, not one per desk. Nothing about the pipelines changed; the
//     desks still each ask their own question in their own words.
//   - **gate overhead**: microseconds per question, against a source that
//     takes 120ms. That ratio is the argument for putting the gate in front of
//     every task rather than only the ones a human guessed would collide.
//   - **identical answers**: the briefs are byte-identical in both runs. A hit
//     is substitutable for the call it replaces or the layer is not a cache,
//     it is a bug.
//   - **the ledger**: six live findings, several corroborated, one retracted
//     at the end — with the desks that had been served it named, which is what
//     a retraction is for.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/findings"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/viz"
)

// companies are the subjects every desk covers. The overlap is the point:
// four desks with their own briefs, one set of facts underneath.
var companies = []string{"northwind", "contoso", "fabrikam", "tailwind", "adventure-works", "litware"}

// desks are the analyst agents. Each has a house style for asking, which is
// exactly why an exact-match cache cannot help them: same subject, same facts
// wanted, four different sentences.
var desks = []struct{ name, phrasing string }{
	{"credit-desk", "what are %s's revenue, headcount and outstanding litigation"},
	{"equity-desk", "%s: earnings, staff count, legal exposure"},
	{"risk-desk", "summarize legal and financial exposure for %s"},
	{"esg-desk", "%s company profile including workforce size and revenue"},
}

func main() {
	addr := flag.String("addr", "", "serve the constellation view on this address (e.g. localhost:8077)")
	latency := flag.Duration("source-latency", 120*time.Millisecond, "how slow the public source is")
	flag.Parse()

	var v *viz.Server
	if *addr != "" {
		v = viz.New()
		url, err := v.Start(*addr)
		if err != nil {
			log.Fatalf("view: %v", err)
		}
		fmt.Printf("constellation view: %s\n\n", url)
	}

	ctx := context.Background()

	// Two runs of exactly the same fleet, distinguished by one option.
	plain, err := run(ctx, *latency, false, nil)
	if err != nil {
		log.Fatalf("run without the commons: %v", err)
	}
	shared, err := run(ctx, *latency, true, v)
	if err != nil {
		log.Fatalf("run with the commons: %v", err)
	}

	fmt.Print(compare(plain, shared))

	// The briefs must be identical either way. This is the safety property the
	// whole design rests on, so the demo checks it rather than claiming it.
	if diff := firstDifference(plain.briefs, shared.briefs); diff != "" {
		fmt.Printf("\n!! answers differ between the two runs: %s\n", diff)
		os.Exit(1)
	}
	fmt.Printf("\nevery brief is byte-identical in both runs — a hit is substitutable\nfor the call it replaced, which is what makes reuse safe rather than merely cheap.\n")

	fmt.Print(retractionDemo(shared))

	if v != nil {
		fmt.Printf("\nview still serving on %s — ctrl-C to stop\n", *addr)
		select {}
	}
}

// --- the run ------------------------------------------------------------

type outcome struct {
	commons  bool
	calls    int
	elapsed  time.Duration
	briefs   map[string]string
	report   loom.FleetReport
	gate     *findings.Gate
	sourceMS time.Duration
}

func run(ctx context.Context, latency time.Duration, commons bool, v *viz.Server) (*outcome, error) {
	src := &source{latency: latency}
	opts := []loom.Option{
		loom.WithRegistry(registry()),
		loom.WithWorkers(8),
		loom.WithTools(src),
		loom.WithEgress(sourceHost),
		loom.WithFleetBudget(core.Budget{MaxCostUSD: 5}),
	}
	if v != nil {
		opts = append(opts, loom.WithEventHandler(v.Handle))
	}
	if commons {
		opts = append(opts, loom.WithFindings(findings.Config{
			Gate: []string{sourceTool},
			// A metered search API bills per query and spends no tokens, so the
			// per-call price is the only thing that can make the dollar saving
			// real. Latency is measured either way.
			Specs: map[string]findings.GuardSpec{
				sourceTool: {CostUSD: sourceCostUSD},
			},
			Policy: findings.Policy{
				Topics: map[string]findings.TopicPolicy{
					// Company fundamentals move over months, not minutes. Saying
					// so once, per topic, is what lets every finding under it be
					// checked for freshness without anyone guessing per answer.
					sourceTool: {Volatility: findings.Slow},
				},
			},
		}))
	}

	fleet, err := loom.NewFleet(opts...)
	if err != nil {
		return nil, err
	}
	defer fleet.Close()

	start := time.Now()
	// Launched together, on purpose. Agents started at the same instant all
	// miss a cold cache at the same instant — the case a result cache cannot
	// help with and the single-flight lease can.
	agents := make([]*loom.Agent, 0, len(desks))
	for _, d := range desks {
		agents = append(agents, fleet.Go(ctx, deskPipeline(d.name, d.phrasing)))
	}

	out := &outcome{
		commons: commons, briefs: map[string]string{}, sourceMS: latency,
	}
	for _, a := range agents {
		res, err := a.Wait()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", a.Name, err)
		}
		for _, r := range res.StageOutputs["research"] {
			out.briefs[a.Name+"/"+r.ID] = r.String("brief")
		}
	}
	out.elapsed = time.Since(start)
	out.calls, out.report, out.gate = src.Count(), fleet.Report(), fleet.Findings()
	return out, nil
}

// deskPipeline is one analyst desk: ask the source about each company in the
// desk's own words, then write a note about what came back.
//
// Nothing here mentions the commons. That is the design working: the stage
// declares the tool it always declared, the planner grants exactly that name,
// and whether the call reaches the source or the ledger is decided beneath it.
func deskPipeline(name, phrasing string) *pipeline.Pipeline {
	recs := make([]core.Record, len(companies))
	for i, c := range companies {
		recs[i] = core.NewRecord(c, map[string]any{"company": c})
	}
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
			// The provenance the guard attaches: a stage — or a prompt — can
			// tell fresh research from reused, and how old the reused is.
			if prov, ok := m["findings"].(map[string]any); ok {
				nr.Data["origin"] = prov["origin"]
			}
			return nr, nil
		},
			pipeline.WithGrants(security.ToolCap(sourceTool)),
			// The stage's own output is not worth caching: the tool call inside
			// it is what costs, and that is what the commons is for.
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
	sourceTool = "dd-search"
	sourceHost = "api.diligence.example"
	// sourceCostUSD is what one query to this diligence API costs. Metered
	// search APIs really are priced this way, and it is the number that makes
	// "avoided $x" mean anything for a source that spends no tokens.
	sourceCostUSD = 0.004
)

// source stands in for whatever reaches the outside world — an MCP server, a
// search API, a scraper. It is slow and it counts itself, which is all the
// demo needs from it.
type source struct {
	calls   int32
	latency time.Duration
}

func (s *source) Name() string     { return sourceTool }
func (s *source) Endpoint() string { return sourceHost }
func (s *source) Count() int       { return int(atomic.LoadInt32(&s.calls)) }

func (s *source) Invoke(ctx context.Context, args map[string]any) (any, error) {
	atomic.AddInt32(&s.calls, 1)
	select {
	case <-time.After(s.latency):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	company, _ := args["company"].(string)
	f, ok := facts[company]
	if !ok {
		// A real dead end. Returning nothing is a finding too, and the guard
		// files it as one so nobody searches this again.
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
		model.WithLatency(15*time.Millisecond),
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

func compare(plain, shared *outcome) string {
	var b strings.Builder
	questions := len(desks) * len(companies)

	fmt.Fprintf(&b, "\n%d desks × %d companies = %d questions about %d subjects\n",
		len(desks), len(companies), questions, len(companies))
	fmt.Fprintf(&b, "the public source takes %s per call\n\n", plain.sourceMS)

	fmt.Fprintf(&b, "%-22s %14s %14s\n", "", "no commons", "with commons")
	fmt.Fprintf(&b, "%-22s %14d %14d\n", "calls to the source", plain.calls, shared.calls)
	fmt.Fprintf(&b, "%-22s %14s %14s\n", "wall clock",
		plain.elapsed.Round(time.Millisecond), shared.elapsed.Round(time.Millisecond))
	fmt.Fprintf(&b, "%-22s %14s %14s\n", "spent at the source",
		fmt.Sprintf("$%.4f", float64(plain.calls)*sourceCostUSD),
		fmt.Sprintf("$%.4f", float64(shared.calls)*sourceCostUSD))
	fmt.Fprintf(&b, "%-22s %14d %14d\n", "result-cache hits",
		cacheHits(plain.report), cacheHits(shared.report))
	fmt.Fprintf(&b, "%-22s %14s %14s\n", "spent on models",
		fmt.Sprintf("$%.4f", plain.report.Spent.CostUSD),
		fmt.Sprintf("$%.4f", shared.report.Spent.CostUSD))

	s := shared.report.Findings
	fmt.Fprintf(&b, "\n%s", s.String())
	fmt.Fprintf(&b, "  %d of %d questions never reached the source; the source was called\n",
		s.Reused(), s.Asked)
	fmt.Fprintf(&b, "  once per subject rather than once per desk.\n")

	// An honest reading of the model column, which does not move the way the
	// source column does — and sometimes moves the wrong way.
	if shared.report.Spent.CostUSD >= plain.report.Spent.CostUSD {
		fmt.Fprintf(&b, "\n  note the model column: spend is *not* lower with the commons, and here it\n")
		fmt.Fprintf(&b, "  is slightly higher. The layer removes duplicate calls to the source, not\n")
		fmt.Fprintf(&b, "  duplicate model calls — and by removing the source's latency it makes the\n")
		fmt.Fprintf(&b, "  desks arrive at the note stage together, so more identical note tasks are\n")
		fmt.Fprintf(&b, "  in flight at once and the result cache (%d hits, against %d without) serves\n",
			cacheHits(shared.report), cacheHits(plain.report))
		fmt.Fprintf(&b, "  fewer of them. That is the same thundering herd, one level up: the result\n")
		fmt.Fprintf(&b, "  cache has no single-flight lease, so concurrent identical tasks both run.\n")
		fmt.Fprintf(&b, "  Worth knowing, and worth fixing there rather than pretending here.\n")
	}

	// The overhead claim, stated as a ratio rather than as an adjective.
	if per := s.Overshoot(); per > 0 {
		fmt.Fprintf(&b, "\n  gate overhead is %s per question against a %s source call:\n",
			per.Round(time.Microsecond), plain.sourceMS)
		fmt.Fprintf(&b, "  the gate costs about 1/%d of the call it decides about.\n",
			int64(plain.sourceMS/per))
	}

	fmt.Fprintf(&b, "\nthe commons now holds:\n")
	for _, t := range shared.report.Commons {
		fmt.Fprintf(&b, "  %-14s %d live finding(s), %d corroboration(s)",
			t.Topic, t.Live, t.Corroborations)
		if t.Negative > 0 {
			fmt.Fprintf(&b, ", %d negative", t.Negative)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// retractionDemo withdraws one claim and shows what rested on it — the reason
// serves are recorded against findings in the first place.
func retractionDemo(o *outcome) string {
	if o.gate == nil {
		return ""
	}
	entries := o.gate.Ledger.Entries()
	if len(entries) == 0 {
		return ""
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Seq < entries[j].Seq })
	target := entries[0]

	deps, err := o.gate.Ledger.Retract(target.Finding.ID, "the source published a correction", time.Now())
	if err != nil {
		return fmt.Sprintf("\nretract: %v\n", err)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\nretracting %q (%s):\n", target.Finding.Asked.Text, target.Hash[:12])
	fmt.Fprintf(&b, "  %d task(s) had already been served it and rest on it:\n", len(deps))
	seen := map[string]bool{}
	for _, d := range deps {
		if seen[d.Stage+d.RunID] {
			continue
		}
		seen[d.Stage+d.RunID] = true
		fmt.Fprintf(&b, "    run %s stage %q\n", d.RunID, d.Stage)
	}
	if _, ok := o.gate.Ledger.Get(target.Hash); ok {
		fmt.Fprintf(&b, "  the withdrawn revision is still resolvable by hash, because lineage\n")
		fmt.Fprintf(&b, "  names it — a ledger that forgets cannot say what it believed when it\n")
		fmt.Fprintf(&b, "  produced a conclusion, which is exactly what a retraction makes urgent.\n")
	}
	return b.String()
}

// cacheHits totals the result-cache replays across a fleet's agents.
func cacheHits(r loom.FleetReport) int {
	n := 0
	for _, a := range r.Agents {
		for _, st := range a.Report.Stages {
			n += st.CacheHits
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
