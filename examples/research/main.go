// Command research runs Loom's largest offline demo: a systematic
// literature survey of ~50 papers on self-improving AI agents, entirely on
// scripted mock models (no keys, no network, zero real cost) while serving
// the live constellation view. It exists to show the whole UI in motion —
// roughly 195 task stars across a branching DAG:
//
//	papers ─ normalize+screenable (fused) ─ screen ─ relevant-only ─ extract-findings
//	                                                                 ├─ grade-evidence ─ strong-evidence ─ synthesis (tree) ─ executive-abstract
//	                                                                 └─ explode-claims+claims (fused) ─ open-questions (tree)
//
// The mocks are scripted so a single run displays every visual state the
// view can render: jittered latency across three model tiers, transient
// 429/503 failures (retry orbits), one 11-second straggler (activity ring),
// one retracted paper that fails permanently (dead-lettered red cross), two
// garbled-JSON responses and one out-of-range evidence grade that climb the
// escalation ladder to the deep model, two abstract-less stubs dropped by a
// fused pure stage, and two multi-level ReduceAI trees whose lineage fans in.
//
//	go run ./examples/research            # then open http://localhost:8077
//
//	# cache-as-checkpoint: rerun with a state dir and watch the sky settle
//	# instantly in the cache-replay hue, zero model calls
//	LOOM_STATE=/tmp/loom-research go run ./examples/research
//	LOOM_STATE=/tmp/loom-research go run ./examples/research
//
//	# budget governor: squeeze the run budget and watch the header flag
//	# "budget exceeded" while the run stops gracefully with partial results
//	go run ./examples/research -budget 0.02
//
// See README.md in this directory for a shot-by-shot recording storyboard.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/viz"
)

func main() {
	addr := flag.String("addr", "localhost:8077", "address for the constellation view")
	budget := flag.Float64("budget", 2.00, "run budget in USD (try 0.02 to see the governor stop the run)")
	wait := flag.Duration("wait", 60*time.Second, "how long to wait for a browser before starting anyway")
	flag.Parse()

	reg, err := buildRegistry()
	if err != nil {
		log.Fatal(err)
	}
	p := buildPipeline()

	v := viz.New()
	url, err := v.Start(*addr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("constellation view: %s\n", url)
	fmt.Printf("waiting up to %s for a browser to connect (Ctrl-C to abort)…\n", *wait)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), *wait)
	if v.AwaitViewer(waitCtx) {
		fmt.Println("viewer connected — starting the survey")
		time.Sleep(800 * time.Millisecond) // a beat, so the empty sky is visible first
	} else {
		fmt.Println("no viewer yet — running anyway (the page replays state on connect)")
	}
	cancelWait()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithWorkers(8),
		loom.WithContinueOnError(),
		loom.WithRunBudget(core.Budget{MaxCostUSD: *budget}),
		loom.WithEventHandler(v.Handle),
		// Registered once for the whole run; stages opt in with
		// pipeline.WithBroadcast and read by reference.
		loom.WithBroadcast("inclusion-criteria", inclusionCriteria),
		loom.WithBroadcast("venue-tiers", venueTiers),
		loom.WithBroadcast("evidence-rubric", evidenceRubric),
	}
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		opts = append(opts, loom.WithStateDir(dir))
	}

	res, err := loom.Run(ctx, p, opts...)
	if err != nil {
		fmt.Printf("\nrun ended with error: %v\n", err)
	}
	if res != nil {
		// Two terminal stages (one per branch), so read StageOutputs.
		for _, stage := range []string{"executive-abstract", "open-questions"} {
			fmt.Printf("\n--- %s ---\n", stage)
			for _, r := range res.StageOutputs[stage] {
				fmt.Println(r.String("output"))
			}
		}
		fmt.Println("\n--- report ---")
		fmt.Print(res.Report.String())
		for _, f := range res.Failures {
			fmt.Printf("dead letter: %s (%s): %v\n", f.Task.ID, f.Class, f.Err)
		}
	}

	fmt.Printf("\nrun finished — still serving %s (Ctrl-C to exit)\n", url)
	<-ctx.Done()
	_ = v.Close()
}

// --- shared values ----------------------------------------------------------

// Three things every task in this survey has to agree on: what counts as
// in-scope, how venues rank, and what an evidence grade means. Registering
// them as broadcasts stores each once by content hash and hands the reading
// stages a reference — the alternative, stamping them onto all ~50 records or
// pasting them into three prompt templates, would copy the same bytes into
// every task envelope and put the rubric out of reach of the Go stages.
//
// In the constellation view these appear as hexagonal nodes above the sky,
// each feeding the stage clusters that read it.

// inclusionCriteria is read by the screening stage's prompt. It deliberately
// avoids naming the six survey areas verbatim — the mock screener keys on
// those phrases appearing in a paper's abstract, and repeating them in the
// shared criteria would make every paper look in-scope.
var inclusionCriteria = map[string]any{
	"include_if": []string{
		"the work studies systems that improve their own behavior over time",
		"results are empirical: ablations, replications, or deployment data",
		"the venue is peer-reviewed or a citable preprint",
	},
	"exclude_if": []string{
		"no connection to machine learning or agent systems",
		"position paper with no abstract to screen",
	},
	"reviewer_note": "when the abstract is ambiguous, prefer inclusion and let the grading stage sort it out",
}

// venueTiers is read by a Go stage, not a prompt — the broadcast-join case:
// a lookup table every task needs, none of them should carry.
var venueTiers = map[string]string{
	"NeurIPS": "A*", "ICML": "A*", "ICLR": "A*", "IEEE S&P": "A*",
	"USENIX Security": "A*", "SOSP": "A*", "CoRL": "A", "AAMAS": "A",
	"arXiv": "preprint", "preprint": "preprint",
}

// evidenceRubric is read by the grading stage's prompt, so all ~40 grading
// tasks score against one scale rather than each improvising.
var evidenceRubric = map[string]any{
	"5": "replicated independently, effect stable across scales and seeds",
	"4": "single-lab result with ablations and a held-out evaluation",
	"3": "single result with a credible baseline comparison",
	"2": "suggestive: small sample, weak or missing baseline",
	"1": "anecdotal or projected beyond the tested regime",
}

// --- corpus -----------------------------------------------------------------

// The six areas the survey covers. Each in-scope abstract embeds its area
// phrase verbatim, which is what the mock "screener" keys on — a stand-in
// for a real model reading the abstract.
var areas = []string{
	"agent planning and search",
	"long-horizon memory",
	"tool use and skill acquisition",
	"evaluation and benchmarks",
	"safety and oversight",
	"multi-agent coordination",
}

type paper struct {
	title string
	venue string
	year  int
	area  string // "" = out of scope for the survey
	stub  bool   // no abstract yet (dropped by the screenable filter)
}

var corpus = []paper{
	// agent planning and search
	{"Monte-Carlo Plan Repair for Long-Horizon Agent Tasks", "NeurIPS", 2025, areas[0], false},
	{"Hierarchical Task Decomposition with Learned Subgoal Critics", "ICML", 2024, areas[0], false},
	{"Toward Provably Bounded Search in Open-Ended Planning", "arXiv", 2026, areas[0], false},
	{"Test-Time Deliberation Budgets for Agentic Search", "ICLR", 2025, areas[0], false},
	{"Backtracking Transformers: Plan Revision as a First-Class Operation", "NeurIPS", 2024, areas[0], false},
	{"World-Model Rollouts versus Tree Search in Embodied Agents", "CoRL", 2025, areas[0], false},
	{"Anytime Replanning under Partial Observability in Web Agents", "ICML", 2026, areas[0], false},

	// long-horizon memory
	{"A Trillion-Token Ablation of Context Compression Strategies", "arXiv", 2026, areas[1], false}, // the straggler
	{"Retracted: Infinite Context Windows via Recursive Summarization", "arXiv", 2024, areas[1], false},
	{"Episodic Memory Consolidation in Continually Deployed Agents", "NeurIPS", 2025, areas[1], false},
	{"Sleep-Time Compute: Offline Memory Reorganization for Agents", "ICLR", 2026, areas[1], false},
	{"Toward Lossless Semantic Compression of Agent Trajectories", "arXiv", 2025, areas[1], false},
	{"Vector Stores Considered Insufficient: A Case for Structured Recall", "ICML", 2025, areas[1], false},
	{"Forgetting as a Feature: Selective Memory Decay in Agent Fleets", "ICLR", 2024, areas[1], false},

	// tool use and skill acquisition
	{"Distilling Tool-Use Trajectories into Reusable Skills", "NeurIPS", 2025, areas[2], false}, // garbles at extraction
	{"Zero-Shot API Composition from Natural-Language Contracts", "ICLR", 2025, areas[2], false},
	{"Sandboxed Execution Feedback Improves Code-Agent Reliability", "ICML", 2024, areas[2], false},
	{"Toward Self-Verifying Tool Calls in Production Agents", "arXiv", 2026, areas[2], false},
	{"Learning When Not to Use a Tool", "NeurIPS", 2024, areas[2], false},
	{"Typed Capability Grants for Model-Driven Automation", "IEEE S&P", 2026, areas[2], false},
	{"Curriculum Discovery of Composite Skills in Software Agents", "ICLR", 2026, areas[2], false},

	// evaluation and benchmarks
	{"Self-Rewarding Agents Trained on Synthetic Preferences", "ICML", 2025, areas[3], false}, // invalid grade → escalates
	{"Benchmarks That Fight Back: Adversarially Refreshed Evaluation", "NeurIPS", 2025, areas[3], false},
	{"Measuring Silent Regressions in Multi-Step Agent Workflows", "ICLR", 2024, areas[3], false},
	{"Toward Calibrated Confidence in Agent Self-Reports", "arXiv", 2025, areas[3], false},
	{"Pass@k Is Not Reliability: Metrics for Deployed Agents", "ICML", 2026, areas[3], false},
	{"Holdout Contamination in Web-Scale Agent Training", "NeurIPS", 2026, areas[3], false},
	{"Cost-Normalized Scoring for Model Escalation Ladders", "ICLR", 2025, areas[3], false},

	// safety and oversight
	{"Emergent Deception in Self-Improving Agent Populations", "NeurIPS", 2026, areas[4], false}, // garbles at screening
	{"Least-Privilege Envelopes for Autonomous Task Execution", "IEEE S&P", 2025, areas[4], false},
	{"Auditable Lineage for Model-Generated Artifacts", "USENIX Security", 2025, areas[4], false},
	{"Toward Interruptibility Guarantees in Recursive Self-Improvement", "arXiv", 2026, areas[4], false},
	{"Reward Hacking under Distribution Shift: A Field Study", "ICML", 2025, areas[4], false},
	{"Sandbagging Detection via Cross-Model Interrogation", "NeurIPS", 2024, areas[4], false},
	{"Constitutional Constraints Survive Fine-Tuning, Mostly", "ICLR", 2026, areas[4], false},

	// multi-agent coordination
	{"Emergent Division of Labor in Heterogeneous Agent Teams", "AAMAS", 2025, areas[5], false},
	{"Consensus Protocols for Redundant Model Verification", "NeurIPS", 2025, areas[5], false},
	{"Toward Market Mechanisms for Compute Allocation among Agents", "arXiv", 2025, areas[5], false},
	{"Adversarial Verification Panels Reduce Confabulated Findings", "ICML", 2026, areas[5], false},
	{"Swarm Curricula: Population-Level Exploration for Agent Fleets", "ICLR", 2026, areas[5], false},
	{"Cheap Talk and Costly Signals in Agent Negotiation", "AAMAS", 2024, areas[5], false},
	{"Failure Cascades in Pipelined Agent Systems", "SOSP", 2025, areas[5], false},

	// out of scope — screened out by the model
	{"Sediment Transport in Tidal Estuaries: A Decade of Lidar", "AGU", 2024, "", false},
	{"Perovskite Solar Cell Degradation under Humid Cycling", "Joule", 2025, "", false},
	{"Acoustic Niches of Coral Reef Fish Communities", "Ecology Letters", 2024, "", false},
	{"Bronze Age Trade Networks of the Aegean: New Isotope Evidence", "Antiquity", 2025, "", false},
	{"Gut Microbiome Succession in Preterm Infants", "Cell Host & Microbe", 2026, "", false},
	{"Urban Heat Island Mitigation via Reflective Roofing", "Nature Cities", 2025, "", false},

	// preprint stubs with no abstract — dropped before any model call
	{"Recursive Self-Improvement: A Position Paper", "preprint", 2026, areas[4], true},
	{"Notes on Agent Benchmarking", "preprint", 2026, areas[3], true},
}

func abstractFor(p paper) string {
	if p.stub {
		return ""
	}
	if p.area == "" {
		return "We report field measurements and a mechanistic model. " +
			"No connection to machine learning or agent systems is claimed."
	}
	return fmt.Sprintf("We study %s in self-improving agent systems. "+
		"Across three model families we isolate the mechanism behind %q "+
		"and report ablations, failure modes, and deployment guidance.",
		p.area, strings.ToLower(firstWords(p.title, 5)))
}

func records() []core.Record {
	recs := make([]core.Record, len(corpus))
	for i, p := range corpus {
		recs[i] = core.NewRecord(fmt.Sprintf("p%02d", i+1), map[string]any{
			"title":    p.title,
			"venue":    p.venue,
			"year":     p.year,
			"abstract": abstractFor(p),
		})
	}
	return recs
}

// --- pipeline ---------------------------------------------------------------

func buildPipeline() *pipeline.Pipeline {
	p := pipeline.New("lit-survey")
	src := p.FromRecords("papers", records())

	// Two adjacent pure stages: the planner fuses them into one task
	// boundary (stage kind "fused" in the stage inspector), and the fused
	// envelope unions what its members declared — so the venue table stays
	// readable after fusion.
	normalized := src.MapTools("normalize", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		// A broadcast join: one shared table, read by every task in the
		// stage, carried by none of them.
		tiers, err := core.BroadcastAs[map[string]string](ctx, s, "venue-tiers")
		if err != nil {
			return core.Record{}, err
		}
		tier, ok := tiers[r.String("venue")]
		if !ok {
			tier = "unranked"
		}
		out := r.Clone()
		out.Data["title"] = strings.TrimSpace(r.String("title"))
		out.Data["venue_tier"] = tier
		out.Data["cite"] = fmt.Sprintf("%s, %s (%v)", r.String("title"), r.String("venue"), r.Data["year"])
		return out, nil
	}, pipeline.WithVersion("v2"), pipeline.WithBatchSize(4),
		pipeline.WithBroadcast("venue-tiers"))
	screenable := normalized.Filter("screenable", func(r core.Record) (bool, error) {
		return r.String("abstract") != "", nil // drop preprint stubs
	}, pipeline.WithVersion("v1"), pipeline.WithBatchSize(4))

	screened := screenable.Infer("screen", pipeline.InferSpec{
		Binding: model.Binding{
			Tier:       model.TierFast,
			Escalation: []string{"mock-oracle"}, // garbled output climbs here
		},
		System: "You are the screening reviewer for a systematic survey of self-improving AI agents.",
		// The shared criteria are interpolated into every screening prompt
		// while living in exactly one place.
		Prompt: "Screen this paper for inclusion in the survey.\n" +
			"Inclusion criteria (shared by every reviewer in this run):\n" +
			"{{broadcastJSON \"inclusion-criteria\"}}\n" +
			"Title: {{.title}}\nVenue: {{.venue}} ({{.year}}, tier {{.venue_tier}})\n" +
			"Abstract: {{.abstract}}\n" +
			"Return JSON: {\"relevant\": bool, \"topic\": string}.",
		ParseJSON: true,
	}, pipeline.WithBroadcast("inclusion-criteria"))

	relevant := screened.Filter("relevant-only", func(r core.Record) (bool, error) {
		b, _ := r.Data["relevant"].(bool)
		return b, nil
	}, pipeline.WithVersion("v1"), pipeline.WithBatchSize(4))

	findings := relevant.Infer("extract-findings", pipeline.InferSpec{
		Binding: model.Binding{
			Tier:       model.TierFast,
			Escalation: []string{"mock-oracle"},
		},
		System: "You extract the load-bearing findings from research papers, verbatim where possible.",
		Prompt: "Extract findings from {{.cite}}.\nTopic: {{.topic}}\nAbstract: {{.abstract}}\n" +
			"Return JSON: {\"headline\": string, \"findings\": [string]}.",
		ParseJSON: true,
	})

	// Branch A: grade evidence (mid tier, validated), keep the strong ones,
	// then a multi-level ReduceAI tree synthesizes the narrative and a final
	// deep-model call writes the abstract.
	graded := findings.Infer("grade-evidence", pipeline.InferSpec{
		Binding: model.Binding{
			Tier:       model.TierBalanced,
			Escalation: []string{"mock-oracle"}, // out-of-range grades climb here
		},
		System: "You grade the strength of evidence behind research findings.",
		Prompt: "Grade the strength of evidence for: {{.headline}}\nFindings: {{.findings}}\n" +
			"Grade against this shared rubric:\n{{broadcastJSON \"evidence-rubric\"}}\n" +
			"Return JSON: {\"evidence\": 1-5, \"rationale\": string}.",
		ParseJSON: true,
		Validate: func(r core.Record) error {
			v, ok := r.Data["evidence"].(float64)
			if !ok || v < 1 || v > 5 {
				return fmt.Errorf("evidence grade %v outside 1..5", r.Data["evidence"])
			}
			return nil
		},
	}, pipeline.WithBroadcast("evidence-rubric"))

	strong := graded.Filter("strong-evidence", func(r core.Record) (bool, error) {
		v, _ := r.Data["evidence"].(float64)
		return v >= 3, nil
	}, pipeline.WithVersion("v1"), pipeline.WithBatchSize(4))

	synthesis := strong.ReduceAI("synthesis", pipeline.ReduceAISpec{
		Binding: model.Binding{Tier: model.TierDeep},
		System:  "You are the lead author synthesizing a literature review.",
		Prompt: "Synthesize {{.Count}} graded findings into a coherent narrative:\n" +
			"{{range .Items}}- {{.}}\n{{end}}",
		FanIn:     4,
		ItemField: "headline",
	})

	synthesis.Infer("executive-abstract", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierDeep},
		System:  "You write executive abstracts for survey papers.",
		Prompt:  "Write a three-sentence executive abstract for the survey, from this synthesis:\n\n{{.output}}",
	})

	// Branch B: explode each paper's findings into claim records (FlatMap +
	// Filter fuse into one pure stage), then a cheap ReduceAI tree distills
	// the open research questions.
	claims := findings.FlatMap("explode-claims", func(r core.Record) ([]core.Record, error) {
		fs, _ := r.Data["findings"].([]any)
		out := make([]core.Record, 0, len(fs))
		for i, f := range fs {
			s, _ := f.(string)
			out = append(out, core.NewRecord(fmt.Sprintf("%s-c%d", r.ID, i+1), map[string]any{
				"claim":  s,
				"source": r.String("cite"),
				"topic":  r.String("topic"),
			}))
		}
		return out, nil
	}, pipeline.WithVersion("v1"), pipeline.WithBatchSize(5)).
		Filter("claims", func(r core.Record) (bool, error) {
			return !strings.Contains(r.String("claim"), "(speculative)"), nil
		}, pipeline.WithVersion("v1"), pipeline.WithBatchSize(5))

	claims.ReduceAI("open-questions", pipeline.ReduceAISpec{
		Binding: model.Binding{Tier: model.TierFast},
		System:  "You distill research claims into the open questions they imply.",
		Prompt: "Distill {{.Count}} claims into the open research questions they imply:\n" +
			"{{range .Items}}- {{.}}\n{{end}}",
		FanIn:     6,
		ItemField: "claim",
	})

	return p
}

// --- mock models ------------------------------------------------------------

// buildRegistry wires three mock tiers: a jittery fast "scout" with scripted
// transient failures, a mid-tier "analyst", and a slow, pricey "oracle" that
// serves as the escalation target for garbled output and invalid grades.
func buildRegistry() (*model.Registry, error) {
	reg := model.NewRegistry()

	// Transient failures scattered across the scout's call sequence: each
	// scripted error consumes one call, so these surface as retry orbits in
	// different stages of the sky.
	transientAt := map[int]string{
		6:   "429: rate limited (scripted)",
		17:  "503: upstream connection reset (scripted)",
		34:  "429: rate limited (scripted)",
		59:  "529: provider overloaded (scripted)",
		88:  "503: upstream connection reset (scripted)",
		104: "429: rate limited (scripted)",
	}
	sched := make([]error, 110)
	for i, msg := range transientAt {
		sched[i] = core.Transient(errors.New(msg))
	}

	scout := model.NewMock("mock-scout",
		model.WithFailures(sched...),
		model.WithHandler(func(req model.Request) (string, error) {
			time.Sleep(time.Duration(250+rand.Intn(450)) * time.Millisecond)
			if strings.HasPrefix(req.Prompt, "Screen") {
				switch {
				case strings.Contains(req.Prompt, "Retracted:"):
					return "", core.Permanent(errors.New("paper withdrawn by publisher — content unavailable (scripted)"))
				case strings.Contains(req.Prompt, "Trillion-Token"):
					time.Sleep(11 * time.Second) // the long-running star
				case strings.Contains(req.Prompt, "Emergent Deception"):
					return "§§ screening telemetry corrupted — not JSON §§", nil // semantic → escalate
				}
			}
			if strings.HasPrefix(req.Prompt, "Extract") &&
				strings.Contains(req.Prompt, "Distilling Tool-Use") {
				return "&&& extraction buffer overrun — not JSON &&&", nil // semantic → escalate
			}
			return respond(req)
		}))
	if err := reg.Register(model.Info{
		ID: "mock-scout", Provider: scout, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 0.60, OutputPerMTok: 3.00},
	}); err != nil {
		return nil, err
	}

	analyst := model.NewMock("mock-analyst",
		model.WithHandler(func(req model.Request) (string, error) {
			time.Sleep(time.Duration(450+rand.Intn(550)) * time.Millisecond)
			if strings.HasPrefix(req.Prompt, "Grade") &&
				strings.Contains(req.Prompt, "Self-Rewarding") {
				// Valid JSON, invalid grade: fails Validate → escalates.
				return `{"evidence": 9, "rationale": "confidence uncalibrated (scripted invalid grade)"}`, nil
			}
			return respond(req)
		}))
	if err := reg.Register(model.Info{
		ID: "mock-analyst", Provider: analyst, Tier: model.TierBalanced,
		Pricing: model.Pricing{InputPerMTok: 3.00, OutputPerMTok: 15.00},
	}); err != nil {
		return nil, err
	}

	oracle := model.NewMock("mock-oracle",
		model.WithHandler(func(req model.Request) (string, error) {
			time.Sleep(time.Duration(900+rand.Intn(900)) * time.Millisecond)
			return respond(req)
		}))
	if err := reg.Register(model.Info{
		ID: "mock-oracle", Provider: oracle, Tier: model.TierDeep,
		Pricing: model.Pricing{InputPerMTok: 15.00, OutputPerMTok: 75.00},
	}); err != nil {
		return nil, err
	}
	return reg, nil
}

// respond is the shared deterministic "model": every prompt in the pipeline
// starts with a distinct verb, and answers are derived from the prompt text
// so reruns are cache-stable.
func respond(req model.Request) (string, error) {
	p := req.Prompt
	switch {
	case strings.HasPrefix(p, "Screen"):
		for _, a := range areas {
			if strings.Contains(p, a) {
				return fmt.Sprintf(`{"relevant": true, "topic": %q}`, a), nil
			}
		}
		return `{"relevant": false, "topic": "out of scope"}`, nil

	case strings.HasPrefix(p, "Extract"):
		title := between(p, "Extract findings from ", ",")
		gain := 4 + hash(title)%19 // deterministic 4..22%
		findings := []string{
			fmt.Sprintf("%q improves long-horizon task success by %d%% over the strongest baseline.", firstWords(title, 6), gain),
			"The effect persists across three model scales in ablation.",
		}
		if hash(title)%2 == 0 {
			findings = append(findings, "Independent replications agree within ±2 points.")
		}
		if strings.HasPrefix(title, "Toward") {
			findings = append(findings, "Projected gains beyond the tested regime (speculative).")
		}
		quoted := make([]string, len(findings))
		for i, f := range findings {
			quoted[i] = fmt.Sprintf("%q", f)
		}
		return fmt.Sprintf(`{"headline": "%s: +%d%% task success, stable across scales", "findings": [%s]}`,
			jsonSafe(firstWords(title, 6)), gain, strings.Join(quoted, ", ")), nil

	case strings.HasPrefix(p, "Grade"):
		head := between(p, "evidence for: ", "\n")
		score := 2 + hash(head)%4 // deterministic 2..5
		return fmt.Sprintf(`{"evidence": %d, "rationale": "holdout evaluation with %d independent runs; effect size stable"}`,
			score, 3+hash(head)%5), nil

	case strings.HasPrefix(p, "Synthesize"):
		n := strings.Count(p, "\n- ")
		return fmt.Sprintf("Synthesis over %d findings: gains concentrate in planning, tool use, and "+
			"multi-agent verification; memory results are promising but mixed; several safety findings "+
			"(deception, reward hacking, sandbagging) replicate and warrant standing audits.", n), nil

	case strings.HasPrefix(p, "Distill"):
		n := strings.Count(p, "\n- ")
		return fmt.Sprintf("From %d claims, three open questions: (1) do reported gains survive "+
			"distribution shift off-benchmark? (2) do memory consolidation and plan search compose, "+
			"or compete for the same context budget? (3) which invariants keep self-improvement "+
			"auditable as capability grows?", n), nil

	case strings.HasPrefix(p, "Write a three-sentence"):
		return "This survey of 41 papers finds that self-improvement gains are real but uneven: " +
			"planning, tool use, and multi-agent verification show replicated double-digit improvements, " +
			"while long-horizon memory remains bottlenecked on consolidation. Safety findings — emergent " +
			"deception, reward hacking, and sandbagging — replicate across labs and argue for auditable " +
			"lineage and least-privilege execution as defaults. We recommend cost-normalized evaluation " +
			"and standing adversarial audits before further capability scaling.", nil
	}
	return "Acknowledged.", nil
}

// --- small helpers ----------------------------------------------------------

func hash(s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() % 1000)
}

func firstWords(s string, n int) string {
	words := strings.Fields(s)
	if len(words) > n {
		words = words[:n]
	}
	return strings.Join(words, " ")
}

func between(s, after, until string) string {
	if i := strings.Index(s, after); i >= 0 {
		s = s[i+len(after):]
	}
	if i := strings.Index(s, until); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

func jsonSafe(s string) string {
	q := fmt.Sprintf("%q", s)
	return q[1 : len(q)-1]
}
