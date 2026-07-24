// Command openai-review runs a real Loom pipeline against the OpenAI API:
// it classifies product reviews with GPT-5.4 nano (fast tier, escalating to
// GPT-5.4 mini on invalid output), triages severity in a pure Go stage
// (fused by the planner and cached like a model call), then branches:
// problem reviews get a suggested support reply drafted per record on
// GPT-5.4 mini, while every review feeds an executive summary synthesized
// with GPT-5.4 — all while serving the live constellation view so you can
// watch tasks and executors light up in real time.
//
// Requires OPENAI_API_KEY. The run is capped at $0.50 by the budget
// governor, and with LOOM_STATE set, reruns replay from the cache for free.
//
//	OPENAI_API_KEY=sk-... go run ./examples/openai-review
//	# then open http://localhost:8077
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/providers/openai"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/viz"
)

func main() {
	addr := flag.String("addr", "localhost:8077", "address for the constellation view")
	flag.Parse()

	key := os.Getenv("OPENAI_API_KEY")
	if key == "" {
		log.Fatal("set OPENAI_API_KEY to run this example")
	}

	reg := model.NewRegistry()
	// Registers gpt-5.4 (deep), gpt-5.4-mini (balanced), and gpt-5.4-nano
	// (fast) with real pricing; admission control at 50 req/min per model.
	if err := openai.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 50}); err != nil {
		log.Fatal(err)
	}

	reviews := []core.Record{
		core.NewRecord("r1", map[string]any{"text": "The battery died after two weeks. Support never answered my emails. Very disappointed."}),
		core.NewRecord("r2", map[string]any{"text": "Absolutely love it — setup took five minutes and it just works."}),
		core.NewRecord("r3", map[string]any{"text": "Decent product but the companion app crashes constantly on Android 15."}),
		core.NewRecord("r4", map[string]any{"text": "Shipping took a month and the box arrived damaged, though the device itself is fine."}),
		core.NewRecord("r5", map[string]any{"text": "Overpriced for what it does. Returned it after three days — at least the refund was painless."}),
		core.NewRecord("r6", map[string]any{"text": "Solid build quality and great sound. Battery life could be better, but I'm happy overall."}),
	}

	p := pipeline.New("review-insights")
	src := p.FromRecords("reviews", reviews)
	classified := src.Infer("classify", pipeline.InferSpec{
		// Fast tier with a semantic-failure escalation ladder: if nano
		// produces output that fails JSON parsing/validation, the retry runs
		// on mini automatically.
		Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"gpt-5.4-mini"}},
		System:  "You extract structured insights from product reviews. Respond with a single JSON object and nothing else.",
		Prompt: "Analyze this review and respond with JSON {\"sentiment\": \"positive|negative|mixed\", " +
			"\"issue\": \"<main issue or 'none'>\"}.\n\nReview: {{.text}}",
		MaxTokens: 200,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			switch r.String("sentiment") {
			case "positive", "negative", "mixed":
				return nil
			}
			return fmt.Errorf("bad sentiment %q", r.String("sentiment"))
		},
	})
	// Pure Go triage: derive a severity from the classification. Pure stages
	// carry no model binding — the planner fuses adjacent ones into a single
	// task boundary, and WithVersion makes them cacheable like model calls.
	triaged := classified.Map("severity", func(r core.Record) (core.Record, error) {
		out := r.Clone()
		switch r.String("sentiment") {
		case "negative":
			out.Data["severity"] = "high"
		case "mixed":
			out.Data["severity"] = "medium"
		default:
			out.Data["severity"] = "low"
		}
		return out, nil
	}, pipeline.WithVersion("v1"))

	// Branch 1: problem reviews only — each gets a suggested support reply,
	// one model call per record on the balanced tier.
	problems := triaged.Filter("problems-only", func(r core.Record) (bool, error) {
		return r.String("sentiment") != "positive", nil
	}, pipeline.WithVersion("v1"))
	problems.Infer("draft-reply", pipeline.InferSpec{
		Binding: model.Binding{Model: "gpt-5.4-mini"},
		System:  "You write empathetic, concrete customer-support replies.",
		Prompt: "Draft a two-sentence reply to this customer. Acknowledge their main issue " +
			"({{.issue}}, severity {{.severity}}) and offer one concrete next step.\n\nReview: {{.text}}",
		MaxTokens:   200,
		OutputField: "reply",
	})

	// Branch 2: every review — not just the problems — feeds the summary.
	triaged.ReduceAI("summary", pipeline.ReduceAISpec{
		Binding:   model.Binding{Model: "gpt-5.4"},
		System:    "You write crisp executive summaries.",
		Prompt:    "Synthesize the key product issues from {{.Count}} review analyses:\n{{range .Items}}- {{.}}\n{{end}}",
		FanIn:     8,
		MaxTokens: 400,
		ItemField: "issue",
	})

	v := viz.New()
	url, err := v.Start(*addr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("constellation view: %s\n", url)
	fmt.Println("waiting up to 60s for a browser to connect (Ctrl-C to abort)…")
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 60*time.Second)
	if v.AwaitViewer(waitCtx) {
		fmt.Println("viewer connected — starting the run")
		time.Sleep(800 * time.Millisecond) // a beat, so the empty sky is visible first
	} else {
		fmt.Println("no viewer yet — running anyway (the page replays state on connect)")
	}
	cancelWait()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := []loom.Option{
		loom.WithRegistry(reg),
		// The broker holds the key; tasks resolve it per call under their
		// granted capabilities, and every resolution is audited.
		loom.WithSecrets(map[security.SecretRef]string{openai.DefaultSecretRef: key}),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 0.50}),
		loom.WithWorkers(4),
		loom.WithEventHandler(v.Handle),
	}
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		opts = append(opts, loom.WithStateDir(dir))
	}

	res, err := loom.Run(ctx, p, opts...)
	if err != nil {
		spent := 0.0
		if res != nil {
			spent = res.Spent.CostUSD
		}
		fmt.Printf("\nrun ended with error: %v (spent $%.4f)\n", err, spent)
	}
	if res != nil {
		// The pipeline has two terminal stages (draft-reply and summary), so
		// results are read per stage rather than from res.Output.
		fmt.Println("\n--- per-review analysis ---")
		for _, r := range res.StageOutputs["severity"] {
			fmt.Printf("%s: sentiment=%s severity=%s issue=%s\n",
				r.ID, r.String("sentiment"), r.String("severity"), r.String("issue"))
		}
		fmt.Println("\n--- suggested replies (problem reviews only) ---")
		for _, r := range res.StageOutputs["draft-reply"] {
			fmt.Printf("%s: %s\n", r.ID, r.String("reply"))
		}
		fmt.Println("\n--- executive summary ---")
		for _, r := range res.StageOutputs["summary"] {
			fmt.Println(r.String("output"))
		}
		fmt.Println("\n--- report ---")
		fmt.Print(res.Report.String())
	}

	fmt.Printf("\nrun finished — still serving %s (Ctrl-C to exit)\n", url)
	<-ctx.Done()
	_ = v.Close()
}
