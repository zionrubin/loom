// Command anthropic-review runs a real Loom pipeline against the Anthropic
// API: it classifies product reviews with Claude Haiku 4.5 (fast tier,
// escalating to Claude Sonnet 5 on invalid output) and synthesizes an
// executive summary with Claude Opus 4.8.
//
// Requires ANTHROPIC_API_KEY. The run is capped at $0.50 by the budget
// governor, and with LOOM_STATE set, reruns replay from the cache for free.
//
//	ANTHROPIC_API_KEY=sk-... go run ./examples/anthropic-review
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/providers/anthropic"
	"github.com/zionrubin/loom/security"
)

func main() {
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		log.Fatal("set ANTHROPIC_API_KEY to run this example")
	}

	reg := model.NewRegistry()
	// Registers claude-opus-4-8 (deep), claude-sonnet-5 (balanced), and
	// claude-haiku-4-5 (fast) with real pricing; admission control at
	// 50 req/min per model.
	if err := anthropic.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 50}); err != nil {
		log.Fatal(err)
	}

	reviews := []core.Record{
		core.NewRecord("r1", map[string]any{"text": "The battery died after two weeks. Support never answered my emails. Very disappointed."}),
		core.NewRecord("r2", map[string]any{"text": "Absolutely love it — setup took five minutes and it just works."}),
		core.NewRecord("r3", map[string]any{"text": "Decent product but the companion app crashes constantly on Android 15."}),
		core.NewRecord("r4", map[string]any{"text": "Shipping took a month and the box arrived damaged, though the device itself is fine."}),
	}

	p := pipeline.New("review-insights")
	src := p.FromRecords("reviews", reviews)
	classified := src.Infer("classify", pipeline.InferSpec{
		// Fast tier with a semantic-failure escalation ladder: if Haiku
		// produces output that fails JSON parsing/validation, the retry runs
		// on Sonnet automatically.
		Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"claude-sonnet-5"}},
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
	classified.ReduceAI("summary", pipeline.ReduceAISpec{
		Binding:   model.Binding{Model: "claude-opus-4-8"},
		System:    "You write crisp executive summaries.",
		Prompt:    "Synthesize the key product issues from {{.Count}} review analyses:\n{{range .Items}}- {{.}}\n{{end}}",
		FanIn:     8,
		MaxTokens: 400,
		ItemField: "issue",
	})

	opts := []loom.Option{
		loom.WithRegistry(reg),
		// The broker holds the key; tasks resolve it per call under their
		// granted capabilities, and every resolution is audited.
		loom.WithSecrets(map[security.SecretRef]string{anthropic.DefaultSecretRef: key}),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 0.50}),
		loom.WithWorkers(4),
	}
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		opts = append(opts, loom.WithStateDir(dir))
	}

	res, err := loom.Run(context.Background(), p, opts...)
	if err != nil {
		log.Fatalf("run: %v (spent $%.4f)", err, res.Spent.CostUSD)
	}

	fmt.Println("--- per-review analysis ---")
	for _, r := range res.StageOutputs["classify"] {
		fmt.Printf("%s: sentiment=%s issue=%s\n", r.ID, r.String("sentiment"), r.String("issue"))
	}
	fmt.Println("\n--- executive summary ---")
	for _, r := range res.Output {
		fmt.Println(r.String("output"))
	}
	fmt.Println("\n--- report ---")
	fmt.Print(res.Report.String())
}
