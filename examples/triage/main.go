// Command triage demonstrates a complete Loom pipeline offline: it uses a
// deterministic mock model, so it runs without any API key.
//
//	go run ./examples/triage
//
// The pipeline classifies support tickets with a "model", filters the urgent
// ones, and tree-aggregates them into a briefing. Run it twice with
// LOOM_STATE=./state to watch the second run replay entirely from the
// content-addressed cache (zero model calls).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	loom "github.com/zionrubin/brian-ai/loom"
	"github.com/zionrubin/brian-ai/loom/core"
	"github.com/zionrubin/brian-ai/loom/model"
	"github.com/zionrubin/brian-ai/loom/observe"
	"github.com/zionrubin/brian-ai/loom/pipeline"
)

func main() {
	// --- A deterministic "model" so the example runs offline. ------------
	reg := model.NewRegistry()
	_, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			if strings.Contains(req.Prompt, "Write one paragraph") {
				n := strings.Count(req.Prompt, "- ")
				return fmt.Sprintf("Briefing over %d urgent items: mostly billing.", n), nil
			}
			urgent := strings.Contains(req.Prompt, "URGENT")
			category := "general"
			if strings.Contains(req.Prompt, "refund") {
				category = "billing"
			} else if strings.Contains(req.Prompt, "crash") {
				category = "bug"
			}
			return fmt.Sprintf(`{"category": %q, "urgent": %v}`, category, urgent), nil
		}))
	if err != nil {
		log.Fatal(err)
	}

	// --- The pipeline. ---------------------------------------------------
	tickets := []core.Record{
		core.NewRecord("t1", map[string]any{"subject": "URGENT refund not processed"}),
		core.NewRecord("t2", map[string]any{"subject": "URGENT app crash on login"}),
		core.NewRecord("t3", map[string]any{"subject": "question about pricing"}),
		core.NewRecord("t4", map[string]any{"subject": "URGENT charged twice, need refund"}),
		core.NewRecord("t5", map[string]any{"subject": "feature request: dark mode"}),
	}

	p := pipeline.New("ticket-triage")
	src := p.FromRecords("tickets", tickets)
	classified := src.Infer("classify", pipeline.InferSpec{
		Binding:   model.Binding{Tier: model.TierFast},
		System:    "You classify support tickets.",
		Prompt:    "Classify this ticket: {{.subject}}",
		ParseJSON: true,
	})
	classified.
		Filter("urgent-only", func(r core.Record) (bool, error) {
			b, _ := r.Data["urgent"].(bool)
			return b, nil
		}).
		ReduceAI("briefing", pipeline.ReduceAISpec{
			Binding:   model.Binding{Tier: model.TierFast},
			Prompt:    "Write one paragraph summarizing {{.Count}} urgent tickets:\n{{range .Items}}- {{.}}\n{{end}}",
			FanIn:     3,
			ItemField: "category",
		})

	// --- Run. ------------------------------------------------------------
	opts := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithWorkers(4),
		loom.WithEventHandler(func(e observe.Event) {
			if e.Type == observe.ModelCalled || e.Type == observe.CacheHit {
				fmt.Printf("  [%s] stage=%s model=%s cost=$%.5f\n", e.Type, e.Stage, e.Model, e.Usage.CostUSD)
			}
		}),
	}
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		opts = append(opts, loom.WithStateDir(dir))
	}

	res, err := loom.Run(context.Background(), p, opts...)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n--- briefing ---")
	for _, r := range res.Output {
		fmt.Println(r.String("output"))
	}
	fmt.Println("\n--- report ---")
	fmt.Print(res.Report.String())
	fmt.Printf("\naudit entries: %d, lineage entries: %d\n", len(res.Audit), len(res.Lineage))
}
