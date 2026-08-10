// Command example is a Loom pipeline scaffold that runs offline against mock
// models, so it costs nothing and needs no API key until you point it at a
// real provider.
//
//	go run ./examples/example              # run it
//	go run ./examples/example -explain     # price it without calling anything
//	LOOM_STATE=./state go run ./examples/example   # twice: the second is free
//
// The graph: items in → classify each one (fast tier, escalating on invalid
// output) → keep the severe ones → tree-reduce them into one digest.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
)

// ---------------------------------------------------------------------------
// 1. The pipeline
//
// Keep this a function rather than inlining it in main: it is what lets the
// test exercise the same graph the binary runs, and what lets loom.Explain
// price it without a main loop around it.
// ---------------------------------------------------------------------------

// rubric is the part of the prompt that never varies. Putting it in Prefix
// rather than Prompt means it is rendered once per task and sent as the same
// leading bytes on every call, so the provider's prompt cache serves it
// instead of reprocessing it per record.
const rubric = `Severity levels:
  high   — blocks the user, or costs them money
  medium — degrades the experience but has a workaround
  low    — cosmetic, or a request rather than a problem
Answer with a single JSON object and nothing else.`

func build(items []core.Record) *pipeline.Pipeline {
	p := pipeline.New("example")

	src := p.FromRecords("items", items)

	classified := src.Infer("classify", pipeline.InferSpec{
		// Bind to a tier, not a model ID: the registry decides what "fast"
		// means, so swapping providers is a registry change, not a pipeline
		// change. The ladder is tried on *semantic* failures only — output
		// that parsed as JSON but failed Validate, or did not parse at all.
		Binding:   model.Binding{Tier: model.TierFast, Escalation: []string{"mock-deep"}},
		System:    "You trage inbound product feedback.",
		Prefix:    rubric,
		Prompt:    "Classify this item.\n\nItem: {{.text}}",
		MaxTokens: 200,
		ParseJSON: true, // merges the JSON object into the record's Data
		Validate: func(r core.Record) error {
			switch r.String("severity") {
			case "high", "medium", "low":
				return nil
			}
			// A semantic failure, so the retry escalates instead of asking
			// the same model the same question again.
			return fmt.Errorf("bad severity %q", r.String("severity"))
		},
	})

	classified.
		// WithVersion is what makes a Go-func stage cacheable: closures are
		// not content-addressable, so this string stands in for the function
		// body. Bump it whenever you change the logic below.
		Filter("severe-only", func(r core.Record) (bool, error) {
			return r.String("severity") == "high", nil
		}, pipeline.WithVersion("v1")).
		ReduceAI("digest", pipeline.ReduceAISpec{
			Binding:   model.Binding{Tier: model.TierDeep},
			System:    "You write short, concrete engineering digests.",
			Prompt:    "Summarize these {{.Count}} high-severity items in one paragraph:\n{{range .Items}}- {{.}}\n{{end}}",
			FanIn:     8,
			MaxTokens: 400,
			ItemField: "topic", // which field of each record feeds the reduce
		})

	return p
}

// ---------------------------------------------------------------------------
// 2. The input
// ---------------------------------------------------------------------------

func items() []core.Record {
	return []core.Record{
		core.NewRecord("i1", map[string]any{"text": "Checkout charges the card twice on retry."}),
		core.NewRecord("i2", map[string]any{"text": "The export button is misaligned on Safari."}),
		core.NewRecord("i3", map[string]any{"text": "Login loops forever after a password reset."}),
		core.NewRecord("i4", map[string]any{"text": "Would love a dark mode."}),
		core.NewRecord("i5", map[string]any{"text": "Invoices show the wrong tax rate for the EU."}),
	}
}

// ---------------------------------------------------------------------------
// 3. The models
//
// Develop against mocks. They are deterministic, free, and model the provider
// prompt cache, so the cache accounting you see offline is the accounting you
// get in production. Swap this one function for a real provider when the shape
// of the pipeline is settled — see references/providers.md.
// ---------------------------------------------------------------------------

func registry() (*model.Registry, error) {
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(classify)); err != nil {
		return nil, err
	}
	// The escalation rung. Registering it under a tier as well means the
	// reduce stage can bind to TierDeep by name.
	if _, err := model.RegisterMock(reg, "mock-deep", model.TierDeep,
		model.WithHandler(classify)); err != nil {
		return nil, err
	}
	return reg, nil
}

// classify answers both stages: the reduce prompt is recognizable by its
// leading verb, everything else is a classification.
func classify(req model.Request) (string, error) {
	if strings.Contains(req.Prompt, "Summarize these") {
		n := strings.Count(req.Prompt, "\n- ")
		return fmt.Sprintf("Digest of %d high-severity items: billing and auth dominate.", n), nil
	}
	severity, topic := "low", "request"
	switch {
	case strings.Contains(req.Prompt, "charges") || strings.Contains(req.Prompt, "tax"):
		severity, topic = "high", "billing"
	case strings.Contains(req.Prompt, "Login"):
		severity, topic = "high", "auth"
	case strings.Contains(req.Prompt, "misaligned"):
		severity, topic = "low", "ui"
	}
	return fmt.Sprintf(`{"severity": %q, "topic": %q}`, severity, topic), nil
}

// ---------------------------------------------------------------------------
// 4. Running it
// ---------------------------------------------------------------------------

func main() {
	explainOnly := flag.Bool("explain", false, "price the pipeline without running it")
	budget := flag.Float64("budget", 1.0, "hard USD ceiling for the run")
	workers := flag.Int("workers", 8, "concurrent tasks")
	flag.Parse()

	reg, err := registry()
	if err != nil {
		log.Fatal(err)
	}
	p := build(items())

	opts := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithWorkers(*workers),
		loom.WithRunBudget(core.Budget{MaxCostUSD: *budget}),
		// ParseJSON is the one operator whose output *shape* no plan can know,
		// because the field names come from the model. Without this, the filter
		// below "classify" sees no "severity" field, drops every record, and
		// everything downstream projects as zero work — an under-count, which
		// is the one direction a ceiling must not be wrong in. Name the fields
		// once and the projection is exact. Explain-only options are ignored by
		// Run, so the same slice describes the run and prices it.
		loom.WithStageSample("classify", map[string]any{
			"severity": "high", // the value that makes the most downstream work
			"topic":    "billing",
		}),
	}
	// Persisting state makes a rerun replay completed work instead of paying
	// for it twice — the single most useful flag while iterating on prompts.
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		opts = append(opts, loom.WithStateDir(dir))
	}

	// Price it before spending. Explain issues no calls, resolves no secrets,
	// and opens no sockets, so this is safe to run against any config.
	proj, err := loom.Explain(p, opts...)
	if err != nil {
		log.Fatalf("explain: %v", err)
	}
	fmt.Print(proj.String())
	if *explainOnly {
		return
	}
	if !proj.FitsBudget() {
		log.Fatalf("projected ceiling $%.4f exceeds the $%.2f budget; raise -budget or trim the pipeline",
			proj.Ceiling().CostUSD, *budget)
	}

	opts = append(opts, loom.WithEventHandler(func(e observe.Event) {
		if e.Type == observe.ModelCalled || e.Type == observe.CacheHit {
			fmt.Printf("  [%s] stage=%s model=%s cost=$%.5f\n", e.Type, e.Stage, e.Model, e.Usage.CostUSD)
		}
	}))

	res, err := loom.Run(context.Background(), p, opts...)
	if err != nil {
		// res is still populated on budget/failure aborts — partial results
		// are results.
		log.Fatalf("run: %v (spent $%.4f)", err, res.Spent.CostUSD)
	}

	fmt.Println("\n--- classified ---")
	for _, r := range res.StageOutputs["classify"] {
		fmt.Printf("%s: severity=%s topic=%s\n", r.ID, r.String("severity"), r.String("topic"))
	}
	fmt.Println("\n--- digest ---")
	for _, r := range res.Output {
		fmt.Println(r.String("output"))
	}
	fmt.Println("\n--- report ---")
	fmt.Print(res.Report.String())
}
