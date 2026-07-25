// Command constellation runs a Loom pipeline offline (deterministic mock
// models, no API keys) while serving the live constellation view — open the
// printed URL and watch tasks and executors light up, pulse, grow activity
// rings, flash to completion, and fail, in real time. Click any star for
// its full detail: stage, executor, model, input, runtime, tokens, cost,
// retries, log, and errors.
//
//	go run ./examples/constellation
//	# then open http://localhost:8077
//
// The mock models are scripted so a single run shows every visual state:
// jittered latency, one slow straggler (long-running ring), transient
// failures (retries), a semantic failure that escalates up the model
// ladder, and one permanently failing record (a dead-lettered red star).
// Run with LOOM_STATE=/tmp/loom twice to watch the second run settle
// instantly from the cache.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
		loom.WithWorkers(5),
		loom.WithContinueOnError(),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 5.00}),
		loom.WithEventHandler(v.Handle),
	}
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		opts = append(opts, loom.WithStateDir(dir))
	}

	res, err := loom.Run(ctx, p, opts...)
	if err != nil {
		fmt.Printf("\nrun ended with error: %v\n", err)
	}
	if res != nil {
		fmt.Println("\n--- briefing ---")
		for _, r := range res.Output {
			fmt.Println(r.String("output"))
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

// buildRegistry wires two mock models: a jittery fast tier (with scripted
// transient failures spread across early calls) and a slower, pricier deep
// tier used as the escalation target.
func buildRegistry() (*model.Registry, error) {
	reg := model.NewRegistry()

	fast := model.NewMock("mock-fast",
		model.WithFailures(
			nil, nil, nil, core.Transient(errors.New("429: rate limited (scripted)")),
			nil, nil, nil, nil, nil, core.Transient(errors.New("503: upstream flapped (scripted)")),
			nil, nil, nil, nil, core.Transient(errors.New("429: rate limited (scripted)")),
		),
		model.WithHandler(func(req model.Request) (string, error) {
			// Jittered "inference" latency so the sky moves organically.
			time.Sleep(time.Duration(250+rand.Intn(650)) * time.Millisecond)
			switch {
			case strings.Contains(req.Prompt, "corrupted"):
				return "", core.Permanent(errors.New("payload failed content policy (scripted)"))
			case strings.Contains(req.Prompt, "straggler"):
				time.Sleep(11 * time.Second) // the long-running star
			case strings.Contains(req.Prompt, "garbled"):
				return "&%$ not json §§", nil // semantic failure → escalate
			}
			return respond(req), nil
		}))
	if err := reg.Register(model.Info{
		ID: "mock-fast", Provider: fast, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 0.80, OutputPerMTok: 4.00},
	}); err != nil {
		return nil, err
	}

	deep := model.NewMock("mock-deep",
		model.WithHandler(func(req model.Request) (string, error) {
			time.Sleep(time.Duration(900+rand.Intn(900)) * time.Millisecond)
			return respond(req), nil
		}))
	if err := reg.Register(model.Info{
		ID: "mock-deep", Provider: deep, Tier: model.TierDeep,
		Pricing: model.Pricing{InputPerMTok: 15.00, OutputPerMTok: 75.00},
	}); err != nil {
		return nil, err
	}
	return reg, nil
}

// respond is the shared deterministic "model": classification JSON for
// classify prompts, prose for everything else.
func respond(req model.Request) string {
	if strings.Contains(req.Prompt, "Classify") {
		urgent := strings.Contains(req.Prompt, "URGENT")
		category := "general"
		if strings.Contains(req.Prompt, "refund") || strings.Contains(req.Prompt, "charged") {
			category = "billing"
		} else if strings.Contains(req.Prompt, "crash") || strings.Contains(req.Prompt, "error") {
			category = "bug"
		}
		return fmt.Sprintf(`{"category": %q, "urgent": %v}`, category, urgent)
	}
	if strings.Contains(req.Prompt, "Summarize") {
		n := strings.Count(req.Prompt, "- ")
		return fmt.Sprintf("Briefing over %d urgent replies: billing dominates; two bugs escalated.", n)
	}
	return "Thanks for reaching out — we've flagged this as urgent and are on it."
}

func buildPipeline() *pipeline.Pipeline {
	subjects := []string{
		"URGENT refund not processed for order 1189",
		"URGENT app crash on login after update",
		"question about pricing tiers",
		"URGENT charged twice, need refund",
		"feature request: dark mode",
		"URGENT export button throws error 500",
		"how do I rotate my API key?",
		"URGENT invoice totals are wrong",
		"praise: the new editor is great",
		"URGENT sync stuck for 3 days (straggler)",
		"billing question about proration",
		"URGENT crash when opening settings",
		"docs link is broken on the pricing page",
		"URGENT refund promised last week, nothing yet",
		"corrupted attachment from customer",  // permanent failure → dead letter
		"URGENT garbled telemetry from agent", // semantic → escalates to mock-deep
		"can I self-host the connector?",
		"URGENT payment declined but order charged",
		"typo in the welcome email",
		"URGENT error importing workspace backup",
		"request: webhook retries configuration",
		"URGENT dashboard shows stale data",
	}
	tickets := make([]core.Record, len(subjects))
	for i, s := range subjects {
		tickets[i] = core.NewRecord(fmt.Sprintf("t%02d", i+1), map[string]any{"subject": s})
	}

	p := pipeline.New("support-copilot")
	src := p.FromRecords("tickets", tickets)

	classified := src.Infer("classify", pipeline.InferSpec{
		Binding: model.Binding{
			Tier:       model.TierFast,
			Escalation: []string{"mock-deep"}, // semantic failures climb here
		},
		System:    "You classify support tickets.",
		Prompt:    "Classify this ticket: {{.subject}}",
		ParseJSON: true,
	})

	urgent := classified.Filter("urgent-only", func(r core.Record) (bool, error) {
		b, _ := r.Data["urgent"].(bool)
		return b, nil
	}, pipeline.WithVersion("v1"))

	drafted := urgent.Infer("draft-reply", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast},
		System:  "You draft empathetic support replies.",
		Prompt:  "Draft a short reply for this {{.category}} ticket: {{.subject}}",
	})

	drafted.ReduceAI("briefing", pipeline.ReduceAISpec{
		Binding:   model.Binding{Tier: model.TierFast},
		Prompt:    "Summarize {{.Count}} support replies into one line:\n{{range .Items}}- {{.}}\n{{end}}",
		FanIn:     3,
		ItemField: "output",
	})

	return p
}
