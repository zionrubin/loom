// Command salesforce-briefing builds a pre-meeting partner brief by joining
// two worlds: the local Salesforce Insurance MCP server as the data plane
// (grant-checked tools) and the OpenAI provider as the reasoning plane.
//
// The pipeline fans each partner out into facets (overview, team, recent
// communications, meeting insights, ops tickets), fetches each facet from
// the MCP server in parallel under least-privilege tool grants, distills
// each facet with GPT-5.4 nano (escalating to mini on invalid output), and
// composes the final brief with GPT-5.4 via tree reduction — while serving
// the live constellation view (http://localhost:8077) so you can watch the
// MCP fetches and model calls light up in real time.
//
// Requires OPENAI_API_KEY and the salesforce-insurance MCP server running
// locally. Pass -mock to run offline against a deterministic model (still
// hitting the real MCP server).
//
//	OPENAI_API_KEY=sk-... go run ./examples/salesforce-briefing "Ethos"
//	go run ./examples/salesforce-briefing -mock "Ethos" "360 Reviews"
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	loom "github.com/zionrubin/brian-ai/loom"
	"github.com/zionrubin/brian-ai/loom/core"
	"github.com/zionrubin/brian-ai/loom/model"
	"github.com/zionrubin/brian-ai/loom/pipeline"
	"github.com/zionrubin/brian-ai/loom/providers/openai"
	"github.com/zionrubin/brian-ai/loom/security"
	"github.com/zionrubin/brian-ai/loom/viz"
)

// facets are the angles a good pre-meeting brief covers. Each maps to one
// read-only MCP tool; the fetch stage is granted exactly these tools.
var facets = []struct {
	name string
	tool string
	args map[string]any
	ask  string
}{
	{"overview", "get-partner-overview", nil,
		"identity, status, tier, vertical, engagement profile and commercial status"},
	{"team", "get-partner-team", nil,
		"who owns this partner internally and who is acting today"},
	{"communications", "get-recent-partner-communications", map[string]any{"limit": 10},
		"the latest touchpoints — emails, calls, meetings — and what they were about"},
	{"meetings", "get-meeting-insights", map[string]any{"limit": 30},
		"topics and competitor mentions detected in recorded meetings"},
	{"tickets", "get-ops-tickets", map[string]any{"limit": 15},
		"open operational work and delivered content"},
}

func main() {
	mcpURL := flag.String("mcp", "http://localhost:6651/external/mcp/salesforce-insurance",
		"endpoint of the salesforce-insurance MCP server")
	mock := flag.Bool("mock", false, "use a deterministic mock model instead of OpenAI (no key needed)")
	addr := flag.String("addr", "localhost:8077", "address for the constellation view")
	flag.Parse()

	partners := flag.Args()
	if len(partners) == 0 {
		partners = []string{"Ethos"}
	}

	// --- Model plane: OpenAI (or an offline mock). ------------------------
	reg := model.NewRegistry()
	secrets := map[security.SecretRef]string{}
	if *mock {
		registerMockModels(reg)
	} else {
		key := os.Getenv("OPENAI_API_KEY")
		if key == "" {
			log.Fatal("set OPENAI_API_KEY, or pass -mock to run offline")
		}
		if err := openai.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 50}); err != nil {
			log.Fatal(err)
		}
		secrets[openai.DefaultSecretRef] = key
	}

	// --- Data plane: the Salesforce MCP server, one Loom tool per MCP tool.
	sf := newMCPClient(*mcpURL)
	var toolCaps []security.Capability
	loomOpts := []loom.Option{}
	for _, f := range facets {
		loomOpts = append(loomOpts, loom.WithTools(sf.tool(f.tool)))
		toolCaps = append(toolCaps, security.ToolCap(f.tool))
	}

	// --- The pipeline. -----------------------------------------------------
	// Seed one record per (partner, facet) — the unit of parallel fetching.
	// (Fanning out with a FlatMap would work too, but the planner fuses
	// adjacent pure stages into one task boundary, which would serialize the
	// five MCP calls per partner inside a single task.)
	var seeds []core.Record
	for _, name := range partners {
		for _, f := range facets {
			args := map[string]any{"partner": name}
			for k, v := range f.args {
				args[k] = v
			}
			seeds = append(seeds, core.NewRecord(name+"/"+f.name, map[string]any{
				"partner": name, "facet": f.name, "tool": f.tool,
				"args": args, "ask": f.ask,
			}))
		}
	}

	p := pipeline.New("partner-briefing")
	src := p.FromRecords("facets", seeds)

	// Fetch each facet from Salesforce via MCP. The envelope grants exactly
	// the five read-only tools; anything else is denied and audited.
	fetched := src.MapTools("fetch", func(ctx context.Context, tools core.Tools, r core.Record) (core.Record, error) {
		args, _ := r.Data["args"].(map[string]any)
		payload, err := tools.Invoke(ctx, r.String("tool"), args)
		if err != nil {
			return core.Record{}, err
		}
		out := r.Clone()
		out.Data["payload"] = payload
		delete(out.Data, "args")
		return out, nil
	},
		pipeline.WithGrants(toolCaps...),
		pipeline.WithVersion("v1"), // cacheable: reruns replay CRM fetches too
		pipeline.WithParallelism(5),
	)

	// Distill each facet with the fast tier; invalid JSON escalates to mini.
	distilled := fetched.Infer("distill", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"gpt-5.4-mini"}},
		System: "You prepare factual pre-meeting notes from CRM data. " +
			"Respond with a single JSON object and nothing else.",
		Prompt: "From this Salesforce data about partner \"{{.partner}}\", extract {{.ask}}. " +
			"Respond with JSON {\"headline\": \"<one-line takeaway>\", " +
			"\"detail\": \"<2-3 sentences of the facts that matter for the meeting>\"}.\n\n" +
			"Data:\n{{.payload}}",
		MaxTokens: 300,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			if strings.TrimSpace(r.String("headline")) == "" {
				return fmt.Errorf("empty headline")
			}
			return nil
		},
	})

	// Label each insight so the reducer sees self-contained items.
	labeled := distilled.Map("label", func(r core.Record) (core.Record, error) {
		out := r.Clone()
		out.Data["brief_item"] = fmt.Sprintf("[%s / %s] %s — %s",
			r.String("partner"), r.String("facet"), r.String("headline"), r.String("detail"))
		return out, nil
	}, pipeline.WithVersion("v1"))

	labeled.ReduceAI("briefing", pipeline.ReduceAISpec{
		Binding: model.Binding{Model: "gpt-5.4"},
		System: "You are a chief of staff writing a pre-meeting brief. " +
			"Be concrete, lead with what matters, flag risks and open items.",
		Prompt: "Compose a pre-meeting partner brief from these {{.Count}} research notes. " +
			"Structure it as: snapshot, relationship & recent activity, risks/competitive signals, " +
			"open items, and suggested talking points.\n\n{{range .Items}}- {{.}}\n{{end}}",
		FanIn:     8,
		MaxTokens: 700,
		ItemField: "brief_item",
	})

	// --- Constellation view: watch the run live. ----------------------------
	v := viz.New()
	url, err := v.Start(*addr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("constellation view: %s\n", url)
	fmt.Println("waiting up to 15s for a browser to connect (Ctrl-C to abort)…")
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 15*time.Second)
	if v.AwaitViewer(waitCtx) {
		fmt.Println("viewer connected — starting the run")
		time.Sleep(800 * time.Millisecond) // a beat, so the empty sky is visible first
	} else {
		fmt.Println("no viewer yet — running anyway (the page replays state on connect)")
	}
	cancelWait()

	// --- Run. ---------------------------------------------------------------
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	loomOpts = append(loomOpts,
		loom.WithEventHandler(v.Handle),
		loom.WithRegistry(reg),
		loom.WithSecrets(secrets),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1.00}),
		loom.WithWorkers(5),
	)
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		loomOpts = append(loomOpts, loom.WithStateDir(dir))
	}

	res, err := loom.Run(ctx, p, loomOpts...)
	if err != nil {
		spent := 0.0
		if res != nil {
			spent = res.Spent.CostUSD
		}
		fmt.Printf("\nrun ended with error: %v (spent $%.4f)\n", err, spent)
	}
	if res == nil {
		return
	}

	fmt.Println("--- facet insights ---")
	for _, r := range res.StageOutputs["distill"] {
		fmt.Printf("%-14s %s\n", r.String("facet")+":", r.String("headline"))
	}
	fmt.Println("\n--- pre-meeting brief ---")
	for _, r := range res.Output {
		fmt.Println(r.String("output"))
	}
	fmt.Println("\n--- report ---")
	fmt.Print(res.Report.String())

	fmt.Printf("\nrun finished — still serving %s (Ctrl-C to exit)\n", url)
	<-ctx.Done()
	_ = v.Close()
}

// registerMockModels mirrors the OpenAI model ids with deterministic
// handlers, so the full pipeline (including the real MCP fetches) runs
// offline.
func registerMockModels(reg *model.Registry) {
	distill := func(req model.Request) (string, error) {
		facet := "facts"
		for _, f := range facets {
			if strings.Contains(req.Prompt, "/ "+f.name+"]") || strings.Contains(req.Prompt, "extract "+f.ask) {
				facet = f.name
			}
		}
		return fmt.Sprintf(`{"headline": "mock %s takeaway", "detail": "Mock distillation of the %s payload (%d chars)."}`,
			facet, facet, len(req.Prompt)), nil
	}
	brief := func(req model.Request) (string, error) {
		n := strings.Count(req.Prompt, "- [")
		return fmt.Sprintf("MOCK BRIEF over %d research notes.", n), nil
	}
	for _, m := range []struct {
		id      string
		tier    model.Tier
		handler func(model.Request) (string, error)
	}{
		{"gpt-5.4-nano", model.TierFast, distill},
		{"gpt-5.4-mini", model.TierBalanced, distill},
		{"gpt-5.4", model.TierDeep, brief},
	} {
		if _, err := model.RegisterMock(reg, m.id, m.tier, model.WithHandler(m.handler)); err != nil {
			log.Fatal(err)
		}
	}
}
