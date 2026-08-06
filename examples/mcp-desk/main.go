// Command mcp-desk demonstrates Loom's Model Context Protocol support, end to
// end and entirely offline: it starts a real MCP server as a child process,
// talks to it over real pipes, and runs a pipeline whose stages call its tools.
//
//	go run ./examples/mcp-desk
//	go run ./examples/mcp-desk -state /tmp/loom-mcp   # then again: replayed free
//	go run ./examples/mcp-desk -view localhost:8077   # watch it
//
// The server is this same binary re-executed with -serve, which is what a real
// deployment does with an npx or uvx command in an mcp.Server descriptor. Three
// things are on show:
//
//   - A deterministic stage that calls one tool per record. Its results are
//     cached like any other stage's, so the second run makes no tool calls at
//     all — an MCP call is work, and Loom does not pay for work twice.
//   - A model that *chooses* a tool, dispatched by mcp.Dispatch. The choice is
//     data in the record, and running it is an ordinary scheduled stage, rather
//     than a loop hidden inside a task.
//   - A resource read from the server once at provisioning and registered as a
//     broadcast, so the prompt that uses it references one copy and recomputes
//     only when the document changes.
//
// The report at the end shows the point of the design: however many records
// and however many agents, one connection was made.
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

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/mcp"
	"github.com/zionrubin/loom/mcp/mcptest"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/viz"
)

func main() {
	serve := flag.Bool("serve", false, "run as the MCP server on stdin/stdout (used internally)")
	state := flag.String("state", os.Getenv("LOOM_STATE"), "state directory for cache/resume")
	view := flag.String("view", "", "serve the constellation view on this address")
	flag.Parse()

	// The child process branch: this is the MCP server.
	if *serve {
		if err := inventoryServer().ServeStdio(); err != nil {
			log.Fatal(err)
		}
		return
	}

	// --- The MCP server, as a descriptor. --------------------------------
	//
	// Credentials would be named here (AuthSecret for an HTTP server,
	// EnvSecrets for a child process) and resolved through the run's broker at
	// provisioning; the descriptor never holds one. Naming the tools is the
	// least-privilege form: this deployment permits three of them, whatever the
	// server grows later.
	inventory := mcp.Stdio("inventory", os.Args[0], "-serve").
		WithTools("lookup_sku", "stock_level", "restock_eta")

	// --- A deterministic "model" so the example runs offline. ------------
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(answer)); err != nil {
		log.Fatal(err)
	}

	opts := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithWorkers(4),

		// One connection for the whole run, made before anything else. Every
		// task leases a call slot from it rather than dialing its own.
		loom.WithMCPServer(inventory),

		// A document that lives behind the server, read once here and shared by
		// reference from then on.
		loom.WithMCPResource("voice", "inventory", "mem://voice"),
	}
	if *state != "" {
		opts = append(opts, loom.WithStateDir(*state))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *view != "" {
		v := viz.New()
		url, err := v.Start(*view)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("constellation view: %s\n\n", url)
		// Wait for a browser before running: this pipeline finishes in
		// milliseconds, and the sky filling is most of what there is to watch.
		// (A fleet shows more here — NewFleet connects its servers before any
		// agent starts, so the empty sky already names them.)
		waitCtx, cancelWait := context.WithTimeout(ctx, 30*time.Second)
		if v.AwaitViewer(waitCtx) {
			time.Sleep(800 * time.Millisecond)
		}
		cancelWait()
		opts = append(opts, loom.WithEventHandler(v.Handle))
	}

	res, err := loom.Run(ctx, desk(), opts...)
	if err != nil {
		log.Fatalf("run: %v", err)
	}

	fmt.Print(res.Report)
	fmt.Println()
	for _, r := range res.Output {
		fmt.Printf("%-6s %s\n", r.ID, r.String("reply"))
	}
	fmt.Println()
	for _, m := range res.MCP {
		fmt.Printf("mcp %s: %d session(s), %d dial(s), %d call(s), %s busy\n",
			m.Server, m.Sessions, m.Dials, m.Calls, m.Busy.Round(1e6))
	}
	if len(res.MCP) > 0 && res.MCP[0].Calls == 0 {
		fmt.Println("(every tool call was replayed from the cache — the second run is free)")
	}

	// The run is over in milliseconds but the sky is the thing you came for:
	// the server ring, which stages called it, and the per-tool breakdown are
	// all still there to read.
	if *view != "" {
		fmt.Println("\nview still serving; press m for the servers, s for the summary; ctrl-c to exit")
		<-ctx.Done()
		return
	}
	fmt.Println("\nwatch it: go run ./examples/mcp-desk -view localhost:8077")
	fmt.Println("  the server is a ring below the stage clusters, its arc the peak calls")
	fmt.Println("  in flight against the ceiling; press m for per-tool timings and callers")
}

// desk is the pipeline: look a SKU up, let the model choose a follow-up tool,
// run it, and write a reply in the voice the server's policy document asks for.
func desk() *pipeline.Pipeline {
	p := pipeline.New("mcp-desk")
	src := p.FromRecords("questions", []core.Record{
		core.NewRecord("q1", map[string]any{"sku": "sku-1", "ask": "is it in stock?"}),
		core.NewRecord("q2", map[string]any{"sku": "sku-2", "ask": "when can you restock?"}),
		core.NewRecord("q3", map[string]any{"sku": "sku-3", "ask": "is it in stock?"}),
	})

	// One tool call per record, from an ordinary Go stage. WithVersion makes it
	// cacheable, which for a lookup is the right call: replaying the recorded
	// answer is as good as asking again. A stage that *writes* through a tool
	// would take WithNoCache instead.
	named := src.MapTools("name", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		out, err := s.Invoke(ctx, mcp.ToolName("inventory", "lookup_sku"),
			map[string]any{"sku": r.String("sku")})
		if err != nil {
			return core.Record{}, err
		}
		r.Data["product"] = mcp.Text(out)
		return r, nil
	}, pipeline.WithMCP("inventory", "lookup_sku"), pipeline.WithVersion("v1"))

	// The model chooses which tool answers the question...
	chosen := named.Infer("choose", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast},
		System:  "You route questions to inventory tools.",
		Prefix: "Tools:\n" + mcp.Describe([]mcp.ToolDesc{
			{Name: "stock_level", Description: "units on hand right now"},
			{Name: "restock_eta", Description: "when the next shipment lands"},
		}),
		Prompt:    `Product {{.product}}. Question: {{.ask}}` + "\n" + `Reply with JSON {"tool": "...", "args": {"sku": "..."}}.`,
		ParseJSON: true,
	})

	// ...and the next stage runs it. Two stages, not a hidden loop: the choice
	// is visible in the record and in the lineage, and the call is an ordinary
	// scheduled task under the same envelope, retry policy, and budget.
	called := chosen.MapTools("call", mcp.Dispatch(mcp.DispatchSpec{
		Server: "inventory",
		Allow:  []string{"stock_level", "restock_eta"},
	}), pipeline.WithMCP("inventory", "stock_level", "restock_eta"), pipeline.WithVersion("v1"))

	// The voice document came from the server and is now an ordinary
	// broadcast: stored once, referenced by every task, and part of this
	// stage's fingerprint.
	called.Infer("reply", pipeline.InferSpec{
		Binding:     model.Binding{Tier: model.TierFast},
		Prefix:      "Style: " + `{{broadcast "voice"}}` + "\n",
		Prompt:      `{{.product}} — {{.tool_result}}`,
		OutputField: "reply",
	}, pipeline.WithBroadcast("voice"))

	return p
}

// inventoryServer is the MCP server this example launches: a small inventory
// system with three tools and one policy document.
func inventoryServer() *mcptest.Server {
	products := map[string]string{"sku-1": "Widget", "sku-2": "Sprocket", "sku-3": "Grommet"}
	stock := map[string]string{"sku-1": "17", "sku-2": "0", "sku-3": "4"}
	eta := map[string]string{"sku-1": "in stock", "sku-2": "2026-08-19", "sku-3": "in stock"}

	tool := func(name, desc string, table map[string]string) mcptest.Tool {
		return mcptest.Tool{
			Name: name, Description: desc,
			Schema: `{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"]}`,
			Fn: func(_ context.Context, args map[string]any) (string, error) {
				sku, _ := args["sku"].(string)
				v, ok := table[sku]
				if !ok {
					return "", fmt.Errorf("unknown sku %q", sku)
				}
				return v, nil
			},
		}
	}
	return &mcptest.Server{
		Name: "inventory", Version: "1.0",
		Tools: []mcptest.Tool{
			tool("lookup_sku", "resolves a SKU to a product name", products),
			tool("stock_level", "units on hand right now", stock),
			tool("restock_eta", "when the next shipment lands", eta),
		},
		Resources: []mcptest.Resource{{
			URI: "mem://voice", Name: "voice", MimeType: "text/plain",
			Text: "One sentence, no apologies, state the number.",
		}},
	}
}

// answer is the mock model: it routes on the question and writes the reply.
// It reads Prompt rather than FullPrompt on purpose — the prefix is the same
// bytes for every record in the stage, which is what makes it cacheable and
// what makes it useless for telling records apart.
func answer(req model.Request) (string, error) {
	if strings.Contains(req.Prompt, `Reply with JSON`) {
		tool := "stock_level"
		if strings.Contains(req.Prompt, "restock") {
			tool = "restock_eta"
		}
		sku := "sku-1"
		for name, id := range map[string]string{"Widget": "sku-1", "Sprocket": "sku-2", "Grommet": "sku-3"} {
			if strings.Contains(req.Prompt, name) {
				sku = id
			}
		}
		return fmt.Sprintf(`{"tool": %q, "args": {"sku": %q}}`, tool, sku), nil
	}
	product, result, ok := strings.Cut(strings.TrimSpace(req.Prompt), " — ")
	if !ok {
		return "No data.", nil
	}
	return fmt.Sprintf("%s: %s.", product, result), nil
}
