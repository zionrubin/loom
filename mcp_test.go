package loom_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/mcp"
	"github.com/zionrubin/loom/mcp/mcptest"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/security"
)

// TestMain lets this test binary double as an MCP server: a child process
// started with LOOM_MCP_SERVER set serves the catalog below on stdin/stdout,
// so the end-to-end tests exercise real pipes and a real child process rather
// than an in-process stand-in for one.
func TestMain(m *testing.M) {
	if os.Getenv("LOOM_MCP_SERVER") == "" {
		os.Exit(m.Run())
	}
	if err := catalogServer(os.Getenv("LOOM_MCP_SERVER")).ServeStdio(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

// catalogServer is a tiny product catalog. Version "v2" changes lookup_sku's
// schema without changing what it returns — the case a content-addressed cache
// has to notice and a name-only grant never would.
func catalogServer(version string) *mcptest.Server {
	schema := `{"type":"object","properties":{"sku":{"type":"string"}}}`
	if version == "v2" {
		schema = `{"type":"object","properties":{"sku":{"type":"string"},"locale":{"type":"string"}}}`
	}
	return &mcptest.Server{
		Name: "catalog", Version: version,
		Tools: []mcptest.Tool{
			{
				Name: "lookup_sku", Description: "resolves a SKU to a product name",
				Schema: schema,
				Fn: func(_ context.Context, args map[string]any) (string, error) {
					names := map[string]string{"sku-1": "Widget", "sku-2": "Sprocket"}
					sku, _ := args["sku"].(string)
					name, ok := names[sku]
					if !ok {
						return "", fmt.Errorf("unknown sku %q", sku)
					}
					return name, nil
				},
			},
			{
				Name: "stock_level", Description: "reports units on hand",
				Fn: func(_ context.Context, args map[string]any) (string, error) {
					return "17", nil
				},
			},
		},
		Resources: []mcptest.Resource{
			{URI: "mem://voice", Name: "voice", MimeType: "text/plain", Text: "Answer in one sentence."},
		},
	}
}

func catalogHelper(version string) mcp.Server {
	s := mcp.Stdio("catalog", os.Args[0], "-test.run=TestMCPHelperEntryPoint")
	s.Env = []string{"LOOM_MCP_SERVER=" + version}
	return s
}

// TestMCPHelperEntryPoint is the -test.run target the child is launched with;
// TestMain diverts before it runs.
func TestMCPHelperEntryPoint(t *testing.T) { t.Skip("helper process entry point") }

// enrichPipeline calls an MCP tool from an ordinary MapTools stage.
func enrichPipeline(opts ...pipeline.Option) *pipeline.Pipeline {
	p := pipeline.New("enrich")
	src := p.FromRecords("skus", []core.Record{
		core.NewRecord("r1", map[string]any{"sku": "sku-1"}),
		core.NewRecord("r2", map[string]any{"sku": "sku-2"}),
	})
	src.MapTools("name", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		out, err := s.Invoke(ctx, mcp.ToolName("catalog", "lookup_sku"),
			map[string]any{"sku": r.String("sku")})
		if err != nil {
			return core.Record{}, err
		}
		r.Data["product"] = mcp.Text(out)
		return r, nil
	}, opts...)
	return p
}

func TestMCPToolCallEndToEnd(t *testing.T) {
	res, err := loom.Run(context.Background(),
		enrichPipeline(pipeline.WithMCP("catalog", "lookup_sku")),
		loom.WithRetry(quickRetry()), loom.WithMCPServer(catalogHelper("v1")))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := map[string]string{}
	for _, r := range res.Output {
		got[r.ID] = r.String("product")
	}
	if got["r1"] != "Widget" || got["r2"] != "Sprocket" {
		t.Fatalf("products = %v", got)
	}

	// The connection is accounted for at the host, not the task.
	if len(res.MCP) != 1 {
		t.Fatalf("MCP stats = %+v", res.MCP)
	}
	stats := res.MCP[0]
	if stats.Sessions != 1 || stats.Dials != 1 {
		t.Fatalf("two records opened %d session(s) over %d dial(s); one connection should serve both",
			stats.Sessions, stats.Dials)
	}
	if stats.Calls != 2 {
		t.Fatalf("calls = %d, want 2", stats.Calls)
	}

	// Tool calls cost nothing in tokens, so they show up in the report as time
	// rather than as money.
	calls, dur := res.Report.ToolCalls()
	if calls != 2 {
		t.Fatalf("report tool calls = %d, want 2", calls)
	}
	if dur <= 0 {
		t.Fatal("report recorded no time spent in tools")
	}

	// Every call is audited under its qualified name.
	var allowed int
	for _, e := range res.Audit {
		if e.Action == "tool.invoke" && e.Subject == "mcp/catalog/lookup_sku" && e.Allowed {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf("audited allowed invocations = %d, want 2", allowed)
	}
}

// A stage that did not declare the server cannot call it, and the denial is
// audited — MCP tools are ordinary capabilities, so least privilege applies to
// them without a second mechanism.
func TestMCPRequiresDeclaration(t *testing.T) {
	res, err := loom.Run(context.Background(), enrichPipeline(),
		loom.WithRetry(quickRetry()), loom.WithMCPServer(catalogHelper("v1")))
	if err == nil {
		t.Fatal("an undeclared MCP tool must fail the run")
	}
	var denied bool
	for _, e := range res.Audit {
		if e.Action == "tool.invoke" && !e.Allowed && e.Subject == "mcp/catalog/lookup_sku" {
			denied = true
		}
	}
	if !denied {
		t.Errorf("denial must be audited; audit = %+v", res.Audit)
	}
}

// Declaring one tool does not grant the others on the same server.
func TestMCPGrantIsPerTool(t *testing.T) {
	p := pipeline.New("stock")
	src := p.FromRecords("skus", []core.Record{core.NewRecord("r1", map[string]any{"sku": "sku-1"})})
	src.MapTools("check", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		_, err := s.Invoke(ctx, mcp.ToolName("catalog", "stock_level"), nil)
		return r, err
	}, pipeline.WithMCP("catalog", "lookup_sku"))

	_, err := loom.Run(context.Background(), p,
		loom.WithRetry(quickRetry()), loom.WithMCPServer(catalogHelper("v1")))
	if err == nil {
		t.Fatal("a stage that declared only lookup_sku must not reach stock_level")
	}
	if !strings.Contains(err.Error(), "capability not granted") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A stage naming a server nobody registered fails compilation, before anything
// is provisioned — the same treatment an unregistered broadcast gets.
func TestMCPUnregisteredServerFailsCompilation(t *testing.T) {
	_, err := loom.Run(context.Background(),
		enrichPipeline(pipeline.WithMCP("nope")), loom.WithRetry(quickRetry()))
	if err == nil || !strings.Contains(err.Error(), "not registered for this run") {
		t.Fatalf("err = %v, want a compile-time rejection", err)
	}
}

// The tool descriptors join the fingerprint, so replacing a server with one
// whose schema changed recomputes exactly the stages that could have called it
// — and leaves everything else in the cache warm.
func TestMCPServerUpgradeInvalidatesCache(t *testing.T) {
	stateDir := t.TempDir()
	run := func(version string) *loom.RunResult {
		t.Helper()
		res, err := loom.Run(context.Background(),
			enrichPipeline(pipeline.WithMCP("catalog", "lookup_sku"), pipeline.WithVersion("v1")),
			loom.WithRetry(quickRetry()), loom.WithStateDir(stateDir),
			loom.WithMCPServer(catalogHelper(version)))
		if err != nil {
			t.Fatalf("run against %s: %v", version, err)
		}
		return res
	}

	first := run("v1")
	if first.MCP[0].Calls != 2 {
		t.Fatalf("first run made %d calls, want 2", first.MCP[0].Calls)
	}

	// Same server, same tools: the stage replays and calls nothing.
	second := run("v1")
	if second.MCP[0].Calls != 0 {
		t.Fatalf("a cached stage made %d tool calls; it should have replayed", second.MCP[0].Calls)
	}
	if second.Output[0].String("product") != "Widget" {
		t.Fatalf("replayed record lost its enrichment: %+v", second.Output[0].Data)
	}

	// A server whose tool schema changed is a different operation.
	third := run("v2")
	if third.MCP[0].Calls != 2 {
		t.Fatalf("after the server changed, the stage made %d calls; it should have recomputed",
			third.MCP[0].Calls)
	}
}

// A networked server's host lands on the declaring stage's egress allowlist and
// nowhere else, so a granted tool still cannot reach a host the envelope has
// not seen.
func TestMCPEgressIsEnforced(t *testing.T) {
	server := &mcptest.Server{Tools: []mcptest.Tool{mcptest.Echo("echo")}}
	httpSrv := server.HTTP()
	defer httpSrv.Close()

	p := pipeline.New("web")
	src := p.FromRecords("in", []core.Record{core.NewRecord("r1", map[string]any{})})
	src.MapTools("call", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		out, err := s.Invoke(ctx, mcp.ToolName("web", "echo"), map[string]any{"x": 1})
		if err != nil {
			return core.Record{}, err
		}
		r.Data["echo"] = mcp.Text(out)
		return r, nil
	}, pipeline.WithMCP("web", "echo"))

	res, err := loom.Run(context.Background(), p,
		loom.WithRetry(quickRetry()), loom.WithMCPServer(mcp.HTTP("web", httpSrv.URL)))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(res.Output[0].String("echo"), `"x":1`) {
		t.Fatalf("echo = %q", res.Output[0].String("echo"))
	}

	// The same tool, granted by name but with the host struck off the
	// envelope, is refused before the call goes out.
	p2 := pipeline.New("web-denied")
	src2 := p2.FromRecords("in", []core.Record{core.NewRecord("r1", map[string]any{})})
	src2.MapTools("call", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		_, err := s.Invoke(ctx, mcp.ToolName("web", "echo"), nil)
		return r, err
	}, pipeline.WithGrants(security.ToolCap(mcp.ToolName("web", "echo"))))

	res2, err := loom.Run(context.Background(), p2,
		loom.WithRetry(quickRetry()), loom.WithMCPServer(mcp.HTTP("web", httpSrv.URL)))
	if err == nil {
		t.Fatal("a tool granted without its server declared must be refused")
	}
	if !strings.Contains(err.Error(), "egress") {
		t.Fatalf("expected an egress refusal, got: %v", err)
	}
	var denied bool
	for _, e := range res2.Audit {
		if e.Action == "egress" && !e.Allowed {
			denied = true
		}
	}
	if !denied {
		t.Error("the egress denial must be audited")
	}
}

// A resource read at provisioning becomes an ordinary broadcast: stored once
// by content hash, referenced by every task that declares it.
func TestMCPResourceBecomesABroadcast(t *testing.T) {
	p := pipeline.New("voice")
	src := p.FromRecords("in", []core.Record{core.NewRecord("r1", map[string]any{})})
	src.MapTools("read", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		v, err := core.BroadcastAs[string](ctx, s, "voice")
		if err != nil {
			return core.Record{}, err
		}
		r.Data["voice"] = v
		return r, nil
	}, pipeline.WithBroadcast("voice"))

	res, err := loom.Run(context.Background(), p,
		loom.WithRetry(quickRetry()),
		loom.WithMCPServer(catalogHelper("v1")),
		loom.WithMCPResource("voice", "catalog", "mem://voice"))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := res.Output[0].String("voice"); got != "Answer in one sentence." {
		t.Fatalf("voice = %q", got)
	}
	if res.Broadcasts["voice"] == "" {
		t.Fatal("the resource should have been registered by content hash")
	}
}

// Every agent on a fleet shares one set of connections — the property that
// makes them the fleet's rather than the pipeline's.
func TestFleetSharesOneConnection(t *testing.T) {
	fleet, err := loom.NewFleet(
		loom.WithWorkers(4), loom.WithRetry(quickRetry()),
		loom.WithMCPServer(catalogHelper("v1")))
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}
	defer fleet.Close()

	const agents = 4
	var wg sync.WaitGroup
	for i := range agents {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := enrichPipeline(pipeline.WithMCP("catalog", "lookup_sku"))
			p.Name = fmt.Sprintf("enrich-%d", i)
			if _, err := fleet.Run(context.Background(), p); err != nil {
				t.Errorf("agent %d: %v", i, err)
			}
		}()
	}
	wg.Wait()

	rep := fleet.Report()
	if len(rep.MCP) != 1 {
		t.Fatalf("fleet MCP rows = %d, want 1", len(rep.MCP))
	}
	if rep.MCP[0].Sessions != 1 || rep.MCP[0].Dials != 1 {
		t.Fatalf("%d agents opened %d session(s) over %d dial(s); a fleet holds one set",
			agents, rep.MCP[0].Sessions, rep.MCP[0].Dials)
	}
	if !strings.Contains(rep.String(), "shared by every agent") {
		t.Errorf("fleet report does not mention the shared connection:\n%s", rep.String())
	}

	// An agent may not bring its own connections: they are the fleet's.
	if _, err := fleet.Run(context.Background(), enrichPipeline(),
		loom.WithMCPServer(catalogHelper("v1"))); err == nil {
		t.Error("per-agent WithMCPServer must be rejected")
	}
}

// Model-directed tool use: the model names a tool, the next stage runs it.
// Two stages rather than a hidden loop, so the choice is data and the call is
// an ordinary scheduled task.
func TestMCPDispatchFromModelChoice(t *testing.T) {
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			sku := "sku-1"
			if strings.Contains(req.Prompt, "sku-2") {
				sku = "sku-2"
			}
			return fmt.Sprintf(`{"tool": "lookup_sku", "args": {"sku": %q}}`, sku), nil
		})); err != nil {
		t.Fatal(err)
	}

	p := pipeline.New("agentic")
	src := p.FromRecords("asks", []core.Record{
		core.NewRecord("a1", map[string]any{"question": "what is sku-1?"}),
		core.NewRecord("a2", map[string]any{"question": "what is sku-2?"}),
	})
	src.
		Infer("choose", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			Prompt:    `{{.question}}`,
			ParseJSON: true,
		}).
		MapTools("call", mcp.Dispatch(mcp.DispatchSpec{
			Server: "catalog", Allow: []string{"lookup_sku"},
		}), pipeline.WithMCP("catalog", "lookup_sku"))

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithMCPServer(catalogHelper("v1")))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	got := map[string]string{}
	for _, r := range res.Output {
		got[r.ID] = r.String("tool_result")
	}
	if got["a1"] != "Widget" || got["a2"] != "Sprocket" {
		t.Fatalf("dispatched results = %v", got)
	}
}

// A tool the model asked for but the stage was not granted is a request, not
// an authorization.
func TestMCPDispatchRefusesUnlistedTool(t *testing.T) {
	dispatch := mcp.Dispatch(mcp.DispatchSpec{Server: "catalog", Allow: []string{"lookup_sku"}})
	rec := core.NewRecord("r", map[string]any{"tool": "stock_level", "args": map[string]any{}})
	if _, err := dispatch(context.Background(), nil, rec); err == nil {
		t.Fatal("expected a refusal for a tool outside Allow")
	}
	rec = core.NewRecord("r", map[string]any{"tool": "mcp/other/lookup_sku"})
	if _, err := dispatch(context.Background(), nil, rec); err == nil {
		t.Fatal("expected a refusal for a tool qualified with another server")
	}
}

// Explain compiles a pipeline that uses MCP without opening a socket: the
// declarations still validate, and the projection says what it could not know.
func TestExplainDoesNotConnect(t *testing.T) {
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast); err != nil {
		t.Fatal(err)
	}
	p := pipeline.New("explained")
	src := p.FromRecords("in", []core.Record{core.NewRecord("r1", map[string]any{"sku": "sku-1"})})
	src.
		MapTools("name", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
			return r, nil
		}, pipeline.WithMCP("catalog", "lookup_sku")).
		Infer("describe", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prompt:  "Describe {{.sku}}",
		})

	// A command that would fail if it were ever executed: Explain must not run it.
	server := mcp.Stdio("catalog", "/nonexistent/mcp-server").WithTools("lookup_sku")
	proj, err := loom.Explain(p, loom.WithRegistry(reg), loom.WithMCPServer(server))
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	var noted bool
	for _, w := range proj.Warnings {
		if strings.Contains(w, "MCP tools") {
			noted = true
		}
	}
	if !noted {
		t.Errorf("projection should say MCP calls are unpriced; warnings = %v", proj.Warnings)
	}

	// A tool outside the deployment's allowlist still fails, without a socket.
	bad := pipeline.New("bad")
	bad.FromRecords("in", nil).MapTools("x",
		func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) { return r, nil },
		pipeline.WithMCP("catalog", "stock_level"))
	if _, err := loom.Explain(bad, loom.WithRegistry(reg), loom.WithMCPServer(server)); err == nil {
		t.Error("a tool outside the allowlist should fail projection")
	}
}

// A server that cannot be started fails the run at provisioning, not at the
// first record that reaches the stage.
func TestUnreachableServerFailsBeforeTheRun(t *testing.T) {
	_, err := loom.Run(context.Background(),
		enrichPipeline(pipeline.WithMCP("catalog", "lookup_sku")),
		loom.WithRetry(quickRetry()),
		loom.WithMCPServer(mcp.Stdio("catalog", "/nonexistent/mcp-server")))
	if err == nil {
		t.Fatal("expected the run to fail when a server cannot be started")
	}
	if !strings.Contains(err.Error(), "mcp catalog") {
		t.Fatalf("error should name the server: %v", err)
	}
}
