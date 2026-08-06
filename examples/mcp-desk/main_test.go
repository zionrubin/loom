package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/mcp"
	"github.com/zionrubin/loom/model"
)

// The pipeline talks to a real child process, so the test binary doubles as the
// MCP server exactly as the example's own binary does.
func TestMain(m *testing.M) {
	if os.Getenv("LOOM_MCP_DESK_SERVER") != "" {
		if err := inventoryServer().ServeStdio(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestServerEntryPoint(t *testing.T) { t.Skip("helper process entry point") }

func server() mcp.Server {
	s := mcp.Stdio("inventory", os.Args[0], "-test.run=TestServerEntryPoint").
		WithTools("lookup_sku", "stock_level", "restock_eta")
	s.Env = []string{"LOOM_MCP_DESK_SERVER=1"}
	return s
}

func run(t *testing.T, stateDir string) *loom.RunResult {
	t.Helper()
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(answer)); err != nil {
		t.Fatal(err)
	}
	opts := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithMCPServer(server()),
		loom.WithMCPResource("voice", "inventory", "mem://voice"),
	}
	if stateDir != "" {
		opts = append(opts, loom.WithStateDir(stateDir))
	}
	res, err := loom.Run(context.Background(), desk(), opts...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return res
}

func TestDeskAnswersFromTools(t *testing.T) {
	res := run(t, "")

	want := map[string]string{
		"q1": "Widget: 17.",           // stock_level
		"q2": "Sprocket: 2026-08-19.", // restock_eta, because the ask says restock
		"q3": "Grommet: 4.",           // stock_level
	}
	for _, r := range res.Output {
		if got := r.String("reply"); got != want[r.ID] {
			t.Errorf("%s: reply = %q, want %q", r.ID, got, want[r.ID])
		}
	}

	// Three lookups plus three model-chosen calls, all over one connection.
	if len(res.MCP) != 1 {
		t.Fatalf("MCP stats = %+v", res.MCP)
	}
	if res.MCP[0].Calls != 6 {
		t.Errorf("tool calls = %d, want 6", res.MCP[0].Calls)
	}
	if res.MCP[0].Dials != 1 {
		t.Errorf("dials = %d; nine records' worth of work should share one connection", res.MCP[0].Dials)
	}
}

// The point of the second run: an MCP call is work, and Loom does not pay for
// work twice.
func TestDeskReplaysWithoutCallingTools(t *testing.T) {
	dir := t.TempDir()
	first := run(t, dir)
	if first.MCP[0].Calls == 0 {
		t.Fatal("the first run should have called tools")
	}
	second := run(t, dir)
	if second.MCP[0].Calls != 0 {
		t.Errorf("the cached run made %d tool calls, want 0", second.MCP[0].Calls)
	}
	if len(second.Output) != 3 || second.Output[0].String("reply") == "" {
		t.Errorf("replayed output is not the same shape: %+v", second.Output)
	}
}
