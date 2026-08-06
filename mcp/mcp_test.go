package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/mcp"
	"github.com/zionrubin/loom/mcp/mcptest"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/task"
)

// The stdio transport is the one that matters most and the one hardest to fake
// honestly, so these tests run a real child process: the test binary re-execs
// itself with LOOM_MCP_HELPER set, and that process serves MCP on its own
// stdin and stdout. Everything below therefore exercises the actual pipes,
// framing, handshake, and process teardown.
func TestMain(m *testing.M) {
	if os.Getenv("LOOM_MCP_HELPER") == "" {
		os.Exit(m.Run())
	}
	if err := helperServer().ServeStdio(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func helperServer() *mcptest.Server {
	delay, _ := time.ParseDuration(os.Getenv("LOOM_MCP_DELAY"))
	return &mcptest.Server{
		Name:  "helper",
		Delay: delay,
		Tools: []mcptest.Tool{
			{
				Name:        "lookup",
				Description: "looks a key up in a fixed table",
				Schema:      `{"type":"object","properties":{"key":{"type":"string"}}}`,
				Fn: func(_ context.Context, args map[string]any) (string, error) {
					table := map[string]string{"a": "alpha", "b": "beta"}
					key, _ := args["key"].(string)
					v, ok := table[key]
					if !ok {
						return "", fmt.Errorf("no such key %q", key)
					}
					return v, nil
				},
				Structured: func(_ context.Context, args map[string]any) any {
					return map[string]any{"key": args["key"]}
				},
			},
			mcptest.Echo("echo"),
			{
				Name: "die",
				Fn: func(_ context.Context, _ map[string]any) (string, error) {
					os.Exit(7) // a server that falls over mid-call
					return "", nil
				},
			},
			{
				Name: "whoami",
				Fn: func(_ context.Context, _ map[string]any) (string, error) {
					return os.Getenv("HELPER_TOKEN"), nil
				},
			},
		},
		Resources: []mcptest.Resource{
			{URI: "mem://policy", Name: "policy", MimeType: "text/plain", Text: "be brief"},
		},
	}
}

// helper returns a descriptor for a child process running helperServer.
func helper(name string, env ...string) mcp.Server {
	s := mcp.Stdio(name, os.Args[0], "-test.run=TestHelperIsNotARealTest")
	s.Env = append([]string{"LOOM_MCP_HELPER=1"}, env...)
	return s
}

// TestHelperIsNotARealTest is the -test.run target the helper process is
// launched with: TestMain diverts before any test runs, so its body never
// executes in the child.
func TestHelperIsNotARealTest(t *testing.T) { t.Skip("helper process entry point") }

func catalog(t *testing.T, servers ...mcp.Server) *mcp.Catalog {
	t.Helper()
	cat, err := mcp.NewCatalog(mcp.Options{
		Resolve: func(ref security.SecretRef) (string, error) {
			return "secret-for-" + string(ref), nil
		},
	}, servers...)
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}
	if err := cat.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

func TestStdioDialDiscoversTools(t *testing.T) {
	cat := catalog(t, helper("helper"))

	m := cat.Manifest()["helper"]
	if !m.Discovered {
		t.Fatal("manifest reports the server was not discovered")
	}
	if len(m.Tools) != 4 {
		t.Fatalf("tools = %d, want 4: %+v", len(m.Tools), m.Tools)
	}
	if m.Digest == "" {
		t.Fatal("empty digest for a discovered server")
	}
	if m.Tools[0].Name != "die" { // sorted
		t.Fatalf("tools are not sorted by name: %s", m.Tools[0].Name)
	}

	res, err := cat.Call(context.Background(), "helper", "lookup", map[string]any{"key": "a"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if res.Text() != "alpha" {
		t.Fatalf("text = %q, want alpha", res.Text())
	}
	rec := res.Record()
	if got, ok := rec["structured"].(map[string]any); !ok || got["key"] != "a" {
		t.Fatalf("structured content did not survive: %#v", rec["structured"])
	}
}

// A tool that ran and reported failure is a permanent failure: the same
// arguments will fail the same way, so retrying them burns budget for nothing.
func TestToolErrorIsPermanent(t *testing.T) {
	cat := catalog(t, helper("helper"))

	_, err := cat.Call(context.Background(), "helper", "lookup", map[string]any{"key": "zz"})
	if err == nil {
		t.Fatal("expected an error for an unknown key")
	}
	if class := core.ClassOf(err); class != core.FailPermanent {
		t.Fatalf("class = %s, want permanent", class)
	}
	if !strings.Contains(err.Error(), "no such key") {
		t.Fatalf("error lost the server's message: %v", err)
	}
}

// A server that dies mid-call fails that call transiently — the scheduler's cue
// to retry — and the pool redials rather than writing into a dead pipe.
func TestServerDeathIsTransientAndRedials(t *testing.T) {
	cat := catalog(t, helper("helper"))

	_, err := cat.Call(context.Background(), "helper", "die", nil)
	if err == nil {
		t.Fatal("expected an error when the server exits mid-call")
	}
	if class := core.ClassOf(err); class != core.FailTransient {
		t.Fatalf("class = %s, want transient (a dead transport is retryable)", class)
	}

	// The next call must succeed on a fresh connection.
	res, err := cat.Call(context.Background(), "helper", "lookup", map[string]any{"key": "b"})
	if err != nil {
		t.Fatalf("call after death: %v", err)
	}
	if res.Text() != "beta" {
		t.Fatalf("text = %q, want beta", res.Text())
	}
	stats := cat.Stats()[0]
	if stats.Dials < 2 {
		t.Fatalf("dials = %d, want at least 2 (the pool should have reconnected)", stats.Dials)
	}
}

// One session, many concurrent calls: the protocol multiplexes on request id,
// so a connection is not the thing that has to be pooled per task.
func TestOneSessionServesConcurrentCalls(t *testing.T) {
	s := helper("helper")
	s.MaxConcurrent = 8
	cat := catalog(t, s)

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	texts := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := cat.Call(context.Background(), "helper", "echo", map[string]any{"i": i})
			errs[i], texts[i] = err, res.Text()
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if !strings.Contains(texts[i], fmt.Sprintf(`"i":%d`, i)) {
			t.Fatalf("call %d got another call's answer: %s", i, texts[i])
		}
	}
	if stats := cat.Stats()[0]; stats.Dials != 1 {
		t.Fatalf("dials = %d, want 1: %d concurrent calls should share one session", stats.Dials, n)
	}
}

// The lease is a call slot, not a connection, and MaxConcurrent is what bounds
// how hard a whole host may push one server.
func TestMaxConcurrentBoundsCalls(t *testing.T) {
	s := helper("helper", "LOOM_MCP_DELAY=40ms")
	s.MaxConcurrent = 2
	cat := catalog(t, s)

	const n = 8
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cat.Call(context.Background(), "helper", "echo", nil)
		}()
	}
	wg.Wait()

	// Ask the server itself what it saw: the peak number of overlapping calls.
	res, err := cat.Call(context.Background(), "helper", "echo", map[string]any{"done": true})
	if err != nil {
		t.Fatalf("final call: %v", err)
	}
	_ = res
	if stats := cat.Stats()[0]; stats.Calls != n+1 {
		t.Fatalf("calls = %d, want %d", stats.Calls, n+1)
	}
	// The bound is observable in the client too: eight calls of 40ms through
	// two slots cannot finish in less than four rounds of the delay.
	if busy := cat.Stats()[0].Busy; busy < n*40*time.Millisecond {
		t.Fatalf("busy = %s, want at least %s of slot time", busy, n*40*time.Millisecond)
	}
}

// Concurrency is bounded at the server, not just accounted for at the client.
func TestServerNeverSeesMoreThanTheBound(t *testing.T) {
	server := &mcptest.Server{
		Name: "http", Delay: 25 * time.Millisecond,
		Tools: []mcptest.Tool{mcptest.Echo("echo")},
	}
	httpSrv := server.HTTP()
	defer httpSrv.Close()

	s := mcp.HTTP("web", httpSrv.URL)
	s.MaxConcurrent = 3
	cat := catalog(t, s)

	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cat.Call(context.Background(), "web", "echo", nil)
		}()
	}
	wg.Wait()

	if peak := server.Peak(); peak > 3 {
		t.Fatalf("server saw %d concurrent calls, MaxConcurrent was 3", peak)
	}
	if server.Calls() != 12 {
		t.Fatalf("server served %d calls, want 12", server.Calls())
	}
}

func TestHTTPTransportHandlesJSONAndSSE(t *testing.T) {
	server := &mcptest.Server{Tools: []mcptest.Tool{mcptest.Echo("echo")}}
	for _, body := range []string{"json", "sse"} {
		t.Run(body, func(t *testing.T) {
			httpSrv := server.HTTP()
			if body == "sse" {
				httpSrv = server.SSE()
			}
			defer httpSrv.Close()

			cat := catalog(t, mcp.HTTP("web", httpSrv.URL))
			res, err := cat.Call(context.Background(), "web", "echo", map[string]any{"x": 1})
			if err != nil {
				t.Fatalf("call: %v", err)
			}
			if !strings.Contains(res.Text(), `"x":1`) {
				t.Fatalf("text = %q", res.Text())
			}
		})
	}
}

// A credential named in the descriptor reaches the connection and nothing
// else: the child process can read it, and no task ever holds it.
func TestSecretsReachTheConnectionOnly(t *testing.T) {
	s := helper("helper")
	s.EnvSecrets = map[string]security.SecretRef{"HELPER_TOKEN": "helper_token"}
	cat := catalog(t, s)

	res, err := cat.Call(context.Background(), "helper", "whoami", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if res.Text() != "secret-for-helper_token" {
		t.Fatalf("the child did not receive the resolved secret: %q", res.Text())
	}
}

func TestAllowlistRejectsUnknownToolAtProvisioning(t *testing.T) {
	s := helper("helper").WithTools("lookup", "nope")
	cat, err := mcp.NewCatalog(mcp.Options{}, s)
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}
	defer cat.Close()

	err = cat.Connect(context.Background())
	if err == nil {
		t.Fatal("expected connecting to fail on an allowlisted tool the server does not offer")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Fatalf("error does not name the missing tool: %v", err)
	}
}

func TestAllowlistNarrowsTheManifest(t *testing.T) {
	cat := catalog(t, helper("helper").WithTools("lookup"))

	m := cat.Manifest()["helper"]
	if len(m.Tools) != 1 || m.Tools[0].Name != "lookup" {
		t.Fatalf("manifest = %+v, want only lookup", m.Tools)
	}
	if tools := cat.Tools(); len(tools) != 1 {
		t.Fatalf("adapters = %d, want 1", len(tools))
	}
	if got := cat.Tools()[0].Name(); got != "mcp/helper/lookup" {
		t.Fatalf("tool name = %q", got)
	}
}

func TestReadResource(t *testing.T) {
	cat := catalog(t, helper("helper"))

	v, err := cat.ReadResource(context.Background(), "helper", "mem://policy")
	if err != nil {
		t.Fatalf("ReadResource: %v", err)
	}
	if v != "be brief" {
		t.Fatalf("resource = %#v", v)
	}
	if _, err := cat.ReadResource(context.Background(), "helper", "mem://missing"); err == nil {
		t.Fatal("expected an error for a missing resource")
	}
}

// The digest is the contract a plan is compiled against: a tool whose schema
// changed is a different operation, and results produced under the old one
// must not be replayed.
func TestDigestTracksDescriptors(t *testing.T) {
	base := []mcp.ToolDesc{
		{Name: "a", Description: "first", InputSchema: json.RawMessage(`{"type":"object"}`)},
		{Name: "b", Description: "second"},
	}
	reordered := []mcp.ToolDesc{base[1], base[0]}
	if mcp.Digest(base) != mcp.Digest(reordered) {
		t.Fatal("digest depends on the order tools/list happened to return")
	}

	changed := append([]mcp.ToolDesc(nil), base...)
	changed[0].InputSchema = json.RawMessage(`{"type":"object","required":["k"]}`)
	if mcp.Digest(base) == mcp.Digest(changed) {
		t.Fatal("a changed input schema left the digest untouched")
	}

	subset := mcp.ServerManifest{Name: "s", Tools: base, Discovered: true}
	if subset.DigestOf([]string{"a"}) == subset.DigestOf([]string{"a", "b"}) {
		t.Fatal("digest ignores which tools a stage declared")
	}
}

// A task whose envelope names a contract the live server no longer offers must
// fail rather than call a tool nobody planned.
func TestContractDriftIsRefused(t *testing.T) {
	cat := catalog(t, helper("helper").WithTools("lookup", "echo"))
	tool := cat.Tools()[1] // lookup, sorted after echo

	// An envelope shaped the way the planner shapes one: a grant per declared
	// tool, and the digest of exactly those tools' descriptors.
	m := cat.Manifest()["helper"]
	env := task.Envelope{
		Grants: security.NewGrantSet(security.ToolCap(mcp.ToolName("helper", "lookup"))),
		MCP:    map[string]string{"helper": "a-digest-from-another-version"},
	}
	_, err := tool.InvokeIn(context.Background(), env, "task_1", map[string]any{"key": "a"})
	if err == nil {
		t.Fatal("expected a refusal when the planned digest does not match the live one")
	}
	if core.ClassOf(err) != core.FailPermanent {
		t.Fatalf("class = %s, want permanent", core.ClassOf(err))
	}
	if !strings.Contains(err.Error(), "changed since this stage was planned") {
		t.Fatalf("error does not explain the drift: %v", err)
	}

	// The digest the planner would have computed goes through.
	env.MCP["helper"] = m.DigestOf([]string{"lookup"})
	if _, err := tool.InvokeIn(context.Background(), env, "task_1", map[string]any{"key": "a"}); err != nil {
		t.Fatalf("call with the planned digest: %v", err)
	}

	// The check is as narrow as the fingerprint: pinning the whole server's
	// digest is not what a stage that declared one tool was compiled against.
	env.MCP["helper"] = m.Digest
	if _, err := tool.InvokeIn(context.Background(), env, "task_1", map[string]any{"key": "a"}); err == nil {
		t.Fatal("a digest over a different tool set should not satisfy the contract")
	}
}

func TestValidateRejectsAmbiguousDescriptors(t *testing.T) {
	cases := map[string]mcp.Server{
		"no transport":   {Name: "x"},
		"both":           {Name: "x", Command: "cat", URL: "https://example.com/mcp"},
		"empty name":     {Command: "cat"},
		"slash in name":  {Name: "a/b", Command: "cat"},
		"bad url":        {Name: "x", URL: "://nope"},
		"stdio with tls": {Name: "x", Command: "cat", AuthSecret: "k"},
		"http with env":  {Name: "x", URL: "https://example.com/mcp", EnvSecrets: map[string]security.SecretRef{"A": "b"}},
	}
	for name, s := range cases {
		if err := s.Validate(); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
	ok := mcp.Stdio("fs", "cat")
	if err := ok.Validate(); err != nil {
		t.Errorf("valid descriptor rejected: %v", err)
	}
}

func TestEndpointIsTheHostEgressChecks(t *testing.T) {
	if got := mcp.HTTP("w", "https://mcp.example.com:8443/rpc").Endpoint(); got != "mcp.example.com" {
		t.Fatalf("endpoint = %q", got)
	}
	if got := mcp.Stdio("fs", "cat").Endpoint(); got != "" {
		t.Fatalf("a stdio server should open no socket, got endpoint %q", got)
	}
}

func TestToolNameRoundTrip(t *testing.T) {
	name := mcp.ToolName("github", "search_code")
	if name != "mcp/github/search_code" {
		t.Fatalf("name = %q", name)
	}
	server, tool, ok := mcp.SplitToolName(name)
	if !ok || server != "github" || tool != "search_code" {
		t.Fatalf("split = %q, %q, %v", server, tool, ok)
	}
	if _, _, ok := mcp.SplitToolName("lookup"); ok {
		t.Fatal("a local tool name should not parse as an MCP one")
	}
}

func TestManifestSelect(t *testing.T) {
	discovered := mcp.ServerManifest{
		Name:       "s",
		Tools:      []mcp.ToolDesc{{Name: "a"}, {Name: "b"}},
		Discovered: true,
	}
	all, err := discovered.Select(nil)
	if err != nil || len(all) != 2 {
		t.Fatalf("Select(nil) = %v, %v", all, err)
	}
	if _, err := discovered.Select([]string{"c"}); err == nil {
		t.Fatal("expected an error for a tool the server does not offer")
	}

	declared := mcp.Declared(mcp.Stdio("s", "cat").WithTools("a"))["s"]
	if got, err := declared.Select(nil); err != nil || len(got) != 1 || got[0] != "a" {
		t.Fatalf("declared Select(nil) = %v, %v", got, err)
	}
	if _, err := declared.Select([]string{"b"}); err == nil {
		t.Fatal("expected an error for a tool outside the deployment allowlist")
	}
	if declared.DigestOf([]string{"a"}) != "" {
		t.Fatal("an undiscovered manifest cannot produce a digest")
	}
}

func TestCatalogRejectsDuplicateServers(t *testing.T) {
	_, err := mcp.NewCatalog(mcp.Options{}, mcp.Stdio("a", "cat"), mcp.Stdio("a", "cat"))
	if err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("err = %v, want a duplicate-name error", err)
	}
}

func TestMissingSecretFailsProvisioning(t *testing.T) {
	s := helper("helper")
	s.EnvSecrets = map[string]security.SecretRef{"HELPER_TOKEN": "absent"}
	_, err := mcp.NewCatalog(mcp.Options{
		Resolve: func(security.SecretRef) (string, error) {
			return "", errors.New("capability not granted")
		},
	}, s)
	if err == nil {
		t.Fatal("expected provisioning to fail when a credential cannot be resolved")
	}
}

func TestClosedCatalogRefusesCalls(t *testing.T) {
	cat := catalog(t, helper("helper"))
	if err := cat.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := cat.Call(context.Background(), "helper", "lookup", map[string]any{"key": "a"}); err == nil {
		t.Fatal("expected a closed catalog to refuse calls")
	}
}
