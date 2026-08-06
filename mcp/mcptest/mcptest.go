// Package mcptest is a minimal in-process MCP server for testing Loom
// pipelines that call tools, and for examples that must run offline.
//
// It is to package mcp what model.Mock is to a provider: a real implementation
// of the protocol with deterministic, scripted behavior, so a pipeline's tool
// path can be exercised — including its failures, its latency, and its
// concurrency — without a network, a credential, or a third-party server.
//
// The same server can be served three ways, which is what makes it useful for
// both halves of the test surface: [Server.Serve] over any pair of streams,
// [Server.ServeStdio] as a child process Loom launches, and [Server.HTTP] as a
// streamable-HTTP endpoint.
package mcptest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// ProtocolVersion is what this server answers initialize with.
const ProtocolVersion = "2025-06-18"

// Tool is one tool the server offers.
type Tool struct {
	Name        string
	Description string
	// Schema is the tool's JSON Schema as a string. Empty yields a permissive
	// object schema.
	Schema string
	// Fn produces the tool's text result. Returning an error produces an
	// isError result — the protocol's way of saying the tool ran and failed,
	// which Loom classifies as a permanent failure.
	Fn func(ctx context.Context, args map[string]any) (string, error)
	// Structured, when set, is returned alongside the text as structuredContent.
	Structured func(ctx context.Context, args map[string]any) any
}

// Resource is one readable resource.
type Resource struct {
	URI      string
	Name     string
	MimeType string
	Text     string
}

// Server is a scriptable MCP server.
type Server struct {
	Name      string
	Version   string
	Tools     []Tool
	Resources []Resource
	// Delay sleeps inside every tool call. A test that wants to observe
	// concurrency needs calls that overlap, and overlap needs duration.
	Delay time.Duration

	mu       sync.Mutex
	calls    int
	inFlight int
	peak     int
	byTool   map[string]int
}

// Calls returns how many tool calls the server has served.
func (s *Server) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// CallsTo returns how many times one tool was called.
func (s *Server) CallsTo(tool string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byTool[tool]
}

// Peak returns the highest number of tool calls that were ever in flight at
// once — what a concurrency bound is supposed to hold down.
func (s *Server) Peak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

func (s *Server) enter(tool string) {
	s.mu.Lock()
	s.calls++
	s.inFlight++
	if s.byTool == nil {
		s.byTool = map[string]int{}
	}
	s.byTool[tool]++
	if s.inFlight > s.peak {
		s.peak = s.inFlight
	}
	s.mu.Unlock()
}

func (s *Server) leave() {
	s.mu.Lock()
	s.inFlight--
	s.mu.Unlock()
}

// --- protocol ------------------------------------------------------------

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int64          `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      *int64         `json:"id,omitempty"`
	Result  any            `json:"result,omitempty"`
	Error   *responseError `json:"error,omitempty"`
}

type responseError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Serve reads newline-delimited JSON-RPC from r and writes responses to w
// until r is exhausted. Requests are handled concurrently and responses may
// return out of order, which is the property a multiplexed client depends on.
func (s *Server) Serve(r io.Reader, w io.Writer) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 32<<20)
	var wmu sync.Mutex
	var wg sync.WaitGroup
	enc := json.NewEncoder(w)

	write := func(resp response) {
		wmu.Lock()
		defer wmu.Unlock()
		_ = enc.Encode(resp)
	}

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var req request
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			continue
		}
		if req.ID == nil {
			continue // a notification: initialized, cancelled, …
		}
		wg.Add(1)
		go func(req request) {
			defer wg.Done()
			result, rerr := s.handle(context.Background(), req)
			resp := response{JSONRPC: "2.0", ID: req.ID}
			if rerr != nil {
				resp.Error = rerr
			} else {
				resp.Result = result
			}
			write(resp)
		}(req)
	}
	wg.Wait()
	return sc.Err()
}

// ServeStdio serves on stdin/stdout, the transport a Loom-launched child
// process speaks.
func (s *Server) ServeStdio() error { return s.Serve(os.Stdin, os.Stdout) }

// HTTP starts the server behind a streamable-HTTP endpoint. Close it when
// done; its URL is what an mcp.Server descriptor points at.
func (s *Server) HTTP() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.ID == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		result, rerr := s.handle(r.Context(), req)
		resp := response{JSONRPC: "2.0", ID: req.ID}
		if rerr != nil {
			resp.Error = rerr
		} else {
			resp.Result = result
		}
		if req.Method == "initialize" {
			w.Header().Set("Mcp-Session-Id", "mcptest-session")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

// SSE starts the server behind a streamable-HTTP endpoint that answers with an
// event stream rather than a JSON body — the other shape the transport must
// handle.
func (s *Server) SSE() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.ID == nil {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		result, rerr := s.handle(r.Context(), req)
		resp := response{JSONRPC: "2.0", ID: req.ID}
		if rerr != nil {
			resp.Error = rerr
		} else {
			resp.Result = result
		}
		blob, _ := json.Marshal(resp)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// A well-behaved stream carries other frames too; emitting one proves
		// the client skips what it is not waiting for.
		fmt.Fprintf(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\n\n")
		fmt.Fprintf(w, "event: message\ndata: %s\n\n", blob)
	}))
}

func (s *Server) handle(ctx context.Context, req request) (any, *responseError) {
	switch req.Method {
	case "initialize":
		return map[string]any{
			"protocolVersion": ProtocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}, "resources": map[string]any{}},
			"serverInfo":      map[string]any{"name": s.name(), "version": s.version()},
		}, nil

	case "ping":
		return map[string]any{}, nil

	case "tools/list":
		descs := make([]map[string]any, 0, len(s.Tools))
		for _, t := range s.Tools {
			schema := t.Schema
			if schema == "" {
				schema = `{"type":"object"}`
			}
			descs = append(descs, map[string]any{
				"name": t.Name, "description": t.Description,
				"inputSchema": json.RawMessage(schema),
			})
		}
		sort.Slice(descs, func(i, j int) bool {
			return descs[i]["name"].(string) < descs[j]["name"].(string)
		})
		return map[string]any{"tools": descs}, nil

	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, &responseError{Code: -32602, Message: "invalid params"}
		}
		tool, ok := s.tool(params.Name)
		if !ok {
			return nil, &responseError{Code: -32602, Message: "unknown tool " + params.Name}
		}
		s.enter(params.Name)
		defer s.leave()
		if s.Delay > 0 {
			select {
			case <-time.After(s.Delay):
			case <-ctx.Done():
			}
		}
		text, err := tool.Fn(ctx, params.Arguments)
		if err != nil {
			return map[string]any{
				"content": []map[string]any{{"type": "text", "text": err.Error()}},
				"isError": true,
			}, nil
		}
		out := map[string]any{
			"content": []map[string]any{{"type": "text", "text": text}},
		}
		if tool.Structured != nil {
			out["structuredContent"] = tool.Structured(ctx, params.Arguments)
		}
		return out, nil

	case "resources/list":
		descs := make([]map[string]any, 0, len(s.Resources))
		for _, r := range s.Resources {
			descs = append(descs, map[string]any{
				"uri": r.URI, "name": r.Name, "mimeType": r.MimeType,
			})
		}
		return map[string]any{"resources": descs}, nil

	case "resources/read":
		var params struct {
			URI string `json:"uri"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return nil, &responseError{Code: -32602, Message: "invalid params"}
		}
		for _, r := range s.Resources {
			if r.URI == params.URI {
				return map[string]any{"contents": []map[string]any{
					{"uri": r.URI, "mimeType": r.MimeType, "text": r.Text},
				}}, nil
			}
		}
		return nil, &responseError{Code: -32002, Message: "resource not found: " + params.URI}

	default:
		return nil, &responseError{Code: -32601, Message: "method not found: " + req.Method}
	}
}

func (s *Server) tool(name string) (Tool, bool) {
	for _, t := range s.Tools {
		if t.Name == name {
			return t, true
		}
	}
	return Tool{}, false
}

func (s *Server) name() string {
	if s.Name == "" {
		return "mcptest"
	}
	return s.Name
}

func (s *Server) version() string {
	if s.Version == "" {
		return "0.1"
	}
	return s.Version
}

// Echo is a tool that returns its arguments as text — the simplest thing that
// proves a call reached the server and came back.
func Echo(name string) Tool {
	return Tool{
		Name:        name,
		Description: "echoes its arguments",
		Fn: func(_ context.Context, args map[string]any) (string, error) {
			blob, err := json.Marshal(args)
			if err != nil {
				return "", err
			}
			return string(blob), nil
		},
	}
}
