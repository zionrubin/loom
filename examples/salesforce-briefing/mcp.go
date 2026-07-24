package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/brian-ai/loom/core"
	"github.com/zionrubin/brian-ai/loom/executor"
)

// mcpClient is a minimal MCP Streamable-HTTP client: JSON-RPC 2.0 over POST,
// accepting both plain-JSON and SSE-framed responses. It performs the
// initialize handshake lazily and carries the session id if the server
// issues one.
type mcpClient struct {
	endpoint string
	hc       *http.Client

	mu        sync.Mutex
	nextID    int
	sessionID string
	ready     bool
}

func newMCPClient(endpoint string) *mcpClient {
	return &mcpClient{endpoint: endpoint, hc: &http.Client{Timeout: 60 * time.Second}}
}

type rpcResponse struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (c *mcpClient) rpc(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	session := c.sessionID
	c.mu.Unlock()

	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": id, "method": method, "params": params,
	})
	if err != nil {
		return nil, core.Permanent(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, core.Permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if session != "" {
		req.Header.Set("Mcp-Session-Id", session)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, core.Transient(err)
	}
	defer resp.Body.Close()

	if s := resp.Header.Get("Mcp-Session-Id"); s != "" {
		c.mu.Lock()
		c.sessionID = s
		c.mu.Unlock()
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return nil, core.Transient(fmt.Errorf("mcp %s: HTTP %d", method, resp.StatusCode))
	}
	if resp.StatusCode >= 400 {
		return nil, core.Permanent(fmt.Errorf("mcp %s: HTTP %d", method, resp.StatusCode))
	}

	var out rpcResponse
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		out, err = readSSEResponse(resp.Body)
	} else {
		err = json.NewDecoder(resp.Body).Decode(&out)
	}
	if err != nil {
		return nil, core.Transient(fmt.Errorf("mcp %s: %w", method, err))
	}
	if out.Error != nil {
		return nil, core.Permanent(fmt.Errorf("mcp %s: %s (code %d)", method, out.Error.Message, out.Error.Code))
	}
	return out.Result, nil
}

// readSSEResponse scans an SSE stream for the first data frame carrying a
// JSON-RPC response (a frame with a "result" or "error" member).
func readSSEResponse(r io.Reader) (rpcResponse, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		var out rpcResponse
		if err := json.Unmarshal([]byte(data), &out); err != nil {
			continue // notification or non-JSON keepalive frame
		}
		if out.Result != nil || out.Error != nil {
			return out, nil
		}
	}
	if err := sc.Err(); err != nil {
		return rpcResponse{}, err
	}
	return rpcResponse{}, fmt.Errorf("event stream ended without a response")
}

// initialize performs the MCP handshake once; safe to call concurrently.
func (c *mcpClient) initialize(ctx context.Context) error {
	c.mu.Lock()
	done := c.ready
	c.mu.Unlock()
	if done {
		return nil
	}
	_, err := c.rpc(ctx, "initialize", map[string]any{
		"protocolVersion": "2025-03-26",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "loom-salesforce-briefing", "version": "0.1.0"},
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.ready = true
	c.mu.Unlock()
	return nil
}

// callTool invokes one MCP tool and returns the concatenated text content.
func (c *mcpClient) callTool(ctx context.Context, name string, args map[string]any) (string, error) {
	if err := c.initialize(ctx); err != nil {
		return "", err
	}
	raw, err := c.rpc(ctx, "tools/call", map[string]any{"name": name, "arguments": args})
	if err != nil {
		return "", err
	}
	var result struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", core.Permanent(fmt.Errorf("mcp tools/call %s: %w", name, err))
	}
	var parts []string
	for _, item := range result.Content {
		if item.Type == "text" && item.Text != "" {
			parts = append(parts, item.Text)
		}
	}
	text := strings.Join(parts, "\n")
	if result.IsError {
		return "", core.Permanent(fmt.Errorf("mcp tool %s: %s", name, text))
	}
	return text, nil
}

// tool exposes one MCP tool as a Loom executor.Tool, so pipeline stages can
// invoke it only when their envelope carries the matching ToolCap grant.
func (c *mcpClient) tool(name string) executor.Tool {
	return executor.FuncTool(name, func(ctx context.Context, args map[string]any) (any, error) {
		return c.callTool(ctx, name, args)
	})
}
