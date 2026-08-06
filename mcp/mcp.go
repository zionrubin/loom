// Package mcp connects Loom to Model Context Protocol servers: their tools
// become capabilities a stage declares and a task invokes, under the same
// envelope that governs models, secrets, and egress.
//
// # Where connections come from
//
// The interesting question about MCP is not the protocol, which is JSON-RPC
// with four methods that matter. It is where the connection lives. A
// connection is expensive to make (a process to spawn or a TLS handshake and
// an initialize round trip), stateful, and quota-bearing — three properties
// that make "just dial one where you need it" wrong at every scale a pipeline
// operates at.
//
// Spark answers this with mapPartitions: open the connection once per
// partition, amortize it across that partition's rows, close it at the end.
// That is the right instinct — per row is absurd, per job is not
// parallel-safe — but it is an answer shaped by JDBC, where a connection
// carries one statement at a time and therefore cannot be shared. MCP is
// JSON-RPC: every request carries an id and responses may return in any
// order, so one session serves any number of concurrent calls. The thing that
// has to be rationed is not the connection, it is the concurrency.
//
// So Loom splits the two. A [Session] is long-lived, multiplexed, and dialed
// once at provisioning — before the first task, so a misconfigured server
// fails the run instead of the first record, and so no task ever pays a
// handshake. A task leases a *call slot* rather than a connection, from a
// per-server semaphore that is the tool-side analogue of the scheduler's
// token-bucket admission control. Loom's partition — the unit Spark would
// open a connection for — is the task, a batch of records under one envelope,
// and it holds a slot for exactly as long as it is calling.
//
// The [Catalog] that owns all of this belongs to the host, next to the rate
// limiter and the budget governor, for the reason those live there: a
// connection is a property of an account and a server process, not of a
// pipeline. Ten agents on a fleet share one connection to the GitHub MCP
// server and one bound on how hard they may hit it, in the same way they share
// one quota and one wallet.
//
// # What a task carries
//
// Not the connection. A task envelope names the servers it may reach and the
// content digest of the tool descriptors it was planned against
// ([task.Envelope.MCP]) — connections are named, never carried, exactly as
// broadcasts are referenced and never copied. That is what keeps a task
// shippable to a remote executor, and it is also a contract: if a server comes
// back from a reconnect advertising different tools than the plan was built
// on, the digest no longer matches and calls fail loudly rather than silently
// invoking a different tool than the one that was planned, priced, and
// granted.
package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
)

// ProtocolVersion is the MCP revision Loom advertises in initialize. Servers
// negotiate down to a revision they support and answer with their own.
const ProtocolVersion = "2025-06-18"

// ClientName and ClientVersion identify Loom to servers in initialize.
const (
	ClientName    = "loom"
	ClientVersion = "0.1"
)

// Defaults applied to a Server that leaves the field zero.
const (
	// DefaultSessions is one, because MCP sessions are multiplexed: a second
	// transport buys parallelism only for a server that serializes internally.
	DefaultSessions = 1
	// DefaultMaxConcurrent bounds in-flight calls per server. It is a ceiling
	// on the load a fleet can put on one server, not on a pipeline's width.
	DefaultMaxConcurrent = 8
	// DefaultTimeout bounds one tool call.
	DefaultTimeout = 30 * time.Second
	// DefaultDialTimeout bounds connecting and discovering one server.
	DefaultDialTimeout = 20 * time.Second
)

// Server describes one MCP server a run may use. Exactly one transport is
// declared: Command for a stdio server Loom launches as a child process, or
// URL for a streamable-HTTP server it connects to.
//
// Credentials are named, not embedded. AuthSecret and EnvSecrets hold
// security.SecretRef values the host resolves through the secret broker at
// provisioning time, so the descriptor stays safe to log, serialize, and check
// into a config file — and so no task, op, or executor ever holds the
// credential. What a task gets is a lease on an already-authenticated session.
type Server struct {
	// Name is what stages declare with pipeline.WithMCP, and what tool names
	// are qualified by ("mcp/<name>/<tool>").
	Name string `json:"name"`

	// --- stdio transport ---

	// Command launches the server as a child process speaking newline-
	// delimited JSON-RPC on stdin/stdout.
	Command string   `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`
	// Env adds "K=V" entries to the child's environment. The child inherits
	// the parent environment; use EnvSecrets for anything credential-shaped.
	Env []string `json:"env,omitempty"`
	// EnvSecrets maps an environment variable name to a secret reference the
	// broker resolves at provisioning. The resolved value reaches the child
	// process and nothing else.
	EnvSecrets map[string]security.SecretRef `json:"env_secrets,omitempty"`
	// Dir is the child's working directory (default: the parent's).
	Dir string `json:"dir,omitempty"`

	// --- streamable HTTP transport ---

	// URL is the server's single MCP endpoint.
	URL string `json:"url,omitempty"`
	// Headers are sent with every request.
	Headers map[string]string `json:"headers,omitempty"`
	// AuthSecret names the credential sent in AuthHeader.
	AuthSecret security.SecretRef `json:"auth_secret,omitempty"`
	// AuthHeader defaults to "Authorization" and AuthScheme to "Bearer", so
	// the common case is one field: AuthSecret.
	AuthHeader string `json:"auth_header,omitempty"`
	AuthScheme string `json:"auth_scheme,omitempty"`

	// --- policy ---

	// Tools allowlists the server's tools. Empty means every tool the server
	// advertises, which is convenient and not least-privilege: naming the
	// three tools a pipeline actually calls means a server that grows a
	// fourth cannot be reached by an existing stage.
	Tools []string `json:"tools,omitempty"`
	// Sessions is how many transports to open (default DefaultSessions).
	// Raise it only for a server that processes requests serially.
	Sessions int `json:"sessions,omitempty"`
	// MaxConcurrent bounds in-flight calls across those sessions
	// (default DefaultMaxConcurrent).
	MaxConcurrent int `json:"max_concurrent,omitempty"`
	// Timeout bounds one tool call (default DefaultTimeout).
	Timeout time.Duration `json:"timeout,omitempty"`
	// DialTimeout bounds connecting and discovering (default DefaultDialTimeout).
	DialTimeout time.Duration `json:"dial_timeout,omitempty"`
}

// Stdio returns a stdio server descriptor.
func Stdio(name, command string, args ...string) Server {
	return Server{Name: name, Command: command, Args: args}
}

// HTTP returns a streamable-HTTP server descriptor.
func HTTP(name, endpoint string) Server {
	return Server{Name: name, URL: endpoint}
}

// WithTools returns a copy of s allowlisted to the named tools.
func (s Server) WithTools(tools ...string) Server {
	s.Tools = append(append([]string(nil), s.Tools...), tools...)
	return s
}

// WithAuth returns a copy of s that authenticates with a bearer credential
// the broker resolves from ref.
func (s Server) WithAuth(ref security.SecretRef) Server {
	s.AuthSecret = ref
	return s
}

// Endpoint is the network host this server contacts, or "" for a stdio server
// that opens no socket of its own. It is what the planner puts on a declaring
// stage's egress allowlist, and what the executor checks before every call —
// the same treatment a model provider's endpoint gets.
func (s Server) Endpoint() string {
	if s.URL == "" {
		return ""
	}
	u, err := url.Parse(s.URL)
	if err != nil {
		return ""
	}
	if h, _, err := net.SplitHostPort(u.Host); err == nil {
		return h
	}
	return u.Host
}

// Validate checks the descriptor is well-formed and unambiguous.
func (s Server) Validate() error {
	switch {
	case s.Name == "":
		return errors.New("mcp: server with empty name")
	case strings.ContainsAny(s.Name, "/ \t"):
		return fmt.Errorf("mcp: server name %q may not contain %q or whitespace", s.Name, "/")
	case s.Command == "" && s.URL == "":
		return fmt.Errorf("mcp: server %q declares neither Command (stdio) nor URL (http)", s.Name)
	case s.Command != "" && s.URL != "":
		return fmt.Errorf("mcp: server %q declares both Command and URL; pick one transport", s.Name)
	case s.URL != "" && s.Endpoint() == "":
		return fmt.Errorf("mcp: server %q: cannot parse a host out of URL %q", s.Name, s.URL)
	case s.Command == "" && len(s.EnvSecrets) > 0:
		return fmt.Errorf("mcp: server %q: EnvSecrets applies to a child process, not an HTTP endpoint", s.Name)
	case s.URL == "" && s.AuthSecret != "":
		return fmt.Errorf("mcp: server %q: AuthSecret is an HTTP header credential; a stdio server takes EnvSecrets", s.Name)
	}
	return nil
}

func (s Server) sessions() int {
	if s.Sessions > 0 {
		return s.Sessions
	}
	return DefaultSessions
}

func (s Server) maxConcurrent() int {
	if s.MaxConcurrent > 0 {
		return s.MaxConcurrent
	}
	return DefaultMaxConcurrent
}

func (s Server) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return DefaultTimeout
}

func (s Server) dialTimeout() time.Duration {
	if s.DialTimeout > 0 {
		return s.DialTimeout
	}
	return DefaultDialTimeout
}

// secretRefs lists every credential this server needs, so the host can resolve
// them once at provisioning and audit exactly that set.
func (s Server) secretRefs() []security.SecretRef {
	var out []security.SecretRef
	if s.AuthSecret != "" {
		out = append(out, s.AuthSecret)
	}
	for _, ref := range s.EnvSecrets {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ToolName qualifies an MCP tool with its server, producing the name a stage
// grants and an op invokes: "mcp/github/search_code". One namespace with local
// tools is deliberate — a grant is a grant, and security.ToolCap governs both.
func ToolName(server, tool string) string { return "mcp/" + server + "/" + tool }

// SplitToolName reverses ToolName, reporting whether name is an MCP tool.
func SplitToolName(name string) (server, tool string, ok bool) {
	rest, found := strings.CutPrefix(name, "mcp/")
	if !found {
		return "", "", false
	}
	server, tool, found = strings.Cut(rest, "/")
	if !found || server == "" || tool == "" {
		return "", "", false
	}
	return server, tool, true
}

// ToolDesc is one tool as its server describes it.
type ToolDesc struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema,omitempty"`
}

// Content is one item of a tool result.
type Content struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	Data     string          `json:"data,omitempty"` // base64, for image and audio
	MimeType string          `json:"mimeType,omitempty"`
	Resource json.RawMessage `json:"resource,omitempty"`
}

// CallResult is a tools/call response.
type CallResult struct {
	Content    []Content       `json:"content,omitempty"`
	Structured json.RawMessage `json:"structuredContent,omitempty"`
	IsError    bool            `json:"isError,omitempty"`
}

// Text concatenates the textual content blocks, which is what most tools
// return and what most prompts want.
func (r CallResult) Text() string {
	var b strings.Builder
	for _, c := range r.Content {
		if c.Text == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(c.Text)
	}
	return b.String()
}

// Record renders the result as the JSON-serializable value an op receives:
// "text" always, "structured" when the server returned structured content.
// Keeping it a map rather than a bare string is what lets a structured tool
// stay structured on its way into a record.
func (r CallResult) Record() map[string]any {
	out := map[string]any{"text": r.Text()}
	if len(r.Structured) > 0 {
		var v any
		if err := json.Unmarshal(r.Structured, &v); err == nil {
			out["structured"] = v
		}
	}
	return out
}

// Text extracts the text of a tool result returned through core.Session.Invoke.
func Text(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case map[string]any:
		if s, ok := t["text"].(string); ok {
			return s
		}
	case CallResult:
		return t.Text()
	}
	return ""
}

// ResourceContent is one item of a resources/read response.
type ResourceContent struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType,omitempty"`
	Text     string `json:"text,omitempty"`
	Blob     string `json:"blob,omitempty"` // base64
}

// Bytes returns the content's bytes, decoding a binary blob.
func (c ResourceContent) Bytes() ([]byte, error) {
	if c.Blob != "" {
		return base64.StdEncoding.DecodeString(c.Blob)
	}
	return []byte(c.Text), nil
}

// ResourceDesc is one resource as its server describes it.
type ResourceDesc struct {
	URI         string `json:"uri"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
}

// Digest is the content hash of a set of tool descriptors: the contract a plan
// was compiled against.
//
// It joins the fingerprint of every stage that declares the server, so
// upgrading a server invalidates exactly the cached results that could have
// called the tools that changed — the same argument that puts a broadcast's
// content hash in its readers' fingerprints. Descriptors are sorted by name
// first, because tools/list is a set and its order is the server's business.
func Digest(tools []ToolDesc) string {
	cp := append([]ToolDesc(nil), tools...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Name < cp[j].Name })
	parts := make([]any, 0, len(cp))
	for _, t := range cp {
		parts = append(parts, map[string]string{
			"name": t.Name, "description": t.Description,
			"schema": string(t.InputSchema),
		})
	}
	h, err := store.Key(parts...)
	if err != nil {
		return ""
	}
	return h
}

// --- JSON-RPC ------------------------------------------------------------

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      *int64 `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *rpcError) Error() string {
	if len(e.Data) > 0 {
		return fmt.Sprintf("%s (code %d): %s", e.Message, e.Code, string(e.Data))
	}
	return fmt.Sprintf("%s (code %d)", e.Message, e.Code)
}

// class maps a JSON-RPC error onto Loom's failure taxonomy, which is what
// decides whether the scheduler retries.
//
// Only the server's own internal error (-32603) is worth retrying identically.
// A parse error, an unknown method, or bad parameters (-32700, -32600, -32601,
// -32602) will be exactly as wrong next time. Server-defined codes
// (-32000..-32099) carry no agreed meaning, so they are treated as permanent
// too: guessing that one of them means "rate limited" would burn a retry budget
// on the strength of a number nobody standardized.
func (e *rpcError) class(err error) error {
	if e.Code == -32603 {
		return core.Transient(err)
	}
	return core.Permanent(err)
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *int64          `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// conn is one JSON-RPC message channel. Both transports multiplex: Call may be
// invoked concurrently and responses are matched to requests by id.
type conn interface {
	Call(ctx context.Context, method string, params any) (json.RawMessage, error)
	Notify(ctx context.Context, method string, params any) error
	Close() error
}

// --- stdio transport -----------------------------------------------------

// stdioConn speaks newline-delimited JSON-RPC over a pair of pipes, with one
// reader goroutine demultiplexing responses to the callers waiting on them.
type stdioConn struct {
	w    io.WriteCloser
	stop func() error // terminates the child process, if any
	name string

	nextID atomic.Int64

	mu      sync.Mutex
	pending map[int64]chan rpcResponse
	closed  bool
	readErr error

	stderr *tail
	done   chan struct{}
}

// maxLine bounds one JSON-RPC message from a server. Tool results carrying a
// file or a page of search results are routinely large, so the default
// bufio.Scanner limit (64 KiB) is far too small.
const maxLine = 32 << 20

func newStdioConn(name string, r io.Reader, w io.WriteCloser, stderr *tail, stop func() error) *stdioConn {
	c := &stdioConn{
		w: w, stop: stop, name: name,
		pending: map[int64]chan rpcResponse{},
		stderr:  stderr,
		done:    make(chan struct{}),
	}
	go c.read(r)
	return c
}

func (c *stdioConn) read(r io.Reader) {
	defer close(c.done)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxLine)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var resp rpcResponse
		if err := json.Unmarshal(line, &resp); err != nil || resp.ID == nil {
			// Server-initiated requests and notifications (logging, progress,
			// list-changed) are not errors — Loom declares no capabilities that
			// invite them, and ignoring one is better than tearing down a
			// working session over it.
			continue
		}
		c.deliver(*resp.ID, resp)
	}
	err := sc.Err()
	if err == nil {
		err = io.EOF
	}
	c.failAll(err)
}

func (c *stdioConn) deliver(id int64, resp rpcResponse) {
	c.mu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if ch != nil {
		ch <- resp
	}
}

// failAll releases every waiter when the pipe dies, so a crashed server fails
// its in-flight calls immediately instead of holding them until their
// deadlines. The failure is transient: the pool redials and the scheduler
// retries.
func (c *stdioConn) failAll(err error) {
	c.mu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	pending := c.pending
	c.pending = map[int64]chan rpcResponse{}
	c.closed = true
	c.mu.Unlock()
	for id, ch := range pending {
		id := id
		ch <- rpcResponse{ID: &id, Error: &rpcError{Code: -32603, Message: c.deadMessage(err)}}
	}
}

func (c *stdioConn) deadMessage(err error) string {
	msg := fmt.Sprintf("mcp server %q closed the connection: %v", c.name, err)
	if c.stderr != nil {
		if s := c.stderr.String(); s != "" {
			msg += "; stderr: " + s
		}
	}
	return msg
}

func (c *stdioConn) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := c.nextID.Add(1)
	ch := make(chan rpcResponse, 1)

	c.mu.Lock()
	if c.closed {
		err := c.readErr
		c.mu.Unlock()
		if err == nil {
			err = errors.New("session closed")
		}
		return nil, core.Transient(fmt.Errorf("mcp %s: %w", c.name, err))
	}
	c.pending[id] = ch
	c.mu.Unlock()

	if err := c.write(rpcRequest{JSONRPC: "2.0", ID: &id, Method: method, Params: params}); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}

	select {
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, core.Transient(fmt.Errorf("mcp %s: %s: %w", c.name, method, ctx.Err()))
	case resp := <-ch:
		if resp.Error != nil {
			return nil, resp.Error.class(fmt.Errorf("mcp %s: %s: %w", c.name, method, resp.Error))
		}
		return resp.Result, nil
	}
}

func (c *stdioConn) Notify(ctx context.Context, method string, params any) error {
	return c.write(rpcRequest{JSONRPC: "2.0", Method: method, Params: params})
}

func (c *stdioConn) write(req rpcRequest) error {
	blob, err := json.Marshal(req)
	if err != nil {
		return core.Permanent(fmt.Errorf("mcp %s: encode %s: %w", c.name, req.Method, err))
	}
	// json.Marshal escapes control characters, so a message never contains an
	// embedded newline and the delimiter is unambiguous.
	blob = append(blob, '\n')

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return core.Transient(fmt.Errorf("mcp %s: session closed", c.name))
	}
	if _, err := c.w.Write(blob); err != nil {
		return core.Transient(fmt.Errorf("mcp %s: write: %w", c.name, err))
	}
	return nil
}

func (c *stdioConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	// Closing stdin is how an MCP server is asked to shut down; the reader
	// goroutine ends when the child closes its stdout in response.
	_ = c.w.Close()
	if c.stop != nil {
		select {
		case <-c.done:
		case <-time.After(2 * time.Second):
		}
		return c.stop()
	}
	return nil
}

// tail keeps the last n bytes a server wrote to stderr. A server that fails to
// start says why there and nowhere else, and that message is the difference
// between a debuggable error and "connection closed".
type tail struct {
	mu  sync.Mutex
	buf []byte
	n   int
}

func newTail(n int) *tail { return &tail{n: n} }

func (t *tail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.n {
		t.buf = t.buf[len(t.buf)-t.n:]
	}
	return len(p), nil
}

func (t *tail) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(string(t.buf))
}

// dialStdio launches the server as a child process. It deliberately takes no
// context: the dial has a deadline, but the process it starts must outlive that
// deadline — binding the child to the dial context would kill the connection
// twenty seconds after it was made.
func dialStdio(s Server, secrets map[security.SecretRef]string) (conn, error) {
	cmd := exec.Command(s.Command, s.Args...)
	cmd.Dir = s.Dir
	cmd.Env = append(os.Environ(), s.Env...)
	for name, ref := range s.EnvSecrets {
		val, ok := secrets[ref]
		if !ok {
			return nil, core.Permanent(fmt.Errorf(
				"mcp %s: secret %q for env %s was not resolved", s.Name, ref, name))
		}
		cmd.Env = append(cmd.Env, name+"="+val)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, core.Permanent(fmt.Errorf("mcp %s: stdin: %w", s.Name, err))
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, core.Permanent(fmt.Errorf("mcp %s: stdout: %w", s.Name, err))
	}
	errTail := newTail(4 << 10)
	cmd.Stderr = errTail

	if err := cmd.Start(); err != nil {
		return nil, core.Permanent(fmt.Errorf("mcp %s: start %q: %w", s.Name, s.Command, err))
	}
	stop := func() error {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil
	}
	return newStdioConn(s.Name, stdout, stdin, errTail, stop), nil
}

// --- streamable HTTP transport -------------------------------------------

// httpConn speaks streamable HTTP: one POST per JSON-RPC message, with the
// response arriving either as a JSON body or as an SSE stream carrying it.
// Concurrency needs no demultiplexing here — the transport pairs request and
// response — but the session id the server assigns at initialize must ride on
// every later request.
type httpConn struct {
	client   *http.Client
	url      string
	name     string
	headers  map[string]string
	nextID   atomic.Int64
	sessMu   sync.RWMutex
	session  string
	protocol string
}

func dialHTTP(s Server, secrets map[security.SecretRef]string) (conn, error) {
	headers := map[string]string{}
	for k, v := range s.Headers {
		headers[k] = v
	}
	if s.AuthSecret != "" {
		val, ok := secrets[s.AuthSecret]
		if !ok {
			return nil, core.Permanent(fmt.Errorf(
				"mcp %s: secret %q was not resolved", s.Name, s.AuthSecret))
		}
		header, scheme := s.AuthHeader, s.AuthScheme
		if header == "" {
			header = "Authorization"
		}
		if scheme == "" {
			scheme = "Bearer"
		}
		headers[header] = strings.TrimSpace(scheme + " " + val)
	}
	return &httpConn{
		client:   &http.Client{Timeout: 0}, // deadlines come from the call context
		url:      s.URL,
		name:     s.Name,
		headers:  headers,
		protocol: ProtocolVersion,
	}, nil
}

func (c *httpConn) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := c.nextID.Add(1)
	body, err := c.post(ctx, rpcRequest{JSONRPC: "2.0", ID: &id, Method: method, Params: params})
	if err != nil {
		return nil, err
	}
	resp, err := decodeHTTPBody(c.name, method, body, id)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error.class(fmt.Errorf("mcp %s: %s: %w", c.name, method, resp.Error))
	}
	return resp.Result, nil
}

func (c *httpConn) Notify(ctx context.Context, method string, params any) error {
	_, err := c.post(ctx, rpcRequest{JSONRPC: "2.0", Method: method, Params: params})
	return err
}

func (c *httpConn) post(ctx context.Context, rpc rpcRequest) ([]byte, error) {
	blob, err := json.Marshal(rpc)
	if err != nil {
		return nil, core.Permanent(fmt.Errorf("mcp %s: encode %s: %w", c.name, rpc.Method, err))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(blob))
	if err != nil {
		return nil, core.Permanent(fmt.Errorf("mcp %s: request: %w", c.name, err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", c.protocol)
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	c.sessMu.RLock()
	if c.session != "" {
		req.Header.Set("Mcp-Session-Id", c.session)
	}
	c.sessMu.RUnlock()

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, core.Transient(fmt.Errorf("mcp %s: %s: %w", c.name, rpc.Method, err))
	}
	defer resp.Body.Close()

	if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
		c.sessMu.Lock()
		c.session = sid
		c.sessMu.Unlock()
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxLine))
	if err != nil {
		return nil, core.Transient(fmt.Errorf("mcp %s: %s: read body: %w", c.name, rpc.Method, err))
	}
	switch {
	case resp.StatusCode == http.StatusAccepted, resp.StatusCode == http.StatusNoContent:
		return nil, nil // a notification the server acknowledged without a body
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return nil, core.Transient(fmt.Errorf("mcp %s: %s: http %d: %s",
			c.name, rpc.Method, resp.StatusCode, strings.TrimSpace(string(body))))
	case resp.StatusCode >= 400:
		return nil, core.Permanent(fmt.Errorf("mcp %s: %s: http %d: %s",
			c.name, rpc.Method, resp.StatusCode, strings.TrimSpace(string(body))))
	}
	return body, nil
}

// decodeHTTPBody reads a JSON-RPC response out of either shape a streamable-
// HTTP server may answer with: a bare JSON body, or an SSE stream whose data
// frames carry the messages.
func decodeHTTPBody(name, method string, body []byte, id int64) (rpcResponse, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return rpcResponse{}, core.Transient(fmt.Errorf("mcp %s: %s: empty response", name, method))
	}
	if trimmed[0] == '{' || trimmed[0] == '[' {
		return decodeRPC(name, method, trimmed, id)
	}
	sc := bufio.NewScanner(bytes.NewReader(trimmed))
	sc.Buffer(make([]byte, 0, 64<<10), maxLine)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data:")
		if !ok {
			continue
		}
		frame := bytes.TrimSpace([]byte(data))
		if len(frame) == 0 {
			continue
		}
		resp, err := decodeRPC(name, method, frame, id)
		if err != nil || resp.ID == nil || *resp.ID != id {
			continue // a notification or another response sharing the stream
		}
		return resp, nil
	}
	return rpcResponse{}, core.Transient(fmt.Errorf(
		"mcp %s: %s: event stream ended without a response", name, method))
}

func decodeRPC(name, method string, blob []byte, id int64) (rpcResponse, error) {
	// A batch is legal JSON-RPC; take the response that answers this request.
	if blob[0] == '[' {
		var batch []rpcResponse
		if err := json.Unmarshal(blob, &batch); err != nil {
			return rpcResponse{}, core.Permanent(fmt.Errorf("mcp %s: %s: decode: %w", name, method, err))
		}
		for _, r := range batch {
			if r.ID != nil && *r.ID == id {
				return r, nil
			}
		}
		return rpcResponse{}, core.Transient(fmt.Errorf(
			"mcp %s: %s: batch carried no response for this request", name, method))
	}
	var resp rpcResponse
	if err := json.Unmarshal(blob, &resp); err != nil {
		return rpcResponse{}, core.Permanent(fmt.Errorf("mcp %s: %s: decode: %w", name, method, err))
	}
	return resp, nil
}

func (c *httpConn) Close() error { return nil }

// --- session -------------------------------------------------------------

// Session is one live, multiplexed connection to a server: initialized, its
// tools discovered, and safe for concurrent use.
type Session struct {
	server Server
	conn   conn

	ServerInfo   ServerInfo
	Capabilities json.RawMessage
	Instructions string
	Tools        []ToolDesc
}

// ServerInfo identifies the peer.
type ServerInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version,omitempty"`
}

type initializeResult struct {
	ProtocolVersion string          `json:"protocolVersion"`
	Capabilities    json.RawMessage `json:"capabilities,omitempty"`
	ServerInfo      ServerInfo      `json:"serverInfo"`
	Instructions    string          `json:"instructions,omitempty"`
}

// Dial opens a session: transport, initialize handshake, and tools/list. The
// three happen together because a connection without a discovered tool set is
// not usable — the digest those descriptors produce is what a plan is
// compiled against.
func Dial(ctx context.Context, s Server, secrets map[security.SecretRef]string) (*Session, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.dialTimeout())
	defer cancel()

	var (
		c   conn
		err error
	)
	if s.Command != "" {
		c, err = dialStdio(s, secrets)
	} else {
		c, err = dialHTTP(s, secrets)
	}
	if err != nil {
		return nil, err
	}

	sess := &Session{server: s, conn: c}
	if err := sess.initialize(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	tools, err := sess.ListTools(ctx)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	sess.Tools = tools
	return sess, nil
}

func (s *Session) initialize(ctx context.Context) error {
	raw, err := s.conn.Call(ctx, "initialize", map[string]any{
		"protocolVersion": ProtocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": ClientName, "version": ClientVersion},
	})
	if err != nil {
		return err
	}
	var res initializeResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return core.Permanent(fmt.Errorf("mcp %s: initialize: %w", s.server.Name, err))
	}
	s.ServerInfo, s.Capabilities, s.Instructions = res.ServerInfo, res.Capabilities, res.Instructions
	if hc, ok := s.conn.(*httpConn); ok && res.ProtocolVersion != "" {
		hc.protocol = res.ProtocolVersion
	}
	return s.conn.Notify(ctx, "notifications/initialized", nil)
}

// ListTools returns every tool the server advertises, following pagination.
func (s *Session) ListTools(ctx context.Context) ([]ToolDesc, error) {
	var out []ToolDesc
	cursor := ""
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := s.conn.Call(ctx, "tools/list", params)
		if err != nil {
			return nil, err
		}
		var page struct {
			Tools      []ToolDesc `json:"tools"`
			NextCursor string     `json:"nextCursor"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, core.Permanent(fmt.Errorf("mcp %s: tools/list: %w", s.server.Name, err))
		}
		out = append(out, page.Tools...)
		if page.NextCursor == "" || page.NextCursor == cursor {
			break
		}
		cursor = page.NextCursor
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Call invokes a tool. A server that answers with isError has run the tool and
// reported failure, which is a permanent failure for the task: the same
// arguments will fail the same way, and burning the retry budget on them is
// exactly what Loom's failure taxonomy exists to prevent.
func (s *Session) Call(ctx context.Context, tool string, args map[string]any) (CallResult, error) {
	if args == nil {
		args = map[string]any{}
	}
	raw, err := s.conn.Call(ctx, "tools/call", map[string]any{"name": tool, "arguments": args})
	if err != nil {
		return CallResult{}, err
	}
	var res CallResult
	if err := json.Unmarshal(raw, &res); err != nil {
		return CallResult{}, core.Permanent(fmt.Errorf("mcp %s: tools/call %s: %w", s.server.Name, tool, err))
	}
	if res.IsError {
		return res, core.Permanent(fmt.Errorf("mcp %s: tool %q failed: %s",
			s.server.Name, tool, res.Text()))
	}
	return res, nil
}

// ListResources returns the resources the server advertises.
func (s *Session) ListResources(ctx context.Context) ([]ResourceDesc, error) {
	var out []ResourceDesc
	cursor := ""
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := s.conn.Call(ctx, "resources/list", params)
		if err != nil {
			return nil, err
		}
		var page struct {
			Resources  []ResourceDesc `json:"resources"`
			NextCursor string         `json:"nextCursor"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, core.Permanent(fmt.Errorf("mcp %s: resources/list: %w", s.server.Name, err))
		}
		out = append(out, page.Resources...)
		if page.NextCursor == "" || page.NextCursor == cursor {
			break
		}
		cursor = page.NextCursor
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URI < out[j].URI })
	return out, nil
}

// ReadResource reads one resource URI.
func (s *Session) ReadResource(ctx context.Context, uri string) ([]ResourceContent, error) {
	raw, err := s.conn.Call(ctx, "resources/read", map[string]any{"uri": uri})
	if err != nil {
		return nil, err
	}
	var res struct {
		Contents []ResourceContent `json:"contents"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, core.Permanent(fmt.Errorf("mcp %s: resources/read %s: %w", s.server.Name, uri, err))
	}
	return res.Contents, nil
}

// Ping checks the session is alive.
func (s *Session) Ping(ctx context.Context) error {
	_, err := s.conn.Call(ctx, "ping", map[string]any{})
	return err
}

// Close ends the session and, for a stdio server, the child process.
func (s *Session) Close() error { return s.conn.Close() }
