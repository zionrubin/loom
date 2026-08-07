// Package llamacpptest is a minimal in-process llama.cpp server for testing
// Loom pipelines that run on local models, and for examples that must run
// offline.
//
// It is to package llamacpp what [mcptest] is to package mcp and what
// model.Mock is to a provider: a real implementation of the wire protocol
// with deterministic, scripted behavior, so the local-inference path can be
// exercised — its prompts, its failures, its KV-cache reuse and its
// concurrency — without a GPU, a model file, or a downloaded gigabyte.
//
// It listens on a real loopback socket rather than short-circuiting the
// client, which is the point: the provider makes a genuine HTTP request to a
// genuine 127.0.0.1 address, so the egress allowlist a local binding produces
// is exercised rather than bypassed.
//
// # Slots are observed, not enforced
//
// A real llama.cpp server queues requests past its slot count, absorbing
// oversubscription silently. This one does not: it records how many calls
// were in flight at once and hands the peak back through [Server.Peak], so a
// test can assert that Loom's admission control kept within the device's
// ceiling instead of asserting that the server papered over exceeding it.
//
// [mcptest]: https://pkg.go.dev/github.com/zionrubin/loom/mcp/mcptest
package llamacpptest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/model"
)

// Prompt is one request as the server received it, already split back into
// the two halves Loom sent: the stage-stable head and the per-record body.
type Prompt struct {
	Model string
	// System is the stage-stable head — the provider's System and Prefix
	// joined, which is what the prompt cache is keyed on.
	System string
	// User is the per-record remainder.
	User        string
	MaxTokens   int
	CachePrompt bool
}

// Full returns the prompt as one string, in the order the model sees it.
func (p Prompt) Full() string {
	if p.System == "" {
		return p.User
	}
	return p.System + "\n\n" + p.User
}

// cacheDepth bounds how many past prompts are kept for prefix matching. A
// real server's reuse is bounded by its KV cache; this is the same idea with
// a much simpler eviction rule.
const cacheDepth = 64

// Server is a scriptable llama.cpp-compatible server.
type Server struct {
	// Model is the file path /props reports as loaded. Empty yields a
	// plausible one, because a deployment reading provenance should get
	// something to read.
	Model string
	// Slots is the server's decode width, reported by /props as total_slots
	// and discovered from there by llamacpp.Register. Zero means one.
	Slots int
	// ContextSize is the per-slot context window reported by /props.
	ContextSize int
	// Generate produces the reply. Returning an error yields HTTP 500, which
	// Loom classifies as transient — the same answer a real server gives when
	// a slot fails mid-decode. Nil generates a fixed reply.
	Generate func(Prompt) (string, error)
	// Delay sleeps inside every request. Observing concurrency needs calls
	// that overlap, and overlap needs duration.
	Delay time.Duration
	// APIKey, when set, is required as a bearer token, mirroring a server
	// started with --api-key.
	APIKey string
	// Failures are HTTP statuses returned by successive requests before
	// Generate runs; each request consumes one until the list is empty. 503
	// is what a real server answers while it is still loading its weights,
	// which makes it the natural way to exercise transient retry.
	Failures []int

	mu       sync.Mutex
	http     *httptest.Server
	calls    int
	cached   int
	inFlight int
	peak     int
	seen     []string
}

// Start listens on a loopback address and returns the base URL to hand
// [llamacpp.New], plus the function that shuts the server down.
func (s *Server) Start() (baseURL string, stop func()) {
	srv := httptest.NewServer(s.Handler())
	s.mu.Lock()
	s.http = srv
	s.mu.Unlock()
	return srv.URL, srv.Close
}

// Handler returns the server's routes, for mounting somewhere of your own —
// a fixed port, a TLS listener, a mux shared with other fakes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/props", s.guarded(s.handleProps))
	mux.HandleFunc("/health", s.guarded(s.handleHealth))
	mux.HandleFunc("/v1/chat/completions", s.guarded(s.handleChat))
	return mux
}

// Calls returns how many completion requests the server has served,
// including scripted failures.
func (s *Server) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// Cached returns how many prompt tokens the server has served from its
// simulated KV cache across every call — what the shared prefix saved.
func (s *Server) Cached() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cached
}

// Peak returns the most calls that were ever in flight at once. Compare it
// with Slots to check that admission control respected the device's ceiling.
func (s *Server) Peak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
}

// slots is the configured decode width, defaulting to one.
func (s *Server) slots() int {
	if s.Slots <= 0 {
		return 1
	}
	return s.Slots
}

// guarded wraps a route with the bearer-token check a server started with
// --api-key applies to all of them.
func (s *Server) guarded(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.APIKey != "" && r.Header.Get("Authorization") != "Bearer "+s.APIKey {
			writeError(w, http.StatusUnauthorized, "Invalid API Key")
			return
		}
		h(w, r)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleProps(w http.ResponseWriter, _ *http.Request) {
	path := s.Model
	if path == "" {
		path = "models/test-model.gguf"
	}
	ctx := s.ContextSize
	if ctx <= 0 {
		ctx = 4096
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"model_path":                  path,
		"total_slots":                 s.slots(),
		"default_generation_settings": map[string]any{"n_ctx": ctx},
	})
}

// chatRequest is the subset of the request this server reads.
type chatRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	MaxTokens   int  `json:"max_tokens"`
	CachePrompt bool `json:"cache_prompt"`
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	var body chatRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	prompt := Prompt{Model: body.Model, MaxTokens: body.MaxTokens, CachePrompt: body.CachePrompt}
	for _, m := range body.Messages {
		switch m.Role {
		case "system":
			prompt.System = m.Content
		default:
			prompt.User = m.Content
		}
	}

	status, cached := s.admit(prompt)
	defer s.release()
	if status != 0 {
		writeError(w, status, "scripted failure")
		return
	}
	if s.Delay > 0 {
		select {
		case <-time.After(s.Delay):
		case <-r.Context().Done():
			return
		}
	}

	text := "OK"
	if s.Generate != nil {
		out, err := s.Generate(prompt)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		text = out
	}

	promptTokens := model.EstimateTokens(prompt.Full())
	if cached > promptTokens {
		cached = promptTokens
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"model":   body.Model,
		"choices": []any{map[string]any{"finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": text}}},
		"usage": map[string]any{
			"prompt_tokens":         promptTokens,
			"completion_tokens":     model.EstimateTokens(text),
			"total_tokens":          promptTokens + model.EstimateTokens(text),
			"prompt_tokens_details": map[string]any{"cached_tokens": cached},
		},
		"timings": map[string]any{
			"prompt_n":    promptTokens - cached,
			"predicted_n": model.EstimateTokens(text),
			"cache_n":     cached,
		},
	})
}

// admit records one call's arrival and returns the scripted status for it (0
// to proceed) along with how many prompt tokens its prefix reuses.
func (s *Server) admit(p Prompt) (status, cached int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	s.inFlight++
	if s.inFlight > s.peak {
		s.peak = s.inFlight
	}
	if len(s.Failures) > 0 {
		status, s.Failures = s.Failures[0], s.Failures[1:]
		return status, 0
	}

	cached = s.reuseLocked(p)
	s.cached += cached
	return 0, cached
}

func (s *Server) release() {
	s.mu.Lock()
	s.inFlight--
	s.mu.Unlock()
}

// reuseLocked returns how many prompt tokens this request shares with one
// already served, which is what a real server finds in its KV cache and skips
// recomputing. Callers hold s.mu.
//
// The match is on the longest common prefix with any remembered prompt rather
// than on equality of the system message, because that is the property a real
// server actually exploits — and it is what makes Loom's Prefix/Prompt split
// pay off here instead of merely being tidy.
func (s *Server) reuseLocked(p Prompt) int {
	full := p.Full()
	defer func() {
		s.seen = append(s.seen, full)
		if len(s.seen) > cacheDepth {
			s.seen = s.seen[len(s.seen)-cacheDepth:]
		}
	}()
	if !p.CachePrompt {
		return 0
	}
	best := 0
	for _, past := range s.seen {
		if n := sharedPrefix(past, full); n > best {
			best = n
		}
	}
	if best == 0 {
		return 0
	}
	return model.EstimateTokens(full[:best])
}

// sharedPrefix returns the length in bytes of the leading run a and b share.
func sharedPrefix(a, b string) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeError answers in llama.cpp's error envelope.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": msg,
			"type":    strings.ToLower(http.StatusText(status)),
		},
	})
}
