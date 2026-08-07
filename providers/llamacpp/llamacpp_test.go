package llamacpp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/providers/llamacpp"
	"github.com/zionrubin/loom/providers/llamacpp/llamacpptest"
	"github.com/zionrubin/loom/security"
)

// capture records the last request body a fake server received, so tests can
// assert on the wire form rather than on the adapter's internals.
type capture struct {
	mu     sync.Mutex
	body   map[string]any
	header http.Header
}

func (c *capture) last() (map[string]any, http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.body, c.header
}

// echoServer answers every completion with reply, recording what it was sent.
func echoServer(t *testing.T, reply string, usage map[string]any) (*llamacpp.Provider, *capture) {
	t.Helper()
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decoding request: %v", err)
		}
		cap.mu.Lock()
		cap.body, cap.header = body, r.Header.Clone()
		cap.mu.Unlock()

		out := map[string]any{
			"model":   "models/whatever-the-server-loaded.gguf",
			"choices": []any{map[string]any{"finish_reason": "stop", "message": map[string]any{"content": reply}}},
		}
		for k, v := range usage {
			out[k] = v
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	return llamacpp.New(srv.URL), cap
}

// TestCompleteRequestShape pins the wire form the adapter sends. The two
// properties that matter are that the stage-stable head arrives as a *single*
// system message — many GGUF chat templates accept only one — and that prompt
// caching is asked for unconditionally, since a local KV-cache write has no
// premium for a break-even rule to weigh.
func TestCompleteRequestShape(t *testing.T) {
	p, cap := echoServer(t, "hello", nil)

	_, err := p.Complete(context.Background(), model.CallContext{}, model.Request{
		Model:     "local-fast",
		System:    "You classify tickets.",
		Prefix:    "Rubric: be brief.",
		Prompt:    "Ticket: printer on fire",
		MaxTokens: 128,
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	body, _ := cap.last()
	if body["model"] != "local-fast" {
		t.Errorf("model = %v, want local-fast", body["model"])
	}
	if body["cache_prompt"] != true {
		t.Errorf("cache_prompt = %v, want true", body["cache_prompt"])
	}
	if body["stream"] != false {
		t.Errorf("stream = %v, want false", body["stream"])
	}
	if body["max_tokens"] != float64(128) {
		t.Errorf("max_tokens = %v, want 128", body["max_tokens"])
	}

	msgs, ok := body["messages"].([]any)
	if !ok || len(msgs) != 2 {
		t.Fatalf("messages = %v, want one system and one user message", body["messages"])
	}
	sys := msgs[0].(map[string]any)
	if sys["role"] != "system" {
		t.Errorf("first message role = %v, want system", sys["role"])
	}
	head, _ := sys["content"].(string)
	if !strings.Contains(head, "You classify tickets.") || !strings.Contains(head, "Rubric: be brief.") {
		t.Errorf("system message must carry System then Prefix, got %q", head)
	}
	if strings.Index(head, "You classify") > strings.Index(head, "Rubric") {
		t.Errorf("System must precede Prefix in the shared head, got %q", head)
	}
	user := msgs[1].(map[string]any)
	if user["role"] != "user" || user["content"] != "Ticket: printer on fire" {
		t.Errorf("second message = %v, want the per-record prompt as user", user)
	}
}

// TestCompleteReportsRegisteredModelID checks that the run report groups by
// the ID a binding names rather than by the path the server happens to have
// loaded — which is provenance, and reachable through Props.
func TestCompleteReportsRegisteredModelID(t *testing.T) {
	p, _ := echoServer(t, "hi", nil)
	resp, err := p.Complete(context.Background(), model.CallContext{},
		model.Request{Model: "local-fast", Prompt: "hi"})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Model != "local-fast" {
		t.Errorf("Model = %q, want the registered ID local-fast", resp.Model)
	}
	if resp.Text != "hi" {
		t.Errorf("Text = %q, want hi", resp.Text)
	}
	if resp.Usage.Requests != 1 {
		t.Errorf("Requests = %d, want 1", resp.Usage.Requests)
	}
}

// TestUsageSplit covers the normalization into core.Usage's disjoint prompt
// classes across the shapes different llama.cpp builds report.
func TestUsageSplit(t *testing.T) {
	tests := []struct {
		name               string
		payload            map[string]any
		input, cached, out int
	}{
		{
			name: "usage with cached details",
			payload: map[string]any{"usage": map[string]any{
				"prompt_tokens": 100, "completion_tokens": 20,
				"prompt_tokens_details": map[string]any{"cached_tokens": 60},
			}},
			input: 40, cached: 60, out: 20,
		},
		{
			name: "no reuse reported",
			payload: map[string]any{"usage": map[string]any{
				"prompt_tokens": 100, "completion_tokens": 20,
			}},
			input: 100, cached: 0, out: 20,
		},
		{
			// An older build with no usage block: timings counts the tokens
			// it had to evaluate, which excludes the ones it reused.
			name: "timings only",
			payload: map[string]any{"timings": map[string]any{
				"prompt_n": 40, "cache_n": 60, "predicted_n": 20,
			}},
			input: 40, cached: 60, out: 20,
		},
		{
			// A breakdown larger than the total must not produce a negative
			// input count, whatever the server claims.
			name: "cached exceeds prompt",
			payload: map[string]any{"usage": map[string]any{
				"prompt_tokens": 50, "completion_tokens": 5,
				"prompt_tokens_details": map[string]any{"cached_tokens": 80},
			}},
			input: 0, cached: 50, out: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := echoServer(t, "out", tc.payload)
			resp, err := p.Complete(context.Background(), model.CallContext{},
				model.Request{Model: "m", Prompt: "p"})
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			u := resp.Usage
			if u.InputTokens != tc.input || u.CacheReadTokens != tc.cached || u.OutputTokens != tc.out {
				t.Errorf("usage = in %d/cached %d/out %d, want in %d/cached %d/out %d",
					u.InputTokens, u.CacheReadTokens, u.OutputTokens, tc.input, tc.cached, tc.out)
			}
			if u.CacheWriteTokens != 0 {
				t.Errorf("CacheWriteTokens = %d; a local KV cache is a byproduct, never a write to pay for", u.CacheWriteTokens)
			}
		})
	}
}

// TestEndpointIsLoopbackHost checks that a local provider names its host
// rather than claiming to be in-process. Naming it is what puts loopback on
// the stage's egress allowlist, where the executor enforces it.
func TestEndpointIsLoopbackHost(t *testing.T) {
	for base, want := range map[string]string{
		"http://127.0.0.1:8080":  "127.0.0.1",
		"http://localhost:8081/": "localhost",
		"https://gpu.internal":   "gpu.internal",
		"http://[::1]:8080":      "::1",
	} {
		if got := llamacpp.New(base).Endpoint(); got != want {
			t.Errorf("New(%q).Endpoint() = %q, want %q", base, got, want)
		}
	}
	if got := llamacpp.New("").BaseURL(); got != llamacpp.DefaultBaseURL {
		t.Errorf("New(\"\").BaseURL() = %q, want %q", got, llamacpp.DefaultBaseURL)
	}
}

// TestErrorClassification checks the recovery each failure earns. The local
// cases are the interesting ones: 503 is both "still loading the weights" and
// "no slot free", and waiting is the right answer to both.
func TestErrorClassification(t *testing.T) {
	for status, want := range map[int]core.FailureClass{
		http.StatusBadRequest:          core.FailPermanent,
		http.StatusUnauthorized:        core.FailPermanent,
		http.StatusNotFound:            core.FailPermanent,
		http.StatusTooManyRequests:     core.FailTransient,
		http.StatusInternalServerError: core.FailTransient,
		http.StatusServiceUnavailable:  core.FailTransient,
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{"code": status, "message": "loading model"},
			})
		}))
		_, err := llamacpp.New(srv.URL).Complete(context.Background(), model.CallContext{},
			model.Request{Model: "m", Prompt: "p"})
		srv.Close()
		if err == nil {
			t.Fatalf("HTTP %d: expected an error", status)
		}
		if got := core.ClassOf(err); got != want {
			t.Errorf("HTTP %d classified %s, want %s (%v)", status, got, want, err)
		}
		if !strings.Contains(err.Error(), "loading model") {
			t.Errorf("HTTP %d should surface the server's explanation, got %v", status, err)
		}
	}

	// A server that is not there at all: transient, because the common case
	// is a server that has not finished starting.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := srv.URL
	srv.Close()
	_, err := llamacpp.New(addr).Complete(context.Background(), model.CallContext{},
		model.Request{Model: "m", Prompt: "p"})
	if err == nil {
		t.Fatal("expected an error against a closed server")
	}
	if got := core.ClassOf(err); got != core.FailTransient {
		t.Errorf("unreachable server classified %s, want transient", got)
	}
	if !strings.Contains(err.Error(), "llama-server") {
		t.Errorf("an unreachable local server should say what is likely wrong, got %v", err)
	}
}

// TestNoCredentialWithoutAuth checks the default: a plain local server needs
// no secret, so no resolver is consulted and the envelope carries no grant.
func TestNoCredentialWithoutAuth(t *testing.T) {
	p, cap := echoServer(t, "ok", nil)
	if p.SecretRef() != "" {
		t.Errorf("SecretRef = %q, want empty for a server with no --api-key", p.SecretRef())
	}
	_, err := p.Complete(context.Background(), model.CallContext{}, model.Request{Model: "m", Prompt: "p"})
	if err != nil {
		t.Fatalf("Complete without a resolver must work: %v", err)
	}
	if _, header := cap.last(); header.Get("Authorization") != "" {
		t.Errorf("Authorization = %q, want none", header.Get("Authorization"))
	}
}

// TestAuthResolvesThroughBroker checks that a server started with --api-key
// is credentialed the same way a hosted provider is: named, never held,
// resolved per call through the task's broker.
func TestAuthResolvesThroughBroker(t *testing.T) {
	const ref = security.SecretRef("llama_api_key")
	srv := &llamacpptest.Server{APIKey: "s3cret", Generate: func(llamacpptest.Prompt) (string, error) {
		return "ok", nil
	}}
	base, stop := srv.Start()
	defer stop()

	p := llamacpp.New(base, llamacpp.WithAuth(ref))
	if p.SecretRef() != ref {
		t.Errorf("SecretRef = %q, want %q", p.SecretRef(), ref)
	}

	var asked security.SecretRef
	call := model.CallContext{TaskID: "t1", ResolveSecret: func(r security.SecretRef) (string, error) {
		asked = r
		return "s3cret", nil
	}}
	if _, err := p.Complete(context.Background(), call, model.Request{Model: "m", Prompt: "p"}); err != nil {
		t.Fatalf("Complete with a valid key: %v", err)
	}
	if asked != ref {
		t.Errorf("resolved %q, want %q", asked, ref)
	}

	// A task whose grants do not cover the secret fails permanently: retrying
	// an ungranted capability cannot help.
	denied := model.CallContext{TaskID: "t2", ResolveSecret: func(security.SecretRef) (string, error) {
		return "", errors.New("capability not granted")
	}}
	_, err := p.Complete(context.Background(), denied, model.Request{Model: "m", Prompt: "p"})
	if core.ClassOf(err) != core.FailPermanent {
		t.Errorf("denied secret classified %s, want permanent (%v)", core.ClassOf(err), err)
	}

	// The wrong key reaches the server and is rejected there — also permanent.
	wrong := model.CallContext{ResolveSecret: func(security.SecretRef) (string, error) { return "nope", nil }}
	_, err = p.Complete(context.Background(), wrong, model.Request{Model: "m", Prompt: "p"})
	if core.ClassOf(err) != core.FailPermanent {
		t.Errorf("rejected key classified %s, want permanent (%v)", core.ClassOf(err), err)
	}
}

// TestRegisterDiscoversTheDevicesCeiling is the point of registering against
// a live server: the concurrency ceiling comes from the hardware's own answer
// rather than from a number somebody typed.
func TestRegisterDiscoversTheDevicesCeiling(t *testing.T) {
	srv := &llamacpptest.Server{Model: "models/qwen3-4b-q4.gguf", Slots: 4, ContextSize: 8192}
	base, stop := srv.Start()
	defer stop()

	reg := model.NewRegistry()
	props, err := llamacpp.Register(context.Background(), reg, llamacpp.New(base), "local-fast", model.TierFast)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if props.Model != "models/qwen3-4b-q4.gguf" || props.Slots != 4 || props.ContextSize != 8192 {
		t.Errorf("Props = %+v, want the server's own answer", props)
	}

	info, err := reg.Get("local-fast")
	if err != nil {
		t.Fatal(err)
	}
	if info.Limits.MaxConcurrent != 4 {
		t.Errorf("MaxConcurrent = %d, want the server's 4 slots", info.Limits.MaxConcurrent)
	}
	if info.Limits.RequestsPerMinute != 0 || info.Limits.TokensPerMinute != 0 {
		t.Error("a local model meters no per-minute quota")
	}
	if info.SecretRef != "" {
		t.Errorf("SecretRef = %q; a plain local server needs no credential", info.SecretRef)
	}
	if (info.Pricing != model.Pricing{}) {
		t.Errorf("Pricing = %+v, want zero — a token you generate yourself costs no dollars", info.Pricing)
	}

	// The tier resolves, so a binding can name the capability rather than the
	// machine.
	if got, err := reg.ForTier(model.TierFast); err != nil || got.ID != "local-fast" {
		t.Errorf("ForTier(fast) = %v, %v", got.ID, err)
	}
}

// TestRegisterAgainstAKeyedServer covers the wiring-time credential: /props
// is gated like every other endpoint on a server started with --api-key, and
// registration happens before any task and therefore before any broker, so
// the key comes from the caller for that one request.
func TestRegisterAgainstAKeyedServer(t *testing.T) {
	const ref = security.SecretRef("llama_api_key")
	srv := &llamacpptest.Server{APIKey: "s3cret", Slots: 3}
	base, stop := srv.Start()
	defer stop()

	p := llamacpp.New(base, llamacpp.WithAuth(ref))
	reg := model.NewRegistry()

	// Without the key the server refuses, and registration says so rather
	// than registering a model nothing can call.
	if _, err := llamacpp.Register(context.Background(), reg, p, "gpu-box", model.TierDeep); err == nil {
		t.Fatal("registering without the key should fail against a keyed server")
	}

	props, err := llamacpp.Register(context.Background(), reg, p, "gpu-box", model.TierDeep,
		llamacpp.StaticKey("s3cret"))
	if err != nil {
		t.Fatalf("Register with the key: %v", err)
	}
	if props.Slots != 3 {
		t.Errorf("Slots = %d, want 3", props.Slots)
	}

	info, err := reg.Get("gpu-box")
	if err != nil {
		t.Fatal(err)
	}
	if info.SecretRef != ref {
		t.Errorf("SecretRef = %q, want %q — tasks must be granted the key they will resolve", info.SecretRef, ref)
	}
	if info.Limits.MaxConcurrent != 3 {
		t.Errorf("MaxConcurrent = %d, want 3", info.Limits.MaxConcurrent)
	}
}

// TestRegisterFailsWhenTheServerIsDown checks that a server which is not
// running is discovered while the pipeline is being wired, not on the first
// record.
func TestRegisterFailsWhenTheServerIsDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := srv.URL
	srv.Close()

	reg := model.NewRegistry()
	if _, err := llamacpp.Register(context.Background(), reg, llamacpp.New(addr), "local", model.TierFast); err == nil {
		t.Fatal("registering against a dead server should fail")
	}
	if _, err := reg.Get("local"); err == nil {
		t.Error("a failed registration must not leave the model registered")
	}
}

// TestKVCacheServesTheSharedPrefix is the local form of prefix caching: the
// second call over a shared head reuses it, and the reuse is reported as
// cache reads so the run report reads the same as it does against a hosted
// provider — at the zero it actually cost.
func TestKVCacheServesTheSharedPrefix(t *testing.T) {
	srv := &llamacpptest.Server{Generate: func(p llamacpptest.Prompt) (string, error) {
		return "classified", nil
	}}
	base, stop := srv.Start()
	defer stop()
	p := llamacpp.New(base)

	req := model.Request{
		Model:  "local-fast",
		System: "You classify tickets.",
		Prefix: strings.Repeat("A long shared rubric that every call in the stage repeats. ", 20),
	}

	req.Prompt = "Ticket: one"
	first, err := p.Complete(context.Background(), model.CallContext{}, req)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if first.Usage.CacheReadTokens != 0 {
		t.Errorf("the first call has nothing to reuse, got %d cached", first.Usage.CacheReadTokens)
	}

	req.Prompt = "Ticket: two"
	second, err := p.Complete(context.Background(), model.CallContext{}, req)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if second.Usage.CacheReadTokens == 0 {
		t.Fatal("the second call should reuse the shared head from the KV cache")
	}
	if second.Usage.InputTokens >= first.Usage.InputTokens {
		t.Errorf("reuse should shrink the tokens actually processed: %d then %d",
			first.Usage.InputTokens, second.Usage.InputTokens)
	}
	if got, want := second.Usage.PromptTokens(), first.Usage.PromptTokens(); got < want-2 || got > want+2 {
		t.Errorf("the whole prompt is still the whole prompt: %d then %d", want, got)
	}
}

// TestParamsAreDefaultsNotOverrides checks that pass-through sampling
// parameters cannot displace the fields the adapter derives from the request.
func TestParamsAreDefaultsNotOverrides(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		cap.mu.Lock()
		cap.body = body
		cap.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"x"}}]}`))
	}))
	defer srv.Close()

	p := llamacpp.New(srv.URL, llamacpp.WithParams(map[string]any{
		"temperature": 0.2,
		"model":       "hijacked",
		"stream":      true,
	}))
	if _, err := p.Complete(context.Background(), model.CallContext{},
		model.Request{Model: "local-fast", Prompt: "p"}); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	body, _ := cap.last()
	if body["temperature"] != 0.2 {
		t.Errorf("temperature = %v, want the configured 0.2", body["temperature"])
	}
	if body["model"] != "local-fast" {
		t.Errorf("model = %v; a parameter must not detach the call from its binding", body["model"])
	}
	if body["stream"] != false {
		t.Errorf("stream = %v; the adapter owns the transport shape", body["stream"])
	}
}
