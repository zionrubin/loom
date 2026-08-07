// Package llamacpp adapts a llama.cpp server as a Loom model provider —
// inference that runs on hardware you own, behind the same Provider seam as a
// hosted API.
//
// Point it at a running `llama-server` and it speaks that server's
// OpenAI-compatible endpoint over plain HTTP, with no SDK and no dependency
// beyond the standard library. It targets that server specifically — it reads
// /props for the device's decode width and asks for llama.cpp's prompt-cache
// reuse by name — so another local server emulating the same endpoint works
// only insofar as it tolerates a field it does not recognize. What makes a
// backend "local" in Loom's sense is not the software but the three
// properties below, and a different one is a sibling package rather than a
// flag here.
//
// # What changes when the model is yours
//
// Nothing in the pipeline: a binding names a model, and stages neither know
// nor care where it runs. What changes is the envelope around the call, and
// each change is a simplification rather than a special case.
//
//   - Cost is zero, so the budget governor's dollar ceiling stops being the
//     bound that matters. The scarce resource is the device, and the device's
//     ceiling is a number of sequences decoded at once, which is what
//     [model.Limits.MaxConcurrent] expresses and [Register] discovers from the
//     server's own slot count rather than guessing.
//   - No credential is involved, so [model.Info.SecretRef] is empty and a
//     stage bound to a local model is planned with no secret grant at all —
//     there is nothing for it to resolve. ([WithAuth] covers a server started
//     with --api-key, which is still a broker-resolved secret like any other.)
//   - Egress is loopback, and it is still on the allowlist rather than
//     exempt from it. A stage bound to a local model carries 127.0.0.1 and
//     nothing else, so the envelope states plainly that this stage's records
//     cannot reach a vendor — and the executor enforces it before every call.
//
// # The prefix cache is the KV cache
//
// Loom splits a prompt into a stage-stable Prefix and a per-record Prompt so
// providers can serve the shared head from their prompt cache. On a llama.cpp
// server that cache is not an analogue of the KV cache; it *is* the KV cache,
// reused across requests whose prompts share a prefix. This adapter therefore
// asks for it unconditionally, where the planner asks a hosted provider for
// it only on stages issuing more than one call: that rule exists to earn back
// the premium a remote cache *write* costs, and writing a local KV cache
// costs nothing to earn back. Reused tokens come back as
// [core.Usage.CacheReadTokens], so the run report reads the same as it does
// against a hosted model, priced at the zero it actually cost.
package llamacpp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/security"
)

// DefaultBaseURL is where `llama-server` listens unless told otherwise.
const DefaultBaseURL = "http://127.0.0.1:8080"

// maxErrorBody bounds how much of a failed response is read into an error
// message, so a server answering with a large body cannot be quoted whole.
const maxErrorBody = 8 << 10

// Provider implements [model.Provider] against one llama.cpp server.
//
// One Provider is one server, because one server is one loaded model. A
// deployment running a small model and a large one runs two servers on two
// ports and registers two providers — which is what makes an escalation
// ladder from a fast local model to a strong one an ordinary Loom binding.
type Provider struct {
	name      string
	baseURL   string
	host      string
	client    *http.Client
	secretRef security.SecretRef
	params    map[string]any
}

// Option configures a Provider.
type Option func(*Provider)

// WithName sets the provider name reported to Loom (default "llamacpp").
// Give two servers distinct names so audit entries and reports say which
// machine — or which loaded model — answered.
func WithName(name string) Option {
	return func(p *Provider) { p.name = name }
}

// WithHTTPClient supplies the client used for every request.
//
// The default client sets no timeout on purpose: a local model on CPU can
// legitimately spend minutes on one response, and the call's context — which
// Loom derives from the run — is the right place to bound it.
func WithHTTPClient(c *http.Client) Option {
	return func(p *Provider) {
		if c != nil {
			p.client = c
		}
	}
}

// WithAuth names the secret holding the server's API key, for a server
// started with --api-key. It is resolved per call through the task's broker
// under the task's own grants and audited, exactly as a hosted provider's key
// is; the provider never holds it. Without this, no credential is involved
// and stages bound to the model are planned with no secret grant.
func WithAuth(ref security.SecretRef) Option {
	return func(p *Provider) { p.secretRef = ref }
}

// WithParams sets sampling parameters sent with every request — temperature,
// top_k, min_p, repeat_penalty, seed, grammar, and the rest of llama.cpp's
// pass-through fields.
//
// They are defaults beneath the request, not overrides on top of it: the
// fields this adapter derives from a [model.Request] (model, messages,
// max_tokens, stream, cache_prompt) always win, so a stray parameter cannot
// quietly detach a call from the pipeline that issued it.
func WithParams(params map[string]any) Option {
	return func(p *Provider) {
		p.params = make(map[string]any, len(params))
		for k, v := range params {
			p.params[k] = v
		}
	}
}

// New returns a provider for the llama.cpp server at baseURL
// ([DefaultBaseURL] if empty).
func New(baseURL string, opts ...Option) *Provider {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	p := &Provider{
		name:    "llamacpp",
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{},
	}
	p.host = hostOf(p.baseURL)
	for _, o := range opts {
		o(p)
	}
	return p
}

func (p *Provider) Name() string { return p.name }

// Endpoint is the host the provider contacts — loopback for a server on this
// machine. It is deliberately not "" (the in-process, always-allowed answer):
// a local model is still reached over a socket, and naming the host is what
// puts it on the stage's egress allowlist, where it is both auditable and
// enforced.
func (p *Provider) Endpoint() string { return p.host }

// BaseURL returns the server address this provider talks to.
func (p *Provider) BaseURL() string { return p.baseURL }

// SecretRef returns the secret this provider resolves, or "" when the server
// needs no credential. Planners use it to grant least-privilege envelopes.
func (p *Provider) SecretRef() security.SecretRef { return p.secretRef }

// hostOf extracts the bare host from a base URL, dropping the port the way
// [security.EgressPolicy] compares hosts.
func hostOf(baseURL string) string {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(u.Host); err == nil {
		return h
	}
	return u.Host
}

// chatMessage is one message in the request Loom sends.
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Complete implements [model.Provider].
func (p *Provider) Complete(ctx context.Context, call model.CallContext, req model.Request) (model.Response, error) {
	body := map[string]any{}
	// Sampling defaults first: the fields below own their keys.
	for k, v := range p.params {
		body[k] = v
	}
	body["model"] = req.Model
	body["messages"] = messages(req)
	body["stream"] = false
	// Reuse the KV cache from a previous request sharing this prompt's
	// prefix. Unconditional: locally there is no cache-write premium to earn
	// back, so there is nothing for a break-even rule to decide.
	body["cache_prompt"] = true
	if req.MaxTokens > 0 {
		body["max_tokens"] = req.MaxTokens
	}

	var resp chatResponse
	if err := p.post(ctx, call, "/v1/chat/completions", body, &resp); err != nil {
		return model.Response{}, err
	}
	if len(resp.Choices) == 0 {
		return model.Response{}, core.Transient(fmt.Errorf("llamacpp: %s returned no choices", p.baseURL))
	}

	return model.Response{
		Text: resp.Choices[0].Message.Content,
		// The registered ID, not the server's answer. A llama.cpp server
		// reports the path of the file it loaded, which is provenance rather
		// than an identifier: bindings, pricing, escalation ladders and the
		// run report all speak the registry's ID, and Props reports what the
		// server actually loaded.
		Model: req.Model,
		Usage: resp.usage(),
	}, nil
}

// messages renders the request as a chat the server's template can apply.
//
// Unlike the hosted adapters, the stage-stable head goes in *one* system
// message rather than two: a GGUF ships whatever chat template its author
// wrote, and plenty of them accept only a single leading system turn. Joining
// System and Prefix costs nothing that matters — both are stage-stable, so
// their concatenation is too, and identical leading bytes across the stage's
// calls is the entire requirement for the KV cache to be reused.
func messages(req model.Request) []chatMessage {
	msgs := make([]chatMessage, 0, 2)
	if head := systemHead(req); head != "" {
		msgs = append(msgs, chatMessage{Role: "system", Content: head})
	}
	return append(msgs, chatMessage{Role: "user", Content: req.Prompt})
}

// systemHead joins the two stage-stable parts of a request in the order every
// provider sends them: System, then Prefix.
func systemHead(req model.Request) string {
	switch {
	case req.System == "":
		return req.Prefix
	case req.Prefix == "":
		return req.System
	default:
		return req.System + "\n\n" + req.Prefix
	}
}

// post sends a JSON request and decodes the JSON reply into out.
func (p *Provider) post(ctx context.Context, call model.CallContext, path string, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return core.Permanent(fmt.Errorf("llamacpp: encoding request: %w", err))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+path, bytes.NewReader(buf))
	if err != nil {
		return core.Permanent(fmt.Errorf("llamacpp: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")
	if err := p.authorize(call, req); err != nil {
		return err
	}
	return p.do(req, out)
}

// authorize attaches the server's API key when one is configured. A server
// without --api-key needs no credential, and the task is granted none.
func (p *Provider) authorize(call model.CallContext, req *http.Request) error {
	if p.secretRef == "" {
		return nil
	}
	if call.ResolveSecret == nil {
		return core.Permanent(fmt.Errorf(
			"llamacpp: %s needs secret %q but the call carries no resolver (at wiring time, pass llamacpp.StaticKey)",
			p.baseURL, p.secretRef))
	}
	key, err := call.ResolveSecret(p.secretRef)
	if err != nil {
		return core.Permanent(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	return nil
}

// do executes a prepared request and decodes a successful reply into out.
func (p *Provider) do(req *http.Request, out any) error {
	resp, err := p.client.Do(req)
	if err != nil {
		// No HTTP response at all: the server is down, still loading its
		// weights, or not where it was said to be. Transient, because the
		// common case of the three is a server that is not up *yet*.
		return core.Transient(fmt.Errorf("llamacpp: %s unreachable (is llama-server running?): %w", p.baseURL, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return classify(resp.StatusCode, p.baseURL, resp.Body)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return core.Transient(fmt.Errorf("llamacpp: decoding response from %s: %w", p.baseURL, err))
	}
	return nil
}

// apiError is llama.cpp's error envelope.
type apiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Type    string `json:"type"`
}

// classify maps an HTTP failure onto Loom's failure taxonomy.
//
// A local server's transient statuses are the interesting ones: 503 is both
// "still loading the model" and "no slot free", and both resolve by waiting,
// which is exactly what the scheduler's backoff does.
func classify(status int, baseURL string, body io.Reader) error {
	msg := errorMessage(body)
	err := fmt.Errorf("llamacpp: %s: HTTP %d%s", baseURL, status, msg)
	switch {
	case status == http.StatusTooManyRequests, status >= 500:
		return core.Transient(err)
	default:
		return core.Permanent(err)
	}
}

// errorMessage extracts the server's explanation, falling back to raw body
// text for a proxy or a build that answers with something else.
func errorMessage(body io.Reader) string {
	raw, err := io.ReadAll(io.LimitReader(body, maxErrorBody))
	if err != nil || len(bytes.TrimSpace(raw)) == 0 {
		return ""
	}
	var env struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err == nil && env.Error.Message != "" {
		return ": " + env.Error.Message
	}
	return ": " + strings.TrimSpace(string(raw))
}

// chatResponse is the reply, including the timings block llama.cpp adds to
// the OpenAI-shaped response.
type chatResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message      chatMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens        int `json:"prompt_tokens"`
		CompletionTokens    int `json:"completion_tokens"`
		PromptTokensDetails struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
	Timings struct {
		PromptN    int `json:"prompt_n"`
		PredictedN int `json:"predicted_n"`
		CacheN     int `json:"cache_n"`
	} `json:"timings"`
}

// usage normalizes the reply into core.Usage's disjoint prompt-token split.
//
// prompt_tokens counts the whole prompt with the reused tokens inside it, so
// the cached count is subtracted out to leave InputTokens holding only what
// the model actually had to process — the same normalization the OpenAI
// adapter performs, and the reason a local run's report reads like a hosted
// one's. Which field carries the reused count depends on the build
// (prompt_tokens_details.cached_tokens on newer ones, the timings block on
// older), so both are read and neither is required: a build reporting no
// reuse simply shows every prompt token as input, which locally costs the
// same nothing either way.
func (r chatResponse) usage() core.Usage {
	cached := r.Usage.PromptTokensDetails.CachedTokens
	if cached == 0 {
		cached = r.Timings.CacheN
	}
	prompt := r.Usage.PromptTokens
	if prompt == 0 {
		// A build without a usage block: timings counts the tokens it had to
		// evaluate, which excludes the ones it reused.
		prompt = r.Timings.PromptN + cached
	}
	if cached > prompt {
		cached = prompt
	}
	output := r.Usage.CompletionTokens
	if output == 0 {
		output = r.Timings.PredictedN
	}
	return core.Usage{
		InputTokens:     prompt - cached,
		OutputTokens:    output,
		CacheReadTokens: cached,
		// A llama.cpp server never writes a cache entry it has to be paid
		// for: the KV cache is a byproduct of the forward pass it was already
		// making, so there is no write to account for and nothing to amortize.
		Requests: 1,
	}
}

// Props is what a llama.cpp server reports about itself.
type Props struct {
	// Model is the model file the server loaded, or the --alias it was given.
	// It is provenance: bindings name the registry's ID, not this.
	Model string
	// Slots is how many sequences the server decodes at once (--parallel),
	// and therefore the ceiling on calls it can have in flight.
	Slots int
	// ContextSize is the context window in tokens available to one slot.
	ContextSize int
}

// StaticKey returns a call context that resolves any secret to key.
//
// It is the wiring-time stand-in for the run's broker. [Provider.Props] and
// [Register] run before any task exists and therefore before any broker does,
// so a server behind --api-key takes its key from the caller for that one
// request — pass the same value the broker will hold during the run. Task
// calls never use this: they are resolved per call under the task's own
// grants, and audited.
func StaticKey(key string) model.CallContext {
	return model.CallContext{
		ResolveSecret: func(security.SecretRef) (string, error) { return key, nil },
	}
}

// Props asks the server what it is running. It is the honest source for the
// two numbers a deployment would otherwise guess — which model is loaded, and
// how much of it can run at once.
//
// A server started with --api-key gates this endpoint like any other, so pass
// [StaticKey] for one; a server needing no credential takes no argument.
func (p *Provider) Props(ctx context.Context, auth ...model.CallContext) (Props, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+"/props", nil)
	if err != nil {
		return Props{}, core.Permanent(fmt.Errorf("llamacpp: %w", err))
	}
	var call model.CallContext
	if len(auth) > 0 {
		call = auth[0]
	}
	if err := p.authorize(call, req); err != nil {
		return Props{}, err
	}

	var body struct {
		TotalSlots                int    `json:"total_slots"`
		ModelPath                 string `json:"model_path"`
		DefaultGenerationSettings struct {
			NCtx int `json:"n_ctx"`
		} `json:"default_generation_settings"`
	}
	if err := p.do(req, &body); err != nil {
		return Props{}, err
	}
	return Props{
		Model:       body.ModelPath,
		Slots:       body.TotalSlots,
		ContextSize: body.DefaultGenerationSettings.NCtx,
	}, nil
}

// Register adds the model a running llama.cpp server has loaded to the
// registry under id and tier, and reports what the server said about itself.
//
// The ceiling comes from the device rather than from a guess: the server's
// slot count becomes [model.Limits.MaxConcurrent], so the scheduler admits
// exactly as many calls as the hardware decodes at once and the rest queue
// where they can be seen. Pricing is left zero, which is the true marginal
// cost of a token you generate yourself — a run against a local model reports
// $0.0000 and means it.
//
// Registering contacts the server, so a server that is not up fails here,
// while the pipeline is still being wired, rather than on the first record.
// A server behind --api-key needs [StaticKey] for that one request; one
// needing no credential takes no argument.
//
//	llamacpp.Register(ctx, reg, p, "local-fast", model.TierFast)
//	llamacpp.Register(ctx, reg, p, "gpu-box", model.TierDeep, llamacpp.StaticKey(key))
func Register(ctx context.Context, reg *model.Registry, p *Provider, id string, tier model.Tier, auth ...model.CallContext) (Props, error) {
	props, err := p.Props(ctx, auth...)
	if err != nil {
		return Props{}, fmt.Errorf("llamacpp: registering %q: %w", id, err)
	}
	slots := props.Slots
	if slots <= 0 {
		// A server that does not report its slots has one.
		slots = 1
	}
	err = reg.Register(model.Info{
		ID:        id,
		Provider:  p,
		Tier:      tier,
		SecretRef: p.secretRef,
		Limits:    model.Limits{MaxConcurrent: slots},
	})
	if err != nil {
		return Props{}, err
	}
	return props, nil
}
