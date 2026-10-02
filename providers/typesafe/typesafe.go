// Package typesafe adapts TypeSafe's System One API — the Jev decision model —
// as a Loom model provider.
//
// Jev is not a text model. It is handed a state and a set of typed questions
// (yes/no "noul", pick-one "choice", rubric "score") and returns one answer per
// question, drawn from the declared type, with a probability distribution and
// a calibrated confidence. There is no generated text to parse, so there is no
// invalid output to recover from — and output tokens are free, so asking one
// more question about the same state costs almost nothing.
//
// In a pipeline it is the bottom rung of a decision ladder:
//
//	tickets.Decide("triage", pipeline.DecideSpec{
//	    Binding: model.Binding{Model: typesafe.Jev,
//	        Escalation: []string{"claude-sonnet-5"}},   // the System 2 fallback
//	    State: "{{.subject}}\n\n{{.body}}",
//	    Questions: model.Questions{
//	        "queue":   model.Choice("Which team owns this?", queues),
//	        "urgent":  model.Noul("Does this need a human within the hour?"),
//	        "anger":   model.Score("How frustrated is the customer?", "calm", "annoyed", "furious"),
//	    },
//	    MinConfidence: 0.7,   // below this, the record climbs the ladder
//	})
//
// Records Jev is sure about are answered in a few hundred milliseconds for a
// fraction of a cent per thousand; the ones it is not sure about — and it says
// so, in a number that is calibrated rather than typed — go to the LLM, which
// answers the same questions under the same schema.
//
// Credentials are never held by the provider: the API key is resolved per
// call through the task's secret broker, scoped to the task's grants and
// audited. Errors are classified with Loom's failure taxonomy: 429 and 5xx
// retry, other 4xx fail fast, and a response that does not answer the schema
// is semantic and escalates.
package typesafe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/security"
)

const (
	// DefaultBaseURL is TypeSafe's API.
	DefaultBaseURL = "https://api.typesafe.ai"
	// Endpoint is the API host, used for egress policy enforcement.
	Endpoint = "api.typesafe.ai"
	// Path is the System One route every decision is posted to.
	Path = "/v1/systemone"
	// DefaultSecretRef is the broker reference for the API key.
	DefaultSecretRef = security.SecretRef("typesafe_api_key")
	// Jev is the model ID RegisterDefaults registers: TypeSafe's flagship
	// decision model, on its rolling "latest" route.
	Jev = "jev-latest"
	// RequestOverheadTokens is the input System One bills on every call over
	// and above the state and the questions: the prompt Jev wraps them in.
	// Measured against jev-1.13.0 — a one-word state and a one-word noul
	// bill 269 input tokens — and rounded up, because projections use it to
	// bound spend and a bound has to err high.
	RequestOverheadTokens = 270
)

// Pricing is Jev's published rate: $0.042 per million input tokens, with
// output free. Output being free is not a detail — it is why asking every
// question you might need in one call ("speculative fan-out") is the right
// way to use it.
var Pricing = model.Pricing{InputPerMTok: 0.042, OutputPerMTok: 0}

// Provider implements model.Provider over the System One HTTP API.
type Provider struct {
	name      string
	baseURL   string
	host      string
	secretRef security.SecretRef
	client    *http.Client
}

// Option configures a Provider.
type Option func(*Provider)

// WithBaseURL points the provider at another server — a gateway, a proxy, or
// the in-process server in package typesafetest.
func WithBaseURL(u string) Option {
	return func(p *Provider) { p.baseURL = strings.TrimRight(u, "/") }
}

// WithSecretRef resolves the API key from another broker reference.
func WithSecretRef(ref security.SecretRef) Option {
	return func(p *Provider) { p.secretRef = ref }
}

// WithHTTPClient supplies the client used for every request.
func WithHTTPClient(c *http.Client) Option {
	return func(p *Provider) {
		if c != nil {
			p.client = c
		}
	}
}

// WithName sets the provider name reported in telemetry.
func WithName(name string) Option {
	return func(p *Provider) { p.name = name }
}

// New returns a provider against DefaultBaseURL unless an option says
// otherwise.
func New(opts ...Option) *Provider {
	p := &Provider{
		name:      "typesafe",
		baseURL:   DefaultBaseURL,
		secretRef: DefaultSecretRef,
		// Decisions return in well under a second; a call still waiting after
		// thirty is not going to be the fast path it was chosen to be.
		client: &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(p)
	}
	p.host = hostOf(p.baseURL)
	return p
}

func (p *Provider) Name() string     { return p.name }
func (p *Provider) Endpoint() string { return p.host }

// SecretRef returns the secret this provider resolves.
func (p *Provider) SecretRef() security.SecretRef { return p.secretRef }

// Capabilities implements model.Capable: decisions only. Binding a text stage
// (Infer without questions, ReduceAI) to Jev is refused when the pipeline
// compiles, not on the first record.
func (p *Provider) Capabilities() model.Capabilities {
	return model.Capabilities{Decisions: true}
}

// WireQuestion is one question as the System One API takes it. Choice
// options travel as a name → description object and score levels as an
// ordered array, both under "criteria".
type WireQuestion struct {
	Type         model.QuestionType `json:"type"`
	Instructions string             `json:"instructions"`
	Criteria     any                `json:"criteria,omitempty"`
}

// WireRequest is the body of POST /v1/systemone.
type WireRequest struct {
	Model     string                  `json:"model"`
	State     any                     `json:"state"`
	Questions map[string]WireQuestion `json:"questions"`
}

// WireResponse is its reply.
type WireResponse struct {
	Model   string                  `json:"model"`
	Answers map[string]model.Answer `json:"answers"`
	Usage   struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

// ToWire converts Loom questions to the API's shape.
func ToWire(qs model.Questions) map[string]WireQuestion {
	out := make(map[string]WireQuestion, len(qs))
	for id, q := range qs {
		w := WireQuestion{Type: q.Type, Instructions: q.Instructions}
		switch q.Type {
		case model.QuestionChoice:
			w.Criteria = q.Options
		case model.QuestionScore:
			w.Criteria = q.Levels
		}
		out[id] = w
	}
	return out
}

// FromWire converts the API's question shape back to Loom's — what a server
// implementing the protocol needs.
func FromWire(ws map[string]WireQuestion) (model.Questions, error) {
	out := make(model.Questions, len(ws))
	for id, w := range ws {
		q := model.Question{Type: w.Type, Instructions: w.Instructions}
		switch c := w.Criteria.(type) {
		case nil:
		case map[string]any:
			q.Options = make(map[string]string, len(c))
			for k, v := range c {
				s, _ := v.(string)
				q.Options[k] = s
			}
		case []any:
			for _, v := range c {
				s, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("question %q: level is not a string", id)
				}
				q.Levels = append(q.Levels, s)
			}
		default:
			return nil, fmt.Errorf("question %q: criteria must be an object or an array", id)
		}
		out[id] = q
	}
	return out, nil
}

// Complete implements model.Provider. Only decision requests are accepted.
//
// Jev has no system prompt: the state is the whole of what it judges. So the
// state sent is the request's Prefix followed by its Prompt — the stage-wide
// context and then the record — and System, which instructs a generative
// rung how to behave, is not sent.
func (p *Provider) Complete(ctx context.Context, call model.CallContext, req model.Request) (model.Response, error) {
	if len(req.Questions) == 0 {
		return model.Response{}, core.Permanent(errors.New(
			"typesafe: Jev answers typed questions, not free-form prompts (use pipeline.Decide)"))
	}
	if call.ResolveSecret == nil {
		return model.Response{}, core.Permanent(errors.New("typesafe: no secret resolver in call context"))
	}
	key, err := call.ResolveSecret(p.secretRef)
	if err != nil {
		return model.Response{}, core.Permanent(err)
	}

	body, err := json.Marshal(WireRequest{
		Model:     req.Model,
		State:     req.FullPrompt(),
		Questions: ToWire(req.Questions),
	})
	if err != nil {
		return model.Response{}, core.Permanent(err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+Path, bytes.NewReader(body))
	if err != nil {
		return model.Response{}, core.Permanent(err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Authorization", "Bearer "+key)

	hresp, err := p.client.Do(hreq)
	if err != nil {
		// No HTTP response at all: the network, not the request.
		return model.Response{}, core.Transient(fmt.Errorf("typesafe: %w", err))
	}
	defer hresp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(hresp.Body, 4<<20))
	if err != nil {
		return model.Response{}, core.Transient(fmt.Errorf("typesafe: read response: %w", err))
	}
	if hresp.StatusCode != http.StatusOK {
		msg := fmt.Errorf("typesafe: HTTP %d: %s", hresp.StatusCode, strings.TrimSpace(string(raw)))
		if hresp.StatusCode == http.StatusTooManyRequests || hresp.StatusCode >= 500 {
			return model.Response{}, core.Transient(msg)
		}
		return model.Response{}, core.Permanent(msg)
	}

	var wr WireResponse
	if err := json.Unmarshal(raw, &wr); err != nil {
		return model.Response{}, core.Semantic(fmt.Errorf("typesafe: malformed response: %w", err))
	}
	if err := req.Questions.Check(wr.Answers); err != nil {
		return model.Response{}, core.Semantic(fmt.Errorf("typesafe: %w", err))
	}
	text, _ := json.Marshal(wr.Answers)
	served := wr.Model
	if served == "" {
		served = req.Model
	}
	return model.Response{
		Text:    string(text),
		Model:   served,
		Answers: wr.Answers,
		Usage: core.Usage{
			InputTokens:  wr.Usage.InputTokens,
			OutputTokens: wr.Usage.OutputTokens,
			Requests:     1,
		},
	}, nil
}

// Register registers a provider's model under id. It carries no tier on
// purpose: a tier is "whatever model does this kind of work", and a text stage
// asking for TierFast must never be handed a model that cannot write text. A
// decision stage names it by ID.
func Register(reg *model.Registry, p *Provider, id string, limits model.Limits) error {
	return reg.Register(model.Info{
		ID:        id,
		Provider:  p,
		Pricing:   Pricing,
		Limits:    limits,
		SecretRef: p.secretRef,

		InputOverheadTokens: RequestOverheadTokens,
	})
}

// RegisterDefaults registers Jev against TypeSafe's API, resolving
// DefaultSecretRef.
func RegisterDefaults(reg *model.Registry, limits model.Limits) error {
	return Register(reg, New(), Jev, limits)
}

// hostOf extracts the bare host from a base URL, dropping the port the way
// security.EgressPolicy compares hosts.
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
