// Package anthropic adapts the official Anthropic Go SDK as a Loom model
// provider.
//
// Credentials are never held by the provider: the API key is resolved
// per call through the task's secret broker, scoped to the task's grants and
// audited. Errors are classified with Loom's failure taxonomy so the
// scheduler retries 429/5xx/529 with backoff and fails fast on 4xx.
package anthropic

import (
	"context"
	"errors"
	"fmt"
	"sync"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/security"
)

// Endpoint is the API host, used for egress policy enforcement.
const Endpoint = "api.anthropic.com"

// DefaultSecretRef is the broker reference for the API key.
const DefaultSecretRef = security.SecretRef("anthropic_api_key")

// Provider implements model.Provider over the official SDK.
type Provider struct {
	secretRef security.SecretRef

	mu      sync.Mutex
	clients map[string]sdk.Client // keyed by API key
}

// New returns a provider resolving its API key from secretRef
// (DefaultSecretRef if empty).
func New(secretRef security.SecretRef) *Provider {
	if secretRef == "" {
		secretRef = DefaultSecretRef
	}
	return &Provider{secretRef: secretRef, clients: map[string]sdk.Client{}}
}

func (p *Provider) Name() string     { return "anthropic" }
func (p *Provider) Endpoint() string { return Endpoint }

func (p *Provider) client(key string) sdk.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.clients[key]
	if !ok {
		c = sdk.NewClient(option.WithAPIKey(key))
		p.clients[key] = c
	}
	return c
}

// Complete implements model.Provider.
func (p *Provider) Complete(ctx context.Context, call model.CallContext, req model.Request) (model.Response, error) {
	if call.ResolveSecret == nil {
		return model.Response{}, core.Permanent(errors.New("anthropic: no secret resolver in call context"))
	}
	key, err := call.ResolveSecret(p.secretRef)
	if err != nil {
		return model.Response{}, core.Permanent(err)
	}

	params := sdk.MessageNewParams{
		Model:     sdk.Model(req.Model),
		MaxTokens: int64(req.MaxTokens),
		Messages:  []sdk.MessageParam{sdk.NewUserMessage(sdk.NewTextBlock(req.Prompt))},
	}
	if req.System != "" {
		params.System = []sdk.TextBlockParam{{Text: req.System}}
	}

	client := p.client(key)
	msg, err := client.Messages.New(ctx, params)
	if err != nil {
		return model.Response{}, classify(err)
	}

	var text string
	for _, block := range msg.Content {
		if block.Type == "text" {
			text += block.AsText().Text
		}
	}
	if msg.StopReason == "refusal" {
		// Safety classifiers declined; retrying identically won't help, but a
		// different (escalation/fallback) model might — semantic failure.
		return model.Response{}, core.Semantic(fmt.Errorf("anthropic: request refused (stop_reason=refusal)"))
	}

	return model.Response{
		Text:  text,
		Model: string(msg.Model),
		Usage: core.Usage{
			InputTokens:  int(msg.Usage.InputTokens),
			OutputTokens: int(msg.Usage.OutputTokens),
			Requests:     1,
		},
	}, nil
}

// classify maps SDK errors onto Loom's failure taxonomy.
func classify(err error) error {
	var apierr *sdk.Error
	if errors.As(err, &apierr) {
		switch {
		case apierr.StatusCode == 429 || apierr.StatusCode >= 500:
			return core.Transient(err)
		default:
			return core.Permanent(err)
		}
	}
	// Network-level failures (no HTTP response) are transient.
	return core.Transient(err)
}

// Model registration defaults (Anthropic first-party API pricing, USD/MTok).
var defaultModels = []struct {
	id      string
	tier    model.Tier
	pricing model.Pricing
}{
	{"claude-opus-4-8", model.TierDeep, model.Pricing{InputPerMTok: 5, OutputPerMTok: 25}},
	{"claude-sonnet-5", model.TierBalanced, model.Pricing{InputPerMTok: 3, OutputPerMTok: 15}},
	{"claude-haiku-4-5", model.TierFast, model.Pricing{InputPerMTok: 1, OutputPerMTok: 5}},
}

// RegisterDefaults registers Claude Opus 4.8 (deep), Claude Sonnet 5
// (balanced), and Claude Haiku 4.5 (fast) with current first-party pricing,
// sharing one provider that resolves DefaultSecretRef. Limits sets
// per-model admission-control ceilings (zero = unlimited).
func RegisterDefaults(reg *model.Registry, limits model.Limits) error {
	p := New("")
	for _, m := range defaultModels {
		err := reg.Register(model.Info{
			ID:        m.id,
			Provider:  p,
			Pricing:   m.pricing,
			Limits:    limits,
			Tier:      m.tier,
			SecretRef: p.secretRef,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
