// Package openai adapts the official OpenAI Go SDK as a Loom model
// provider.
//
// Credentials are never held by the provider: the API key is resolved
// per call through the task's secret broker, scoped to the task's grants and
// audited. Errors are classified with Loom's failure taxonomy so the
// scheduler retries 429/5xx with backoff and fails fast on 4xx.
package openai

import (
	"context"
	"errors"
	"fmt"
	"sync"

	sdk "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/shared"

	"github.com/zionrubin/brian-ai/loom/core"
	"github.com/zionrubin/brian-ai/loom/model"
	"github.com/zionrubin/brian-ai/loom/security"
)

// Endpoint is the API host, used for egress policy enforcement.
const Endpoint = "api.openai.com"

// DefaultSecretRef is the broker reference for the API key.
const DefaultSecretRef = security.SecretRef("openai_api_key")

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

func (p *Provider) Name() string     { return "openai" }
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
		return model.Response{}, core.Permanent(errors.New("openai: no secret resolver in call context"))
	}
	key, err := call.ResolveSecret(p.secretRef)
	if err != nil {
		return model.Response{}, core.Permanent(err)
	}

	messages := make([]sdk.ChatCompletionMessageParamUnion, 0, 2)
	if req.System != "" {
		messages = append(messages, sdk.SystemMessage(req.System))
	}
	messages = append(messages, sdk.UserMessage(req.Prompt))
	params := sdk.ChatCompletionNewParams{
		Model:    shared.ChatModel(req.Model),
		Messages: messages,
	}
	if req.MaxTokens > 0 {
		// Reasoning-era models only accept max_completion_tokens; the older
		// max_tokens parameter is rejected.
		params.MaxCompletionTokens = sdk.Int(int64(req.MaxTokens))
	}

	client := p.client(key)
	completion, err := client.Chat.Completions.New(ctx, params)
	if err != nil {
		return model.Response{}, classify(err)
	}
	if len(completion.Choices) == 0 {
		return model.Response{}, core.Transient(errors.New("openai: response contained no choices"))
	}

	choice := completion.Choices[0]
	if choice.Message.Refusal != "" || choice.FinishReason == "content_filter" {
		// Safety systems declined; retrying identically won't help, but a
		// different (escalation/fallback) model might — semantic failure.
		return model.Response{}, core.Semantic(fmt.Errorf("openai: request refused (finish_reason=%s)", choice.FinishReason))
	}

	return model.Response{
		Text:  choice.Message.Content,
		Model: completion.Model,
		Usage: core.Usage{
			InputTokens:  int(completion.Usage.PromptTokens),
			OutputTokens: int(completion.Usage.CompletionTokens),
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

// Model registration defaults (OpenAI first-party API pricing, USD/MTok,
// July 2026).
var defaultModels = []struct {
	id      string
	tier    model.Tier
	pricing model.Pricing
}{
	{string(shared.ChatModelGPT5_4), model.TierDeep, model.Pricing{InputPerMTok: 2.50, OutputPerMTok: 15}},
	{string(shared.ChatModelGPT5_4Mini), model.TierBalanced, model.Pricing{InputPerMTok: 0.75, OutputPerMTok: 4.50}},
	{string(shared.ChatModelGPT5_4Nano), model.TierFast, model.Pricing{InputPerMTok: 0.20, OutputPerMTok: 1.25}},
}

// RegisterDefaults registers GPT-5.4 (deep), GPT-5.4 mini (balanced), and
// GPT-5.4 nano (fast) with current first-party pricing, sharing one provider
// that resolves DefaultSecretRef. Limits sets per-model admission-control
// ceilings (zero = unlimited).
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
