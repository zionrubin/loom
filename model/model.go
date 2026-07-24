// Package model defines Loom's model-provider abstraction: providers,
// pricing, rate limits, tiers, the registry, and bindings — the declaration
// of what model capability a stage needs, including its escalation ladder for
// semantic retries.
package model

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/security"
)

// Request is a single completion request to a provider.
type Request struct {
	Model     string `json:"model"`
	System    string `json:"system,omitempty"`
	Prompt    string `json:"prompt"`
	MaxTokens int    `json:"max_tokens"`
}

// Response is a provider's completion result. CostUSD on Usage is filled in
// by the framework from the registry's pricing, not by providers.
type Response struct {
	Text  string
	Model string
	Usage core.Usage
}

// CallContext carries per-task security context into a provider call.
// ResolveSecret is pre-scoped to the calling task's grants: the provider can
// obtain exactly the credentials that task was granted, nothing else, and
// every resolution is audited.
type CallContext struct {
	TaskID        string
	ResolveSecret func(ref security.SecretRef) (string, error)
}

// Provider executes completion requests against one backend (an API, a local
// model, or a mock). Providers classify their own errors with the core
// failure taxonomy (core.Transient / core.Permanent) so the scheduler can
// pick the right recovery.
type Provider interface {
	Name() string
	// Endpoint is the network host the provider contacts, or "" for
	// in-process providers. Used to enforce the task's egress policy.
	Endpoint() string
	Complete(ctx context.Context, call CallContext, req Request) (Response, error)
}

// Pricing is per-million-token cost.
type Pricing struct {
	InputPerMTok  float64
	OutputPerMTok float64
}

// Cost computes the dollar cost of a usage under this pricing.
func (p Pricing) Cost(u core.Usage) float64 {
	return float64(u.InputTokens)*p.InputPerMTok/1e6 + float64(u.OutputTokens)*p.OutputPerMTok/1e6
}

// Limits describes provider-enforced throughput ceilings, consumed by the
// scheduler's admission control. Zero values mean unlimited.
type Limits struct {
	RequestsPerMinute int
	TokensPerMinute   int
}

// Tier is a capability class, letting stages declare "what kind of model"
// instead of hard-coding IDs. The registry maps tiers to concrete models.
type Tier string

const (
	TierFast     Tier = "fast"     // cheap, high-throughput
	TierBalanced Tier = "balanced" // default quality/cost tradeoff
	TierDeep     Tier = "deep"     // maximum capability
)

// Info is a registered model: provider, economics, limits, and the secret it
// requires (if any) so planners can auto-grant least-privilege envelopes.
type Info struct {
	ID        string
	Provider  Provider
	Pricing   Pricing
	Limits    Limits
	Tier      Tier
	SecretRef security.SecretRef
}

// Registry holds all models a deployment may use and the tier mapping.
type Registry struct {
	mu     sync.RWMutex
	models map[string]Info
	tiers  map[Tier]string
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{models: map[string]Info{}, tiers: map[Tier]string{}}
}

// Register adds a model. The first model registered for a tier becomes that
// tier's default (override with SetTier).
func (r *Registry) Register(info Info) error {
	if info.ID == "" {
		return errors.New("model: empty ID")
	}
	if info.Provider == nil {
		return fmt.Errorf("model %q: nil provider", info.ID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.models[info.ID]; dup {
		return fmt.Errorf("model %q: already registered", info.ID)
	}
	r.models[info.ID] = info
	if info.Tier != "" {
		if _, ok := r.tiers[info.Tier]; !ok {
			r.tiers[info.Tier] = info.ID
		}
	}
	return nil
}

// SetTier maps a tier to a registered model ID.
func (r *Registry) SetTier(t Tier, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.models[id]; !ok {
		return fmt.Errorf("model %q: not registered", id)
	}
	r.tiers[t] = id
	return nil
}

// Get returns a model by ID.
func (r *Registry) Get(id string) (Info, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.models[id]
	if !ok {
		return Info{}, fmt.Errorf("model %q: not registered", id)
	}
	return info, nil
}

// ForTier returns the model mapped to a tier.
func (r *Registry) ForTier(t Tier) (Info, error) {
	r.mu.RLock()
	id, ok := r.tiers[t]
	r.mu.RUnlock()
	if !ok {
		return Info{}, fmt.Errorf("tier %q: no model registered", t)
	}
	return r.Get(id)
}

// All returns every registered model, sorted by ID.
func (r *Registry) All() []Info {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Info, 0, len(r.models))
	for _, info := range r.models {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Binding declares a stage's model requirement: an explicit model or a tier,
// plus an optional escalation ladder tried in order on semantic failures
// (invalid output → retry on a stronger model).
type Binding struct {
	Model      string   `json:"model,omitempty"`
	Tier       Tier     `json:"tier,omitempty"`
	Escalation []string `json:"escalation,omitempty"`
}

// IsZero reports whether the binding declares no model requirement.
func (b Binding) IsZero() bool { return b.Model == "" && b.Tier == "" }

// Candidates returns the ordered models a binding may use: the base model
// followed by its escalation ladder.
func (r *Registry) Candidates(b Binding) ([]Info, error) {
	var base Info
	var err error
	switch {
	case b.Model != "":
		base, err = r.Get(b.Model)
	case b.Tier != "":
		base, err = r.ForTier(b.Tier)
	default:
		return nil, errors.New("binding declares neither model nor tier")
	}
	if err != nil {
		return nil, err
	}
	out := []Info{base}
	for _, id := range b.Escalation {
		info, err := r.Get(id)
		if err != nil {
			return nil, fmt.Errorf("escalation: %w", err)
		}
		out = append(out, info)
	}
	return out, nil
}

// Resolve returns the model for the given escalation level, clamped to the
// end of the ladder.
func (r *Registry) Resolve(b Binding, escalation int) (Info, error) {
	c, err := r.Candidates(b)
	if err != nil {
		return Info{}, err
	}
	if escalation < 0 {
		escalation = 0
	}
	if escalation >= len(c) {
		escalation = len(c) - 1
	}
	return c[escalation], nil
}
