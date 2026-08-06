package openai

import (
	"context"
	"errors"
	"fmt"

	sdk "github.com/openai/openai-go/v3"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/memory"
	"github.com/zionrubin/loom/security"
)

// Embedding model defaults. text-embedding-3-small is the sensible starting
// point for a Loom knowledge base: it is the cheapest first-party option, and
// it supports the `dimensions` parameter, so an index can be narrowed to trade
// a little recall quality for a lot of store size.
const (
	DefaultEmbeddingModel = string(sdk.EmbeddingModelTextEmbedding3Small)
	// EmbeddingPricePerMTok is text-embedding-3-small's list price in USD per
	// million input tokens. It is applied here, not in model.Registry, because
	// the registry prices completions: an embedder is not a Provider and does
	// not belong in a stage's binding ladder.
	EmbeddingPricePerMTok = 0.02
)

// Embedder adapts the OpenAI embeddings endpoint as a memory.Embedder,
// resolving its API key per call through the task's secret broker exactly as
// the completion provider does.
type Embedder struct {
	provider *Provider
	model    string
	dims     int
	price    float64
}

// NewEmbedder returns an embedder over modelID (DefaultEmbeddingModel if
// empty). dims requests a narrowed vector width, supported by
// text-embedding-3 and later; zero uses the model's native width. secretRef
// selects the broker reference for the API key (DefaultSecretRef if empty).
func NewEmbedder(modelID string, dims int, secretRef security.SecretRef) *Embedder {
	if modelID == "" {
		modelID = DefaultEmbeddingModel
	}
	if dims <= 0 {
		// text-embedding-3-small's native width; -large is 3072.
		dims = 1536
		if modelID == string(sdk.EmbeddingModelTextEmbedding3Large) {
			dims = 3072
		}
	}
	return &Embedder{
		provider: New(secretRef),
		model:    modelID,
		dims:     dims,
		price:    EmbeddingPricePerMTok,
	}
}

// WithPrice overrides the per-MTok input price used for cost accounting, for
// a model or contract whose rate differs from the default.
func (e *Embedder) WithPrice(perMTok float64) *Embedder {
	e.price = perMTok
	return e
}

// Name implements memory.Embedder. It carries the requested width because two
// indexes built at different widths are not interchangeable, and the name is
// what recall stages fingerprint.
func (e *Embedder) Name() string { return fmt.Sprintf("openai:%s:%d", e.model, e.dims) }

// Dims implements memory.Embedder.
func (e *Embedder) Dims() int { return e.dims }

// Endpoint implements memory.Embedder.
func (e *Embedder) Endpoint() string { return Endpoint }

// Secret implements memory.Embedder.
func (e *Embedder) Secret() security.SecretRef { return e.provider.secretRef }

// Embed implements memory.Embedder, batching every text into one request.
func (e *Embedder) Embed(ctx context.Context, call memory.Call, texts []string) ([][]float32, core.Usage, error) {
	if len(texts) == 0 {
		return nil, core.Usage{}, nil
	}
	if call.ResolveSecret == nil {
		return nil, core.Usage{}, core.Permanent(errors.New("openai: no secret resolver in call context"))
	}
	key, err := call.ResolveSecret(e.provider.secretRef)
	if err != nil {
		return nil, core.Usage{}, core.Permanent(err)
	}

	params := sdk.EmbeddingNewParams{
		Model: sdk.EmbeddingModel(e.model),
		Input: sdk.EmbeddingNewParamsInputUnion{OfArrayOfStrings: texts},
	}
	if e.dims > 0 && e.model != string(sdk.EmbeddingModelTextEmbeddingAda002) {
		params.Dimensions = sdk.Int(int64(e.dims))
	}

	client := e.provider.client(key)
	resp, err := client.Embeddings.New(ctx, params)
	if err != nil {
		return nil, core.Usage{}, classify(err)
	}
	if len(resp.Data) != len(texts) {
		return nil, core.Usage{}, core.Transient(fmt.Errorf(
			"openai: embedded %d of %d texts", len(resp.Data), len(texts)))
	}

	// The API may return embeddings out of order; Index says where each
	// belongs, and a vector attached to the wrong text would silently poison
	// the index rather than fail.
	out := make([][]float32, len(texts))
	for _, d := range resp.Data {
		if d.Index < 0 || int(d.Index) >= len(out) {
			return nil, core.Usage{}, core.Transient(fmt.Errorf("openai: embedding index %d out of range", d.Index))
		}
		v := make([]float32, len(d.Embedding))
		for i, f := range d.Embedding {
			v[i] = float32(f)
		}
		out[d.Index] = memory.Normalize(v)
	}
	for i, v := range out {
		if v == nil {
			return nil, core.Usage{}, core.Transient(fmt.Errorf("openai: no embedding for text %d", i))
		}
	}

	usage := core.Usage{
		InputTokens: int(resp.Usage.PromptTokens),
		Requests:    1,
		CostUSD:     float64(resp.Usage.PromptTokens) * e.price / 1e6,
	}
	return out, usage, nil
}
