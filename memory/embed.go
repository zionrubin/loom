package memory

import (
	"context"
	"hash/fnv"
	"strings"
	"unicode"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/security"
)

// HashEmbedder is a deterministic, dependency-free, offline embedder: it
// hashes word tokens and character trigrams into a fixed-width vector.
//
// It is the memory package's model.Mock — the thing that makes the whole
// mechanism testable and developable without a network, a key, or a bill. What
// it measures is lexical overlap, not meaning: "car" and "automobile" are
// orthogonal to it. Use it for tests, for local development, and for the
// examples; use a real embedder (providers/openai.NewEmbedder, or any
// Embedder implementation) for anything whose recall quality matters.
//
// Trigrams are included alongside whole words so that morphological variants
// ("deploy", "deployed", "deployment") land near each other, which is enough
// for the mechanism's behaviour to be visible in a test without pretending to
// be semantic.
type HashEmbedder struct {
	// Dim is the vector width (default 256).
	Dim int
}

// NewHashEmbedder returns a HashEmbedder of the given width (0 = 256).
func NewHashEmbedder(dim int) *HashEmbedder {
	if dim <= 0 {
		dim = 256
	}
	return &HashEmbedder{Dim: dim}
}

func (e *HashEmbedder) Name() string     { return "hash-v1" }
func (e *HashEmbedder) Endpoint() string { return "" }

// Secret implements Embedder: an in-process embedder needs no credential.
func (e *HashEmbedder) Secret() security.SecretRef { return "" }

// Dims implements Embedder.
func (e *HashEmbedder) Dims() int {
	if e.Dim <= 0 {
		return 256
	}
	return e.Dim
}

// Embed implements Embedder. It costs nothing and reports no usage, which is
// the truth: no request left the process.
func (e *HashEmbedder) Embed(_ context.Context, _ Call, texts []string) ([][]float32, core.Usage, error) {
	out := make([][]float32, 0, len(texts))
	for _, t := range texts {
		out = append(out, e.vector(t))
	}
	return out, core.Usage{}, nil
}

func (e *HashEmbedder) vector(text string) []float32 {
	d := e.Dims()
	v := make([]float32, d)
	add := func(tok string, w float32) {
		h := fnv.New32a()
		_, _ = h.Write([]byte(tok))
		sum := h.Sum32()
		// The low bit picks a sign so unrelated tokens cancel rather than
		// accumulate, which is what keeps unrelated texts near-orthogonal
		// instead of uniformly similar.
		sign := float32(1)
		if sum&1 == 1 {
			sign = -1
		}
		v[int(sum>>1)%d] += sign * w
	}
	for _, word := range tokenize(text) {
		add("w:"+word, 1)
		r := []rune(word)
		for i := 0; i+3 <= len(r); i++ {
			add("g:"+string(r[i:i+3]), 0.35)
		}
	}
	return Normalize(v)
}

// tokenize lowercases and splits on anything that is not a letter or digit.
func tokenize(s string) []string {
	fields := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := fields[:0]
	for _, f := range fields {
		if len(f) > 1 {
			out = append(out, f)
		}
	}
	return out
}
