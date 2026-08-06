package plan

import (
	"strings"
	"testing"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/security"
)

// memEnv is a run offering the named spaces at the given epochs, with an
// embedder that needs a credential and a host.
func memEnv(epochs map[string]uint64) MemoryEnv {
	return MemoryEnv{
		Epochs:   epochs,
		Embedder: "hash-v1",
		Secret:   security.SecretRef("embed_key"),
		Endpoint: "embeddings.example",
	}
}

// recallPipeline builds recall → infer, plus an unrelated map stage that
// should reach neither the knowledge base nor the embedding API.
func recallPipeline() *pipeline.Pipeline {
	p := pipeline.New("t")
	src := p.FromRecords("src", nil)
	src.Map("unrelated", func(r core.Record) (core.Record, error) { return r, nil })
	src.Recall("history", pipeline.RecallSpec{Space: "kb", Query: "{{.q}}", K: 3}).
		Infer("answer", pipeline.InferSpec{
			Binding: model.Binding{Model: "small"},
			Prompt:  "{{.memory}} {{.q}}",
		})
	return p
}

// TestMemoryEnvelopeLeastPrivilege: only the stage that touches memory gets
// the space, the embedder's credential, and the embedder's host.
func TestMemoryEnvelopeLeastPrivilege(t *testing.T) {
	pl, err := Compile(recallPipeline(), reg(t), WithMemory(memEnv(map[string]uint64{"kb": 7})))
	if err != nil {
		t.Fatal(err)
	}

	recall := pl.ByID["history"].Envelope("run1", nil)
	if !recall.Grants.Has(security.MemoryReadCap("kb")) {
		t.Error("recall stage missing memory:read:kb")
	}
	if recall.Grants.Has(security.MemoryWriteCap("kb")) {
		t.Error("a recall stage must not be granted write access")
	}
	if !recall.Grants.Has(security.SecretCap("embed_key")) {
		t.Error("recall stage missing the embedder's secret")
	}
	if !recall.Egress.Allowed("embeddings.example") {
		t.Errorf("recall stage cannot reach the embedder: %v", recall.Egress.Hosts)
	}
	if got := recall.Memory["kb"]; got != 7 {
		t.Errorf("recall stage pinned epoch %d, want 7", got)
	}

	for _, stage := range []string{"unrelated", "answer"} {
		env := pl.ByID[stage].Envelope("run1", nil)
		if env.Grants.Has(security.MemoryReadCap("kb")) {
			t.Errorf("stage %q was granted memory it never declared", stage)
		}
		if len(env.Memory) != 0 {
			t.Errorf("stage %q carries a memory pin it never declared: %v", stage, env.Memory)
		}
		if env.Grants.Has(security.SecretCap("embed_key")) {
			t.Errorf("stage %q was granted the embedder's secret", stage)
		}
		if env.Egress.Allowed("embeddings.example") {
			t.Errorf("stage %q can reach the embedding API", stage)
		}
	}
}

// TestRememberGrantsWriteNotRead: writing to the knowledge base does not make
// a stage a reader of it.
func TestRememberGrantsWriteNotRead(t *testing.T) {
	p := pipeline.New("t")
	p.FromRecords("src", nil).
		Remember("write", pipeline.RememberSpec{Space: "kb", Text: "{{.text}}"})

	pl, err := Compile(p, reg(t), WithMemory(memEnv(map[string]uint64{"kb": 3})))
	if err != nil {
		t.Fatal(err)
	}
	env := pl.ByID["write"].Envelope("run1", nil)
	if !env.Grants.Has(security.MemoryWriteCap("kb")) {
		t.Error("remember stage missing memory:write:kb")
	}
	if env.Grants.Has(security.MemoryReadCap("kb")) {
		t.Error("a remember stage must not be granted read access")
	}
	if len(env.Memory) != 0 {
		t.Errorf("a write-only stage carries a read pin: %v", env.Memory)
	}
}

// TestEpochInvalidatesOnlyTheReadingStage is the fingerprint half of
// recall-keyed caching: a commit moves the recall stage's key and leaves every
// other stage's alone.
func TestEpochInvalidatesOnlyTheReadingStage(t *testing.T) {
	at := func(epoch uint64) map[string]string {
		t.Helper()
		pl, err := Compile(recallPipeline(), reg(t),
			WithMemory(memEnv(map[string]uint64{"kb": epoch})))
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for _, sp := range pl.Order {
			out[sp.Stage.ID] = sp.Fingerprint
		}
		return out
	}
	before, after := at(1), at(2)

	if before["history"] == after["history"] {
		t.Error("a new epoch left the recall stage's fingerprint unchanged")
	}
	for _, stage := range []string{"unrelated", "answer"} {
		if before[stage] != after[stage] {
			t.Errorf("a new epoch changed stage %q's fingerprint, invalidating work "+
				"that never read the knowledge base", stage)
		}
	}
}

// TestEmbedderIdentityIsInTheFingerprint: the same query under a different
// embedder has different neighbours, so its results are not interchangeable.
func TestEmbedderIdentityIsInTheFingerprint(t *testing.T) {
	with := func(embedder string) string {
		t.Helper()
		env := memEnv(map[string]uint64{"kb": 1})
		env.Embedder = embedder
		pl, err := Compile(recallPipeline(), reg(t), WithMemory(env))
		if err != nil {
			t.Fatal(err)
		}
		return pl.ByID["history"].Fingerprint
	}
	if with("hash-v1") == with("openai:text-embedding-3-small:1536") {
		t.Error("changing the embedder left the recall fingerprint unchanged")
	}
}

// TestMemoryLeavesExistingFingerprintsUntouched: adopting long-term memory
// somewhere in a pipeline must not cold-start the stages that never touch it.
func TestMemoryLeavesExistingFingerprintsUntouched(t *testing.T) {
	p := pipeline.New("t")
	p.FromRecords("src", nil).Infer("answer", pipeline.InferSpec{
		Binding: model.Binding{Model: "small"}, Prompt: "{{.q}}",
	})

	plain, err := Compile(p, reg(t))
	if err != nil {
		t.Fatal(err)
	}
	withMem, err := Compile(p, reg(t), WithMemory(memEnv(map[string]uint64{"kb": 9})))
	if err != nil {
		t.Fatal(err)
	}
	if plain.ByID["answer"].Fingerprint != withMem.ByID["answer"].Fingerprint {
		t.Error("configuring a memory store changed the fingerprint of a stage that " +
			"does not use it")
	}
}

// TestUnavailableSpaceFailsCompilation: a typo, or a space with no store
// behind it, should surface before any money is spent.
func TestUnavailableSpaceFailsCompilation(t *testing.T) {
	p := pipeline.New("t")
	p.FromRecords("src", nil).
		Recall("history", pipeline.RecallSpec{Space: "kbb", Query: "{{.q}}", K: 1})

	_, err := Compile(p, reg(t), WithMemory(memEnv(map[string]uint64{"kb": 1})))
	if err == nil {
		t.Fatal("compiling against an unavailable memory space succeeded")
	}
	if !strings.Contains(err.Error(), "kbb") || !strings.Contains(err.Error(), "WithMemory") {
		t.Errorf("error %q should name the space and how to fix it", err)
	}
}

// TestRecallValidatedAtCompileTime: authoring errors in a retrieval stage are
// caught with the rest of them.
func TestRecallValidatedAtCompileTime(t *testing.T) {
	cases := map[string]pipeline.RecallSpec{
		"memory space":   {Query: "{{.q}}"},
		"query template": {Space: "kb"},
		"template":       {Space: "kb", Query: "{{.q"},
		"filter":         {Space: "kb", Query: "{{.q}}", Filter: map[string]string{"t": "{{.x"}},
	}
	for want, spec := range cases {
		t.Run(want, func(t *testing.T) {
			p := pipeline.New("t")
			p.FromRecords("src", nil).Recall("history", spec)
			_, err := Compile(p, reg(t), WithMemory(memEnv(map[string]uint64{"kb": 1})))
			if err == nil {
				t.Fatalf("invalid recall spec compiled")
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		})
	}
}

// TestRecallWithoutAStoreFailsCompilation: a recall stage needs an embedder,
// and saying so at compile time beats failing on the first record.
func TestRecallWithoutAStoreFailsCompilation(t *testing.T) {
	p := pipeline.New("t")
	p.FromRecords("src", nil).
		Recall("history", pipeline.RecallSpec{Space: "kb", Query: "{{.q}}"})

	// Epochs offered but no embedder — the shape a misconfigured host produces.
	_, err := Compile(p, reg(t), WithMemory(MemoryEnv{Epochs: map[string]uint64{"kb": 1}}))
	if err == nil || !strings.Contains(err.Error(), "loom.WithMemory") {
		t.Fatalf("compiling a recall stage without an embedder gave %v", err)
	}
}
