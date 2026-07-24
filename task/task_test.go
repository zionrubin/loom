package task

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/zionrubin/brian-ai/loom/core"
	"github.com/zionrubin/brian-ai/loom/model"
	"github.com/zionrubin/brian-ai/loom/security"
)

// TestTaskSerializability proves the distribution seam: a task — including
// its full envelope — survives a JSON round trip unchanged, so it can be
// shipped to remote or sandboxed executors.
func TestTaskSerializability(t *testing.T) {
	orig := Task{
		ID: "task_1", Seq: 3, Stage: "classify", Fingerprint: "fp",
		Input: []core.Record{core.NewRecord("r1", map[string]any{"text": "hello"})},
		Envelope: Envelope{
			RunID: "run_1", Stage: "classify",
			Binding: model.Binding{Model: "small", Escalation: []string{"big"}},
			Grants: security.NewGrantSet(
				security.ModelCap("small"), security.SecretCap("key")),
			Egress:  security.EgressPolicy{Hosts: []string{"api.example.com"}},
			Context: ContextBundle{System: "sys", Fragments: []Fragment{{Name: "doc", Content: "body"}}},
			Budget:  core.Budget{MaxDuration: 5 * time.Second, MaxAttempts: 2},
			Sandbox: SandboxInline,
		},
		CacheKey: "ck", EstTokens: 100,
	}

	b, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	var back Task
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}

	if back.ID != orig.ID || back.Stage != orig.Stage || back.CacheKey != orig.CacheKey {
		t.Errorf("task fields lost: %+v", back)
	}
	if !back.Envelope.Grants.Has(security.ModelCap("small")) ||
		!back.Envelope.Grants.Has(security.SecretCap("key")) {
		t.Error("grants lost in round trip")
	}
	if !back.Envelope.Egress.Allowed("api.example.com") {
		t.Error("egress policy lost in round trip")
	}
	if back.Envelope.Binding.Escalation[0] != "big" {
		t.Error("binding lost in round trip")
	}
	if back.Envelope.Context.Fragments[0].Content != "body" {
		t.Error("context bundle lost in round trip")
	}
	if back.Input[0].String("text") != "hello" {
		t.Error("input records lost in round trip")
	}
}
