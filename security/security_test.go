package security

import (
	"encoding/json"
	"testing"
)

func TestGrantSet(t *testing.T) {
	g := NewGrantSet(ModelCap("m1"), ToolCap("web"))
	if !g.Has(ModelCap("m1")) || !g.Has(ToolCap("web")) {
		t.Fatal("granted capabilities missing")
	}
	if g.Has(ModelCap("m2")) {
		t.Fatal("ungranted capability present")
	}
	g2 := g.With(SecretCap("k"))
	if g.Has(SecretCap("k")) {
		t.Fatal("With mutated the original set")
	}
	if !g2.Has(SecretCap("k")) {
		t.Fatal("With did not add capability")
	}

	// JSON round trip keeps the envelope serializable.
	b, err := json.Marshal(g2)
	if err != nil {
		t.Fatal(err)
	}
	var back GrantSet
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Has(ModelCap("m1")) || !back.Has(SecretCap("k")) {
		t.Fatal("round-tripped set lost capabilities")
	}
}

func TestSecretBroker(t *testing.T) {
	audit := &AuditLog{}
	b := NewStaticBroker(map[SecretRef]string{"api_key": "sk-123"}, audit)

	granted := NewGrantSet(SecretCap("api_key"))
	val, err := b.Resolve("task1", "api_key", granted)
	if err != nil || val != "sk-123" {
		t.Fatalf("granted resolve failed: %v %q", err, val)
	}

	if _, err := b.Resolve("task2", "api_key", NewGrantSet()); err == nil {
		t.Fatal("resolve without grant should fail")
	}
	if _, err := b.Resolve("task3", "missing", NewGrantSet(SecretCap("missing"))); err == nil {
		t.Fatal("resolve of unconfigured secret should fail")
	}

	denials := audit.Denials()
	if len(denials) != 2 {
		t.Fatalf("want 2 audit denials, got %d", len(denials))
	}
	if denials[0].TaskID != "task2" || denials[0].Action != "secret.resolve" {
		t.Errorf("unexpected denial entry: %+v", denials[0])
	}
	if len(audit.Entries()) != 3 {
		t.Errorf("want 3 audit entries total, got %d", len(audit.Entries()))
	}
}

func TestEgressPolicy(t *testing.T) {
	var p EgressPolicy // zero value denies everything
	if p.Allowed("api.anthropic.com") {
		t.Fatal("empty policy must deny")
	}
	p = p.With("api.anthropic.com", "api.anthropic.com", "example.com")
	if !p.Allowed("api.anthropic.com") || !p.Allowed("example.com") {
		t.Fatal("allowed host denied")
	}
	if p.Allowed("evil.com") {
		t.Fatal("unlisted host allowed")
	}
	if len(p.Hosts) != 2 {
		t.Fatalf("expected dedupe, got %v", p.Hosts)
	}
}
