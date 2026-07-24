// Package security implements Loom's least-privilege model: capability
// grants, a secret broker that keeps credentials out of executor hands,
// deny-by-default egress policies, and an append-only audit log.
//
// The design principle: an executor can only do what its task envelope
// explicitly grants. Model access, secrets, tools, and network egress are all
// capabilities resolved at the moment of use and audited.
package security

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Capability names a single permission, e.g. "model:claude-opus-4-8",
// "secret:anthropic_api_key", or "tool:web_fetch".
type Capability string

// ModelCap grants access to a model ID.
func ModelCap(id string) Capability { return Capability("model:" + id) }

// SecretCap grants resolution of a secret reference.
func SecretCap(ref SecretRef) Capability { return Capability("secret:" + string(ref)) }

// ToolCap grants invocation of a tool.
func ToolCap(name string) Capability { return Capability("tool:" + name) }

// GrantSet is an immutable set of capabilities. The zero value grants nothing.
type GrantSet struct {
	caps map[Capability]struct{}
}

// NewGrantSet builds a grant set from capabilities.
func NewGrantSet(caps ...Capability) GrantSet {
	g := GrantSet{caps: make(map[Capability]struct{}, len(caps))}
	for _, c := range caps {
		g.caps[c] = struct{}{}
	}
	return g
}

// With returns a new set extended with additional capabilities.
func (g GrantSet) With(caps ...Capability) GrantSet {
	out := make(map[Capability]struct{}, len(g.caps)+len(caps))
	for c := range g.caps {
		out[c] = struct{}{}
	}
	for _, c := range caps {
		out[c] = struct{}{}
	}
	return GrantSet{caps: out}
}

// Has reports whether the capability is granted.
func (g GrantSet) Has(c Capability) bool {
	_, ok := g.caps[c]
	return ok
}

// List returns the granted capabilities in sorted order.
func (g GrantSet) List() []Capability {
	out := make([]Capability, 0, len(g.caps))
	for c := range g.caps {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// MarshalJSON encodes the set as a sorted array, keeping envelopes
// serializable and deterministic.
func (g GrantSet) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.List())
}

// UnmarshalJSON decodes an array of capabilities.
func (g *GrantSet) UnmarshalJSON(b []byte) error {
	var caps []Capability
	if err := json.Unmarshal(b, &caps); err != nil {
		return err
	}
	*g = NewGrantSet(caps...)
	return nil
}

// SecretRef names a secret without containing it, e.g. "anthropic_api_key".
type SecretRef string

// SecretBroker resolves secret references at the moment of use. Executors and
// ops never hold raw credentials; providers request them per call, and every
// resolution is checked against the task's grants and audited.
type SecretBroker interface {
	Resolve(taskID string, ref SecretRef, grants GrantSet) (string, error)
}

// StaticBroker is an in-memory SecretBroker backed by a fixed secret map.
// Production deployments would substitute a vault-backed implementation.
type StaticBroker struct {
	mu      sync.RWMutex
	secrets map[SecretRef]string
	audit   *AuditLog
}

// NewStaticBroker builds a broker over the given secrets, auditing to log
// (which may be nil).
func NewStaticBroker(secrets map[SecretRef]string, audit *AuditLog) *StaticBroker {
	cp := make(map[SecretRef]string, len(secrets))
	for k, v := range secrets {
		cp[k] = v
	}
	return &StaticBroker{secrets: cp, audit: audit}
}

// Resolve returns the secret if (and only if) the grants include it.
func (b *StaticBroker) Resolve(taskID string, ref SecretRef, grants GrantSet) (string, error) {
	b.mu.RLock()
	val, exists := b.secrets[ref]
	b.mu.RUnlock()

	if !grants.Has(SecretCap(ref)) {
		b.record(taskID, "secret.resolve", string(ref), false, "capability not granted")
		return "", fmt.Errorf("secret %q: capability not granted", ref)
	}
	if !exists {
		b.record(taskID, "secret.resolve", string(ref), false, "secret not configured")
		return "", fmt.Errorf("secret %q: not configured", ref)
	}
	b.record(taskID, "secret.resolve", string(ref), true, "")
	return val, nil
}

func (b *StaticBroker) record(taskID, action, subject string, allowed bool, reason string) {
	if b.audit != nil {
		b.audit.Record(AuditEntry{
			TaskID: taskID, Action: action, Subject: subject,
			Allowed: allowed, Reason: reason,
		})
	}
}

// EgressPolicy is a deny-by-default network allowlist. A task may only reach
// hosts explicitly listed; the empty policy denies all egress.
type EgressPolicy struct {
	Hosts []string `json:"hosts,omitempty"`
}

// Allowed reports whether host is on the allowlist.
func (p EgressPolicy) Allowed(host string) bool {
	for _, h := range p.Hosts {
		if h == host {
			return true
		}
	}
	return false
}

// With returns a policy extended with additional hosts (deduplicated).
func (p EgressPolicy) With(hosts ...string) EgressPolicy {
	seen := make(map[string]struct{}, len(p.Hosts)+len(hosts))
	out := make([]string, 0, len(p.Hosts)+len(hosts))
	for _, h := range append(append([]string{}, p.Hosts...), hosts...) {
		if _, dup := seen[h]; dup || h == "" {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	sort.Strings(out)
	return EgressPolicy{Hosts: out}
}

// AuditEntry is one access decision: who (task), what (action + subject),
// and the outcome.
type AuditEntry struct {
	Time    time.Time `json:"time"`
	TaskID  string    `json:"task_id"`
	Action  string    `json:"action"`
	Subject string    `json:"subject"`
	Allowed bool      `json:"allowed"`
	Reason  string    `json:"reason,omitempty"`
}

// AuditLog is an append-only, concurrency-safe log of access decisions.
type AuditLog struct {
	mu      sync.Mutex
	entries []AuditEntry
}

// Record appends an entry, stamping Time if unset.
func (l *AuditLog) Record(e AuditEntry) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	l.mu.Lock()
	l.entries = append(l.entries, e)
	l.mu.Unlock()
}

// Entries returns a copy of all recorded entries.
func (l *AuditLog) Entries() []AuditEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]AuditEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// Denials returns only the denied entries.
func (l *AuditLog) Denials() []AuditEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []AuditEntry
	for _, e := range l.entries {
		if !e.Allowed {
			out = append(out, e)
		}
	}
	return out
}
