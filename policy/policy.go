// Package policy is the authority a pipeline author does not hold.
//
// Loom's envelope already declares everything a task may use — its model
// binding, capability grants, secret references, egress allowlist, sandbox
// profile and budget — and the planner assembles it minimally from what the
// stage asked for. That makes the *author* the security officer: whatever a
// pipeline declares, it gets. In a deployment those are two people. A platform
// team decides which models may see which data, which tools are reachable from
// production, and what a run may spend; an analyst writes the pipeline. Nothing
// in Loom expressed the first of those.
//
// A Policy does, and it needs no new vocabulary to do it. Because the envelope
// is the complete statement of intent, a policy is a predicate over envelopes:
//
//	Check   the loud gate. Every stage's envelope is examined before a single
//	        model call is made, and every violation is reported at once rather
//	        than the first one found — a security review wants the list.
//	Bound   the quiet containment. Every envelope is narrowed to what the
//	        policy permits, so a capability the gate refused cannot reappear in
//	        a task built after it ran.
//
// The two are related by an invariant worth stating: Bound never widens. If
// Check admitted an envelope, Bound returns it unchanged; if it did not, Bound
// is what makes the refusal true of the tasks as well as of the plan. Loom
// applies both, and package policy's own tests assert the invariant directly.
//
// A Policy is plain data with JSON tags, which is the point. It is a
// deployment artifact — reviewed, versioned, and checked into a repository
// beside the infrastructure it governs — rather than a function somebody
// compiled into the program it is supposed to constrain.
package policy

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/task"
)

// Rule is an allow/deny pair over glob patterns, and it is the whole matching
// language: every axis a policy governs is one of these, so learning it once
// is learning all of them.
//
// The semantics are the two an operator expects, in the order they expect:
//
//   - Deny wins. A subject matching any Deny pattern is refused whatever
//     Allow says, so a carve-out cannot be widened by a later broadening.
//   - A non-empty Allow is an allowlist: only subjects matching it pass.
//     An empty Allow leaves the axis unconstrained.
//
// The empty-means-unconstrained default is deliberate. A policy that governs
// models should not have to enumerate every egress host to avoid bricking the
// run, and every allowlist an operator does write is by construction already
// deny-by-default for its own axis. Denying an axis outright is Deny: ["*"].
type Rule struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

// Governs reports whether the rule constrains anything at all.
func (r Rule) Governs() bool { return len(r.Allow) > 0 || len(r.Deny) > 0 }

// Permits reports whether subject passes, and why not when it does not.
func (r Rule) Permits(subject string) (bool, string) {
	for _, pat := range r.Deny {
		if match(pat, subject) {
			return false, fmt.Sprintf("denied by %q", pat)
		}
	}
	if len(r.Allow) == 0 {
		return true, ""
	}
	for _, pat := range r.Allow {
		if match(pat, subject) {
			return true, ""
		}
	}
	return false, "not on the allowlist"
}

// match is glob matching over '*', and nothing else. Unlike path.Match it
// gives no character class its own meaning — a policy names model IDs, hosts
// and tool names, where '/' and '.' are ordinary characters and a separator
// rule would surprise whoever wrote the pattern.
func match(pattern, s string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == s
	}
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	s = s[len(parts[0]):]
	for _, part := range parts[1 : len(parts)-1] {
		i := strings.Index(s, part)
		if i < 0 {
			return false
		}
		s = s[i+len(part):]
	}
	last := parts[len(parts)-1]
	return strings.HasSuffix(s, last) && len(s) >= len(last)
}

// Policy is a deployment's constraint on what the pipelines it runs may do.
// The zero Policy governs nothing, which is what makes adding one to an
// existing deployment a change an operator can make one axis at a time.
type Policy struct {
	// Name identifies the policy in reports and audit entries. It is the
	// string an operator reads when a run is refused, so it should name the
	// document rather than describe it: "prod-eu", "pci-tier1".
	Name string `json:"name,omitempty"`

	// Models constrains the model IDs a stage may bind or escalate to,
	// matched against the IDs the deployment registered. Provider-level rules
	// are written the same way — a deployment that registers its self-hosted
	// models as "local/..." says "nothing leaves the building" with
	// Allow: ["local/*"].
	Models Rule `json:"models,omitzero"`
	// Tools constrains tool invocation, over the same qualified names the
	// planner grants: an MCP tool is "<server>.<tool>".
	Tools Rule `json:"tools,omitzero"`
	// Egress constrains the network hosts a task's allowlist may carry —
	// including the ones the planner derives from a model binding, so a
	// policy that denies a host denies the models reached through it whether
	// or not it also names them.
	Egress Rule `json:"egress,omitzero"`
	// Secrets constrains which secret references a task may resolve.
	Secrets Rule `json:"secrets,omitzero"`
	// Sandbox constrains the isolation profiles a stage may run under.
	// It is a Rule rather than a minimum because "stronger" is not a total
	// order anyone agrees on — a container and a WASM sandbox are differently
	// strong, not comparably so — and a deployment that wants a floor writes
	// the profiles it accepts.
	Sandbox Rule `json:"sandbox,omitzero"`

	// Data is the clearance table: a data class mapped to the rule its
	// models must satisfy. It answers the question a regulated deployment
	// asks first, which is not what a pipeline costs but what it is allowed
	// to show to whom.
	//
	// Classes reach a stage by propagation rather than declaration — a source
	// labelled "pii" taints every stage downstream of it, because that is
	// where the data goes — so labelling the input is the whole of the
	// author's obligation and the planner computes the rest.
	//
	// An empty table does not govern data. A non-empty one governs every
	// class: a stage carrying a class the table does not list is refused
	// rather than waved through, because an unrecognized classification is
	// exactly the case where silence is most expensive.
	Data map[string]Rule `json:"data,omitempty"`

	// MaxCostUSD caps what a run may spend, composing with loom.WithRunBudget
	// the way the shared quota's ceiling does: the run stops at whichever
	// limit it reaches first. A run budget *above* this ceiling is a
	// violation rather than a clamp — a run silently given a tenth of the
	// money it asked for would die at ten percent and report it as budget
	// exhaustion, which tells the operator nothing about why. A run that
	// names no budget inherits this one.
	MaxCostUSD float64 `json:"max_cost_usd,omitempty"`
}

// IsZero reports whether the policy constrains nothing, in which case Loom
// skips admission entirely rather than admitting everything slowly.
func (p Policy) IsZero() bool {
	return !p.Models.Governs() && !p.Tools.Governs() && !p.Egress.Governs() &&
		!p.Secrets.Governs() && !p.Sandbox.Governs() &&
		len(p.Data) == 0 && p.MaxCostUSD == 0
}

// Axis names the kind of thing a violation is about.
type Axis string

const (
	AxisModel   Axis = "model"
	AxisTool    Axis = "tool"
	AxisEgress  Axis = "egress"
	AxisSecret  Axis = "secret"
	AxisSandbox Axis = "sandbox"
	AxisData    Axis = "data"
	AxisBudget  Axis = "budget"
)

// Violation is one thing a plan asked for that the policy does not permit.
type Violation struct {
	// Stage is the stage that asked, or "" for a run-level violation.
	Stage   string `json:"stage,omitempty"`
	Axis    Axis   `json:"axis"`
	Subject string `json:"subject"`
	Reason  string `json:"reason"`
}

func (v Violation) String() string {
	where := "run"
	if v.Stage != "" {
		where = "stage " + quote(v.Stage)
	}
	return fmt.Sprintf("%s: %s %s %s", where, v.Axis, quote(v.Subject), v.Reason)
}

func quote(s string) string { return "\"" + s + "\"" }

// Report is the verdict on a whole plan: the policy that judged it and every
// way it fell short. A report with no violations is an admission.
type Report struct {
	// Policy is the judging policy's name, empty when it had none.
	Policy string `json:"policy,omitempty"`
	// Checked counts the stages examined, so a report can distinguish
	// "nothing was wrong" from "nothing was looked at".
	Checked    int         `json:"checked"`
	Violations []Violation `json:"violations,omitempty"`
}

// Admitted reports whether the plan may run.
func (r Report) Admitted() bool { return len(r.Violations) == 0 }

// Err returns a *Denied carrying this report, or nil when it admits.
// Callers wanting the structure rather than the message use errors.As.
func (r Report) Err() error {
	if r.Admitted() {
		return nil
	}
	return &Denied{Report: r}
}

// Add appends violations, keeping the report deterministically ordered so two
// runs of the same plan produce the same text.
func (r *Report) Add(vs ...Violation) {
	r.Violations = append(r.Violations, vs...)
	sort.SliceStable(r.Violations, func(i, j int) bool {
		a, b := r.Violations[i], r.Violations[j]
		if a.Stage != b.Stage {
			return a.Stage < b.Stage
		}
		if a.Axis != b.Axis {
			return a.Axis < b.Axis
		}
		return a.Subject < b.Subject
	})
}

// String renders the report as an operator reads it: the verdict, then one
// line per violation. It is the whole user experience of a refused run, so it
// says what was asked for and which rule refused it rather than only that
// something was refused.
func (r Report) String() string {
	name := r.Policy
	if name == "" {
		name = "policy"
	}
	if r.Admitted() {
		return fmt.Sprintf("%s: admitted (%d stages checked)", name, r.Checked)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: refused %d of %d stages checked\n",
		name, distinctStages(r.Violations), r.Checked)
	for _, v := range r.Violations {
		fmt.Fprintf(&b, "  - %s\n", v)
	}
	return b.String()
}

func distinctStages(vs []Violation) int {
	seen := map[string]struct{}{}
	for _, v := range vs {
		seen[v.Stage] = struct{}{}
	}
	return len(seen)
}

// Denied is the error a run returns when a policy refuses it, before any model
// call is made. It carries the whole report, so a caller can render the
// violations rather than re-deriving them from a message.
type Denied struct{ Report Report }

func (d *Denied) Error() string { return strings.TrimRight(d.Report.String(), "\n") }

// Check reports every way env violates p.
//
// It reads the envelope and nothing else, which is the property that makes a
// policy enforceable without a new declaration: a task's grants already
// enumerate the models, secrets and tools it may reach, its egress policy the
// hosts, its sandbox the isolation, and its data classes what it is handling.
// Whatever an executor could do with an envelope is visible here.
func (p Policy) Check(env task.Envelope) []Violation {
	var out []Violation
	deny := func(axis Axis, subject, reason string) {
		out = append(out, Violation{Stage: env.Stage, Axis: axis, Subject: subject, Reason: reason})
	}

	models := modelsOf(env.Grants)
	for _, c := range env.Grants.List() {
		axis, subject, ok := classify(c)
		if !ok {
			continue // a capability kind this policy has no rule for
		}
		if allowed, why := p.ruleFor(axis).Permits(subject); !allowed {
			deny(axis, subject, why)
		}
	}
	for _, h := range env.Egress.Hosts {
		if allowed, why := p.Egress.Permits(h); !allowed {
			deny(AxisEgress, h, why)
		}
	}
	if p.Sandbox.Governs() {
		if allowed, why := p.Sandbox.Permits(string(env.Sandbox)); !allowed {
			deny(AxisSandbox, string(env.Sandbox), why)
		}
	}

	// Data clearance is checked against every model the task could reach,
	// not only the one it starts on: a ladder that escalates to a model
	// uncleared for the data is a ladder that leaks it on the second call,
	// and finding that out then is finding it out too late.
	if len(p.Data) > 0 {
		for _, class := range env.DataClasses {
			rule, known := p.Data[class]
			if !known {
				deny(AxisData, class, "no clearance rule for this data class")
				continue
			}
			for _, id := range models {
				if allowed, why := rule.Permits(id); !allowed {
					deny(AxisData, class+" on "+id,
						fmt.Sprintf("model not cleared for %q: %s", class, why))
				}
			}
		}
	}
	return out
}

// CheckRun reports violations of the run-level inputs a plan does not carry:
// the run budget and any egress hosts added for the whole run.
func (p Policy) CheckRun(b core.Budget, egress []string) []Violation {
	var out []Violation
	if p.MaxCostUSD > 0 && b.MaxCostUSD > p.MaxCostUSD {
		out = append(out, Violation{Axis: AxisBudget,
			Subject: fmt.Sprintf("$%.2f", b.MaxCostUSD),
			Reason:  fmt.Sprintf("exceeds the policy ceiling of $%.2f", p.MaxCostUSD)})
	}
	for _, h := range egress {
		if allowed, why := p.Egress.Permits(h); !allowed {
			out = append(out, Violation{Axis: AxisEgress, Subject: h, Reason: why})
		}
	}
	return out
}

// Bound returns env narrowed to what the policy permits.
//
// It narrows the *sets* an envelope carries — grants and egress hosts —
// because those are the fields something downstream of admission can still add
// to: a self-directed loop that discovers a target, a server that begins
// advertising a tool the plan was not compiled against. The scalar fields are
// left alone: a stage's sandbox and binding are fixed when the stage is
// written, so Check is the whole of their enforcement, and quietly rewriting
// either would change what a pipeline does rather than what it may do.
//
// A capability whose kind this policy has no rule for passes through. The
// alternative — strip what you do not recognize — would make every new
// capability kind silently unusable under every policy written before it, and
// Bound is defence in depth behind an explicit gate rather than the gate.
func (p Policy) Bound(env task.Envelope) task.Envelope {
	var kept []security.Capability
	for _, c := range env.Grants.List() {
		axis, subject, ok := classify(c)
		if !ok {
			kept = append(kept, c)
			continue
		}
		if allowed, _ := p.ruleFor(axis).Permits(subject); allowed {
			kept = append(kept, c)
		}
	}
	env.Grants = security.NewGrantSet(kept...)

	var hosts []string
	for _, h := range env.Egress.Hosts {
		if allowed, _ := p.Egress.Permits(h); allowed {
			hosts = append(hosts, h)
		}
	}
	env.Egress = security.EgressPolicy{Hosts: hosts}

	// Budgets compose downward: a stage may ask for less than the ceiling,
	// never more.
	if p.MaxCostUSD > 0 &&
		(env.Budget.MaxCostUSD == 0 || env.Budget.MaxCostUSD > p.MaxCostUSD) {
		env.Budget.MaxCostUSD = p.MaxCostUSD
	}
	return env
}

// BoundBudget returns b capped by the policy's ceiling. A run naming no
// ceiling of its own inherits the policy's, which is what makes a policy a
// spending control rather than only a spending check.
func (p Policy) BoundBudget(b core.Budget) core.Budget {
	if p.MaxCostUSD > 0 && (b.MaxCostUSD == 0 || b.MaxCostUSD > p.MaxCostUSD) {
		b.MaxCostUSD = p.MaxCostUSD
	}
	return b
}

func (p Policy) ruleFor(a Axis) Rule {
	switch a {
	case AxisModel:
		return p.Models
	case AxisTool:
		return p.Tools
	case AxisSecret:
		return p.Secrets
	case AxisEgress:
		return p.Egress
	case AxisSandbox:
		return p.Sandbox
	}
	return Rule{}
}

// classify splits a capability into the axis that governs it and the subject
// it names. It reports false for capability kinds a policy does not govern —
// today, the run-local broadcast reads, which name shared state a run
// registered rather than a resource outside it.
func classify(c security.Capability) (Axis, string, bool) {
	s := string(c)
	switch {
	case strings.HasPrefix(s, "model:"):
		return AxisModel, s[len("model:"):], true
	case strings.HasPrefix(s, "tool:"):
		return AxisTool, s[len("tool:"):], true
	case strings.HasPrefix(s, "secret:"):
		return AxisSecret, s[len("secret:"):], true
	}
	return "", "", false
}

func modelsOf(g security.GrantSet) []string {
	var out []string
	for _, c := range g.List() {
		if axis, id, ok := classify(c); ok && axis == AxisModel {
			out = append(out, id)
		}
	}
	return out
}

// Parse reads a policy from JSON — the form it takes as a deployment artifact,
// reviewed and versioned beside the infrastructure it governs.
func Parse(b []byte) (Policy, error) {
	var p Policy
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields() // a misspelled rule is a rule that does not apply
	if err := dec.Decode(&p); err != nil {
		return Policy{}, fmt.Errorf("policy: %w", err)
	}
	return p, nil
}
