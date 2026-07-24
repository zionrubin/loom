// Package task defines Loom's unit of scheduled work and — centrally — the
// Envelope: the complete, explicit, serializable declaration of everything a
// task is allowed to use. Executors receive an envelope and can reach
// exactly the model, secrets, tools, network hosts, context, and budget it
// grants — nothing else. The envelope is the security and portability
// boundary: because tasks and envelopes are plain serializable data, they
// can be shipped to remote or sandboxed executors unchanged.
package task

import (
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/security"
)

// SandboxProfile selects the isolation level a task's ops run under.
// The local executor implements Inline; the profile taxonomy is the seam for
// stronger isolation backends (subprocess pools, containers, WASM).
type SandboxProfile string

const (
	// SandboxInline runs in the executor's process. Appropriate for trusted
	// first-party ops.
	SandboxInline SandboxProfile = "inline"
	// SandboxSubprocess isolates the op in a child process.
	SandboxSubprocess SandboxProfile = "subprocess"
	// SandboxContainer isolates the op in a container.
	SandboxContainer SandboxProfile = "container"
	// SandboxWASM isolates the op in a WebAssembly sandbox with
	// capability-based imports.
	SandboxWASM SandboxProfile = "wasm"
)

// Fragment is one named context document.
type Fragment struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

// ContextBundle is the exact context a task receives — assembled by the
// planner, not discovered by the executor.
type ContextBundle struct {
	System    string     `json:"system,omitempty"`
	Fragments []Fragment `json:"fragments,omitempty"`
}

// Envelope declares everything a task may use.
type Envelope struct {
	RunID   string                `json:"run_id"`
	Stage   string                `json:"stage"`
	Binding model.Binding         `json:"binding"`
	Grants  security.GrantSet     `json:"grants"`
	Egress  security.EgressPolicy `json:"egress"`
	Context ContextBundle         `json:"context"`
	Budget  core.Budget           `json:"budget"`
	Sandbox SandboxProfile        `json:"sandbox"`
}

// Task is one schedulable unit: a batch of input records plus the envelope
// governing their processing. Attempt/Escalation/ResolvedModel are set by
// the scheduler as recovery progresses.
type Task struct {
	ID          string        `json:"id"`
	Seq         int           `json:"seq"`
	Stage       string        `json:"stage"`
	Fingerprint string        `json:"fingerprint,omitempty"`
	Input       []core.Record `json:"input"`
	Envelope    Envelope      `json:"envelope"`
	CacheKey    string        `json:"cache_key,omitempty"`
	EstTokens   int           `json:"est_tokens,omitempty"`

	Attempt       int    `json:"attempt,omitempty"`
	Escalation    int    `json:"escalation,omitempty"`
	ResolvedModel string `json:"resolved_model,omitempty"`
}

// Result is the outcome of one executed task.
type Result struct {
	TaskID   string        `json:"task_id"`
	Seq      int           `json:"seq"`
	Stage    string        `json:"stage"`
	Output   []core.Record `json:"output"`
	Usage    core.Usage    `json:"usage"`
	Model    string        `json:"model,omitempty"`
	CacheHit bool          `json:"cache_hit,omitempty"`
	Artifact string        `json:"artifact,omitempty"`
	Latency  time.Duration `json:"latency,omitempty"`
}
