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
	"github.com/zionrubin/loom/delta"
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
	// Chain references an evolving context held in shared storage: the
	// envelope carries one revision's hash where it would otherwise carry the
	// transcript.
	//
	// It is the same indirection Broadcasts uses, applied to a value that
	// changes. A broadcast is shipped once because every task reads the same
	// bytes; a continuation is shipped as a hash because the next task reads
	// almost the same bytes, and copying a megabyte to express that two
	// kilobytes were added is the cost this removes. What an executor does with
	// it — splice onto state it already holds, or render the whole chain — is
	// its own business and cannot change the result. See package delta.
	//
	// Unlike a broadcast it needs no grant, because it is not reachable by
	// name from inside a prompt: the planner put it here, the executor
	// materializes it, and nothing a model says can widen it.
	Chain delta.Ref `json:"chain,omitzero"`
}

// Envelope declares everything a task may use.
type Envelope struct {
	RunID   string                `json:"run_id"`
	Stage   string                `json:"stage"`
	Binding model.Binding         `json:"binding"`
	Grants  security.GrantSet     `json:"grants"`
	Egress  security.EgressPolicy `json:"egress"`
	Context ContextBundle         `json:"context"`
	// Broadcasts maps each run-level shared value this task may read to the
	// content hash of that value. Broadcasts are referenced, never copied:
	// the envelope stays small however large the shared value is, and an
	// executor fetches each value once from content-addressed storage instead
	// of once per task. Reads additionally require the matching
	// security.DataCap grant.
	Broadcasts map[string]string `json:"broadcasts,omitempty"`
	// MCP maps each Model Context Protocol server this task may reach to the
	// content digest of the tool descriptors the stage was planned against.
	//
	// Connections are named, never carried — the same indirection that makes a
	// broadcast a hash rather than a copy, and for the same reason: a task
	// holding a live socket cannot be shipped anywhere, while a task naming a
	// server can be run by any executor that can reach it. The digest makes the
	// name a contract as well as an address: an executor whose server
	// advertises a different tool set than the plan was compiled against
	// refuses the call rather than invoking a tool nobody planned. Invocation
	// additionally requires the matching security.ToolCap and, for a networked
	// server, its host on the egress allowlist.
	MCP map[string]string `json:"mcp,omitempty"`
	// CachePrefix authorizes the executor to ask the provider to cache this
	// stage's shared prompt prefix. The planner sets it only when the stage
	// issues more than one model call, so a cache entry is never written
	// without a second call to read it.
	CachePrefix bool           `json:"cache_prefix,omitempty"`
	Budget      core.Budget    `json:"budget"`
	Sandbox     SandboxProfile `json:"sandbox"`
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

// Locality is the evolving object this task's context belongs to, or "" when it
// has none.
//
// It is what a queue scores worker affinity on, and it is deliberately derived
// rather than declared. A second field saying where a task would rather run
// could disagree with the context it actually carries, and the failure that
// produced — work steered toward a worker holding state for something else —
// would be invisible in every report.
func (t Task) Locality() string { return t.Envelope.Context.Chain.Key }

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
