// Package executor defines the execution boundary: the Executor interface
// that scheduling talks to, the capability-scoped Runtime handed to ops, the
// broker-mediated ModelClient, the grant-checked ToolSet, and the local
// in-process executor implementation.
//
// The Executor interface is Loom's distribution seam. Tasks are plain
// serializable data (see package task), so alternative implementations —
// subprocess pools, container fleets, remote worker services — plug in here
// without touching planning or scheduling.
package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/task"
)

// Executor executes a single task to completion (or classified failure).
type Executor interface {
	Execute(ctx context.Context, t task.Task) (task.Result, error)
}

// Runtime is the capability-scoped set of facilities available to an op
// while executing one task. Everything it exposes is checked against the
// task's envelope.
type Runtime struct {
	Env    task.Envelope
	TaskID string
	Models *ModelClient
	// Memory is the run's long-term knowledge base, reached under the
	// envelope's grants and at the epoch it pinned. Nil when the run has none.
	Memory  *MemoryClient
	Session core.Session // grant-checked tools, broadcast reads, and memory
}

// OpRunner executes one stage's operation for one task. Implementations live
// in package ops; the executor dispatches by stage name.
type OpRunner interface {
	// Run returns the output records, accumulated usage, and the model used
	// (if any).
	Run(ctx context.Context, rt *Runtime, t task.Task) (out []core.Record, usage core.Usage, modelUsed string, err error)
}

// ModelClient mediates every model call: capability check, egress check,
// secret scoping, pricing, and telemetry. Ops never talk to providers
// directly.
type ModelClient struct {
	Registry *model.Registry
	Broker   security.SecretBroker
	Audit    *security.AuditLog
	Bus      *observe.Bus
}

// Call invokes modelID under the task's envelope. It enforces:
//   - the envelope grants access to this model,
//   - the provider's endpoint is on the envelope's egress allowlist,
//   - secret resolution is scoped to the envelope's grants (and audited).
//
// It also computes dollar cost from registry pricing and publishes a
// model.called event with usage and latency.
func (m *ModelClient) Call(ctx context.Context, env task.Envelope, taskID, modelID string, req model.Request) (model.Response, error) {
	info, err := m.Registry.Get(modelID)
	if err != nil {
		return model.Response{}, core.Permanent(err)
	}
	if !env.Grants.Has(security.ModelCap(modelID)) {
		m.audit(taskID, "model.call", modelID, false, "capability not granted")
		return model.Response{}, core.Permanent(fmt.Errorf("model %q: capability not granted", modelID))
	}
	if host := info.Provider.Endpoint(); host != "" && !env.Egress.Allowed(host) {
		m.audit(taskID, "egress", host, false, "host not on egress allowlist")
		return model.Response{}, core.Permanent(fmt.Errorf("egress to %q denied", host))
	}

	call := model.CallContext{
		TaskID: taskID,
		ResolveSecret: func(ref security.SecretRef) (string, error) {
			return m.Broker.Resolve(taskID, ref, env.Grants)
		},
	}
	req.Model = modelID

	// The full rendered request, for observability consumers (e.g. the
	// constellation view's per-call drill-down).
	rendered := req.FullPrompt()
	if req.System != "" {
		rendered = "[system] " + req.System + "\n\n" + rendered
	}
	rendered = observe.Clip(rendered)

	start := time.Now()
	resp, err := info.Provider.Complete(ctx, call, req)
	latency := time.Since(start)
	if err != nil {
		if m.Bus != nil {
			m.Bus.Publish(observe.Event{
				Type: observe.ModelCalled, RunID: env.RunID, Stage: env.Stage,
				TaskID: taskID, Model: modelID, Latency: latency, Err: err.Error(),
				Prompt: rendered,
			})
		}
		return model.Response{}, err
	}
	if resp.Model == "" {
		resp.Model = modelID
	}
	if resp.Usage.Requests == 0 {
		resp.Usage.Requests = 1
	}
	resp.Usage.CostUSD = info.Pricing.Cost(resp.Usage)
	if m.Bus != nil {
		m.Bus.Publish(observe.Event{
			Type: observe.ModelCalled, RunID: env.RunID, Stage: env.Stage,
			TaskID: taskID, Model: resp.Model, Usage: resp.Usage, Latency: latency,
			Prompt: rendered, Response: observe.Clip(resp.Text),
			// Pricing lives in the registry, so the saving a prefix cache hit
			// produced is computed here, where both usage and rates are known.
			Saved: info.Pricing.Saved(resp.Usage),
		})
	}
	return resp, nil
}

func (m *ModelClient) audit(taskID, action, subject string, allowed bool, reason string) {
	if m.Audit != nil {
		m.Audit.Record(security.AuditEntry{
			TaskID: taskID, Action: action, Subject: subject,
			Allowed: allowed, Reason: reason,
		})
	}
}

// Tool is a named side-effecting capability ops may invoke when granted.
type Tool interface {
	Name() string
	Invoke(ctx context.Context, args map[string]any) (any, error)
}

// FuncTool wraps a function as a Tool.
func FuncTool(name string, fn func(ctx context.Context, args map[string]any) (any, error)) Tool {
	return funcTool{name: name, fn: fn}
}

type funcTool struct {
	name string
	fn   func(ctx context.Context, args map[string]any) (any, error)
}

func (t funcTool) Name() string { return t.name }
func (t funcTool) Invoke(ctx context.Context, args map[string]any) (any, error) {
	return t.fn(ctx, args)
}

// ToolSet is the registry of available tools. Access is granted per task via
// the envelope; BindTools produces the grant-checked view ops receive.
type ToolSet struct {
	tools map[string]Tool
}

// NewToolSet builds a set from tools.
func NewToolSet(tools ...Tool) *ToolSet {
	s := &ToolSet{tools: map[string]Tool{}}
	for _, t := range tools {
		s.tools[t.Name()] = t
	}
	return s
}

// BindTools returns the capability-checked tool surface for one task.
func BindTools(set *ToolSet, env task.Envelope, audit *security.AuditLog, taskID string) core.Tools {
	return boundTools{set: set, env: env, audit: audit, taskID: taskID}
}

type boundTools struct {
	set    *ToolSet
	env    task.Envelope
	audit  *security.AuditLog
	taskID string
}

func (b boundTools) Invoke(ctx context.Context, name string, args map[string]any) (any, error) {
	if !b.env.Grants.Has(security.ToolCap(name)) {
		if b.audit != nil {
			b.audit.Record(security.AuditEntry{
				TaskID: b.taskID, Action: "tool.invoke", Subject: name,
				Allowed: false, Reason: "capability not granted",
			})
		}
		return nil, core.Permanent(fmt.Errorf("tool %q: capability not granted", name))
	}
	var tool Tool
	if b.set != nil {
		tool = b.set.tools[name]
	}
	if tool == nil {
		return nil, core.Permanent(fmt.Errorf("tool %q: not registered", name))
	}
	if b.audit != nil {
		b.audit.Record(security.AuditEntry{
			TaskID: b.taskID, Action: "tool.invoke", Subject: name, Allowed: true,
		})
	}
	return tool.Invoke(ctx, args)
}

// BindBroadcasts returns the capability-checked broadcast surface for one
// task. A read is served only when the envelope both grants the name and
// carries its content hash; the value itself comes from shared
// content-addressed storage, so tasks and executors reference one copy rather
// than each carrying their own.
//
// Allowed reads are audited once per task and name: a broadcast read inside a
// map over a million records would otherwise drown the audit log without
// recording anything the first line didn't already say. Denials are always
// audited.
func BindBroadcasts(shared *store.Broadcasts, env task.Envelope, audit *security.AuditLog, bus *observe.Bus, taskID string) core.Broadcaster {
	return &boundBroadcasts{
		shared: shared, env: env, audit: audit, bus: bus,
		taskID: taskID, seen: map[string]bool{},
	}
}

type boundBroadcasts struct {
	shared *store.Broadcasts
	env    task.Envelope
	audit  *security.AuditLog
	bus    *observe.Bus
	taskID string

	mu   sync.Mutex
	seen map[string]bool
}

func (b *boundBroadcasts) Broadcast(ctx context.Context, name string) (any, error) {
	if !b.env.Grants.Has(security.DataCap(name)) {
		b.record(name, false, "capability not granted")
		return nil, core.Permanent(fmt.Errorf("broadcast %q: capability not granted", name))
	}
	hash, ok := b.env.Broadcasts[name]
	if !ok {
		b.record(name, false, "not present in the task envelope")
		return nil, core.Permanent(fmt.Errorf("broadcast %q: not present in the task envelope", name))
	}
	if b.shared == nil {
		b.record(name, false, "no broadcast store configured")
		return nil, core.Permanent(fmt.Errorf("broadcast %q: no broadcast store configured", name))
	}
	v, err := b.shared.Resolve(hash)
	if err != nil {
		b.record(name, false, err.Error())
		return nil, core.Permanent(fmt.Errorf("broadcast %q: %w", name, err))
	}
	b.recordOnce(name)
	return v, nil
}

// recordOnce audits and publishes the first allowed read of name by this
// task. Once per task and name is the useful granularity for both: it is the
// fact that this task reached this shared value that matters, not how many
// records it then applied it to.
func (b *boundBroadcasts) recordOnce(name string) {
	b.mu.Lock()
	first := !b.seen[name]
	b.seen[name] = true
	b.mu.Unlock()
	if !first {
		return
	}
	b.record(name, true, "")
	if b.bus != nil {
		b.bus.Publish(observe.Event{
			Type: observe.BroadcastRead, RunID: b.env.RunID, Stage: b.env.Stage,
			TaskID: b.taskID, Broadcast: name, Artifact: b.env.Broadcasts[name],
		})
	}
}

func (b *boundBroadcasts) record(name string, allowed bool, reason string) {
	if b.audit != nil {
		b.audit.Record(security.AuditEntry{
			TaskID: b.taskID, Action: "broadcast.read", Subject: name,
			Allowed: allowed, Reason: reason,
		})
	}
}

// session joins the capability-checked surfaces ops are handed.
type session struct {
	core.Tools
	core.Broadcaster
	core.Memory
}

// Local executes tasks in-process with the Inline sandbox profile. It
// implements the cache short-circuit, per-task deadlines from the envelope
// budget, and lineage recording.
type Local struct {
	Runners    map[string]OpRunner
	Client     *ModelClient
	Memory     *MemoryClient
	Tools      *ToolSet
	Broadcasts *store.Broadcasts
	Audit      *security.AuditLog
	Cache      *store.Cache
	Lineage    *store.Lineage
	Bus        *observe.Bus
}

// Execute implements Executor.
func (l *Local) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	start := time.Now()

	if t.Envelope.Sandbox != "" && t.Envelope.Sandbox != task.SandboxInline {
		return task.Result{}, core.Permanent(fmt.Errorf(
			"sandbox profile %q not supported by the local executor", t.Envelope.Sandbox))
	}

	// Cache short-circuit: identical op + identical inputs → replay.
	if t.CacheKey != "" && l.Cache != nil {
		if recs, ok := l.Cache.Get(t.CacheKey); ok {
			if l.Bus != nil {
				l.Bus.Publish(observe.Event{
					Type: observe.CacheHit, RunID: t.Envelope.RunID,
					Stage: t.Stage, TaskID: t.ID,
				})
			}
			return task.Result{
				TaskID: t.ID, Seq: t.Seq, Stage: t.Stage, Output: recs,
				CacheHit: true, Latency: time.Since(start),
			}, nil
		}
	}

	runner, ok := l.Runners[t.Stage]
	if !ok {
		return task.Result{}, core.Permanent(fmt.Errorf("stage %q: no runner registered", t.Stage))
	}

	if d := t.Envelope.Budget.MaxDuration; d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}

	mem := BindMemory(l.Memory, t.Envelope, t.ID)
	rt := &Runtime{
		Env:    t.Envelope,
		TaskID: t.ID,
		Models: l.Client,
		Memory: l.Memory,
		Session: session{
			Tools:       BindTools(l.Tools, t.Envelope, l.Audit, t.ID),
			Broadcaster: BindBroadcasts(l.Broadcasts, t.Envelope, l.Audit, l.Bus, t.ID),
			Memory:      mem,
		},
	}

	out, usage, modelUsed, err := runner.Run(ctx, rt, t)
	if err != nil {
		return task.Result{}, err
	}
	// Embeddings a Go-function op paid for through the session reach the
	// governor and the run report the same way a model call's tokens do.
	usage.Add(mem.Usage())

	artifact := ""
	if t.CacheKey != "" && l.Cache != nil {
		artifact, _ = l.Cache.Put(t.CacheKey, out)
	}
	if l.Lineage != nil {
		l.Lineage.Record(store.LineageEntry{
			Artifact:   artifact,
			RunID:      t.Envelope.RunID,
			Stage:      t.Stage,
			Op:         t.Fingerprint,
			Model:      modelUsed,
			Inputs:     store.RecordHashes(t.Input),
			Broadcasts: t.Envelope.Broadcasts,
		})
	}

	return task.Result{
		TaskID: t.ID, Seq: t.Seq, Stage: t.Stage, Output: out,
		Usage: usage, Model: modelUsed, Artifact: artifact,
		Latency: time.Since(start),
	}, nil
}
