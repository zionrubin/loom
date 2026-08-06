package executor

import (
	"context"
	"fmt"
	"sync"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/memory"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/task"
)

// MemoryClient mediates every access to long-term memory, exactly as
// ModelClient does for completions: it checks the capability, checks the
// embedder's endpoint against the envelope's egress allowlist, scopes secret
// resolution to the task's grants, serves reads at the epoch the envelope
// pinned, accounts for the embedding, and publishes telemetry. Ops never reach
// the store directly.
//
// The epoch check is the one that has no analogue on the model side, and it is
// the load-bearing one. Without it a task would read whatever the knowledge
// base happened to hold when it ran, and a result cached under a deterministic
// key would depend on wall-clock timing.
type MemoryClient struct {
	Store    memory.Store
	Embedder memory.Embedder
	Broker   security.SecretBroker
	Audit    *security.AuditLog
	Bus      *observe.Bus
}

// RecallRequest is one retrieval against one space.
type RecallRequest struct {
	Space    string
	Query    string
	K        int
	Filter   map[string]string
	MinScore float32
}

// Recall retrieves the nearest items to req.Query under the task's envelope.
func (m *MemoryClient) Recall(ctx context.Context, env task.Envelope, taskID string, req RecallRequest) ([]memory.Hit, core.Usage, error) {
	epoch, err := m.authorizeRead(env, taskID, req.Space)
	if err != nil {
		return nil, core.Usage{}, err
	}
	vecs, usage, err := m.embed(ctx, env, taskID, []string{req.Query})
	if err != nil {
		return nil, usage, err
	}
	hits, err := m.Store.Search(ctx, memory.Query{
		Space:    req.Space,
		Vector:   vecs[0],
		K:        req.K,
		AsOf:     epoch,
		Filter:   req.Filter,
		MinScore: req.MinScore,
	})
	if err != nil {
		// A store that cannot answer is infrastructure, not authoring: let the
		// scheduler back off and retry rather than dead-lettering the record.
		return nil, usage, core.Transient(fmt.Errorf("recall %q: %w", req.Space, err))
	}
	if m.Bus != nil {
		m.Bus.Publish(observe.Event{
			Type: observe.MemoryRecalled, RunID: env.RunID, Stage: env.Stage,
			TaskID: taskID, Space: req.Space, Epoch: epoch, Items: len(hits),
			Usage: usage,
		})
	}
	return hits, usage, nil
}

// Remember stages items for the next epoch under the task's envelope. Items
// arrive with their text and metadata set; this fills in the embedding, the
// content-addressed ID, and the provenance that makes the entry auditable.
func (m *MemoryClient) Remember(ctx context.Context, env task.Envelope, taskID string, items []memory.Item) ([]string, core.Usage, error) {
	if len(items) == 0 {
		return nil, core.Usage{}, nil
	}
	if m.Store == nil {
		return nil, core.Usage{}, core.Permanent(fmt.Errorf("memory: no store configured for this run"))
	}
	texts := make([]string, 0, len(items))
	for i := range items {
		if !env.Grants.Has(security.MemoryWriteCap(items[i].Space)) {
			m.audit(taskID, "memory.write", items[i].Space, false, "capability not granted")
			return nil, core.Usage{}, core.Permanent(fmt.Errorf(
				"memory %q: write capability not granted", items[i].Space))
		}
		texts = append(texts, items[i].Text)
	}
	vecs, usage, err := m.embed(ctx, env, taskID, texts)
	if err != nil {
		return nil, usage, err
	}
	for i := range items {
		items[i].Vector = vecs[i]
		items[i].ID = memory.NewItem(items[i].Space, items[i].Text, items[i].Meta).ID
		// Provenance is stamped here rather than by the op, so it cannot be
		// forged by one and cannot be forgotten by another. A knowledge base of
		// model output is only usable later if every entry says which run,
		// stage, task, and model produced it.
		items[i].Source = memory.Source{
			RunID: env.RunID, Stage: env.Stage, Task: taskID,
			Model: items[i].Source.Model, Op: items[i].Source.Op,
		}
	}
	ids, err := m.Store.Upsert(ctx, items)
	if err != nil {
		return nil, usage, core.Transient(fmt.Errorf("remember: %w", err))
	}
	for _, it := range items {
		m.audit(taskID, "memory.write", it.Space, true, "")
	}
	if m.Bus != nil {
		m.Bus.Publish(observe.Event{
			Type: observe.MemoryWritten, RunID: env.RunID, Stage: env.Stage,
			TaskID: taskID, Space: items[0].Space, Items: len(ids), Usage: usage,
		})
	}
	return ids, usage, nil
}

// authorizeRead checks the grant and the envelope's pin, returning the epoch
// reads are served at.
func (m *MemoryClient) authorizeRead(env task.Envelope, taskID, space string) (uint64, error) {
	if m.Store == nil {
		return 0, core.Permanent(fmt.Errorf("memory: no store configured for this run"))
	}
	if !env.Grants.Has(security.MemoryReadCap(space)) {
		m.audit(taskID, "memory.read", space, false, "capability not granted")
		return 0, core.Permanent(fmt.Errorf("memory %q: read capability not granted", space))
	}
	epoch, ok := env.Memory[space]
	if !ok {
		m.audit(taskID, "memory.read", space, false, "not pinned in the task envelope")
		return 0, core.Permanent(fmt.Errorf("memory %q: not pinned in the task envelope", space))
	}
	m.audit(taskID, "memory.read", space, true, "")
	return epoch, nil
}

// embed runs the run's embedder under the task's egress policy and grants.
func (m *MemoryClient) embed(ctx context.Context, env task.Envelope, taskID string, texts []string) ([][]float32, core.Usage, error) {
	if m.Embedder == nil {
		return nil, core.Usage{}, core.Permanent(fmt.Errorf("memory: no embedder configured for this run"))
	}
	if host := m.Embedder.Endpoint(); host != "" && !env.Egress.Allowed(host) {
		m.audit(taskID, "egress", host, false, "host not on egress allowlist")
		return nil, core.Usage{}, core.Permanent(fmt.Errorf("egress to %q denied", host))
	}
	call := memory.Call{
		TaskID: taskID,
		ResolveSecret: func(ref security.SecretRef) (string, error) {
			if m.Broker == nil {
				return "", fmt.Errorf("secret %q: no broker configured", ref)
			}
			return m.Broker.Resolve(taskID, ref, env.Grants)
		},
	}
	vecs, usage, err := m.Embedder.Embed(ctx, call, texts)
	if err != nil {
		return nil, usage, err
	}
	if len(vecs) != len(texts) {
		return nil, usage, core.Transient(fmt.Errorf(
			"embedder %s returned %d vectors for %d texts", m.Embedder.Name(), len(vecs), len(texts)))
	}
	return vecs, usage, nil
}

func (m *MemoryClient) audit(taskID, action, subject string, allowed bool, reason string) {
	if m.Audit != nil {
		m.Audit.Record(security.AuditEntry{
			TaskID: taskID, Action: action, Subject: subject,
			Allowed: allowed, Reason: reason,
		})
	}
}

// BindMemory returns the capability-checked long-term memory surface for one
// task — the core.Memory half of the session handed to Go-function ops.
//
// Usage accumulated through this surface is held on the binding rather than
// returned, because a Map function's signature has no room for it; the runner
// drains it when the task finishes so embeddings a user function paid for
// still reach the governor and the run report.
func BindMemory(client *MemoryClient, env task.Envelope, taskID string) *BoundMemory {
	return &BoundMemory{client: client, env: env, taskID: taskID}
}

// BoundMemory is one task's view of long-term memory.
type BoundMemory struct {
	client *MemoryClient
	env    task.Envelope
	taskID string

	mu    sync.Mutex
	usage core.Usage
}

// Recall implements core.Memory.
func (b *BoundMemory) Recall(ctx context.Context, space, query string, k int) ([]core.MemoryHit, error) {
	if b.client == nil {
		return nil, core.Permanent(fmt.Errorf("memory: no store configured for this run"))
	}
	hits, usage, err := b.client.Recall(ctx, b.env, b.taskID, RecallRequest{
		Space: space, Query: query, K: k,
	})
	b.add(usage)
	if err != nil {
		return nil, err
	}
	out := make([]core.MemoryHit, 0, len(hits))
	for _, h := range hits {
		out = append(out, core.MemoryHit{
			ID: h.Item.ID, Text: h.Item.Text, Score: h.Score, Meta: h.Item.Meta,
		})
	}
	return out, nil
}

// Remember implements core.Memory.
func (b *BoundMemory) Remember(ctx context.Context, space, text string, meta map[string]any) (string, error) {
	if b.client == nil {
		return "", core.Permanent(fmt.Errorf("memory: no store configured for this run"))
	}
	ids, usage, err := b.client.Remember(ctx, b.env, b.taskID,
		[]memory.Item{{Space: space, Text: text, Meta: meta}})
	b.add(usage)
	if err != nil {
		return "", err
	}
	if len(ids) == 0 {
		return "", nil
	}
	return ids[0], nil
}

func (b *BoundMemory) add(u core.Usage) {
	b.mu.Lock()
	b.usage.Add(u)
	b.mu.Unlock()
}

// Usage returns and clears what this task spent on embeddings through the
// session surface.
func (b *BoundMemory) Usage() core.Usage {
	b.mu.Lock()
	defer b.mu.Unlock()
	u := b.usage
	b.usage = core.Usage{}
	return u
}
