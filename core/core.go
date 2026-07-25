// Package core defines the foundational types shared by every Loom component:
// records, usage accounting, budgets, identifiers, and the failure taxonomy
// that drives retry and escalation decisions.
package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Record is the unit of data flowing through a pipeline. Data holds the
// payload fields; Meta holds framework- or user-attached metadata that ops
// should generally preserve. Values must be JSON-serializable so records can
// cross executor boundaries and be content-addressed.
type Record struct {
	ID   string         `json:"id"`
	Data map[string]any `json:"data"`
	Meta map[string]any `json:"meta,omitempty"`
}

// NewRecord returns a Record with the given ID and data payload.
func NewRecord(id string, data map[string]any) Record {
	if data == nil {
		data = map[string]any{}
	}
	return Record{ID: id, Data: data}
}

// Clone returns a copy of the record with fresh top-level maps, so ops can
// mutate the copy without aliasing upstream state.
func (r Record) Clone() Record {
	d := make(map[string]any, len(r.Data))
	for k, v := range r.Data {
		d[k] = v
	}
	var m map[string]any
	if r.Meta != nil {
		m = make(map[string]any, len(r.Meta))
		for k, v := range r.Meta {
			m[k] = v
		}
	}
	return Record{ID: r.ID, Data: d, Meta: m}
}

// String returns the data field rendered as a string ("" when absent).
func (r Record) String(key string) string {
	v, ok := r.Data[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// NewID returns a random identifier with the given prefix, e.g. "task_9f2c...".
func NewID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

// Usage accumulates token and cost accounting across model calls.
type Usage struct {
	InputTokens  int     `json:"input_tokens"`
	OutputTokens int     `json:"output_tokens"`
	Requests     int     `json:"requests"`
	CostUSD      float64 `json:"cost_usd"`
}

// Add accumulates another usage into u.
func (u *Usage) Add(o Usage) {
	u.InputTokens += o.InputTokens
	u.OutputTokens += o.OutputTokens
	u.Requests += o.Requests
	u.CostUSD += o.CostUSD
}

// TotalTokens returns input + output tokens.
func (u Usage) TotalTokens() int { return u.InputTokens + u.OutputTokens }

// Budget bounds what a run, stage, or task may spend. Zero values mean
// "no limit" for that dimension.
type Budget struct {
	MaxCostUSD  float64       `json:"max_cost_usd,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
	MaxDuration time.Duration `json:"max_duration,omitempty"`
	MaxAttempts int           `json:"max_attempts,omitempty"`
}

// FailureClass partitions errors by the correct recovery strategy. This
// taxonomy is the backbone of Loom's retry logic: AI workloads fail in ways
// classic data systems don't (an op can "succeed" transport-wise yet produce
// unusable output), so recovery must be class-aware.
type FailureClass string

const (
	// FailTransient: infrastructure hiccups (rate limits, 5xx, timeouts).
	// Retry the same work with backoff.
	FailTransient FailureClass = "transient"
	// FailSemantic: the call succeeded but the output is unacceptable
	// (schema violation, failed validation). Retrying identically is often
	// wasteful; Loom escalates along the binding's model ladder.
	FailSemantic FailureClass = "semantic"
	// FailPermanent: retrying cannot help (bad request, missing grant,
	// programming error). Fail fast.
	FailPermanent FailureClass = "permanent"
	// FailBudget: a budget was exhausted. Aborts scheduling of new work.
	FailBudget FailureClass = "budget"
)

// TaskError attaches a FailureClass to an underlying error.
type TaskError struct {
	Class FailureClass
	Err   error
}

func (e *TaskError) Error() string { return fmt.Sprintf("%s: %v", e.Class, e.Err) }
func (e *TaskError) Unwrap() error { return e.Err }

// Transient wraps err as a transient failure.
func Transient(err error) error { return &TaskError{Class: FailTransient, Err: err} }

// Semantic wraps err as a semantic failure.
func Semantic(err error) error { return &TaskError{Class: FailSemantic, Err: err} }

// Permanent wraps err as a permanent failure.
func Permanent(err error) error { return &TaskError{Class: FailPermanent, Err: err} }

// BudgetExceeded wraps err as a budget failure.
func BudgetExceeded(err error) error { return &TaskError{Class: FailBudget, Err: err} }

// ClassOf reports the failure class of err. Deadline expiry is transient;
// unclassified errors are treated as permanent so user-code bugs fail fast
// instead of burning retries.
func ClassOf(err error) FailureClass {
	var te *TaskError
	if errors.As(err, &te) {
		return te.Class
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return FailTransient
	}
	return FailPermanent
}

// Tools is the capability-checked tool invocation surface handed to user ops.
// Every invocation is checked against the task's grants and audited.
type Tools interface {
	Invoke(ctx context.Context, tool string, args map[string]any) (any, error)
}

// Broadcaster reads the run-level broadcast values a task is granted.
//
// A broadcast is a value registered once for the whole run — a lookup table, a
// taxonomy, a rubric — stored once by content hash and *referenced* by every
// task envelope that declares it. That reference, rather than a copy, is what
// makes a broadcast shareable across tasks and across executors: the envelope
// stays small however large the value is, and a remote worker fetches each
// value once instead of receiving it with every task.
//
// The returned value is shared by every task in the run and must be treated as
// read-only. Use BroadcastAs for a typed, independently-owned copy.
type Broadcaster interface {
	Broadcast(ctx context.Context, name string) (any, error)
}

// Session is the capability-scoped surface handed to Go-function ops: tool
// invocation plus broadcast reads. Every access is checked against the task's
// envelope and audited.
type Session interface {
	Tools
	Broadcaster
}

// BroadcastAs reads a broadcast and converts it to T. Broadcast values make a
// JSON round trip on the way into the store, so a value registered as
// map[string]string arrives back as map[string]any; BroadcastAs re-decodes it
// into the type the op actually wants, yielding a copy the op owns.
func BroadcastAs[T any](ctx context.Context, b Broadcaster, name string) (T, error) {
	var zero T
	v, err := b.Broadcast(ctx, name)
	if err != nil {
		return zero, err
	}
	if typed, ok := v.(T); ok {
		return typed, nil
	}
	blob, err := json.Marshal(v)
	if err != nil {
		return zero, Permanent(fmt.Errorf("broadcast %q: %w", name, err))
	}
	var out T
	if err := json.Unmarshal(blob, &out); err != nil {
		return zero, Permanent(fmt.Errorf("broadcast %q: cannot decode as %T: %w", name, out, err))
	}
	return out, nil
}
