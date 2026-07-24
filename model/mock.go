package model

import (
	"context"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
)

// Mock is a deterministic in-process Provider for tests, examples, and
// offline pipeline development. It supports scripted failures (to exercise
// retry and escalation paths), simulated latency, and call counting.
type Mock struct {
	name     string
	endpoint string
	latency  time.Duration
	handler  func(Request) (string, error)

	mu       sync.Mutex
	calls    int
	failures []error
}

// MockOption configures a Mock.
type MockOption func(*Mock)

// WithHandler sets the completion function (request → response text).
func WithHandler(fn func(Request) (string, error)) MockOption {
	return func(m *Mock) { m.handler = fn }
}

// WithLatency simulates per-call latency.
func WithLatency(d time.Duration) MockOption {
	return func(m *Mock) { m.latency = d }
}

// WithEndpoint sets a fake network endpoint so egress policies can be
// exercised in tests. Default "" (in-process, always allowed).
func WithEndpoint(host string) MockOption {
	return func(m *Mock) { m.endpoint = host }
}

// WithFailures scripts errors returned by successive calls before the
// handler runs; each call consumes one error until the list is empty.
func WithFailures(errs ...error) MockOption {
	return func(m *Mock) { m.failures = append(m.failures, errs...) }
}

// NewMock builds a mock provider.
func NewMock(name string, opts ...MockOption) *Mock {
	m := &Mock{name: name}
	for _, o := range opts {
		o(m)
	}
	if m.handler == nil {
		m.handler = func(req Request) (string, error) { return "OK", nil }
	}
	return m
}

func (m *Mock) Name() string     { return m.name }
func (m *Mock) Endpoint() string { return m.endpoint }

// Calls returns how many Complete calls have been made (including scripted
// failures).
func (m *Mock) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// Complete implements Provider.
func (m *Mock) Complete(ctx context.Context, call CallContext, req Request) (Response, error) {
	m.mu.Lock()
	m.calls++
	var scripted error
	if len(m.failures) > 0 {
		scripted = m.failures[0]
		m.failures = m.failures[1:]
	}
	m.mu.Unlock()

	if m.latency > 0 {
		select {
		case <-time.After(m.latency):
		case <-ctx.Done():
			return Response{}, core.Transient(ctx.Err())
		}
	}
	if scripted != nil {
		return Response{}, scripted
	}
	text, err := m.handler(req)
	if err != nil {
		return Response{}, err
	}
	return Response{
		Text:  text,
		Model: req.Model,
		Usage: core.Usage{
			InputTokens:  estimateTokens(req.System) + estimateTokens(req.Prompt),
			OutputTokens: estimateTokens(text),
			Requests:     1,
		},
	}, nil
}

// RegisterMock creates a mock provider, registers it under id/tier, and
// returns it. Convenient for tests and examples.
func RegisterMock(reg *Registry, id string, tier Tier, opts ...MockOption) (*Mock, error) {
	m := NewMock(id, opts...)
	err := reg.Register(Info{ID: id, Provider: m, Tier: tier})
	return m, err
}

// estimateTokens is the standard rough heuristic (~4 chars/token) used where
// exact counts are unavailable (mocks, admission-control estimates).
func estimateTokens(s string) int { return len(s)/4 + 1 }

// EstimateTokens is exported for planners estimating admission-control cost.
func EstimateTokens(s string) int { return estimateTokens(s) }
