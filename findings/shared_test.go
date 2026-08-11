package findings_test

// In-process tests for the properties a multi-process test cannot pin down: how
// many times the backend was touched, and what happens when it is not there.
//
// These use a backend wrapper rather than a second process, because the
// question is not "do two executors share findings" — the multi-process suite
// answers that — but "how often does the gate reach for the network at all",
// which is a question about call counts and only visible from inside.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/findings"
	"github.com/zionrubin/loom/findings/filestore"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
)

// --- backends that can be counted and broken ----------------------------

// counting wraps a backend and records every call that crosses the process
// boundary. It is how "the local path is network-free" becomes a measurement.
type counting struct {
	findings.Backend
	calls atomic.Int64
	log   struct {
		sync.Mutex
		ops []string
	}
}

func (c *counting) note(op string) {
	c.calls.Add(1)
	c.log.Lock()
	c.log.ops = append(c.log.ops, op)
	c.log.Unlock()
}

func (c *counting) ops() []string {
	c.log.Lock()
	defer c.log.Unlock()
	return append([]string(nil), c.log.ops...)
}

func (c *counting) Put(ctx context.Context, e findings.Entry) (findings.Entry, error) {
	c.note("put")
	return c.Backend.Put(ctx, e)
}

func (c *counting) Candidates(ctx context.Context, q findings.CandidateQuery) ([]findings.Entry, error) {
	c.note("candidates")
	return c.Backend.Candidates(ctx, q)
}

func (c *counting) Fetch(ctx context.Context, hashes []string) ([]findings.Entry, error) {
	c.note("fetch")
	return c.Backend.Fetch(ctx, hashes)
}

func (c *counting) Nearest(ctx context.Context, q findings.VectorQuery) ([]findings.VectorMatch, error) {
	c.note("nearest")
	return c.Backend.Nearest(ctx, q)
}

func (c *counting) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (findings.Lease, bool, error) {
	c.note("acquire")
	return c.Backend.Acquire(ctx, key, owner, ttl)
}

func (c *counting) Release(ctx context.Context, l findings.Lease) error {
	c.note("release")
	return c.Backend.Release(ctx, l)
}

func (c *counting) Peek(ctx context.Context, key string) (findings.Lease, bool, error) {
	c.note("peek")
	return c.Backend.Peek(ctx, key)
}

func (c *counting) Cite(ctx context.Context, hash string, d findings.Dependent) error {
	c.note("cite")
	return c.Backend.Cite(ctx, hash, d)
}

// broken is a backend that is there but not working: every call fails, the way
// a database behind a saturated connection pool or a partitioned network fails.
type broken struct {
	findings.Backend
	writes bool // fail only the writes
}

var errDown = errors.New("the commons is unreachable")

func (b broken) Put(context.Context, findings.Entry) (findings.Entry, error) {
	return findings.Entry{}, errDown
}

func (b broken) Candidates(ctx context.Context, q findings.CandidateQuery) ([]findings.Entry, error) {
	if b.writes {
		return b.Backend.Candidates(ctx, q)
	}
	return nil, errDown
}

func (b broken) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (findings.Lease, bool, error) {
	if b.writes {
		return b.Backend.Acquire(ctx, key, owner, ttl)
	}
	return findings.Lease{}, false, errDown
}

func (b broken) Peek(ctx context.Context, key string) (findings.Lease, bool, error) {
	if b.writes {
		return b.Backend.Peek(ctx, key)
	}
	return findings.Lease{}, false, errDown
}

// --- harness ------------------------------------------------------------

type harness struct {
	gate   *findings.Gate
	shared *findings.Shared
	calls  atomic.Int32
}

func newHarness(t *testing.T, backend findings.Backend, cfg findings.SharedConfig, p findings.Policy) *harness {
	t.Helper()
	cas, err := store.NewCAS("")
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	ledger, err := findings.NewLedger(cas, "")
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	t.Cleanup(func() { _ = ledger.Close() })

	cfg.Backend = backend
	if cfg.Executor == "" {
		cfg.Executor = "executor-test"
	}
	shared := findings.NewShared(cfg)
	t.Cleanup(func() { _ = shared.Close() })

	return &harness{gate: findings.NewGate(ledger, p).Share(shared), shared: shared}
}

func (h *harness) fetch() findings.Fetch {
	return func(ctx context.Context, q findings.Question) (findings.Result, error) {
		h.calls.Add(1)
		return findings.Result{
			Text:    "Northwind — revenue $4.2bn.",
			Fields:  map[string]any{"revenue": "$4.2bn"},
			Sources: []findings.Source{{Tool: "search", Host: "api.example.com"}},
			Cost:    core.Usage{Requests: 1, CostUSD: 0.004},
			Latency: 50 * time.Millisecond,
		}, nil
	}
}

func request(text string) findings.Request {
	return findings.Request{
		Question: findings.Question{
			Topic: "company", Text: text, Facets: map[string]string{"co": "northwind"},
		},
		Grants: security.NewGrantSet(security.ToolCap("search")),
		Egress: security.EgressPolicy{}.With("api.example.com"),
		RunID:  "run_a", Stage: "research", TaskID: "task_a",
	}
}

func fileBackend(t *testing.T) findings.Backend {
	t.Helper()
	s, err := filestore.Open(t.TempDir(), filestore.Options{})
	if err != nil {
		t.Fatalf("filestore: %v", err)
	}
	return s
}

// --- the network-free guarantee -----------------------------------------

// The claim the whole ladder rests on: a local hit costs no I/O. If it did, the
// gate could not stand in front of every task — it would be a network call in
// front of the calls it was meant to save.
func TestLocalHitsNeverTouchTheSharedBackend(t *testing.T) {
	ctx := context.Background()
	backend := &counting{Backend: fileBackend(t)}
	h := newHarness(t, backend, findings.SharedConfig{}, findings.Policy{})

	if _, err := h.gate.Research(ctx, request("northwind revenue"), h.fetch()); err != nil {
		t.Fatalf("research: %v", err)
	}
	// The miss path is allowed to reach the backend: it looked, it led, it
	// published.
	if backend.calls.Load() == 0 {
		t.Fatalf("a miss must consult the commons")
	}
	h.shared.Flush(ctx)
	before := backend.calls.Load()

	// Every one of these is an L1 exact hit.
	for i := 0; i < 20; i++ {
		ans, err := h.gate.Research(ctx, request("northwind revenue"), h.fetch())
		if err != nil {
			t.Fatalf("research: %v", err)
		}
		if ans.Origin != findings.OriginExact {
			t.Fatalf("hit %d: origin = %s, want exact", i, ans.Origin)
		}
	}
	h.shared.Flush(ctx)

	// Citations are the one write a hit produces, and they are deliberately
	// off the caller's goroutine — so they may appear here, and nothing else
	// may.
	for _, op := range backend.ops()[before:] {
		if op != "cite" {
			t.Fatalf("a local hit reached the backend for %q; the local path must be network-free "+
				"(all calls after the miss: %v)", op, backend.ops()[before:])
		}
	}
	if h.calls.Load() != 1 {
		t.Fatalf("the source was called %d times, want 1", h.calls.Load())
	}
}

// --- failure behaviour --------------------------------------------------

// A layer whose job is to avoid calls should cost money when it breaks, not
// correctness.
func TestBackendFailureFailsOpenToOrdinaryResearch(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, broken{Backend: fileBackend(t)}, findings.SharedConfig{}, findings.Policy{})

	ans, err := h.gate.Research(ctx, request("northwind revenue"), h.fetch())
	if err != nil {
		t.Fatalf("an unreachable commons must not fail the research: %v", err)
	}
	if ans.Origin != findings.OriginFresh {
		t.Fatalf("origin = %s, want fresh", ans.Origin)
	}
	if ans.Text == "" {
		t.Fatalf("the answer must be the source's answer")
	}
	stats := h.gate.Stats()
	if stats.BackendFailures == 0 {
		t.Fatalf("a failing backend must be counted, not swallowed: %+v", stats)
	}
	if stats.FailedOpen == 0 {
		t.Fatalf("proceeding without the commons must be counted as failing open: %+v", stats)
	}
	if h.calls.Load() != 1 {
		t.Fatalf("the source must have been called, got %d", h.calls.Load())
	}
}

// Strict mode is the opposite trade, and it is opt-in because it converts an
// optimization into a dependency.
func TestStrictModeFailsClosedWhenTheCommonsIsUnreachable(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, broken{Backend: fileBackend(t)},
		findings.SharedConfig{Strict: true}, findings.Policy{})

	_, err := h.gate.Research(ctx, request("northwind revenue"), h.fetch())
	if err == nil {
		t.Fatalf("strict mode must fail rather than research uncoordinated")
	}
	if !errors.Is(err, findings.ErrStrict) {
		t.Fatalf("the error must say the commons was the problem, got %v", err)
	}
	if h.calls.Load() != 0 {
		t.Fatalf("strict mode must not reach the source, called %d times", h.calls.Load())
	}
}

// The rule that outranks both: research that succeeded must not be failed by a
// commons that could not record it.
func TestContributionFailureNeverFailsSuccessfulResearch(t *testing.T) {
	ctx := context.Background()
	// A backend whose reads and leases work and whose writes do not — the
	// asymmetric failure a read replica or a full disk actually produces.
	h := newHarness(t, broken{Backend: fileBackend(t), writes: true},
		findings.SharedConfig{Strict: true}, findings.Policy{})

	ans, err := h.gate.Research(ctx, request("northwind revenue"), h.fetch())
	if err != nil {
		t.Fatalf("a commons that cannot record the answer must not fail it: %v", err)
	}
	if ans.Text == "" {
		t.Fatalf("the answer must survive a failed contribution")
	}
	if h.gate.Stats().BackendFailures == 0 {
		t.Fatalf("the failed contribution must still be counted")
	}
	// And it is in the local ledger, so this executor at least does not repeat
	// itself.
	again, err := h.gate.Research(ctx, request("northwind revenue"), h.fetch())
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if again.Origin != findings.OriginExact {
		t.Fatalf("origin = %s, want exact — a failed publish must not lose the local entry", again.Origin)
	}
}

// --- scope --------------------------------------------------------------

// Capability containment protects the answer; scope protects the question. A
// private topic's findings must not reach a table every executor can read.
func TestPrivateTopicsAreNeverPublished(t *testing.T) {
	ctx := context.Background()
	backend := &counting{Backend: fileBackend(t)}
	h := newHarness(t, backend, findings.SharedConfig{}, findings.Policy{
		Topics: map[string]findings.TopicPolicy{
			"company": {Scope: findings.ScopePrivate},
		},
	})

	if _, err := h.gate.Research(ctx, request("northwind revenue"), h.fetch()); err != nil {
		t.Fatalf("research: %v", err)
	}
	h.shared.Flush(ctx)

	for _, op := range backend.ops() {
		if op == "put" {
			t.Fatalf("a private finding must not be written to the shared commons: %v", backend.ops())
		}
	}
	if h.gate.Stats().Published != 0 {
		t.Fatalf("nothing may be published from a private topic")
	}
}

// --- reporting ----------------------------------------------------------

// A report has to be able to say which reuse was local and which crossed a
// process boundary, because those are different claims about different
// mechanisms.
func TestStatsDistinguishLocalReuseFromCrossExecutorReuse(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	open := func(name string) *harness {
		s, err := filestore.Open(dir, filestore.Options{})
		if err != nil {
			t.Fatalf("filestore: %v", err)
		}
		return newHarness(t, s, findings.SharedConfig{Executor: name}, findings.Policy{})
	}

	// One executor learns it and then reuses it locally.
	a := open("executor-a")
	if _, err := a.gate.Research(ctx, request("northwind revenue"), a.fetch()); err != nil {
		t.Fatalf("research: %v", err)
	}
	if _, err := a.gate.Research(ctx, request("northwind revenue"), a.fetch()); err != nil {
		t.Fatalf("research: %v", err)
	}
	a.shared.Flush(ctx)

	// Another executor — a different gate, a different ledger — is served the
	// same finding out of the commons.
	b := open("executor-b")
	ans, err := b.gate.Research(ctx, request("northwind revenue"), b.fetch())
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if !ans.Origin.Remote() {
		t.Fatalf("origin = %s, want a remote tier", ans.Origin)
	}

	as, bs := a.gate.Stats(), b.gate.Stats()
	if as.LocalReuse() != 1 || as.SharedReuse() != 0 {
		t.Fatalf("the learner's reuse is local: %+v", as)
	}
	if bs.SharedReuse() != 1 || bs.LocalReuse() != 0 {
		t.Fatalf("the second executor's reuse is another executor's research: %+v", bs)
	}
	if bs.Adopted != 1 {
		t.Fatalf("a finding copied from the commons into a local ledger must be counted: %+v", bs)
	}
	if a.calls.Load() != 1 || b.calls.Load() != 0 {
		t.Fatalf("source calls: a=%d b=%d, want 1 and 0", a.calls.Load(), b.calls.Load())
	}

	// And the report says so in words.
	report := bs.String()
	if !strings.Contains(report, "shared") {
		t.Fatalf("the report must distinguish shared reuse:\n%s", report)
	}
	if !strings.Contains(report, "external call(s) another executor had already made") {
		t.Fatalf("the report must say what cross-executor reuse bought:\n%s", report)
	}
}
