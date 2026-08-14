// Package queuetest is the conformance suite every worker.Queue must pass: one
// set of tests, run against the in-memory queue, against a shared directory,
// and against whatever anybody writes next.
//
// It exists because "the queue is replaceable behind a clean interface" is a
// claim, and the only way to hold an interface to a claim like that is to have
// two implementations and one written-down definition of what they both owe.
// Everything worker.Client and worker.Worker assume is a test here rather than
// a paragraph: that a lease is granted to exactly one of two racing workers,
// that an unrenewed lease is redelivered, that a late worker cannot commit
// over its replacement, that a duplicate commit yields the first receipt
// rather than a second one, and that an envelope crosses the queue with every
// grant it had.
//
// A new backend is finished when this passes:
//
//	func TestConformance(t *testing.T) {
//	    queuetest.Run(t, func(t *testing.T, ttl time.Duration) worker.Queue {
//	        return open(t, ttl)
//	    })
//	}
package queuetest

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/task"
	"github.com/zionrubin/loom/worker"
)

// Open builds an empty queue whose maximum lease is ttl. Each call must produce
// a queue with no history, and the implementation is responsible for cleaning
// it up (t.Cleanup is the natural place).
//
// The TTL is a parameter because half of what a queue owes its callers is only
// observable when a lease runs out, and a suite that waited out a production
// default would take minutes to say so.
type Open func(t *testing.T, ttl time.Duration) worker.Queue

// TTL is the lease the suite asks for: long enough that a test doing two
// round trips does not race it, short enough that waiting one out is free.
const TTL = 120 * time.Millisecond

// Run executes the conformance suite against a queue implementation.
func Run(t *testing.T, open Open) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(t *testing.T, q worker.Queue)
	}{
		{"SubmitIsIdempotent", testSubmitIdempotent},
		{"EnvelopeCrossesTheQueueIntact", testEnvelopeIntact},
		{"ClaimIsGrantedToOneWorker", testClaimExclusive},
		{"ClaimRespectsCapabilities", testClaimCapabilities},
		{"ClaimHonorsTheBatchLimit", testClaimBatch},
		{"HeartbeatHoldsTheLease", testHeartbeat},
		{"LeaseExpiryRedelivers", testExpiryRedelivers},
		{"FencedWorkerCannotRenewOrCommit", testFencing},
		{"CommitIsIdempotent", testCommitIdempotent},
		{"AbandonCarriesTheFailureClass", testAbandonClass},
		{"FailedTaskReArmsOnResubmit", testReArm},
		{"DeliveryBudgetStopsAPoisonTask", testDeliveryBudget},
		{"CancelWithdrawsAndFences", testCancel},
		{"AwaitReturnsTheTerminalState", testAwait},
		{"StatsCountDepthsAndEvents", testStats},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := open(t, TTL)
			tc.fn(t, q)
		})
	}
}

// --- the fixtures -------------------------------------------------------

// caps is a worker that can serve the suite's task.
func caps(name string) worker.Capabilities {
	return worker.Capabilities{
		Worker: name, Stages: []string{"summarize"},
		Providers:   []string{"mock-fast", "mock-deep"},
		Tools:       []string{"web_fetch"},
		Sandboxes:   []task.SandboxProfile{task.SandboxInline},
		MCP:         []string{"desk"},
		Concurrency: 4,
	}
}

// fixture is a task with a fully populated envelope — every field that has to
// survive the crossing, so a backend that drops one is caught by
// testEnvelopeIntact rather than by a pipeline months later.
func fixture(id string) task.Task {
	return task.Task{
		ID: id, Seq: 7, Stage: "summarize", Fingerprint: "fp_abc",
		CacheKey: "ck_abc", EstTokens: 1200, Attempt: 2, Escalation: 1,
		ResolvedModel: "mock-deep",
		Input: []core.Record{
			core.NewRecord("r1", map[string]any{"text": "the quick brown fox"}),
			core.NewRecord("r2", map[string]any{"text": "jumps over"}),
		},
		Envelope: task.Envelope{
			RunID: "run_1", Stage: "summarize",
			Binding: model.Binding{Model: "mock-fast", Escalation: []string{"mock-deep"}},
			Grants: security.NewGrantSet(
				security.ModelCap("mock-fast"), security.ModelCap("mock-deep"),
				security.ToolCap("web_fetch"), security.DataCap("rubric"),
				security.SecretCap("mock_key"),
			),
			Egress: security.EgressPolicy{}.With("api.example.com"),
			Context: task.ContextBundle{
				System:    "you summarize",
				Fragments: []task.Fragment{{Name: "style", Content: "be brief"}},
			},
			Broadcasts:  map[string]string{"rubric": "1f2e3d"},
			MCP:         map[string]string{"desk": "digest_9"},
			CachePrefix: true,
			Budget:      core.Budget{MaxCostUSD: 1.5, MaxTokens: 50000, MaxDuration: time.Minute, MaxAttempts: 3},
			Sandbox:     task.SandboxInline,
		},
	}
}

func submit(t *testing.T, q worker.Queue, id string) worker.Submission {
	t.Helper()
	tk := fixture(id)
	s := worker.Submission{Task: tk, Needs: worker.Require(tk)}
	if _, err := q.Submit(context.Background(), s); err != nil {
		t.Fatalf("submit %s: %v", id, err)
	}
	return s
}

func claimOne(t *testing.T, q worker.Queue, name string) worker.Assignment {
	t.Helper()
	got, err := q.Claim(context.Background(), worker.Claim{
		Worker: name, Caps: caps(name), Max: 1, TTL: TTL,
	})
	if err != nil {
		t.Fatalf("claim as %s: %v", name, err)
	}
	if len(got) != 1 {
		t.Fatalf("claim as %s returned %d assignments, want 1", name, len(got))
	}
	return got[0]
}

func receiptFor(a worker.Assignment, output string) worker.Receipt {
	return worker.Receipt{
		TaskID: a.Lease.TaskID, Seq: a.Task.Seq, Stage: a.Task.Stage,
		Worker: a.Lease.Worker, Output: output, Records: 1,
		Usage: core.Usage{InputTokens: 10, OutputTokens: 5, Requests: 1, CostUSD: 0.01},
		Model: "mock-deep",
	}
}

func status(t *testing.T, q worker.Queue, id string) worker.Status {
	t.Helper()
	s, ok, err := q.Status(context.Background(), id)
	if err != nil {
		t.Fatalf("status %s: %v", id, err)
	}
	if !ok {
		t.Fatalf("status %s: the queue has never heard of it", id)
	}
	return s
}

// --- the suite ----------------------------------------------------------

// A client that is unsure whether its submission landed re-submits. Getting a
// second copy of the work would be the expensive answer.
func testSubmitIdempotent(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	s := submit(t, q, "t1")
	if _, err := q.Submit(ctx, s); err != nil {
		t.Fatalf("re-submit: %v", err)
	}

	got, err := q.Claim(ctx, worker.Claim{Worker: "w1", Caps: caps("w1"), Max: 10, TTL: TTL})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("two submissions of one task ID produced %d claimable tasks, want 1", len(got))
	}
}

// The envelope is the security and portability boundary, so the queue owes it
// byte-for-byte fidelity: a grant lost in transit is a task that fails on the
// worker, and a broadcast hash lost is a task that reads nothing.
func testEnvelopeIntact(t *testing.T, q worker.Queue) {
	want := fixture("t1")
	submit(t, q, "t1")
	a := claimOne(t, q, "w1")

	if !reflect.DeepEqual(a.Task, want) {
		wb, _ := json.MarshalIndent(want, "", "  ")
		gb, _ := json.MarshalIndent(a.Task, "", "  ")
		t.Fatalf("the task changed crossing the queue:\nwant %s\ngot  %s", wb, gb)
	}
	// And the same again through a serialization round trip, which is what a
	// queue that is not in this process's memory does to it anyway.
	blob, err := json.Marshal(a.Task)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back task.Task
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(back, want) {
		t.Fatalf("the task did not survive JSON:\nwant %+v\ngot  %+v", want, back)
	}
	if !back.Envelope.Grants.Has(security.ToolCap("web_fetch")) {
		t.Fatal("the tool grant did not survive the crossing")
	}
	if back.Envelope.Broadcasts["rubric"] != "1f2e3d" {
		t.Fatal("the broadcast hash did not survive the crossing")
	}
}

// Two workers, one task: exactly one lease. This is the property everything
// else rests on, and the one a queue that merely "hands out work" gets wrong.
func testClaimExclusive(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	submit(t, q, "t1")

	first, err := q.Claim(ctx, worker.Claim{Worker: "w1", Caps: caps("w1"), Max: 5, TTL: TTL})
	if err != nil {
		t.Fatalf("claim w1: %v", err)
	}
	second, err := q.Claim(ctx, worker.Claim{Worker: "w2", Caps: caps("w2"), Max: 5, TTL: TTL})
	if err != nil {
		t.Fatalf("claim w2: %v", err)
	}
	if len(first)+len(second) != 1 {
		t.Fatalf("one task leased to %d workers, want 1", len(first)+len(second))
	}
	if first[0].Lease.Token == 0 {
		t.Fatal("a granted lease carries no fencing token")
	}
}

// A worker only claims what it advertises for. The reason is money: a worker
// that takes a task it cannot run fails it after paying to find out.
func testClaimCapabilities(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	submit(t, q, "t1")

	for _, tc := range []struct {
		name string
		caps worker.Capabilities
	}{
		{"no runner for the stage", worker.Capabilities{
			Worker: "w", Stages: []string{"classify"}, Providers: []string{"mock-deep"},
			Tools: []string{"web_fetch"}, MCP: []string{"desk"},
			Sandboxes: []task.SandboxProfile{task.SandboxInline}}},
		{"no provider for the model", worker.Capabilities{
			Worker: "w", Stages: []string{"summarize"}, Providers: []string{"other"},
			Tools: []string{"web_fetch"}, MCP: []string{"desk"},
			Sandboxes: []task.SandboxProfile{task.SandboxInline}}},
		{"tool not registered", worker.Capabilities{
			Worker: "w", Stages: []string{"summarize"}, Providers: []string{"mock-deep"},
			MCP: []string{"desk"}, Sandboxes: []task.SandboxProfile{task.SandboxInline}}},
		{"sandbox not supported", worker.Capabilities{
			Worker: "w", Stages: []string{"summarize"}, Providers: []string{"mock-deep"},
			Tools: []string{"web_fetch"}, MCP: []string{"desk"},
			Sandboxes: []task.SandboxProfile{task.SandboxContainer}}},
		{"no connection to the MCP server", worker.Capabilities{
			Worker: "w", Stages: []string{"summarize"}, Providers: []string{"mock-deep"},
			Tools: []string{"web_fetch"}, Sandboxes: []task.SandboxProfile{task.SandboxInline}}},
	} {
		got, err := q.Claim(ctx, worker.Claim{Worker: "w", Caps: tc.caps, Max: 5, TTL: TTL})
		if err != nil {
			t.Fatalf("%s: claim: %v", tc.name, err)
		}
		if len(got) != 0 {
			t.Fatalf("%s: the task was leased to a worker that cannot run it", tc.name)
		}
	}

	// And the worker that can, does.
	if got := claimOne(t, q, "w1"); got.Lease.Worker != "w1" {
		t.Fatalf("lease granted to %q, want w1", got.Lease.Worker)
	}
}

// A claim takes at most the worker's free slots, so the queue never leases
// work a worker cannot start.
func testClaimBatch(t *testing.T, q worker.Queue) {
	for _, id := range []string{"t1", "t2", "t3", "t4"} {
		submit(t, q, id)
	}
	got, err := q.Claim(context.Background(), worker.Claim{
		Worker: "w1", Caps: caps("w1"), Max: 2, TTL: TTL})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("claim of 2 returned %d", len(got))
	}
}

// Renewal is what lets the TTL be short: a task that legitimately runs longer
// than one keeps its lease, and nobody else may take it.
func testHeartbeat(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	submit(t, q, "t1")
	a := claimOne(t, q, "w1")

	deadline := time.Now().Add(2 * TTL)
	lease := a.Lease
	for time.Now().Before(deadline) {
		time.Sleep(TTL / 4)
		next, still, err := q.Renew(ctx, lease, TTL)
		if err != nil {
			t.Fatalf("renew: %v", err)
		}
		if !still {
			t.Fatal("a lease renewed every quarter-TTL was taken away anyway")
		}
		lease = next
	}

	// Past two TTLs, and still nobody else can have it.
	other, err := q.Claim(ctx, worker.Claim{Worker: "w2", Caps: caps("w2"), Max: 1, TTL: TTL})
	if err != nil {
		t.Fatalf("claim w2: %v", err)
	}
	if len(other) != 0 {
		t.Fatal("a heartbeated task was redelivered to another worker")
	}
}

// The worker-death case. Nothing is renewed, the lease runs out, and the task
// comes back — with a higher delivery count and, crucially, a higher token.
func testExpiryRedelivers(t *testing.T, q worker.Queue) {
	submit(t, q, "t1")
	dead := claimOne(t, q, "w1")

	time.Sleep(TTL + TTL/2)

	if s := status(t, q, "t1"); s.State != worker.StatePending {
		t.Fatalf("an expired lease left the task %s, want pending", s.State)
	}
	live := claimOne(t, q, "w2")
	if live.Lease.Token <= dead.Lease.Token {
		t.Fatalf("takeover token %d does not exceed the dead worker's %d — "+
			"a fencing token that does not increase fences nothing",
			live.Lease.Token, dead.Lease.Token)
	}
	if live.Delivery != 2 {
		t.Fatalf("redelivery reported as delivery %d, want 2", live.Delivery)
	}
	if !reflect.DeepEqual(live.Task, dead.Task) {
		t.Fatal("the redelivered task is not the task that was lost")
	}
}

// The late-result case: a worker that stalled past its expiry wakes up and
// tries to carry on. It may neither renew nor commit, and the worker that
// replaced it is unaffected.
func testFencing(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	submit(t, q, "t1")
	stale := claimOne(t, q, "w1")

	time.Sleep(TTL + TTL/2)
	live := claimOne(t, q, "w2")

	if _, still, err := q.Renew(ctx, stale.Lease, TTL); err != nil {
		t.Fatalf("renew a fenced lease: %v", err)
	} else if still {
		t.Fatal("a fenced worker renewed a lease it no longer holds")
	}

	if _, err := q.Commit(ctx, stale.Lease, receiptFor(stale, "stale-output")); !errors.Is(err, worker.ErrFenced) {
		t.Fatalf("a fenced commit returned %v, want ErrFenced", err)
	}

	got, err := q.Commit(ctx, live.Lease, receiptFor(live, "live-output"))
	if err != nil {
		t.Fatalf("the live worker could not commit: %v", err)
	}
	if got.Output != "live-output" {
		t.Fatalf("committed output %q, want the live worker's", got.Output)
	}
	if s := status(t, q, "t1"); s.Receipt == nil || s.Receipt.Output != "live-output" {
		t.Fatalf("the queue holds %+v, want the live worker's receipt", s.Receipt)
	}
}

// The duplicate-execution case. Two workers both ran the task — because the
// first was slow, not dead — and both commit. The queue holds one receipt, and
// the second worker is told whose.
func testCommitIdempotent(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	submit(t, q, "t1")
	first := claimOne(t, q, "w1")

	winner, err := q.Commit(ctx, first.Lease, receiptFor(first, "output-a"))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The same worker retrying a commit whose acknowledgement it never saw.
	again, err := q.Commit(ctx, first.Lease, receiptFor(first, "output-b"))
	if err != nil {
		t.Fatalf("re-commit: %v", err)
	}
	if again.Output != winner.Output {
		t.Fatalf("a retried commit changed the result to %q", again.Output)
	}

	// And a second worker's late duplicate, under a lease that was never
	// granted for this round: it is answered with the winner's receipt rather
	// than an error, because the outcome is what it needs to know.
	other := worker.Lease{TaskID: "t1", Worker: "w2", Token: first.Lease.Token + 99,
		Expires: time.Now().Add(TTL)}
	dup, err := q.Commit(ctx, other, receiptFor(first, "output-c"))
	if err != nil {
		t.Fatalf("duplicate commit against a finished task: %v", err)
	}
	if dup.Token != winner.Token || dup.Output != winner.Output {
		t.Fatalf("the duplicate was told %+v, want the winning receipt %+v", dup, winner)
	}
	if s := status(t, q, "t1"); s.State != worker.StateDone || s.Receipt.Output != "output-a" {
		t.Fatalf("three commits produced %s/%+v, want one done receipt for output-a",
			s.State, s.Receipt)
	}
}

// An error crossing a process boundary loses its Go type. Carrying the class
// explicitly is what keeps a semantic failure escalating up the model ladder
// instead of dead-lettering as an unclassified one.
func testAbandonClass(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	submit(t, q, "t1")
	a := claimOne(t, q, "w1")

	f := worker.Failed(core.Semantic(errors.New("the model refused")), "w1")
	if _, err := q.Abandon(ctx, a.Lease, f); err != nil {
		t.Fatalf("abandon: %v", err)
	}
	s := status(t, q, "t1")
	if s.State != worker.StateFailed || s.Failure == nil {
		t.Fatalf("abandoned task is %s with failure %+v", s.State, s.Failure)
	}
	if s.Failure.Class != core.FailSemantic {
		t.Fatalf("failure class %q crossed the queue as %q", core.FailSemantic, s.Failure.Class)
	}
	if got := core.ClassOf(s.Failure.Err()); got != core.FailSemantic {
		t.Fatalf("rebuilt error classified %q, want semantic", got)
	}
}

// The scheduler owns recovery, so its retry has to reach the fleet: the same
// task ID, re-submitted after a failure, runs again — against the envelope it
// now carries, which may have escalated.
func testReArm(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	submit(t, q, "t1")
	a := claimOne(t, q, "w1")
	if _, err := q.Abandon(ctx, a.Lease, worker.Failed(core.Semantic(errors.New("nope")), "w1")); err != nil {
		t.Fatalf("abandon: %v", err)
	}

	escalated := fixture("t1")
	escalated.Escalation, escalated.ResolvedModel, escalated.Attempt = 2, "mock-deep", 3
	if _, err := q.Submit(ctx, worker.Submission{Task: escalated, Needs: worker.Require(escalated)}); err != nil {
		t.Fatalf("re-submit: %v", err)
	}
	if s := status(t, q, "t1"); s.State != worker.StatePending {
		t.Fatalf("re-submitting a failed task left it %s, want pending", s.State)
	}
	got := claimOne(t, q, "w2")
	if got.Task.Attempt != 3 || got.Task.Escalation != 2 {
		t.Fatalf("the re-armed task runs attempt %d/escalation %d, want the new submission's 3/2",
			got.Task.Attempt, got.Task.Escalation)
	}
}

// Redelivery cannot be unbounded: a task that kills every worker that touches
// it has to be declared the problem, or it keeps the fleet busy forever.
func testDeliveryBudget(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	tk := fixture("t1")
	if _, err := q.Submit(ctx, worker.Submission{
		Task: tk, Needs: worker.Require(tk), Deliveries: 2}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	for i := 1; i <= 2; i++ {
		if got, err := q.Claim(ctx, worker.Claim{Worker: "w1", Caps: caps("w1"), Max: 1, TTL: TTL}); err != nil {
			t.Fatalf("claim %d: %v", i, err)
		} else if len(got) != 1 {
			t.Fatalf("delivery %d was refused before the budget was spent", i)
		}
		time.Sleep(TTL + TTL/2)
	}

	s := status(t, q, "t1")
	if s.State != worker.StateFailed {
		t.Fatalf("a task delivered twice with a budget of 2 is %s, want failed", s.State)
	}
	if s.Failure == nil || s.Failure.Class != core.FailTransient {
		t.Fatalf("delivery exhaustion reported as %+v, want a transient failure", s.Failure)
	}
	if got, err := q.Claim(ctx, worker.Claim{Worker: "w2", Caps: caps("w2"), Max: 1, TTL: TTL}); err != nil {
		t.Fatalf("claim: %v", err)
	} else if len(got) != 0 {
		t.Fatal("a task past its delivery budget was handed out again")
	}
}

// Cancellation is how a client stops the fleet spending on work nobody is
// waiting for. The holder learns through the same mechanism a takeover uses.
func testCancel(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	submit(t, q, "t1")
	a := claimOne(t, q, "w1")

	if err := q.Cancel(ctx, "t1", "the run was abandoned"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, still, err := q.Renew(ctx, a.Lease, TTL); err != nil {
		t.Fatalf("renew after cancel: %v", err)
	} else if still {
		t.Fatal("the holder of a cancelled task kept its lease")
	}
	if _, err := q.Commit(ctx, a.Lease, receiptFor(a, "too-late")); !errors.Is(err, worker.ErrFenced) {
		t.Fatalf("committing a cancelled task returned %v, want ErrFenced", err)
	}
	s := status(t, q, "t1")
	if s.State != worker.StateFailed || s.Failure == nil {
		t.Fatalf("a cancelled task is %s", s.State)
	}
}

// Await is the client's whole protocol: block, and be told the outcome.
func testAwait(t *testing.T, q worker.Queue) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	submit(t, q, "t1")

	done := make(chan worker.Status, 1)
	go func() {
		s, err := q.Await(ctx, "t1")
		if err != nil {
			t.Errorf("await: %v", err)
			close(done)
			return
		}
		done <- s
	}()

	a := claimOne(t, q, "w1")
	time.Sleep(5 * time.Millisecond)
	if _, err := q.Commit(ctx, a.Lease, receiptFor(a, "output")); err != nil {
		t.Fatalf("commit: %v", err)
	}

	select {
	case s, ok := <-done:
		if !ok {
			t.Fatal("await failed")
		}
		if s.State != worker.StateDone || s.Receipt == nil || s.Receipt.Output != "output" {
			t.Fatalf("await returned %s/%+v", s.State, s.Receipt)
		}
	case <-ctx.Done():
		t.Fatal("await did not return after the task was committed")
	}

	// A task that is already terminal returns immediately.
	if s, err := q.Await(ctx, "t1"); err != nil || s.State != worker.StateDone {
		t.Fatalf("await on a finished task: %v / %s", err, s.State)
	}
}

// The accounting is what tells an operator the lease TTL is wrong, so it has
// to count the things that say so.
func testStats(t *testing.T, q worker.Queue) {
	ctx := context.Background()
	submit(t, q, "t1")
	submit(t, q, "t2")

	a := claimOne(t, q, "w1")
	s, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if s.Submitted != 2 || s.Claims != 1 || s.Leased != 1 || s.Pending != 1 {
		t.Fatalf("after two submissions and one claim: %+v", s)
	}

	if _, err := q.Commit(ctx, a.Lease, receiptFor(a, "out")); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// A late duplicate, and a fenced abandon.
	stale := worker.Lease{TaskID: "t1", Worker: "w2", Token: a.Lease.Token + 5, Expires: time.Now().Add(TTL)}
	if _, err := q.Commit(ctx, stale, receiptFor(a, "dup")); err != nil {
		t.Fatalf("duplicate commit: %v", err)
	}

	time.Sleep(TTL + TTL/2) // t2 is still pending, nothing to expire
	s, err = q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if s.Done != 1 || s.Pending != 1 {
		t.Fatalf("depths after one commit: %+v", s)
	}
	if s.Duplicates != 1 {
		t.Fatalf("a duplicate commit was not counted: %+v", s)
	}
}
