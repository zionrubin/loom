package worker_test

// The failure tests: what a fleet does when a worker dies, when a lease runs
// out, when a result arrives too late, when the network goes away, and when a
// task ends up running twice.
//
// They are written against the client and the worker rather than against the
// queue — the queue's own obligations are in worker/queuetest, run against both
// backends — because these are the properties a *user* is promised. The claim
// under test is not "the queue refuses a stale token"; it is "killing a worker
// mid-execution neither loses the task nor corrupts the result", and that
// sentence is only true if the client, the worker, the lease and the shared
// store all behave.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/task"
	"github.com/zionrubin/loom/worker"
)

const ttl = 100 * time.Millisecond

// --- harness ------------------------------------------------------------

// fleet is a queue, a shared store, a client, and however many workers a test
// wants — the same wiring loom.WithWorkerService and loom.Serve produce, with
// the pipeline replaced by a scripted executor so a test can hold a worker
// still at the exact moment it matters.
type fleet struct {
	t      *testing.T
	q      *worker.MemQueue
	cas    *store.CAS
	client *worker.Client
}

func newFleet(t *testing.T) *fleet {
	t.Helper()
	cas, err := store.NewCAS("")
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	q := worker.NewMemQueue(worker.MemOptions{LeaseTTL: ttl})
	t.Cleanup(func() { _ = q.Close() })
	return &fleet{
		t: t, q: q, cas: cas,
		client: worker.NewClient(worker.ClientConfig{
			Queue: q, Blobs: cas, Name: "client", Backoff: 5 * time.Millisecond,
		}),
	}
}

// start runs a worker against the fleet until the test ends.
func (f *fleet) start(name string, q worker.Queue, exec *scripted) *worker.Worker {
	f.t.Helper()
	return f.startWith(name, q, exec, wildcard(name))
}

// startWith runs a worker advertising exactly caps.
func (f *fleet) startWith(name string, q worker.Queue, exec *scripted, caps worker.Capabilities) *worker.Worker {
	f.t.Helper()
	w, err := worker.New(worker.Config{
		Queue: q, Blobs: f.cas, Exec: exec, Name: name, Caps: caps,
		LeaseTTL:  ttl,
		Heartbeat: ttl / 4,
		Poll:      2 * time.Millisecond, PollCeiling: 10 * time.Millisecond,
		Commits: 20, Drain: time.Second,
	})
	if err != nil {
		f.t.Fatalf("worker %s: %v", name, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx) }()
	f.t.Cleanup(func() { cancel(); <-done })
	return w
}

// execute runs one task through the client, off the test goroutine.
func (f *fleet) execute(ctx context.Context, t task.Task) <-chan outcome {
	out := make(chan outcome, 1)
	go func() {
		res, err := f.client.Execute(ctx, t)
		out <- outcome{res: res, err: err}
	}()
	return out
}

type outcome struct {
	res task.Result
	err error
}

func (o outcome) mustSucceed(t *testing.T) task.Result {
	t.Helper()
	if o.err != nil {
		t.Fatalf("the task failed: %v", o.err)
	}
	return o.res
}

func await(t *testing.T, ch <-chan outcome, within time.Duration) outcome {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(within):
		t.Fatalf("the task did not finish within %s", within)
		return outcome{}
	}
}

// scripted is an executor a test can hold still, fail, or count.
//
// The default work is deterministic — output derived from input by a pure
// function — which is not a convenience but the point: two workers that both
// execute one task must produce identical bytes, or the claim that duplicate
// execution is harmless is a claim about this test rather than about the
// design.
type scripted struct {
	mu      sync.Mutex
	calls   int
	entered chan string // task IDs, as they begin
	gate    chan struct{}
	err     error
	// cancelled records tasks whose context was cancelled underneath them,
	// which is how a fenced worker is observed to stop paying for its work.
	cancelled map[string]bool
}

func newScripted() *scripted {
	return &scripted{entered: make(chan string, 64), cancelled: map[string]bool{}}
}

// hold makes every execution block until release is called.
func (s *scripted) hold() {
	s.mu.Lock()
	s.gate = make(chan struct{})
	s.mu.Unlock()
}

func (s *scripted) release() {
	s.mu.Lock()
	g := s.gate
	s.gate = nil
	s.mu.Unlock()
	if g != nil {
		close(g)
	}
}

func (s *scripted) Execute(ctx context.Context, t task.Task) (task.Result, error) {
	s.mu.Lock()
	s.calls++
	gate, err := s.gate, s.err
	s.mu.Unlock()

	select {
	case s.entered <- t.ID:
	default:
	}

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			s.mu.Lock()
			s.cancelled[t.ID] = true
			s.mu.Unlock()
			return task.Result{}, core.Transient(ctx.Err())
		}
	}
	if err != nil {
		return task.Result{}, err
	}

	out := make([]core.Record, 0, len(t.Input))
	for _, r := range t.Input {
		c := r.Clone()
		c.Data["summary"] = strings.ToUpper(r.String("text"))
		out = append(out, c)
	}
	return task.Result{
		TaskID: t.ID, Seq: t.Seq, Stage: t.Stage, Output: out,
		Usage: core.Usage{InputTokens: 10, OutputTokens: 4, Requests: 1, CostUSD: 0.002},
		Model: "mock-fast",
	}, nil
}

func (s *scripted) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *scripted) wasCancelled(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelled[id]
}

// severed is a queue handle that has stopped working — the view of the world a
// worker has after its machine is partitioned, its process is paused, or its
// connection is dropped. Cutting the handle rather than the worker is what
// makes a death testable: the worker keeps believing everything is fine, which
// is exactly the belief the lease exists to overrule.
type severed struct {
	worker.Queue
	mu   sync.Mutex
	down bool
}

func (s *severed) cut()  { s.mu.Lock(); s.down = true; s.mu.Unlock() }
func (s *severed) heal() { s.mu.Lock(); s.down = false; s.mu.Unlock() }
func (s *severed) broken() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.down
}

var errNetwork = errors.New("connection refused")

func (s *severed) Claim(ctx context.Context, c worker.Claim) ([]worker.Assignment, error) {
	if s.broken() {
		return nil, errNetwork
	}
	return s.Queue.Claim(ctx, c)
}

func (s *severed) Renew(ctx context.Context, l worker.Lease, ttl time.Duration) (worker.Lease, bool, error) {
	if s.broken() {
		return worker.Lease{}, false, errNetwork
	}
	return s.Queue.Renew(ctx, l, ttl)
}

func (s *severed) Commit(ctx context.Context, l worker.Lease, r worker.Receipt) (worker.Receipt, error) {
	if s.broken() {
		return worker.Receipt{}, errNetwork
	}
	return s.Queue.Commit(ctx, l, r)
}

func (s *severed) Abandon(ctx context.Context, l worker.Lease, f worker.Failure) (worker.Status, error) {
	if s.broken() {
		return worker.Status{}, errNetwork
	}
	return s.Queue.Abandon(ctx, l, f)
}

func (s *severed) Submit(ctx context.Context, sub worker.Submission) (worker.Status, error) {
	if s.broken() {
		return worker.Status{}, errNetwork
	}
	return s.Queue.Submit(ctx, sub)
}

func (s *severed) Await(ctx context.Context, id string) (worker.Status, error) {
	if s.broken() {
		return worker.Status{}, errNetwork
	}
	return s.Queue.Await(ctx, id)
}

func job(id string, text string) task.Task {
	return task.Task{
		ID: id, Seq: 1, Stage: "summarize",
		Input:    []core.Record{core.NewRecord("r1", map[string]any{"text": text})},
		Envelope: task.Envelope{RunID: "run_1", Stage: "summarize", Sandbox: task.SandboxInline},
	}
}

// --- worker death -------------------------------------------------------

// A worker dies holding a task. The lease is the only thing that knows, and it
// is enough: the task is redelivered, another worker finishes it, and the
// client — which never learns any of this happened — gets the right answer.
func TestWorkerDeathDoesNotLoseTheTask(t *testing.T) {
	f := newFleet(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The doomed worker: it takes the task, starts work, and stops existing.
	doomedQ := &severed{Queue: f.q}
	doomed := newScripted()
	doomed.hold()
	f.start("doomed", doomedQ, doomed)

	out := f.execute(ctx, job("t1", "hello"))

	select {
	case <-doomed.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first worker never started the task")
	}
	doomedQ.cut() // the process is gone: no heartbeat, no commit, no goodbye

	// The survivor arrives while the corpse is still holding the lease, and
	// gets nothing until the lease runs out — which is the property that keeps
	// "at least once" from meaning "twice, always".
	survivor := newScripted()
	f.start("survivor", f.q, survivor)

	res := await(t, out, 8*time.Second).mustSucceed(t)
	if len(res.Output) != 1 || res.Output[0].String("summary") != "HELLO" {
		t.Fatalf("the result is wrong: %+v", res.Output)
	}
	if survivor.count() != 1 {
		t.Fatalf("the surviving worker executed the task %d times, want 1", survivor.count())
	}

	s, _, err := f.q.Status(ctx, "t1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if s.State != worker.StateDone {
		t.Fatalf("the task ended %s, want done", s.State)
	}
	if s.Receipt.Worker != "survivor" {
		t.Fatalf("the receipt names %q, want the worker that actually finished it", s.Receipt.Worker)
	}
	if s.Deliveries != 2 {
		t.Fatalf("the task was delivered %d times, want 2 (one lost, one done)", s.Deliveries)
	}

	// And the corpse comes back. Its work is refused, and nothing it does
	// changes what the run already read.
	doomedQ.heal()
	doomed.release()
	time.Sleep(200 * time.Millisecond)
	after, _, _ := f.q.Status(ctx, "t1")
	if after.Receipt.Worker != "survivor" || after.Receipt.Token != s.Receipt.Token {
		t.Fatalf("a resurrected worker changed the committed result to %+v", after.Receipt)
	}
}

// --- late results -------------------------------------------------------

// The same shape without the client: a worker that stalled past its expiry
// finishes and commits. The commit is refused, and the fleet's answer is the
// one the live worker produced.
func TestLateResultCannotOverwriteTheWinner(t *testing.T) {
	f := newFleet(t)
	ctx := t.Context()
	tk := job("t1", "hello")
	if _, err := f.q.Submit(ctx, worker.Submission{Task: tk, Needs: worker.Require(tk)}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	stale, err := f.q.Claim(ctx, worker.Claim{Worker: "slow", Caps: wildcard("slow"), Max: 1, TTL: ttl})
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v (%d)", err, len(stale))
	}

	time.Sleep(ttl + ttl/2) // the slow worker's lease runs out underneath it

	live, err := f.q.Claim(ctx, worker.Claim{Worker: "fast", Caps: wildcard("fast"), Max: 1, TTL: ttl})
	if err != nil || len(live) != 1 {
		t.Fatalf("takeover claim: %v (%d)", err, len(live))
	}
	winner, err := f.q.Commit(ctx, live[0].Lease, worker.Receipt{
		TaskID: "t1", Worker: "fast", Output: "right-answer"})
	if err != nil {
		t.Fatalf("the live worker could not commit: %v", err)
	}

	// The stale worker wakes up, none the wiser.
	_, err = f.q.Commit(ctx, stale[0].Lease, worker.Receipt{
		TaskID: "t1", Worker: "slow", Output: "stale-answer"})
	if err != nil {
		t.Fatalf("a late commit against a finished task should learn the outcome, not error: %v", err)
	}

	s, _, _ := f.q.Status(ctx, "t1")
	if s.Receipt.Output != winner.Output || s.Receipt.Worker != "fast" {
		t.Fatalf("the late result won: %+v", s.Receipt)
	}
}

// --- lease expiry -------------------------------------------------------

// Losing the lease stops the work. It is not politeness: every model call a
// fenced worker makes is money spent on output that is already refused.
func TestFencedWorkerStopsPayingForItsTask(t *testing.T) {
	f := newFleet(t)
	ctx := t.Context()

	held := newScripted()
	held.hold()
	f.start("holder", f.q, held)

	tk := job("t1", "hello")
	if _, err := f.q.Submit(ctx, worker.Submission{Task: tk, Needs: worker.Require(tk)}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	select {
	case <-held.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the worker never started the task")
	}

	// Cancelling the task is the queue's way of fencing a live holder, and it
	// travels the same path an expiry does: the next heartbeat is refused.
	if err := f.q.Cancel(ctx, "t1", "the client gave up"); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for !held.wasCancelled("t1") {
		if time.Now().After(deadline) {
			t.Fatal("a fenced worker kept working: its execution context was never cancelled")
		}
		time.Sleep(5 * time.Millisecond)
	}
	held.release()
}

// A worker that keeps heartbeating keeps its task, however long the work takes
// relative to the TTL. This is the other half of expiry: without it, the TTL
// would have to exceed the slowest task anybody ever runs.
func TestHeartbeatKeepsALongTaskFromBeingRedelivered(t *testing.T) {
	f := newFleet(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	slow := newScripted()
	slow.hold()
	f.start("slow", f.q, slow)

	out := f.execute(ctx, job("t1", "hello"))
	select {
	case <-slow.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the task never started")
	}

	// The second worker arrives with nothing to do, and must keep having
	// nothing to do for as long as the first keeps its lease alive.
	other := newScripted()
	f.start("other", f.q, other)

	time.Sleep(4 * ttl) // four lease lifetimes of honest, heartbeated work
	if other.count() != 0 {
		t.Fatalf("a heartbeated task was redelivered %d times", other.count())
	}
	slow.release()

	res := await(t, out, 5*time.Second).mustSucceed(t)
	if res.Output[0].String("summary") != "HELLO" {
		t.Fatalf("result: %+v", res.Output)
	}
	if slow.count() != 1 {
		t.Fatalf("the task ran %d times, want 1", slow.count())
	}
}

// --- network interruption -----------------------------------------------

// The queue goes away while a result is in hand. The work is finished and paid
// for by then, so the worker holds onto it and lands it when the network comes
// back — rather than dropping it and letting a redelivery buy it again.
func TestNetworkInterruptionDoesNotLoseAPaidResult(t *testing.T) {
	f := newFleet(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	link := &severed{Queue: f.q}
	exec := newScripted()
	exec.hold()
	f.start("w1", link, exec)

	out := f.execute(ctx, job("t1", "hello"))
	select {
	case <-exec.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the task never started")
	}

	// The interruption straddles the moment the result is produced.
	link.cut()
	exec.release()
	time.Sleep(30 * time.Millisecond)
	link.heal()

	res := await(t, out, 8*time.Second).mustSucceed(t)
	if res.Output[0].String("summary") != "HELLO" {
		t.Fatalf("result: %+v", res.Output)
	}
	if n := exec.count(); n != 1 {
		t.Fatalf("the task was executed %d times: the interruption cost a re-run", n)
	}
}

// The client's side of the same interruption: a submission and an await that
// do not go through are calls that failed, not tasks that failed.
func TestClientSurvivesAnInterruptedSubmission(t *testing.T) {
	cas, err := store.NewCAS("")
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	q := worker.NewMemQueue(worker.MemOptions{LeaseTTL: ttl})
	t.Cleanup(func() { _ = q.Close() })
	link := &severed{Queue: q}
	link.cut()

	client := worker.NewClient(worker.ClientConfig{
		Queue: link, Blobs: cas, Calls: 20, Backoff: 5 * time.Millisecond})

	w, err := worker.New(worker.Config{
		Queue: q, Blobs: cas, Exec: newScripted(), Name: "w1",
		Caps:     worker.Capabilities{Worker: "w1", Wildcard: true, Concurrency: 1},
		LeaseTTL: ttl, Heartbeat: ttl / 4, Poll: 2 * time.Millisecond,
		PollCeiling: 10 * time.Millisecond, Drain: time.Second,
	})
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = w.Run(ctx) }()
	defer func() { cancel(); <-done }()

	out := make(chan outcome, 1)
	go func() {
		res, err := client.Execute(ctx, job("t1", "hello"))
		out <- outcome{res: res, err: err}
	}()

	time.Sleep(40 * time.Millisecond) // the client is retrying into a wall
	link.heal()

	res := await(t, out, 8*time.Second).mustSucceed(t)
	if res.Output[0].String("summary") != "HELLO" {
		t.Fatalf("result: %+v", res.Output)
	}
}

// --- duplicate execution ------------------------------------------------

// Two workers execute one task, because the first was slow rather than dead.
// Both finish, both commit, and the fleet ends up with one result — and the
// bytes the losing worker wrote are the same bytes, at the same address, as
// the winner's.
func TestDuplicateExecutionYieldsOneResult(t *testing.T) {
	f := newFleet(t)
	ctx := t.Context()

	tk := job("t1", "hello")
	if _, err := f.q.Submit(ctx, worker.Submission{Task: tk, Needs: worker.Require(tk)}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Two leases over one task, arranged by letting the first expire — the
	// exact situation a paused process produces.
	first, err := f.q.Claim(ctx, worker.Claim{Worker: "w1", Caps: wildcard("w1"), Max: 1, TTL: ttl})
	if err != nil || len(first) != 1 {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(ttl + ttl/2)
	second, err := f.q.Claim(ctx, worker.Claim{Worker: "w2", Caps: wildcard("w2"), Max: 1, TTL: ttl})
	if err != nil || len(second) != 1 {
		t.Fatalf("takeover: %v", err)
	}

	// Both run the same task through the same deterministic executor, and both
	// store their output in the shared CAS.
	exec := newScripted()
	var hashes []string
	for _, a := range []worker.Assignment{first[0], second[0]} {
		res, err := exec.Execute(ctx, a.Task)
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
		blob, _ := json.Marshal(res.Output)
		h, err := f.cas.Put(blob)
		if err != nil {
			t.Fatalf("cas put: %v", err)
		}
		hashes = append(hashes, h)
	}
	if hashes[0] != hashes[1] {
		t.Fatalf("two executions of one task produced different addresses (%s, %s) — "+
			"content addressing is what makes duplicate delivery harmless",
			hashes[0], hashes[1])
	}

	// The live worker commits; the expired one commits afterwards and is told
	// whose result stands.
	winner, err := f.q.Commit(ctx, second[0].Lease, worker.Receipt{
		TaskID: "t1", Worker: "w2", Output: hashes[1], Records: 1})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	loser, err := f.q.Commit(ctx, first[0].Lease, worker.Receipt{
		TaskID: "t1", Worker: "w1", Output: hashes[0], Records: 1})
	if err != nil {
		t.Fatalf("the duplicate's commit: %v", err)
	}
	if loser.Token != winner.Token {
		t.Fatalf("the duplicate was told token %d, want the winner's %d", loser.Token, winner.Token)
	}

	stats, err := f.q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Done != 1 {
		t.Fatalf("two commits produced %d results, want 1", stats.Done)
	}
	if stats.Duplicates != 1 {
		t.Fatalf("the duplicate execution was not counted: %+v", stats)
	}
}

// Duplicate delivery through the whole stack: several workers racing on a
// short lease, and one result per task at the end of it.
func TestConcurrentWorkersProduceOneResultPerTask(t *testing.T) {
	f := newFleet(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	execs := make([]*scripted, 4)
	for i := range execs {
		execs[i] = newScripted()
		f.start(fmt.Sprintf("w%d", i+1), f.q, execs[i])
	}

	const tasks = 12
	outs := make([]<-chan outcome, tasks)
	for i := range outs {
		outs[i] = f.execute(ctx, job(fmt.Sprintf("t%d", i), fmt.Sprintf("record %d", i)))
	}
	for i, ch := range outs {
		res := await(t, ch, 15*time.Second).mustSucceed(t)
		want := strings.ToUpper(fmt.Sprintf("record %d", i))
		if got := res.Output[0].String("summary"); got != want {
			t.Fatalf("task %d returned %q, want %q", i, got, want)
		}
	}

	stats, err := f.q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Done != tasks {
		t.Fatalf("%d tasks produced %d results", tasks, stats.Done)
	}
	total := 0
	for _, e := range execs {
		total += e.count()
	}
	if total != tasks {
		t.Fatalf("%d tasks cost %d executions — nothing here should have been "+
			"redelivered, so anything above %d is a lease expiring under live work",
			tasks, total, tasks)
	}
}

// --- the seam -----------------------------------------------------------

// Everything above rests on the task being data. If a field stops surviving
// the crossing, a fleet starts running tasks that are subtly not the ones the
// planner built — so the envelope's fidelity is asserted rather than assumed.
func TestTaskSurvivesTheCrossingWithItsInputDetached(t *testing.T) {
	f := newFleet(t)
	ctx := t.Context()

	// Inline: -1 detaches every input, which is what a queue with a row-size
	// limit is configured for and what makes the CAS path the tested one.
	client := worker.NewClient(worker.ClientConfig{
		Queue: f.q, Blobs: f.cas, Inline: -1, Backoff: time.Millisecond})

	tk := job("t1", "hello")
	out := make(chan outcome, 1)
	go func() {
		res, err := client.Execute(ctx, tk)
		out <- outcome{res: res, err: err}
	}()

	// What the queue holds carries no records at all — only the hash.
	var a worker.Assignment
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := f.q.Claim(ctx, worker.Claim{Worker: "w1", Caps: wildcard("w1"), Max: 1, TTL: time.Minute})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if len(got) == 1 {
			a = got[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("nothing was ever queued")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if len(a.Task.Input) != 0 {
		t.Fatalf("a detached task still carries %d records", len(a.Task.Input))
	}
	if a.Input == "" {
		t.Fatal("a detached task carries no reference to its input either")
	}

	// And a worker resolves it from shared storage, byte for byte.
	blob, ok := f.cas.Get(a.Input)
	if !ok {
		t.Fatal("the input is not in shared storage")
	}
	var back []core.Record
	if err := json.Unmarshal(blob, &back); err != nil {
		t.Fatalf("input blob: %v", err)
	}
	if len(back) != 1 || back[0].String("text") != "hello" {
		t.Fatalf("the input did not survive the crossing: %+v", back)
	}

	f.start("w2", f.q, newScripted())
	res := await(t, out, 8*time.Second).mustSucceed(t)
	if res.Output[0].String("summary") != "HELLO" {
		t.Fatalf("result: %+v", res.Output)
	}
}

// A task nobody advertises for is a run that would otherwise hang. It fails,
// and the failure says what is missing.
func TestUnservableTaskFailsWithADiagnosis(t *testing.T) {
	f := newFleet(t)
	client := worker.NewClient(worker.ClientConfig{
		Queue: f.q, Blobs: f.cas, Wait: 150 * time.Millisecond, Backoff: time.Millisecond})

	// A worker that serves a different stage entirely.
	f.startWith("classifier", f.q, newScripted(), worker.Capabilities{
		Worker: "classifier", Stages: []string{"classify"}, Concurrency: 1,
		Sandboxes: []task.SandboxProfile{task.SandboxInline},
	})

	_, err := client.Execute(t.Context(), job("t1", "hello"))
	if err == nil {
		t.Fatal("a task no worker can run should not succeed")
	}
	if core.ClassOf(err) != core.FailTransient {
		t.Fatalf("an unclaimed task failed as %s, want transient so the scheduler retries",
			core.ClassOf(err))
	}
	if !strings.Contains(err.Error(), "summarize") {
		t.Fatalf("the failure does not name the stage nobody advertises: %v", err)
	}
}

func wildcard(name string) worker.Capabilities {
	return worker.Capabilities{Worker: name, Wildcard: true, Concurrency: 4}
}
