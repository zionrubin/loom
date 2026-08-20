// Package worker implements Loom's execution boundary as a service: a durable
// task queue with leases, a worker that claims from it, and a client that
// satisfies executor.Executor by putting tasks on it.
//
// The seam was already there. executor.Executor is one method over plain
// serializable data — Execute(ctx, task.Task) (task.Result, error) — and
// task.Envelope was designed from the start to carry everything a task is
// allowed to use rather than to point at it: grants, egress, context, budget,
// sandbox, and for the values too large to copy, content hashes into shared
// storage. Nothing in a task holds a pointer, a socket or a closure, so a task
// is already shippable. What was missing was somewhere to ship it to.
//
// This package is that, and its whole design is in three properties:
//
//	the client is an adapter        Local stays the default executor; Client is
//	                                another implementation of the same one
//	                                method, so planning, scheduling, retry,
//	                                escalation and budgets do not learn that
//	                                execution moved off-process.
//
//	the queue owns delivery         Leases, heartbeats, expiry and fencing
//	                                tokens live here, so a worker that dies
//	                                mid-task loses its claim rather than the
//	                                task. Delivery is therefore at-least-once.
//
//	the CAS owns payloads           Inputs, broadcasts and outputs travel by
//	                                content hash through storage both sides can
//	                                reach. Two workers executing one task write
//	                                identical bytes to one address, which is
//	                                what makes at-least-once delivery produce
//	                                exactly-once *work*.
//
// The third is the load-bearing one. A queue that leases work must redeliver
// what a dead worker was holding, and cannot tell "dead" from "slow": the
// standard consequence is that a task occasionally runs twice. Most systems
// answer that with deduplication in the consumer. Here it needs no answer,
// because a result is written to an address derived from its own bytes and
// committed under a fencing token — a duplicate execution produces the same
// blob at the same hash, and exactly one of the two commits becomes the task's
// receipt. The other is told which one won.
//
// # Where the pieces live
//
//	Queue        the contract: submit, claim, renew, commit, abandon, await
//	MemQueue     one process, many goroutines — the default and the tests
//	filequeue    many processes on one host, over a shared directory
//	Client       executor.Executor over a Queue: submit and await
//	Worker       the other side: claim, heartbeat, execute, commit
//
// Nothing in Client or Worker names a queue implementation, which is the same
// discipline findings applies to its backends: the contract is what is written
// against, so replacing the queue with SQS, Postgres or a broker is an import
// change rather than a rewrite.
package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/task"
)

// State is where a submitted task stands. The machine is deliberately small:
//
//	pending ──claim──▶ leased ──commit──▶ done
//	   ▲                  │
//	   └──expiry/abandon──┘──abandon(terminal)──▶ failed
//
// Expiry is the only edge no participant takes: it is what the queue does to a
// lease nobody renewed, and it is the reason a killed worker costs a
// redelivery rather than a result.
type State string

const (
	// StatePending is queued and claimable.
	StatePending State = "pending"
	// StateLeased is held by a worker whose lease has not yet expired.
	StateLeased State = "leased"
	// StateDone carries a receipt. Terminal, and the only state a result is
	// read from.
	StateDone State = "done"
	// StateFailed carries a failure the client re-raises with its original
	// class. Terminal.
	StateFailed State = "failed"
)

// Terminal reports whether a state admits no further transitions.
func (s State) Terminal() bool { return s == StateDone || s == StateFailed }

// Errors the queue raises. They are classified where a caller would otherwise
// have to guess: a fenced commit is permanent (retrying it cannot help — the
// work has an owner and it is not you), an unknown task is permanent, and a
// backend that cannot be reached is transient, which is what puts it back
// under the scheduler's ordinary backoff.
var (
	// ErrFenced is a lease that no longer stands: expired underneath its
	// holder, taken over by another worker, or superseded by a cancellation.
	ErrFenced = errors.New("worker: lease is fenced")
	// ErrNotFound names a task the queue has never held.
	ErrNotFound = errors.New("worker: no such task")
	// ErrClosed is a queue that has been shut down.
	ErrClosed = errors.New("worker: queue is closed")
	// ErrCancelled is the failure recorded against a task withdrawn by its
	// client.
	ErrCancelled = errors.New("worker: task cancelled")
)

// Queue is the durable boundary between the process that plans work and the
// processes that do it.
//
// Everything about the contract follows from one assumption: any participant
// may vanish between two calls. A client can die after submitting, a worker
// after claiming, and neither can be distinguished from one that is merely
// slow. So the queue holds the task rather than the connection, hands out
// leases rather than assignments, and settles every ambiguity with a fencing
// token instead of a clock.
//
// Implementations must be safe for concurrent use by many goroutines and, for
// the durable ones, by many processes.
type Queue interface {
	// Submit enqueues a task and returns its status. The queue holds one entry
	// per task ID, and what a second submission of the same ID does depends on
	// where that entry stands:
	//
	//	pending, leased  returned as it stands. This is the case that makes a
	//	                 lost acknowledgement free: the client that is not sure
	//	                 its submission landed re-submits, finds the work
	//	                 already in flight, and goes back to waiting rather than
	//	                 enqueueing a second copy of it.
	//	done             returned with its receipt, and nothing runs. A retry of
	//	                 a task that actually succeeded is answered from the
	//	                 result the fleet already paid for.
	//	failed           re-armed as pending, carrying the *new* submission.
	//	                 This is how the scheduler's recovery reaches the fleet:
	//	                 a retry that climbed the escalation ladder arrives here
	//	                 as the same task ID with a different resolved model, and
	//	                 must run against the envelope it now carries rather than
	//	                 the one that failed.
	//
	// Submission is therefore idempotent where it needs to be — within one
	// attempt — without freezing a task at its first outcome, which would put
	// class-aware recovery on the wrong side of the queue.
	Submit(ctx context.Context, s Submission) (Status, error)

	// Claim leases up to c.Max ready tasks this worker's capabilities can
	// serve, returning them with the leases governing them. It does not block:
	// an empty slice means there is nothing this worker can run right now, and
	// the polling interval is the caller's policy rather than the queue's.
	//
	// Claiming is where expiry is realized. A lease nobody renewed is reclaimed
	// here and the task becomes claimable again, its delivery count one higher.
	Claim(ctx context.Context, c Claim) ([]Assignment, error)

	// Renew extends a held lease, reporting false when it no longer stands.
	//
	// False is not an error. It is how a worker that stalled past its expiry —
	// a long GC pause, a machine suspended, a network that went away and came
	// back — learns that its work is no longer wanted, while it still has the
	// chance to stop rather than commit over whoever replaced it.
	Renew(ctx context.Context, l Lease, ttl time.Duration) (Lease, bool, error)

	// Commit records a terminal result under a lease and returns the receipt
	// the queue now holds.
	//
	// The returned receipt is not always the one passed in. A task already done
	// keeps its first receipt and returns it, so a duplicate delivery — or a
	// worker retrying a commit whose acknowledgement it never saw — learns the
	// outcome instead of creating a second one. Callers detect that by
	// comparing tokens: a receipt whose Token differs from the lease's is
	// somebody else's, and this execution was the redundant one.
	//
	// A commit under a fenced lease against a task that is *not* done is
	// refused with ErrFenced. That is the late-result case, and accepting it
	// would let a worker everyone had given up on overwrite the result of the
	// worker that replaced it.
	Commit(ctx context.Context, l Lease, r Receipt) (Receipt, error)

	// Abandon gives a lease back with a failure.
	//
	// The failure is terminal for the task: recovery policy — backoff,
	// escalation up the model ladder, dead-lettering — belongs to the
	// scheduler that submitted the work, which knows the run's budget and the
	// binding's ladder, and the queue would only be guessing at both. What the
	// queue redelivers is not failure but *silence*.
	Abandon(ctx context.Context, l Lease, f Failure) (Status, error)

	// Status reports one task as the queue currently sees it, with expiry
	// already applied.
	Status(ctx context.Context, taskID string) (Status, bool, error)

	// Await blocks until a task is done or failed, ctx ends, or the queue
	// closes. Implementations may signal or poll; the client does not care
	// which, which is what leaves room for a backend with real notifications.
	Await(ctx context.Context, taskID string) (Status, error)

	// Cancel withdraws a task. A pending task is failed immediately; a leased
	// one is fenced, so its worker learns from the next heartbeat that the
	// work is no longer wanted and stops paying for it.
	Cancel(ctx context.Context, taskID, reason string) error

	// Stats summarizes the queue: depth, and the accounting that says whether
	// leases are sized right.
	Stats(ctx context.Context) (Stats, error)

	Close() error
}

// Submission is a task offered to the fleet.
//
// It is the task plus the two things the queue needs that the task itself does
// not carry: what a worker must be able to do to run it, and how much
// redelivery it is worth. Everything else — model binding, grants, egress,
// context, budget, sandbox — is already in the envelope, which is why this
// type is as thin as it is.
type Submission struct {
	Task task.Task `json:"task"`
	// Input is the content hash of the task's input records when they have
	// been detached into shared storage, in which case Task.Input is empty and
	// the worker rehydrates it. Detaching keeps a redelivered task from
	// carrying its payload through the queue again, and keeps one copy of a
	// large batch rather than one per delivery.
	Input string `json:"input,omitempty"`
	// Needs is what a worker must advertise to claim this task.
	Needs Requirements `json:"needs"`
	// Deliveries bounds how many times the queue will hand this task out after
	// a lease expires (zero uses the queue's default). It is poison protection:
	// a task that kills every worker that touches it must eventually be
	// declared the problem rather than the workers.
	Deliveries int `json:"deliveries,omitempty"`
	// Affinity is where this task would rather run. It is a preference and
	// never a requirement — see the type.
	Affinity Affinity `json:"affinity,omitzero"`
	// Client identifies the submitter, for reporting.
	Client string `json:"client,omitempty"`
}

// Affinity is the queue's one soft input: work that would go faster on a
// particular worker, without becoming work that only that worker can do.
//
// The distinction from Requirements is the whole of it, and it is a distinction
// the fleet cannot afford to blur:
//
//	Requirements   hard. A worker that cannot serve them must not claim the
//	               task, because claiming it means failing it.
//	Affinity       soft. A worker that does not hold the state runs the task
//	               correctly and more slowly, because state is an optimization
//	               and the executor's fallback is the reference path.
//
// Encoding "prefers worker A" as a requirement would make a task unclaimable
// the moment A died — which is precisely the failure locality exists to survive
// — so it is encoded here, where the queue may act on it and is never obliged
// to. A worker that holds the key is offered the task first. Every other worker
// may take it, immediately if Grace is zero and after Grace otherwise.
type Affinity struct {
	// Key is the state the task would like to find already materialized: a
	// continuation key, in practice, since that is what task.Locality returns.
	Key string `json:"key,omitempty"`
	// Grace is how long the queue holds a keyed task back from workers that do
	// not hold its state, giving one that does a chance to ask (default zero:
	// no waiting at all, pure preference).
	//
	// It is the one place this mechanism can cost latency, so it is opt-in and
	// wants to be small — a poll interval or two, not a lease. What it buys is
	// affinity that survives contention: with no grace, a busy fleet hands the
	// session to whichever worker asked first, and the state-holder gets it
	// only by luck. What it costs, in the worst case where the state-holder has
	// died, is exactly Grace once — after which the task goes to anybody, which
	// is what keeps "never wait forever" true rather than aspirational.
	Grace time.Duration `json:"grace,omitempty"`
}

// Zero reports whether no affinity is expressed.
func (a Affinity) Zero() bool { return a.Key == "" }

// Claim is a worker asking for work.
type Claim struct {
	Worker string       `json:"worker"`
	Caps   Capabilities `json:"caps"`
	// Max is how many assignments to take — in practice the worker's free
	// slots, so the queue never leases work the worker cannot start.
	Max int `json:"max"`
	// Resident is the affinity keys this worker currently holds state for.
	//
	// It sits on the claim rather than on Capabilities on purpose. Capabilities
	// are what a worker *can* do: they change when the binary changes, they are
	// a hard filter, and a task nobody advertises for is a bug. Residency is
	// what a worker *happens to hold*: it changes on every eviction, it is a
	// preference, and a key nobody holds is the ordinary state of the world one
	// second after a process starts. Advertising it as a capability would make
	// the first request of every session unclaimable.
	//
	// It is bounded by whatever bounds the worker's state — delta.Options
	// MaxBytes, in the usual wiring — because a claim is a request and a
	// request should not grow without limit.
	Resident []string `json:"resident,omitempty"`
	// TTL is the requested lease duration. Implementations may clamp it; the
	// granted expiry is on the returned lease and is the only one that counts.
	TTL time.Duration `json:"ttl"`
}

// Assignment is one leased task, as a worker receives it.
type Assignment struct {
	Lease Lease     `json:"lease"`
	Task  task.Task `json:"task"`
	Input string    `json:"input,omitempty"`
	// Delivery counts how many times this task has been handed out, this one
	// included. Above one it means an earlier lease expired, which a worker
	// can log and a report can total.
	Delivery int `json:"delivery"`
	// Local reports that this assignment matched the worker's residency: the
	// queue handed it here because the state is here. It is the number that
	// says whether locality is working, and the only place it can be observed
	// — the worker knows it holds the state and the client knows the task
	// wanted it, but only the queue saw the two meet.
	Local bool `json:"local,omitempty"`
}

// Lease is a worker's exclusive, expiring, fenced claim on one task.
//
// The three fields beyond the identity are the whole of the failure model.
// Expires makes a crash survivable — the claim ends whether or not its holder
// is alive to end it. Token makes takeover safe: it increases on every claim,
// so a worker that stalled past its expiry and woke up holding token 3 can be
// told apart from the live owner holding token 4 by comparing integers rather
// than by trusting either machine's clock. And Worker makes both legible in a
// report.
//
// Fencing is what turns "at-least-once delivery" from a hazard into an
// accounting detail. Without it, the only defence against a resurrected worker
// committing over its replacement is a timeout longer than the longest pause
// anyone will ever have, which is not a defence.
type Lease struct {
	TaskID  string    `json:"task_id"`
	Worker  string    `json:"worker"`
	Token   int64     `json:"token"`
	Granted time.Time `json:"granted"`
	Expires time.Time `json:"expires"`
}

// Live reports whether the lease still stands at now.
func (l Lease) Live(now time.Time) bool {
	return l.Token > 0 && !l.Expires.IsZero() && l.Expires.After(now)
}

// Zero reports whether no lease is held.
func (l Lease) Zero() bool { return l.Token == 0 }

// Receipt is the terminal record of one task's execution: everything
// task.Result carries except the records themselves, which live in shared
// storage under Output.
//
// Splitting the payload out is not only about queue size. The output blob is
// addressed by its own content, so two workers that executed the same task
// wrote the same bytes to the same address — the receipt that loses the race
// is redundant rather than conflicting, and a client that reads the winner's
// receipt gets the loser's blob if that is the one that landed first. There is
// no version of "which copy is real" to answer.
type Receipt struct {
	TaskID string `json:"task_id"`
	Seq    int    `json:"seq"`
	Stage  string `json:"stage"`
	Worker string `json:"worker"`
	// Token is the lease this receipt was committed under. A caller comparing
	// it to its own lease learns whether its execution was the one that counted.
	Token int64 `json:"token"`
	// Output is the content hash of the output records.
	Output   string        `json:"output,omitempty"`
	Records  int           `json:"records"`
	Usage    core.Usage    `json:"usage"`
	Model    string        `json:"model,omitempty"`
	CacheHit bool          `json:"cache_hit,omitempty"`
	Artifact string        `json:"artifact,omitempty"`
	Latency  time.Duration `json:"latency,omitempty"`
	Delivery int           `json:"delivery,omitempty"`
	At       time.Time     `json:"at"`
}

// Failure is an execution that ended badly, carried across the process
// boundary with the one thing recovery depends on: its class.
//
// An error crossing a queue loses its Go type, and with it everything
// core.ClassOf reads. Carrying the class explicitly is what keeps a semantic
// failure escalating up the model ladder and a permanent one dead-lettering
// immediately, instead of every remote failure degrading to the unclassified
// default.
type Failure struct {
	Class   core.FailureClass `json:"class"`
	Message string            `json:"message"`
	// Usage is what the failed execution spent before it failed, and it
	// crosses the queue for the same reason the class does: it is the other
	// thing the client cannot reconstruct. A worker that called a model and
	// then rejected its answer has spent real money, and a failure that
	// reported only what went wrong would leave the run's budget governor
	// counting a distributed run cheaper than the identical local one.
	Usage    core.Usage `json:"usage,omitzero"`
	Worker   string     `json:"worker,omitempty"`
	Delivery int        `json:"delivery,omitempty"`
	At       time.Time  `json:"at"`
}

// Failed describes err as a Failure, preserving its class.
func Failed(err error, worker string) Failure {
	if err == nil {
		return Failure{}
	}
	return Failure{
		Class: core.ClassOf(err), Message: err.Error(),
		Worker: worker, At: time.Now(),
	}
}

// Err rebuilds a classified error from the failure. The message is the remote
// one; the class is what the scheduler acts on.
func (f Failure) Err() error {
	msg := f.Message
	if msg == "" {
		msg = "task failed on a worker"
	}
	err := errors.New(msg)
	switch f.Class {
	case core.FailTransient:
		return core.Transient(err)
	case core.FailSemantic:
		return core.Semantic(err)
	case core.FailBudget:
		return core.BudgetExceeded(err)
	default:
		return core.Permanent(err)
	}
}

// Status is the queue's view of one task, with expiry already applied — a
// lease whose time has passed reads as pending here, because that is what it
// is to everyone who might claim it next.
type Status struct {
	TaskID     string    `json:"task_id"`
	RunID      string    `json:"run_id,omitempty"`
	Stage      string    `json:"stage,omitempty"`
	State      State     `json:"state"`
	Lease      Lease     `json:"lease,omitempty"`
	Receipt    *Receipt  `json:"receipt,omitempty"`
	Failure    *Failure  `json:"failure,omitempty"`
	Deliveries int       `json:"deliveries"`
	Submitted  time.Time `json:"submitted"`
	Updated    time.Time `json:"updated"`
}

// Terminal reports whether the task has finished, either way.
func (s Status) Terminal() bool { return s.State.Terminal() }

// Stats is a queue's accounting, in two halves.
//
// Pending, Leased, Done and Failed are current depths, with expiry applied — a
// lease whose time has passed counts as pending, because that is what it is to
// the next worker that asks. The rest are cumulative counters over everything
// the queue has ever been told, and three of them are the ones worth watching:
// Expired says the lease TTL is short relative to the work, Duplicates says how
// often that cost a redundant execution, and Fenced says how often a late
// worker tried to commit over its replacement.
type Stats struct {
	// Depths.
	Pending int `json:"pending"`
	Leased  int `json:"leased"`
	Done    int `json:"done"`
	Failed  int `json:"failed"`
	// Counters.
	Submitted  int `json:"submitted"`
	Claims     int `json:"claims"`
	Expired    int `json:"expired"`
	Duplicates int `json:"duplicates"`
	Fenced     int `json:"fenced"`
	Cancelled  int `json:"cancelled"`
	// Local counts claims that matched a worker's residency, and Displaced
	// those where a keyed task went to a worker that did not hold its state.
	// Their ratio is how well affinity is working; Displaced climbing is
	// either a fleet too busy to keep sessions together or a grace of zero on
	// a workload that needed one.
	Local     int `json:"local"`
	Displaced int `json:"displaced"`
}

// --- Capabilities -------------------------------------------------------

// Capabilities is what a worker advertises: the work it is equipped to do.
//
// A remote executor needs this and a local one does not, for a reason worth
// stating. In one process the executor is provisioned from the same plan that
// produced the task, so "can this executor run this task" is true by
// construction. Across a fleet it is a question — workers are deployed
// separately, upgraded separately, and given different credentials, tools and
// hardware on purpose. A worker that runs a stage it has no runner for fails
// the task; a worker that claims a model it holds no key for fails it more
// expensively. Advertising is how a claim is refused before it is made rather
// than after it has cost something.
//
// The four dimensions are the four ways workers actually differ:
//
//	Stages       which op runners are compiled into this binary — the deepest
//	             difference, because a runner is code
//	Providers    which models it can reach, which is credentials and network
//	             and, for a local engine, hardware
//	Tools        which side-effecting capabilities it can serve
//	Sandboxes    which isolation profiles it can honor — the envelope's
//	             sandbox field is a demand, not a hint
//
// Concurrency is not a filter but a ceiling: how many tasks this worker runs
// at once. It travels with the advertisement so a queue can see the fleet's
// total capacity, and it bounds what the worker asks for on each claim.
type Capabilities struct {
	Worker      string                `json:"worker"`
	Stages      []string              `json:"stages,omitempty"`
	Providers   []string              `json:"providers,omitempty"`
	Tools       []string              `json:"tools,omitempty"`
	Sandboxes   []task.SandboxProfile `json:"sandboxes,omitempty"`
	MCP         []string              `json:"mcp,omitempty"`
	Concurrency int                   `json:"concurrency,omitempty"`
	// Labels are the deployment's own axes — region, tier, tenant — matched
	// exactly against a task's demands. The framework attaches no meaning to
	// them, which is what makes them useful for the ones it did not anticipate.
	Labels map[string]string `json:"labels,omitempty"`
	// Wildcard advertises that this worker can serve anything asked of it. It
	// is for the common deployment where every worker runs the same binary
	// against the same account, where enumerating the four lists is
	// bookkeeping that can only go stale.
	Wildcard bool `json:"wildcard,omitempty"`
}

// Requirements is what one task demands of a worker: the mirror of
// Capabilities, derived from the envelope rather than declared.
type Requirements struct {
	Stage   string              `json:"stage,omitempty"`
	Models  []string            `json:"models,omitempty"`
	Tools   []string            `json:"tools,omitempty"`
	Sandbox task.SandboxProfile `json:"sandbox,omitempty"`
	MCP     []string            `json:"mcp,omitempty"`
	Labels  map[string]string   `json:"labels,omitempty"`
}

// Require derives what a task demands from its envelope.
//
// Grants are the source, not the pipeline definition, and that is deliberate:
// the envelope is the planner's minimal, explicit statement of what this task
// may touch, so reading requirements off it cannot drift from what the task
// will actually be allowed to do. A tool the task is granted counts as
// required even if this particular record never invokes it — the alternative
// is a worker that claims work it can serve only if the model happens not to
// ask for the tool.
//
// The model is the resolved one when the scheduler has picked it (recovery may
// have climbed the ladder since submission), and otherwise every model the
// envelope grants, which is the ladder itself.
func Require(t task.Task) Requirements {
	r := Requirements{Stage: t.Stage, Sandbox: t.Envelope.Sandbox}
	if r.Sandbox == "" {
		r.Sandbox = task.SandboxInline
	}
	if t.ResolvedModel != "" {
		r.Models = []string{t.ResolvedModel}
	}
	for _, c := range t.Envelope.Grants.List() {
		switch s := string(c); {
		case strings.HasPrefix(s, "model:"):
			if t.ResolvedModel == "" {
				r.Models = append(r.Models, strings.TrimPrefix(s, "model:"))
			}
		case strings.HasPrefix(s, "tool:"):
			r.Tools = append(r.Tools, strings.TrimPrefix(s, "tool:"))
		}
	}
	for name := range t.Envelope.MCP {
		r.MCP = append(r.MCP, name)
	}
	sort.Strings(r.MCP)
	return r
}

// CanRun reports why these capabilities cannot serve a task, or nil when they
// can. The reason is returned rather than a bare false because a task nobody
// claims is otherwise the hardest thing in a fleet to diagnose: it is not
// failing, it is not running, and the queue's depth is the only symptom.
func (c Capabilities) CanRun(r Requirements) error {
	if c.Wildcard {
		return nil
	}
	if r.Stage != "" && !has(c.Stages, r.Stage) {
		return fmt.Errorf("no runner for stage %q", r.Stage)
	}
	sandbox := r.Sandbox
	if sandbox == "" {
		sandbox = task.SandboxInline
	}
	if !hasSandbox(c.Sandboxes, sandbox) {
		return fmt.Errorf("sandbox profile %q not supported", sandbox)
	}
	// A ladder is satisfied by any rung: the scheduler resolves the model, and
	// a worker that can serve one of the models this task may use can run it.
	if len(r.Models) > 0 {
		served := false
		for _, m := range r.Models {
			if has(c.Providers, m) {
				served = true
				break
			}
		}
		if !served {
			return fmt.Errorf("no provider for model(s) %s", strings.Join(r.Models, ", "))
		}
	}
	for _, t := range r.Tools {
		if !has(c.Tools, t) {
			return fmt.Errorf("tool %q not registered", t)
		}
	}
	for _, s := range r.MCP {
		if !has(c.MCP, s) {
			return fmt.Errorf("no connection to MCP server %q", s)
		}
	}
	for k, v := range r.Labels {
		if c.Labels[k] != v {
			return fmt.Errorf("label %s=%q not matched", k, v)
		}
	}
	return nil
}

// Prefers reports whether this claim's worker holds the state a submission
// wants, which is the queue's whole affinity decision.
//
// It lives here rather than in either queue implementation because it is a rule
// about the protocol and not about storage: two queues that scored locality
// differently would be two queues, and the conformance suite would have nothing
// to assert.
func (c Claim) Prefers(a Affinity) bool {
	return !a.Zero() && has(c.Resident, a.Key)
}

// Held reports whether a keyed submission is still within its grace window,
// counting from when it became claimable — so a task redelivered after its
// worker died gets a fresh, brief chance to reach another worker holding the
// same session before it goes to whoever asks.
func (a Affinity) Held(since, now time.Time) bool {
	return !a.Zero() && a.Grace > 0 && now.Sub(since) < a.Grace
}

func has(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func hasSandbox(list []task.SandboxProfile, want task.SandboxProfile) bool {
	for _, s := range list {
		if s == want || (s == "" && want == task.SandboxInline) {
			return true
		}
	}
	return false
}

// ID mints an identifier for this process: host, pid and entropy.
//
// The entropy matters for the same reason it does for a findings executor: two
// containers of one deployment share a hostname pattern and can share a pid,
// and a worker ID that collides is two processes the queue believes are one —
// which is exactly the confusion leases exist to prevent.
func ID(prefix string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	if i := strings.IndexByte(host, '.'); i > 0 {
		host = host[:i]
	}
	if prefix == "" {
		prefix = "w"
	}
	return fmt.Sprintf("%s-%s-%d-%04x", prefix, host, os.Getpid(), rand.Int31n(1<<16))
}

// defaults shared by every queue implementation, so a worker moved from the
// memory queue to a durable one meets the same timings.
const (
	// DefaultLeaseTTL is how long a claim stands without a heartbeat. It bounds
	// how long a killed worker delays the task it was holding, so it wants to
	// be short; renewal is what lets it be short without punishing a task that
	// legitimately runs longer than one.
	DefaultLeaseTTL = 30 * time.Second
	// DefaultDeliveries bounds redelivery after expiry. Three is enough to
	// survive a bad machine and few enough that a task which kills workers is
	// declared the problem before it has killed many.
	DefaultDeliveries = 3
	// DefaultClaimBatch is how many tasks a claim takes when the caller does
	// not say.
	DefaultClaimBatch = 1
)
