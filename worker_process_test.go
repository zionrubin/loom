package loom_test

// The exit criterion, tested the only way it can honestly be tested: real
// worker processes, a real kill, and a pipeline neither side was written
// specially for.
//
// Every claim here is a measurement rather than a counter the system keeps
// about itself. "The client made no model calls" is checked with a provider
// that fails if it is ever called in the parent. "The fleet did the work" is
// counted from a file only the mock provider appends to, from whichever
// process happens to call it. "Killing a worker did not corrupt the result" is
// checked by running the same pipeline locally and comparing every record.
//
// The children are this test binary re-executed with a spec in the
// environment, which is the same trick findings/distributed_test.go uses and
// for the same reason: a worker has to be a separate address space for any of
// this to mean anything.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/worker"
	"github.com/zionrubin/loom/worker/filequeue"
)

// childEnv carries the spec to a worker process. Its presence is what turns
// this test binary into a worker.
const childEnv = "LOOM_WORKER_SPEC"

// workerSpec is one worker process: which fleet to join, who to be, and how
// slowly to pretend to think.
type workerSpec struct {
	State   string        `json:"state"`   // the shared state dir: CAS and cache
	Queue   string        `json:"queue"`   // the queue directory
	Calls   string        `json:"calls"`   // the log the mock provider appends to
	Name    string        `json:"name"`    // this worker's identity in the fleet
	Latency time.Duration `json:"latency"` // how long one model call takes
	Lease   time.Duration `json:"lease"`
	Slots   int           `json:"slots"`
	Ready   string        `json:"ready"` // touched once the worker is serving
}

// --- the pipeline, compiled into both sides -----------------------------

func fleetRecords(n int) []core.Record {
	recs := make([]core.Record, n)
	for i := range recs {
		recs[i] = core.NewRecord(fmt.Sprintf("doc%02d", i),
			map[string]any{"text": fmt.Sprintf("document number %d", i)})
	}
	return recs
}

// fleetPipeline is the pipeline under test, and the point of the exercise is
// that there is exactly one of it. The client process builds it to plan the
// run; every worker process builds it to have runners for the stages. Neither
// mentions a queue, a worker or a lease.
func fleetPipeline(n int) *pipeline.Pipeline {
	p := pipeline.New("fleet-digest")
	p.FromRecords("docs", fleetRecords(n)).
		Map("tag", func(r core.Record) (core.Record, error) {
			r.Data["tagged"] = true
			return r, nil
		}).
		Infer("summarize", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			System:  "You summarize documents.",
			Prompt:  "Summarize: {{.text}}",
		})
	return p
}

// fleetOptions are the options both sides pass. That they are the same list is
// the whole claim: a worker is provisioned from the run's configuration, not
// from a separate deployment description that can drift from it.
//
// Workers is the one number that means different things on the two sides — how
// many tasks the scheduler keeps in flight, and how many a worker runs at once
// — so it is the one each side sets for itself.
func fleetOptions(spec workerSpec, reg *model.Registry) []loom.Option {
	return []loom.Option{
		loom.WithRegistry(reg),
		loom.WithStateDir(spec.State),
		loom.WithRetry(runtime.RetryPolicy{MaxAttempts: 4, BaseDelay: 20 * time.Millisecond,
			MaxDelay: 200 * time.Millisecond}),
	}
}

// countingMock is the provider the workers run: deterministic output, a
// configurable delay so a kill can land mid-call, and one line appended to a
// shared file per call.
//
// The file is the instrument. Counters inside a process cannot see the other
// processes, and the question this test asks — how much work did the fleet
// actually do — spans all of them.
func countingMock(reg *model.Registry, callLog string, latency time.Duration) error {
	_, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithLatency(latency),
		model.WithHandler(func(req model.Request) (string, error) {
			if callLog != "" {
				f, err := os.OpenFile(callLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
				if err == nil {
					// One short line, one write: append is atomic at this size on
					// every filesystem this runs on, so concurrent workers cannot
					// interleave into each other's records.
					_, _ = f.WriteString(fmt.Sprintf("%s|%d\n", req.Prompt, os.Getpid()))
					_ = f.Close()
				}
			}
			return "SUMMARY: " + strings.TrimPrefix(req.Prompt, "Summarize: "), nil
		}))
	return err
}

// TestFleetWorkerProcess is the worker a spawned child runs. It is a test
// function because a test binary has no other entry point; without a spec in
// its environment it does nothing at all.
func TestFleetWorkerProcess(t *testing.T) {
	blob := os.Getenv(childEnv)
	if blob == "" {
		t.Skip("not a worker child")
	}
	var spec workerSpec
	if err := json.Unmarshal([]byte(blob), &spec); err != nil {
		t.Fatalf("spec: %v", err)
	}

	reg := model.NewRegistry()
	if err := countingMock(reg, spec.Calls, spec.Latency); err != nil {
		t.Fatalf("registry: %v", err)
	}
	q, err := filequeue.Open(spec.Queue, filequeue.Options{LeaseTTL: spec.Lease})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	defer q.Close()

	opts := append(fleetOptions(spec, reg),
		loom.WithWorkerService(q),
		loom.WithWorkerName(spec.Name),
		loom.WithWorkerLease(spec.Lease),
		loom.WithWorkers(spec.Slots))

	w, err := loom.NewWorker(fleetPipeline(0), opts...)
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	defer w.Close()

	// Announced only once the worker is provisioned and about to claim, so the
	// parent's "everyone is up" is a fact rather than a guess.
	if spec.Ready != "" {
		if err := os.WriteFile(spec.Ready, []byte(w.Name()), 0o644); err != nil {
			t.Fatalf("ready: %v", err)
		}
	}
	// The parent ends this process, by kill or by signal. Serving forever is
	// the correct behaviour for a worker.
	_ = w.Run(context.Background())
}

// --- the exit criterion -------------------------------------------------

// One pipeline, three worker processes, and one of them killed with SIGKILL
// while it is holding work. The run finishes, every record has the answer a
// single-process run produces, and the only cost of the kill is that the tasks
// it was holding were executed twice.
func TestPipelineSurvivesAKilledWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns worker processes")
	}
	const records = 12
	dir := t.TempDir()
	spec := workerSpec{
		State: filepath.Join(dir, "state"),
		Queue: filepath.Join(dir, "queue"),
		Calls: filepath.Join(dir, "calls.log"),
		// Long enough that a kill lands inside a model call rather than
		// between two, which is the case the exit criterion is about.
		Latency: 200 * time.Millisecond,
		// Short enough that the fleet notices the death in about a second.
		Lease: time.Second,
		Slots: 2,
	}
	if err := os.MkdirAll(spec.State, 0o755); err != nil {
		t.Fatal(err)
	}

	workers := startWorkers(t, spec, "worker-1", "worker-2", "worker-3")

	// The client's registry knows the same models — the scheduler resolves the
	// binding and applies rate limits — but its provider is a trap. If the
	// parent ever calls a model, execution did not move off this process.
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(model.Request) (string, error) {
			return "", fmt.Errorf("the client process called a model: execution did not leave it")
		})); err != nil {
		t.Fatal(err)
	}

	q, err := filequeue.Open(spec.Queue, filequeue.Options{LeaseTTL: spec.Lease})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	type runResult struct {
		res *loom.RunResult
		err error
	}
	done := make(chan runResult, 1)
	go func() {
		res, err := loom.Run(ctx, fleetPipeline(records),
			append(fleetOptions(spec, reg),
				loom.WithWorkerService(q), loom.WithWorkers(8))...)
		done <- runResult{res, err}
	}()

	// Kill a worker that is demonstrably in the middle of something. Aiming at
	// the queue's own record of who holds what — rather than at a stopwatch —
	// is what makes this a test of "killed during execution" rather than of
	// "killed at some point".
	victim, held := waitForLease(t, q, "summarize", 30*time.Second)
	t.Logf("killing %s while it holds %s", victim, held)
	if err := workers[victim].Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill %s: %v", victim, err)
	}

	var got runResult
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal("the run did not finish after a worker was killed")
	}
	if got.err != nil {
		t.Fatalf("the run failed: %v", got.err)
	}

	// --- nothing was lost ---
	out := got.res.StageOutputs["summarize"]
	if len(out) != records {
		t.Fatalf("the run produced %d records, want %d — a task was lost", len(out), records)
	}

	// --- nothing was corrupted ---
	// The same pipeline, run locally in this process against a plain mock. If
	// the fleet's answers differ from these in any record, the distribution
	// changed the computation.
	want := localRun(t, records)
	for _, r := range out {
		if want[r.ID] == "" {
			t.Fatalf("the fleet produced a record %q the pipeline does not have", r.ID)
		}
		if got := r.String("output"); got != want[r.ID] {
			t.Fatalf("record %s: the fleet answered %q, a local run answers %q",
				r.ID, got, want[r.ID])
		}
	}
	if len(want) != len(out) {
		t.Fatalf("the fleet returned %d records, a local run returns %d", len(out), len(want))
	}

	// --- the kill actually cost something, and it cost the right thing ---
	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	// Every task the run created finished — including the cheap ones, since a
	// Map is a task on this queue like anything else.
	queued, err := q.Tasks()
	if err != nil {
		t.Fatalf("tasks: %v", err)
	}
	for _, s := range queued {
		if s.State != worker.StateDone {
			t.Fatalf("task %s (%s) ended %s: the fleet lost it", s.TaskID, s.Stage, s.State)
		}
	}
	if stats.Done != len(queued) || len(queued) < records {
		t.Fatalf("the queue holds %d results for %d tasks", stats.Done, len(queued))
	}
	if stats.Expired == 0 {
		t.Fatal("no lease expired: the kill did not land while the worker held work, " +
			"so this run did not test what it claims to")
	}

	calls := countLines(t, spec.Calls)
	if calls < records {
		t.Fatalf("the fleet made %d model calls for %d records: work was skipped", calls, records)
	}
	// Redelivery is the price of at-least-once, and it is bounded by what the
	// dead worker was holding: its slots, plus the one task the survivors may
	// have re-run for the same reason. Anything beyond that is leases expiring
	// under live workers.
	if maxCalls := records + 2*spec.Slots; calls > maxCalls {
		t.Fatalf("the fleet made %d model calls for %d records (at most %d expected): "+
			"work is being redelivered under live workers", calls, records, maxCalls)
	}
	t.Logf("%d records, %d model calls across the fleet, %d lease expiries, %d duplicate commits",
		records, calls, stats.Expired, stats.Duplicates)

	// --- and the surviving workers did all of it ---
	if hosts := distinctPIDs(t, spec.Calls); hosts < 2 {
		t.Fatalf("every model call came from %d process(es): the work did not spread "+
			"across the fleet", hosts)
	}
}

// The same fleet without a kill: the plain claim that a pipeline runs unchanged
// across several worker processes, and costs exactly what it would locally.
func TestPipelineRunsAcrossWorkerProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns worker processes")
	}
	const records = 12
	dir := t.TempDir()
	spec := workerSpec{
		State: filepath.Join(dir, "state"),
		Queue: filepath.Join(dir, "queue"),
		Calls: filepath.Join(dir, "calls.log"),
		Lease: 2 * time.Second,
		Slots: 3,
	}
	if err := os.MkdirAll(spec.State, 0o755); err != nil {
		t.Fatal(err)
	}
	startWorkers(t, spec, "worker-1", "worker-2")

	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(model.Request) (string, error) {
			return "", fmt.Errorf("the client process called a model")
		})); err != nil {
		t.Fatal(err)
	}
	q, err := filequeue.Open(spec.Queue, filequeue.Options{LeaseTTL: spec.Lease})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	res, err := loom.Run(ctx, fleetPipeline(records),
		append(fleetOptions(spec, reg), loom.WithWorkerService(q), loom.WithWorkers(8))...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(res.StageOutputs["summarize"]) != records {
		t.Fatalf("produced %d records, want %d", len(res.StageOutputs["summarize"]), records)
	}
	want := localRun(t, records)
	for _, r := range res.StageOutputs["summarize"] {
		if got := r.String("output"); got != want[r.ID] {
			t.Fatalf("record %s: fleet %q, local %q", r.ID, got, want[r.ID])
		}
	}
	if calls := countLines(t, spec.Calls); calls != records {
		t.Fatalf("%d model calls for %d records: an undisturbed fleet must cost "+
			"exactly what a local run costs", calls, records)
	}

	// The run's own accounting crossed back with the results: usage is charged
	// against the fleet's budget governor even though the tokens were spent in
	// another process.
	if res.Spent.Requests != records {
		t.Fatalf("the governor recorded %d requests, want %d — usage did not come "+
			"back with the results", res.Spent.Requests, records)
	}
	if res.Report.Totals().Requests != records {
		t.Fatalf("the report recorded %d requests, want %d", res.Report.Totals().Requests, records)
	}

	// A second run over the same shared state replays from the cache the
	// workers wrote into it, without reaching a model at all — the checkpoint
	// property, surviving the process boundary because the cache key is
	// content and the CAS is shared.
	res2, err := loom.Run(ctx, fleetPipeline(records),
		append(fleetOptions(spec, reg), loom.WithWorkerService(q), loom.WithWorkers(8))...)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if calls := countLines(t, spec.Calls); calls != records {
		t.Fatalf("the second run made %d new model calls, want 0", calls-records)
	}
	for _, r := range res2.StageOutputs["summarize"] {
		if got := r.String("output"); got != want[r.ID] {
			t.Fatalf("cached replay changed record %s: %q", r.ID, got)
		}
	}
}

// --- harness ------------------------------------------------------------

// startWorkers spawns worker processes and waits until each is serving.
func startWorkers(t *testing.T, spec workerSpec, names ...string) map[string]*exec.Cmd {
	t.Helper()
	out := map[string]*exec.Cmd{}
	for _, name := range names {
		s := spec
		s.Name = name
		s.Ready = filepath.Join(filepath.Dir(spec.State), "ready-"+name)

		blob, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("spec: %v", err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestFleetWorkerProcess$", "-test.timeout=120s")
		cmd.Env = append(os.Environ(), childEnv+"="+string(blob))
		cmd.Stdout, cmd.Stderr = &strings.Builder{}, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		out[name] = cmd
		t.Cleanup(func() {
			_ = cmd.Process.Signal(syscall.SIGKILL)
			_ = cmd.Wait()
		})
	}
	for _, name := range names {
		ready := filepath.Join(filepath.Dir(spec.State), "ready-"+name)
		if !waitForFile(ready, 60*time.Second) {
			t.Fatalf("%s never started serving", name)
		}
	}
	return out
}

// waitForLease blocks until some worker is holding a task of the given stage,
// and reports which worker and which task. It is how the kill is aimed: the
// queue's record of who holds what is the only thing in the system that knows
// a worker is mid-execution.
//
// The stage matters. A pipeline's cheap stages — a Map, a Filter — are tasks
// too, and a kill that landed on one would prove only that an instantaneous
// task survives being interrupted. Waiting for the inference stage is what
// makes the victim a process that is part-way through a paid model call.
func waitForLease(t *testing.T, q *filequeue.Queue, stage string, within time.Duration) (string, string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		tasks, err := q.Tasks()
		if err != nil {
			t.Fatalf("tasks: %v", err)
		}
		for _, s := range tasks {
			if s.State == worker.StateLeased && s.Stage == stage && s.Lease.Worker != "" {
				return s.Lease.Worker, s.TaskID
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no worker ever held a %s task", stage)
	return "", ""
}

func waitForFile(path string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// localRun executes the same pipeline in this process, and is the definition of
// a correct answer that the fleet is held to.
func localRun(t *testing.T, n int) map[string]string {
	t.Helper()
	reg := model.NewRegistry()
	if err := countingMock(reg, "", 0); err != nil {
		t.Fatalf("registry: %v", err)
	}
	res, err := loom.Run(context.Background(), fleetPipeline(n),
		loom.WithRegistry(reg), loom.WithWorkers(4))
	if err != nil {
		t.Fatalf("local run: %v", err)
	}
	out := map[string]string{}
	for _, r := range res.StageOutputs["summarize"] {
		out[r.ID] = r.String("output")
	}
	return out
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	blob, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("read %s: %v", path, err)
	}
	n := 0
	for _, line := range strings.Split(string(blob), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

// distinctPIDs is how many processes actually called a model, read off the
// call log the mock appends its pid to.
func distinctPIDs(t *testing.T, path string) int {
	t.Helper()
	blob, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	seen := map[string]struct{}{}
	for _, line := range strings.Split(string(blob), "\n") {
		if i := strings.LastIndexByte(line, '|'); i >= 0 {
			seen[line[i+1:]] = struct{}{}
		}
	}
	return len(seen)
}
