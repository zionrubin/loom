// Command worker-fleet runs one Loom pipeline across several *worker
// processes*, and kills one of them in the middle to show what that costs.
//
// It is the distributed half of every other example in this directory. Those
// run a pipeline in one process, where "the executor can run this task" is
// true by construction and a crash takes the whole run with it. This one puts
// the same pipeline behind a durable queue with leases: the parent plans the
// run and waits, worker processes claim tasks and execute them, and when one
// of them is killed with SIGKILL while it holds paid work, the task is
// redelivered and the run finishes with the answer it would have had anyway.
//
//	go run ./examples/worker-fleet                  # 3 workers, one killed mid-run
//	go run ./examples/worker-fleet -workers 5       # a bigger fleet
//	go run ./examples/worker-fleet -kill=false      # nobody dies
//	go run ./examples/worker-fleet -docs 40         # more work to spread
//
// The whole thing runs offline: mock models with realistic latency, a queue in
// a temporary directory, and shared state in another. Swapping the file queue
// for a real one is an import change — nothing in the client or the worker
// names a backend.
//
// What to look for:
//
//   - **the pipeline is unchanged**. buildPipeline is called by the parent to
//     plan the run and by every worker to have runners for the stages. Neither
//     mentions a queue, a lease, or a worker. That is the whole claim.
//   - **where the model calls happened**. The parent's registry holds a
//     provider that fails if it is ever called, so every token in the report
//     was spent in another process.
//   - **what the kill cost**. One or two redeliveries — exactly the tasks the
//     dead worker was holding — and no lost records. The killed worker's
//     partial work is wasted; nothing else is.
//   - **the answers**. Byte-identical to a single-process run of the same
//     pipeline, which the example computes alongside for comparison. A fleet
//     that produced different answers would not be a scaling story.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/worker"
	"github.com/zionrubin/loom/worker/filequeue"
)

func main() {
	var (
		asWorker = flag.Bool("worker", false, "run as one worker process (set by the parent)")
		name     = flag.String("name", "", "this worker's name")
		queueDir = flag.String("queue", "", "the shared queue directory")
		stateDir = flag.String("state", "", "the shared state directory (CAS + cache)")
		callLog  = flag.String("calls", "", "where the mock provider records its calls")
		workers  = flag.Int("workers", 3, "how many worker processes to run")
		docs     = flag.Int("docs", 24, "how many documents to process")
		slots    = flag.Int("slots", 2, "how many tasks each worker runs at once")
		latency  = flag.Duration("latency", 150*time.Millisecond, "how slow one model call is")
		lease    = flag.Duration("lease", time.Second, "how long a claim stands without a heartbeat")
		kill     = flag.Bool("kill", true, "kill a worker while it is holding work")
	)
	flag.Parse()

	cfg := config{
		Queue: *queueDir, State: *stateDir, Calls: *callLog, Name: *name,
		Docs: *docs, Slots: *slots, Latency: *latency, Lease: *lease,
	}
	if *asWorker {
		if err := serve(context.Background(), cfg); err != nil {
			log.Fatalf("worker %s: %v", cfg.Name, err)
		}
		return
	}
	if err := demo(cfg, *workers, *kill); err != nil {
		log.Fatal(err)
	}
}

// config is what the parent tells a worker, and what the parent keeps for
// itself. It is deliberately small: a worker needs to know which fleet to join
// and nothing about the work.
type config struct {
	Queue   string
	State   string
	Calls   string
	Name    string
	Docs    int
	Slots   int
	Latency time.Duration
	Lease   time.Duration
}

// --- the pipeline -------------------------------------------------------

// documents are the records the fleet processes.
func documents(n int) []core.Record {
	topics := []string{
		"the quarterly migration plan", "an incident review", "a vendor contract",
		"the onboarding checklist", "a capacity forecast", "a security exception",
	}
	recs := make([]core.Record, n)
	for i := range recs {
		recs[i] = core.NewRecord(fmt.Sprintf("doc%02d", i), map[string]any{
			"title": topics[i%len(topics)],
			"body": fmt.Sprintf("Document %d concerning %s. It runs to several "+
				"paragraphs and needs summarizing.", i, topics[i%len(topics)]),
		})
	}
	return recs
}

// buildPipeline is the program, and there is exactly one of it.
//
// The parent calls this to compile a plan and schedule the run. Every worker
// calls it to have runners for the stages — because an op is code, and a Go
// function cannot be put on a queue. A worker does not *receive* a pipeline;
// it is one. Nothing here knows that any of that is happening.
func buildPipeline(n int) *pipeline.Pipeline {
	p := pipeline.New("document-digest")
	p.FromRecords("documents", documents(n)).
		Map("prepare", func(r core.Record) (core.Record, error) {
			r.Data["words"] = len(strings.Fields(r.String("body")))
			return r, nil
		}).
		Infer("summarize", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			System:  "You summarize internal documents in one line.",
			Prompt:  "Summarize this document titled {{.title}}:\n\n{{.body}}",
		})
	return p
}

// fleetOptions are the options both sides pass. That the two processes are
// configured from one list rather than from two that can drift is what makes
// "the same pipeline" mean the same plan, the same fingerprints, and the same
// shared state.
func fleetOptions(cfg config, reg *model.Registry) []loom.Option {
	return []loom.Option{
		loom.WithRegistry(reg),
		loom.WithStateDir(cfg.State),
	}
}

// --- the worker process -------------------------------------------------

// serve runs this process as a worker until ctx ends or it is killed.
func serve(ctx context.Context, cfg config) error {
	reg := model.NewRegistry()
	if err := registerMock(reg, cfg.Calls, cfg.Latency); err != nil {
		return err
	}
	q, err := filequeue.Open(cfg.Queue, filequeue.Options{LeaseTTL: cfg.Lease})
	if err != nil {
		return err
	}
	defer q.Close()

	w, err := loom.NewWorker(buildPipeline(0), append(fleetOptions(cfg, reg),
		loom.WithWorkerService(q),
		loom.WithWorkerName(cfg.Name),
		loom.WithWorkerLease(cfg.Lease),
		loom.WithWorkers(cfg.Slots))...)
	if err != nil {
		return err
	}
	defer w.Close()

	// Announced once the worker is provisioned and about to claim, so the
	// parent's "the fleet is up" is a fact rather than a sleep.
	if err := os.WriteFile(readyFile(cfg.Queue, cfg.Name), []byte(w.Name()), 0o644); err != nil {
		return err
	}
	return w.Run(ctx)
}

// registerMock is the provider the workers run: deterministic output, a delay
// so a kill can land inside a call, and one line appended to a shared file per
// call.
//
// The file is the instrument. A counter inside a process cannot see the other
// processes, and the question this example asks — how much did the fleet
// actually spend — spans all of them.
func registerMock(reg *model.Registry, callLog string, latency time.Duration) error {
	_, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			// Logged as the call *starts*, and the delay taken afterwards. The
			// order is the measurement: a worker killed mid-call has already
			// spent the money, and a log that recorded only completed calls
			// would report the kill as free.
			if callLog != "" {
				if f, err := os.OpenFile(callLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
					// One short line, one write: an append this size is atomic,
					// so concurrent workers cannot interleave into each other.
					_, _ = fmt.Fprintf(f, "%d|%s\n", os.Getpid(), firstLine(req.Prompt))
					_ = f.Close()
				}
			}
			time.Sleep(latency)
			title := between(req.Prompt, "titled ", ":")
			return "One line on " + title + ".", nil
		}))
	return err
}

// --- the parent ---------------------------------------------------------

// result is what one run produced, however it was executed.
type result struct {
	answers map[string]string
	calls   int
	elapsed time.Duration
	spent   core.Usage
}

// demo runs the pipeline locally, then on a fleet, and compares them.
func demo(cfg config, workers int, kill bool) error {
	dir, err := os.MkdirTemp("", "worker-fleet-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	fmt.Printf("worker-fleet: %d documents, %d worker processes, %d slots each, "+
		"%s per model call\n\n", cfg.Docs, workers, cfg.Slots, cfg.Latency)

	// The baseline: the same pipeline, in this process, the way every other
	// example runs it.
	local, err := runLocal(cfg, filepath.Join(dir, "local"))
	if err != nil {
		return fmt.Errorf("local run: %w", err)
	}
	fmt.Printf("  local     %2d records  %2d model calls  %s\n",
		len(local.answers), local.calls, local.elapsed.Round(time.Millisecond))

	cfg.State = filepath.Join(dir, "state")
	cfg.Queue = filepath.Join(dir, "queue")
	cfg.Calls = filepath.Join(dir, "fleet-calls.log")
	if err := os.MkdirAll(cfg.State, 0o755); err != nil {
		return err
	}

	fleet, stats, killed, err := runFleet(cfg, workers, kill)
	if err != nil {
		return fmt.Errorf("fleet run: %w", err)
	}
	fmt.Printf("  fleet     %2d records  %2d model calls  %s   across %d processes\n\n",
		len(fleet.answers), fleet.calls, fleet.elapsed.Round(time.Millisecond),
		distinctPIDs(cfg.Calls))

	// The claim worth checking, checked rather than printed.
	if diff := compare(local.answers, fleet.answers); diff != "" {
		return fmt.Errorf("the fleet's answers differ from a local run's:\n%s", diff)
	}
	fmt.Println("  every record's answer is byte-identical to the local run's.")

	if killed != "" {
		fmt.Printf("  %s was killed with SIGKILL while it held work.\n", killed)
	}
	fmt.Printf("\n  queue: %d tasks, %d done, %d lease expiries, %d redundant executions\n",
		stats.Submitted, stats.Done, stats.Expired, stats.Duplicates)
	fmt.Printf("  the fleet spent %d requests / %d tokens; the client process spent none.\n",
		fleet.spent.Requests, fleet.spent.TotalTokens())

	switch extra := fleet.calls - local.calls; {
	case killed == "":
		fmt.Println("  an undisturbed fleet costs exactly what a local run costs.")
	case extra == 0:
		fmt.Println("  the kill cost nothing: the dead worker had claimed its tasks\n" +
			"  but had not started a call on any of them.")
	default:
		fmt.Printf("  the kill cost %d re-executed model call(s) — the calls the dead\n"+
			"  worker had started and not finished, and nothing else. That is the\n"+
			"  price of at-least-once delivery, and it is paid per redelivery\n"+
			"  rather than per task.\n", extra)
	}
	return nil
}

// runLocal executes the pipeline in this process: the definition of a correct
// answer that the fleet is held to.
func runLocal(cfg config, state string) (result, error) {
	reg := model.NewRegistry()
	calls := filepath.Join(state, "calls.log")
	if err := os.MkdirAll(state, 0o755); err != nil {
		return result{}, err
	}
	if err := registerMock(reg, calls, cfg.Latency); err != nil {
		return result{}, err
	}
	started := time.Now()
	res, err := loom.Run(context.Background(), buildPipeline(cfg.Docs),
		loom.WithRegistry(reg), loom.WithStateDir(state), loom.WithWorkers(8))
	if err != nil {
		return result{}, err
	}
	return result{
		answers: answers(res), calls: countLines(calls),
		elapsed: time.Since(started), spent: res.Spent,
	}, nil
}

// runFleet starts worker processes, drives the run through the queue, and —
// when asked — kills a worker that is demonstrably mid-execution.
func runFleet(cfg config, n int, kill bool) (result, worker.Stats, string, error) {
	procs, err := startWorkers(cfg, n)
	if err != nil {
		return result{}, worker.Stats{}, "", err
	}
	defer func() {
		for _, p := range procs {
			_ = p.Process.Signal(syscall.SIGKILL)
			_ = p.Wait()
		}
	}()

	// The client's registry knows the same models, because the scheduler
	// resolves the binding and applies its rate limits — but its provider is a
	// trap. If this process ever calls a model, execution did not leave it.
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(model.Request) (string, error) {
			return "", fmt.Errorf("the client process called a model")
		})); err != nil {
		return result{}, worker.Stats{}, "", err
	}

	q, err := filequeue.Open(cfg.Queue, filequeue.Options{LeaseTTL: cfg.Lease})
	if err != nil {
		return result{}, worker.Stats{}, "", err
	}
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	type outcome struct {
		res *loom.RunResult
		err error
	}
	done := make(chan outcome, 1)
	started := time.Now()
	go func() {
		res, err := loom.Run(ctx, buildPipeline(cfg.Docs),
			append(fleetOptions(cfg, reg), loom.WithWorkerService(q), loom.WithWorkers(8))...)
		done <- outcome{res, err}
	}()

	victim := ""
	if kill {
		// Aimed at the queue's own record of who holds what, because that is
		// the only thing in the system that knows a worker is part-way through
		// a paid call rather than between two of them.
		if name, task := awaitLease(q, "summarize", 60*time.Second); name != "" {
			fmt.Printf("  killing %s, which is holding %s...\n", name, task)
			if p, ok := procs[name]; ok {
				_ = p.Process.Signal(syscall.SIGKILL)
				victim = name
			}
		}
	}

	got := <-done
	if got.err != nil {
		return result{}, worker.Stats{}, victim, got.err
	}
	stats, err := q.Stats(ctx)
	if err != nil {
		return result{}, worker.Stats{}, victim, err
	}
	return result{
		answers: answers(got.res), calls: countLines(cfg.Calls),
		elapsed: time.Since(started), spent: got.res.Spent,
	}, stats, victim, nil
}

// startWorkers spawns worker processes and waits until every one is serving.
func startWorkers(cfg config, n int) (map[string]*exec.Cmd, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Queue, 0o755); err != nil {
		return nil, err
	}
	out := map[string]*exec.Cmd{}
	for i := 1; i <= n; i++ {
		name := fmt.Sprintf("worker-%d", i)
		cmd := exec.Command(self,
			"-worker", "-name", name,
			"-queue", cfg.Queue, "-state", cfg.State, "-calls", cfg.Calls,
			"-slots", fmt.Sprint(cfg.Slots),
			"-latency", cfg.Latency.String(), "-lease", cfg.Lease.String())
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return out, fmt.Errorf("start %s: %w", name, err)
		}
		out[name] = cmd
	}
	for name := range out {
		if !awaitFile(readyFile(cfg.Queue, name), 60*time.Second) {
			return out, fmt.Errorf("%s never started serving", name)
		}
	}
	return out, nil
}

// awaitLease blocks until some worker holds a task of the given stage.
//
// The stage matters: a pipeline's cheap stages are tasks too, and killing a
// worker during an instantaneous Map would demonstrate nothing.
func awaitLease(q *filequeue.Queue, stage string, within time.Duration) (string, string) {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		tasks, err := q.Tasks()
		if err == nil {
			for _, s := range tasks {
				if s.State == worker.StateLeased && s.Stage == stage && s.Lease.Worker != "" {
					return s.Lease.Worker, s.TaskID
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return "", ""
}

// --- small helpers ------------------------------------------------------

func readyFile(queueDir, name string) string {
	return filepath.Join(queueDir, "ready-"+name)
}

func awaitFile(path string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func answers(res *loom.RunResult) map[string]string {
	out := map[string]string{}
	for _, r := range res.StageOutputs["summarize"] {
		out[r.ID] = r.String("output")
	}
	return out
}

// compare reports the first way two runs disagree, or "" when they do not.
func compare(want, got map[string]string) string {
	var diffs []string
	for id, w := range want {
		if g, ok := got[id]; !ok {
			diffs = append(diffs, fmt.Sprintf("    %s: missing from the fleet's output", id))
		} else if g != w {
			diffs = append(diffs, fmt.Sprintf("    %s:\n      local %q\n      fleet %q", id, w, g))
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			diffs = append(diffs, fmt.Sprintf("    %s: the fleet invented a record", id))
		}
	}
	sort.Strings(diffs)
	return strings.Join(diffs, "\n")
}

func countLines(path string) int {
	blob, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(string(blob), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

// distinctPIDs is how many processes actually called a model, read off the pid
// the mock stamps on every line.
func distinctPIDs(path string) int {
	blob, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	seen := map[string]struct{}{}
	for _, line := range strings.Split(string(blob), "\n") {
		if i := strings.IndexByte(line, '|'); i > 0 {
			seen[line[:i]] = struct{}{}
		}
	}
	return len(seen)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func between(s, start, end string) string {
	i := strings.Index(s, start)
	if i < 0 {
		return "the document"
	}
	s = s[i+len(start):]
	if j := strings.Index(s, end); j >= 0 {
		return s[:j]
	}
	return s
}
