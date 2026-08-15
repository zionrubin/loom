// Command delta-session runs one long agent session across several worker
// processes, appending a turn at a time, and kills the worker holding the
// session's state in the middle of it.
//
// It is the counterpart to examples/worker-fleet. That one asks what a fleet
// costs when a worker dies mid-task; this one asks what a fleet costs when the
// thing a worker was holding is not a task but a *context* — a transcript that
// has been growing for twenty rounds and that every round needs all of.
//
//	go run ./examples/delta-session                 # 2 workers, one killed mid-session
//	go run ./examples/delta-session -rounds 20      # a longer session
//	go run ./examples/delta-session -turns 200      # a bigger context to carry
//	go run ./examples/delta-session -kill=false     # nobody dies
//	go run ./examples/delta-session -workers 4      # a bigger fleet
//
// Everything runs offline: mock models, a queue in a temporary directory,
// shared state in another.
//
// # What to look for
//
//   - **the envelope does not grow**. The context reaches a megabyte and the
//     task that references it stays a few hundred bytes, because what travels
//     is a revision hash. The report prints both.
//   - **the session stays put**. Rounds go to the worker that already holds
//     the state, because the queue prefers it — softly. The "worker" column
//     shows the same name until something happens to it.
//   - **stable bytes**. Each round tells its provider how many leading bytes
//     of the prompt are certified identical to the previous round's. It is the
//     retained region of a certified splice, not an estimate, and it is what a
//     KV cache would act on.
//   - **the kill**. The state-holder is sent SIGKILL. The next round lands on
//     a worker that has never seen this session, which rebuilds the whole
//     context from the chain — the slow path, taken automatically, reported as
//     `rebuild`, and costing exactly one round of latency.
//   - **the answers**. Byte-identical to a single process doing the whole
//     session by itself, which the example computes alongside for comparison.
//     Every answer is a digest of the full rendered prompt, so identical
//     answers mean identical contexts rather than a forgiving mock.
//
// The claim being demonstrated is not that this is fast. It is that fast and
// correct are separable: state makes a round cheap, and never makes it right.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/delta"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/worker/filequeue"
)

func main() {
	var (
		asWorker = flag.Bool("worker", false, "run as one worker process (set by the parent)")
		name     = flag.String("name", "", "this worker's name")
		queueDir = flag.String("queue", "", "the shared queue directory")
		stateDir = flag.String("state", "", "the shared state directory (CAS + cache)")
		callLog  = flag.String("calls", "", "where the mock provider records its calls")
		workers  = flag.Int("workers", 2, "how many worker processes to run")
		rounds   = flag.Int("rounds", 8, "how many turns the session takes")
		turns    = flag.Int("turns", 60, "how many turns the session already carries")
		latency  = flag.Duration("latency", 60*time.Millisecond, "how slow one model call is")
		lease    = flag.Duration("lease", time.Second, "how long a claim stands without a heartbeat")
		affinity = flag.Duration("affinity", 200*time.Millisecond,
			"how long the queue holds a round for the worker that has the state")
		kill = flag.Bool("kill", true, "kill the state-holding worker mid-session")
	)
	flag.Parse()

	cfg := config{
		Queue: *queueDir, State: *stateDir, Calls: *callLog, Name: *name,
		Latency: *latency, Lease: *lease,
	}
	if *asWorker {
		if err := serve(cfg); err != nil {
			log.Fatalf("worker %s: %v", cfg.Name, err)
		}
		return
	}
	if err := demo(cfg, *workers, *rounds, *turns, *affinity, *kill); err != nil {
		log.Fatal(err)
	}
}

// config is what the parent tells a worker. A worker needs to know which fleet
// to join and nothing at all about the session: revisions arrive on envelopes.
type config struct {
	Queue   string
	State   string
	Calls   string
	Name    string
	Latency time.Duration
	Lease   time.Duration
}

// --- the session --------------------------------------------------------

// sessionKey is the evolving object's identity: what the queue scores locality
// on, and what a worker advertises holding.
const sessionKey = "session/support-thread"

// transcript is the context the session already carries when the example
// starts: a support thread somebody has been working for a while.
func transcript(n int) []delta.Segment {
	speakers := []string{"customer", "agent", "system"}
	out := make([]delta.Segment, n)
	for i := range out {
		out[i] = delta.Segment{
			Name: fmt.Sprintf("%s-%03d", speakers[i%len(speakers)], i),
			Body: fmt.Sprintf("[turn %d] %s: %s", i, speakers[i%len(speakers)],
				strings.Repeat("The migration window, the failed webhook, and the retry policy. ", 24)),
		}
	}
	return out
}

// nextTurn is what each round appends: one message, about a kilobyte.
func nextTurn(round int) delta.Segment {
	return delta.Segment{
		Name: fmt.Sprintf("turn-%03d", round),
		Body: fmt.Sprintf("[round %d] customer: %s", round,
			strings.Repeat("One more detail about the webhook retries. ", 22)),
	}
}

// sessionPipeline is one stage that answers from the whole session. The only
// line that has anything to do with this example is WithContinuation.
func sessionPipeline() *pipeline.Pipeline {
	p := pipeline.New("support-session")
	src := p.FromRecords("ask", []core.Record{
		core.NewRecord("q", map[string]any{"q": "What should we tell the customer next?"}),
	})
	src.Infer("reply", pipeline.InferSpec{
		Prompt:      "Question: {{.q}}",
		System:      "You are a support agent working from the thread above.",
		Binding:     model.Binding{Tier: model.TierFast},
		OutputField: "reply",
		MaxTokens:   256,
	}, pipeline.WithContinuation("session"))
	return p
}

// --- the provider -------------------------------------------------------

// digestMock answers with a digest of everything it was shown, and records one
// line per call in a file every process appends to.
//
// The digest is what makes "the answers are identical" a statement about the
// context rather than about the mock. The line is how the parent — which makes
// no model calls at all — learns which worker served a round and how much of
// that round's prompt was certified unchanged since the last one.
func digestMock(reg *model.Registry, callLog, worker string, latency time.Duration) error {
	_, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			full := req.FullPrompt()
			sum := sha256.Sum256([]byte(full))
			digest := hex.EncodeToString(sum[:])[:12]
			if callLog != "" {
				if f, err := os.OpenFile(callLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
					fmt.Fprintf(f, "%s|%s|%d|%d\n", worker, digest, req.Continuation.Stable, len(full))
					_ = f.Close()
				}
			}
			time.Sleep(latency)
			return "reply(" + digest + ")", nil
		}))
	return err
}

// --- the worker side ----------------------------------------------------

func serve(cfg config) error {
	reg := model.NewRegistry()
	if err := digestMock(reg, cfg.Calls, cfg.Name, cfg.Latency); err != nil {
		return err
	}
	q, err := filequeue.Open(cfg.Queue, filequeue.Options{LeaseTTL: cfg.Lease})
	if err != nil {
		return err
	}
	defer q.Close()

	// The worker compiles the pipeline for its runners and is told no revision
	// of the continuation it declares. It never picks one — it is sent one per
	// round, on the envelope — so being told at startup would mean holding a
	// fact that is stale before the first claim.
	return loom.Serve(context.Background(), sessionPipeline(),
		loom.WithRegistry(reg),
		loom.WithStateDir(cfg.State),
		loom.WithWorkerService(q),
		loom.WithWorkerName(cfg.Name),
		loom.WithWorkerLease(cfg.Lease),
		loom.WithWorkers(1),
		// Every splice this worker accepts is also recomputed from scratch and
		// compared, which is not what a production deployment would pay and is
		// exactly what a demonstration should.
		loom.WithDeltaPolicy(delta.Policy{Verify: 1}))
}

// --- the parent ---------------------------------------------------------

// round is what the example learns about one round of the session.
type round struct {
	n        int
	appended int
	context  int
	worker   string
	stable   int
	prompt   int
	answer   string
}

// route names how the round's context was materialized, as read off the one
// number the parent can see.
func (r round) route() string {
	if r.stable > 0 {
		return "splice"
	}
	return "rebuild"
}

func demo(cfg config, workers, rounds, turns int, affinity time.Duration, kill bool) error {
	dir, err := os.MkdirTemp("", "loom-delta-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	state := filepath.Join(dir, "state")
	queueDir := filepath.Join(dir, "queue")
	calls := filepath.Join(dir, "calls.log")
	if err := os.MkdirAll(state, 0o755); err != nil {
		return err
	}

	// The session, written into shared storage as an immutable chain. This is
	// the only place the transcript exists; everything after this refers to it.
	cas, err := store.NewCAS(filepath.Join(state, "cas"))
	if err != nil {
		return err
	}
	chain, err := delta.NewChain(cas, delta.Tags{}, sessionKey)
	if err != nil {
		return err
	}
	ref, err := chain.Root(transcript(turns)...)
	if err != nil {
		return err
	}

	q, err := filequeue.Open(queueDir, filequeue.Options{LeaseTTL: cfg.Lease})
	if err != nil {
		return err
	}
	defer q.Close()

	spec := config{Queue: queueDir, State: state, Calls: calls, Latency: cfg.Latency, Lease: cfg.Lease}
	names := make([]string, workers)
	for i := range names {
		names[i] = fmt.Sprintf("w%d", i+1)
	}
	procs, err := startWorkers(dir, spec, names)
	if err != nil {
		return err
	}
	defer stopAll(procs)

	fmt.Printf("session %s: %d turns, %s in shared storage\n",
		sessionKey, ref.Segments, bytes(ref.Bytes))
	fmt.Printf("fleet: %d worker processes, lease %s, affinity grace %s\n\n",
		workers, cfg.Lease, affinity)

	// A registry whose provider fails if it is ever called: proof that every
	// digest below was computed in another process.
	parent := model.NewRegistry()
	if _, err := model.RegisterMock(parent, "mock-fast", model.TierFast,
		model.WithHandler(func(model.Request) (string, error) {
			return "", fmt.Errorf("the client executed a task itself")
		})); err != nil {
		return err
	}

	killAt := rounds/2 + 1
	killed := ""
	var log []round
	var refs []delta.Ref

	for n := 1; n <= rounds; n++ {
		turn := nextTurn(n)
		before := ref.Bytes
		if ref, err = chain.Append(ref, turn); err != nil {
			return err
		}
		refs = append(refs, ref)

		res, err := loom.Run(context.Background(), sessionPipeline(),
			loom.WithRegistry(parent),
			loom.WithStateDir(state),
			loom.WithWorkerService(q),
			loom.WithWorkerWait(60*time.Second),
			loom.WithContinuation("session", ref),
			loom.WithAffinity(affinity))
		if err != nil {
			return fmt.Errorf("round %d: %w", n, err)
		}

		who, stable, prompt := lastCall(calls)
		log = append(log, round{
			n: n, appended: ref.Bytes - before, context: ref.Bytes,
			worker: who, stable: stable, prompt: prompt,
			answer: res.Output[0].String("reply"),
		})

		if kill && n == killAt {
			if cmd, ok := procs[who]; ok {
				killed = who
				_ = cmd.Process.Signal(syscall.SIGKILL)
				_, _ = cmd.Process.Wait()
				delete(procs, who)
			}
		}
	}

	report(log, killed, killAt, ref)
	return compare(state, refs, log)
}

// report prints what happened, round by round.
func report(log []round, killed string, killAt int, final delta.Ref) {
	envelope := envelopeBytes(final)

	fmt.Println("round   appended    context   worker   route     stable     prompt   answer")
	fmt.Println("─────   ────────   ────────   ──────   ───────   ──────   ────────   ────────────────")
	for _, r := range log {
		fmt.Printf("%5d   %8s   %8s   %-6s   %-7s   %5.1f%%   %8s   %s\n",
			r.n, bytes(r.appended), bytes(r.context), r.worker, r.route(),
			100*float64(r.stable)/float64(max(r.prompt, 1)), bytes(r.prompt), r.answer)
		if killed != "" && r.n == killAt {
			fmt.Printf("        ── %s killed (SIGKILL) while holding this session's state ──\n", killed)
		}
	}

	fmt.Printf("\nwhat crossed the queue, per round\n")
	fmt.Printf("  envelope referencing the session   %8s\n", bytes(envelope))
	fmt.Printf("  the session itself                 %8s\n", bytes(final.Bytes))
	fmt.Printf("  ratio                              %8.0f×\n",
		float64(final.Bytes)/float64(max(envelope, 1)))

	var spliced, rebuilt, saved int
	for _, r := range log {
		if r.stable > 0 {
			spliced++
			saved += r.stable
		} else {
			rebuilt++
		}
	}
	fmt.Printf("\nhow the context was materialized\n")
	fmt.Printf("  spliced onto state already held    %8d rounds\n", spliced)
	fmt.Printf("  rendered in full                   %8d rounds\n", rebuilt)
	fmt.Printf("  bytes certified unchanged          %8s\n", bytes(saved))
}

// compare replays the whole session in this process, against a fresh result
// cache, and checks every answer.
//
// The cache has to be fresh or this proves nothing: a baseline sharing the
// fleet's state directory would answer from what the fleet already wrote and
// agree with it perfectly. What it copies is the chain, which is the one thing
// the two are supposed to share.
func compare(state string, refs []delta.Ref, log []round) error {
	fresh, err := os.MkdirTemp("", "loom-delta-baseline-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(fresh)
	if err := copyCAS(filepath.Join(state, "cas"), filepath.Join(fresh, "cas")); err != nil {
		return err
	}

	reg := model.NewRegistry()
	if err := digestMock(reg, "", "baseline", 0); err != nil {
		return err
	}
	for i, ref := range refs {
		res, err := loom.Run(context.Background(), sessionPipeline(),
			loom.WithRegistry(reg), loom.WithStateDir(fresh),
			loom.WithContinuation("session", ref))
		if err != nil {
			return fmt.Errorf("baseline round %d: %w", i+1, err)
		}
		if got, want := res.Output[0].String("reply"), log[i].answer; got != want {
			return fmt.Errorf("round %d: the fleet answered %s, one process answers %s "+
				"— the context materialized differently", i+1, want, got)
		}
	}
	fmt.Printf("\nevery round matches a single process doing the whole session by itself.\n")
	return nil
}

// --- plumbing -----------------------------------------------------------

func startWorkers(dir string, spec config, names []string) (map[string]*exec.Cmd, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	out := map[string]*exec.Cmd{}
	for _, name := range names {
		cmd := exec.Command(self,
			"-worker", "-name", name,
			"-queue", spec.Queue, "-state", spec.State, "-calls", spec.Calls,
			"-latency", spec.Latency.String(), "-lease", spec.Lease.String())
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			stopAll(out)
			return nil, fmt.Errorf("start %s: %w", name, err)
		}
		out[name] = cmd
	}
	// A worker announces nothing here; the first round's wait is what proves
	// they came up, and the client's bounded wait is what says so if they did
	// not.
	time.Sleep(300 * time.Millisecond)
	return out, nil
}

func stopAll(procs map[string]*exec.Cmd) {
	for _, cmd := range procs {
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	}
}

// callLines reads the shared call log: one line per model call, from whichever
// process made it.
func callLines(path string) []string {
	blob, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(string(blob), "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

// parseCall reads one line of it: who made the call, how many leading bytes of
// its prompt were certified unchanged since the previous revision, and how long
// the prompt was.
func parseCall(line string) (worker string, stable, prompt int) {
	parts := strings.Split(line, "|")
	if len(parts) < 4 {
		return "?", 0, 0
	}
	stable, _ = strconv.Atoi(parts[2])
	prompt, _ = strconv.Atoi(parts[3])
	return parts[0], stable, prompt
}

// lastCall is the most recent one.
func lastCall(path string) (worker string, stable, prompt int) {
	lines := callLines(path)
	if len(lines) == 0 {
		return "?", 0, 0
	}
	return parseCall(lines[len(lines)-1])
}

// envelopeBytes is what one task actually carries for its context: the
// serialized reference, and nothing that grows with the session.
func envelopeBytes(ref delta.Ref) int {
	blob, _ := json.Marshal(ref)
	return len(blob)
}

func copyCAS(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		blob, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), blob, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func bytes(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f kB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
