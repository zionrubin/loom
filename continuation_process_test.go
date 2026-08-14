package loom_test

// The exit criterion for stateful delta execution, tested the only way it can
// honestly be tested: real worker processes, a session that grows a turn at a
// time, and the worker holding that session's state killed in the middle of it.
//
// The claim under test is not that this is fast. It is that speed and
// correctness are separable here — that state is what makes a round cheap and
// never what makes it right — so the measurement is:
//
//	rounds before the kill      served by the worker holding the session
//	the kill                    SIGKILL, mid-session, to that worker
//	rounds after the kill       served by a worker that has never seen it
//	every round's answer        identical to a single process doing the whole
//	                            session by itself, byte for byte
//
// The answers are digests of the full rendered prompt, so "identical answers"
// means "identical contexts" rather than "the mock is forgiving".

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/zionrubin/loom/delta"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/worker/filequeue"
)

// sessionChildEnv turns this test binary into a worker serving the session
// pipeline. Its own variable rather than childEnv's, because the two children
// serve different pipelines and a worker *is* its pipeline.
const sessionChildEnv = "LOOM_SESSION_WORKER_SPEC"

// digestMock answers with the digest of everything it was shown, and appends
// the worker's name and that digest to a shared file.
//
// Both halves are instruments. The digest makes an answer a statement about the
// exact bytes of the context, which is what has to survive a worker dying. The
// file is the only place the parent can learn which process served a round,
// because the parent makes no model calls and the workers publish to their own
// event buses.
func digestMock(reg *model.Registry, callLog, worker string) error {
	_, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			sum := sha256.Sum256([]byte(req.FullPrompt()))
			digest := hex.EncodeToString(sum[:])[:16]
			if callLog != "" {
				if f, err := os.OpenFile(callLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
					_, _ = f.WriteString(fmt.Sprintf("%s|%s|%d\n", worker, digest, req.Continuation.Stable))
					_ = f.Close()
				}
			}
			return digest, nil
		}))
	return err
}

// TestSessionWorkerProcess is the worker a spawned child runs.
func TestSessionWorkerProcess(t *testing.T) {
	blob := os.Getenv(sessionChildEnv)
	if blob == "" {
		t.Skip("not a session worker child")
	}
	var spec workerSpec
	if err := json.Unmarshal([]byte(blob), &spec); err != nil {
		t.Fatalf("spec: %v", err)
	}

	reg := model.NewRegistry()
	if err := digestMock(reg, spec.Calls, spec.Name); err != nil {
		t.Fatalf("registry: %v", err)
	}
	q, err := filequeue.Open(spec.Queue, filequeue.Options{LeaseTTL: spec.Lease})
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	defer q.Close()

	// The worker is handed the pipeline and no revision of the continuation it
	// declares. That is the whole point of the arrangement: revisions arrive on
	// envelopes, one per round, and a worker that had to be told one at startup
	// would be holding a fact that is stale before it claims anything.
	w, err := loom.NewWorker(sessionPipeline("session"),
		loom.WithRegistry(reg),
		loom.WithStateDir(spec.State),
		loom.WithWorkerService(q),
		loom.WithWorkerName(spec.Name),
		loom.WithWorkerLease(spec.Lease),
		loom.WithWorkers(spec.Slots),
		loom.WithDeltaPolicy(delta.Policy{Verify: 1}))
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	defer w.Close()

	if spec.Ready != "" {
		if err := os.WriteFile(spec.Ready, []byte(w.Name()), 0o644); err != nil {
			t.Fatalf("ready: %v", err)
		}
	}
	_ = w.Run(context.Background())
}

// TestSessionSurvivesLosingItsStateHolder is the exit criterion.
func TestSessionSurvivesLosingItsStateHolder(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns worker processes")
	}
	const (
		root   = 40 // turns the session already carries
		rounds = 8  // turns appended, one per round
		killAt = 4
	)
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	queueDir := filepath.Join(dir, "queue")
	calls := filepath.Join(dir, "calls.log")
	if err := os.MkdirAll(state, 0o755); err != nil {
		t.Fatal(err)
	}

	// The session, written once into shared storage. Nothing else about it
	// crosses the queue for the rest of the test.
	refs := writeSession(t, state, "session/a", root, rounds)
	if refs[len(refs)-1].Bytes < 100_000 {
		t.Fatalf("the session is only %d bytes; this proves nothing at that size",
			refs[len(refs)-1].Bytes)
	}

	q, err := filequeue.Open(queueDir, filequeue.Options{LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	spec := workerSpec{
		State: state, Queue: queueDir, Calls: calls,
		Lease: time.Second, Slots: 1,
	}
	workers := startSessionWorkers(t, spec, "alpha", "beta")

	// The parent's provider fails if it is ever called, so every digest below
	// was computed in another process.
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(model.Request) (string, error) {
			return "", fmt.Errorf("the client executed a task itself")
		})); err != nil {
		t.Fatal(err)
	}

	answers := make([]string, 0, len(refs))
	servers := make([]string, 0, len(refs))
	killed := ""
	for i, ref := range refs {
		res, err := loom.Run(context.Background(), sessionPipeline("session"),
			loom.WithRegistry(reg),
			loom.WithStateDir(state),
			loom.WithWorkerService(q),
			loom.WithWorkerWait(60*time.Second),
			loom.WithContinuation("session", ref),
			// Enough of a hold that the state-holder gets a chance to ask, and
			// short enough that losing it costs a blink.
			loom.WithAffinity(200*time.Millisecond))
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if len(res.Output) != 1 {
			t.Fatalf("round %d: %d records", i, len(res.Output))
		}
		answers = append(answers, res.Output[0].String("answer"))

		who, _ := lastCall(t, calls)
		servers = append(servers, who)

		if i == killAt-1 {
			// The worker that has been carrying this session dies holding its
			// state. Everything it knew about the rendering is gone; everything
			// that mattered is in the CAS.
			killed = who
			cmd, ok := workers[killed]
			if !ok {
				t.Fatalf("round %d was served by %q, which is not a worker this test started", i, killed)
			}
			if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
				t.Fatalf("kill %s: %v", killed, err)
			}
			_, _ = cmd.Process.Wait()
			delete(workers, killed)
		}
	}

	// What a single process, doing the whole session by itself, would have
	// said. It is the definition of a correct answer; the fleet is held to it.
	want := sessionBaseline(t, state, refs)
	for i := range refs {
		if answers[i] != want[i] {
			t.Fatalf("round %d: the fleet answered %q, a single process answers %q "+
				"— the context materialized differently", i, answers[i], want[i])
		}
	}

	// The queue's own accounting of where the work went. It is the only place
	// affinity is observable as a number rather than as a pattern in a log: the
	// client knows the task wanted a worker and the worker knows it held the
	// state, but only the queue saw the two meet.
	stats, err := q.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Local < 5 {
		t.Fatalf("%d of %d claims reached a worker holding the session's state",
			stats.Local, len(refs))
	}
	// And it never became a requirement: the first round, when nobody held the
	// state, and the round after the kill both went to a worker without it.
	if stats.Displaced < 2 {
		t.Fatalf("%d claims went to a worker without the state; the first round and "+
			"the round after the kill both had to", stats.Displaced)
	}

	// Affinity did its job before the kill: one worker carried the session.
	for i := 1; i < killAt; i++ {
		if servers[i] != servers[0] {
			t.Fatalf("round %d was served by %s, round 0 by %s — the session moved "+
				"while its state-holder was alive", i, servers[i], servers[0])
		}
	}
	// And the kill moved it, to a worker that had never seen it.
	if killed == "" {
		t.Fatal("no worker was killed")
	}
	for i := killAt; i < len(refs); i++ {
		if servers[i] == killed {
			t.Fatalf("round %d was served by the worker that was killed", i)
		}
	}
	if servers[killAt] == servers[killAt-1] {
		t.Fatal("the session did not move off the killed worker")
	}
}

// startSessionWorkers spawns worker children and waits for each to be serving.
func startSessionWorkers(t *testing.T, spec workerSpec, names ...string) map[string]*exec.Cmd {
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
		cmd := exec.Command(os.Args[0], "-test.run=^TestSessionWorkerProcess$", "-test.timeout=180s")
		cmd.Env = append(os.Environ(), sessionChildEnv+"="+string(blob))
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
		if !waitForFile(filepath.Join(filepath.Dir(spec.State), "ready-"+name), 60*time.Second) {
			t.Fatalf("%s never started serving", name)
		}
	}
	return out
}

// lastCall reads the most recent line of the shared call log: which worker made
// the call, and the digest it answered with.
func lastCall(t *testing.T, path string) (served, digest string) {
	t.Helper()
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("call log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(blob)), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatal("no model call was ever made")
	}
	parts := strings.Split(lines[len(lines)-1], "|")
	if len(parts) < 2 {
		t.Fatalf("malformed call log line %q", lines[len(lines)-1])
	}
	return parts[0], parts[1]
}

// sessionBaseline replays the session in this process, one round at a time,
// against a fresh result cache — no queue, no workers, and nothing carried
// between rounds.
//
// The cache has to be fresh, and that is the one subtle thing in this file. A
// baseline sharing the fleet's state directory would share its result cache,
// answer every round from what the fleet had already written, and agree with
// the fleet perfectly while testing nothing at all. What it copies is the CAS
// alone, which is where the chain lives — the fleet and the baseline read the
// same session and share nothing else.
func sessionBaseline(t *testing.T, state string, refs []delta.Ref) []string {
	t.Helper()
	fresh := t.TempDir()
	copyCAS(t, filepath.Join(state, "cas"), filepath.Join(fresh, "cas"))

	reg := model.NewRegistry()
	if err := digestMock(reg, "", "baseline"); err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(refs))
	for i, ref := range refs {
		res, err := loom.Run(context.Background(), sessionPipeline("session"),
			loom.WithRegistry(reg), loom.WithStateDir(fresh),
			loom.WithContinuation("session", ref))
		if err != nil {
			t.Fatalf("baseline round %d: %v", i, err)
		}
		if len(res.Output) != 1 {
			t.Fatalf("baseline round %d: %d records", i, len(res.Output))
		}
		out = append(out, res.Output[0].String("answer"))
	}
	return out
}

// copyCAS copies content-addressed blobs into a fresh store. Copying by name is
// safe precisely because the names are hashes: there is nothing to reconcile.
func copyCAS(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		blob, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), blob, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
