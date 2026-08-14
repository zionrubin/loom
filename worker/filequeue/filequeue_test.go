package filequeue_test

import (
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/task"
	"github.com/zionrubin/loom/worker"
	"github.com/zionrubin/loom/worker/filequeue"
	"github.com/zionrubin/loom/worker/queuetest"
)

// The same suite the in-memory queue passes, against a directory. Nothing in
// it knows which one it is running against, which is the whole point of having
// written it once.
func TestConformance(t *testing.T) {
	queuetest.Run(t, func(t *testing.T, ttl time.Duration) worker.Queue {
		q, err := filequeue.Open(t.TempDir(), filequeue.Options{
			LeaseTTL: ttl, Poll: 5 * time.Millisecond, PollCeiling: 20 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = q.Close() })
		return q
	})
}

// A second process opening the same directory sees the first one's work. This
// is the property the in-memory queue cannot have and the whole reason this
// backend exists — tested here at the level of two independent handles, and
// end-to-end with real processes in worker/process_test.go.
func TestTwoHandlesShareOneQueue(t *testing.T) {
	dir := t.TempDir()
	open := func() *filequeue.Queue {
		q, err := filequeue.Open(dir, filequeue.Options{LeaseTTL: queuetest.TTL})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = q.Close() })
		return q
	}
	client, workerSide := open(), open()

	tk := taskFixture("t1")
	if _, err := client.Submit(t.Context(), worker.Submission{Task: tk, Needs: worker.Require(tk)}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	got, err := workerSide.Claim(t.Context(), worker.Claim{
		Worker: "w1", Caps: worker.Capabilities{Worker: "w1", Wildcard: true}, Max: 1,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("a second handle saw %d tasks, want the one the first submitted", len(got))
	}
	if _, err := workerSide.Commit(t.Context(), got[0].Lease, worker.Receipt{
		TaskID: "t1", Output: "blob", Records: 1,
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	s, err := client.Await(t.Context(), "t1")
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if s.State != worker.StateDone || s.Receipt.Output != "blob" {
		t.Fatalf("the submitting handle saw %s/%+v", s.State, s.Receipt)
	}
}

func taskFixture(id string) task.Task {
	return task.Task{
		ID: id, Stage: "summarize",
		Input:    []core.Record{core.NewRecord("r1", map[string]any{"text": "hello"})},
		Envelope: task.Envelope{RunID: "run_1", Stage: "summarize", Sandbox: task.SandboxInline},
	}
}
