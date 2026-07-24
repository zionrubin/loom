package viz

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zionrubin/brian-ai/loom/core"
	"github.com/zionrubin/brian-ai/loom/observe"
)

func at(ms int64) time.Time { return time.UnixMilli(ms) }

func feedLifecycle(v *Server) {
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Time: at(1000)})
	v.Handle(observe.Event{Type: observe.StageStarted, RunID: "run_1", Stage: "classify",
		Kind: "infer", Detail: "per-record inference · tier \"fast\"", Time: at(1001)})
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "run_1", Stage: "classify",
		TaskID: "task_a", Records: 2, Input: `[{"id":"r1","data":{"subject":"hi"}}]`,
		InputIDs: []string{"r1", "r2"}, Time: at(1002)})
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "run_1", Stage: "classify",
		TaskID: "task_b", Records: 1, Time: at(1002)})

	// task_a: fails once, retries, succeeds on w1.
	v.Handle(observe.Event{Type: observe.TaskStarted, RunID: "run_1", Stage: "classify",
		TaskID: "task_a", Worker: "w1", Attempt: 1, Model: "mock-fast", Time: at(1010)})
	v.Handle(observe.Event{Type: observe.ModelCalled, RunID: "run_1", Stage: "classify",
		TaskID: "task_a", Model: "mock-fast", Err: "rate limited",
		Prompt: "Classify: hi", Latency: 5 * time.Millisecond, Time: at(1015)})
	v.Handle(observe.Event{Type: observe.TaskRetried, RunID: "run_1", Stage: "classify",
		TaskID: "task_a", Worker: "w1", Attempt: 1, Err: "rate limited", Note: "transient", Time: at(1016)})
	v.Handle(observe.Event{Type: observe.TaskStarted, RunID: "run_1", Stage: "classify",
		TaskID: "task_a", Worker: "w1", Attempt: 2, Model: "mock-fast", Time: at(1050)})
	v.Handle(observe.Event{Type: observe.ModelCalled, RunID: "run_1", Stage: "classify",
		TaskID: "task_a", Model: "mock-fast",
		Prompt: "Classify: hi", Response: `{"urgent":true}`,
		Usage:   core.Usage{InputTokens: 100, OutputTokens: 20, Requests: 1, CostUSD: 0.002},
		Latency: 30 * time.Millisecond, Time: at(1080)})
	v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "run_1", Stage: "classify",
		TaskID: "task_a", Worker: "w1", Attempt: 2, Model: "mock-fast",
		Output: `[{"id":"r1","data":{"urgent":true}}]`, OutIDs: []string{"r1", "r2"},
		Usage:   core.Usage{InputTokens: 100, OutputTokens: 20, Requests: 1, CostUSD: 0.002},
		Latency: 35 * time.Millisecond, Time: at(1090)})

	// task_b: permanent failure on w2.
	v.Handle(observe.Event{Type: observe.TaskStarted, RunID: "run_1", Stage: "classify",
		TaskID: "task_b", Worker: "w2", Attempt: 1, Model: "mock-fast", Time: at(1010)})
	v.Handle(observe.Event{Type: observe.TaskFailed, RunID: "run_1", Stage: "classify",
		TaskID: "task_b", Worker: "w2", Attempt: 1, Err: "permanent: boom", Time: at(1030)})

	v.Handle(observe.Event{Type: observe.StageFinished, RunID: "run_1", Stage: "classify", Time: at(1100)})
	v.Handle(observe.Event{Type: observe.RunFinished, RunID: "run_1", Time: at(1101)})
}

func snapshot(t *testing.T, v *Server) Snapshot {
	t.Helper()
	v.mu.Lock()
	b := v.snapshotLocked()
	v.mu.Unlock()
	var snap Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		t.Fatalf("snapshot unmarshal: %v", err)
	}
	return snap
}

func TestStateMachine(t *testing.T) {
	v := New()
	feedLifecycle(v)
	snap := snapshot(t, v)

	if snap.RunID != "run_1" || !snap.Done {
		t.Fatalf("run header = %+v", snap.runHeader)
	}
	if len(snap.Stages) != 1 || snap.Stages[0].Status != "done" {
		t.Fatalf("stages = %+v", snap.Stages)
	}
	if snap.Stages[0].Kind != "infer" || snap.Stages[0].Detail == "" {
		t.Errorf("stage spec = kind %q detail %q", snap.Stages[0].Kind, snap.Stages[0].Detail)
	}
	if len(snap.Tasks) != 2 {
		t.Fatalf("want 2 tasks, got %d", len(snap.Tasks))
	}

	var a, b *Node
	for _, n := range snap.Tasks {
		switch n.ID {
		case "task_a":
			a = n
		case "task_b":
			b = n
		}
	}
	if a == nil || b == nil {
		t.Fatal("missing tasks in snapshot")
	}
	if a.Status != "completed" || a.Retries != 1 || a.Attempts != 2 {
		t.Errorf("task_a = status %q retries %d attempts %d", a.Status, a.Retries, a.Attempts)
	}
	if a.Worker != "w1" || a.Model != "mock-fast" || a.Records != 2 || a.Input == "" {
		t.Errorf("task_a details = %+v", a)
	}
	if a.Usage.InputTokens != 100 || a.Usage.CostUSD != 0.002 || a.Calls != 2 {
		t.Errorf("task_a usage = %+v calls %d", a.Usage, a.Calls)
	}
	if len(a.CallLog) != 2 {
		t.Fatalf("task_a call log = %+v", a.CallLog)
	}
	if a.CallLog[0].Err == "" || a.CallLog[0].Prompt == "" {
		t.Errorf("first call should record the failed request: %+v", a.CallLog[0])
	}
	if a.CallLog[1].Response != `{"urgent":true}` || a.CallLog[1].In != 100 {
		t.Errorf("second call = %+v", a.CallLog[1])
	}
	if a.Output == "" || len(a.OutputIDs) != 2 || len(a.InputIDs) != 2 {
		t.Errorf("task_a payloads = output %q inputIds %v outputIds %v", a.Output, a.InputIDs, a.OutputIDs)
	}
	if a.StartedAt != 1010 || a.EndedAt != 1090 || a.LatencyMS != 35 {
		t.Errorf("task_a timing = start %d end %d latency %d", a.StartedAt, a.EndedAt, a.LatencyMS)
	}
	if a.Error != "" {
		t.Errorf("completed task should clear error, got %q", a.Error)
	}
	if len(a.Log) == 0 {
		t.Error("task_a should have a log")
	}
	if b.Status != "failed" || b.Error != "permanent: boom" {
		t.Errorf("task_b = status %q err %q", b.Status, b.Error)
	}

	if len(snap.Workers) != 2 {
		t.Fatalf("want 2 workers, got %d", len(snap.Workers))
	}
	for _, w := range snap.Workers {
		if w.Current != "" {
			t.Errorf("worker %s should be idle, current=%q", w.ID, w.Current)
		}
		switch w.ID {
		case "w1":
			if w.Done != 1 || w.Failed != 0 || w.BusyMS != 80 {
				t.Errorf("w1 = %+v", w)
			}
		case "w2":
			if w.Done != 0 || w.Failed != 1 {
				t.Errorf("w2 = %+v", w)
			}
		}
	}
}

func TestStageUpstream(t *testing.T) {
	v := New()
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "r", Time: at(1)})
	v.Handle(observe.Event{Type: observe.StageStarted, RunID: "r", Stage: "a", Time: at(2)})
	v.Handle(observe.Event{Type: observe.StageStarted, RunID: "r", Stage: "b", Upstream: "a", Time: at(3)})
	snap := snapshot(t, v)
	if len(snap.Stages) != 2 || snap.Stages[1].Upstream != "a" {
		t.Fatalf("stages = %+v", snap.Stages)
	}
}

func TestResetOnNewRun(t *testing.T) {
	v := New()
	feedLifecycle(v)
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_2", Time: at(5000)})
	snap := snapshot(t, v)
	if snap.RunID != "run_2" || snap.Done || len(snap.Tasks) != 0 || len(snap.Stages) != 0 {
		t.Fatalf("state not reset: %+v", snap)
	}
}

func TestLogCap(t *testing.T) {
	v := New()
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "r", Time: at(1)})
	for i := 0; i < maxLogEntries+40; i++ {
		v.Handle(observe.Event{Type: observe.ModelCalled, RunID: "r", Stage: "s",
			TaskID: "t1", Model: "m", Usage: core.Usage{Requests: 1}, Time: at(int64(i + 2))})
	}
	snap := snapshot(t, v)
	if got := len(snap.Tasks[0].Log); got != maxLogEntries {
		t.Fatalf("log length = %d, want %d", got, maxLogEntries)
	}
}

func TestServesUIAndState(t *testing.T) {
	v := New()
	feedLifecycle(v)
	srv := httptest.NewServer(v)
	defer srv.Close()

	res, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || !strings.Contains(string(page), "constellation") {
		t.Fatalf("GET / status %d", res.StatusCode)
	}

	res, err = http.Get(srv.URL + "/api/state")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var snap Snapshot
	if err := json.NewDecoder(res.Body).Decode(&snap); err != nil {
		t.Fatalf("state decode: %v", err)
	}
	if snap.RunID != "run_1" || len(snap.Tasks) != 2 {
		t.Fatalf("state = %+v", snap)
	}

	res, err = http.Get(srv.URL + "/nope")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatalf("GET /nope status %d, want 404", res.StatusCode)
	}
}

func TestEventStream(t *testing.T) {
	v := New()
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_sse", Time: at(1)})

	srv := httptest.NewServer(v)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/api/events", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q", ct)
	}

	// AwaitViewer should have been released by the connection.
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer awaitCancel()
	if !v.AwaitViewer(awaitCtx) {
		t.Fatal("AwaitViewer did not observe the connected client")
	}

	sc := bufio.NewScanner(res.Body)
	readFrame := func() (event, data string) {
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "event: ") {
				event = strings.TrimPrefix(line, "event: ")
			} else if strings.HasPrefix(line, "data: ") {
				data = strings.TrimPrefix(line, "data: ")
			} else if line == "" && event != "" {
				return event, data
			}
		}
		t.Fatalf("stream ended early: %v", sc.Err())
		return "", ""
	}

	ev, data := readFrame()
	if ev != "snapshot" || !strings.Contains(data, "run_sse") {
		t.Fatalf("first frame = %q %q", ev, data)
	}

	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "run_sse", Stage: "s",
		TaskID: "t1", Records: 1, Time: at(2)})

	ev, data = readFrame()
	if ev != "delta" || !strings.Contains(data, "t1") {
		t.Fatalf("delta frame = %q %q", ev, data)
	}
	var d struct {
		Task *Node `json:"task"`
	}
	if err := json.Unmarshal([]byte(data), &d); err != nil || d.Task == nil || d.Task.Status != "pending" {
		t.Fatalf("delta payload = %q (%v)", data, err)
	}
}

func TestStartAndClose(t *testing.T) {
	v := New()
	url, err := v.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	res, err := http.Get(url + "/api/state")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("status %d", res.StatusCode)
	}
}
