package viz

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
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
	if a.Worker != "w1" || a.Model != "mock-fast" || a.Records != 2 {
		t.Errorf("task_a details = %+v", a)
	}
	if a.Usage.InputTokens != 100 || a.Usage.CostUSD != 0.002 || a.Calls != 2 {
		t.Errorf("task_a usage = %+v calls %d", a.Usage, a.Calls)
	}
	if len(a.OutputIDs) != 2 || len(a.InputIDs) != 2 {
		t.Errorf("task_a lineage = inputIds %v outputIds %v", a.InputIDs, a.OutputIDs)
	}
	if a.StartedAt != 1010 || a.EndedAt != 1090 || a.LatencyMS != 35 {
		t.Errorf("task_a timing = start %d end %d latency %d", a.StartedAt, a.EndedAt, a.LatencyMS)
	}
	if a.Error != "" {
		t.Errorf("completed task should clear error, got %q", a.Error)
	}
	// The heavy payloads deliberately do not ride the snapshot; they are
	// fetched per task. Their *existence* is advertised so the UI can show
	// sizes without paying for the bytes.
	if a.Input != "" || a.Output != "" || len(a.CallLog) != 0 || len(a.Log) != 0 {
		t.Error("snapshot carried heavy task payloads; they belong on /api/task")
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
	v.mu.Lock()
	got := len(v.taskIx["t1"].Log)
	v.mu.Unlock()
	if got != maxLogEntries {
		t.Fatalf("log length = %d, want %d", got, maxLogEntries)
	}
}

// TestSnapshotOmitsHeavyPayloads is the performance contract. A task's
// rendered prompts, responses, and record JSON can run to hundreds of
// kilobytes; multiplied by every task and re-sent on every event, that is
// what made large runs unusable. The wire form must stay small no matter how
// much detail a node accumulates.
func TestSnapshotOmitsHeavyPayloads(t *testing.T) {
	v := New()
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "r", Time: at(1)})

	huge := strings.Repeat("x", 40_000)
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("t%d", i)
		v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "r", Stage: "s",
			TaskID: id, Records: 1, Input: huge, Time: at(2)})
		v.Handle(observe.Event{Type: observe.ModelCalled, RunID: "r", Stage: "s",
			TaskID: id, Model: "m", Prompt: huge, Response: huge,
			Usage: core.Usage{Requests: 1}, Time: at(3)})
		v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "r", Stage: "s",
			TaskID: id, Output: huge, Time: at(4)})
	}

	v.mu.Lock()
	b := v.snapshotLocked()
	v.mu.Unlock()

	// 50 tasks × ~160KB of payload would be ~8MB if it all shipped.
	if len(b) > 100_000 {
		t.Errorf("snapshot = %d bytes; heavy payloads are riding the wire", len(b))
	}
	if strings.Contains(string(b), huge) {
		t.Error("snapshot contains a full payload verbatim")
	}

	// The sizes still travel, so the UI can describe what it has not fetched.
	var snap struct {
		Tasks []struct {
			ID         string `json:"id"`
			InputBytes int    `json:"inBytes"`
			CallCount  int    `json:"callN"`
			Rev        int    `json:"rev"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(b, &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Tasks) != 50 {
		t.Fatalf("tasks = %d, want 50", len(snap.Tasks))
	}
	if snap.Tasks[0].InputBytes != len(huge) || snap.Tasks[0].CallCount != 1 {
		t.Errorf("detail sizes missing: %+v", snap.Tasks[0])
	}
	if snap.Tasks[0].Rev == 0 {
		t.Error("rev should advance as detail accumulates, so clients can spot staleness")
	}
}

// TestServeTaskDetail covers the lazy half: the payloads the snapshot skipped
// must be retrievable for the one task a viewer opens.
func TestServeTaskDetail(t *testing.T) {
	v := New()
	feedLifecycle(v)
	srv := httptest.NewServer(v)
	defer srv.Close()

	res, err := http.Get(srv.URL + "/api/task?id=task_a")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("GET /api/task status %d", res.StatusCode)
	}
	var d TaskDetail
	if err := json.NewDecoder(res.Body).Decode(&d); err != nil {
		t.Fatal(err)
	}
	if d.ID != "task_a" || d.Input == "" || d.Output == "" {
		t.Errorf("detail = %+v", d)
	}
	if len(d.Calls) != 2 {
		t.Fatalf("call log = %+v", d.Calls)
	}
	if d.Calls[0].Err == "" || d.Calls[0].Prompt == "" {
		t.Errorf("first call should record the failed request: %+v", d.Calls[0])
	}
	if d.Calls[1].Response != `{"urgent":true}` || d.Calls[1].In != 100 {
		t.Errorf("second call = %+v", d.Calls[1])
	}
	if len(d.Log) == 0 {
		t.Error("detail should carry the event log")
	}

	missing, err := http.Get(srv.URL + "/api/task?id=nope")
	if err != nil {
		t.Fatal(err)
	}
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Errorf("unknown task status = %d, want 404", missing.StatusCode)
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

func TestMethodNotAllowed(t *testing.T) {
	v := New()
	srv := httptest.NewServer(v)
	defer srv.Close()

	for _, path := range []string{"/api/state", "/api/events"} {
		res, err := http.Post(srv.URL+path, "text/plain", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s status %d, want 405", path, res.StatusCode)
		}
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

// projectionEvents is what loom.Explain publishes: a forecast per stage, then
// the run-level total, all before the run itself starts.
func projectionEvents(v *Server) {
	v.Handle(observe.Event{Type: observe.StageProjected, Stage: "classify", Kind: "infer",
		Records: 3, Time: at(900),
		Usage:   core.Usage{InputTokens: 300, OutputTokens: 60, Requests: 3, CostUSD: 0.006},
		Ceiling: core.Usage{InputTokens: 300, OutputTokens: 300, Requests: 3, CostUSD: 0.020},
		Latency: 30 * time.Second})
	v.Handle(observe.Event{Type: observe.RunProjected, Kind: "barrier", Time: at(901),
		Usage:   core.Usage{InputTokens: 300, OutputTokens: 60, Requests: 3, CostUSD: 0.006},
		Ceiling: core.Usage{InputTokens: 300, OutputTokens: 300, Requests: 3, CostUSD: 0.020},
		Budget:  core.Budget{MaxCostUSD: 0.01},
		Latency: 30 * time.Second,
		Detail:  "stage \"load\" is a function"})
}

// TestProjectionBeforeRun checks a projection published ahead of the run lands
// on the stage it describes and on the run header.
func TestProjectionBeforeRun(t *testing.T) {
	v := New()
	projectionEvents(v)

	if v.projection == nil {
		t.Fatal("run-level projection was not recorded")
	}
	if got, want := v.projection.ExpectedUSD, 0.006; got != want {
		t.Errorf("expected cost = %v, want %v", got, want)
	}
	if got, want := v.projection.CeilingUSD, 0.020; got != want {
		t.Errorf("ceiling cost = %v, want %v", got, want)
	}
	// A $0.01 budget cannot cover a $0.02 ceiling.
	if v.projection.FitsBudget {
		t.Error("a budget below the ceiling was reported as covering it")
	}
	if len(v.projection.Warnings) != 1 {
		t.Errorf("warnings = %v, want the one that was published", v.projection.Warnings)
	}
	st := v.stageIx["classify"]
	if st == nil || st.Proj == nil {
		t.Fatal("stage projection did not reach the stage")
	}
	if st.Proj.Calls != 3 || st.Proj.FloorMS != 30_000 {
		t.Errorf("stage projection = %+v, want 3 calls and a 30s floor", st.Proj)
	}
}

// TestProjectionSurvivesRunReset is the property the whole feature rests on:
// the projection describes the pipeline, and the run that follows is the thing
// it predicted, so run.started must not throw the comparison away.
func TestProjectionSurvivesRunReset(t *testing.T) {
	v := New()
	projectionEvents(v)
	feedLifecycle(v)

	if v.projection == nil {
		t.Fatal("run.started discarded the projection")
	}
	st := v.stageIx["classify"]
	if st == nil || st.Proj == nil {
		t.Fatal("run.started discarded the stage's projection")
	}
	if st.Status == "" {
		t.Error("the stage lost its live status while regaining its projection")
	}

	var snap Snapshot
	if err := json.Unmarshal(v.snapshotLocked(), &snap); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Projection == nil {
		t.Error("snapshot omitted the projection, so a reconnecting viewer loses it")
	}
	for _, s := range snap.Stages {
		if s.ID == "classify" && s.Proj == nil {
			t.Error("snapshot omitted the stage's projection")
		}
	}
}

// TestNoProjectionIsAbsentFromSnapshot keeps a run without a projection
// rendering exactly as it did before the feature existed.
func TestNoProjectionIsAbsentFromSnapshot(t *testing.T) {
	v := New()
	feedLifecycle(v)
	blob := v.snapshotLocked()
	if strings.Contains(string(blob), `"projection"`) {
		t.Errorf("unprojected run carries a projection key:\n%s", blob)
	}
	if strings.Contains(string(blob), `"proj"`) {
		t.Errorf("unprojected stage carries a proj key:\n%s", blob)
	}
}
