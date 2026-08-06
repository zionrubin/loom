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
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Pipeline: "triage", Time: at(1000)})
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
	return snapshotOf(t, v, nil)
}

// snapshotOf serializes one run of the universe; a nil run means the live one.
func snapshotOf(t *testing.T, v *Server, r *runState) Snapshot {
	t.Helper()
	v.mu.Lock()
	if r == nil {
		r = v.cur
	}
	b := v.snapshotLocked(r)
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

// TestNewRunOpensItsOwnSky covers the half of the universe that always
// worked: a new run is a clean sky, not the previous run with more stars in
// it.
func TestNewRunOpensItsOwnSky(t *testing.T) {
	v := New()
	feedLifecycle(v)
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_2", Time: at(5000)})
	snap := snapshot(t, v)
	if snap.RunID != "run_2" || snap.Done || len(snap.Tasks) != 0 || len(snap.Stages) != 0 {
		t.Fatalf("new run did not start clean: %+v", snap)
	}
}

// TestUniverseRetainsFinishedRuns is the point of the universe: a pipeline
// that finishes while the next one starts stays watchable. Before this, the
// second run.started erased the first run and there was no way back to it.
func TestUniverseRetainsFinishedRuns(t *testing.T) {
	v := New()
	feedLifecycle(v) // run_1, pipeline "triage", 2 tasks, finished
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_2", Pipeline: "overview", Time: at(5000)})
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "run_2", Stage: "fuse",
		TaskID: "task_c", Records: 2, Time: at(5001)})

	live := snapshot(t, v)
	if live.RunID != "run_2" || live.Pipeline != "overview" || live.Index != 2 {
		t.Fatalf("live run header = %+v", live.runHeader)
	}
	if live.Live != "run_2" {
		t.Errorf("live run = %q, want run_2", live.Live)
	}
	if len(live.Runs) != 2 {
		t.Fatalf("roster = %d runs, want both", len(live.Runs))
	}

	first, second := live.Runs[0], live.Runs[1]
	if first.RunID != "run_1" || first.Pipeline != "triage" || first.Index != 1 {
		t.Errorf("first run in roster = %+v", first.runHeader)
	}
	if !first.Done {
		t.Error("the finished run lost its completion in the roster")
	}
	if first.Tasks != 2 || first.Completed != 1 || first.Failed != 1 || first.Retries != 1 {
		t.Errorf("run_1 tallies = %+v", first)
	}
	if first.CostUSD != 0.002 || first.Tokens != 120 {
		t.Errorf("run_1 economics = cost %v tokens %d", first.CostUSD, first.Tokens)
	}
	if len(first.Stages) != 1 || first.Stages[0].ID != "classify" || first.Stages[0].Tasks != 2 {
		t.Errorf("run_1 stage briefs = %+v", first.Stages)
	}
	if first.Stages[0].Done != 1 || first.Stages[0].Failed != 1 {
		t.Errorf("run_1 stage outcome = %+v", first.Stages[0])
	}
	if second.Done || second.Tasks != 1 {
		t.Errorf("run_2 summary = %+v", second)
	}

	// And the whole first run is still there to walk back into.
	v.mu.Lock()
	prev := v.runIx["run_1"]
	v.mu.Unlock()
	if prev == nil {
		t.Fatal("the finished run was dropped from the universe")
	}
	old := snapshotOf(t, v, prev)
	if old.RunID != "run_1" || len(old.Tasks) != 2 || len(old.Stages) != 1 {
		t.Fatalf("previous run no longer inspectable: %+v", old.runHeader)
	}
	if old.Live != "run_2" {
		t.Error("a past run's snapshot should still name the run that is live")
	}
}

// TestConcurrentRunsStayApart: two pipelines sharing one handler used to
// interleave into a single unreadable sky. Events carry a run ID; each run
// gets its own.
func TestConcurrentRunsStayApart(t *testing.T) {
	v := New()
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "a", Pipeline: "left", Time: at(1)})
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "b", Pipeline: "right", Time: at(2)})
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "a", Stage: "sa", TaskID: "ta", Time: at(3)})
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "b", Stage: "sb", TaskID: "tb", Time: at(4)})
	v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "a", Stage: "sa", TaskID: "ta", Time: at(5)})

	v.mu.Lock()
	ra, rb := v.runIx["a"], v.runIx["b"]
	v.mu.Unlock()
	if ra == nil || rb == nil {
		t.Fatal("both runs should be held")
	}
	if len(ra.tasks) != 1 || ra.tasks[0].ID != "ta" || ra.tasks[0].Status != "completed" {
		t.Errorf("run a tasks = %+v", ra.tasks)
	}
	if len(rb.tasks) != 1 || rb.tasks[0].ID != "tb" || rb.tasks[0].Status != "pending" {
		t.Errorf("run b tasks = %+v", rb.tasks)
	}
}

// TestRetainDropsOldestRuns bounds the universe: runs are held whole, so the
// history has to end somewhere, and it ends at the oldest run.
func TestRetainDropsOldestRuns(t *testing.T) {
	v := New(Retain(2))
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("run_%d", i)
		v.Handle(observe.Event{Type: observe.RunStarted, RunID: id, Time: at(int64(i))})
		v.Handle(observe.Event{Type: observe.RunFinished, RunID: id, Time: at(int64(i) + 1)})
	}
	snap := snapshot(t, v)
	if len(snap.Runs) != 2 {
		t.Fatalf("roster = %d runs, want 2 retained", len(snap.Runs))
	}
	if snap.Runs[0].RunID != "run_3" || snap.Runs[1].RunID != "run_4" {
		t.Errorf("retained the wrong runs: %q, %q", snap.Runs[0].RunID, snap.Runs[1].RunID)
	}
	if snap.Runs[1].Index != 4 {
		t.Errorf("index should count every run seen, got %d", snap.Runs[1].Index)
	}
	v.mu.Lock()
	_, gone := v.runIx["run_1"]
	v.mu.Unlock()
	if gone {
		t.Error("evicted run still indexed")
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
	got := len(v.cur.taskIx["t1"].Log)
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
	b := v.snapshotLocked(v.cur)
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

// TestServesPastRunAndRoster is the HTTP half of the universe: the run that
// finished has to be reachable while another one is live.
func TestServesPastRunAndRoster(t *testing.T) {
	v := New()
	feedLifecycle(v)
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_2", Pipeline: "overview", Time: at(5000)})
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "run_2", Stage: "fuse",
		TaskID: "task_c", Records: 2, Input: `[{"id":"r9"}]`, Time: at(5001)})

	srv := httptest.NewServer(v)
	defer srv.Close()

	get := func(path string, into any) int {
		t.Helper()
		res, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode == 200 && into != nil {
			if err := json.NewDecoder(res.Body).Decode(into); err != nil {
				t.Fatalf("decode %s: %v", path, err)
			}
		}
		return res.StatusCode
	}

	var live Snapshot
	if code := get("/api/state", &live); code != 200 || live.RunID != "run_2" {
		t.Fatalf("/api/state = %d, run %q; want the live run", code, live.RunID)
	}

	var past Snapshot
	if code := get("/api/state?run=run_1", &past); code != 200 {
		t.Fatalf("/api/state?run=run_1 = %d", code)
	}
	if past.RunID != "run_1" || len(past.Tasks) != 2 || past.Pipeline != "triage" {
		t.Errorf("past run = %+v with %d tasks", past.runHeader, len(past.Tasks))
	}
	if past.Live != "run_2" {
		t.Errorf("past snapshot should name the live run, got %q", past.Live)
	}
	if code := get("/api/state?run=nope", nil); code != http.StatusNotFound {
		t.Errorf("unknown run = %d, want 404", code)
	}

	var roster struct {
		Live string        `json:"live"`
		Runs []*RunSummary `json:"runs"`
	}
	if code := get("/api/runs", &roster); code != 200 {
		t.Fatalf("/api/runs = %d", code)
	}
	if roster.Live != "run_2" || len(roster.Runs) != 2 {
		t.Fatalf("roster = live %q, %d runs", roster.Live, len(roster.Runs))
	}
	if len(roster.Runs[0].Stages) == 0 {
		t.Error("roster runs should carry their stage shape")
	}

	// A task is addressable in its own run, and only there.
	var d TaskDetail
	if code := get("/api/task?id=task_a&run=run_1", &d); code != 200 || d.Input == "" {
		t.Errorf("/api/task in run_1 = %d, detail %+v", code, d)
	}
	if code := get("/api/task?id=task_a&run=run_2", nil); code != http.StatusNotFound {
		t.Errorf("task from another run = %d, want 404", code)
	}
	// Without a run it still resolves, so an old link keeps working.
	if code := get("/api/task?id=task_a", nil); code != 200 {
		t.Errorf("unscoped task lookup = %d", code)
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
		Task    *Node       `json:"task"`
		RunID   string      `json:"runId"`
		Summary *RunSummary `json:"summary"`
	}
	if err := json.Unmarshal([]byte(data), &d); err != nil || d.Task == nil || d.Task.Status != "pending" {
		t.Fatalf("delta payload = %q (%v)", data, err)
	}
	// A delta names its run, so a viewer watching a different one knows to
	// keep its own sky and just update the overview.
	if d.RunID != "run_sse" {
		t.Errorf("delta run = %q, want run_sse", d.RunID)
	}
	if d.Summary == nil || d.Summary.Tasks != 1 || d.Summary.RunID != "run_sse" {
		t.Errorf("delta summary = %+v", d.Summary)
	}
}

// TestDeltaSummaryStaysLean keeps the hot path cheap: the overview's stage
// shape rides snapshots, not the delta sent for every model call.
func TestDeltaSummaryStaysLean(t *testing.T) {
	v := New()
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "r", Pipeline: "p", Time: at(1)})

	sub := &subscriber{ch: make(chan []byte, 8)}
	v.mu.Lock()
	v.subs[sub] = struct{}{}
	v.mu.Unlock()

	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "r", Stage: "s", TaskID: "t", Time: at(2)})
	select {
	case b := <-sub.ch:
		var d struct {
			Runs    []*RunSummary `json:"runs"`
			Summary *RunSummary   `json:"summary"`
		}
		if err := json.Unmarshal(b, &d); err != nil {
			t.Fatal(err)
		}
		if d.Runs != nil {
			t.Error("the full roster rode a task delta; it belongs on run.started")
		}
		if d.Summary == nil {
			t.Fatal("delta carried no summary")
		}
		if len(d.Summary.Stages) != 0 {
			t.Errorf("stage briefs rode a task delta: %+v", d.Summary.Stages)
		}
	default:
		t.Fatal("no delta was broadcast")
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
	v.Handle(observe.Event{Type: observe.StageProjected, Pipeline: "triage",
		Stage: "classify", Kind: "infer",
		Records: 3, Time: at(900),
		Usage:   core.Usage{InputTokens: 300, OutputTokens: 60, Requests: 3, CostUSD: 0.006},
		Ceiling: core.Usage{InputTokens: 300, OutputTokens: 300, Requests: 3, CostUSD: 0.020},
		Latency: 30 * time.Second})
	v.Handle(observe.Event{Type: observe.RunProjected, Pipeline: "triage", Kind: "barrier", Time: at(901),
		Usage:   core.Usage{InputTokens: 300, OutputTokens: 60, Requests: 3, CostUSD: 0.006},
		Ceiling: core.Usage{InputTokens: 300, OutputTokens: 300, Requests: 3, CostUSD: 0.020},
		Budget:  core.Budget{MaxCostUSD: 0.01},
		Latency: 30 * time.Second,
		Detail:  "stage \"load\" is a function"})
}

// TestProjectionBeforeRun checks a forecast published ahead of its run is
// held, and reaches a viewer connecting while the sky is still empty — the
// one moment when knowing the price is still actionable.
func TestProjectionBeforeRun(t *testing.T) {
	v := New()
	projectionEvents(v)

	v.mu.Lock()
	fc := v.forecasts["triage"]
	v.mu.Unlock()
	if fc == nil || fc.run == nil {
		t.Fatal("run-level projection was not recorded")
	}
	if got, want := fc.run.ExpectedUSD, 0.006; got != want {
		t.Errorf("expected cost = %v, want %v", got, want)
	}
	if got, want := fc.run.CeilingUSD, 0.020; got != want {
		t.Errorf("ceiling cost = %v, want %v", got, want)
	}
	// A $0.01 budget cannot cover a $0.02 ceiling.
	if fc.run.FitsBudget {
		t.Error("a budget below the ceiling was reported as covering it")
	}
	if len(fc.run.Warnings) != 1 {
		t.Errorf("warnings = %v, want the one that was published", fc.run.Warnings)
	}
	if p := fc.stages["classify"]; p == nil || p.Calls != 3 || p.FloorMS != 30_000 {
		t.Errorf("stage projection = %+v, want 3 calls and a 30s floor", p)
	}
	if snap := snapshot(t, v); snap.Projection == nil {
		t.Error("a viewer connecting before the run sees no forecast")
	}
}

// TestProjectionSurvivesRunReset is the property the whole feature rests on:
// the projection describes the pipeline, and the run that follows is the thing
// it predicted, so run.started must not throw the comparison away.
func TestProjectionSurvivesRunReset(t *testing.T) {
	v := New()
	projectionEvents(v)
	feedLifecycle(v)

	v.mu.Lock()
	proj := v.cur.proj
	st := v.cur.stageIx["classify"]
	v.mu.Unlock()
	if proj == nil {
		t.Fatal("run.started discarded the projection")
	}
	if st == nil || st.Proj == nil {
		t.Fatal("run.started discarded the stage's projection")
	}
	if st.Status == "" {
		t.Error("the stage lost its live status while regaining its projection")
	}

	snap := snapshot(t, v)
	if snap.Projection == nil {
		t.Error("snapshot omitted the projection, so a reconnecting viewer loses it")
	}
	for _, s := range snap.Stages {
		if s.ID == "classify" && s.Proj == nil {
			t.Error("snapshot omitted the stage's projection")
		}
	}
}

// TestProjectionGoesToItsOwnPipeline: in a universe, a forecast has to find
// the run it predicted. Handing one pipeline's numbers to another's stages
// would read as a wild overspend that never happened.
func TestProjectionGoesToItsOwnPipeline(t *testing.T) {
	v := New()
	projectionEvents(v) // pipeline "triage"
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "other", Pipeline: "overview", Time: at(1000)})
	v.Handle(observe.Event{Type: observe.StageStarted, RunID: "other", Stage: "classify", Time: at(1001)})

	v.mu.Lock()
	other := v.cur
	v.mu.Unlock()
	if other.proj != nil {
		t.Error("another pipeline's forecast landed on this run")
	}
	if st := other.stageIx["classify"]; st != nil && st.Proj != nil {
		t.Error("another pipeline's forecast landed on a same-named stage")
	}

	// The run it was published for still claims it, whenever it starts.
	feedLifecycle(v)
	v.mu.Lock()
	claimed := v.cur.proj != nil && v.cur.stageIx["classify"].Proj != nil
	v.mu.Unlock()
	if !claimed {
		t.Error("the pipeline that was projected did not claim its forecast")
	}
}

// TestNoProjectionIsAbsentFromSnapshot keeps a run without a projection
// rendering exactly as it did before the feature existed.
func TestNoProjectionIsAbsentFromSnapshot(t *testing.T) {
	v := New()
	feedLifecycle(v)
	v.mu.Lock()
	blob := v.snapshotLocked(v.cur)
	v.mu.Unlock()
	if strings.Contains(string(blob), `"projection"`) {
		t.Errorf("unprojected run carries a projection key:\n%s", blob)
	}
	if strings.Contains(string(blob), `"proj"`) {
		t.Errorf("unprojected stage carries a proj key:\n%s", blob)
	}
}

// feedRounds drives one iterative stage through three supersteps with a
// shrinking frontier, the shape a converging loop actually makes.
func feedRounds(v *Server) {
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_i", Pipeline: "walk", Time: at(1000)})
	v.Handle(observe.Event{Type: observe.StageStarted, RunID: "run_i", Stage: "explore",
		Kind: "iterate", Detail: "iterative · bsp algorithm", Time: at(1001)})

	for _, r := range []struct{ n, active, msgs int }{{1, 3, 0}, {2, 2, 5}, {3, 1, 2}} {
		v.Handle(observe.Event{Type: observe.RoundStarted, RunID: "run_i", Stage: "explore",
			Round: r.n, Records: r.active, Messages: r.msgs, Time: at(int64(1000 + r.n*100))})
		for i := 0; i < r.active; i++ {
			id := fmt.Sprintf("t%d_%d", r.n, i)
			v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "run_i", Stage: "explore",
				TaskID: id, Records: 1, Time: at(int64(1000 + r.n*100 + 1))})
			v.Handle(observe.Event{Type: observe.TaskStarted, RunID: "run_i", Stage: "explore",
				TaskID: id, Worker: "w1", Attempt: 1, Time: at(int64(1000 + r.n*100 + 2))})
			v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "run_i", Stage: "explore",
				TaskID: id, Worker: "w1", Attempt: 1, Time: at(int64(1000 + r.n*100 + 50))})
		}
		v.Handle(observe.Event{Type: observe.RoundFinished, RunID: "run_i", Stage: "explore",
			Round: r.n, Records: r.active, Time: at(int64(1000 + r.n*100 + 60)),
			Usage: core.Usage{InputTokens: 10 * r.active, OutputTokens: r.active, CostUSD: 0.001 * float64(r.active)}})
	}
	v.Handle(observe.Event{Type: observe.StageConverged, RunID: "run_i", Stage: "explore",
		Round: 3, Records: 6, Note: "quiet", Time: at(1400)})
	v.Handle(observe.Event{Type: observe.StageFinished, RunID: "run_i", Stage: "explore", Time: at(1401)})
	v.Handle(observe.Event{Type: observe.RunFinished, RunID: "run_i", Pipeline: "walk", Time: at(1402)})
}

// A loop's output cannot be read on its own: the same records come back
// whether it settled or was cut off by the round cap. The view has to carry
// the per-round shape and the halt reason, or it is showing one stage that
// happened to run a lot of tasks.
func TestRoundsReachTheView(t *testing.T) {
	v := New()
	defer v.Close()
	feedRounds(v)

	rec := httptest.NewRecorder()
	v.ServeHTTP(rec, httptest.NewRequest("GET", "/api/state?run=run_i", nil))
	var snap struct {
		Stages []StageInfo `json:"stages"`
		Tasks  []struct {
			ID    string `json:"id"`
			Round int    `json:"round"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var st *StageInfo
	for i := range snap.Stages {
		if snap.Stages[i].ID == "explore" {
			st = &snap.Stages[i]
		}
	}
	if st == nil {
		t.Fatal("no explore stage in the snapshot")
	}
	if st.Halt != "quiet" {
		t.Errorf("halt = %q, want %q", st.Halt, "quiet")
	}
	if len(st.Rounds) != 3 {
		t.Fatalf("rounds = %d, want 3", len(st.Rounds))
	}
	for i, want := range []struct{ active, msgs, done int }{{3, 0, 3}, {2, 5, 2}, {1, 2, 1}} {
		got := st.Rounds[i]
		if got.N != i+1 || got.Active != want.active || got.Messages != want.msgs || got.Done != want.done {
			t.Errorf("round %d = %+v, want n=%d active=%d msgs=%d done=%d",
				i+1, got, i+1, want.active, want.msgs, want.done)
		}
		if got.EndedAt == 0 || got.CostUSD == 0 {
			t.Errorf("round %d has no end time or cost: %+v", i+1, got)
		}
	}

	// Every task is attributed to the superstep it ran in, which is what the
	// concentric-orbit layout is drawn from. A round is a barrier, so this is
	// exactly the round that was open when the task was scheduled.
	byRound := map[int]int{}
	for _, task := range snap.Tasks {
		byRound[task.Round]++
	}
	for round, want := range map[int]int{1: 3, 2: 2, 3: 1} {
		if byRound[round] != want {
			t.Errorf("round %d holds %d tasks, want %d (all: %v)", round, byRound[round], want, byRound)
		}
	}
	if byRound[0] != 0 {
		t.Errorf("%d tasks of an iterative stage carry no round", byRound[0])
	}
}

// A stage that is not iterative must be unchanged by any of this: no rounds,
// no halt, and no round stamped onto its tasks.
func TestNonIterativeStagesCarryNoRounds(t *testing.T) {
	v := New()
	defer v.Close()
	feedLifecycle(v)

	rec := httptest.NewRecorder()
	v.ServeHTTP(rec, httptest.NewRequest("GET", "/api/state?run=run_1", nil))
	var snap struct {
		Stages []StageInfo `json:"stages"`
		Tasks  []struct {
			Round int `json:"round"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, st := range snap.Stages {
		if len(st.Rounds) != 0 || st.Halt != "" {
			t.Errorf("stage %q gained rounds/halt: %+v / %q", st.ID, st.Rounds, st.Halt)
		}
	}
	for _, task := range snap.Tasks {
		if task.Round != 0 {
			t.Errorf("a non-iterative task carries round %d", task.Round)
		}
	}
}

// --- MCP servers ---------------------------------------------------------

func connectMCP(v *Server) {
	v.Handle(observe.Event{
		Type: observe.MCPConnected, Server: "catalog", Kind: "stdio",
		Note: "npx -y @example/catalog-mcp", Artifact: "digest0000000000",
		Records: 3, Slots: 4, Latency: 40 * time.Millisecond,
		Detail: "catalog 1.0\nstdio · npx", Time: at(900),
	})
}

func mcpCall(v *Server, run, stage, task, tool string, ms int64, queued int64, err string) observe.Event {
	return observe.Event{
		Type: observe.MCPCalled, RunID: run, Stage: stage, TaskID: task,
		Server: "catalog", Tool: tool, Slots: 4, InFlight: 2, Bytes: 12,
		Latency: time.Duration(ms) * time.Millisecond,
		Queued:  time.Duration(queued) * time.Millisecond,
		Err:     err, Time: at(2000 + ms),
	}
}

// A connection is the host's: it is made before any run exists, and conjuring
// a sky for it would put a run in the universe that never happened.
func TestMCPConnectionBelongsToTheHostNotARun(t *testing.T) {
	v := New()
	connectMCP(v)

	v.mu.Lock()
	runs, cur := len(v.runs), v.cur
	servers := len(v.servers)
	v.mu.Unlock()

	if runs != 0 || cur != nil {
		t.Fatalf("connecting opened %d run(s); a connection is not a run", runs)
	}
	if servers != 1 {
		t.Fatalf("host servers = %d, want 1", servers)
	}

	// It is still visible: an empty universe should say what it is wired to.
	var snap Snapshot
	v.mu.Lock()
	blob := v.snapshotLocked(nil)
	v.mu.Unlock()
	if err := json.Unmarshal(blob, &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.MCP) != 1 || snap.MCP[0].ID != "catalog" {
		t.Fatalf("empty-universe snapshot omits the server: %+v", snap.MCP)
	}
	if snap.MCP[0].Tools != 3 || snap.MCP[0].Slots != 4 || snap.MCP[0].Kind != "stdio" {
		t.Fatalf("server identity lost: %+v", snap.MCP[0])
	}
}

// Every run's sky opens already knowing the servers, because the connection
// predates it — and each run counts only its own calls against it.
func TestMCPServersSeedEveryRun(t *testing.T) {
	v := New()
	connectMCP(v)
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Pipeline: "a", Time: at(1000)})
	v.Handle(mcpCall(v, "run_1", "enrich", "task_a", "lookup", 12, 0, ""))
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_2", Pipeline: "b", Time: at(3000)})

	v.mu.Lock()
	r1, r2 := v.runIx["run_1"], v.runIx["run_2"]
	v.mu.Unlock()

	if len(r1.servers) != 1 || len(r2.servers) != 1 {
		t.Fatalf("servers per run = %d / %d, want 1 each", len(r1.servers), len(r2.servers))
	}
	if r2.servers[0].Tools != 3 || r2.servers[0].Kind != "stdio" {
		t.Fatalf("the second run did not inherit the server's identity: %+v", r2.servers[0])
	}
	if r1.servers[0].Calls != 1 {
		t.Fatalf("run_1 calls = %d, want 1", r1.servers[0].Calls)
	}
	if r2.servers[0].Calls != 0 {
		t.Fatalf("run_2 inherited run_1's traffic: %d calls", r2.servers[0].Calls)
	}
}

func TestMCPCallsAreTracked(t *testing.T) {
	v := New()
	connectMCP(v)
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Pipeline: "a", Time: at(1000)})
	v.Handle(mcpCall(v, "run_1", "enrich", "task_a", "lookup", 10, 0, ""))
	v.Handle(mcpCall(v, "run_1", "enrich", "task_b", "lookup", 30, 5, ""))
	v.Handle(mcpCall(v, "run_1", "call", "task_c", "stock", 20, 0, "boom"))

	v.mu.Lock()
	defer v.mu.Unlock()
	m := v.runIx["run_1"].servers[0]

	if m.Calls != 3 || m.Errors != 1 {
		t.Fatalf("calls/errors = %d/%d, want 3/1", m.Calls, m.Errors)
	}
	if m.BusyUS != 60_000 || m.QueueUS != 5_000 {
		t.Fatalf("busy/queue = %d/%d µs, want 60000/5000", m.BusyUS, m.QueueUS)
	}
	if m.Peak != 2 || m.Slots != 4 {
		t.Fatalf("peak/slots = %d/%d, want 2/4", m.Peak, m.Slots)
	}
	if m.LastErr != "boom" {
		t.Fatalf("last error = %q", m.LastErr)
	}
	if len(m.Stages) != 2 {
		t.Fatalf("stages = %v, want both callers", m.Stages)
	}

	// Per tool, because "which tool is slow" is the question a single
	// server-wide average cannot answer.
	byTool := map[string]ToolStat{}
	for _, ts := range m.ByTool {
		byTool[ts.Name] = ts
	}
	if got := byTool["lookup"]; got.Calls != 2 || got.TotalUS != 40_000 || got.MaxUS != 30_000 {
		t.Fatalf("lookup stats = %+v", got)
	}
	if got := byTool["stock"]; got.Calls != 1 || got.Errors != 1 {
		t.Fatalf("stock stats = %+v", got)
	}
	if len(m.Recent) != 3 || m.Recent[2].Tool != "stock" {
		t.Fatalf("recent tail = %+v", m.Recent)
	}

	// And the call belongs to the task too: a tool call is the only work a
	// task does that leaves no trace in the cost column.
	n := v.runIx["run_1"].taskIx["task_a"]
	if len(n.MCPCalls) != 1 || n.MCPCalls[0] != "catalog/lookup" {
		t.Fatalf("task mcp calls = %v", n.MCPCalls)
	}
	var logged bool
	for _, l := range n.Log {
		if strings.Contains(l.Msg, "mcp catalog/lookup") && strings.Contains(l.Msg, "no tokens") {
			logged = true
		}
	}
	if !logged {
		t.Fatalf("the task log does not explain the tool call: %+v", n.Log)
	}
}

// A call that arrives without a preceding connection event still lands: the
// view must not depend on having seen provisioning.
func TestMCPCallWithoutConnectionEvent(t *testing.T) {
	v := New()
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Pipeline: "a", Time: at(1000)})
	v.Handle(mcpCall(v, "run_1", "enrich", "task_a", "lookup", 10, 0, ""))

	v.mu.Lock()
	defer v.mu.Unlock()
	servers := v.runIx["run_1"].servers
	if len(servers) != 1 || servers[0].Calls != 1 {
		t.Fatalf("servers = %+v", servers)
	}
}

// A reconnect mid-run is visible in the run being watched.
func TestMCPReconnectUpdatesTheLiveRun(t *testing.T) {
	v := New()
	connectMCP(v)
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Pipeline: "a", Time: at(1000)})
	connectMCP(v)

	v.mu.Lock()
	defer v.mu.Unlock()
	m := v.runIx["run_1"].servers[0]
	if m.Dials != 2 {
		t.Fatalf("dials = %d, want 2 after a reconnect", m.Dials)
	}
	if len(v.runs) != 1 {
		t.Fatalf("a reconnect opened %d runs", len(v.runs))
	}
}

// The UI is an embedded single file with no build step, so nothing checks that
// it reads the fields the Go side emits. This does: every JSON name the MCP
// delta carries must appear in the page, so renaming one here without renaming
// it there fails the build rather than silently drawing a blank server.
func TestUIReadsTheMCPWireFields(t *testing.T) {
	page, err := uiFS.ReadFile("ui.html")
	if err != nil {
		t.Fatal(err)
	}
	blob, err := json.Marshal(&MCPInfo{
		ID: "s", Kind: "stdio", Endpoint: "e", Detail: "d", Digest: "h",
		Tools: 1, Sessions: 1, Dials: 1, DialMS: 1, Calls: 1, Errors: 1,
		BusyUS: 1, QueueUS: 1, Slots: 1, Peak: 1, LastAt: 1,
		Stages: []string{"a"}, Tasks: []string{"t"},
		ByTool: []ToolStat{{Name: "x", Calls: 1, Errors: 1, TotalUS: 1, MaxUS: 1}},
		Recent: []MCPCall{{Tool: "x", US: 1, QueueUS: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(blob, &fields); err != nil {
		t.Fatal(err)
	}
	// Nested field names matter as much as the top-level ones.
	names := []string{"totalUs", "maxUs", "queueUs", "us"}
	for k := range fields {
		names = append(names, k)
	}
	for _, name := range names {
		if !strings.Contains(string(page), name) {
			t.Errorf("ui.html never reads MCPInfo field %q — the view will draw it as missing", name)
		}
	}
	// And the node has to be drawn by something.
	for _, fn := range []string{"function drawMCP(", "function renderMCPPanel(", "'mcp'"} {
		if !strings.Contains(string(page), fn) {
			t.Errorf("ui.html is missing %q", fn)
		}
	}
}
