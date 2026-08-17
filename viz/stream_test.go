package viz

import (
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
)

var eventTime = time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

func evt(sec int) time.Time { return eventTime.Add(time.Duration(sec) * time.Second) }

// feedStream drives one window's worth of a stream job: a split opened, event
// time advancing, a pane fired, the aggregation it caused, and the checkpoint
// that made it recoverable.
func feedStream(v *Server) {
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "job_1",
		Pipeline: "watchtower", Kind: "stream", Time: at(1000)})
	v.Handle(observe.Event{Type: observe.SplitOpened, RunID: "job_1", Stage: "incidents",
		Split: "feed/part-0.jsonl", Note: "resumed at offset 4096", Time: at(1001)})
	v.Handle(observe.Event{Type: observe.SplitOpened, RunID: "job_1", Stage: "incidents",
		Split: "feed/part-1.jsonl", Note: "from the source's default start", Time: at(1001)})
	v.Handle(observe.Event{Type: observe.StageStarted, RunID: "job_1", Stage: "per-minute",
		Kind: "window", Detail: "tumbling/1m0s windows, at-watermark", Time: at(1002)})
	v.Handle(observe.Event{Type: observe.StageStarted, RunID: "job_1", Stage: "digest",
		Kind: "reduce_ai", Upstream: "per-minute", Time: at(1002)})

	v.Handle(observe.Event{Type: observe.WatermarkAdvanced, RunID: "job_1",
		Watermark: evt(30), Split: "feed/part-1.jsonl", Lag: 9 * time.Second, Time: at(1010)})

	// The identity is stage-qualified — two window stages can cut windows of the
	// same interval — and it is the same string on the pane event, on the tasks
	// the pane caused, and on the sink write it produced.
	pane := "per-minute#w1#1"
	v.Handle(observe.Event{Type: observe.PaneFired, RunID: "job_1", Stage: "per-minute",
		Pane: pane, Records: 12, Watermark: evt(60), Note: "final",
		Detail: "2026-03-01T09:00:00Z..2026-03-01T09:01:00Z[api]", Time: at(1020)})
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "job_1", Stage: "digest",
		TaskID: "task_p1", Records: 12, Pane: pane, Time: at(1021)})
	v.Handle(observe.Event{Type: observe.TaskStarted, RunID: "job_1", Stage: "digest",
		TaskID: "task_p1", Worker: "w1", Attempt: 1, Pane: pane, Time: at(1022)})
	v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "job_1", Stage: "digest",
		TaskID: "task_p1", Worker: "w1", Attempt: 1, Pane: pane,
		Usage:   core.Usage{InputTokens: 400, OutputTokens: 60, Requests: 1, CostUSD: 0.004},
		Latency: 40 * time.Millisecond, Time: at(1060)})
	v.Handle(observe.Event{Type: observe.SinkWrote, RunID: "job_1", Stage: "digest",
		Pane: pane, Records: 1, Time: at(1061)})

	v.Handle(observe.Event{Type: observe.RecordsLate, RunID: "job_1", Stage: "per-minute",
		Records: 3, Time: at(1070)})
	v.Handle(observe.Event{Type: observe.CheckpointCommitted, RunID: "job_1",
		Epoch: 4, Records: 20, Watermark: evt(90), Latency: 12 * time.Millisecond,
		Time: at(1080)})
}

func TestStreamStateIsBuiltFromTheStreamEvents(t *testing.T) {
	v := New()
	feedStream(v)

	r := v.runIx["job_1"]
	if r == nil {
		t.Fatal("no run state for the job")
	}
	if r.hdr.Driver != "stream" {
		t.Fatalf("driver = %q, want stream", r.hdr.Driver)
	}
	sm := r.stream
	if sm == nil {
		t.Fatal("a stream job produced no stream state")
	}
	if sm.Panes != 1 || sm.Late != 3 || sm.Batches != 1 {
		t.Fatalf("stream = %+v", sm)
	}
	if sm.Epoch != 4 || sm.Checkpoints != 1 || sm.QuiesceMS != 12 {
		t.Fatalf("checkpoint state = %+v", sm)
	}
	// The last watermark wins, and the split holding it back is named — a
	// watermark is a minimum, and the useful half of a minimum is which member
	// set it.
	if got := time.UnixMilli(sm.Watermark).UTC(); !got.Equal(evt(90)) {
		t.Fatalf("watermark = %s, want %s", got, evt(90))
	}
	if sm.Laggard != "feed/part-1.jsonl" || sm.LagMS != 9000 {
		t.Fatalf("laggard = %q at %dms", sm.Laggard, sm.LagMS)
	}
	if len(sm.Splits) != 2 {
		t.Fatalf("splits = %d, want 2", len(sm.Splits))
	}
	if sm.Splits[0].Note != "resumed at offset 4096" {
		t.Fatalf("split note = %q", sm.Splits[0].Note)
	}
}

func TestPaneCarriesTheCostOfTheWindowThatCausedIt(t *testing.T) {
	v := New()
	feedStream(v)
	r := v.runIx["job_1"]

	win := r.stageIx["per-minute"]
	if win == nil || !win.Windowed {
		t.Fatalf("window stage = %+v, want it marked as one", win)
	}
	if win.PaneCount != 1 || win.WindowRecords != 12 || win.Late != 3 {
		t.Fatalf("window stage counters = %+v", win)
	}
	if len(win.Panes) != 1 {
		t.Fatalf("panes = %d, want 1", len(win.Panes))
	}
	p := win.Panes[0]
	if p.ID != "per-minute#w1#1" {
		t.Fatalf("pane identity = %q, want the stage-qualified one", p.ID)
	}
	if p.Records != 12 || p.Seq != 1 || p.Kind != "final" || !p.Written {
		t.Fatalf("pane = %+v", p)
	}
	// Everything downstream of a window runs once per pane, so the pane is
	// where the money lands. Attribution is by the identity the task carried,
	// not by whichever pane happened to have fired most recently.
	if p.Tasks != 1 || p.Done != 1 {
		t.Fatalf("pane tasks = %d done %d, want 1/1", p.Tasks, p.Done)
	}
	if p.Tokens != 460 || p.CostUSD == 0 {
		t.Fatalf("pane cost = %d tokens, $%v", p.Tokens, p.CostUSD)
	}
	if n := r.taskIx["task_p1"]; n == nil || n.Pane == "" {
		t.Fatalf("task did not keep its pane: %+v", n)
	}
}

func TestPaneAttributionSurvivesALaterWindowFiring(t *testing.T) {
	v := New()
	feedStream(v)

	// A second window fires while the first one's aggregation is still running.
	// Attributing by "most recent pane" would move the running task's cost onto
	// the wrong window; the identity it carries prevents that.
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "job_1", Stage: "digest",
		TaskID: "task_p2", Records: 5, Pane: "per-minute#w1#1", Time: at(1090)})
	v.Handle(observe.Event{Type: observe.PaneFired, RunID: "job_1", Stage: "per-minute",
		Pane: "per-minute#w2#1", Records: 7, Watermark: evt(120), Note: "final",
		Detail: "2026-03-01T09:01:00Z..2026-03-01T09:02:00Z[api]", Time: at(1091)})
	v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "job_1", Stage: "digest",
		TaskID: "task_p2", Worker: "w1", Attempt: 1, Pane: "per-minute#w1#1",
		Usage: core.Usage{InputTokens: 100, OutputTokens: 10, CostUSD: 0.001}, Time: at(1100)})

	win := v.runIx["job_1"].stageIx["per-minute"]
	if len(win.Panes) != 2 {
		t.Fatalf("panes = %d, want 2", len(win.Panes))
	}
	first, second := win.Panes[0], win.Panes[1]
	if first.Tasks != 2 || first.Done != 2 {
		t.Fatalf("the first pane should hold both its tasks, got %d/%d", first.Done, first.Tasks)
	}
	if second.Tasks != 0 {
		t.Fatalf("the second pane took %d tasks it never caused", second.Tasks)
	}
}

func TestPaneHistoryIsBounded(t *testing.T) {
	v := New()
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "job_1", Kind: "stream", Time: at(1)})
	for i := 0; i < maxPanesPerStage+20; i++ {
		v.Handle(observe.Event{Type: observe.PaneFired, RunID: "job_1", Stage: "per-minute",
			Pane:    "per-minute#w" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + "#1",
			Records: 1, Note: "final", Time: at(int64(100 + i))})
	}
	win := v.runIx["job_1"].stageIx["per-minute"]
	if len(win.Panes) != maxPanesPerStage {
		t.Fatalf("held %d panes, want the bound of %d", len(win.Panes), maxPanesPerStage)
	}
	// The list is bounded; the count is not, because "how many windows has this
	// job closed" is a fact about the job rather than about the viewer.
	if win.PaneCount != maxPanesPerStage+20 {
		t.Fatalf("pane count = %d, want every firing counted", win.PaneCount)
	}
	if len(v.runIx["job_1"].paneIx) != maxPanesPerStage {
		t.Fatalf("the pane index kept %d entries", len(v.runIx["job_1"].paneIx))
	}
}

func TestAStreamJobForgetsItsOldestSettledTasks(t *testing.T) {
	v := New(RetainTasks(10))
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "job_1", Kind: "stream", Time: at(1)})

	// Fifty tasks, each scheduled and completed: an endless job in miniature.
	for i := 0; i < 50; i++ {
		id := "task_" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "job_1", Stage: "grade",
			TaskID: id, Records: 1, Time: at(int64(100 + i))})
		v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "job_1", Stage: "grade",
			TaskID: id, Attempt: 1, Time: at(int64(101 + i))})
	}
	r := v.runIx["job_1"]
	if len(r.tasks) > 10 {
		t.Fatalf("held %d tasks, want at most the retention bound of 10", len(r.tasks))
	}
	if len(r.taskIx) != len(r.tasks) {
		t.Fatalf("index holds %d for %d tasks", len(r.taskIx), len(r.tasks))
	}
	// What the job did is not forgotten with the tasks that did it.
	sum := v.summaryLocked(r, false)
	if sum.Tasks != 50 || sum.Completed != 50 {
		t.Fatalf("summary = %d tasks, %d completed, want 50/50", sum.Tasks, sum.Completed)
	}
	if st := r.stageIx["grade"]; st.Tasks != 50 || st.Done != 50 {
		t.Fatalf("stage counters = %d tasks, %d done", st.Tasks, st.Done)
	}
}

func TestABoundedRunIsNeverForgotten(t *testing.T) {
	v := New(RetainTasks(4))
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "run_1", Kind: "barrier", Time: at(1)})
	for i := 0; i < 20; i++ {
		id := "task_" + string(rune('a'+i))
		v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "run_1", Stage: "classify",
			TaskID: id, Records: 1, Time: at(int64(100 + i))})
		v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "run_1", Stage: "classify",
			TaskID: id, Attempt: 1, Time: at(int64(101 + i))})
	}
	// A run ends, so holding all of it is a bounded promise already.
	if got := len(v.runIx["run_1"].tasks); got != 20 {
		t.Fatalf("a bounded run held %d of its 20 tasks", got)
	}
}

func TestForgettingKeepsTheNewestAndDropsTheOldest(t *testing.T) {
	v := New(RetainTasks(1))
	v.Handle(observe.Event{Type: observe.RunStarted, RunID: "job_1", Kind: "stream", Time: at(1)})
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "job_1", Stage: "grade",
		TaskID: "t1", Time: at(2)})
	v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "job_1", Stage: "grade",
		TaskID: "t1", Attempt: 1, Time: at(3)})
	v.Handle(observe.Event{Type: observe.TaskScheduled, RunID: "job_1", Stage: "grade",
		TaskID: "t2", Time: at(4)})
	v.Handle(observe.Event{Type: observe.TaskCompleted, RunID: "job_1", Stage: "grade",
		TaskID: "t2", Attempt: 1, Time: at(5)})

	r := v.runIx["job_1"]
	if _, held := r.taskIx["t1"]; held {
		t.Fatal("the older settled task should have been forgotten")
	}
	if _, held := r.taskIx["t2"]; !held {
		t.Fatal("the newest task should still be held")
	}
}

func TestSnapshotCarriesTheStream(t *testing.T) {
	v := New()
	feedStream(v)
	body := string(v.snapshotLocked(v.runIx["job_1"]))
	for _, want := range []string{`"stream"`, `"watermark"`, `"laggard"`, `"panes"`,
		`"splits"`, `"windowed":true`} {
		if !containsSub(body, want) {
			t.Fatalf("snapshot is missing %s:\n%s", want, body)
		}
	}
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
