package stream_test

import (
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/stream"
)

var base = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func at(sec int) time.Time { return base.Add(time.Duration(sec) * time.Second) }

func rec(id string) core.Record { return core.NewRecord(id, map[string]any{"v": id}) }

// add feeds a record and fails the test if the windower rejected it.
func add(t *testing.T, w *stream.Windower, id string, sec int) []stream.Fired {
	t.Helper()
	fired, err := w.Add(rec(id), at(sec))
	if err != nil {
		t.Fatalf("add %s: %v", id, err)
	}
	return fired
}

func ids(fired []stream.Fired) [][]string {
	out := make([][]string, 0, len(fired))
	for _, f := range fired {
		var names []string
		for _, r := range f.Records {
			names = append(names, r.ID)
		}
		out = append(out, names)
	}
	return out
}

func TestTumblingFiresOnWatermark(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{Assigner: stream.Tumbling(time.Minute)})

	// Three records in the 12:00 minute, one in the 12:01 minute.
	add(t, w, "a", 5)
	add(t, w, "b", 30)
	add(t, w, "c", 59)
	add(t, w, "d", 61)

	// The watermark inside the first window closes nothing.
	if fired := w.Advance(at(45)); len(fired) != 0 {
		t.Fatalf("watermark inside the window fired %d panes", len(fired))
	}
	// Reaching the window's end closes it, and only it.
	fired := w.Advance(at(60))
	if len(fired) != 1 {
		t.Fatalf("panes = %d, want 1", len(fired))
	}
	if got := ids(fired)[0]; len(got) != 3 {
		t.Fatalf("pane carried %v, want a,b,c", got)
	}
	if p := fired[0].Pane; !p.Final || p.Seq != 1 {
		t.Fatalf("pane = %+v, want the window's first and final firing", p)
	}
	if p := fired[0].Pane; !p.Window.Start.Equal(base) || !p.Window.End.Equal(at(60)) {
		t.Fatalf("window = %s, want 12:00..12:01", p.Window)
	}

	// The second window is still open, and drains at the end of the stream.
	if fired := w.Drain(); len(fired) != 1 || len(fired[0].Records) != 1 {
		t.Fatalf("drain = %v, want one pane of one record", ids(fired))
	}
	if s := w.Stats(); s.Panes != 2 || s.Records != 4 || s.LiveWindows != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestWatermarkIsMonotonic(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{Assigner: stream.Tumbling(time.Minute)})
	w.Advance(at(120))
	if fired := w.Advance(at(30)); fired != nil {
		t.Fatalf("a backwards watermark fired %d panes", len(fired))
	}
	if got := w.Watermark(); !got.Equal(at(120)) {
		t.Fatalf("watermark went backwards to %s", got)
	}
}

func TestLatenessDelaysTheFiringRatherThanRepeatingIt(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{
		Assigner: stream.Tumbling(time.Minute),
		Lateness: 20 * time.Second,
	})
	add(t, w, "a", 10)

	// Past the window's end but inside its lateness: still open.
	if fired := w.Advance(at(65)); len(fired) != 0 {
		t.Fatalf("window fired at %v despite tolerated lateness", fired)
	}
	// A straggler for that window is accepted, not dropped.
	add(t, w, "late", 30)
	if s := w.Stats(); s.Dropped != 0 {
		t.Fatalf("dropped %d records inside the lateness allowance", s.Dropped)
	}

	// Past end+lateness: one firing, carrying both.
	fired := w.Advance(at(80))
	if len(fired) != 1 {
		t.Fatalf("panes = %d, want 1", len(fired))
	}
	if got := ids(fired)[0]; len(got) != 2 {
		t.Fatalf("pane carried %v, want both the record and the straggler", got)
	}
	if !fired[0].Pane.Final {
		t.Fatal("the firing after the lateness allowance should be final")
	}
}

func TestRecordsPastLatenessAreDroppedAndCounted(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{
		Assigner: stream.Tumbling(time.Minute),
		Lateness: 10 * time.Second,
	})
	add(t, w, "a", 10)
	w.Advance(at(90)) // closes 12:00..12:01

	if fired := add(t, w, "too-late", 20); len(fired) != 0 {
		t.Fatalf("a record for a closed window fired %v", ids(fired))
	}
	if s := w.Stats(); s.Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", s.Dropped)
	}
}

func TestFailLateStopsTheJob(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{
		Assigner: stream.Tumbling(time.Minute), Late: stream.FailLate,
	})
	w.Advance(at(120))
	if _, err := w.Add(rec("x"), at(5)); err == nil {
		t.Fatal("FailLate should refuse a record for a window already gone")
	}
}

func TestKeyedWindowsCloseIndependentlyOfEachOther(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{
		Assigner: stream.Tumbling(time.Minute),
		Key: func(r core.Record) string {
			if r.ID == "eu-1" || r.ID == "eu-2" {
				return "eu"
			}
			return "us"
		},
	})
	add(t, w, "eu-1", 5)
	add(t, w, "us-1", 10)
	add(t, w, "eu-2", 20)

	fired := w.Advance(at(60))
	if len(fired) != 2 {
		t.Fatalf("panes = %d, want one per key", len(fired))
	}
	// Ordering is by end, then start, then key: "eu" before "us".
	if fired[0].Pane.Window.Key != "eu" || len(fired[0].Records) != 2 {
		t.Fatalf("first pane = %+v with %d records", fired[0].Pane.Window, len(fired[0].Records))
	}
	if fired[1].Pane.Window.Key != "us" || len(fired[1].Records) != 1 {
		t.Fatalf("second pane = %+v with %d records", fired[1].Pane.Window, len(fired[1].Records))
	}
}

func TestSlidingAssignsEachRecordToEveryCoveringWindow(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{
		Assigner: stream.Sliding(time.Minute, 30*time.Second),
	})
	// A record at 12:00:45 is covered by the windows starting 12:00:00 and
	// 12:00:30.
	add(t, w, "a", 45)
	if s := w.Stats(); s.Assignments != 2 || s.Records != 1 {
		t.Fatalf("stats = %+v, want one record in two windows", s)
	}
	fired := w.Advance(at(60))
	if len(fired) != 1 || !fired[0].Pane.Window.Start.Equal(base) {
		t.Fatalf("first close = %v, want the 12:00:00 window only", fired)
	}
	if fired := w.Advance(at(90)); len(fired) != 1 {
		t.Fatalf("second close = %d panes, want 1", len(fired))
	}
}

func TestMaxRecordsFiresEarlyRatherThanBufferingForever(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{
		Assigner: stream.Tumbling(time.Hour), MaxRecords: 3,
	})
	add(t, w, "a", 1)
	add(t, w, "b", 2)
	fired := add(t, w, "c", 3)
	if len(fired) != 1 {
		t.Fatalf("panes = %d, want an early firing at the bound", len(fired))
	}
	if fired[0].Pane.Final {
		t.Fatal("a bound-driven firing is not the window's final word")
	}
	if s := w.Stats(); s.Evicted != 1 || s.Early != 1 {
		t.Fatalf("stats = %+v, want one eviction counted", s)
	}
	// The window survives, purged, and takes the next records.
	add(t, w, "d", 4)
	if s := w.Stats(); s.LiveRecords != 1 {
		t.Fatalf("live records = %d, want the buffer purged and refilling", s.LiveRecords)
	}
}

func TestAtCountOnGlobalWindowMakesFixedSizeBatches(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{
		Assigner: stream.GlobalWindow(), Trigger: stream.AtCount(2),
	})
	if fired := add(t, w, "a", 1); len(fired) != 0 {
		t.Fatal("fired before the count was reached")
	}
	fired := add(t, w, "b", 2)
	if len(fired) != 1 || len(fired[0].Records) != 2 {
		t.Fatalf("fired %v, want one pane of two", ids(fired))
	}
	// The global window never expires on a watermark.
	if fired := w.Advance(at(10_000)); len(fired) != 0 {
		t.Fatalf("a global window closed on a watermark: %v", ids(fired))
	}
}

func TestSnapshotRestoreResumesHalfFilledWindows(t *testing.T) {
	spec := stream.WindowSpec{Assigner: stream.Tumbling(time.Minute)}
	w := stream.NewWindower(spec)
	add(t, w, "a", 5)
	add(t, w, "b", 30)
	w.Advance(at(40))

	blob, err := w.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// A fresh windower — a restarted process — picks up where it left off.
	restored := stream.NewWindower(spec)
	if err := restored.Restore(blob); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got := restored.Watermark(); !got.Equal(at(40)) {
		t.Fatalf("restored watermark = %s, want %s", got, at(40))
	}
	add(t, restored, "c", 50)

	fired := restored.Advance(at(60))
	if len(fired) != 1 {
		t.Fatalf("panes = %d, want 1", len(fired))
	}
	if got := ids(fired)[0]; len(got) != 3 {
		t.Fatalf("pane carried %v, want the two buffered records and the new one", got)
	}
}

func TestRestoreOfNothingIsACleanStart(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{Assigner: stream.Tumbling(time.Minute)})
	if err := w.Restore(nil); err != nil {
		t.Fatalf("restore(nil): %v", err)
	}
	if err := w.Restore([]byte("{not json")); err == nil {
		t.Fatal("restoring a corrupt snapshot should fail loudly")
	}
}

func TestValidateRefusesAWindowThatCanNeverFire(t *testing.T) {
	err := stream.WindowSpec{Assigner: stream.GlobalWindow()}.Validate()
	if err == nil {
		t.Fatal("a global window with no trigger should not compile")
	}
	if err := (stream.WindowSpec{Assigner: stream.Tumbling(time.Minute)}).Validate(); err != nil {
		t.Fatalf("a tumbling window should compile: %v", err)
	}
}

func TestWindowIdentityIsStable(t *testing.T) {
	a := stream.Window{Start: base, End: at(60), Key: "eu"}
	b := stream.Window{Start: base, End: at(60), Key: "eu"}
	if a.ID() != b.ID() {
		t.Fatalf("identical windows have different identities: %q vs %q", a.ID(), b.ID())
	}
	if (stream.Window{Start: base, End: at(60)}).ID() == a.ID() {
		t.Fatal("windows of different keys share an identity")
	}
	p := stream.Pane{Window: a, Seq: 2}
	if p.ID() != a.ID()+"#2" {
		t.Fatalf("pane identity = %q", p.ID())
	}
}

func TestOnlyAFiringThatCarriedAStragglerIsLate(t *testing.T) {
	w := stream.NewWindower(stream.WindowSpec{
		Assigner: stream.Tumbling(time.Minute),
		Lateness: 30 * time.Second,
	})
	// Two windows. Only the first receives a record after the watermark has
	// passed its end; the second merely closes on schedule.
	add(t, w, "a", 10)
	add(t, w, "b", 70)
	w.Advance(at(65)) // past window one's end, inside its lateness
	add(t, w, "straggler", 20)

	fired := w.Advance(at(200)) // closes both
	if len(fired) != 2 {
		t.Fatalf("panes = %d, want 2", len(fired))
	}
	if !fired[0].Pane.Late {
		t.Fatal("the window that took a straggler should report a late firing")
	}
	if fired[1].Pane.Late {
		t.Fatal("a window that closed on schedule is not late, however long its allowance")
	}
	if s := w.Stats(); s.Late != 1 {
		t.Fatalf("late firings = %d, want 1", s.Late)
	}
}
