package stream

import (
	"testing"
	"time"
)

var epochBase = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func evt(sec int) time.Time { return epochBase.Add(time.Duration(sec) * time.Second) }

// clock lets the idleness rules be tested without sleeping.
type clock struct{ now time.Time }

func (c *clock) tick(d time.Duration) { c.now = c.now.Add(d) }

func tracker(lateness, idle time.Duration) (*Watermarks, *clock) {
	c := &clock{now: epochBase}
	w := NewWatermarks(lateness, idle)
	w.now = func() time.Time { return c.now }
	return w, c
}

func TestWatermarkIsTheMinimumAcrossSplits(t *testing.T) {
	w, _ := tracker(0, 0)
	w.Track("p0")
	w.Track("p1")

	// A tracked split that has produced nothing holds the line at the
	// beginning of time: nothing is known about what it will deliver.
	w.Observe("p0", evt(100), Position{Offset: 1})
	if got := w.Now(); !got.IsZero() {
		t.Fatalf("watermark = %s, want zero while p1 has said nothing", got)
	}

	w.Observe("p1", evt(40), Position{Offset: 1})
	if got := w.Now(); !got.Equal(evt(40)) {
		t.Fatalf("watermark = %s, want the slower split's %s", got, evt(40))
	}

	// The laggard catching up moves the whole job forward.
	w.Observe("p1", evt(90), Position{Offset: 2})
	if got := w.Now(); !got.Equal(evt(90)) {
		t.Fatalf("watermark = %s, want %s", got, evt(90))
	}
}

func TestLatenessHoldsTheWatermarkBack(t *testing.T) {
	w, _ := tracker(10*time.Second, 0)
	w.Observe("p0", evt(100), Position{})
	if got := w.Now(); !got.Equal(evt(90)) {
		t.Fatalf("watermark = %s, want the largest event time less the allowance", got)
	}
}

func TestWatermarkNeverGoesBackwards(t *testing.T) {
	w, _ := tracker(0, 0)
	w.Observe("p0", evt(100), Position{})
	w.Now()
	// An out-of-order record does not retract a claim already made.
	w.Observe("p0", evt(20), Position{})
	if got := w.Now(); !got.Equal(evt(100)) {
		t.Fatalf("watermark = %s, want it to hold at %s", got, evt(100))
	}
}

func TestAnIdleSplitStopsHoldingTheLine(t *testing.T) {
	w, c := tracker(0, 30*time.Second)
	w.Track("busy")
	w.Track("quiet")
	w.Observe("busy", evt(100), Position{})

	// The quiet split has never produced, so it holds everything back.
	if got := w.Now(); !got.IsZero() {
		t.Fatalf("watermark = %s, want zero while a tracked split is silent", got)
	}

	// Past the idle timeout it is excluded, and the busy split's progress
	// becomes the job's.
	c.tick(31 * time.Second)
	if got := w.Now(); !got.Equal(evt(100)) {
		t.Fatalf("watermark = %s, want %s once the silent split goes idle", got, evt(100))
	}

	// It speaking again rejoins the minimum — but cannot move time backwards.
	w.Observe("quiet", evt(50), Position{})
	if got := w.Now(); !got.Equal(evt(100)) {
		t.Fatalf("watermark = %s, want it held at %s", got, evt(100))
	}
}

func TestRetiredSplitsAreExcluded(t *testing.T) {
	w, _ := tracker(0, 0)
	w.Observe("a", evt(100), Position{})
	w.Observe("b", evt(20), Position{})
	if got := w.Now(); !got.Equal(evt(20)) {
		t.Fatalf("watermark = %s, want %s", got, evt(20))
	}
	w.Retire("b")
	if got := w.Now(); !got.Equal(evt(100)) {
		t.Fatalf("watermark = %s, want the retired split to stop constraining it", got)
	}
}

func TestEveryoneIdleAdvancesToTheFurthestReached(t *testing.T) {
	w, c := tracker(0, time.Second)
	w.Observe("a", evt(100), Position{})
	w.Observe("b", evt(60), Position{})
	c.tick(2 * time.Second)
	if got := w.Now(); !got.Equal(evt(100)) {
		t.Fatalf("watermark = %s, want the furthest any split reached", got)
	}
}

func TestForceAdvancesPastEveryConstraint(t *testing.T) {
	w, _ := tracker(0, 0)
	w.Track("a")
	if got := w.Force(evt(500)); !got.Equal(evt(500)) {
		t.Fatalf("forced watermark = %s", got)
	}
	if got := w.Now(); !got.Equal(evt(500)) {
		t.Fatalf("watermark = %s, want the forced value to stick", got)
	}
}

func TestPositionsAndLagsReportPerSplitProgress(t *testing.T) {
	w, _ := tracker(0, 0)
	w.Observe("a", evt(100), Position{Offset: 7})
	w.Observe("b", evt(40), Position{Offset: 3})
	w.Track("never")

	pos := w.Positions()
	if pos["a"].Offset != 7 || pos["b"].Offset != 3 {
		t.Fatalf("positions = %+v", pos)
	}
	if _, ok := pos["never"]; ok {
		t.Fatal("a split that produced nothing should have no position to resume from")
	}

	lags := w.Lags()
	if len(lags) != 3 {
		t.Fatalf("lags = %d entries, want 3", len(lags))
	}
	// Sorted by split ID: a, b, never.
	if lags[0].Split != "a" || lags[0].Events != 1 {
		t.Fatalf("first lag = %+v", lags[0])
	}
	if lags[1].Lag != 0 {
		t.Fatalf("the split setting the watermark should report no lag, got %s", lags[1].Lag)
	}
}
