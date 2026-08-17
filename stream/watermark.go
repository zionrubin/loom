package stream

import (
	"sort"
	"sync"
	"time"
)

// Watermarks tracks event-time progress across a source's splits and reduces it
// to the single claim a window stage needs: no record older than this will
// arrive again.
//
// The claim is a minimum, not a maximum, and that is the whole of the design. A
// job reading four partitions is only as caught up as its furthest-behind
// partition, so a window closes when *every* split has moved past it. Taking a
// maximum would produce a watermark that races ahead of the slowest reader and
// declares complete a window that is still filling — which does not fail, it
// quietly drops data.
//
// Two things keep that minimum from being useless. A split that has gone quiet
// is excluded after IdleTimeout, so an empty partition cannot hold every window
// open forever; and a split that has ended is retired outright. Both are
// reversible: a split that speaks again rejoins the minimum, and because the
// watermark is monotonic, its rejoining can never move event time backwards.
//
// Watermarks is safe for concurrent use: one goroutine per split reports into
// it while the ingestor reads from it.
type Watermarks struct {
	lateness time.Duration
	idle     time.Duration
	now      func() time.Time

	mu     sync.Mutex
	splits map[string]*splitState
	wm     time.Time
}

type splitState struct {
	max     time.Time // the largest event time this split has produced
	seen    time.Time // wall clock of its last event
	started time.Time // wall clock of when it was first tracked
	retired bool
	events  int64
	pos     Position
}

// NewWatermarks returns a tracker.
//
// lateness is the bounded out-of-orderness allowance: a split's watermark is
// the largest event time it has produced, less this. It is the answer to "how
// far behind its own clock can this source deliver?" and it is a property of
// the source, not of the windows — a window's own Lateness is a separate,
// later grace period on top of it.
//
// idle is how long a split may produce nothing before it stops holding the
// watermark back. Zero disables it, which is right for a source whose splits
// all carry traffic and wrong for anything partitioned by a key that goes
// quiet.
func NewWatermarks(lateness, idle time.Duration) *Watermarks {
	return &Watermarks{
		lateness: lateness, idle: idle, now: time.Now,
		splits: map[string]*splitState{},
	}
}

// Track registers a split. Until it produces an event it holds the watermark at
// the beginning of time — correctly, since nothing is known about what it will
// deliver — and it is released by its first event, by going idle, or by
// retiring.
func (w *Watermarks) Track(split string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.splits[split]; !ok {
		w.splits[split] = &splitState{started: w.now()}
	}
}

// Observe records an event from a split: its event time, and the position after
// it. A zero event time is ignored for watermark purposes — the job substitutes
// ingestion time before it gets here — but still counts as activity.
func (w *Watermarks) Observe(split string, t time.Time, pos Position) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.splits[split]
	if st == nil {
		st = &splitState{started: w.now()}
		w.splits[split] = st
	}
	st.seen = w.now()
	st.events++
	st.pos = pos
	st.retired = false
	if t.After(st.max) {
		st.max = t
	}
}

// Retire marks a split as finished: it will produce nothing further and stops
// constraining the watermark.
func (w *Watermarks) Retire(split string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if st := w.splits[split]; st != nil {
		st.retired = true
	}
}

// Now returns the current watermark: the minimum over the splits still holding
// the line, monotonically non-decreasing.
func (w *Watermarks) Now() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.compute()
}

// compute derives the watermark. Callers hold w.mu.
func (w *Watermarks) compute() time.Time {
	var (
		active   bool
		min, max time.Time
	)
	now := w.now()
	for _, st := range w.splits {
		wm := st.max.Add(-w.lateness)
		if st.max.IsZero() {
			// A split that has produced nothing has no event time to offer;
			// what it offers is the constraint that it might.
			wm = time.Time{}
		}
		if wm.After(max) {
			max = wm
		}
		if st.retired || w.isIdle(st, now) {
			continue
		}
		if !active || wm.Before(min) {
			min, active = wm, true
		}
	}
	// With nothing active — every split idle or ended — the constraint is gone
	// and event time may advance to the furthest any split reached. This is what
	// lets a quiet stream's last window close instead of hanging.
	candidate := min
	if !active {
		candidate = max
	}
	if candidate.After(w.wm) {
		w.wm = candidate
	}
	return w.wm
}

// isIdle reports whether a split has been silent long enough to be excluded.
// A split that has never produced is measured from when it was tracked, so a
// permanently empty partition releases the watermark on the same timer as one
// that fell silent.
func (w *Watermarks) isIdle(st *splitState, now time.Time) bool {
	if w.idle <= 0 {
		return false
	}
	since := st.seen
	if since.IsZero() {
		since = st.started
	}
	return now.Sub(since) >= w.idle
}

// Force advances the watermark to at least t, whatever the splits say. The
// ingestor uses it once, at the end of a stream, to close every remaining
// window; nothing else should.
func (w *Watermarks) Force(t time.Time) time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	if t.After(w.wm) {
		w.wm = t
	}
	return w.wm
}

// SplitLag reports one split's event-time progress against the job's.
type SplitLag struct {
	Split string `json:"split"`
	// Watermark is this split's own event-time position.
	Watermark time.Time `json:"watermark,omitempty"`
	// Lag is how far behind the job's watermark this split is holding — zero
	// for the split that is setting it.
	Lag time.Duration `json:"lag,omitempty"`
	// Events is how many events it has produced.
	Events int64 `json:"events"`
	// Position is where its reader has reached.
	Position Position `json:"position"`
	Idle     bool     `json:"idle,omitempty"`
	Retired  bool     `json:"retired,omitempty"`
}

// Lags reports every tracked split, ordered by ID, which is what a stream
// report prints and what tells you which partition is holding your windows
// open.
func (w *Watermarks) Lags() []SplitLag {
	w.mu.Lock()
	defer w.mu.Unlock()
	wm := w.compute()
	now := w.now()
	out := make([]SplitLag, 0, len(w.splits))
	for id, st := range w.splits {
		lag := SplitLag{
			Split: id, Events: st.events, Position: st.pos,
			Retired: st.retired, Idle: w.isIdle(st, now),
		}
		if !st.max.IsZero() {
			lag.Watermark = st.max.Add(-w.lateness)
			if d := wm.Sub(lag.Watermark); d > 0 {
				lag.Lag = d
			}
		}
		out = append(out, lag)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Split < out[j].Split })
	return out
}

// Positions returns each split's last observed position, which is what a
// checkpoint stores.
func (w *Watermarks) Positions() map[string]Position {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]Position, len(w.splits))
	for id, st := range w.splits {
		if st.events > 0 {
			out[id] = st.pos
		}
	}
	return out
}
