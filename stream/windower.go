package stream

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/zionrubin/loom/core"
)

// Windower is the engine behind a window stage: records go in with their event
// times, the watermark advances, and panes come out.
//
// It is deliberately a plain, single-goroutine data structure with no channels,
// no clock, and no knowledge of tasks or models. Everything about windowing
// that can be got wrong — which window a record belongs to, when a window is
// complete, what counts as late, what a restart restores — is decided here,
// where it can be tested by calling three methods and reading what comes back.
//
// The one semantic worth stating up front, because it differs from classic
// stream engines: a window fires **once**. Lateness delays that firing rather
// than causing a second one, and speculative firings are opt-in through a
// trigger. In a system whose operators are model calls, a window that re-fires
// on every straggler is not a latency choice, it is a bill.
type Windower struct {
	spec  WindowSpec
	asg   Assigner
	trig  Trigger
	live  map[string]*windowState
	wm    time.Time
	stats WindowStats
}

// windowState is one live window's buffer and firing history.
type windowState struct {
	Window Window `json:"window"`
	Items  []item `json:"items"`
	Fires  int    `json:"fires"`
	// Stragglers counts records admitted after the watermark had already passed
	// this window's end — tolerated because of Lateness, but late all the same.
	// It is recorded when the record arrives because that is the only moment it
	// is knowable: by the time the window fires, every record in it looks the
	// same.
	Stragglers int `json:"stragglers,omitempty"`
}

// item is a buffered record and the event time it was assigned by.
//
// The time is kept rather than discarded because a pane is emitted in
// event-time order, and that is not a presentational choice. Records reach a
// window in completion order — the stage before it runs several model calls at
// once — so a pane assembled in arrival order would differ between two runs
// over the same input. Sorting makes a pane a function of its contents, which
// is what lets a replayed window hit the result cache instead of being paid
// for again.
type item struct {
	Rec core.Record `json:"rec"`
	At  time.Time   `json:"at"`
}

// WindowStats reports what a window stage has done.
type WindowStats struct {
	// Records is how many records were assigned to at least one window.
	Records int64 `json:"records"`
	// Assignments counts window memberships, which exceeds Records under a
	// sliding assigner — and is the number that predicts the bill.
	Assignments int64 `json:"assignments"`
	// Panes is how many panes fired, Early those that fired before their
	// window closed, and Late those that fired carrying tolerated late data.
	Panes int64 `json:"panes"`
	Early int64 `json:"early"`
	Late  int64 `json:"late"`
	// Dropped counts records that arrived for a window already gone.
	Dropped int64 `json:"dropped"`
	// Evicted counts windows fired early because a bound was hit rather than
	// because they were complete. A non-zero value means MaxRecords or
	// MaxWindows is shaping the output.
	Evicted int64 `json:"evicted"`
	// LiveWindows and LiveRecords are the state resident right now.
	LiveWindows int `json:"live_windows"`
	LiveRecords int `json:"live_records"`
	// Watermark is how far event time has advanced.
	Watermark time.Time `json:"watermark"`
}

// NewWindower returns a windower for the spec. The spec should already have
// passed Validate.
func NewWindower(spec WindowSpec) *Windower {
	return &Windower{
		spec: spec,
		asg:  spec.assigner(),
		trig: spec.trigger(),
		live: map[string]*windowState{},
	}
}

// Watermark returns how far event time has advanced.
func (w *Windower) Watermark() time.Time { return w.wm }

// Stats returns a snapshot of what this windower has done.
func (w *Windower) Stats() WindowStats {
	s := w.stats
	s.Watermark = w.wm
	s.LiveWindows = len(w.live)
	for _, st := range w.live {
		s.LiveRecords += len(st.Items)
	}
	return s
}

// Add assigns a record to its windows and returns any panes its arrival
// triggered. The event time t is the one the ingestor carried; a spec with a
// Time function overrides it from the record itself.
//
// A record whose windows have all closed is late: it is counted and dropped, or
// the job is failed, according to the spec's LatePolicy.
func (w *Windower) Add(rec core.Record, t time.Time) ([]Fired, error) {
	if w.spec.Time != nil {
		if override := w.spec.Time(rec); !override.IsZero() {
			t = override
		}
	}
	windows := w.asg.Assign(t)
	if len(windows) == 0 {
		return nil, nil
	}
	key := ""
	if w.spec.Key != nil {
		key = w.spec.Key(rec)
	}

	var fired []Fired
	assigned := 0
	for _, win := range windows {
		win.Key = key
		if w.expired(win) {
			w.stats.Dropped++
			if w.spec.Late == FailLate {
				return nil, fmt.Errorf("stream: record %q is late for %s "+
					"(watermark %s, tolerating %s)", rec.ID, win,
					w.wm.UTC().Format(time.RFC3339), w.spec.Lateness)
			}
			continue
		}
		assigned++
		st := w.live[win.ID()]
		if st == nil {
			st = &windowState{Window: win}
			w.live[win.ID()] = st
		}
		st.Items = append(st.Items, item{Rec: rec, At: t})
		if !win.Global() && !w.wm.Before(win.End) {
			st.Stragglers++
		}
		w.stats.Assignments++

		switch {
		case w.spec.MaxRecords > 0 && len(st.Items) >= w.spec.MaxRecords:
			w.stats.Evicted++
			fired = append(fired, w.fire(st, false, true))
		default:
			if act := w.trig.OnRecord(win, len(st.Items), w.wm); act != Continue {
				fired = append(fired, w.fire(st, false, act == FirePurge))
			}
		}
	}
	if assigned > 0 {
		w.stats.Records++
	}
	fired = append(fired, w.evictOverflow()...)
	return order(fired), nil
}

// Advance moves the watermark and returns the panes that closing it produced.
// The watermark is monotonic: an attempt to move it backwards is ignored, which
// keeps a restart or a briefly-idle split from reopening a decided question.
func (w *Windower) Advance(wm time.Time) []Fired {
	if !wm.After(w.wm) {
		return nil
	}
	w.wm = wm

	var fired []Fired
	for _, st := range w.sorted() {
		if w.expired(st.Window) {
			// A window emptied by an earlier purge has nothing left to say; it
			// is discarded rather than fired, because a pane with no records
			// would still cost an aggregation downstream.
			if len(st.Items) == 0 {
				delete(w.live, st.Window.ID())
				continue
			}
			fired = append(fired, w.fire(st, true, true))
			continue
		}
		if act := w.trig.OnWatermark(st.Window, len(st.Items), w.wm); act != Continue {
			fired = append(fired, w.fire(st, false, act == FirePurge))
		}
	}
	return order(fired)
}

// Drain fires every live window as final, which is what the end of a stream
// means: there is no more evidence coming, so every open question is now
// closed. It leaves the windower empty.
func (w *Windower) Drain() []Fired {
	var fired []Fired
	for _, st := range w.sorted() {
		if len(st.Items) == 0 {
			delete(w.live, st.Window.ID())
			continue
		}
		fired = append(fired, w.fire(st, true, true))
	}
	return order(fired)
}

// expired reports whether the watermark has moved past what win can still
// tolerate. A global window never expires — only a trigger or the end of the
// stream closes it.
func (w *Windower) expired(win Window) bool {
	if win.Global() {
		return false
	}
	return !w.wm.Before(win.End.Add(w.spec.Lateness))
}

// fire emits st's buffer as a pane. final marks it as the window's last word;
// purge discards the buffer, and always does when final.
func (w *Windower) fire(st *windowState, final, purge bool) Fired {
	st.Fires++
	// Late is a property of the firing rather than of the window: this pane
	// carried at least one record that arrived after the watermark had passed
	// the window's end. A window that merely closed on schedule is not late,
	// however long its lateness allowance was.
	late := st.Stragglers > 0
	p := Pane{
		Window: st.Window, Seq: st.Fires, Final: final, Late: late,
		Count: len(st.Items), Watermark: w.wm,
	}
	recs := ordered(st.Items)
	w.stats.Panes++
	if !final {
		w.stats.Early++
	}
	if late {
		w.stats.Late++
	}
	if purge || final {
		st.Items = nil
		st.Stragglers = 0
	}
	if final {
		delete(w.live, st.Window.ID())
	}
	return Fired{Pane: p, Records: recs}
}

// ordered renders a window's buffer as records in event-time order, breaking
// ties on record identity so the result is total rather than merely stable.
func ordered(items []item) []core.Record {
	sorted := make([]item, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool {
		if !sorted[i].At.Equal(sorted[j].At) {
			return sorted[i].At.Before(sorted[j].At)
		}
		return sorted[i].Rec.ID < sorted[j].Rec.ID
	})
	out := make([]core.Record, len(sorted))
	for i, it := range sorted {
		out[i] = it.Rec
	}
	return out
}

// evictOverflow enforces MaxWindows by firing the oldest live windows.
func (w *Windower) evictOverflow() []Fired {
	if w.spec.MaxWindows <= 0 || len(w.live) <= w.spec.MaxWindows {
		return nil
	}
	sorted := w.sorted()
	var fired []Fired
	for i := 0; len(w.live) > w.spec.MaxWindows && i < len(sorted); i++ {
		w.stats.Evicted++
		fired = append(fired, w.fire(sorted[i], true, true))
	}
	return fired
}

// sorted returns the live windows in closing order: by end, then start, then
// key. Deterministic ordering is what makes a pane sequence reproducible across
// a restart, and reproducible panes are what make a sink's writes idempotent.
func (w *Windower) sorted() []*windowState {
	out := make([]*windowState, 0, len(w.live))
	for _, st := range w.live {
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return less(out[i].Window, out[j].Window) })
	return out
}

func order(fired []Fired) []Fired {
	sort.SliceStable(fired, func(i, j int) bool {
		if fired[i].Pane.Window == fired[j].Pane.Window {
			return fired[i].Pane.Seq < fired[j].Pane.Seq
		}
		return less(fired[i].Pane.Window, fired[j].Pane.Window)
	})
	return fired
}

func less(a, b Window) bool {
	switch {
	case !a.End.Equal(b.End):
		return a.End.Before(b.End)
	case !a.Start.Equal(b.Start):
		return a.Start.Before(b.Start)
	default:
		return a.Key < b.Key
	}
}

// snapshot is the checkpointed form of a windower: every live window's buffer,
// its firing history, and the watermark that was reached.
type snapshot struct {
	Watermark time.Time      `json:"watermark"`
	Windows   []*windowState `json:"windows"`
	Stats     WindowStats    `json:"stats"`
}

// Snapshot serializes the windower's state for a checkpoint.
//
// Buffered records are part of it, and that is the point: a window half full
// when a job stopped resumes half full rather than firing on what happened to
// survive. The cost is that a checkpoint is as large as the data in flight,
// which is the same bound the bounded driver materializes anyway.
func (w *Windower) Snapshot() ([]byte, error) {
	return json.Marshal(snapshot{Watermark: w.wm, Windows: w.sorted(), Stats: w.stats})
}

// Restore replaces the windower's state with a snapshot's. The spec is not
// stored and is not checked: restoring a snapshot into a differently-windowed
// stage is a deployment error that Loom cannot detect for you, and a changed
// window is a reason to start a new job rather than resume an old one.
func (w *Windower) Restore(blob []byte) error {
	if len(blob) == 0 {
		return nil
	}
	var snap snapshot
	if err := json.Unmarshal(blob, &snap); err != nil {
		return fmt.Errorf("stream: restoring window state: %w", err)
	}
	w.live = make(map[string]*windowState, len(snap.Windows))
	for _, st := range snap.Windows {
		w.live[st.Window.ID()] = st
	}
	w.wm = snap.Watermark
	w.stats = snap.Stats
	return nil
}
