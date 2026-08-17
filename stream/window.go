package stream

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/zionrubin/loom/core"
)

// Window is a bounded interval of event time, optionally scoped to a key. It is
// half-open: a record at exactly End belongs to the next window, which is what
// makes consecutive tumbling windows partition the timeline rather than overlap
// at their seams.
//
// The zero End means unbounded — the global window, which only a trigger can
// close.
type Window struct {
	Key   string    `json:"key,omitempty"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Global reports whether w has no end and can therefore only be fired by a
// trigger or by the end of the stream.
func (w Window) Global() bool { return w.End.IsZero() }

// Contains reports whether an event at t belongs in w.
func (w Window) Contains(t time.Time) bool {
	if w.Global() {
		return true
	}
	return !t.Before(w.Start) && t.Before(w.End)
}

// ID is w's stable identity: the same window computed on two machines, or
// before and after a restart, produces the same string. Pane identity is built
// on it, and pane identity is what makes a sink write idempotent.
func (w Window) ID() string {
	if w.Global() {
		return "global/" + w.Key
	}
	return strconv.FormatInt(w.Start.UnixNano(), 36) + "-" +
		strconv.FormatInt(w.End.UnixNano(), 36) + "/" + w.Key
}

// String renders the window for logs and reports.
func (w Window) String() string {
	if w.Global() {
		if w.Key == "" {
			return "global"
		}
		return "global[" + w.Key + "]"
	}
	s := w.Start.UTC().Format(time.RFC3339) + ".." + w.End.UTC().Format(time.RFC3339)
	if w.Key != "" {
		s += "[" + w.Key + "]"
	}
	return s
}

// Pane is one firing of one window: the unit of work a stream mode job hands
// downstream, and the unit of output a sink receives.
//
// A window can fire more than once — speculatively, before it closes, or again
// when tolerated late data arrives — so a pane is a window plus which firing
// this is. Seq starts at 1.
type Pane struct {
	Window Window `json:"window"`
	Seq    int    `json:"seq"`
	// Final marks the last firing of this window: the point after which its
	// state is discarded and any further record for it is late. A sink that
	// keeps only one result per window should overwrite until it sees this.
	Final bool `json:"final"`
	// Late marks a firing caused by data that arrived after the window's end
	// but within its tolerated lateness.
	Late bool `json:"late,omitempty"`
	// Count is how many records the firing carried.
	Count int `json:"count"`
	// Watermark is where event time had reached when the pane fired, which is
	// the evidence for the claim that the window was complete.
	Watermark time.Time `json:"watermark"`
}

// ID is the pane's stable identity: window identity plus firing number.
func (p Pane) ID() string { return p.Window.ID() + "#" + strconv.Itoa(p.Seq) }

// String renders the pane for logs and reports.
func (p Pane) String() string {
	s := p.Window.String() + " #" + strconv.Itoa(p.Seq)
	switch {
	case p.Late:
		s += " late"
	case !p.Final:
		s += " early"
	}
	return s
}

// Fired is a pane and the records it carried.
type Fired struct {
	Pane    Pane
	Records []core.Record
}

// Assigner decides which windows a record belongs to, from its event time.
//
// An assigner is pure: same time, same windows, on any machine and after any
// restart. That is what lets a checkpoint store window state by identity rather
// than by whatever the assigner was thinking at the time.
type Assigner interface {
	// Name identifies the assigner in reports and in a stage's fingerprint.
	Name() string
	// Assign returns the windows an event at t belongs to, with Key unset —
	// the windower fills that in. Returning none drops the record.
	Assign(t time.Time) []Window
}

// Tumbling assigns each event to exactly one window of the given size, cut on
// multiples of size from the epoch, so every job in a fleet cuts the same
// boundaries and two jobs' results line up.
func Tumbling(size time.Duration) Assigner {
	if size <= 0 {
		panic("stream.Tumbling: size must be positive")
	}
	return tumbling{size: size}
}

type tumbling struct{ size time.Duration }

func (t tumbling) Name() string { return "tumbling/" + t.size.String() }

func (t tumbling) Assign(at time.Time) []Window {
	start := at.Truncate(t.size)
	return []Window{{Start: start, End: start.Add(t.size)}}
}

// Sliding assigns each event to every window of the given size that covers it,
// spaced slide apart. An event therefore lands in size/slide windows, and the
// cost of the pipeline downstream multiplies by the same factor — which is
// worth saying out loud when the operators are model calls.
func Sliding(size, slide time.Duration) Assigner {
	if size <= 0 || slide <= 0 {
		panic("stream.Sliding: size and slide must be positive")
	}
	if slide > size {
		panic("stream.Sliding: slide must not exceed size (it would drop events)")
	}
	return sliding{size: size, slide: slide}
}

type sliding struct{ size, slide time.Duration }

func (s sliding) Name() string { return "sliding/" + s.size.String() + "/" + s.slide.String() }

func (s sliding) Assign(at time.Time) []Window {
	// The last window that can contain at starts at the slide boundary at or
	// before it; earlier ones follow until the window no longer reaches.
	last := at.Truncate(s.slide)
	var out []Window
	for start := last; !at.Before(start) && at.Before(start.Add(s.size)); start = start.Add(-s.slide) {
		out = append(out, Window{Start: start, End: start.Add(s.size)})
	}
	// Oldest first, so downstream sees windows in the order they will close.
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

// GlobalWindow assigns every event to one window that never ends. It is what
// you want with a count or interval trigger — an aggregate every thousand
// records, or every five minutes of wall clock, with no event-time meaning at
// all — and it is what a job uses when it wants a whole stream folded once at
// the end.
func GlobalWindow() Assigner { return global{} }

type global struct{}

func (global) Name() string { return "global" }

func (global) Assign(time.Time) []Window { return []Window{{}} }

// Action is a trigger's verdict on a window.
type Action uint8

const (
	// Continue leaves the window buffering.
	Continue Action = iota
	// Fire emits the buffered records as a pane and keeps them, so a later
	// firing sees them again. Use it for speculative output that will be
	// superseded.
	Fire
	// FirePurge emits the buffered records and discards them, so the next
	// firing sees only what arrived since. Use it for output that accumulates
	// downstream.
	FirePurge
)

// Trigger decides when a window fires.
//
// The default — AtWatermark — fires each window once, when event time has moved
// past its end plus whatever lateness it tolerates. Everything else here is for
// deliberately trading completeness against latency, and the trade is sharper
// in Loom than in a classic stream engine: a firing is a model call, so a
// trigger that fires four times has quadrupled the bill rather than the CPU.
type Trigger interface {
	// Name identifies the trigger in reports.
	Name() string
	// OnRecord is consulted when a record joins a window, with the resulting
	// buffer size and the current watermark.
	OnRecord(w Window, buffered int, wm time.Time) Action
	// OnWatermark is consulted when the watermark advances. It is not called
	// for a window whose end the watermark has already passed — the windower
	// closes those itself.
	OnWatermark(w Window, buffered int, wm time.Time) Action
}

// AtWatermark fires each window once, when the windower judges it complete. It
// is the default and the only trigger that is purely event-time driven.
func AtWatermark() Trigger { return atWatermark{} }

type atWatermark struct{}

func (atWatermark) Name() string                              { return "at-watermark" }
func (atWatermark) OnRecord(Window, int, time.Time) Action    { return Continue }
func (atWatermark) OnWatermark(Window, int, time.Time) Action { return Continue }

// AtCount fires a window every n records, purging as it goes, which turns the
// global window into fixed-size batches. Combined with a real event-time
// window it emits a speculative pane every n records and still closes the
// window on the watermark.
func AtCount(n int) Trigger {
	if n <= 0 {
		panic("stream.AtCount: n must be positive")
	}
	return atCount{n: n}
}

type atCount struct{ n int }

func (c atCount) Name() string { return "at-count/" + strconv.Itoa(c.n) }

func (c atCount) OnRecord(_ Window, buffered int, _ time.Time) Action {
	if buffered >= c.n {
		return FirePurge
	}
	return Continue
}

func (atCount) OnWatermark(Window, int, time.Time) Action { return Continue }

// AtInterval fires a window each time event time advances another d past its
// start, emitting a speculative pane that a later firing supersedes. The
// records are kept, so each pane holds everything the window has seen so far.
func AtInterval(d time.Duration) Trigger {
	if d <= 0 {
		panic("stream.AtInterval: d must be positive")
	}
	return &atInterval{d: d}
}

type atInterval struct {
	d time.Duration
	// next is keyed by window identity rather than held per window, because a
	// trigger instance is shared by every window of a stage.
	next map[string]time.Time
}

func (i *atInterval) Name() string { return "at-interval/" + i.d.String() }

func (i *atInterval) OnRecord(Window, int, time.Time) Action { return Continue }

func (i *atInterval) OnWatermark(w Window, buffered int, wm time.Time) Action {
	if buffered == 0 {
		return Continue
	}
	if i.next == nil {
		i.next = map[string]time.Time{}
	}
	id := w.ID()
	due, ok := i.next[id]
	if !ok {
		due = w.Start.Add(i.d)
		if w.Global() {
			due = wm.Add(i.d)
		}
		i.next[id] = due
		return Continue
	}
	if wm.Before(due) {
		return Continue
	}
	for !wm.Before(i.next[id]) {
		i.next[id] = i.next[id].Add(i.d)
	}
	return Fire
}

// Any fires when any of the given triggers does, taking the strongest action
// any of them asked for.
func Any(ts ...Trigger) Trigger { return anyOf{ts: ts} }

type anyOf struct{ ts []Trigger }

func (a anyOf) Name() string {
	names := make([]string, len(a.ts))
	for i, t := range a.ts {
		names[i] = t.Name()
	}
	return "any(" + join(names, ",") + ")"
}

func (a anyOf) OnRecord(w Window, n int, wm time.Time) Action {
	out := Continue
	for _, t := range a.ts {
		if act := t.OnRecord(w, n, wm); act > out {
			out = act
		}
	}
	return out
}

func (a anyOf) OnWatermark(w Window, n int, wm time.Time) Action {
	out := Continue
	for _, t := range a.ts {
		if act := t.OnWatermark(w, n, wm); act > out {
			out = act
		}
	}
	return out
}

func join(parts []string, sep string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += sep
		}
		out += p
	}
	return out
}

// Limit bounds how long a stream job runs. Every field is optional and any one
// of them reached stops the job cleanly, with a final checkpoint.
//
// It exists because "runs forever" is an operational property and not a testing
// one. The same job, the same windows and the same code run over two hundred
// records in a test and over a topic in production, which is the only way the
// test is evidence about the production job.
type Limit struct {
	// Records stops the job after this many events have been ingested.
	Records int64
	// Panes stops it after this many window firings.
	Panes int64
	// Duration stops it after this much wall clock.
	Duration time.Duration
}

// Zero reports whether no bound is set, which is the production case.
func (l Limit) Zero() bool { return l.Records == 0 && l.Panes == 0 && l.Duration == 0 }

// LatePolicy says what becomes of a record that arrives for a window already
// discarded.
type LatePolicy uint8

const (
	// DropLate discards the record and counts it. A late record is a fact
	// about the source, and a job that silently absorbed it would be hiding
	// the one number that tells you your lateness bound is wrong.
	DropLate LatePolicy = iota
	// FailLate stops the job. For streams where lateness means corruption
	// rather than delay.
	FailLate
)

// WindowSpec declares how a stage cuts its input into windows. It is the
// argument to pipeline.Dataset.Window.
type WindowSpec struct {
	// Assigner cuts event time into windows (default: one global window,
	// which without a trigger fires only when the stream ends).
	Assigner Assigner
	// Key scopes windows to a partition of the data — per customer, per region,
	// per session — so each key's window closes and fires on its own. Nil means
	// one window per interval for the whole stream.
	//
	// The number of live windows is bounded by keys times intervals, and each
	// one that fires is a model call downstream, so a high-cardinality key is a
	// bill rather than a memory problem.
	Key func(core.Record) string
	// Time overrides the event time carried by the ingested event. Use it when
	// an upstream stage produced the timestamp that matters — a model that
	// extracted an incident time from free text, say — rather than the source.
	Time func(core.Record) time.Time
	// Lateness is how far past a window's end event time must advance before
	// the window fires. Records arriving in that grace period join the window
	// they belong to and are included in its one firing; records arriving after
	// it are late, and LatePolicy applies.
	//
	// It buys completeness with latency, and unlike a classic stream engine it
	// does not buy it with money: the window still fires exactly once. See
	// docs/STREAMING.md for why re-firing is opt-in here.
	Lateness time.Duration
	// Trigger fires windows before they close (default AtWatermark: fire once,
	// when complete).
	Trigger Trigger
	// MaxRecords caps how many records one window may buffer. Past it the
	// window fires early and purges, which bounds the memory a hot key or a
	// stalled watermark can consume. Zero leaves it unbounded.
	MaxRecords int
	// MaxWindows caps how many windows may be live at once. Past it the oldest
	// fires and is discarded. Zero leaves it unbounded.
	MaxWindows int
	// Late says what happens to records that arrive after their window is gone.
	Late LatePolicy
}

// Validate reports whether the spec is coherent, and is called at compile time
// so a misconfigured window fails before a source is opened rather than at the
// first record.
func (s WindowSpec) Validate() error {
	if s.Lateness < 0 {
		return fmt.Errorf("window: negative lateness %s", s.Lateness)
	}
	if s.MaxRecords < 0 || s.MaxWindows < 0 {
		return fmt.Errorf("window: negative bound")
	}
	if _, ok := s.assigner().(global); ok && s.Trigger == nil && s.MaxRecords == 0 {
		return fmt.Errorf("window: a global window with no trigger and no MaxRecords " +
			"fires only when the stream ends; give it stream.AtCount(n), " +
			"stream.AtInterval(d), or an event-time assigner")
	}
	return nil
}

func (s WindowSpec) assigner() Assigner {
	if s.Assigner == nil {
		return GlobalWindow()
	}
	return s.Assigner
}

func (s WindowSpec) trigger() Trigger {
	if s.Trigger == nil {
		return AtWatermark()
	}
	return s.Trigger
}

// Describe renders the spec for stage detail and reports.
func (s WindowSpec) Describe() string {
	out := s.assigner().Name() + " windows, " + s.trigger().Name()
	if s.Key != nil {
		out += ", keyed"
	}
	if s.Lateness > 0 {
		out += ", tolerating " + s.Lateness.String() + " of lateness"
	}
	if s.MaxRecords > 0 {
		out += ", at most " + strconv.Itoa(s.MaxRecords) + " records per window"
	}
	if s.MaxWindows > 0 {
		out += ", at most " + strconv.Itoa(s.MaxWindows) + " live windows"
	}
	return out
}
