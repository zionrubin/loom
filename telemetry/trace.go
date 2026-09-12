package telemetry

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"sort"
	"sync"
	"time"
)

// This file is the trace side: the run, its stages, its tasks and the model
// calls under them, as a span tree.
//
// Two decisions shape everything here.
//
// The first is that identifiers are *derived* rather than generated. A trace
// ID is a hash of the run ID; a stage's span ID is a hash of the run ID and
// the stage name. Nothing is random, and that buys two things a random
// generator cannot. A run ID printed in a RunResult, in a report, in a log
// line or on the constellation view is enough to find the trace — the
// operator does not need a correlation field that nobody remembered to log.
// And a worker process that never saw the driver's context still computes the
// same trace ID from the run ID its envelope already carries, so a fleet's
// spans assemble into one trace with no propagation header anywhere. Task and
// call spans mix in the process instance, so the driver's view of a task
// (dispatch, queue, wait) and the worker's view of it (the call) are two
// spans under one stage rather than two writers fighting over one ID.
//
// The second is that sampling decides at the *end* of a task rather than at
// its start. A run of ten thousand records is ten thousand task spans, and
// nobody wants them; but the interesting ones are exactly the ones a head
// sampler cannot recognize yet — the task that failed, the one that climbed
// the escalation ladder, the one that took four minutes. So spans are held
// until their task settles and the decision is made with the outcome in hand:
// a ratio for the ordinary ones, and everything for the rest.

// SpanKind mirrors OpenTelemetry's span kinds, narrowed to the two Loom
// produces: work inside the process, and a call to something outside it.
type SpanKind int

const (
	SpanInternal SpanKind = 1
	SpanClient   SpanKind = 3
)

// StatusCode mirrors OpenTelemetry's span status.
type StatusCode int

const (
	StatusUnset StatusCode = 0
	StatusOK    StatusCode = 1
	StatusError StatusCode = 2
)

// Attr is one span attribute. Values are string, int64, float64 or bool;
// anything else is rendered as a string.
type Attr struct {
	Key   string
	Value any
}

// SpanEvent is a timestamped annotation inside a span — a retry, a cache
// replay, a budget trip.
type SpanEvent struct {
	Name  string
	Time  time.Time
	Attrs []Attr
}

// Span is one finished unit of work, ready to export.
type Span struct {
	TraceID  string // 32 hex characters
	SpanID   string // 16 hex characters
	ParentID string // 16 hex characters, empty at the root
	Name     string
	Kind     SpanKind
	Start    time.Time
	End      time.Time
	Attrs    []Attr
	Events   []SpanEvent
	Status   StatusCode
	Message  string
}

// Exporter receives finished spans. Export is called from a single background
// goroutine and may block; Shutdown is called once, with whatever deadline the
// caller of Close allowed.
type Exporter interface {
	Export(ctx context.Context, spans []Span) error
	Shutdown(ctx context.Context) error
}

// Sampling decides which task spans survive.
//
// Ratio is applied to a hash of the span's own ID, so the decision is
// deterministic: the same task in a replayed event stream is kept or dropped
// the same way, and two processes tracing the same run do not disagree about
// which tasks are interesting.
type Sampling struct {
	// Ratio is the share of ordinary tasks kept, in [0,1]. Zero keeps only
	// the tasks the rules below force; one keeps everything.
	Ratio float64
	// Slow keeps every task at least this slow, whatever the ratio says.
	// Zero disables the rule.
	Slow time.Duration
	// Errors keeps every task that failed, retried, or climbed the
	// escalation ladder. It defaults to true and there is no good reason to
	// turn it off; the field exists so that a deployment drowning in a known
	// failure can stop paying to trace it.
	Errors *bool
}

func (s Sampling) keepErrors() bool { return s.Errors == nil || *s.Errors }

// keep decides whether a settled task's span survives.
func (s Sampling) keep(spanID string, interesting bool, dur time.Duration) bool {
	if interesting && s.keepErrors() {
		return true
	}
	if s.Slow > 0 && dur >= s.Slow {
		return true
	}
	if s.Ratio >= 1 {
		return true
	}
	if s.Ratio <= 0 {
		return false
	}
	return fraction(spanID) < s.Ratio
}

// fraction maps a span ID onto [0,1) by reading its first eight bytes as a
// big-endian integer.
func fraction(spanID string) float64 {
	raw, err := hex.DecodeString(spanID)
	if err != nil || len(raw) < 8 {
		return 1 // an ID we cannot read is never sampled away silently
	}
	return float64(binary.BigEndian.Uint64(raw[:8])) / math.MaxUint64
}

// TraceIDFor returns the trace ID a run's spans carry. It is exported because
// it is the whole point of deriving rather than generating: given a run ID
// from a report, a log line or the constellation view, this is the string to
// paste into a tracing backend.
func TraceIDFor(runID string) string {
	sum := sha256.Sum256([]byte("loom/trace\x00" + runID))
	return hex.EncodeToString(sum[:16])
}

// spanIDFor derives a span ID from the run and a key. Keys that name
// something the whole fleet agrees on — the run, a stage — are derived from
// the run ID alone, so every process computes the same one; keys that name
// this process's own view of a task mix in the instance.
func spanIDFor(runID, key string) string {
	sum := sha256.Sum256([]byte("loom/span\x00" + runID + "\x00" + key))
	return hex.EncodeToString(sum[:8])
}

// openSpan is a span still accumulating.
type openSpan struct {
	Span
	children []Span // held until the parent's sampling decision is made
}

func (o *openSpan) attr(key string, value any) {
	if value == nil {
		return
	}
	o.Attrs = append(o.Attrs, Attr{Key: key, Value: value})
}

func (o *openSpan) event(name string, at time.Time, attrs ...Attr) {
	o.Events = append(o.Events, SpanEvent{Name: name, Time: at, Attrs: attrs})
}

// finish sorts a span's attributes and returns it plus its held children, so
// that two identical runs export byte-identical payloads.
func (o *openSpan) finish(end time.Time) []Span {
	if o.End.IsZero() {
		o.End = end
	}
	sort.SliceStable(o.Attrs, func(i, j int) bool { return o.Attrs[i].Key < o.Attrs[j].Key })
	out := make([]Span, 0, len(o.children)+1)
	out = append(out, o.Span)
	out = append(out, o.children...)
	return out
}

// tracer batches finished spans and hands them to an Exporter from one
// background goroutine, so nothing on the event bus ever waits on a network.
//
// The queue is bounded and drops when full. That is the only defensible
// behaviour for telemetry attached to a run: a collector that has gone away
// must cost the run a metric about lost spans, never a stalled pipeline.
type tracer struct {
	exp      Exporter
	batch    int
	interval time.Duration

	queue chan Span
	flush chan chan struct{}
	stop  chan struct{}
	done  chan struct{}

	// onDrop and onFail report the tracer's own health back to the metric
	// registry, which is where an operator will look for it.
	onDrop func()
	onFail func()

	stopOnce sync.Once
}

func newTracer(exp Exporter, queue, batch int, interval time.Duration, onDrop, onFail func()) *tracer {
	t := &tracer{
		exp: exp, batch: batch, interval: interval,
		queue:  make(chan Span, queue),
		flush:  make(chan chan struct{}),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		onDrop: onDrop, onFail: onFail,
	}
	go t.loop()
	return t
}

// emit queues spans without ever blocking the caller.
func (t *tracer) emit(spans []Span) {
	for _, s := range spans {
		select {
		case t.queue <- s:
		default:
			t.onDrop()
		}
	}
}

func (t *tracer) loop() {
	defer close(t.done)
	tick := time.NewTicker(t.interval)
	defer tick.Stop()
	pending := make([]Span, 0, t.batch)

	send := func() {
		if len(pending) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := t.exp.Export(ctx, pending); err != nil {
			t.onFail()
		}
		cancel()
		pending = pending[:0]
	}
	drain := func() {
		for {
			select {
			case s := <-t.queue:
				pending = append(pending, s)
			default:
				return
			}
		}
	}

	for {
		select {
		case s := <-t.queue:
			pending = append(pending, s)
			if len(pending) >= t.batch {
				send()
			}
		case <-tick.C:
			send()
		case ack := <-t.flush:
			drain()
			send()
			close(ack)
		case <-t.stop:
			drain()
			send()
			return
		}
	}
}

// waitFlush exports everything queued so far and returns once it has been
// handed to the exporter.
func (t *tracer) waitFlush(ctx context.Context) error {
	ack := make(chan struct{})
	select {
	case t.flush <- ack:
	case <-t.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *tracer) shutdown(ctx context.Context) error {
	t.stopOnce.Do(func() { close(t.stop) })
	select {
	case <-t.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	return t.exp.Shutdown(ctx)
}
