// Package stream is Loom's vocabulary for unbounded input: the sources records
// arrive from, the windows that make an endless stream finite enough to
// aggregate, the sinks results leave through, and the checkpoints that let a
// job stop and start again without losing work or paying for it twice.
//
// A Loom pipeline is bounded because its aggregates are. ReduceAI folds a set,
// Combine folds a set, Iterate cannot start a superstep before every vertex's
// mail has arrived; each of them needs to know that the input is complete, and
// on a finite dataset the end of the input is what tells them. A stream has no
// end, so something else has to say when a set is closed. That something is a
// window, and the watermark is how a window knows.
//
// Everything in this package serves that one sentence. A Source produces events
// carrying an event time and a resumable position. Watermarks turn those times
// into a claim about what can still arrive. A Windower turns that claim into
// closed sets. A Sink makes a closed set's result durable, and a Checkpoint ties
// the three together into a point a job can be restarted from.
//
// Nothing here executes a pipeline: the types are plain data and small
// interfaces, so a source, a window, or a sink can be written and tested
// without a model, a scheduler, or a run anywhere in sight. loom.Stream is what
// binds them to a pipeline.
package stream

import (
	"context"
	"errors"
	"time"

	"github.com/zionrubin/loom/core"
)

// Position is a resume point inside a split: everything at or before it has
// been read. It is opaque to Loom, which only stores it in checkpoints and
// hands it back to Source.Open on restart, so a source may use whichever of the
// fields fits its addressing — a Kafka offset, a byte offset in a file, an
// opaque continuation token from an API.
//
// It must survive a JSON round trip, because that is how it reaches a
// checkpoint and comes back.
type Position struct {
	// Offset is a numeric position: a Kafka offset, a byte offset, a row
	// number. Sources that resume by number should use it, because it is the
	// one field a reader of a checkpoint file can interpret.
	Offset int64 `json:"offset,omitempty"`
	// Token is a position that is not a number — a cursor, a continuation
	// token, an ETag.
	Token string `json:"token,omitempty"`
	// Meta carries anything else the source needs to resume.
	Meta map[string]any `json:"meta,omitempty"`
}

// Zero reports whether p carries no position at all, which is what Source.Open
// is given when a split has never been read and should start wherever the
// source's own policy says.
func (p Position) Zero() bool {
	return p.Offset == 0 && p.Token == "" && len(p.Meta) == 0
}

// Split is one independently readable, independently resumable part of a
// source: a topic partition, a file, a shard.
//
// Splits are what let ingestion be parallel and what make watermarks honest.
// Each is read by one reader that tracks its own position and its own event-time
// progress, and the job's watermark is the *minimum* across them — so a
// partition that has fallen behind holds its windows open instead of letting
// them close on data that has not arrived yet.
type Split struct {
	ID   string         `json:"id"`
	Meta map[string]any `json:"meta,omitempty"`
}

// Event is one record as it arrives, with the two things a stream needs of it
// that a batch does not: when it happened, and where the reader was when it
// produced it.
type Event struct {
	// Record is the payload, and is what the pipeline sees. Everything else on
	// this struct belongs to the streaming machinery and is stripped before the
	// record reaches a stage.
	Record core.Record
	// Time is the event time: when the thing described by the record happened,
	// which is generally not when Loom read it. Windows are cut on this. A zero
	// Time means the source has no notion of one, and the job substitutes the
	// time of ingestion — correct only if you are willing to have replays land
	// in different windows than the original.
	Time time.Time
	// Pos is the position *after* this event: committing it means this event
	// and everything before it will not be read again.
	Pos Position
}

// Source is an unbounded, partitioned, resumable reader.
//
// It is pulled rather than pushed. A push source would have to be told to stop
// when the pipeline is full, and every source would have to implement that
// correctly; a pulled one is backpressured by construction, because nothing
// arrives that the job did not ask for. It is the same reason a stage takes
// records off a pipe rather than being handed them.
type Source interface {
	// Splits enumerates the parts of this source that can be read
	// independently. It may be called more than once — a job re-reads it to
	// discover partitions that appeared while it was running — and should
	// return the current set each time.
	Splits(ctx context.Context) ([]Split, error)

	// Open begins reading a split. A zero from means "wherever this source
	// starts by default"; anything else is a position this source previously
	// reported, handed back after a restart.
	Open(ctx context.Context, sp Split, from Position) (Reader, error)

	// Close releases whatever the source holds. Readers are closed first.
	Close() error
}

// Reader reads one split.
//
// A reader is used by exactly one goroutine, so it needs no internal locking.
type Reader interface {
	// Read returns up to max events, blocking for at most wait. Returning zero
	// events and a nil error is normal and means "nothing yet" — a live stream
	// spends most of its time doing exactly that, and it is how a reader
	// declares itself idle so its split stops holding the watermark back.
	//
	// It reports ErrSplitDone when this split will never produce again: a file
	// that has been fully read, a partition revoked by a group rebalance. The
	// split is then retired and stops holding the watermark back.
	Read(ctx context.Context, max int, wait time.Duration) ([]Event, error)

	// Commit records that everything up to pos has been processed *and made
	// durable downstream*. It is called after a checkpoint completes, never
	// before, because that is the only moment at which the claim is true.
	//
	// Sources that keep their own position (a Kafka consumer group) write it
	// here. Sources whose position lives only in Loom's checkpoint may do
	// nothing.
	Commit(ctx context.Context, pos Position) error

	// Close releases the reader. A closed reader's split may be reopened later
	// from its last committed position.
	Close() error
}

// ErrSplitDone is reported by Reader.Read when a split has ended: it will
// produce no further events, and the job may retire it.
var ErrSplitDone = errors.New("stream: split done")

// Batch is one pane's worth of output arriving at a sink.
type Batch struct {
	// Stage is the pipeline stage whose output this is.
	Stage string
	// Pane identifies the window and the firing. Together with Stage it is
	// what makes a write idempotent: the same pane written twice after a
	// restart carries the same identity, so a sink can overwrite rather than
	// append.
	Pane Pane
	// Epoch is the checkpoint this batch belongs to. A sink that stages writes
	// and commits them transactionally keys its transaction on this.
	Epoch int64
	// Records are the stage's output for this pane.
	Records []core.Record
}

// Key returns the batch's idempotency key: stable across restarts, distinct
// across panes, and safe to use as a filename, a message key, or a primary key.
func (b Batch) Key() string { return b.Stage + "/" + b.Pane.ID() }

// Sink is where a job's results leave it.
//
// The contract is deliberately small, and the interesting half of it is the
// relationship between Write and Commit. Write is called once per pane per
// terminal stage, as panes fire. Commit is called after a checkpoint has been
// durably recorded, and means: everything written since the last Commit is now
// covered by a checkpoint, and the source positions behind it are about to be
// advanced.
//
// A sink that writes directly — a file appended, a topic produced to — can
// ignore Commit and be at-least-once: a crash between a write and a checkpoint
// replays the pane, and Batch.Key is what lets the destination recognize it. A
// sink that stages its writes and makes them visible in Commit is
// exactly-once, at the cost of holding uncommitted output for a checkpoint
// interval.
type Sink interface {
	// Write makes a pane's records durable. It may be called concurrently for
	// different panes.
	Write(ctx context.Context, b Batch) error
	// Commit is called after checkpoint epoch has been recorded.
	Commit(ctx context.Context, epoch int64) error
	// Close flushes and releases. It is called once, after the last Write.
	Close() error
}

// SinkFunc adapts a plain function to Sink, for the common case of a
// destination with nothing to commit and nothing to close.
type SinkFunc func(ctx context.Context, b Batch) error

func (f SinkFunc) Write(ctx context.Context, b Batch) error { return f(ctx, b) }
func (f SinkFunc) Commit(context.Context, int64) error      { return nil }
func (f SinkFunc) Close() error                             { return nil }

// EventTime returns an event-time extractor reading a record field, for the
// common case of a timestamp travelling in the payload. It understands RFC3339
// strings, and seconds or milliseconds since the epoch as numbers.
//
// A record whose field is missing or unparseable gets a zero time, which the
// job replaces with the time of ingestion.
func EventTime(field string) func(core.Record) time.Time {
	return func(r core.Record) time.Time {
		switch v := r.Data[field].(type) {
		case time.Time:
			return v
		case string:
			for _, layout := range []string{time.RFC3339Nano, time.RFC3339, time.DateTime} {
				if t, err := time.Parse(layout, v); err == nil {
					return t
				}
			}
		case float64:
			return epoch(int64(v))
		case int64:
			return epoch(v)
		case int:
			return epoch(int64(v))
		}
		return time.Time{}
	}
}

// epoch interprets a number as seconds, milliseconds, or microseconds since the
// epoch, by magnitude. The thresholds are three orders of magnitude apart, so
// the guess is only wrong for timestamps outside the years this will run in.
func epoch(n int64) time.Time {
	switch {
	case n == 0:
		return time.Time{}
	case n > 1e17: // nanoseconds
		return time.Unix(0, n).UTC()
	case n > 1e14: // microseconds
		return time.UnixMicro(n).UTC()
	case n > 1e11: // milliseconds
		return time.UnixMilli(n).UTC()
	default:
		return time.Unix(n, 0).UTC()
	}
}
