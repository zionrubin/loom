package recall

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/stream"
)

// Message is one turn of one conversation: the unit a desk ingests.
//
// It is deliberately thin. A desk does not model participants, threading,
// attachments or reactions, because the thing downstream of ingestion is a
// model call and a model call reads text. What a desk needs of a message is
// what conversation it belongs to, when it happened, and what was said — the
// first so slices are cut per conversation, the second so they are cut on event
// time rather than on arrival, and the third because it is the payload.
type Message struct {
	// Conversation groups messages into one thread. Empty means the message
	// stands alone and gets a conversation of its own.
	Conversation string `json:"conversation"`
	// ID is the message's identity within the feed. Empty is filled in on
	// ingest, at the cost of making a replay a different record — supply one
	// for anything that might be delivered twice.
	ID string `json:"id,omitempty"`
	// Role is who is speaking in the abstract: "user", "assistant", "agent",
	// "support", whatever the feed's vocabulary is. It reaches the model.
	Role string `json:"role,omitempty"`
	// Speaker names the participant, when the feed knows one.
	Speaker string `json:"speaker,omitempty"`
	// Text is what was said.
	Text string `json:"text"`
	// At is the event time. Zero is filled in with the time of ingest, which
	// is correct for a live feed and wrong for a backfill — a replayed message
	// with no timestamp lands in whatever window it is read in rather than the
	// one it happened in.
	At time.Time `json:"at,omitempty"`
	// Meta travels with the message into the record and is available to the
	// prompts.
	Meta map[string]any `json:"meta,omitempty"`
}

// Record renders a message as the record a pipeline stage sees.
//
// The field names are the ones the default prompts template against, so a
// replaced Spec.Read is written against this shape.
func (m Message) Record() core.Record {
	data := map[string]any{
		"conversation": m.Conversation,
		"role":         m.Role,
		"speaker":      m.Speaker,
		"text":         m.Text,
		"at":           m.At.UTC().Format(time.RFC3339Nano),
	}
	for k, v := range m.Meta {
		if _, taken := data[k]; !taken {
			data[k] = v
		}
	}
	return core.NewRecord(m.ID, data)
}

// Conversation is a batch of messages that belong together, for the caller who
// has a whole exchange rather than a turn.
type Conversation struct {
	ID       string         `json:"id"`
	Messages []Message      `json:"messages"`
	Meta     map[string]any `json:"meta,omitempty"`
}

// normalize fills in what a conversation implies about its messages: the
// conversation ID they belong to and the metadata they inherit.
func (c Conversation) normalize() []Message {
	out := make([]Message, 0, len(c.Messages))
	for _, m := range c.Messages {
		if m.Conversation == "" {
			m.Conversation = c.ID
		}
		if len(c.Meta) > 0 {
			merged := make(map[string]any, len(c.Meta)+len(m.Meta))
			for k, v := range c.Meta {
				merged[k] = v
			}
			for k, v := range m.Meta {
				merged[k] = v
			}
			m.Meta = merged
		}
		out = append(out, m)
	}
	return out
}

// --- the feed ------------------------------------------------------------

// ErrFeedClosed is returned by Feed.Post after the feed has been closed.
var ErrFeedClosed = errors.New("recall: feed closed")

// Feed is an in-process conversation source: messages handed to it by a caller
// rather than pulled from a broker.
//
// It exists because the most common way a serving layer receives conversations
// is a function call — an HTTP handler, a bot's callback, an agent finishing a
// turn — and making that shape reach a stream job should not require standing
// up Kafka. It is a stream.Source like any other, so the desk that reads it
// cannot tell the difference.
//
// What it is not is durable. A Feed holds messages in memory between Post and
// the job reading them, and a position in it means nothing to a process that
// has restarted: a desk resuming from a checkpoint will find the feed empty at
// whatever offset it asks for and carry on from there, having lost whatever was
// posted and not yet digested. That is the right trade for a live feed and the
// wrong one for a system of record, and the fix is not to make Feed durable but
// to point Config.Source at a source that already is — a directory of JSONL
// (stream/file), a topic (stream/kafka) — with the durable write happening
// before the desk ever sees it.
type Feed struct {
	splits int
	quiet  time.Duration

	mu     sync.Mutex
	cond   *sync.Cond
	queues [][]stream.Event
	// seq counts every message ever posted, and is the position within a
	// split: monotonic, and meaningful only while this process lives.
	seq    []int64
	posted int64
	// last is when a message was last posted, and ticked when each split last
	// emitted an idle heartbeat.
	last   time.Time
	ticked []time.Time
	closed bool
}

// NewFeed returns a feed partitioned into n splits (minimum one), declaring
// itself quiet after quiet with nothing posted.
//
// Splits are how ingestion parallelizes and how the watermark stays honest, and
// for an in-process feed they are also how one slow conversation avoids holding
// up every other: a message is assigned to a split by its conversation ID, so a
// conversation is read in order and different conversations are not read behind
// each other.
//
// See Feed.Post for what quiet does, and why a feed needs the notion at all.
func NewFeed(n int, quiet time.Duration) *Feed {
	if n < 1 {
		n = 1
	}
	f := &Feed{
		splits: n, quiet: quiet,
		queues: make([][]stream.Event, n),
		seq:    make([]int64, n),
		ticked: make([]time.Time, n),
	}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// Post hands messages to the feed and returns how many messages the feed has
// accepted in its whole life — the number to hand Desk.Await to wait until
// exactly these have been accounted for.
//
// # Why a feed has a notion of being quiet
//
// A window closes when event time advances past its end, and event time
// advances because messages arrive. So the last slice of a conversation stays
// open until somebody says something else — which on a busy feed is no time at
// all, and on a feed that has gone quiet is forever. A desk whose newest
// conversation only became queryable once an unrelated one started would be a
// desk nobody could trust.
//
// A quiet feed resolves it by saying so. After the configured quiet period with
// nothing posted, each split emits a heartbeat carrying the present as its
// event time, which advances the watermark and closes every window whose end
// has passed. The claim it makes is the one a live feed is entitled to make —
// nothing further will arrive stamped earlier than now — and it is exactly the
// claim a backfill is not entitled to make: replaying an archive through a Feed
// with a long pause in the middle will have that pause read as "that was all of
// it", and everything after it as late. Replay through a durable source
// instead, where positions and event times mean what they say.
func (f *Feed) Post(msgs ...Message) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return f.posted, ErrFeedClosed
	}
	now := time.Now().UTC()
	for _, m := range msgs {
		if m.Conversation == "" {
			m.Conversation = "conv_" + m.ID
		}
		if m.ID == "" {
			m.ID = core.NewID("msg")
		}
		if m.At.IsZero() {
			m.At = now
		}
		i := splitOf(m.Conversation, f.splits)
		f.seq[i]++
		f.posted++
		f.queues[i] = append(f.queues[i], stream.Event{
			Record: m.Record(),
			Time:   m.At,
			Pos:    stream.Position{Offset: f.seq[i]},
		})
	}
	f.last = now
	f.cond.Broadcast()
	return f.posted, nil
}

// Posted returns how many messages the feed has accepted.
func (f *Feed) Posted() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.posted
}

// Splits implements stream.Source.
func (f *Feed) Splits(context.Context) ([]stream.Split, error) {
	out := make([]stream.Split, f.splits)
	for i := range out {
		out[i] = stream.Split{ID: fmt.Sprintf("feed-%d", i)}
	}
	return out, nil
}

// Open implements stream.Source.
//
// The from position is accepted and ignored, which is the honest behaviour for
// a source with no history: a restarting job asks to resume at an offset whose
// messages this process never held, and the only thing it can be given is
// what has been posted since.
func (f *Feed) Open(_ context.Context, sp stream.Split, _ stream.Position) (stream.Reader, error) {
	var i int
	if _, err := fmt.Sscanf(sp.ID, "feed-%d", &i); err != nil || i < 0 || i >= f.splits {
		return nil, fmt.Errorf("recall: feed has no split %q", sp.ID)
	}
	return &feedReader{f: f, i: i}, nil
}

// Close implements stream.Source: it ends the feed, which retires its splits
// and lets a job that was reading it drain and stop.
func (f *Feed) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		f.cond.Broadcast()
	}
	return nil
}

// feedReader reads one of a feed's splits.
type feedReader struct {
	f *Feed
	i int
}

// Read implements stream.Reader. It blocks up to wait for events rather than
// spinning, and reports ErrSplitDone once the feed is closed and drained.
func (r *feedReader) Read(ctx context.Context, max int, wait time.Duration) ([]stream.Event, error) {
	f := r.f
	deadline := time.Now().Add(wait)

	// A condition variable cannot be woken by a context, so a waiter is woken
	// by both: the timer bounds the wait, and the context's own broadcast is
	// what makes cancellation prompt rather than eventual.
	stop := context.AfterFunc(ctx, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.cond.Broadcast()
	})
	defer stop()

	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.queues[r.i]) == 0 && !f.closed && ctx.Err() == nil && wait > 0 {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		timer := time.AfterFunc(remaining, func() {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.cond.Broadcast()
		})
		f.cond.Wait()
		timer.Stop()
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	q := f.queues[r.i]
	if len(q) == 0 {
		if f.closed {
			return nil, stream.ErrSplitDone
		}
		if tick, ok := f.heartbeatLocked(r.i); ok {
			return []stream.Event{tick}, nil
		}
		return nil, nil
	}
	if max > 0 && len(q) > max {
		f.queues[r.i] = q[max:]
		return q[:max:max], nil
	}
	f.queues[r.i] = nil
	return q, nil
}

// Commit implements stream.Reader. A feed's position lives only in Loom's
// checkpoint, so there is nothing here to record.
func (r *feedReader) Commit(context.Context, stream.Position) error { return nil }

// Close implements stream.Reader.
func (r *feedReader) Close() error { return nil }

// splitOf hashes a conversation onto a split, so one conversation is always
// read by one reader and therefore always read in order.
func splitOf(conversation string, n int) int {
	if n <= 1 {
		return 0
	}
	var h uint32 = 2166136261
	for i := 0; i < len(conversation); i++ {
		h ^= uint32(conversation[i])
		h *= 16777619
	}
	return int(h % uint32(n))
}

// tickMeta marks a heartbeat: an event that exists to move event time and
// carries no message. The ingest pipeline drops it before anything reads it.
const tickMeta = "recall.tick"

// heartbeatLocked returns this split's idle heartbeat, if one is due.
//
// One per split, because the watermark is a minimum across them: a heartbeat on
// one split while another still holds the line would move nothing. And at most
// one per quiet period per split, because the point is to unstick event time
// rather than to keep a stream of empty records flowing through the pipeline.
func (f *Feed) heartbeatLocked(i int) (stream.Event, bool) {
	if f.quiet <= 0 {
		return stream.Event{}, false
	}
	now := time.Now()
	since := f.last
	if f.ticked[i].After(since) {
		since = f.ticked[i]
	}
	if since.IsZero() || now.Sub(since) < f.quiet {
		return stream.Event{}, false
	}
	f.ticked[i] = now
	f.seq[i]++
	return stream.Event{
		Record: core.Record{
			ID:   core.NewID("tick"),
			Data: map[string]any{},
			Meta: map[string]any{tickMeta: true},
		},
		Time: now.UTC(),
		Pos:  stream.Position{Offset: f.seq[i]},
	}, true
}

// heartbeat reports whether a record is one of a feed's idle heartbeats.
func heartbeat(r core.Record) bool {
	v, ok := r.Meta[tickMeta]
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}
