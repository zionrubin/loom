// Package kafka is a stream source and sink over Apache Kafka.
//
// A split is a topic partition and a position is an offset, which is the
// mapping Kafka was built for: partitions are already independent, already
// ordered, and already resumable by number. Almost everything in this package
// is therefore about the one thing Kafka does not decide for you — *when* an
// offset may be advanced — and the answer is the same one the rest of stream
// mode gives: after the checkpoint that covers it, never before.
//
// # Who owns the offset
//
// Kafka's own answer is the consumer group. Loom's answer is the checkpoint,
// because the checkpoint is the only record that is consistent with the window
// buffers and the sink writes that the offset is supposed to be safe against. A
// group offset committed independently would, after a crash, resume a job at a
// point its windows had never reached.
//
// So Loom assigns partitions itself and resumes them from its own checkpoint.
// Group offsets are still written, after each checkpoint, for the benefit of
// everything outside the job that watches consumer lag — they are a report,
// not a source of truth. Multi-instance group membership, where the assignment
// itself has to be negotiated, is a different problem and belongs with the rest
// of stream mode's scale-out work.
//
// # Bringing your own client
//
// Consumer and Producer are narrow interfaces, and Options.Consumer /
// Options.Producer accept an implementation of them. Left nil, this package
// dials Kafka with franz-go. The seam exists because a Kafka client in a real
// deployment is already configured — TLS, SASL, a schema registry, metrics —
// and a second one inside Loom would be a second set of all of that, and a
// second share of the broker's quota.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/stream"
)

// Partition names one topic partition.
type Partition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// String renders the partition as "topic/3", which is also its split ID.
func (p Partition) String() string { return p.Topic + "/" + strconv.Itoa(int(p.Partition)) }

// ParsePartition parses a split ID back into a partition.
func ParsePartition(id string) (Partition, error) {
	i := strings.LastIndex(id, "/")
	if i < 0 {
		return Partition{}, fmt.Errorf("kafka: bad split id %q", id)
	}
	n, err := strconv.Atoi(id[i+1:])
	if err != nil {
		return Partition{}, fmt.Errorf("kafka: bad split id %q: %w", id, err)
	}
	return Partition{Topic: id[:i], Partition: int32(n)}, nil
}

// Message is one Kafka record, in both directions.
type Message struct {
	Partition Partition
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Headers   map[string][]byte
}

// Start says where a partition with no stored offset begins.
type Start uint8

const (
	// Earliest replays the topic from its retention horizon. It is the default
	// because it is the one that cannot silently lose the records that arrived
	// while the job was being deployed.
	Earliest Start = iota
	// Latest starts at the end: only what arrives from now on.
	Latest
)

// Consumer is the slice of a Kafka consumer that a source needs.
//
// Implementations are used from one goroutine at a time; the source serializes
// access, so they need no internal locking.
type Consumer interface {
	// Partitions lists the partitions of the given topics.
	Partitions(ctx context.Context, topics []string) ([]Partition, error)
	// Assign begins consuming a partition at offset. A negative offset means
	// "wherever Start says".
	Assign(ctx context.Context, p Partition, offset int64) error
	// Unassign stops consuming a partition.
	Unassign(ctx context.Context, p Partition) error
	// Poll returns up to max messages across the assigned partitions, waiting
	// at most wait. Returning nothing is normal.
	Poll(ctx context.Context, max int, wait time.Duration) ([]Message, error)
	// CommitOffsets stores group offsets. It is called after a checkpoint, and
	// only for the benefit of external lag monitoring — the source never reads
	// them back.
	CommitOffsets(ctx context.Context, offsets map[Partition]int64) error
	Close() error
}

// Producer is the slice of a Kafka producer that a sink needs.
type Producer interface {
	// Produce writes messages, returning when the broker has acknowledged them
	// or the context ends.
	Produce(ctx context.Context, msgs []Message) error
	Close() error
}

// Options configures a Kafka source.
type Options struct {
	// Brokers is the bootstrap list, used when Consumer is nil.
	Brokers []string
	// Topics are the topics to consume. Every partition of each becomes a
	// split.
	Topics []string
	// Group names the consumer group whose offsets are updated after each
	// checkpoint. Empty writes no offsets, which is fine for a job nobody is
	// watching lag on.
	Group string
	// Consumer supplies a client. Nil dials Brokers with franz-go.
	Consumer Consumer
	// StartAt is where a partition with no checkpointed offset begins.
	StartAt Start
	// Decode turns a message into a record (default: JSON value into Data,
	// with the message key, or topic/partition/offset, as the record ID).
	Decode func(Message) (core.Record, error)
	// Time extracts event time. Nil uses the message timestamp the broker
	// carries, which is the right default and is wrong exactly when the
	// producer's clock is: use stream.EventTime to read it from the payload
	// instead.
	Time func(Message, core.Record) time.Time
	// MaxPollRecords bounds one Poll (default 500).
	MaxPollRecords int
	// OnDecodeError, when set, is given messages that will not decode instead
	// of dropping them — a dead-letter hook. Returning an error fails the job.
	OnDecodeError func(Message, error) error
}

// Source consumes Kafka topics as a stream.
type Source struct {
	opts Options
	cons Consumer

	mu       sync.Mutex
	queues   map[string][]stream.Event // split ID → buffered events
	assigned map[string]bool
	closed   bool
	dropped  int64
}

// Open returns a Kafka source. It does not connect until Splits is called, so a
// misconfigured broker list fails when the job starts rather than here.
func Open(opts Options) (*Source, error) {
	if len(opts.Topics) == 0 {
		return nil, fmt.Errorf("kafka: at least one topic is required")
	}
	if opts.Consumer == nil && len(opts.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: either Brokers or Consumer is required")
	}
	if opts.Decode == nil {
		opts.Decode = DecodeJSON
	}
	if opts.MaxPollRecords <= 0 {
		opts.MaxPollRecords = 500
	}
	s := &Source{
		opts:     opts,
		cons:     opts.Consumer,
		queues:   map[string][]stream.Event{},
		assigned: map[string]bool{},
	}
	return s, nil
}

// consumer dials on first use when none was supplied.
func (s *Source) consumer() (Consumer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cons != nil {
		return s.cons, nil
	}
	c, err := newFranzConsumer(s.opts)
	if err != nil {
		return nil, err
	}
	s.cons = c
	return c, nil
}

// Splits returns one split per partition of the configured topics, ordered so
// that a restart enumerates them the same way.
func (s *Source) Splits(ctx context.Context) ([]stream.Split, error) {
	cons, err := s.consumer()
	if err != nil {
		return nil, err
	}
	parts, err := cons.Partitions(ctx, s.opts.Topics)
	if err != nil {
		return nil, fmt.Errorf("kafka: listing partitions: %w", err)
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].Topic != parts[j].Topic {
			return parts[i].Topic < parts[j].Topic
		}
		return parts[i].Partition < parts[j].Partition
	})
	out := make([]stream.Split, 0, len(parts))
	for _, p := range parts {
		out = append(out, stream.Split{
			ID:   p.String(),
			Meta: map[string]any{"topic": p.Topic, "partition": p.Partition},
		})
	}
	return out, nil
}

// Open assigns a partition and returns a reader for it.
//
// The offset in from is the position *after* the last record that made it
// through the pipeline, so it is where consumption resumes — not the last
// record consumed, which would be re-delivered.
func (s *Source) Open(ctx context.Context, sp stream.Split, from stream.Position) (stream.Reader, error) {
	p, err := ParsePartition(sp.ID)
	if err != nil {
		return nil, err
	}
	cons, err := s.consumer()
	if err != nil {
		return nil, err
	}
	offset := int64(-1)
	if !from.Zero() {
		offset = from.Offset
	}
	if err := cons.Assign(ctx, p, offset); err != nil {
		return nil, fmt.Errorf("kafka: assigning %s: %w", p, err)
	}
	s.mu.Lock()
	s.assigned[sp.ID] = true
	s.mu.Unlock()
	return &Reader{src: s, part: p, id: sp.ID}, nil
}

// Close releases the consumer.
func (s *Source) Close() error {
	s.mu.Lock()
	if s.closed || s.cons == nil {
		s.closed = true
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cons := s.cons
	s.mu.Unlock()
	return cons.Close()
}

// Undecodable reports how many messages were dropped because they would not
// decode.
func (s *Source) Undecodable() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

// pump polls the shared consumer once and files what comes back under the
// partition it belongs to.
//
// Every reader calls it, and the mutex means exactly one poll is in flight at a
// time — so a topic's partitions share one connection and one fetch, and a
// reader that finds its queue empty is either about to poll or about to be
// filled by the reader that is. This is the whole reason a source demultiplexes
// instead of giving each partition its own client: one client is one set of the
// broker's connection quota.
func (s *Source) pump(ctx context.Context, max int, wait time.Duration) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return stream.ErrSplitDone
	}
	cons := s.cons
	s.mu.Unlock()
	if cons == nil {
		return fmt.Errorf("kafka: source not opened")
	}

	msgs, err := cons.Poll(ctx, max, wait)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range msgs {
		rec, decErr := s.opts.Decode(m)
		if decErr != nil {
			s.dropped++
			if s.opts.OnDecodeError != nil {
				if hookErr := s.opts.OnDecodeError(m, decErr); hookErr != nil {
					return hookErr
				}
			}
			continue
		}
		ev := stream.Event{
			Record: rec,
			Time:   m.Timestamp,
			// The offset after this message: committing it means this message
			// will not be delivered again.
			Pos: stream.Position{Offset: m.Offset + 1},
		}
		if s.opts.Time != nil {
			ev.Time = s.opts.Time(m, rec)
		}
		id := m.Partition.String()
		s.queues[id] = append(s.queues[id], ev)
	}
	return nil
}

// Reader reads one partition.
type Reader struct {
	src  *Source
	part Partition
	id   string
	done bool
}

// Read drains this partition's queue, polling the shared consumer when it is
// empty.
func (r *Reader) Read(ctx context.Context, max int, wait time.Duration) ([]stream.Event, error) {
	if r.done {
		return nil, stream.ErrSplitDone
	}
	if out := r.take(max); len(out) > 0 {
		return out, nil
	}
	deadline := time.Now().Add(wait)
	for {
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		if err := r.src.pump(ctx, r.src.opts.MaxPollRecords, remaining); err != nil {
			if err == stream.ErrSplitDone {
				r.done = true
			}
			return nil, err
		}
		if out := r.take(max); len(out) > 0 {
			return out, nil
		}
		if !time.Now().Before(deadline) {
			return nil, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

func (r *Reader) take(max int) []stream.Event {
	r.src.mu.Lock()
	defer r.src.mu.Unlock()
	q := r.src.queues[r.id]
	if len(q) == 0 {
		return nil
	}
	if max > len(q) {
		max = len(q)
	}
	out := make([]stream.Event, max)
	copy(out, q[:max])
	r.src.queues[r.id] = q[max:]
	return out
}

// Commit writes the group offset for this partition, when a group was
// configured. Loom does not read it back — the checkpoint is what a restart
// resumes from — so a broker that refuses the commit is reported and not fatal
// to the job's correctness.
func (r *Reader) Commit(ctx context.Context, pos stream.Position) error {
	if r.src.opts.Group == "" {
		return nil
	}
	r.src.mu.Lock()
	cons := r.src.cons
	r.src.mu.Unlock()
	if cons == nil {
		return nil
	}
	return cons.CommitOffsets(ctx, map[Partition]int64{r.part: pos.Offset})
}

// Close unassigns the partition. The consumer itself belongs to the source and
// outlives its readers.
func (r *Reader) Close() error {
	r.done = true
	r.src.mu.Lock()
	cons := r.src.cons
	delete(r.src.queues, r.id)
	delete(r.src.assigned, r.id)
	closed := r.src.closed
	r.src.mu.Unlock()
	if cons == nil || closed {
		return nil
	}
	return cons.Unassign(context.Background(), r.part)
}

// DecodeJSON is the default decoder: a JSON object value becomes the record's
// Data. The record ID is the message key when there is one, and
// "topic/partition/offset" otherwise — deterministic either way, which is what
// lets a replayed message hit the result cache instead of being paid for twice.
func DecodeJSON(m Message) (core.Record, error) {
	var data map[string]any
	if err := json.Unmarshal(m.Value, &data); err != nil {
		return core.Record{}, fmt.Errorf("value is not a JSON object: %w", err)
	}
	id := string(m.Key)
	if id == "" {
		id = m.Partition.String() + "/" + strconv.FormatInt(m.Offset, 10)
	}
	return core.NewRecord(id, data), nil
}

// DecodeValue puts the raw message value into one field, for topics that are
// not JSON.
func DecodeValue(field string) func(Message) (core.Record, error) {
	return func(m Message) (core.Record, error) {
		id := string(m.Key)
		if id == "" {
			id = m.Partition.String() + "/" + strconv.FormatInt(m.Offset, 10)
		}
		return core.NewRecord(id, map[string]any{field: string(m.Value)}), nil
	}
}

// SinkOptions configures a Kafka sink.
type SinkOptions struct {
	// Brokers is the bootstrap list, used when Producer is nil.
	Brokers []string
	// Topic is where panes are written.
	Topic string
	// Producer supplies a client. Nil dials Brokers with franz-go.
	Producer Producer
	// Key selects a message key per record (default: the record ID), which is
	// what Kafka's own compaction and partitioning key on.
	Key func(core.Record) []byte
	// Encode turns a record into a message value (default: its Data as JSON).
	Encode func(core.Record) ([]byte, error)
}

// Sink produces a job's output to a Kafka topic.
//
// Delivery is at-least-once: a pane written but not yet covered by a checkpoint
// is written again after a restart. Every message carries the pane's identity in
// its headers — loom-pane, loom-stage, loom-window, loom-epoch — so a consumer
// that must see each pane once can deduplicate on loom-pane without knowing
// anything else about the job. Kafka transactions, which would make the
// duplicate invisible rather than merely labelled, are the next step here.
type Sink struct {
	opts SinkOptions

	mu   sync.Mutex
	prod Producer
}

// NewSink returns a Kafka sink.
func NewSink(opts SinkOptions) (*Sink, error) {
	if opts.Topic == "" {
		return nil, fmt.Errorf("kafka: sink Topic is required")
	}
	if opts.Producer == nil && len(opts.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: sink needs either Brokers or Producer")
	}
	if opts.Encode == nil {
		opts.Encode = func(r core.Record) ([]byte, error) { return json.Marshal(r.Data) }
	}
	if opts.Key == nil {
		opts.Key = func(r core.Record) []byte { return []byte(r.ID) }
	}
	return &Sink{opts: opts, prod: opts.Producer}, nil
}

func (s *Sink) producer() (Producer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prod != nil {
		return s.prod, nil
	}
	p, err := newFranzProducer(s.opts)
	if err != nil {
		return nil, err
	}
	s.prod = p
	return p, nil
}

// Write produces one message per record in the pane.
func (s *Sink) Write(ctx context.Context, b stream.Batch) error {
	prod, err := s.producer()
	if err != nil {
		return err
	}
	msgs := make([]Message, 0, len(b.Records))
	for _, rec := range b.Records {
		value, err := s.opts.Encode(rec)
		if err != nil {
			return fmt.Errorf("kafka: sink: encoding %q: %w", rec.ID, err)
		}
		msgs = append(msgs, Message{
			Partition: Partition{Topic: s.opts.Topic, Partition: -1},
			Key:       s.opts.Key(rec),
			Value:     value,
			Headers: map[string][]byte{
				"loom-pane":   []byte(b.Pane.ID()),
				"loom-stage":  []byte(b.Stage),
				"loom-window": []byte(b.Pane.Window.String()),
				"loom-epoch":  []byte(strconv.FormatInt(b.Epoch, 10)),
			},
		})
	}
	if len(msgs) == 0 {
		return nil
	}
	return prod.Produce(ctx, msgs)
}

// Commit is a no-op: Produce does not return until the broker has acknowledged,
// so there is nothing left staged when a checkpoint arrives.
func (s *Sink) Commit(context.Context, int64) error { return nil }

// Close releases the producer.
func (s *Sink) Close() error {
	s.mu.Lock()
	prod := s.prod
	s.prod = nil
	s.mu.Unlock()
	if prod == nil {
		return nil
	}
	return prod.Close()
}
