package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/stream"
	"github.com/zionrubin/loom/stream/kafka"
)

// fakeConsumer is a broker's worth of behavior in fifty lines: partitions with
// ordered logs, an assignment with a cursor per partition, and offsets stored
// under a group. It exercises the part of this package Loom actually owns —
// which offsets are read, when they are committed, and how partitions are
// demultiplexed to readers — without a broker.
type fakeConsumer struct {
	mu        sync.Mutex
	logs      map[kafka.Partition][]kafka.Message
	cursors   map[kafka.Partition]int64
	assigned  map[kafka.Partition]bool
	committed map[kafka.Partition]int64
	polls     int
	closed    bool
}

func newFake() *fakeConsumer {
	return &fakeConsumer{
		logs:      map[kafka.Partition][]kafka.Message{},
		cursors:   map[kafka.Partition]int64{},
		assigned:  map[kafka.Partition]bool{},
		committed: map[kafka.Partition]int64{},
	}
}

// produce appends a message to a partition's log, at the next offset.
func (f *fakeConsumer) produce(p kafka.Partition, id string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	value, _ := json.Marshal(map[string]any{"id": id, "text": "hello " + id})
	f.logs[p] = append(f.logs[p], kafka.Message{
		Partition: p, Offset: int64(len(f.logs[p])),
		Key: []byte(id), Value: value, Timestamp: at,
	})
}

func (f *fakeConsumer) Partitions(context.Context, []string) ([]kafka.Partition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]kafka.Partition, 0, len(f.logs))
	for p := range f.logs {
		out = append(out, p)
	}
	return out, nil
}

func (f *fakeConsumer) Assign(_ context.Context, p kafka.Partition, offset int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if offset < 0 {
		offset = 0 // Earliest
	}
	f.assigned[p] = true
	f.cursors[p] = offset
	return nil
}

func (f *fakeConsumer) Unassign(_ context.Context, p kafka.Partition) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.assigned, p)
	return nil
}

func (f *fakeConsumer) Poll(_ context.Context, max int, _ time.Duration) ([]kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls++
	var out []kafka.Message
	for p := range f.assigned {
		log := f.logs[p]
		for f.cursors[p] < int64(len(log)) && len(out) < max {
			out = append(out, log[f.cursors[p]])
			f.cursors[p]++
		}
	}
	return out, nil
}

func (f *fakeConsumer) CommitOffsets(_ context.Context, offsets map[kafka.Partition]int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for p, off := range offsets {
		f.committed[p] = off
	}
	return nil
}

func (f *fakeConsumer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeConsumer) committedAt(p kafka.Partition) (int64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	off, ok := f.committed[p]
	return off, ok
}

var (
	p0 = kafka.Partition{Topic: "incidents", Partition: 0}
	p1 = kafka.Partition{Topic: "incidents", Partition: 1}
)

func at(sec int) time.Time {
	return time.Date(2026, 3, 1, 12, 0, sec, 0, time.UTC)
}

func TestSplitsAreTopicPartitions(t *testing.T) {
	fake := newFake()
	fake.produce(p0, "a", at(1))
	fake.produce(p1, "b", at(2))

	src, err := kafka.Open(kafka.Options{Topics: []string{"incidents"}, Consumer: fake})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	splits, err := src.Splits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) != 2 {
		t.Fatalf("splits = %d, want 2", len(splits))
	}
	if splits[0].ID != "incidents/0" || splits[1].ID != "incidents/1" {
		t.Fatalf("split IDs = %q, %q", splits[0].ID, splits[1].ID)
	}
	// The ID round-trips, which is what lets a checkpoint name a partition.
	got, err := kafka.ParsePartition(splits[1].ID)
	if err != nil || got != p1 {
		t.Fatalf("ParsePartition(%q) = %v, %v", splits[1].ID, got, err)
	}
}

func TestReadersSeeOnlyTheirOwnPartition(t *testing.T) {
	fake := newFake()
	fake.produce(p0, "a0", at(1))
	fake.produce(p0, "a1", at(2))
	fake.produce(p1, "b0", at(3))

	src, _ := kafka.Open(kafka.Options{Topics: []string{"incidents"}, Consumer: fake})
	defer src.Close()
	splits, _ := src.Splits(context.Background())

	r0, err := src.Open(context.Background(), splits[0], stream.Position{})
	if err != nil {
		t.Fatal(err)
	}
	r1, err := src.Open(context.Background(), splits[1], stream.Position{})
	if err != nil {
		t.Fatal(err)
	}

	// One poll fetches for every assigned partition; the source files each
	// message under the reader it belongs to.
	got0, err := r0.Read(context.Background(), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(got0) != 2 || got0[0].Record.ID != "a0" || got0[1].Record.ID != "a1" {
		t.Fatalf("partition 0 read %v", ids(got0))
	}
	got1, err := r1.Read(context.Background(), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(got1) != 1 || got1[0].Record.ID != "b0" {
		t.Fatalf("partition 1 read %v", ids(got1))
	}
	// Both readers were served by shared polls rather than one client each.
	fake.mu.Lock()
	polls := fake.polls
	fake.mu.Unlock()
	if polls == 0 {
		t.Fatal("no poll reached the consumer")
	}
}

func TestPositionIsTheOffsetAfterTheMessage(t *testing.T) {
	fake := newFake()
	fake.produce(p0, "a0", at(1))
	fake.produce(p0, "a1", at(2))

	src, _ := kafka.Open(kafka.Options{Topics: []string{"incidents"}, Consumer: fake})
	defer src.Close()
	splits, _ := src.Splits(context.Background())
	r, _ := src.Open(context.Background(), splits[0], stream.Position{})

	got, err := r.Read(context.Background(), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// Offsets 0 and 1 were read, so resuming means starting at 1 and 2: the
	// position is where reading continues, not the last record consumed.
	if got[0].Pos.Offset != 1 || got[1].Pos.Offset != 2 {
		t.Fatalf("positions = %d, %d", got[0].Pos.Offset, got[1].Pos.Offset)
	}
	if !got[0].Time.Equal(at(1)) {
		t.Fatalf("event time = %s, want the message timestamp %s", got[0].Time, at(1))
	}
}

func TestResumingOpensAtTheCheckpointedOffset(t *testing.T) {
	fake := newFake()
	for i := 0; i < 4; i++ {
		fake.produce(p0, fmt.Sprintf("a%d", i), at(i))
	}
	src, _ := kafka.Open(kafka.Options{Topics: []string{"incidents"}, Consumer: fake})
	defer src.Close()
	splits, _ := src.Splits(context.Background())

	r, err := src.Open(context.Background(), splits[0], stream.Position{Offset: 2})
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Read(context.Background(), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Record.ID != "a2" {
		t.Fatalf("resumed read = %v, want a2 onward", ids(got))
	}
}

func TestCommitWritesGroupOffsetsOnlyWhenAGroupIsConfigured(t *testing.T) {
	fake := newFake()
	fake.produce(p0, "a0", at(1))

	// No group: nothing is written, because Loom's checkpoint is the truth and
	// a group offset is only ever a report for something else to read.
	quiet, _ := kafka.Open(kafka.Options{Topics: []string{"incidents"}, Consumer: fake})
	splits, _ := quiet.Splits(context.Background())
	r, _ := quiet.Open(context.Background(), splits[0], stream.Position{})
	if err := r.Commit(context.Background(), stream.Position{Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if _, ok := fake.committedAt(p0); ok {
		t.Fatal("an offset was committed without a group configured")
	}
	quiet.Close()

	fake2 := newFake()
	fake2.produce(p0, "a0", at(1))
	watched, _ := kafka.Open(kafka.Options{
		Topics: []string{"incidents"}, Consumer: fake2, Group: "desk",
	})
	defer watched.Close()
	splits2, _ := watched.Splits(context.Background())
	r2, _ := watched.Open(context.Background(), splits2[0], stream.Position{})
	if err := r2.Commit(context.Background(), stream.Position{Offset: 7}); err != nil {
		t.Fatal(err)
	}
	off, ok := fake2.committedAt(p0)
	if !ok || off != 7 {
		t.Fatalf("committed = %d (present %v), want 7", off, ok)
	}
}

func TestUndecodableMessagesAreSkippedAndCounted(t *testing.T) {
	fake := newFake()
	fake.mu.Lock()
	fake.logs[p0] = []kafka.Message{
		{Partition: p0, Offset: 0, Key: []byte("bad"), Value: []byte("not json")},
		{Partition: p0, Offset: 1, Key: []byte("good"), Value: []byte(`{"text":"ok"}`)},
	}
	fake.mu.Unlock()

	src, _ := kafka.Open(kafka.Options{Topics: []string{"incidents"}, Consumer: fake})
	defer src.Close()
	splits, _ := src.Splits(context.Background())
	r, _ := src.Open(context.Background(), splits[0], stream.Position{})

	got, err := r.Read(context.Background(), 10, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Record.ID != "good" {
		t.Fatalf("read %v, want only the decodable message", ids(got))
	}
	if src.Undecodable() != 1 {
		t.Fatalf("undecodable = %d, want 1", src.Undecodable())
	}
}

func TestDecodeErrorHookCanFailTheJob(t *testing.T) {
	fake := newFake()
	fake.mu.Lock()
	fake.logs[p0] = []kafka.Message{{Partition: p0, Offset: 0, Value: []byte("nope")}}
	fake.mu.Unlock()

	sentinel := errors.New("dead letter refused")
	src, _ := kafka.Open(kafka.Options{
		Topics: []string{"incidents"}, Consumer: fake,
		OnDecodeError: func(kafka.Message, error) error { return sentinel },
	})
	defer src.Close()
	splits, _ := src.Splits(context.Background())
	r, _ := src.Open(context.Background(), splits[0], stream.Position{})

	if _, err := r.Read(context.Background(), 10, time.Millisecond); !errors.Is(err, sentinel) {
		t.Fatalf("read err = %v, want the hook's error", err)
	}
}

func TestReadOnAQuietTopicReturnsNothingRatherThanBlocking(t *testing.T) {
	fake := newFake()
	fake.mu.Lock()
	fake.logs[p0] = nil
	fake.mu.Unlock()

	src, _ := kafka.Open(kafka.Options{Topics: []string{"incidents"}, Consumer: fake})
	defer src.Close()
	r, err := src.Open(context.Background(), stream.Split{ID: "incidents/0"}, stream.Position{})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	got, err := r.Read(context.Background(), 10, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("read %d events from an empty partition", len(got))
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("an idle read took %s", elapsed)
	}
}

func TestOpenRefusesAnIncompleteConfiguration(t *testing.T) {
	if _, err := kafka.Open(kafka.Options{Consumer: newFake()}); err == nil {
		t.Fatal("a source with no topics should not open")
	}
	if _, err := kafka.Open(kafka.Options{Topics: []string{"t"}}); err == nil {
		t.Fatal("a source with neither brokers nor a client should not open")
	}
}

// fakeProducer records what a sink produced.
type fakeProducer struct {
	mu   sync.Mutex
	sent []kafka.Message
}

func (f *fakeProducer) Produce(_ context.Context, msgs []kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, msgs...)
	return nil
}

func (f *fakeProducer) Close() error { return nil }

func TestSinkStampsEveryMessageWithThePaneItCameFrom(t *testing.T) {
	prod := &fakeProducer{}
	sink, err := kafka.NewSink(kafka.SinkOptions{Topic: "digests", Producer: prod})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	pane := stream.Pane{
		Window: stream.Window{
			Start: at(0), End: at(60), Key: "eu",
		},
		Seq: 1, Final: true, Count: 1,
	}
	batch := stream.Batch{
		Stage: "digest", Pane: pane, Epoch: 9,
		Records: []core.Record{core.NewRecord("d1", map[string]any{"output": "summary"})},
	}
	if err := sink.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	prod.mu.Lock()
	defer prod.mu.Unlock()
	if len(prod.sent) != 1 {
		t.Fatalf("produced %d messages, want 1", len(prod.sent))
	}
	m := prod.sent[0]
	if m.Partition.Topic != "digests" {
		t.Fatalf("topic = %q", m.Partition.Topic)
	}
	if string(m.Key) != "d1" {
		t.Fatalf("key = %q, want the record ID", m.Key)
	}
	// The pane identity is what a downstream consumer deduplicates on after a
	// replay, so it has to be on the message rather than only in Loom.
	if got := string(m.Headers["loom-pane"]); got != pane.ID() {
		t.Fatalf("loom-pane = %q, want %q", got, pane.ID())
	}
	if string(m.Headers["loom-stage"]) != "digest" {
		t.Fatalf("loom-stage = %q", m.Headers["loom-stage"])
	}
	if string(m.Headers["loom-epoch"]) != "9" {
		t.Fatalf("loom-epoch = %q", m.Headers["loom-epoch"])
	}
	var payload map[string]any
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		t.Fatalf("value is not JSON: %v", err)
	}
	if payload["output"] != "summary" {
		t.Fatalf("value = %v", payload)
	}
}

func ids(events []stream.Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Record.ID
	}
	return out
}
