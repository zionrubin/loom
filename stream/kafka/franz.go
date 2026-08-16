package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// This file is the only place in Loom that knows what Kafka's wire protocol
// looks like. Everything above it — offsets aligned to checkpoints, watermarks
// per partition, panes, sinks — is written against the Consumer and Producer
// interfaces in kafka.go, which is what lets a deployment substitute the client
// it has already configured and lets this package be tested without a broker.

// franzConsumer implements Consumer over franz-go, in direct-assignment mode:
// Loom decides which partitions it reads and from which offsets, because Loom's
// checkpoint is what those offsets have to agree with.
type franzConsumer struct {
	cl    *kgo.Client
	start Start
	group string
}

func newFranzConsumer(opts Options) (Consumer, error) {
	kopts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ClientID("loom-stream"),
		// No ConsumeTopics: partitions are added explicitly by Assign, which is
		// how the assignment stays Loom's rather than the broker's.
		kgo.DisableAutoCommit(),
	}
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: dialing %v: %w", opts.Brokers, err)
	}
	return &franzConsumer{cl: cl, start: opts.StartAt, group: opts.Group}, nil
}

// Partitions asks the cluster for each topic's partitions.
func (c *franzConsumer) Partitions(ctx context.Context, topics []string) ([]Partition, error) {
	req := kmsg.NewMetadataRequest()
	for _, t := range topics {
		topic := kmsg.NewMetadataRequestTopic()
		name := t
		topic.Topic = &name
		req.Topics = append(req.Topics, topic)
	}
	resp, err := req.RequestWith(ctx, c.cl)
	if err != nil {
		return nil, err
	}
	var out []Partition
	for _, t := range resp.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			return nil, fmt.Errorf("kafka: metadata for topic: %w", err)
		}
		if t.Topic == nil {
			continue
		}
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				// A partition without a leader right now is still a partition;
				// the fetch will retry. Only topic-level errors are fatal.
				continue
			}
			out = append(out, Partition{Topic: *t.Topic, Partition: p.Partition})
		}
	}
	return out, nil
}

// Assign begins consuming a partition. A negative offset defers to StartAt,
// which is the only moment Loom lets Kafka choose where reading begins.
func (c *franzConsumer) Assign(_ context.Context, p Partition, offset int64) error {
	at := kgo.NewOffset()
	switch {
	case offset >= 0:
		at = at.At(offset)
	case c.start == Latest:
		at = at.AtEnd()
	default:
		at = at.AtStart()
	}
	c.cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		p.Topic: {p.Partition: at},
	})
	return nil
}

func (c *franzConsumer) Unassign(_ context.Context, p Partition) error {
	c.cl.RemoveConsumePartitions(map[string][]int32{p.Topic: {p.Partition}})
	return nil
}

// Poll fetches across every assigned partition at once.
func (c *franzConsumer) Poll(ctx context.Context, max int, wait time.Duration) ([]Message, error) {
	if wait <= 0 {
		wait = time.Millisecond
	}
	pollCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	fetches := c.cl.PollRecords(pollCtx, max)
	if err := fetchErr(fetches, ctx); err != nil {
		return nil, err
	}
	var out []Message
	fetches.EachRecord(func(r *kgo.Record) {
		m := Message{
			Partition: Partition{Topic: r.Topic, Partition: r.Partition},
			Offset:    r.Offset,
			Key:       r.Key,
			Value:     r.Value,
			Timestamp: r.Timestamp,
		}
		if len(r.Headers) > 0 {
			m.Headers = make(map[string][]byte, len(r.Headers))
			for _, h := range r.Headers {
				m.Headers[h.Key] = h.Value
			}
		}
		out = append(out, m)
	})
	return out, nil
}

// fetchErr distinguishes "the poll deadline passed" — the normal outcome of an
// idle topic, and not an error — from an actual fetch failure.
func fetchErr(fetches kgo.Fetches, parent context.Context) error {
	var firstErr error
	fetches.EachError(func(topic string, part int32, err error) {
		if errors.Is(err, context.DeadlineExceeded) && parent.Err() == nil {
			return
		}
		if errors.Is(err, context.Canceled) && parent.Err() != nil {
			if firstErr == nil {
				firstErr = parent.Err()
			}
			return
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("kafka: fetching %s/%d: %w", topic, part, err)
		}
	})
	return firstErr
}

// CommitOffsets writes offsets against the configured group without joining it:
// generation -1 is Kafka's own way of saying "this group is being used to store
// offsets only", which is exactly what Loom is doing with it.
func (c *franzConsumer) CommitOffsets(ctx context.Context, offsets map[Partition]int64) error {
	if c.group == "" || len(offsets) == 0 {
		return nil
	}
	byTopic := map[string][]kmsg.OffsetCommitRequestTopicPartition{}
	for p, off := range offsets {
		part := kmsg.NewOffsetCommitRequestTopicPartition()
		part.Partition = p.Partition
		part.Offset = off
		part.LeaderEpoch = -1
		byTopic[p.Topic] = append(byTopic[p.Topic], part)
	}
	req := kmsg.NewOffsetCommitRequest()
	req.Group = c.group
	req.Generation = -1
	for topic, parts := range byTopic {
		t := kmsg.NewOffsetCommitRequestTopic()
		t.Topic = topic
		t.Partitions = parts
		req.Topics = append(req.Topics, t)
	}
	resp, err := req.RequestWith(ctx, c.cl)
	if err != nil {
		return fmt.Errorf("kafka: committing offsets: %w", err)
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return fmt.Errorf("kafka: committing %s/%d: %w", t.Topic, p.Partition, err)
			}
		}
	}
	return nil
}

func (c *franzConsumer) Close() error {
	c.cl.Close()
	return nil
}

// franzProducer implements Producer over franz-go.
type franzProducer struct{ cl *kgo.Client }

func newFranzProducer(opts SinkOptions) (Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ClientID("loom-stream"),
		kgo.DefaultProduceTopic(opts.Topic),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: dialing %v: %w", opts.Brokers, err)
	}
	return &franzProducer{cl: cl}, nil
}

// Produce writes synchronously: the call returns when the broker has
// acknowledged every message, which is what lets the sink's Commit be a no-op
// and a pane's write be durable by the time the checkpoint covering it is
// taken.
func (p *franzProducer) Produce(ctx context.Context, msgs []Message) error {
	recs := make([]*kgo.Record, 0, len(msgs))
	for _, m := range msgs {
		r := &kgo.Record{Topic: m.Partition.Topic, Key: m.Key, Value: m.Value}
		if m.Partition.Partition >= 0 {
			r.Partition = m.Partition.Partition
		}
		for k, v := range m.Headers {
			r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: v})
		}
		recs = append(recs, r)
	}
	if err := p.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: producing: %w", err)
	}
	return nil
}

func (p *franzProducer) Close() error {
	p.cl.Close()
	return nil
}
