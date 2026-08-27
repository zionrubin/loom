package recall

import (
	"context"
	"errors"
	"testing"
	"time"

	"strings"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/stream"
)

func TestFeedPartitionsByConversation(t *testing.T) {
	f := NewFeed(4, 0)
	if _, err := f.Post(
		Message{Conversation: "a", Text: "one"},
		Message{Conversation: "a", Text: "two"},
		Message{Conversation: "b", Text: "three"},
	); err != nil {
		t.Fatalf("post: %v", err)
	}

	ctx := context.Background()
	splits, err := f.Splits(ctx)
	if err != nil {
		t.Fatalf("splits: %v", err)
	}
	if len(splits) != 4 {
		t.Fatalf("feed has %d splits, want 4", len(splits))
	}

	byConv := map[string][]string{}
	for _, sp := range splits {
		r, err := f.Open(ctx, sp, stream.Position{})
		if err != nil {
			t.Fatalf("open %s: %v", sp.ID, err)
		}
		events, err := r.Read(ctx, 100, 0)
		if err != nil {
			t.Fatalf("read %s: %v", sp.ID, err)
		}
		for _, ev := range events {
			byConv[ev.Record.String("conversation")] = append(
				byConv[ev.Record.String("conversation")], sp.ID)
		}
		_ = r.Close()
	}

	// One conversation is read by one split, which is what makes its messages
	// arrive in order and its neighbours not wait behind it.
	if got := byConv["a"]; len(got) != 2 || got[0] != got[1] {
		t.Errorf("conversation a was split across readers: %v", got)
	}
	if len(byConv["b"]) != 1 {
		t.Errorf("conversation b produced %d events, want 1", len(byConv["b"]))
	}
}

func TestFeedFillsInWhatAMessageOmits(t *testing.T) {
	f := NewFeed(1, 0)
	before := time.Now().UTC()
	if _, err := f.Post(Message{Conversation: "a", Text: "no id, no timestamp"}); err != nil {
		t.Fatalf("post: %v", err)
	}
	ev := readOne(t, f)
	if ev.Record.ID == "" {
		t.Error("a message without an ID was not given one")
	}
	if ev.Time.Before(before) {
		t.Errorf("event time %s predates the post", ev.Time)
	}
	if ev.Pos.Offset != 1 {
		t.Errorf("first event is at offset %d, want 1", ev.Pos.Offset)
	}
}

// TestFeedHeartbeatUnsticksEventTime covers the reason a feed has a notion of
// being quiet at all: without it the last window of a conversation stays open
// until an unrelated one produces a later event.
func TestFeedHeartbeatUnsticksEventTime(t *testing.T) {
	f := NewFeed(1, 40*time.Millisecond)
	if _, err := f.Post(Message{Conversation: "a", Text: "the last thing said"}); err != nil {
		t.Fatalf("post: %v", err)
	}
	ctx := context.Background()
	r := openSplit(t, f, 0)

	first, err := r.Read(ctx, 10, 0)
	if err != nil || len(first) != 1 {
		t.Fatalf("read: %d events, %v", len(first), err)
	}
	if heartbeat(first[0].Record) {
		t.Fatal("a posted message was delivered as a heartbeat")
	}

	// Nothing yet: the feed has only just gone quiet.
	if got, err := r.Read(ctx, 10, 0); err != nil || len(got) != 0 {
		t.Fatalf("read while still fresh: %d events, %v", len(got), err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var tick stream.Event
	for time.Now().Before(deadline) {
		got, err := r.Read(ctx, 10, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(got) == 1 {
			tick = got[0]
			break
		}
	}
	if tick.Time.IsZero() {
		t.Fatal("a quiet feed never emitted a heartbeat")
	}
	if !heartbeat(tick.Record) {
		t.Error("the heartbeat is not marked as one, so the pipeline would infer over it")
	}
	if !tick.Time.After(first[0].Time) {
		t.Errorf("the heartbeat did not advance event time: %s is not after %s",
			tick.Time, first[0].Time)
	}
}

func TestFeedClosesAndRetires(t *testing.T) {
	f := NewFeed(1, 0)
	if _, err := f.Post(Message{Conversation: "a", Text: "last words"}); err != nil {
		t.Fatalf("post: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ctx := context.Background()
	r := openSplit(t, f, 0)
	// What was posted before the close is still delivered: closing ends the
	// feed, it does not discard it.
	if got, err := r.Read(ctx, 10, 0); err != nil || len(got) != 1 {
		t.Fatalf("read after close: %d events, %v", len(got), err)
	}
	if _, err := r.Read(ctx, 10, 0); !errors.Is(err, stream.ErrSplitDone) {
		t.Errorf("a drained closed feed reported %v, want ErrSplitDone", err)
	}
	if _, err := f.Post(Message{Text: "too late"}); !errors.Is(err, ErrFeedClosed) {
		t.Errorf("posting to a closed feed reported %v, want ErrFeedClosed", err)
	}
}

func TestFeedReadHonoursContextCancellation(t *testing.T) {
	f := NewFeed(1, 0)
	r := openSplit(t, f, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Read(ctx, 10, time.Minute)
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("read reported %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled read did not return")
	}
}

func TestConversationNormalizesItsMessages(t *testing.T) {
	c := Conversation{
		ID:   "acme",
		Meta: map[string]any{"tier": "enterprise", "owner": "dana"},
		Messages: []Message{
			{Text: "inherits both"},
			{Conversation: "other", Text: "keeps its own thread", Meta: map[string]any{"owner": "sam"}},
		},
	}
	got := c.normalize()
	if got[0].Conversation != "acme" {
		t.Errorf("message did not inherit the conversation: %q", got[0].Conversation)
	}
	if got[0].Meta["tier"] != "enterprise" {
		t.Errorf("message did not inherit conversation metadata: %v", got[0].Meta)
	}
	if got[1].Conversation != "other" {
		t.Errorf("an explicit conversation was overwritten: %q", got[1].Conversation)
	}
	// A message's own metadata wins over the conversation's, so a per-message
	// override is possible at all.
	if got[1].Meta["owner"] != "sam" || got[1].Meta["tier"] != "enterprise" {
		t.Errorf("metadata merged wrongly: %v", got[1].Meta)
	}
}

func TestMessageRecordCarriesWhatThePromptsRead(t *testing.T) {
	m := Message{
		Conversation: "acme", ID: "m1", Role: "user", Speaker: "dana",
		Text: "we are blocked", At: time.Unix(1770000000, 0).UTC(),
		Meta: map[string]any{"tier": "enterprise", "text": "must not win"},
	}
	r := m.Record()
	for field, want := range map[string]string{
		"conversation": "acme",
		"role":         "user",
		"speaker":      "dana",
		"text":         "we are blocked",
		"tier":         "enterprise",
	} {
		if got := r.String(field); got != want {
			t.Errorf("record field %q = %q, want %q", field, got, want)
		}
	}
	if r.String("at") == "" {
		t.Error("record carries no event time for the window to cut on")
	}
}

func TestKeepStampsObservationsSoAPaneCanBeReadInOrder(t *testing.T) {
	// Records reach a pane in the order inference finished, which for
	// concurrent per-record calls is not the order things were said. The line
	// the digest reads carries its own time so the aggregation can put the
	// exchange back together.
	d := &Desk{}
	out, err := d.worthKeeping(core.NewRecord("m", map[string]any{
		"at":      "2026-01-01T10:00:02Z",
		"speaker": "dana",
		"kind":    "decision",
		"note":    "Dana agreed to ship SAML in Q3",
	}))
	if err != nil {
		t.Fatalf("keep: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("kept %d records, want 1", len(out))
	}
	item := out[0].String(itemField)
	for _, want := range []string{"10:00:02", "dana", "(decision)", "ship SAML"} {
		if !strings.Contains(item, want) {
			t.Errorf("observation %q does not carry %q", item, want)
		}
	}
}

func TestKeepDropsWhatIsNotWorthRemembering(t *testing.T) {
	d := &Desk{}
	for _, tc := range []struct {
		name string
		rec  map[string]any
	}{
		{"noise", map[string]any{"kind": "noise", "note": "said hello"}},
		{"no note", map[string]any{"kind": "fact", "note": "   "}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := d.dropped.Load()
			out, err := d.worthKeeping(core.NewRecord("m", tc.rec))
			if err != nil {
				t.Fatalf("keep: %v", err)
			}
			if len(out) != 0 {
				t.Errorf("kept %d records, want none", len(out))
			}
			// Counted, because nothing downstream ever will: a dropped message
			// reaches no pane, and a caller waiting on it would wait forever.
			if d.dropped.Load() != before+1 {
				t.Error("a dropped message was not counted")
			}
		})
	}
}

// --- helpers -------------------------------------------------------------

func openSplit(t *testing.T, f *Feed, i int) stream.Reader {
	t.Helper()
	splits, err := f.Splits(context.Background())
	if err != nil {
		t.Fatalf("splits: %v", err)
	}
	r, err := f.Open(context.Background(), splits[i], stream.Position{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func readOne(t *testing.T, f *Feed) stream.Event {
	t.Helper()
	events, err := openSplit(t, f, 0).Read(context.Background(), 10, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("read %d events, want 1", len(events))
	}
	return events[0]
}
