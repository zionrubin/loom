package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
)

func recs(ids ...string) []core.Record {
	out := make([]core.Record, len(ids))
	for i, id := range ids {
		out[i] = core.NewRecord(id, nil)
	}
	return out
}

func TestPipeGroupsAvailableRecords(t *testing.T) {
	p := NewPipe()
	p.Send(recs("a", "b", "c")...)

	got, ok := p.Next(context.Background(), 2, 10*time.Millisecond)
	if !ok || len(got) != 2 {
		t.Fatalf("Next = %v (ok=%v), want a full group of 2", got, ok)
	}
	if got[0].ID != "a" || got[1].ID != "b" {
		t.Errorf("group = %v, want FIFO order a,b", got)
	}
}

// TestPipeShipsPartialBatchOnDeadline is the property that keeps batching
// from reintroducing a barrier: the remainder of a stream must not wait
// forever for peers that will never arrive.
func TestPipeShipsPartialBatchOnDeadline(t *testing.T) {
	p := NewPipe()
	p.Send(recs("only")...)

	start := time.Now()
	got, ok := p.Next(context.Background(), 8, 20*time.Millisecond)
	elapsed := time.Since(start)

	if !ok || len(got) != 1 {
		t.Fatalf("Next = %v (ok=%v), want the single buffered record", got, ok)
	}
	if elapsed > time.Second {
		t.Errorf("waited %v for a batch that could never fill", elapsed)
	}
}

// TestPipeWaitsForFirstRecord checks a reader blocks rather than spinning on
// an empty pipe, and wakes when a record arrives.
func TestPipeWaitsForFirstRecord(t *testing.T) {
	p := NewPipe()
	go func() {
		time.Sleep(10 * time.Millisecond)
		p.Send(recs("late")...)
	}()

	got, ok := p.Next(context.Background(), 4, time.Millisecond)
	if !ok || len(got) != 1 || got[0].ID != "late" {
		t.Fatalf("Next = %v (ok=%v), want the record sent after the wait", got, ok)
	}
}

func TestPipeDrainsThenReportsClosed(t *testing.T) {
	p := NewPipe()
	p.Send(recs("a", "b")...)
	p.Close()

	got, ok := p.Next(context.Background(), 8, 0)
	if !ok || len(got) != 2 {
		t.Fatalf("Next = %v (ok=%v), want both buffered records after close", got, ok)
	}
	if _, ok := p.Next(context.Background(), 8, 0); ok {
		t.Error("a drained, closed pipe should report exhaustion")
	}
}

func TestPipeUnblocksOnCancel(t *testing.T) {
	p := NewPipe()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Next(ctx, 4, time.Millisecond)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Next did not return after its context was cancelled")
	}
}

// TestPipeCloseUnblocksWaitingReader guards the streaming driver's shutdown
// path: every stage closes its children's pipes on exit, and that close is
// what lets a blocked downstream stage terminate.
func TestPipeCloseUnblocksWaitingReader(t *testing.T) {
	p := NewPipe()
	done := make(chan bool, 1)
	go func() {
		_, ok := p.Next(context.Background(), 4, time.Millisecond)
		done <- ok
	}()
	time.Sleep(10 * time.Millisecond)
	p.Close()

	select {
	case ok := <-done:
		if ok {
			t.Error("Next reported records from a closed, empty pipe")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("closing a pipe did not wake its blocked reader")
	}
}
