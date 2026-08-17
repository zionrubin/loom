package file_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/stream"
	"github.com/zionrubin/loom/stream/file"
)

func write(t *testing.T, path string, lines ...string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func event(id string, ts string) string {
	blob, _ := json.Marshal(map[string]any{"id": id, "at": ts, "text": "hello " + id})
	return string(blob)
}

// readAll drains a reader until the split ends.
func readAll(t *testing.T, r stream.Reader) []stream.Event {
	t.Helper()
	var out []stream.Event
	for {
		batch, err := r.Read(context.Background(), 10, 0)
		out = append(out, batch...)
		if errors.Is(err, stream.ErrSplitDone) {
			return out
		}
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(batch) == 0 {
			return out
		}
	}
}

func TestReadsEveryFileAsItsOwnSplit(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "a.jsonl"),
		event("a1", "2026-03-01T12:00:05Z"), event("a2", "2026-03-01T12:00:35Z"))
	write(t, filepath.Join(dir, "b.jsonl"), event("b1", "2026-03-01T12:01:05Z"))

	src, err := file.Open(file.SourceOptions{
		Glob: filepath.Join(dir, "*.jsonl"),
		Time: stream.EventTime("at"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	splits, err := src.Splits(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(splits) != 2 {
		t.Fatalf("splits = %d, want one per file", len(splits))
	}

	r, err := src.Open(context.Background(), splits[0], stream.Position{})
	if err != nil {
		t.Fatal(err)
	}
	events := readAll(t, r)
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[0].Record.ID != "a1" {
		t.Fatalf("first record = %q", events[0].Record.ID)
	}
	want := time.Date(2026, 3, 1, 12, 0, 5, 0, time.UTC)
	if !events[0].Time.Equal(want) {
		t.Fatalf("event time = %s, want %s", events[0].Time, want)
	}
	if events[0].Pos.Offset == 0 || events[1].Pos.Offset <= events[0].Pos.Offset {
		t.Fatalf("positions do not advance: %d then %d", events[0].Pos.Offset, events[1].Pos.Offset)
	}
}

func TestResumingFromAPositionSkipsWhatWasRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	write(t, path, event("e1", ""), event("e2", ""), event("e3", ""))

	src, err := file.Open(file.SourceOptions{Glob: path})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	splits, _ := src.Splits(context.Background())

	r, err := src.Open(context.Background(), splits[0], stream.Position{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := r.Read(context.Background(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Record.ID != "e1" {
		t.Fatalf("first read = %v", first)
	}
	after := first[0].Pos
	r.Close()

	// A restart hands the stored position back, and reading continues from
	// exactly the record after the one that was covered.
	r2, err := src.Open(context.Background(), splits[0], after)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	rest := readAll(t, r2)
	if len(rest) != 2 || rest[0].Record.ID != "e2" || rest[1].Record.ID != "e3" {
		t.Fatalf("resumed read = %v", rest)
	}
}

func TestWithoutFollowAFullyReadFileEnds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	write(t, path, event("e1", ""))

	src, _ := file.Open(file.SourceOptions{Glob: path})
	defer src.Close()
	splits, _ := src.Splits(context.Background())
	r, _ := src.Open(context.Background(), splits[0], stream.Position{})
	defer r.Close()

	if _, err := r.Read(context.Background(), 10, 0); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if _, err := r.Read(context.Background(), 10, 0); !errors.Is(err, stream.ErrSplitDone) {
		t.Fatalf("second read err = %v, want ErrSplitDone", err)
	}
}

func TestFollowPicksUpAppendsAndHoldsPartialLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live.jsonl")
	write(t, path, event("e1", ""))

	src, err := file.Open(file.SourceOptions{
		Glob: path, Follow: true, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	splits, _ := src.Splits(context.Background())
	r, _ := src.Open(context.Background(), splits[0], stream.Position{})
	defer r.Close()

	got, err := r.Read(context.Background(), 10, 5*time.Millisecond)
	if err != nil || len(got) != 1 {
		t.Fatalf("first read = %v (err %v)", got, err)
	}
	posAfterFirst := got[0].Pos

	// A writer that has written half a record must not produce half a record.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"id":"e2","te`); err != nil {
		t.Fatal(err)
	}
	partial, err := r.Read(context.Background(), 10, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("read over a partial line: %v", err)
	}
	if len(partial) != 0 {
		t.Fatalf("a partial line produced %d records", len(partial))
	}

	// Finishing the line completes the record.
	if _, err := f.WriteString("xt\":\"rest\"}\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	deadline := time.Now().Add(2 * time.Second)
	var done []stream.Event
	for time.Now().Before(deadline) && len(done) == 0 {
		done, err = r.Read(context.Background(), 10, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if len(done) != 1 || done[0].Record.ID != "e2" {
		t.Fatalf("completed line = %v", done)
	}
	if done[0].Record.String("text") != "rest" {
		t.Fatalf("record reassembled wrong: %v", done[0].Record.Data)
	}
	if done[0].Pos.Offset <= posAfterFirst.Offset {
		t.Fatalf("position did not advance past the reassembled record")
	}
}

func TestFollowDiscoversFilesThatAppearLater(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "a.jsonl"), event("a1", ""))

	src, _ := file.Open(file.SourceOptions{Glob: filepath.Join(dir, "*.jsonl"), Follow: true})
	defer src.Close()

	if splits, _ := src.Splits(context.Background()); len(splits) != 1 {
		t.Fatalf("splits = %d, want 1", len(splits))
	}
	write(t, filepath.Join(dir, "b.jsonl"), event("b1", ""))
	splits, _ := src.Splits(context.Background())
	if len(splits) != 2 {
		t.Fatalf("splits after a new file = %d, want 2", len(splits))
	}
}

func TestBadLinesAreSkippedAndCounted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.jsonl")
	write(t, path, event("e1", ""), "not json at all", "", event("e2", ""))

	src, _ := file.Open(file.SourceOptions{Glob: path})
	defer src.Close()
	splits, _ := src.Splits(context.Background())
	r, _ := src.Open(context.Background(), splits[0], stream.Position{})
	defer r.Close()

	events := readAll(t, r)
	if len(events) != 2 {
		t.Fatalf("events = %d, want the two good lines", len(events))
	}
	if src.Undecodable() != 1 {
		t.Fatalf("undecodable = %d, want 1", src.Undecodable())
	}
}

func TestFailOnBadLinesStops(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.jsonl")
	write(t, path, "not json")

	src, _ := file.Open(file.SourceOptions{Glob: path, OnDecodeError: file.FailOnBadLines})
	defer src.Close()
	splits, _ := src.Splits(context.Background())
	r, _ := src.Open(context.Background(), splits[0], stream.Position{})
	defer r.Close()

	if _, err := r.Read(context.Background(), 10, 0); err == nil {
		t.Fatal("FailOnBadLines should surface the decode error")
	}
}

func TestRecordIDsFallBackToFileAndOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "anon.jsonl")
	write(t, path, `{"text":"one"}`, `{"text":"two"}`)

	src, _ := file.Open(file.SourceOptions{Glob: path})
	defer src.Close()
	splits, _ := src.Splits(context.Background())
	r, _ := src.Open(context.Background(), splits[0], stream.Position{})
	defer r.Close()

	events := readAll(t, r)
	if len(events) != 2 {
		t.Fatalf("events = %d", len(events))
	}
	if events[0].Record.ID != "anon.jsonl:0" {
		t.Fatalf("first ID = %q, want file:offset", events[0].Record.ID)
	}
	if events[1].Record.ID == events[0].Record.ID {
		t.Fatal("two records share a synthesized ID")
	}
}

func TestTruncatedFileRestartsRatherThanSkipping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rotated.jsonl")
	write(t, path, event("new", ""))

	src, _ := file.Open(file.SourceOptions{Glob: path})
	defer src.Close()
	splits, _ := src.Splits(context.Background())

	// A stored offset beyond the file's end means it was replaced under us.
	r, err := src.Open(context.Background(), splits[0], stream.Position{Offset: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	events := readAll(t, r)
	if len(events) != 1 || events[0].Record.ID != "new" {
		t.Fatalf("events = %v, want the rotated file read from the start", events)
	}
}

func TestSinkWritesOnePaneToOneFileIdempotently(t *testing.T) {
	dir := t.TempDir()
	sink, err := file.NewSink(file.SinkOptions{Dir: dir, Meta: true})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	pane := stream.Pane{
		Window: stream.Window{Start: base(), End: base().Add(time.Minute)},
		Seq:    1, Final: true, Count: 1, Watermark: base().Add(time.Minute),
	}
	batch := stream.Batch{
		Stage: "digest", Pane: pane, Epoch: 2,
		Records: []core.Record{core.NewRecord("d1", map[string]any{"output": "summary"})},
	}
	if err := sink.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	// Replaying the same pane after a restart must not double the output.
	if err := sink.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("wrote %d files for one pane written twice", len(entries))
	}
	blob, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(blob), "\n") != 1 {
		t.Fatalf("file holds %q, want one line", blob)
	}
	var line map[string]any
	if err := json.Unmarshal(blob, &line); err != nil {
		t.Fatalf("output is not JSON: %v", err)
	}
	data, _ := line["data"].(map[string]any)
	if _, ok := data["_pane"]; !ok {
		t.Fatalf("Meta was requested but the pane is missing: %v", data)
	}
}

func TestAppendSinkAccumulates(t *testing.T) {
	dir := t.TempDir()
	sink, err := file.NewSink(file.SinkOptions{Dir: dir, Layout: file.AppendJSONL})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	for i := 0; i < 3; i++ {
		batch := stream.Batch{
			Stage: "digest",
			Pane:  stream.Pane{Seq: i + 1},
			Records: []core.Record{
				core.NewRecord("r", map[string]any{"n": i}),
			},
		}
		if err := sink.Write(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Commit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	blob, err := os.ReadFile(filepath.Join(dir, "output.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(blob), "\n"); n != 3 {
		t.Fatalf("appended %d lines, want 3", n)
	}
}

func base() time.Time { return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC) }
