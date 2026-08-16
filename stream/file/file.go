// Package file is a stream source and sink over ordinary files.
//
// It exists for three reasons, in ascending order of seriousness. It is the
// source you develop against, because a directory of JSONL is the cheapest
// stream there is. It is the source you *test* against, because a fixed set of
// files replayed twice produces identical windows, identical panes, and — with
// a warm cache — identical results for no money. And it is the source you
// backfill with, because a stream job pointed at yesterday's files is the same
// job, the same windows, and the same code as the one pointed at the live feed.
//
// A split is a file; a position is a byte offset. Both are chosen so that a
// resumed job can be checked by hand: the checkpoint says offset 4096 of
// events-03.jsonl, and that is exactly what it means.
package file

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/stream"
)

// Decoder turns one line of a file into a record. The offset is the byte
// position of the line's start, which is what the default decoder uses to give
// a record a stable identity.
type Decoder func(line []byte, split string, offset int64) (core.Record, error)

// OnError says what a source does with a line it cannot decode.
type OnError uint8

const (
	// SkipBadLines drops the line and counts it. A stream that stops on the
	// first malformed record is a stream that stops.
	SkipBadLines OnError = iota
	// FailOnBadLines ends the job.
	FailOnBadLines
)

// SourceOptions configures a file source.
type SourceOptions struct {
	// Glob selects the files to read, e.g. "./events/*.jsonl". Each matching
	// file is a split, read independently and resumed independently.
	Glob string
	// Decode turns a line into a record (default: JSON object into Data, with
	// an "id" field used as the record ID when present).
	Decode Decoder
	// Time extracts event time from the decoded record. Without it every
	// record is stamped with the time it was read, which makes windows a
	// function of when the job ran rather than of when things happened.
	Time func(core.Record) time.Time
	// Follow keeps the source open at the end of every file, waiting for
	// appends and rescanning the glob for new files. Without it a file that has
	// been read to its end is done, and a job whose splits are all done drains
	// and stops — which is what makes this a backfill source as well as a live
	// one.
	Follow bool
	// PollInterval is how long a reader waits on an exhausted file before
	// looking again (default 100ms, Follow only).
	PollInterval time.Duration
	// OnDecodeError says what to do with a line that will not decode.
	OnDecodeError OnError
}

// Source reads a set of files as a stream.
type Source struct {
	opts SourceOptions

	mu      sync.Mutex
	readers []*Reader
	bad     atomic.Int64
}

// Open returns a source over the files matching opts.Glob. The glob is
// evaluated again on every call to Splits, so files that appear later are
// picked up by a following source.
func Open(opts SourceOptions) (*Source, error) {
	if opts.Glob == "" {
		return nil, fmt.Errorf("file: Glob is required")
	}
	if _, err := filepath.Match(opts.Glob, ""); err != nil {
		return nil, fmt.Errorf("file: bad glob %q: %w", opts.Glob, err)
	}
	if opts.Decode == nil {
		opts.Decode = DecodeJSON
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 100 * time.Millisecond
	}
	return &Source{opts: opts}, nil
}

// Splits returns one split per matching file, ordered by name so a replay reads
// them in the same order every time.
func (s *Source) Splits(context.Context) ([]stream.Split, error) {
	paths, err := filepath.Glob(s.opts.Glob)
	if err != nil {
		return nil, fmt.Errorf("file: glob %q: %w", s.opts.Glob, err)
	}
	sort.Strings(paths)
	out := make([]stream.Split, 0, len(paths))
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil || info.IsDir() {
			continue
		}
		out = append(out, stream.Split{ID: p, Meta: map[string]any{"bytes": info.Size()}})
	}
	return out, nil
}

// Open begins reading a file from a byte offset.
func (s *Source) Open(_ context.Context, sp stream.Split, from stream.Position) (stream.Reader, error) {
	f, err := os.Open(sp.ID)
	if err != nil {
		return nil, fmt.Errorf("file: opening split %q: %w", sp.ID, err)
	}
	if from.Offset > 0 {
		// A file shorter than the stored offset has been truncated or replaced
		// under us. Restarting it from the beginning is the only reading that
		// cannot silently skip data; the duplicate records it produces are the
		// cache's problem rather than correctness's.
		if info, statErr := f.Stat(); statErr == nil && info.Size() < from.Offset {
			from.Offset = 0
		}
		if _, err := f.Seek(from.Offset, io.SeekStart); err != nil {
			f.Close()
			return nil, fmt.Errorf("file: seeking %q to %d: %w", sp.ID, from.Offset, err)
		}
	}
	r := &Reader{
		src: s, split: sp.ID, file: f, buf: bufio.NewReaderSize(f, 64<<10),
		offset: from.Offset,
	}
	s.mu.Lock()
	s.readers = append(s.readers, r)
	s.mu.Unlock()
	return r, nil
}

// Close closes every reader this source opened.
func (s *Source) Close() error {
	s.mu.Lock()
	readers := s.readers
	s.readers = nil
	s.mu.Unlock()
	var err error
	for _, r := range readers {
		if cerr := r.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// Undecodable reports how many lines this source skipped because they would not
// decode. A stream job prints it, because a number that grows is a schema
// change nobody told you about.
func (s *Source) Undecodable() int64 { return s.bad.Load() }

// Reader reads one file.
type Reader struct {
	src    *Source
	split  string
	file   *os.File
	buf    *bufio.Reader
	offset int64
	// partial holds an incomplete trailing line: a writer appending to this
	// file has written some of a record but not its newline. Consuming it would
	// decode half a record, so it is held until the rest arrives.
	partial []byte
	done    bool
}

// Read returns up to max records, waiting at most wait for the file to grow.
//
// r.offset is the offset of the start of the next unfinished record, so it only
// moves when a whole line has been consumed. That is what makes it safe to
// checkpoint: resuming from it re-reads a partial line rather than skipping it.
func (r *Reader) Read(ctx context.Context, max int, wait time.Duration) ([]stream.Event, error) {
	if r.done {
		return nil, stream.ErrSplitDone
	}
	deadline := time.Now().Add(wait)
	var out []stream.Event
	for len(out) < max {
		line, err := r.buf.ReadBytes('\n')
		switch {
		case err == nil:
			start := r.offset
			full := line
			if len(r.partial) > 0 {
				full = append(r.partial, line...)
				r.partial = nil
			}
			r.offset = start + int64(len(full))
			ev, ok, decErr := r.decode(full, start)
			if decErr != nil {
				return out, decErr
			}
			if ok {
				out = append(out, ev)
			}

		case errors.Is(err, io.EOF):
			// Hold the fragment: ReadBytes consumed it from the file, and it is
			// not a record until its newline arrives. The offset stays where the
			// fragment starts, so a checkpoint taken now resumes before it.
			if len(line) > 0 {
				r.partial = append(r.partial, line...)
			}
			if !r.src.opts.Follow {
				r.done = true
				if len(out) > 0 {
					return out, nil
				}
				return nil, stream.ErrSplitDone
			}
			if len(out) > 0 || !time.Now().Before(deadline) {
				return out, nil
			}
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			case <-time.After(r.src.opts.PollInterval):
			}

		default:
			return out, fmt.Errorf("file: reading %q: %w", r.split, err)
		}
	}
	return out, nil
}

// decode turns a raw line into an event, reporting false for a line that was
// skipped (blank, or undecodable under SkipBadLines).
func (r *Reader) decode(raw []byte, start int64) (stream.Event, bool, error) {
	line := strings.TrimRight(string(raw), "\r\n")
	if strings.TrimSpace(line) == "" {
		return stream.Event{}, false, nil
	}
	rec, err := r.src.opts.Decode([]byte(line), r.split, start)
	if err != nil {
		r.src.bad.Add(1)
		if r.src.opts.OnDecodeError == FailOnBadLines {
			return stream.Event{}, false, core.Permanent(
				fmt.Errorf("file: %s:%d: %w", r.split, start, err))
		}
		return stream.Event{}, false, nil
	}
	ev := stream.Event{Record: rec, Pos: stream.Position{Offset: r.offset}}
	if r.src.opts.Time != nil {
		ev.Time = r.src.opts.Time(rec)
	}
	return ev, true, nil
}

// Commit is a no-op: a file has nowhere to record a consumer position, so the
// position lives in Loom's checkpoint and nowhere else.
func (r *Reader) Commit(context.Context, stream.Position) error { return nil }

// Close closes the underlying file.
func (r *Reader) Close() error { return r.file.Close() }

// DecodeJSON is the default decoder: a line of JSON object becomes a record's
// Data, and an "id" field, if present, becomes the record ID. Without one the
// ID is the file and offset the record came from, which is stable across
// replays and unique within the source — exactly what the result cache wants.
func DecodeJSON(line []byte, split string, offset int64) (core.Record, error) {
	var data map[string]any
	if err := json.Unmarshal(line, &data); err != nil {
		return core.Record{}, fmt.Errorf("not a JSON object: %w", err)
	}
	id := ""
	if v, ok := data["id"]; ok {
		if s, ok := v.(string); ok && s != "" {
			id = s
		}
	}
	if id == "" {
		id = filepath.Base(split) + ":" + strconv.FormatInt(offset, 10)
	}
	return core.NewRecord(id, data), nil
}

// DecodeText puts each line into a single field, for sources that are not JSON
// at all: a log file, a feed of prompts, a CSV column.
func DecodeText(field string) Decoder {
	return func(line []byte, split string, offset int64) (core.Record, error) {
		return core.NewRecord(
			filepath.Base(split)+":"+strconv.FormatInt(offset, 10),
			map[string]any{field: string(line)}), nil
	}
}

// Layout selects how a sink arranges its output.
type Layout uint8

const (
	// PanePerFile writes each pane to its own file, named for the pane. It is
	// the idempotent layout: a pane replayed after a restart rewrites the same
	// path with the same bytes, so at-least-once delivery leaves exactly one
	// copy on disk.
	PanePerFile Layout = iota
	// AppendJSONL appends every pane to one file. Simpler to consume, and
	// at-least-once in the way that actually shows: a replayed pane appears
	// twice, distinguishable only by the pane key each line carries.
	AppendJSONL
)

// SinkOptions configures a file sink.
type SinkOptions struct {
	// Dir is where output goes. It is created if missing.
	Dir string
	// Layout selects one file per pane (default) or one appended file.
	Layout Layout
	// File names the appended file under AppendJSONL (default "output.jsonl").
	File string
	// Meta adds the pane's window, sequence and watermark to every emitted
	// line under a "_pane" key, so the output says which window produced it.
	Meta bool
}

// Sink writes panes as JSONL.
type Sink struct {
	opts SinkOptions

	mu     sync.Mutex
	append *os.File
	// staged holds the paths written since the last commit, purely so Commit
	// can fsync the directory once rather than per pane.
	staged []string
}

// NewSink returns a file sink, creating the output directory.
func NewSink(opts SinkOptions) (*Sink, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("file: sink Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("file: sink dir: %w", err)
	}
	if opts.File == "" {
		opts.File = "output.jsonl"
	}
	return &Sink{opts: opts}, nil
}

// Write emits one pane.
func (s *Sink) Write(_ context.Context, b stream.Batch) error {
	blob, err := s.encode(b)
	if err != nil {
		return err
	}
	if s.opts.Layout == AppendJSONL {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.append == nil {
			f, err := os.OpenFile(filepath.Join(s.opts.Dir, s.opts.File),
				os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				return fmt.Errorf("file: sink: %w", err)
			}
			s.append = f
		}
		if _, err := s.append.Write(blob); err != nil {
			return fmt.Errorf("file: sink: %w", err)
		}
		return nil
	}

	path := filepath.Join(s.opts.Dir, sanitize(b.Key())+".jsonl")
	if err := writeAtomic(path, blob); err != nil {
		return err
	}
	s.mu.Lock()
	s.staged = append(s.staged, path)
	s.mu.Unlock()
	return nil
}

func (s *Sink) encode(b stream.Batch) ([]byte, error) {
	var out []byte
	for _, rec := range b.Records {
		payload := rec.Data
		if s.opts.Meta {
			payload = make(map[string]any, len(rec.Data)+1)
			for k, v := range rec.Data {
				payload[k] = v
			}
			payload["_pane"] = map[string]any{
				"stage": b.Stage, "window": b.Pane.Window.String(),
				"seq": b.Pane.Seq, "final": b.Pane.Final,
				"watermark": b.Pane.Watermark.UTC().Format(time.RFC3339),
			}
		}
		line, err := json.Marshal(map[string]any{"id": rec.ID, "data": payload})
		if err != nil {
			return nil, fmt.Errorf("file: sink: encoding %q: %w", rec.ID, err)
		}
		out = append(out, line...)
		out = append(out, '\n')
	}
	return out, nil
}

// Commit flushes what has been written. The pane files are already durable —
// each was renamed into place — so this only syncs the appended file.
func (s *Sink) Commit(context.Context, int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.staged = nil
	if s.append == nil {
		return nil
	}
	if err := s.append.Sync(); err != nil {
		return fmt.Errorf("file: sink: %w", err)
	}
	return nil
}

// Close closes the appended file, if one was opened.
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.append == nil {
		return nil
	}
	err := s.append.Close()
	s.append = nil
	return err
}

func writeAtomic(path string, blob []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("file: sink: %w", err)
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(blob); err != nil {
		tmp.Close()
		return fmt.Errorf("file: sink: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("file: sink: %w", err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("file: sink: %w", err)
	}
	return nil
}

// sanitize turns a pane key into a filename.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			b.WriteRune(r)
		case r == '/', r == '#':
			b.WriteByte('-')
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
