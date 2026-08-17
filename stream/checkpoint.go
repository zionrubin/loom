package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Checkpoint is a point a stream job can be restarted from: where each split
// had been read to, what each window stage was holding, and what each sink had
// been told.
//
// The three have to be one object rather than three, because their consistency
// is the entire guarantee. Source positions saved without window buffers would
// resume a job that has forgotten half-filled windows; window buffers saved
// without positions would replay records into windows that already hold them.
// A checkpoint is written when the job is momentarily holding still, so what it
// records is a state the job was actually in.
type Checkpoint struct {
	JobID string `json:"job_id"`
	// Epoch numbers checkpoints from 1. It is what a transactional sink keys
	// its commits on, and what tells a restarted job how far it got.
	Epoch int64     `json:"epoch"`
	Time  time.Time `json:"time"`
	// Watermark is how far event time had advanced.
	Watermark time.Time `json:"watermark"`
	// Positions maps "sourceStage/splitID" to the position after the last
	// record that made it through the pipeline.
	Positions map[string]Position `json:"positions,omitempty"`
	// Windows maps a window stage's ID to its Windower snapshot.
	Windows map[string]json.RawMessage `json:"windows,omitempty"`
	// Sinks maps a sink's stage ID to whatever state it asked to have kept.
	Sinks map[string]json.RawMessage `json:"sinks,omitempty"`
	// Progress is the running total the job reports and resumes from, so an
	// uptime counter does not reset every restart.
	Progress Progress `json:"progress"`
}

// Progress is the cumulative work a job has done, carried across restarts.
type Progress struct {
	Records int64 `json:"records"`
	Panes   int64 `json:"panes"`
	Late    int64 `json:"late"`
	Dropped int64 `json:"dropped"`
	Batches int64 `json:"batches"`
}

// Add accumulates another progress into p.
func (p *Progress) Add(o Progress) {
	p.Records += o.Records
	p.Panes += o.Panes
	p.Late += o.Late
	p.Dropped += o.Dropped
	p.Batches += o.Batches
}

// Store holds a job's checkpoints. A job needs only the latest, but a store
// that keeps a few lets an operator roll back to before the deploy that broke
// something.
type Store interface {
	// Save records a checkpoint durably. It must not return until the
	// checkpoint would survive a crash, because the caller commits source
	// positions on the strength of its return.
	Save(ctx context.Context, ck Checkpoint) error
	// Load returns the latest checkpoint for a job, reporting false when the
	// job has never checkpointed.
	Load(ctx context.Context, jobID string) (Checkpoint, bool, error)
	Close() error
}

// MemStore keeps checkpoints in memory. It is what a job gets when no state
// directory is configured: checkpointing still happens, sinks and sources are
// still committed in the right order, and none of it survives the process —
// which is the honest behavior for a job that was not given anywhere to write.
type MemStore struct {
	mu   sync.Mutex
	last map[string]Checkpoint
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore { return &MemStore{last: map[string]Checkpoint{}} }

func (m *MemStore) Save(_ context.Context, ck Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.last[ck.JobID] = ck
	return nil
}

func (m *MemStore) Load(_ context.Context, jobID string) (Checkpoint, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ck, ok := m.last[jobID]
	return ck, ok, nil
}

func (m *MemStore) Close() error { return nil }

// FileStore keeps checkpoints as JSON files under a directory, newest wins.
//
// Writes are atomic — a temporary file renamed into place — so a crash during a
// checkpoint leaves the previous one intact rather than a truncated newer one.
// That matters more here than in most places that say it: the file is the only
// evidence of what has already been paid for.
type FileStore struct {
	dir  string
	keep int
}

// NewFileStore opens (and creates) a checkpoint directory, retaining the last
// keep checkpoints per job (minimum 1).
func NewFileStore(dir string, keep int) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("stream: checkpoint directory is empty")
	}
	if keep < 1 {
		keep = 1
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("stream: checkpoint dir: %w", err)
	}
	return &FileStore{dir: dir, keep: keep}, nil
}

func (f *FileStore) jobDir(jobID string) string { return filepath.Join(f.dir, safeName(jobID)) }

// Save writes the checkpoint and prunes older ones.
func (f *FileStore) Save(_ context.Context, ck Checkpoint) error {
	dir := f.jobDir(ck.JobID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("stream: checkpoint dir: %w", err)
	}
	// Compact rather than indented: a checkpoint carries every record sitting
	// in a half-filled window, so it is sized by the data in flight rather than
	// by the handful of scalars around it — and indenting a nested snapshot
	// would rewrite the bytes a window stage produced instead of storing them.
	blob, err := json.Marshal(ck)
	if err != nil {
		return fmt.Errorf("stream: encoding checkpoint: %w", err)
	}
	name := fmt.Sprintf("ckpt-%020d.json", ck.Epoch)
	if err := writeFileAtomic(filepath.Join(dir, name), blob); err != nil {
		return err
	}
	// The pointer is written after the checkpoint it names, so a crash between
	// the two leaves a complete checkpoint that nothing points at rather than a
	// pointer to a file that does not exist.
	if err := writeFileAtomic(filepath.Join(dir, "latest"), []byte(strconv.FormatInt(ck.Epoch, 10))); err != nil {
		return err
	}
	f.prune(dir)
	return nil
}

// Load reads the latest checkpoint, falling back to the newest intact file if
// the pointer is missing or names something unreadable.
func (f *FileStore) Load(_ context.Context, jobID string) (Checkpoint, bool, error) {
	dir := f.jobDir(jobID)
	names, err := f.checkpoints(dir)
	if err != nil || len(names) == 0 {
		return Checkpoint{}, false, err
	}
	if blob, err := os.ReadFile(filepath.Join(dir, "latest")); err == nil {
		epoch, convErr := strconv.ParseInt(strings.TrimSpace(string(blob)), 10, 64)
		if convErr == nil {
			want := fmt.Sprintf("ckpt-%020d.json", epoch)
			if ck, ok := readCheckpoint(filepath.Join(dir, want)); ok {
				return ck, true, nil
			}
		}
	}
	for i := len(names) - 1; i >= 0; i-- {
		if ck, ok := readCheckpoint(filepath.Join(dir, names[i])); ok {
			return ck, true, nil
		}
	}
	return Checkpoint{}, false, nil
}

func (f *FileStore) Close() error { return nil }

// checkpoints lists a job's checkpoint files, oldest first.
func (f *FileStore) checkpoints(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stream: reading checkpoints: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "ckpt-") && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	// Zero-padded epochs sort lexically in numeric order.
	sort.Strings(names)
	return names, nil
}

func (f *FileStore) prune(dir string) {
	names, err := f.checkpoints(dir)
	if err != nil {
		return
	}
	for i := 0; i < len(names)-f.keep; i++ {
		_ = os.Remove(filepath.Join(dir, names[i]))
	}
}

func readCheckpoint(path string) (Checkpoint, bool) {
	blob, err := os.ReadFile(path)
	if err != nil {
		return Checkpoint{}, false
	}
	var ck Checkpoint
	if err := json.Unmarshal(blob, &ck); err != nil {
		return Checkpoint{}, false
	}
	return ck, true
}

// writeFileAtomic writes through a temporary file in the same directory, so the
// destination either has the old contents or the new ones and never a partial
// write.
func writeFileAtomic(path string, blob []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(blob); err != nil {
		tmp.Close()
		return fmt.Errorf("stream: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("stream: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	return nil
}

// safeName makes a job ID usable as a directory name.
func safeName(s string) string {
	if s == "" {
		return "job"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
