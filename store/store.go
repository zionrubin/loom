// Package store implements Loom's state layer: a content-addressed artifact
// store (CAS), a deterministic result cache built on it, and lineage
// tracking.
//
// The cache doubles as Loom's checkpoint mechanism: task results are keyed by
// a deterministic fingerprint of (operation spec, input content), so
// re-running a pipeline — after a crash, a partial failure, or on identical
// inputs — replays completed AI work from the cache instead of re-spending
// tokens. With a state directory configured, this survives process restarts.
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
)

// Hash returns the hex SHA-256 of data.
func Hash(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// Key builds a deterministic cache key from parts via canonical JSON
// (encoding/json marshals map keys in sorted order) hashed with SHA-256.
func Key(parts ...any) (string, error) {
	b, err := json.Marshal(parts)
	if err != nil {
		return "", fmt.Errorf("cache key: %w", err)
	}
	return Hash(b), nil
}

// CAS is a content-addressed store. With dir == "" it is memory-only;
// otherwise blobs are also persisted under dir for durability across runs.
type CAS struct {
	mu  sync.RWMutex
	mem map[string][]byte
	dir string
}

// NewCAS opens a CAS, creating dir if given.
func NewCAS(dir string) (*CAS, error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return &CAS{mem: map[string][]byte{}, dir: dir}, nil
}

// Put stores data and returns its hash.
func (c *CAS) Put(data []byte) (string, error) {
	h := Hash(data)
	c.mu.Lock()
	if _, ok := c.mem[h]; !ok {
		cp := make([]byte, len(data))
		copy(cp, data)
		c.mem[h] = cp
	}
	c.mu.Unlock()
	if c.dir != "" {
		path := filepath.Join(c.dir, h)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if err := os.WriteFile(path, data, 0o644); err != nil {
				return h, fmt.Errorf("cas persist: %w", err)
			}
		}
	}
	return h, nil
}

// Get returns the blob for hash, consulting memory then disk.
func (c *CAS) Get(hash string) ([]byte, bool) {
	c.mu.RLock()
	b, ok := c.mem[hash]
	c.mu.RUnlock()
	if ok {
		return b, true
	}
	if c.dir != "" {
		b, err := os.ReadFile(filepath.Join(c.dir, hash))
		if err == nil {
			c.mu.Lock()
			c.mem[hash] = b
			c.mu.Unlock()
			return b, true
		}
	}
	return nil, false
}

// Cache maps deterministic task keys to CAS artifacts holding record slices.
// With dir != "", the index is persisted as JSONL and reloaded on open,
// giving cross-run resume.
//
// The index is also re-read on a miss, which is what makes resume work across
// *processes* rather than only across runs. Several executors sharing a state
// directory — a fleet of worker processes, a batch beside a service — each
// hold their own fold of one append-only file, and an entry another process
// wrote is invisible until this one looks again. Looking on a miss is the
// cheapest possible place to do it: a miss is about to cost a model call, so a
// file read is free by comparison, and a hit never touches the disk at all. A
// size check keeps a cold run from re-reading its own writes.
type Cache struct {
	mu   sync.Mutex
	idx  map[string]string
	cas  *CAS
	file *os.File
	path string // the index, "" when memory-only
	off  int64  // how far into the index this process has folded
}

type cacheIndexEntry struct {
	Key      string `json:"key"`
	Artifact string `json:"artifact"`
}

// NewCache opens a cache over cas. If dir is non-empty the index persists at
// dir/index.jsonl.
func NewCache(cas *CAS, dir string) (*Cache, error) {
	c := &Cache{idx: map[string]string{}, cas: cas}
	if dir == "" {
		return c, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c.path = filepath.Join(dir, "index.jsonl")
	c.mu.Lock()
	c.syncLocked()
	c.mu.Unlock()

	f, err := os.OpenFile(c.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	c.file = f
	return c, nil
}

// syncLocked folds whatever has been appended to the index since this process
// last looked. Callers hold c.mu.
//
// Only whole records advance the offset, so an append another process is in
// the middle of writing is picked up on the next look rather than folded in
// half.
func (c *Cache) syncLocked() {
	if c.path == "" {
		return
	}
	info, err := os.Stat(c.path)
	if err != nil || info.Size() <= c.off {
		return
	}
	f, err := os.Open(c.path)
	if err != nil {
		return
	}
	defer f.Close()
	base := c.off
	if _, err := f.Seek(base, io.SeekStart); err != nil {
		return
	}
	dec := json.NewDecoder(f)
	for {
		var e cacheIndexEntry
		if err := dec.Decode(&e); err != nil {
			return
		}
		c.idx[e.Key] = e.Artifact
		c.off = base + dec.InputOffset()
	}
}

// Get returns cached records for key.
func (c *Cache) Get(key string) ([]core.Record, bool) {
	c.mu.Lock()
	artifact, ok := c.idx[key]
	if !ok {
		// A miss is worth one look at what other processes have written: the
		// alternative is re-running work the fleet has already paid for.
		c.syncLocked()
		artifact, ok = c.idx[key]
	}
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	blob, ok := c.cas.Get(artifact)
	if !ok {
		return nil, false
	}
	var recs []core.Record
	if err := json.Unmarshal(blob, &recs); err != nil {
		return nil, false
	}
	return recs, true
}

// Put stores records under key and returns the artifact hash.
func (c *Cache) Put(key string, recs []core.Record) (string, error) {
	blob, err := json.Marshal(recs)
	if err != nil {
		return "", err
	}
	artifact, err := c.cas.Put(blob)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, dup := c.idx[key]; !dup {
		c.idx[key] = artifact
		if c.file != nil {
			line := append(mustMarshal(cacheIndexEntry{Key: key, Artifact: artifact}), '\n')
			if n, err := c.file.Write(line); err == nil {
				// Advance past our own append, but only when it landed exactly
				// where our fold ended. With O_APPEND the write goes to the
				// end of the file, which is further along than we have read if
				// another process has written since — and skipping to it would
				// silently drop that process's entries. When that has happened
				// the offset stays put and the next miss re-folds our own line,
				// which costs a few bytes and cannot lose anything.
				if at, err := c.file.Seek(0, io.SeekCurrent); err == nil && at == c.off+int64(n) {
					c.off = at
				}
			}
		}
	}
	return artifact, nil
}

// mustMarshal encodes an index entry, which cannot fail: both fields are
// strings.
func mustMarshal(e cacheIndexEntry) []byte {
	b, _ := json.Marshal(e)
	return b
}

// Len returns the number of cached entries.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.idx)
}

// Close flushes the persistent index, if any.
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file != nil {
		return c.file.Close()
	}
	return nil
}

// Broadcasts holds a run's shared read-only values. Each value is serialized
// and stored in the CAS once; task envelopes carry only the resulting content
// hash. That indirection is what makes a value shareable between tasks and
// between executors: a table read by ten thousand tasks is stored once, and a
// remote worker fetches it once rather than receiving a copy per task.
//
// Values are JSON-serializable by construction, so a task that references one
// stays shippable to another process or machine. Registration happens once,
// before any task runs; afterwards the type is read-only and safe for
// concurrent use.
type Broadcasts struct {
	cas *CAS

	mu     sync.RWMutex
	hashes map[string]string // name → content hash
	values map[string]any    // content hash → decoded value (memoized)
}

// NewBroadcasts returns an empty broadcast set backed by cas.
func NewBroadcasts(cas *CAS) *Broadcasts {
	return &Broadcasts{cas: cas, hashes: map[string]string{}, values: map[string]any{}}
}

// Register serializes value, stores it in the CAS, and binds it to name,
// returning the content hash. Registering identical content twice costs
// nothing extra: the CAS deduplicates by hash.
func (b *Broadcasts) Register(name string, value any) (string, error) {
	if name == "" {
		return "", fmt.Errorf("broadcast: empty name")
	}
	blob, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("broadcast %q: value must be JSON-serializable: %w", name, err)
	}
	hash, err := b.cas.Put(blob)
	if err != nil {
		return "", fmt.Errorf("broadcast %q: %w", name, err)
	}
	b.mu.Lock()
	b.hashes[name] = hash
	b.mu.Unlock()
	return hash, nil
}

// Hashes returns name → content hash for every registered broadcast. The
// planner folds the hashes a stage declares into that stage's fingerprint, so
// changing a broadcast's value invalidates exactly the cached results that
// could have observed it.
func (b *Broadcasts) Hashes() map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]string, len(b.hashes))
	for k, v := range b.hashes {
		out[k] = v
	}
	return out
}

// Resolve decodes the value stored under a content hash, memoizing the decode
// so repeated reads across tasks cost one map lookup. Executors resolve by
// hash rather than by name: a worker holding nothing but an envelope can serve
// the value straight from shared storage.
//
// The returned value is shared with every other reader and must not be
// mutated.
func (b *Broadcasts) Resolve(hash string) (any, error) {
	b.mu.RLock()
	v, ok := b.values[hash]
	b.mu.RUnlock()
	if ok {
		return v, nil
	}
	blob, ok := b.cas.Get(hash)
	if !ok {
		return nil, fmt.Errorf("broadcast artifact %s: not found", hash)
	}
	var decoded any
	if err := json.Unmarshal(blob, &decoded); err != nil {
		return nil, fmt.Errorf("broadcast artifact %s: %w", hash, err)
	}
	b.mu.Lock()
	b.values[hash] = decoded
	b.mu.Unlock()
	return decoded, nil
}

// BroadcastEntry describes one registered shared value: its name, the content
// hash tasks reference it by, and the serialized bytes that hash resolves to.
type BroadcastEntry struct {
	Name  string
	Hash  string
	Bytes int
	JSON  string // the serialized value
}

// Entries lists the registered broadcasts sorted by name. Observability
// consumers use it to report what a run agreed to share before any task
// reads it.
func (b *Broadcasts) Entries() []BroadcastEntry {
	b.mu.RLock()
	hashes := make(map[string]string, len(b.hashes))
	names := make([]string, 0, len(b.hashes))
	for n, h := range b.hashes {
		names = append(names, n)
		hashes[n] = h
	}
	b.mu.RUnlock()

	sort.Strings(names)
	out := make([]BroadcastEntry, 0, len(names))
	for _, n := range names {
		e := BroadcastEntry{Name: n, Hash: hashes[n]}
		if blob, ok := b.cas.Get(e.Hash); ok {
			e.Bytes = len(blob)
			e.JSON = string(blob)
		}
		out = append(out, e)
	}
	return out
}

// Len returns the number of registered broadcasts.
func (b *Broadcasts) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.hashes)
}

// LineageEntry records how an artifact came to exist: the operation
// fingerprint, model, and input hashes. Together with the CAS this gives
// reproducibility and audit: any output can be traced to the exact op,
// model, and inputs that produced it.
type LineageEntry struct {
	Artifact string   `json:"artifact"`
	RunID    string   `json:"run_id"`
	Stage    string   `json:"stage"`
	Op       string   `json:"op"`
	Model    string   `json:"model,omitempty"`
	Inputs   []string `json:"inputs,omitempty"`
	// Broadcasts names the run-level shared values (name → content hash) the
	// producing task could read. They are inputs too — just ones that arrived
	// by reference rather than in the record stream.
	Broadcasts map[string]string `json:"broadcasts,omitempty"`
	Time       time.Time         `json:"time"`
}

// Lineage is an append-only, concurrency-safe lineage log.
type Lineage struct {
	mu      sync.Mutex
	entries []LineageEntry
}

// Record appends an entry, stamping Time if unset.
func (l *Lineage) Record(e LineageEntry) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	l.mu.Lock()
	l.entries = append(l.entries, e)
	l.mu.Unlock()
}

// Entries returns a copy of all lineage entries.
func (l *Lineage) Entries() []LineageEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LineageEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// RecordHashes returns content hashes for records (used as lineage inputs).
func RecordHashes(recs []core.Record) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		b, _ := json.Marshal(r)
		out = append(out, Hash(b))
	}
	return out
}
