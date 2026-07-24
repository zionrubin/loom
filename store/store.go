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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zionrubin/brian-ai/loom/core"
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
type Cache struct {
	mu   sync.Mutex
	idx  map[string]string
	cas  *CAS
	file *os.File
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
	path := filepath.Join(dir, "index.jsonl")
	if data, err := os.ReadFile(path); err == nil {
		dec := json.NewDecoder(bytes.NewReader(data))
		for dec.More() {
			var e cacheIndexEntry
			if err := dec.Decode(&e); err != nil {
				break
			}
			c.idx[e.Key] = e.Artifact
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	c.file = f
	return c, nil
}

// Get returns cached records for key.
func (c *Cache) Get(key string) ([]core.Record, bool) {
	c.mu.Lock()
	artifact, ok := c.idx[key]
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
			line, _ := json.Marshal(cacheIndexEntry{Key: key, Artifact: artifact})
			_, _ = c.file.Write(append(line, '\n'))
		}
	}
	return artifact, nil
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

// LineageEntry records how an artifact came to exist: the operation
// fingerprint, model, and input hashes. Together with the CAS this gives
// reproducibility and audit: any output can be traced to the exact op,
// model, and inputs that produced it.
type LineageEntry struct {
	Artifact string    `json:"artifact"`
	RunID    string    `json:"run_id"`
	Stage    string    `json:"stage"`
	Op       string    `json:"op"`
	Model    string    `json:"model,omitempty"`
	Inputs   []string  `json:"inputs,omitempty"`
	Time     time.Time `json:"time"`
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
