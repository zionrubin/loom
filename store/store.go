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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
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

	// flights holds the single-flight lease, under its own mutex: a claim
	// interleaves lease bookkeeping with index lookups, and the index lock is
	// also the one Put holds while writing to disk. Keeping them apart is what
	// stops a task taking a lease from queueing behind a task storing a result.
	fmu     sync.Mutex
	flights map[string]*flight
	// waiting counts the callers parked on somebody else's lease right now.
	waiting atomic.Int64
}

type cacheIndexEntry struct {
	Key      string `json:"key"`
	Artifact string `json:"artifact"`
}

// NewCache opens a cache over cas. If dir is non-empty the index persists at
// dir/index.jsonl.
func NewCache(cas *CAS, dir string) (*Cache, error) {
	c := &Cache{idx: map[string]string{}, cas: cas, flights: map[string]*flight{}}
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

// --- Single-flight lease ------------------------------------------------

// DefaultCoalesceWait bounds how long a task waits behind an identical task
// already running before it gives up on sharing that task's answer and
// computes its own.
//
// It is a ceiling on a wait that is nearly always short — the leader is making
// the same call this task was about to make — and its job is to keep a stalled
// leader from holding a herd of followers in their execution slots for the
// length of a provider timeout. Overshooting it costs a duplicate call, which
// is exactly what the run would have paid without the lease, so the failure
// mode of guessing wrong is the status quo rather than a worse outcome.
const DefaultCoalesceWait = 30 * time.Second

// flight is one task computing the value for a cache key, which later tasks
// with the same key wait on instead of computing it a second time.
//
// The result cache is keyed on a deterministic fingerprint of (op spec, input
// content), so two tasks holding one key are running the same computation —
// which is what makes waiting for the other one sound rather than merely
// cheap. What the cache cannot do on its own is serve the *second* asker while
// the first is still in flight: an entry exists only after a task finishes, so
// identical tasks admitted together all miss, all call the model, and all
// write the same entry. The findings gate regulates that herd for external
// research (see findings.flight); this is the same mechanism one level down,
// for the paid call a task makes itself.
//
// A flight carries no answer, only a completion signal. A released follower
// re-reads the cache rather than taking the leader's records, which costs one
// map lookup and buys the property worth having: a coalesced serve is an
// ordinary cache hit, produced by the same code path and subject to the same
// checks, so collapsing two tasks together can never hand one of them
// something the cache itself would not have served.
type flight struct{ done chan struct{} }

// Claim is the outcome of asking the cache for a key: the records, or the
// obligation to compute them.
//
// On a hit, Records holds the answer and the caller runs nothing. Otherwise
// the caller computes, usually holding the lease for that key, and must
// Release the claim when it is done — on success *after* writing the result
// with Put, so that the followers this claim is holding find the entry rather
// than a gap, and on failure too, so a leader that never got an answer does
// not strand them.
//
// A miss can also arrive holding no lease, when the wait for one ran out. The
// obligation is the same and Release is simply a no-op, which is also what the
// zero Claim is — so a call site can defer Release unconditionally, whether or
// not it went to the cache at all.
//
// A Claim is not safe to copy while it holds a lease, for the same reason a
// lock is not: two copies are two Releases of one thing.
type Claim struct {
	// Records is the cached answer, valid when Hit.
	Records []core.Record
	Hit     bool
	// Coalesced marks a hit that was served by waiting for another task to
	// finish computing it, rather than by finding it already there. It is the
	// saving a cache cannot report on its own, because without a lease those
	// tasks both ran.
	Coalesced bool
	// Waited is how long the wait took, zero unless Coalesced. A run whose
	// coalesced serves are slow is a run whose duplicates are expensive, which
	// is the number that says the lease is earning its place.
	Waited time.Duration

	cache *Cache
	key   string
	fl    *flight
}

// Release ends the claim's lease and wakes whatever is waiting on it. It is
// safe to call on any Claim and safe to call more than once, so the call site
// can defer it beside the claim.
func (c *Claim) Release() {
	if c == nil || c.fl == nil {
		return
	}
	fl := c.fl
	c.fl = nil
	c.cache.retire(c.key, fl)
}

// Claim returns the cached records for key, or a lease to compute them.
//
// It is Get with one addition: on a miss, either this caller takes the lease
// and computes — the ordinary case, and the one a cold cache is made of — or
// it waits for the caller that already holds it and re-reads what that caller
// wrote. The wait is bounded three ways: the leader finishing, ctx, and wait
// (non-positive: DefaultCoalesceWait). Every one of those bounds resolves to
// the same fallback, computing the value, because a lease that failed to save
// a call must never cost an answer.
//
// A leader that fails writes nothing, and its followers wake to a miss. They
// do not fan out: the wait deadline is a total, so the first of them takes the
// vacated lease and the rest queue behind it until their own deadline runs
// out. Serializing retries is the right shape for a failure the tasks share —
// they are the same computation, so what the leader could not do this attempt
// is very likely what the follower cannot do either — and the deadline is what
// keeps that from turning one task's bad luck into a run that waits forever.
func (c *Cache) Claim(ctx context.Context, key string, wait time.Duration) Claim {
	if wait <= 0 {
		wait = DefaultCoalesceWait
	}
	deadline := time.Now().Add(wait)
	var waited time.Duration

	for {
		if recs, ok := c.Get(key); ok {
			return Claim{Records: recs, Hit: true, Coalesced: waited > 0, Waited: waited}
		}

		c.fmu.Lock()
		fl, busy := c.flights[key]
		if !busy {
			fl = &flight{done: make(chan struct{})}
			c.flights[key] = fl
			c.fmu.Unlock()
			// The lookup above is a snapshot, and the caller that was holding
			// this key may have stored its result and let the lease go in the
			// window between reading it and taking the lease. Looking once more
			// from *inside* the lease is what closes that window: a caller
			// standing here is ordered after whoever released, so if there is
			// an entry to find it is visible now. Without it, a task can be
			// admitted to make a call whose answer is already in the cache —
			// rare, and the whole point of the mechanism.
			if recs, ok := c.Get(key); ok {
				c.retire(key, fl)
				return Claim{Records: recs, Hit: true, Coalesced: waited > 0, Waited: waited}
			}
			return Claim{cache: c, key: key, fl: fl}
		}
		c.fmu.Unlock()

		left := time.Until(deadline)
		if left <= 0 {
			// The leader is slower than the bound allows, or a chain of them
			// was. Correctness is not at stake — compute it — but the
			// deduplication is lost, so the caller pays what it would have paid
			// anyway rather than waiting any longer for a saving.
			return Claim{}
		}
		start := time.Now()
		timer := time.NewTimer(left)
		c.waiting.Add(1)
		select {
		case <-fl.done:
		case <-ctx.Done():
			c.waiting.Add(-1)
			timer.Stop()
			// The caller is going away. Hand it a miss rather than an error:
			// its own ctx is about to end whatever it does next, and inventing
			// an outcome here would put a failure in the report for work that
			// never ran.
			return Claim{}
		case <-timer.C:
		}
		c.waiting.Add(-1)
		timer.Stop()
		waited += time.Since(start)
	}
}

// retire removes a flight and releases everyone waiting on it. The flight
// leaves the map before it is signalled, so a woken follower that finds no
// entry starts a fresh lease rather than joining a finished one.
//
// Only the caller that removes the flight signals it. Nothing else can remove
// one, so that is the same thing as "only the first release signals" — which
// is what makes a second Release harmless instead of a closed-channel panic.
func (c *Cache) retire(key string, fl *flight) {
	c.fmu.Lock()
	cur, ok := c.flights[key]
	mine := ok && cur == fl
	if mine {
		delete(c.flights, key)
	}
	c.fmu.Unlock()
	if mine {
		close(fl.done)
	}
}

// InFlight reports how many keys are currently held under a lease, and
// Waiting how many callers are parked on one.
//
// Together they say how much of a run's concurrency is waiting on itself: a
// process whose Waiting is persistently high is one whose tasks duplicate each
// other, which is worth knowing whether the answer is to celebrate the saving
// or to stop generating the duplicates.
func (c *Cache) InFlight() int {
	c.fmu.Lock()
	defer c.fmu.Unlock()
	return len(c.flights)
}

// Waiting reports how many callers are currently blocked on another caller's
// lease.
func (c *Cache) Waiting() int { return int(c.waiting.Load()) }

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
