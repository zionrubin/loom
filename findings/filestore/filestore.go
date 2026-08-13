// Package filestore is a findings backend over a shared directory: a commons
// several executor processes can share with no database, no server and no
// coordination system between them.
//
// It exists for two reasons, and neither is "PostgreSQL is too heavy". The
// first is that a set of interfaces with one implementation is not a set of
// interfaces — it is an abstraction nobody has tested the shape of — and the
// cheapest honest test of whether findings.Store, findings.VectorStore and
// findings.Leases are really replaceable is to replace them. Nothing in the
// gate distinguishes this backend from the PostgreSQL one; the tests that prove
// executors share findings, coalesce their calls and survive each other's
// crashes run against both.
//
// The second is that a great many fleets are one machine. A batch of worker
// processes started by a systemd unit, a laptop running four executors to see
// what the layer does, a CI job proving a pipeline works — all of them want
// cross-process sharing and none of them wants a database. A directory on a
// local filesystem gives them exactly the guarantees they need:
//
//   - an append-only log, replayed incrementally from a byte offset, so a
//     reader pays for what has changed rather than for what exists;
//   - a lock directory created with O_EXCL, which is atomic on every POSIX
//     filesystem and on Windows, so mutation is serialized between processes;
//   - findings, vectors, leases, verdicts and citations in one place, so a
//     "backend" is a path and nothing else.
//
// What it is not is a distributed backend. A shared directory is a machine
// (or an NFS mount, whose locking guarantees are exactly as good as its
// administrator's claims), so use it for many processes on one host and use
// findings/pgstore for many hosts.
package filestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"context"

	"github.com/zionrubin/loom/findings"
)

// Store is a findings backend rooted at a directory. It implements
// findings.Store, findings.VectorStore and findings.Leases — that is,
// findings.Backend — and is safe for concurrent use by any number of goroutines
// and processes.
type Store struct {
	dir  string
	opts Options

	mu     sync.Mutex
	offset int64 // how far into the log this process has replayed
	state  *state
}

// Options tunes the backend. The zero value works.
type Options struct {
	// Scan bounds how many vectors a similarity search scores (default 2048).
	// The search is linear because the store is a file; the bound is what keeps
	// a large commons from making it unboundedly slow.
	Scan int
	// LockTimeout is how long a writer waits for the directory lock before
	// giving up (default 5s), and LockStale how old a lock must be before it is
	// assumed to belong to a process that died holding it (default 30s).
	LockTimeout time.Duration
	LockStale   time.Duration
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

const (
	defaultScan        = 2048
	defaultLockTimeout = 5 * time.Second
	defaultLockStale   = 30 * time.Second
	logName            = "commons.jsonl"
	lockName           = "commons.lock"
)

func (o Options) normalize() Options {
	if o.Scan <= 0 {
		o.Scan = defaultScan
	}
	if o.LockTimeout <= 0 {
		o.LockTimeout = defaultLockTimeout
	}
	if o.LockStale <= 0 {
		o.LockStale = defaultLockStale
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Open opens (and creates) a shared commons at dir.
func Open(dir string, opts Options) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("filestore: a directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, opts: opts.normalize(), state: newState()}
	return s, nil
}

// Close releases the store. The log is the durable state, so there is nothing
// to flush: every mutation was already written and fsync-free durability is the
// same promise the local ledger's log makes.
func (s *Store) Close() error { return nil }

// Dir returns the directory this commons lives in.
func (s *Store) Dir() string { return s.dir }

// --- The log ------------------------------------------------------------

// op is one mutation. The log is a sequence of them, and the state every reader
// holds is their fold — which is what makes a file into a store several
// processes can share: they do not need to agree on the state, only on the
// bytes, and the bytes are append-only.
type op struct {
	Kind string `json:"kind"`

	Entry *findings.Entry `json:"entry,omitempty"`

	Hash  string  `json:"hash,omitempty"`
	Key   string  `json:"key,omitempty"`
	ID    string  `json:"id,omitempty"`
	Value float64 `json:"value,omitempty"`
	OK    bool    `json:"ok,omitempty"`

	Dep     *findings.Dependent `json:"dep,omitempty"`
	Verdict *findings.Judgement `json:"verdict,omitempty"`
	Lease   *findings.Lease     `json:"lease,omitempty"`
	Vector  []float32           `json:"vector,omitempty"`
	Topic   string              `json:"topic,omitempty"`
	Class   string              `json:"class,omitempty"`
	Reason  string              `json:"reason,omitempty"`
	At      time.Time           `json:"at,omitempty"`
	Active  bool                `json:"active,omitempty"`
}

const (
	opEntry       = "entry"
	opCorroborate = "corroborate"
	opAlias       = "alias"
	opThreshold   = "threshold"
	opVerdict     = "verdict"
	opCite        = "cite"
	opVector      = "vector"
	opLease       = "lease"
)

// state is the fold of the log: everything the store answers questions from.
type state struct {
	entries map[string]*findings.Entry // by hash
	order   []string                   // hashes in log order
	byKey   map[string][]string        // question key → hashes
	byClass map[string][]string        // subject class → hashes
	byID    map[string][]string        // claim ID → hashes, oldest first
	heads   map[string]string          // claim ID → current revision hash
	byKnown map[string][]string        // knowledge hash → hashes
	deps    map[string][]findings.Dependent
	verdict map[string]findings.Judgement // questionKey|hash
	vectors map[string]vector
	leases  map[string]findings.Lease
}

type vector struct {
	topic, class string
	embedding    []float32
	active       bool
}

func newState() *state {
	return &state{
		entries: map[string]*findings.Entry{},
		byKey:   map[string][]string{},
		byClass: map[string][]string{},
		byID:    map[string][]string{},
		heads:   map[string]string{},
		byKnown: map[string][]string{},
		deps:    map[string][]findings.Dependent{},
		verdict: map[string]findings.Judgement{},
		vectors: map[string]vector{},
		leases:  map[string]findings.Lease{},
	}
}

// sync replays whatever other processes have appended since this one last
// looked. Callers hold s.mu.
//
// Reading from an offset is what makes a file-backed store usable rather than
// merely correct: the cost of a lookup is the cost of the mutations it has not
// seen, which on a busy commons is a handful of lines and on an idle one is a
// stat call.
func (s *Store) sync() error {
	path := filepath.Join(s.dir, logName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() == s.offset {
		return nil
	}
	if info.Size() < s.offset {
		// The log was truncated or replaced underneath us. Replay it whole
		// rather than guessing which half is real.
		s.state, s.offset = newState(), 0
	}
	if _, err := f.Seek(s.offset, io.SeekStart); err != nil {
		return err
	}

	start := s.offset
	dec := json.NewDecoder(f)
	for dec.More() {
		var o op
		if err := dec.Decode(&o); err != nil {
			// A partial record is a writer mid-append. Stop at the last
			// complete one and pick the rest up next time rather than folding
			// half of it — which is the whole reason the offset advances per
			// record decoded rather than per byte read.
			break
		}
		s.state.apply(&o)
		s.offset = start + dec.InputOffset()
	}
	return nil
}

func (s *state) apply(o *op) {
	switch o.Kind {
	case opEntry:
		if o.Entry == nil || o.Entry.Hash == "" {
			return
		}
		e := *o.Entry
		if _, dup := s.entries[e.Hash]; dup {
			return
		}
		s.entries[e.Hash] = &e
		s.order = append(s.order, e.Hash)
		if e.Key != "" {
			s.byKey[e.Key] = append(s.byKey[e.Key], e.Hash)
		}
		if e.Class != "" {
			s.byClass[e.Class] = append(s.byClass[e.Class], e.Hash)
		}
		if e.Knowledge != "" {
			s.byKnown[e.Knowledge] = append(s.byKnown[e.Knowledge], e.Hash)
		}
		if id := e.Finding.ID; id != "" {
			s.byID[id] = append(s.byID[id], e.Hash)
			if head, ok := s.heads[id]; !ok || e.Finding.Rev >= s.entries[head].Finding.Rev {
				s.heads[id] = e.Hash
			}
		}
	case opCorroborate:
		if e, ok := s.entries[o.Hash]; ok {
			e.Corroborations++
		}
	case opAlias:
		if o.Key == "" || o.Hash == "" {
			return
		}
		for _, h := range s.byKey[o.Key] {
			if h == o.Hash {
				return
			}
		}
		s.byKey[o.Key] = append(s.byKey[o.Key], o.Hash)
	case opThreshold:
		if e, ok := s.entries[o.Hash]; ok {
			e.Threshold = o.Value
		}
	case opVerdict:
		if o.Verdict != nil {
			s.verdict[o.Verdict.QuestionKey+"|"+o.Verdict.Hash] = *o.Verdict
		}
	case opCite:
		if o.Dep != nil && o.Hash != "" {
			for _, d := range s.deps[o.Hash] {
				if d == *o.Dep {
					return
				}
			}
			s.deps[o.Hash] = append(s.deps[o.Hash], *o.Dep)
		}
	case opVector:
		if o.Hash == "" {
			return
		}
		v := s.vectors[o.Hash]
		if len(o.Vector) > 0 {
			v.embedding = o.Vector
		}
		if o.Topic != "" {
			v.topic = o.Topic
		}
		if o.Class != "" {
			v.class = o.Class
		}
		v.active = o.Active
		s.vectors[o.Hash] = v
	case opLease:
		if o.Lease != nil {
			s.leases[o.Lease.Key] = *o.Lease
		}
	}
}

// write appends operations under the directory lock, then folds them into this
// process's state.
func (s *Store) write(ops ...op) error {
	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()
	return s.writeLocked(ops...)
}

// writeLocked appends operations. Callers hold both the directory lock and s.mu
// when they need the state to reflect them.
func (s *Store) writeLocked(ops ...op) error {
	if len(ops) == 0 {
		return nil
	}
	f, err := os.OpenFile(filepath.Join(s.dir, logName),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	for i := range ops {
		line, err := json.Marshal(ops[i])
		if err != nil {
			return err
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	return nil
}

// --- The directory lock -------------------------------------------------

// lock takes the cross-process write lock.
//
// A directory created with O_EXCL is the portable atomic test-and-set: the
// filesystem either creates it or reports that it exists, on every POSIX system
// and on Windows, with no advisory-locking semantics to reason about. The stale
// break is what keeps a process that died holding it from stopping the commons
// forever, which is the same reasoning the leases themselves are built on one
// level up.
func (s *Store) lock() (func(), error) {
	path := filepath.Join(s.dir, lockName)
	deadline := time.Now().Add(s.opts.LockTimeout)
	wait := 200 * time.Microsecond
	for {
		err := os.Mkdir(path, 0o755)
		if err == nil {
			return func() { _ = os.Remove(path) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		if info, serr := os.Stat(path); serr == nil {
			if time.Since(info.ModTime()) > s.opts.LockStale {
				_ = os.Remove(path)
				continue
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("filestore: lock at %s is held (waited %s)", path, s.opts.LockTimeout)
		}
		time.Sleep(wait)
		if wait < 5*time.Millisecond {
			wait *= 2
		}
	}
}

// read replays and then runs fn against the state, under s.mu.
func (s *Store) read(fn func(st *state)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.sync(); err != nil {
		return err
	}
	fn(s.state)
	return nil
}

// mutate takes the directory lock, replays, and runs fn — which returns the
// operations to append. It is the read-modify-write the store's correctness
// rests on: corroboration, revision and lease acquisition all decide what to
// write from what is already there.
func (s *Store) mutate(fn func(st *state) ([]op, error)) error {
	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.sync(); err != nil {
		return err
	}
	ops, err := fn(s.state)
	if err != nil || len(ops) == 0 {
		return err
	}
	// Written and not applied: the next read replays them from the log like any
	// other process's, which is what keeps one fold of the log — rather than a
	// fold plus whatever this process remembers writing — the only state there
	// is. Folding an increment twice is exactly the bug that costs.
	return s.writeLocked(ops...)
}

func (s *Store) now() time.Time { return s.opts.Now() }

// --- findings.Store -----------------------------------------------------

// Put records a finding revision, folding rediscovery into corroboration.
func (s *Store) Put(ctx context.Context, e findings.Entry) (findings.Entry, error) {
	if e.Hash == "" {
		return findings.Entry{}, errors.New("filestore: an entry must carry its hash")
	}
	var out findings.Entry
	err := s.mutate(func(st *state) ([]op, error) {
		// Rediscovery: the same claim about the same subject, learned again on
		// some executor. Corroboration is a property of the recording, so it is
		// the only thing an append may change about an entry that exists.
		for _, h := range st.byKnown[e.Knowledge] {
			held := st.entries[h]
			if held == nil || held.Class != e.Class || held.Finding.Retracted {
				continue
			}
			if st.heads[held.Finding.ID] != h {
				continue
			}
			out = *held
			out.Corroborations++
			ops := []op{{Kind: opCorroborate, Hash: h}}
			if e.Key != "" && e.Key != held.Key {
				ops = append(ops, op{Kind: opAlias, Key: e.Key, Hash: h})
			}
			return ops, nil
		}
		if held, ok := st.entries[e.Hash]; ok {
			out = *held
			return nil, nil
		}
		stored := e
		stored.Seq = len(st.order)
		out = stored
		ops := []op{{Kind: opEntry, Entry: &stored}}
		if len(e.Vector) > 0 {
			ops = append(ops, op{
				Kind: opVector, Hash: e.Hash, Vector: e.Vector,
				Topic: e.Finding.Topic, Class: e.Class, Active: true,
			})
		}
		return ops, nil
	})
	if err != nil {
		return findings.Entry{}, err
	}
	return out, nil
}

// Candidates returns the live entries recorded under a question key or about a
// subject class, newest first.
func (s *Store) Candidates(ctx context.Context, q findings.CandidateQuery) ([]findings.Entry, error) {
	var out []findings.Entry
	err := s.read(func(st *state) {
		seen := map[string]bool{}
		add := func(hashes []string) {
			for _, h := range hashes {
				if seen[h] {
					continue
				}
				seen[h] = true
				e := st.entries[h]
				if e == nil || !st.live(h) {
					continue
				}
				out = append(out, *e)
			}
		}
		if q.Key != "" {
			add(st.byKey[q.Key])
		}
		if q.Class != "" {
			add(st.byClass[q.Class])
		}
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq > out[j].Seq })
	if q.Limit > 0 && len(out) > q.Limit {
		out = out[:q.Limit]
	}
	return out, nil
}

// live reports whether a hash is the current, unretracted revision of its
// claim.
func (st *state) live(hash string) bool {
	e := st.entries[hash]
	if e == nil || e.Finding.Retracted {
		return false
	}
	if id := e.Finding.ID; id != "" {
		if head, ok := st.heads[id]; ok && head != hash {
			return false
		}
	}
	return true
}

// Fetch resolves entries by hash, including superseded and retracted ones.
func (s *Store) Fetch(ctx context.Context, hashes []string) ([]findings.Entry, error) {
	var out []findings.Entry
	err := s.read(func(st *state) {
		for _, h := range hashes {
			if e, ok := st.entries[h]; ok {
				out = append(out, *e)
			}
		}
	})
	return out, err
}

// Retract withdraws a claim and returns everything served one of its revisions.
func (s *Store) Retract(ctx context.Context, id, reason string, now time.Time) ([]findings.Dependent, error) {
	var deps []findings.Dependent
	err := s.mutate(func(st *state) ([]op, error) {
		head, ok := st.heads[id]
		if !ok {
			return nil, fmt.Errorf("filestore: no finding %q", id)
		}
		prev := st.entries[head]
		f := prev.Finding
		f.Rev++
		f.Retracted = true
		f.Note = reason
		f.Supersedes = prev.Hash

		e := findings.Entry{
			Hash: f.Hash(), Finding: f, Key: prev.Key, Class: prev.Class,
			Knowledge: prev.Knowledge, Learned: now, Learner: prev.Learner,
			Executor: prev.Executor, Threshold: prev.Threshold, Seq: len(st.order),
		}
		ops := []op{{Kind: opEntry, Entry: &e}}
		for _, h := range st.byID[id] {
			deps = append(deps, st.deps[h]...)
			ops = append(ops, op{Kind: opVector, Hash: h, Active: false})
		}
		return ops, nil
	})
	if err != nil {
		return nil, err
	}
	return deps, nil
}

// Cite records that a finding was served to a task on some executor.
func (s *Store) Cite(ctx context.Context, hash string, d findings.Dependent) error {
	if hash == "" {
		return nil
	}
	return s.write(op{Kind: opCite, Hash: hash, Dep: &d})
}

// Dependents returns everything that was served a finding.
func (s *Store) Dependents(ctx context.Context, hash string) ([]findings.Dependent, error) {
	var out []findings.Dependent
	err := s.read(func(st *state) {
		out = append(out, st.deps[hash]...)
	})
	return out, err
}

// Verdicts returns the adjudications recorded against a set of findings.
func (s *Store) Verdicts(ctx context.Context, hashes []string, limit int) ([]findings.Judgement, error) {
	want := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		want[h] = true
	}
	var out []findings.Judgement
	err := s.read(func(st *state) {
		for _, j := range st.verdict {
			if want[j.Hash] {
				out = append(out, j)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Decided.After(out[j].Decided) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// RecordVerdict memoizes an adjudication for every executor.
func (s *Store) RecordVerdict(ctx context.Context, j findings.Judgement) error {
	if j.QuestionKey == "" || j.Hash == "" {
		return nil
	}
	if j.Decided.IsZero() {
		j.Decided = s.now()
	}
	return s.write(op{Kind: opVerdict, Verdict: &j})
}

// SetThreshold persists an entry's learned near-match boundary.
func (s *Store) SetThreshold(ctx context.Context, hash string, threshold float64) error {
	if hash == "" || threshold <= 0 {
		return nil
	}
	return s.write(op{Kind: opThreshold, Hash: hash, Value: threshold})
}

// Topics summarizes what the commons holds, by topic.
func (s *Store) Topics(ctx context.Context) ([]findings.TopicStat, error) {
	byTopic := map[string]*findings.TopicStat{}
	err := s.read(func(st *state) {
		for _, h := range st.order {
			e := st.entries[h]
			stat, ok := byTopic[e.Finding.Topic]
			if !ok {
				stat = &findings.TopicStat{Topic: e.Finding.Topic}
				byTopic[e.Finding.Topic] = stat
			}
			stat.Entries++
			if e.Finding.Retracted {
				stat.Retracted++
				continue
			}
			if !st.live(h) {
				continue
			}
			stat.Live++
			stat.Corroborations += e.Corroborations
			if e.Finding.NoEvidence {
				stat.Negative++
			}
			stat.Cost.Add(e.Finding.Cost)
		}
	})
	if err != nil {
		return nil, err
	}
	out := make([]findings.TopicStat, 0, len(byTopic))
	for _, st := range byTopic {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out, nil
}

// --- findings.VectorStore -----------------------------------------------

// Upsert indexes a finding's question embedding.
func (s *Store) Upsert(ctx context.Context, v findings.Vector) error {
	if v.Hash == "" || len(v.Embedding) == 0 {
		return nil
	}
	return s.write(op{
		Kind: opVector, Hash: v.Hash, Vector: v.Embedding,
		Topic: v.Topic, Class: v.Class, Active: true,
	})
}

// Nearest scores the topic's (or class's) vectors and returns the best matches.
//
// The scan is linear, which is the honest shape for a file: there is no index
// to consult, so the cost is bounded by Options.Scan rather than by cleverness.
// A commons big enough for that to hurt is a commons that wants pgstore.
func (s *Store) Nearest(ctx context.Context, q findings.VectorQuery) ([]findings.VectorMatch, error) {
	var out []findings.VectorMatch
	err := s.read(func(st *state) {
		scanned := 0
		for hash, v := range st.vectors {
			if scanned >= s.opts.Scan {
				break
			}
			if !v.active || len(v.embedding) == 0 {
				continue
			}
			if q.Topic != "" && v.topic != "" && v.topic != q.Topic {
				continue
			}
			if q.Class != "" && v.class != "" && v.class != q.Class {
				continue
			}
			if !st.live(hash) {
				continue
			}
			scanned++
			sim := cosine(q.Embedding, v.embedding)
			if sim < q.MinSimilarity {
				continue
			}
			out = append(out, findings.VectorMatch{Hash: hash, Similarity: sim})
		}
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Similarity > out[j].Similarity })
	if q.TopK > 0 && len(out) > q.TopK {
		out = out[:q.TopK]
	}
	return out, nil
}

// Remove deactivates a finding's vector.
func (s *Store) Remove(ctx context.Context, hash string) error {
	if hash == "" {
		return nil
	}
	return s.write(op{Kind: opVector, Hash: hash, Active: false})
}

func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// --- findings.Leases ----------------------------------------------------

// Acquire takes the research lease for a key, or reports the current holder.
//
// The whole of the mutual exclusion is here: the read of the current lease and
// the write of the new one happen under the directory lock, so two processes
// racing for one key cannot both see it free. The token increments on every
// grant, which is what lets a holder that stalled past its expiry be told from
// the one that replaced it.
func (s *Store) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (findings.Lease, bool, error) {
	var out findings.Lease
	var held bool
	err := s.mutate(func(st *state) ([]op, error) {
		now := s.now()
		cur, exists := st.leases[key]
		if exists && !cur.Done(now) {
			out = cur
			return nil, nil
		}
		next := findings.Lease{
			Key: key, Owner: owner, Token: cur.Token + 1,
			Acquired: now, Expires: now.Add(ttl),
			Takeover: exists && !cur.Released,
		}
		out, held = next, true
		return []op{{Kind: opLease, Lease: &next}}, nil
	})
	if err != nil {
		return findings.Lease{}, false, err
	}
	return out, held, nil
}

// Renew extends a held lease, reporting false when it has been taken over.
func (s *Store) Renew(ctx context.Context, l findings.Lease, ttl time.Duration) (findings.Lease, bool, error) {
	var out findings.Lease
	var still bool
	err := s.mutate(func(st *state) ([]op, error) {
		cur, ok := st.leases[l.Key]
		if !ok || cur.Owner != l.Owner || cur.Token != l.Token || cur.Released {
			return nil, nil
		}
		next := cur
		next.Expires = s.now().Add(ttl)
		out, still = next, true
		return []op{{Kind: opLease, Lease: &next}}, nil
	})
	if err != nil {
		return findings.Lease{}, false, err
	}
	return out, still, nil
}

// Release ends a lease, waking every follower at once. A stale token is refused
// silently: an owner that was fenced has nothing to release, and letting it
// release the *new* owner's lease would release followers onto a finding
// nobody has contributed.
func (s *Store) Release(ctx context.Context, l findings.Lease) error {
	return s.mutate(func(st *state) ([]op, error) {
		cur, ok := st.leases[l.Key]
		if !ok || cur.Owner != l.Owner || cur.Token != l.Token || cur.Released {
			return nil, nil
		}
		next := cur
		next.Released = true
		next.Expires = s.now()
		return []op{{Kind: opLease, Lease: &next}}, nil
	})
}

// Peek reports the current state of a lease without taking it.
func (s *Store) Peek(ctx context.Context, key string) (findings.Lease, bool, error) {
	var out findings.Lease
	var ok bool
	err := s.read(func(st *state) {
		out, ok = st.leases[key]
	})
	return out, ok, err
}

// Store implements the whole backend contract.
var _ findings.Backend = (*Store)(nil)
