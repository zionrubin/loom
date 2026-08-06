// Package chromem backs Loom's long-term memory with chromem-go, an embedded
// pure-Go vector database.
//
// It is the recommended step up from memory.InMemory, and the reason is the
// same one that makes store.CAS memory-first with optional persistence:
// chromem-go is a library, not a server. No CGO, no daemon, no container, no
// operational surface — a `go get` and a directory. An application can carry a
// knowledge base of a few hundred thousand documents with nothing else
// deployed, and the interface it plugs into is the same one a hosted index
// (Qdrant, pgvector, Milvus) plugs into when the corpus outgrows one process.
//
// Two things chromem-go does not have, which this adapter adds:
//
//   - Epochs. The database is a mutable set of documents; Loom needs a
//     versioned one, so each document carries the epoch it became visible at
//     and the adapter keeps the current epoch per space in a small sidecar
//     file beside the database.
//   - Staging. Writes are held here until Commit, so nothing a run writes is
//     visible to the run that wrote it.
//
// Embeddings are always supplied by Loom's memory.Embedder and never by
// chromem-go's own embedding function, which defaults to calling OpenAI. The
// collection is created with an embedding function that refuses, so a code
// path that forgot to pass a vector fails loudly instead of quietly issuing an
// unbudgeted, unaudited, un-egress-checked API call.
package chromem

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	cm "github.com/philippgille/chromem-go"

	"github.com/zionrubin/loom/memory"
)

// epochKey is the document metadata key holding the epoch an item became
// visible at. It is zero-padded so lexical and numeric order agree, which
// matters because chromem-go's metadata filter is string equality.
const epochKey = "__loom_epoch"

const epochWidth = 20

// Store implements memory.Store over an embedded chromem-go database.
type Store struct {
	db  *cm.DB
	dir string

	mu     sync.RWMutex
	epochs map[string]uint64
	staged map[string][]memory.Item // space → items awaiting Commit
}

// Open returns a store backed by a chromem-go database.
//
// With dir non-empty the database persists there and reopens with its
// contents and epochs intact; with dir empty it is in-memory, which is what
// tests and short-lived processes want. compress trades CPU for disk on the
// persisted documents.
func Open(dir string, compress bool) (*Store, error) {
	s := &Store{
		dir:    dir,
		epochs: map[string]uint64{},
		staged: map[string][]memory.Item{},
	}
	if dir == "" {
		s.db = cm.NewDB()
		return s, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("chromem: %w", err)
	}
	db, err := cm.NewPersistentDB(filepath.Join(dir, "db"), compress)
	if err != nil {
		return nil, fmt.Errorf("chromem: %w", err)
	}
	s.db = db

	// chromem-go persists documents but exposes no way to enumerate them, so
	// the per-space epoch is kept alongside rather than recomputed from a scan
	// the library cannot offer.
	if data, err := os.ReadFile(s.epochPath()); err == nil {
		if err := json.Unmarshal(data, &s.epochs); err != nil {
			return nil, fmt.Errorf("chromem: epoch file: %w", err)
		}
	}
	return s, nil
}

func (s *Store) epochPath() string { return filepath.Join(s.dir, "epochs.json") }

// refuseEmbedding is the collection's embedding function. Reaching it means a
// document arrived without a vector, and the correct response is to fail: the
// alternative is chromem-go's default, which calls OpenAI outside the task's
// budget, egress allowlist, and audit log.
func refuseEmbedding(context.Context, string) ([]float32, error) {
	return nil, errors.New("chromem: document reached the store without an embedding; " +
		"Loom supplies vectors through memory.Embedder")
}

func (s *Store) collection(name string) (*cm.Collection, error) {
	c, err := s.db.GetOrCreateCollection(name, nil, refuseEmbedding)
	if err != nil {
		return nil, fmt.Errorf("chromem: space %q: %w", name, err)
	}
	return c, nil
}

// Epoch implements memory.Store.
func (s *Store) Epoch(_ context.Context, space string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epochs[space], nil
}

// Upsert implements memory.Store: items are staged, not indexed.
func (s *Store) Upsert(_ context.Context, items []memory.Item) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(items))
	for _, it := range items {
		if it.Space == "" {
			return nil, errors.New("chromem: item with no space")
		}
		if it.ID == "" {
			it.ID = memory.NewItem(it.Space, it.Text, it.Meta).ID
		}
		if it.Created.IsZero() {
			it.Created = time.Now().UTC()
		}
		ids = append(ids, it.ID)
		s.staged[it.Space] = append(s.staged[it.Space], it)
	}
	return ids, nil
}

// Commit implements memory.Store, indexing each space's staged items at a new
// epoch.
func (s *Store) Commit(ctx context.Context, spaces ...string) (map[string]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(spaces) == 0 {
		for n := range s.staged {
			spaces = append(spaces, n)
		}
	}
	sort.Strings(spaces)

	out := make(map[string]uint64, len(spaces))
	changed := false
	for _, name := range spaces {
		staged := s.staged[name]
		if len(staged) == 0 {
			out[name] = s.epochs[name]
			continue
		}
		coll, err := s.collection(name)
		if err != nil {
			return out, err
		}
		epoch := s.epochs[name] + 1

		// Deduplicate within the batch and against what is already indexed, so
		// the same fact staged twice costs one document and keeps the
		// provenance of the run that first learned it.
		docs := make([]cm.Document, 0, len(staged))
		seen := make(map[string]bool, len(staged))
		for _, it := range staged {
			if seen[it.ID] {
				continue
			}
			seen[it.ID] = true
			if _, err := coll.GetByID(ctx, it.ID); err == nil {
				continue
			}
			it.Epoch = epoch
			docs = append(docs, cm.Document{
				ID:        it.ID,
				Content:   it.Text,
				Embedding: it.Vector,
				Metadata:  encodeMeta(it, epoch),
			})
		}
		s.staged[name] = nil
		if len(docs) == 0 {
			// Everything staged was already known. Publishing an epoch nothing
			// moved into would invalidate every reader's cache for no change.
			out[name] = s.epochs[name]
			continue
		}
		// Vectors are already computed, so the concurrency here only spreads
		// the persistence writes.
		if err := coll.AddDocuments(ctx, docs, runtime.NumCPU()); err != nil {
			return out, fmt.Errorf("chromem: space %q: %w", name, err)
		}
		s.epochs[name] = epoch
		out[name] = epoch
		changed = true
	}
	if changed {
		if err := s.persistEpochs(); err != nil {
			return out, err
		}
	}
	return out, nil
}

// persistEpochs writes the sidecar atomically, so a crash mid-write leaves the
// previous epochs readable rather than a truncated file.
func (s *Store) persistEpochs() error {
	if s.dir == "" {
		return nil
	}
	blob, err := json.Marshal(s.epochs)
	if err != nil {
		return fmt.Errorf("chromem: epoch file: %w", err)
	}
	tmp := s.epochPath() + ".tmp"
	if err := os.WriteFile(tmp, blob, 0o644); err != nil {
		return fmt.Errorf("chromem: epoch file: %w", err)
	}
	if err := os.Rename(tmp, s.epochPath()); err != nil {
		return fmt.Errorf("chromem: epoch file: %w", err)
	}
	return nil
}

// Search implements memory.Store.
//
// The epoch bound is the part chromem-go cannot express: its metadata filter
// is string equality, and visibility is an inequality. When the query's pin is
// the current epoch — the overwhelmingly common case, since a run pins at
// launch and reads for its own lifetime — nothing is excluded and the index is
// queried for exactly K. When the pin is stale, because another process
// committed while this run was working, the query over-fetches and filters,
// widening until it has K survivors or has seen the whole collection. That is
// slower and exactly correct, which is the right way round for the rare path.
func (s *Store) Search(ctx context.Context, q memory.Query) ([]memory.Hit, error) {
	if q.K <= 0 {
		q.K = 5
	}
	s.mu.RLock()
	current := s.epochs[q.Space]
	s.mu.RUnlock()

	coll, err := s.collection(q.Space)
	if err != nil {
		return nil, err
	}
	total := coll.Count()
	if total == 0 {
		return nil, nil
	}

	stale := q.AsOf != memory.Latest && q.AsOf < current
	fetch := q.K
	if stale {
		fetch = q.K * 4
	}
	for {
		if fetch > total {
			fetch = total
		}
		res, err := coll.QueryEmbedding(ctx, q.Vector, fetch, encodeFilter(q.Filter), nil)
		if err != nil {
			return nil, fmt.Errorf("chromem: space %q: %w", q.Space, err)
		}
		hits := make([]memory.Hit, 0, len(res))
		for _, r := range res {
			it := decodeItem(q.Space, r)
			if q.AsOf != memory.Latest && it.Epoch > q.AsOf {
				continue
			}
			if r.Similarity < q.MinScore {
				continue
			}
			hits = append(hits, memory.Hit{Item: it, Score: r.Similarity})
		}
		if len(hits) >= q.K || fetch >= total {
			return memory.TopK(hits, q.K), nil
		}
		fetch *= 2
	}
}

// Get implements memory.Store.
func (s *Store) Get(ctx context.Context, space, id string) (memory.Item, bool, error) {
	coll, err := s.collection(space)
	if err != nil {
		return memory.Item{}, false, err
	}
	doc, err := coll.GetByID(ctx, id)
	if err != nil {
		return memory.Item{}, false, nil
	}
	return decodeItem(space, cm.Result{
		ID: doc.ID, Content: doc.Content, Metadata: doc.Metadata, Embedding: doc.Embedding,
	}), true, nil
}

// Spaces implements memory.Store.
func (s *Store) Spaces(context.Context) ([]string, error) {
	out := make([]string, 0)
	for name := range s.db.ListCollections() {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// Close implements memory.Store, flushing the epoch sidecar. chromem-go
// persists each document as it is added, so there is nothing else to flush.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistEpochs()
}

// encodeMeta flattens an item's metadata and provenance into chromem-go's
// string map. Provenance travels with the document rather than in a side
// table, so an item exported from the database still says which run produced
// it.
func encodeMeta(it memory.Item, epoch uint64) map[string]string {
	out := make(map[string]string, len(it.Meta)+7)
	for k, v := range it.Meta {
		out[k] = fmt.Sprint(v)
	}
	out[epochKey] = fmt.Sprintf("%0*d", epochWidth, epoch)
	setIf(out, "__loom_run", it.Source.RunID)
	setIf(out, "__loom_stage", it.Source.Stage)
	setIf(out, "__loom_task", it.Source.Task)
	setIf(out, "__loom_model", it.Source.Model)
	setIf(out, "__loom_op", it.Source.Op)
	if !it.Created.IsZero() {
		out["__loom_created"] = it.Created.UTC().Format(time.RFC3339Nano)
	}
	return out
}

func setIf(m map[string]string, k, v string) {
	if v != "" {
		m[k] = v
	}
}

// encodeFilter renders a query filter for chromem-go, which matches metadata
// by string equality — the same semantics memory.Matches applies.
func encodeFilter(f map[string]string) map[string]string {
	if len(f) == 0 {
		return nil
	}
	out := make(map[string]string, len(f))
	for k, v := range f {
		out[k] = v
	}
	return out
}

// decodeItem reverses encodeMeta, separating Loom's reserved keys from the
// author's own metadata so a recalled item looks the way it was written.
func decodeItem(space string, r cm.Result) memory.Item {
	it := memory.Item{
		ID: r.ID, Space: space, Text: r.Content, Vector: r.Embedding,
	}
	var meta map[string]any
	for k, v := range r.Metadata {
		switch k {
		case epochKey:
			n, _ := strconv.ParseUint(v, 10, 64)
			it.Epoch = n
		case "__loom_run":
			it.Source.RunID = v
		case "__loom_stage":
			it.Source.Stage = v
		case "__loom_task":
			it.Source.Task = v
		case "__loom_model":
			it.Source.Model = v
		case "__loom_op":
			it.Source.Op = v
		case "__loom_created":
			it.Created, _ = time.Parse(time.RFC3339Nano, v)
		default:
			if meta == nil {
				meta = map[string]any{}
			}
			meta[k] = v
		}
	}
	it.Meta = meta
	return it
}
