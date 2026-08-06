package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// InMemory is the reference Store: an exact nearest-neighbour index held in
// memory, with an optional append-only journal on disk.
//
// Exact rather than approximate is the right default here for the same reason
// store.CAS is memory-first with optional persistence — it is obviously
// correct, has no tuning surface, needs no dependency, and runs in a test. It
// scans every visible item per query, which is fine into the low hundreds of
// thousands and wrong above that; at that point swap in memory/chromem or a
// hosted index behind the same interface. Nothing above this type knows which
// is underneath.
//
// Durability is a JSONL journal at dir/memory.jsonl, written at Commit and
// replayed at open, so the knowledge base an application accumulates survives
// the process that accumulated it. With dir == "" the store is ephemeral,
// which is what tests want.
type InMemory struct {
	mu     sync.RWMutex
	spaces map[string]*space
	dir    string
	file   *os.File
}

type space struct {
	epoch  uint64
	items  map[string]*Item // committed, by ID
	order  []string         // insertion order, for deterministic scans
	staged map[string]*Item // awaiting the next Commit
}

// NewInMemory opens a store. With dir non-empty the journal under it is
// created if absent and replayed if present.
func NewInMemory(dir string) (*InMemory, error) {
	s := &InMemory{spaces: map[string]*space{}, dir: dir}
	if dir == "" {
		return s, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("memory: %w", err)
	}
	path := filepath.Join(dir, "memory.jsonl")
	if data, err := os.ReadFile(path); err == nil {
		dec := json.NewDecoder(bytes.NewReader(data))
		for dec.More() {
			var it Item
			if err := dec.Decode(&it); err != nil {
				// A torn final line is the expected failure of an append-only
				// journal killed mid-write. Everything before it is intact and
				// is what the store opens with.
				break
			}
			sp := s.space(it.Space)
			if _, dup := sp.items[it.ID]; dup {
				continue
			}
			cp := it
			sp.items[it.ID] = &cp
			sp.order = append(sp.order, it.ID)
			if it.Epoch > sp.epoch {
				sp.epoch = it.Epoch
			}
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("memory: %w", err)
	}
	s.file = f
	return s, nil
}

// space returns (creating if needed) the named partition. Callers hold the
// lock, except during open where the store is not yet shared.
func (s *InMemory) space(name string) *space {
	sp, ok := s.spaces[name]
	if !ok {
		sp = &space{items: map[string]*Item{}, staged: map[string]*Item{}}
		s.spaces[name] = sp
	}
	return sp
}

// Epoch implements Store.
func (s *InMemory) Epoch(_ context.Context, name string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if sp, ok := s.spaces[name]; ok {
		return sp.epoch, nil
	}
	return 0, nil
}

// Search implements Store: a full scan of the space's visible items, filtered,
// scored, and truncated to K.
func (s *InMemory) Search(_ context.Context, q Query) ([]Hit, error) {
	if q.K <= 0 {
		q.K = 5
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	sp, ok := s.spaces[q.Space]
	if !ok {
		return nil, nil
	}
	var hits []Hit
	for _, id := range sp.order {
		it := sp.items[id]
		// Visibility, not recency: an item committed after the run pinned its
		// epoch does not exist as far as this query is concerned.
		if it.Epoch == 0 || (q.AsOf != Latest && it.Epoch > q.AsOf) {
			continue
		}
		if !Matches(*it, q.Filter) {
			continue
		}
		score := Similarity(q.Vector, it.Vector)
		if score < q.MinScore {
			continue
		}
		hits = append(hits, Hit{Item: *it, Score: score})
	}
	return TopK(hits, q.K), nil
}

// Upsert implements Store: items are staged, never made visible here. An ID
// already committed or already staged is dropped — the fact is the same, and
// the provenance worth keeping is the run that first learned it.
func (s *InMemory) Upsert(_ context.Context, items []Item) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]string, 0, len(items))
	for _, it := range items {
		if it.Space == "" {
			return nil, fmt.Errorf("memory: item with no space")
		}
		if it.ID == "" {
			it.ID = it.contentID()
		}
		if it.Created.IsZero() {
			it.Created = time.Now().UTC()
		}
		ids = append(ids, it.ID)

		sp := s.space(it.Space)
		if _, dup := sp.items[it.ID]; dup {
			continue
		}
		if _, dup := sp.staged[it.ID]; dup {
			continue
		}
		it.Epoch = 0
		cp := it
		sp.staged[it.ID] = &cp
	}
	return ids, nil
}

// Commit implements Store. Named spaces with nothing staged keep their epoch:
// a run that read the knowledge base without adding to it must not invalidate
// every other reader's cached work.
func (s *InMemory) Commit(_ context.Context, names ...string) (map[string]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(names) == 0 {
		for n := range s.spaces {
			names = append(names, n)
		}
	}
	sort.Strings(names)

	out := make(map[string]uint64, len(names))
	var journal []Item
	for _, n := range names {
		sp, ok := s.spaces[n]
		if !ok {
			out[n] = 0
			continue
		}
		if len(sp.staged) == 0 {
			out[n] = sp.epoch
			continue
		}
		sp.epoch++
		// Publish in ID order so two stores fed the same items in different
		// orders lay out identically, and a replayed journal reproduces the
		// scan order that produced a given recall.
		ids := make([]string, 0, len(sp.staged))
		for id := range sp.staged {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			it := sp.staged[id]
			it.Epoch = sp.epoch
			sp.items[id] = it
			sp.order = append(sp.order, id)
			journal = append(journal, *it)
		}
		sp.staged = map[string]*Item{}
		out[n] = sp.epoch
	}
	if s.file != nil && len(journal) > 0 {
		var buf bytes.Buffer
		for _, it := range journal {
			line, err := json.Marshal(it)
			if err != nil {
				return out, fmt.Errorf("memory journal: %w", err)
			}
			buf.Write(append(line, '\n'))
		}
		if _, err := s.file.Write(buf.Bytes()); err != nil {
			return out, fmt.Errorf("memory journal: %w", err)
		}
	}
	return out, nil
}

// Get implements Store.
func (s *InMemory) Get(_ context.Context, name, id string) (Item, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sp, ok := s.spaces[name]
	if !ok {
		return Item{}, false, nil
	}
	if it, ok := sp.items[id]; ok {
		return *it, true, nil
	}
	return Item{}, false, nil
}

// Spaces implements Store.
func (s *InMemory) Spaces(context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.spaces))
	for n := range s.spaces {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// Len returns how many items a space holds, committed and staged.
func (s *InMemory) Len(name string) (committed, staged int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if sp, ok := s.spaces[name]; ok {
		return len(sp.items), len(sp.staged)
	}
	return 0, 0
}

// Close implements Store, flushing the journal.
func (s *InMemory) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}
