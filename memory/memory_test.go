package memory_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/zionrubin/loom/memory"
	chromemstore "github.com/zionrubin/loom/memory/chromem"
)

// stores exercises every Store implementation against the same contract, so
// the epoch and staging rules the framework relies on cannot hold for one
// backend and quietly not for another.
func stores(t *testing.T) map[string]func(dir string) memory.Store {
	t.Helper()
	return map[string]func(string) memory.Store{
		"inmemory": func(dir string) memory.Store {
			s, err := memory.NewInMemory(dir)
			if err != nil {
				t.Fatalf("open in-memory store: %v", err)
			}
			return s
		},
		"chromem": func(dir string) memory.Store {
			s, err := chromemstore.Open(dir, false)
			if err != nil {
				t.Fatalf("open chromem store: %v", err)
			}
			return s
		},
	}
}

// write stages one item, embedding it with the offline hash embedder.
func write(t *testing.T, s memory.Store, e memory.Embedder, space, text string, meta map[string]any) string {
	t.Helper()
	vecs, _, err := e.Embed(context.Background(), memory.Call{}, []string{text})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	it := memory.NewItem(space, text, meta)
	it.Vector = vecs[0]
	ids, err := s.Upsert(context.Background(), []memory.Item{it})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	return ids[0]
}

func search(t *testing.T, s memory.Store, e memory.Embedder, space, query string, k int, asOf uint64) []memory.Hit {
	t.Helper()
	vecs, _, err := e.Embed(context.Background(), memory.Call{}, []string{query})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	hits, err := s.Search(context.Background(), memory.Query{
		Space: space, Vector: vecs[0], K: k, AsOf: asOf,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	return hits
}

// TestStagedWritesAreInvisibleUntilCommit is the rule that makes long-term
// memory safe to combine with content-addressed replay: a task cannot change
// what a later task in its own run retrieves, so a cached result cannot depend
// on execution order.
func TestStagedWritesAreInvisibleUntilCommit(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := open(t.TempDir())
			defer s.Close()
			e := memory.NewHashEmbedder(0)

			write(t, s, e, "kb", "the deploy pipeline retries on 429", nil)

			if hits := search(t, s, e, "kb", "deploy pipeline", 5, memory.Latest); len(hits) != 0 {
				t.Fatalf("staged item was visible before commit: %d hit(s)", len(hits))
			}
			epochs, err := s.Commit(ctx, "kb")
			if err != nil {
				t.Fatalf("commit: %v", err)
			}
			if epochs["kb"] != 1 {
				t.Fatalf("first commit reached epoch %d, want 1", epochs["kb"])
			}
			if hits := search(t, s, e, "kb", "deploy pipeline", 5, memory.Latest); len(hits) != 1 {
				t.Fatalf("committed item not visible: %d hit(s)", len(hits))
			}
		})
	}
}

// TestPinnedEpochHidesLaterCommits is what a run relies on: it fixes an epoch
// before its first task, and another process committing meanwhile cannot
// change what it retrieves.
func TestPinnedEpochHidesLaterCommits(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := open(t.TempDir())
			defer s.Close()
			e := memory.NewHashEmbedder(0)

			write(t, s, e, "kb", "incident 1: database connection pool exhausted", nil)
			if _, err := s.Commit(ctx, "kb"); err != nil {
				t.Fatalf("commit: %v", err)
			}
			pinned, err := s.Epoch(ctx, "kb")
			if err != nil {
				t.Fatalf("epoch: %v", err)
			}

			// A second run commits while the first is still working.
			write(t, s, e, "kb", "incident 2: database connection pool exhausted again", nil)
			if _, err := s.Commit(ctx, "kb"); err != nil {
				t.Fatalf("commit: %v", err)
			}

			atPin := search(t, s, e, "kb", "database connection pool", 5, pinned)
			if len(atPin) != 1 {
				t.Fatalf("pinned read saw %d item(s), want 1 — the pin did not hold", len(atPin))
			}
			atLatest := search(t, s, e, "kb", "database connection pool", 5, memory.Latest)
			if len(atLatest) != 2 {
				t.Fatalf("latest read saw %d item(s), want 2", len(atLatest))
			}
		})
	}
}

// TestItemsAreContentAddressed: a knowledge base is fed by every run of every
// pipeline pointed at it, so the same conclusion reached twice must cost one
// entry.
func TestItemsAreContentAddressed(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := open(t.TempDir())
			defer s.Close()
			e := memory.NewHashEmbedder(0)

			const fact = "rollbacks require a green canary"
			first := write(t, s, e, "kb", fact, map[string]any{"kind": "rule"})
			second := write(t, s, e, "kb", fact, map[string]any{"kind": "rule"})
			if first != second {
				t.Fatalf("same fact hashed to %s and %s", first, second)
			}
			if _, err := s.Commit(ctx, "kb"); err != nil {
				t.Fatalf("commit: %v", err)
			}
			// And again in a later epoch, the way a nightly rerun would.
			write(t, s, e, "kb", fact, map[string]any{"kind": "rule"})
			if _, err := s.Commit(ctx, "kb"); err != nil {
				t.Fatalf("commit: %v", err)
			}

			hits := search(t, s, e, "kb", fact, 10, memory.Latest)
			if len(hits) != 1 {
				t.Fatalf("stored the same fact %d times, want 1", len(hits))
			}
		})
	}
}

// TestCommitWithoutWritesLeavesEpochAlone: a run that reads the knowledge base
// without adding to it must not invalidate every other reader's cached work.
func TestCommitWithoutWritesLeavesEpochAlone(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := open(t.TempDir())
			defer s.Close()
			e := memory.NewHashEmbedder(0)

			write(t, s, e, "kb", "something worth knowing", nil)
			if _, err := s.Commit(ctx, "kb"); err != nil {
				t.Fatalf("commit: %v", err)
			}
			before, _ := s.Epoch(ctx, "kb")

			for range 3 {
				if _, err := s.Commit(ctx, "kb"); err != nil {
					t.Fatalf("commit: %v", err)
				}
			}
			after, _ := s.Epoch(ctx, "kb")
			if before != after {
				t.Fatalf("empty commits moved the epoch from %d to %d", before, after)
			}
		})
	}
}

// TestSpacesAreIsolated: a space is a partition, and a query against one must
// not see another's items however similar they are.
func TestSpacesAreIsolated(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := open(t.TempDir())
			defer s.Close()
			e := memory.NewHashEmbedder(0)

			write(t, s, e, "tenant-a", "the quarterly revenue target is confidential", nil)
			write(t, s, e, "tenant-b", "the quarterly revenue target is confidential", nil)
			if _, err := s.Commit(ctx); err != nil {
				t.Fatalf("commit: %v", err)
			}

			hits := search(t, s, e, "tenant-a", "quarterly revenue target", 10, memory.Latest)
			if len(hits) != 1 {
				t.Fatalf("space tenant-a returned %d item(s), want 1", len(hits))
			}
			if hits[0].Item.Space != "tenant-a" {
				t.Fatalf("recalled an item from space %q", hits[0].Item.Space)
			}
		})
	}
}

// TestFilterNarrowsRecall covers the structured half of retrieval: a
// nearest-neighbour search is approximate, and a metadata filter is not.
func TestFilterNarrowsRecall(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := open(t.TempDir())
			defer s.Close()
			e := memory.NewHashEmbedder(0)

			write(t, s, e, "kb", "checkout latency regression traced to redis", map[string]any{"team": "payments"})
			write(t, s, e, "kb", "checkout latency regression traced to cdn", map[string]any{"team": "web"})
			if _, err := s.Commit(ctx, "kb"); err != nil {
				t.Fatalf("commit: %v", err)
			}

			vecs, _, err := e.Embed(ctx, memory.Call{}, []string{"checkout latency regression"})
			if err != nil {
				t.Fatalf("embed: %v", err)
			}
			hits, err := s.Search(ctx, memory.Query{
				Space: "kb", Vector: vecs[0], K: 10, AsOf: memory.Latest,
				Filter: map[string]string{"team": "payments"},
			})
			if err != nil {
				t.Fatalf("search: %v", err)
			}
			if len(hits) != 1 {
				t.Fatalf("filtered search returned %d item(s), want 1", len(hits))
			}
			if got := hits[0].Item.Meta["team"]; got != "payments" {
				t.Fatalf("filter admitted team %v", got)
			}
		})
	}
}

// TestRecallOrdersByRelevance: retrieval has to actually retrieve. The hash
// embedder is lexical, so this asserts only what lexical overlap can support.
func TestRecallOrdersByRelevance(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := open(t.TempDir())
			defer s.Close()
			e := memory.NewHashEmbedder(0)

			write(t, s, e, "kb", "kubernetes pods evicted under memory pressure", nil)
			write(t, s, e, "kb", "invoice reconciliation runs on the first of the month", nil)
			write(t, s, e, "kb", "the billing export is a csv upload", nil)
			if _, err := s.Commit(ctx, "kb"); err != nil {
				t.Fatalf("commit: %v", err)
			}

			hits := search(t, s, e, "kb", "pods evicted memory pressure kubernetes", 2, memory.Latest)
			if len(hits) == 0 {
				t.Fatal("recalled nothing")
			}
			if want := "kubernetes pods evicted under memory pressure"; hits[0].Item.Text != want {
				t.Fatalf("best hit is %q, want %q", hits[0].Item.Text, want)
			}
			if hits[0].Score <= 0 {
				t.Fatalf("best hit scored %v, want a positive similarity", hits[0].Score)
			}
		})
	}
}

// TestPersistenceSurvivesReopen: a knowledge base that did not outlive its
// process would not be long-term memory.
func TestPersistenceSurvivesReopen(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "state")
			e := memory.NewHashEmbedder(0)

			s := open(dir)
			write(t, s, e, "kb", "postmortem: the cache stampede was caused by a cold start", nil)
			if _, err := s.Commit(ctx, "kb"); err != nil {
				t.Fatalf("commit: %v", err)
			}
			epoch, _ := s.Epoch(ctx, "kb")
			if err := s.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			reopened := open(dir)
			defer reopened.Close()
			if got, _ := reopened.Epoch(ctx, "kb"); got != epoch {
				t.Fatalf("epoch after reopen is %d, want %d", got, epoch)
			}
			hits := search(t, reopened, e, "kb", "cache stampede cold start", 5, memory.Latest)
			if len(hits) != 1 {
				t.Fatalf("reopened store recalled %d item(s), want 1", len(hits))
			}
		})
	}
}

// TestProvenanceSurvivesRoundTrip: a knowledge base of model output is only
// usable later if each entry says where it came from.
func TestProvenanceSurvivesRoundTrip(t *testing.T) {
	for name, open := range stores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := open(t.TempDir())
			defer s.Close()
			e := memory.NewHashEmbedder(0)

			vecs, _, err := e.Embed(ctx, memory.Call{}, []string{"a conclusion a model reached"})
			if err != nil {
				t.Fatalf("embed: %v", err)
			}
			it := memory.NewItem("kb", "a conclusion a model reached", nil)
			it.Vector = vecs[0]
			it.Source = memory.Source{
				RunID: "run_abc", Stage: "summarize", Task: "task_1", Model: "mock-fast",
			}
			if _, err := s.Upsert(ctx, []memory.Item{it}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
			if _, err := s.Commit(ctx, "kb"); err != nil {
				t.Fatalf("commit: %v", err)
			}

			got, ok, err := s.Get(ctx, "kb", it.ID)
			if err != nil || !ok {
				t.Fatalf("get: ok=%v err=%v", ok, err)
			}
			if got.Source != it.Source {
				t.Fatalf("provenance round-tripped as %+v, want %+v", got.Source, it.Source)
			}
		})
	}
}

// TestMinScoreAllowsRecallingNothing: nearest-neighbour search always returns
// its k nearest, even when the nearest is unrelated.
func TestMinScoreAllowsRecallingNothing(t *testing.T) {
	ctx := context.Background()
	s, err := memory.NewInMemory("")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	e := memory.NewHashEmbedder(0)

	write(t, s, e, "kb", "the invoice export runs nightly", nil)
	if _, err := s.Commit(ctx, "kb"); err != nil {
		t.Fatalf("commit: %v", err)
	}

	vecs, _, err := e.Embed(ctx, memory.Call{}, []string{"volcanic activity in iceland"})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	hits, err := s.Search(ctx, memory.Query{
		Space: "kb", Vector: vecs[0], K: 5, AsOf: memory.Latest, MinScore: 0.5,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("similarity floor admitted %d unrelated item(s)", len(hits))
	}
}

// TestTopKIsDeterministic: recalled IDs are a cache key, so an ordering that
// depended on map iteration would make every rerun a miss.
func TestTopKIsDeterministic(t *testing.T) {
	tied := []memory.Hit{
		{Item: memory.Item{ID: "mem_c"}, Score: 0.5},
		{Item: memory.Item{ID: "mem_a"}, Score: 0.5},
		{Item: memory.Item{ID: "mem_b"}, Score: 0.9},
	}
	want := []string{"mem_b", "mem_a", "mem_c"}
	for range 10 {
		got := memory.IDs(memory.TopK(append([]memory.Hit(nil), tied...), 3))
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("TopK ordered %v, want %v", got, want)
			}
		}
	}
}
