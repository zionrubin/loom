package main

import (
	"context"
	"strings"
	"testing"

	"github.com/zionrubin/loom/memory"
	chromemstore "github.com/zionrubin/loom/memory/chromem"
)

// backends runs the whole example against every Store implementation, because
// the properties it demonstrates are the interface's, not one backend's.
func backends(t *testing.T) map[string]func(dir string) memory.Store {
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

type outcome struct {
	calls    int
	hits     int
	epoch    uint64
	recalled map[string]int
}

func answer(t *testing.T, store memory.Store, dir string, day int, learn bool) outcome {
	t.Helper()
	res, calls, err := runDay(context.Background(), store, dir, 3, day, days[day-1], learn)
	if err != nil {
		t.Fatalf("day %d: %v", day, err)
	}
	out := outcome{calls: calls, epoch: res.Memory[space], recalled: map[string]int{}}
	for _, s := range res.Report.Stages {
		out.hits += s.CacheHits
	}
	for _, r := range res.StageOutputs["similar"] {
		switch ids := r.Data["memory_ids"].(type) {
		case []string:
			out.recalled[r.ID] = len(ids)
		case []any:
			out.recalled[r.ID] = len(ids)
		}
	}
	return out
}

// TestKnowledgeAccumulates: day 1 has nothing to draw on and day 3 does,
// because the runs in between wrote what they concluded.
func TestKnowledgeAccumulates(t *testing.T) {
	for name, open := range backends(t) {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			store := open(dir)
			defer store.Close()

			first := answer(t, store, dir, 1, true)
			if first.epoch != 0 {
				t.Errorf("day 1 read epoch %d, want 0 — the store starts empty", first.epoch)
			}
			for id, n := range first.recalled {
				if n != 0 {
					t.Errorf("day 1 ticket %s recalled %d item(s) from an empty store", id, n)
				}
			}

			answer(t, store, dir, 2, true)
			third := answer(t, store, dir, 3, true)
			if third.epoch != 2 {
				t.Errorf("day 3 read epoch %d, want 2", third.epoch)
			}
			for id, n := range third.recalled {
				if n == 0 {
					t.Errorf("day 3 ticket %s recalled nothing after two days of learning", id)
				}
			}
		})
	}
}

// TestRecallKeyedInvalidation is the example's headline claim as a test: a
// commit invalidates every recall, and exactly one model call.
func TestRecallKeyedInvalidation(t *testing.T) {
	for name, open := range backends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			store := open(dir)
			defer store.Close()

			for day := 1; day <= 3; day++ {
				answer(t, store, dir, day, true)
			}

			// Warm the cache on the read-only shape.
			warm := answer(t, store, dir, 3, false)
			if warm.calls != 3 {
				t.Fatalf("cold read-only pass made %d model call(s), want 3", warm.calls)
			}

			unchanged := answer(t, store, dir, 3, false)
			if unchanged.calls != 0 {
				t.Errorf("replay against an unchanged knowledge base made %d model "+
					"call(s), want 0", unchanged.calls)
			}
			if unchanged.hits != 6 {
				t.Errorf("replay produced %d cache hit(s), want 6 (3 recalls + 3 drafts)",
					unchanged.hits)
			}

			// One fact, in the billing product. The stage filters by product,
			// so it reaches exactly one of the three tickets.
			if err := commitFact(ctx, store, "billing",
				"invoice missing line items → regenerate the pdf after the nightly rollup"); err != nil {
				t.Fatalf("commit fact: %v", err)
			}

			after := answer(t, store, dir, 3, false)
			if after.epoch != unchanged.epoch+1 {
				t.Fatalf("epoch went %d → %d, want one step", unchanged.epoch, after.epoch)
			}
			if after.calls != 1 {
				t.Errorf("after a commit reaching one of three tickets, the run made %d "+
					"model call(s), want 1 — recall-keyed invalidation is not holding",
					after.calls)
			}
			if after.recalled["d3-3"] == unchanged.recalled["d3-3"] {
				t.Errorf("premise failed: the billing fact did not change d3-3's recall")
			}
			for _, id := range []string{"d3-1", "d3-2"} {
				if after.recalled[id] != unchanged.recalled[id] {
					t.Errorf("premise failed: the billing fact changed %s's recall", id)
				}
			}
		})
	}
}

// TestWritesAreInvisibleToTheirOwnRun: the day that learns a fact does not
// then recall it.
func TestWritesAreInvisibleToTheirOwnRun(t *testing.T) {
	dir := t.TempDir()
	store, err := memory.NewInMemory(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	res, _, err := runDay(context.Background(), store, dir, 3, 1, days[0], true)
	if err != nil {
		t.Fatalf("day 1: %v", err)
	}
	for _, r := range res.StageOutputs["similar"] {
		if got := r.String("memory"); got != "" {
			t.Fatalf("a task recalled something on an empty store:\n%s", got)
		}
	}
	if res.Committed[space] != 1 {
		t.Fatalf("day 1 committed epoch %d, want 1", res.Committed[space])
	}
}

// TestRememberedItemsCarryProvenance: an entry that cannot say where it came
// from is indistinguishable from something the previous run invented.
func TestRememberedItemsCarryProvenance(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := memory.NewInMemory(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

	res, _, err := runDay(ctx, store, dir, 3, 1, days[0], true)
	if err != nil {
		t.Fatalf("day 1: %v", err)
	}

	e := memory.NewHashEmbedder(0)
	vecs, _, err := e.Embed(ctx, memory.Call{}, []string{"payment declined"})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	hits, err := store.Search(ctx, memory.Query{
		Space: space, Vector: vecs[0], K: 10, AsOf: memory.Latest,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) == 0 {
		t.Fatal("day 1 wrote nothing")
	}
	for _, h := range hits {
		src := h.Item.Source
		if src.RunID != res.RunID {
			t.Errorf("item %s names run %q, want %q", h.Item.ID, src.RunID, res.RunID)
		}
		if src.Stage != "learn" {
			t.Errorf("item %s names stage %q, want \"learn\"", h.Item.ID, src.Stage)
		}
		if src.Task == "" || src.Op == "" {
			t.Errorf("item %s is missing its task or op fingerprint: %+v", h.Item.ID, src)
		}
	}
}

// TestUnknownBackendIsRejected keeps the flag honest.
func TestUnknownBackendIsRejected(t *testing.T) {
	_, err := openStore("qdrant", "")
	if err == nil || !strings.Contains(err.Error(), "unknown backend") {
		t.Fatalf("openStore with an unknown backend gave %v", err)
	}
}
