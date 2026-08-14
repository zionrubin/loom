package store

import (
	"fmt"
	"testing"

	"github.com/zionrubin/loom/core"
)

func TestCASRoundTrip(t *testing.T) {
	cas, err := NewCAS("")
	if err != nil {
		t.Fatal(err)
	}
	h, err := cas.Put([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := cas.Get(h)
	if !ok || string(got) != "hello" {
		t.Fatalf("Get(%s) = %q, %v", h, got, ok)
	}
	h2, _ := cas.Put([]byte("hello"))
	if h2 != h {
		t.Error("identical content must produce identical hashes")
	}
}

func TestBroadcastsShareOneCopy(t *testing.T) {
	cas, err := NewCAS("")
	if err != nil {
		t.Fatal(err)
	}
	b := NewBroadcasts(cas)

	table := map[string]string{"US": "United States", "FR": "France"}
	hash, err := b.Register("countries", table)
	if err != nil {
		t.Fatal(err)
	}
	if hash == "" || b.Hashes()["countries"] != hash {
		t.Fatalf("Hashes() = %v, want countries → %s", b.Hashes(), hash)
	}

	// Identical content under a second name is the same blob: broadcasts are
	// stored by content, so sharing costs one copy no matter how many
	// references point at it.
	same, err := b.Register("countries-alias", map[string]string{"FR": "France", "US": "United States"})
	if err != nil {
		t.Fatal(err)
	}
	if same != hash {
		t.Error("identical broadcast content must deduplicate to one artifact")
	}

	// Executors resolve by hash, never by name — that is what lets a worker
	// holding only an envelope serve the value.
	v, err := b.Resolve(hash)
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok || m["US"] != "United States" {
		t.Fatalf("Resolve = %#v, want the registered table", v)
	}

	// The decode is memoized: repeated reads return the very same value.
	again, err := b.Resolve(hash)
	if err != nil {
		t.Fatal(err)
	}
	m2, _ := again.(map[string]any)
	if len(m2) != len(m) {
		t.Error("repeated resolution must return the shared value")
	}

	if _, err := b.Resolve("0000"); err == nil {
		t.Error("resolving an unknown artifact must fail")
	}
	if _, err := b.Register("bad", func() {}); err == nil {
		t.Error("a non-serializable broadcast must be rejected at registration")
	}
	if b.Len() != 2 {
		t.Errorf("Len = %d, want 2", b.Len())
	}
}

// TestBroadcastsSurviveRestart proves the sharing story across processes: a
// broadcast written by one run resolves in the next from the same state dir,
// with nothing in memory to carry it.
func TestBroadcastsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	first, err := NewCAS(dir)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := NewBroadcasts(first).Register("rubric", "score 1-5")
	if err != nil {
		t.Fatal(err)
	}

	second, err := NewCAS(dir)
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewBroadcasts(second).Resolve(hash)
	if err != nil {
		t.Fatal(err)
	}
	if v != "score 1-5" {
		t.Errorf("resolved %#v across restart, want the original value", v)
	}
}

func TestKeyDeterminism(t *testing.T) {
	a1, err := Key("op", map[string]any{"b": 2, "a": 1}, []string{"x"})
	if err != nil {
		t.Fatal(err)
	}
	a2, _ := Key("op", map[string]any{"a": 1, "b": 2}, []string{"x"})
	if a1 != a2 {
		t.Error("map key order must not affect the cache key")
	}
	b, _ := Key("op", map[string]any{"a": 1, "b": 3}, []string{"x"})
	if b == a1 {
		t.Error("different content must produce different keys")
	}
}

func TestCachePersistence(t *testing.T) {
	dir := t.TempDir()
	recs := []core.Record{core.NewRecord("r1", map[string]any{"v": "out"})}

	cas, _ := NewCAS(dir + "/cas")
	cache, err := NewCache(cas, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Put("key1", recs); err != nil {
		t.Fatal(err)
	}
	cache.Close()

	// Reopen: index and artifacts must survive — this is Loom's
	// resume-across-restart mechanism.
	cas2, _ := NewCAS(dir + "/cas")
	cache2, err := NewCache(cas2, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cache2.Close()
	got, ok := cache2.Get("key1")
	if !ok || len(got) != 1 || got[0].String("v") != "out" {
		t.Fatalf("persisted cache lookup failed: %v %v", got, ok)
	}
}

func TestLineage(t *testing.T) {
	var l Lineage
	l.Record(LineageEntry{Artifact: "a", Stage: "s", Op: "fp", Inputs: []string{"i1"}})
	entries := l.Entries()
	if len(entries) != 1 || entries[0].Artifact != "a" || entries[0].Time.IsZero() {
		t.Fatalf("unexpected lineage: %+v", entries)
	}
}

// Two live handles on one state directory are what a fleet of worker processes
// is, and the checkpoint property has to hold across them: work one member
// paid for must not be paid for again by the next.
//
// The interleaving is the point. Each handle appends to the same index and
// folds it independently, so a handle that advanced its read offset past
// another's writes would lose them silently — the run would simply cost more,
// with nothing in it looking wrong.
func TestCacheSharedBetweenConcurrentHandles(t *testing.T) {
	dir := t.TempDir()
	open := func() *Cache {
		cas, err := NewCAS(dir + "/cas")
		if err != nil {
			t.Fatal(err)
		}
		c, err := NewCache(cas, dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	a, b := open(), open()

	rec := func(v string) []core.Record {
		return []core.Record{core.NewRecord("r", map[string]any{"v": v})}
	}

	// Interleaved, so each handle's own append lands after the other's.
	for i := range 6 {
		key := fmt.Sprintf("key%d", i)
		writer := a
		if i%2 == 1 {
			writer = b
		}
		if _, err := writer.Put(key, rec(key)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	for _, c := range []*Cache{a, b} {
		for i := range 6 {
			key := fmt.Sprintf("key%d", i)
			got, ok := c.Get(key)
			if !ok {
				t.Fatalf("%s is missing from a handle that did not write it: "+
					"the fleet would re-run work it has already paid for", key)
			}
			if got[0].String("v") != key {
				t.Fatalf("%s resolved to %q", key, got[0].String("v"))
			}
		}
	}

	// And a handle opened afterwards sees everything both of them wrote.
	if fresh := open(); fresh.Len() != 6 {
		t.Fatalf("a fresh handle folded %d entries, want 6", fresh.Len())
	}
}
