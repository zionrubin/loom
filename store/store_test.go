package store

import (
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
