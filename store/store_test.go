package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// --- Single-flight lease ------------------------------------------------

// waitFor blocks until cond holds, so a lease test synchronises on the state
// it is actually about rather than on a sleep long enough to hope for it.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// newTestCache is a memory-only cache, which is what every lease test wants:
// the lease is a property of the process, not of the index on disk.
func newTestCache(t *testing.T) *Cache {
	t.Helper()
	cas, err := NewCAS("")
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCache(cas, "")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClaimServesAHitWithoutALease(t *testing.T) {
	c := newTestCache(t)
	want := []core.Record{core.NewRecord("r1", map[string]any{"answer": "42"})}
	if _, err := c.Put("k", want); err != nil {
		t.Fatal(err)
	}

	cl := c.Claim(context.Background(), "k", time.Second)
	defer cl.Release()
	if !cl.Hit || len(cl.Records) != 1 || cl.Records[0].String("answer") != "42" {
		t.Fatalf("Claim on a warm key = %+v, want the cached record", cl)
	}
	// An answer that was already there is not a coalesced serve: nothing was
	// collapsed, and reporting it as one would overstate what the lease did.
	if cl.Coalesced {
		t.Error("a plain hit must not be reported as coalesced")
	}
	if n := c.InFlight(); n != 0 {
		t.Errorf("InFlight() = %d after a hit, want 0: a hit takes no lease", n)
	}
}

func TestClaimCollapsesConcurrentIdenticalKeys(t *testing.T) {
	c := newTestCache(t)
	const followers = 7

	// The leader takes the lease first, so the followers below are guaranteed
	// to arrive at a key that is already in flight — which is the case the
	// cache alone cannot serve.
	leader := c.Claim(context.Background(), "k", time.Second)
	if leader.Hit {
		t.Fatal("cold key must miss")
	}

	var wg sync.WaitGroup
	got := make([]Claim, followers)
	for i := range followers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i] = c.Claim(context.Background(), "k", 5*time.Second)
		}()
	}
	// Every follower is provably parked before the leader finishes, which is
	// what makes the coalesced counts below a fact rather than a race.
	waitFor(t, "the followers to park on the lease", func() bool {
		return c.Waiting() == followers
	})

	// One execution, then release — in that order, so a woken follower finds
	// the entry rather than the gap the leader was standing in.
	want := []core.Record{core.NewRecord("r1", map[string]any{"answer": "42"})}
	if _, err := c.Put("k", want); err != nil {
		t.Fatal(err)
	}
	leader.Release()
	wg.Wait()

	for i, cl := range got {
		if !cl.Hit {
			t.Fatalf("follower %d missed: the leader's result must serve it", i)
		}
		if !cl.Coalesced {
			t.Errorf("follower %d served a hit but did not report it as coalesced", i)
		}
		if len(cl.Records) != 1 || cl.Records[0].String("answer") != "42" {
			t.Errorf("follower %d = %v, want the leader's record", i, cl.Records)
		}
		cl.Release()
	}
	if n := c.InFlight(); n != 0 {
		t.Errorf("InFlight() = %d, want 0: every lease must be released", n)
	}
}

func TestClaimReleasesFollowersWhenTheLeaderFails(t *testing.T) {
	c := newTestCache(t)

	leader := c.Claim(context.Background(), "k", time.Second)
	if leader.Hit {
		t.Fatal("cold key must miss")
	}

	done := make(chan Claim, 1)
	go func() { done <- c.Claim(context.Background(), "k", 5*time.Second) }()
	waitFor(t, "the follower to park on the lease", func() bool { return c.Waiting() == 1 })

	// A failing task writes nothing and releases anyway. The follower must
	// come away with the obligation to compute, never with an error the
	// leader received and it did not.
	leader.Release()

	select {
	case cl := <-done:
		if cl.Hit {
			t.Fatal("a leader that stored nothing cannot produce a hit")
		}
		if cl.fl == nil {
			t.Error("the vacated lease must pass to the follower, so the next " +
				"arrival waits rather than making a third call")
		}
		cl.Release()
	case <-time.After(5 * time.Second):
		t.Fatal("follower still waiting on a released lease")
	}
}

func TestClaimGivesUpOnASlowLeader(t *testing.T) {
	c := newTestCache(t)

	leader := c.Claim(context.Background(), "k", time.Second)
	if leader.Hit {
		t.Fatal("cold key must miss")
	}
	defer leader.Release()

	start := time.Now()
	cl := c.Claim(context.Background(), "k", 20*time.Millisecond)
	elapsed := time.Since(start)

	if cl.Hit {
		t.Fatal("nothing was stored, so nothing can be served")
	}
	if cl.fl != nil {
		t.Error("a follower that timed out must not hold the lease: the leader still has it")
	}
	if elapsed < 20*time.Millisecond {
		t.Errorf("gave up after %s, want at least the 20ms bound", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("waited %s, want the bound to be a bound", elapsed)
	}
}

func TestClaimStopsWaitingWhenTheCallerDoes(t *testing.T) {
	c := newTestCache(t)

	leader := c.Claim(context.Background(), "k", time.Minute)
	defer leader.Release()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Claim, 1)
	go func() { done <- c.Claim(ctx, "k", time.Minute) }()
	waitFor(t, "the follower to park on the lease", func() bool { return c.Waiting() == 1 })
	cancel()

	select {
	case cl := <-done:
		if cl.Hit || cl.fl != nil {
			t.Errorf("a cancelled wait must return an empty claim, got %+v", cl)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled caller is still waiting on the lease")
	}
}

// TestClaimAdmitsOneComputationPerKey is the property the whole mechanism
// exists for, stated as a count: however many identical tasks arrive at once,
// exactly one of them computes.
func TestClaimAdmitsOneComputationPerKey(t *testing.T) {
	c := newTestCache(t)
	const askers = 32

	// Repeated on fresh keys, because the interesting orderings are the narrow
	// ones: an asker whose lookup missed a moment before the holder stored its
	// result and let the lease go. One round would find the common case and
	// call it proof.
	for round := range 25 {
		key := fmt.Sprintf("k%d", round)
		var computed atomic.Int64
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range askers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				cl := c.Claim(context.Background(), key, 10*time.Second)
				defer cl.Release()
				if cl.Hit {
					return
				}
				computed.Add(1)
				// Whatever the "computation" is, every asker must be able to
				// use its result, so it is stored before the lease is dropped.
				_, err := c.Put(key, []core.Record{core.NewRecord(
					fmt.Sprintf("r%d", i), map[string]any{"answer": "42"})})
				if err != nil {
					t.Error(err)
				}
			}()
		}
		close(start)
		wg.Wait()

		if n := computed.Load(); n != 1 {
			t.Fatalf("round %d: %d of %d askers computed, want exactly 1",
				round, n, askers)
		}
	}
}

func TestClaimIsPerKey(t *testing.T) {
	c := newTestCache(t)

	// Different keys are different computations and must not queue behind each
	// other: the lease deduplicates work, it does not serialize the run.
	a := c.Claim(context.Background(), "a", time.Second)
	b := c.Claim(context.Background(), "b", time.Second)
	if a.Hit || b.Hit {
		t.Fatal("cold keys must miss")
	}
	if a.fl == nil || b.fl == nil || a.fl == b.fl {
		t.Fatal("two keys must hold two leases")
	}
	if n := c.InFlight(); n != 2 {
		t.Errorf("InFlight() = %d, want 2", n)
	}
	a.Release()
	b.Release()
	if n := c.InFlight(); n != 0 {
		t.Errorf("InFlight() = %d after both releases, want 0", n)
	}
	if n := c.Waiting(); n != 0 {
		t.Errorf("Waiting() = %d, want 0: nothing ever waited", n)
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	c := newTestCache(t)
	cl := c.Claim(context.Background(), "k", time.Second)
	cl.Release()
	cl.Release() // a deferred release beside an explicit one must not panic
	var zero Claim
	zero.Release() // nor must a claim that never held a lease
	if n := c.InFlight(); n != 0 {
		t.Errorf("InFlight() = %d, want 0", n)
	}
}
