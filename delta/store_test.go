package delta

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/zionrubin/loom/observe"
)

func newStore(t *testing.T, b Blobs, opts Options) *Store {
	t.Helper()
	s, err := NewStore(b, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// session writes a chain that grows by one turn a round, and returns the refs.
func session(t *testing.T, b Blobs, r Renderer, key string, base, rounds int) []Ref {
	t.Helper()
	c, err := NewChain(b, r, key)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := c.Root(segs(base)...)
	if err != nil {
		t.Fatal(err)
	}
	refs := []Ref{ref}
	for i := 1; i <= rounds; i++ {
		ref, err = c.Append(ref, seg(1000+i))
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	return refs
}

// reference renders a revision the long way: resolve the whole chain, render
// everything. Every test in this file compares against it.
func reference(t *testing.T, b Blobs, r Renderer, ref Ref) string {
	t.Helper()
	c, _ := NewChain(b, r, ref.Key)
	l, err := c.Resolve(ref)
	if err != nil {
		t.Fatal(err)
	}
	st, _, err := Build(r, ref, l)
	if err != nil {
		t.Fatal(err)
	}
	return st.Text
}

// TestStoreSplicesAcrossRoundsAndStaysExact is the ordinary case, asserted
// twice: the store takes the fast path, and the fast path's bytes are the slow
// path's bytes.
func TestStoreSplicesAcrossRoundsAndStaysExact(t *testing.T) {
	b := newBlobs(t)
	refs := session(t, b, Tags{}, "session/a", 60, 10)
	s := newStore(t, b, Options{Policy: Policy{Verify: 1}})

	for i, ref := range refs {
		m, err := s.Materialize(context.Background(), Attribution{}, ref)
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if want := reference(t, b, Tags{}, ref); m.Text != want {
			t.Fatalf("round %d: materialized text differs from a full render", i)
		}
		switch {
		case i == 0 && m.Route != RouteRebuild:
			t.Fatalf("round 0 took route %s; there was nothing to build on", m.Route)
		case i > 0 && m.Route != RouteSplice:
			t.Fatalf("round %d took route %s (%s), want a splice", i, m.Route, m.Cert.Reason)
		}
	}

	st := s.Stats()
	if st.Splices != 10 || st.Rebuilds != 1 {
		t.Fatalf("%d splices and %d rebuilds, want 10 and 1", st.Splices, st.Rebuilds)
	}
	if st.Diverged != 0 {
		t.Fatalf("%d divergences with every splice shadow-verified", st.Diverged)
	}
	if st.Verified != 10 {
		t.Fatalf("%d splices verified, want all 10", st.Verified)
	}
	// The claim, measured: the bytes reused should dwarf the bytes rendered.
	if st.Retained < 4*st.Rendered {
		t.Fatalf("retained %d B against %d B rendered — the fast path is not paying",
			st.Retained, st.Rendered)
	}
}

// TestColdProcessMatchesWarmOne is the killed-worker story with the queue taken
// out: two processes reach the same revision, one having followed every round
// and one having seen nothing, and they must agree byte for byte.
func TestColdProcessMatchesWarmOne(t *testing.T) {
	b := newBlobs(t)
	refs := session(t, b, Tags{}, "session/a", 40, 8)

	warm := newStore(t, b, Options{})
	var last string
	for _, ref := range refs {
		m, err := warm.Materialize(context.Background(), Attribution{}, ref)
		if err != nil {
			t.Fatal(err)
		}
		last = m.Text
	}

	cold := newStore(t, b, Options{})
	m, err := cold.Materialize(context.Background(), Attribution{}, refs[len(refs)-1])
	if err != nil {
		t.Fatal(err)
	}
	if m.Route != RouteRebuild {
		t.Fatalf("a process that has seen nothing took route %s", m.Route)
	}
	if m.Text != last {
		t.Fatal("the cold process produced different bytes from the warm one")
	}
	if !strings.Contains(m.Cert.Reason, "no state") {
		t.Fatalf("reason %q does not explain the rebuild", m.Cert.Reason)
	}
}

// TestForgottenStateRebuilds is the same property from the other direction: a
// process that loses its state mid-session recovers by rebuilding, and the
// round after that is spliceable again.
func TestForgottenStateRebuilds(t *testing.T) {
	b := newBlobs(t)
	refs := session(t, b, Tags{}, "session/a", 30, 4)
	s := newStore(t, b, Options{})

	for _, ref := range refs[:3] {
		if _, err := s.Materialize(context.Background(), Attribution{}, ref); err != nil {
			t.Fatal(err)
		}
	}
	s.Forget()
	if got := s.Resident(); len(got) != 0 {
		t.Fatalf("still holding %v after forgetting everything", got)
	}

	m, err := s.Materialize(context.Background(), Attribution{}, refs[3])
	if err != nil {
		t.Fatal(err)
	}
	if m.Route != RouteRebuild {
		t.Fatalf("route %s after losing every state", m.Route)
	}
	if want := reference(t, b, Tags{}, refs[3]); m.Text != want {
		t.Fatal("the rebuild differs from a full render")
	}
	m, err = s.Materialize(context.Background(), Attribution{}, refs[4])
	if err != nil {
		t.Fatal(err)
	}
	if m.Route != RouteSplice {
		t.Fatalf("route %s: the round after a rebuild should splice onto it", m.Route)
	}
}

// TestStoreCatchesUpOverSeveralRevisions: a process that missed rounds splices
// from the last state it has rather than starting over.
func TestStoreCatchesUpOverSeveralRevisions(t *testing.T) {
	b := newBlobs(t)
	refs := session(t, b, Tags{}, "session/a", 50, 6)
	s := newStore(t, b, Options{})

	if _, err := s.Materialize(context.Background(), Attribution{}, refs[1]); err != nil {
		t.Fatal(err)
	}
	m, err := s.Materialize(context.Background(), Attribution{}, refs[5])
	if err != nil {
		t.Fatal(err)
	}
	if m.Route != RouteSplice {
		t.Fatalf("route %s after missing four rounds, want a splice from what it had", m.Route)
	}
	if m.Cert.Segments != 55 {
		t.Fatalf("state holds %d segments, want 55", m.Cert.Segments)
	}
	if want := reference(t, b, Tags{}, refs[5]); m.Text != want {
		t.Fatal("catching up produced different bytes from a full render")
	}
}

// TestShadowVerificationCatchesALyingRenderer is the rung of the ladder no
// local check can reach.
//
// sneaky rewrites the front of the context once it passes five segments while
// declaring that nothing changes behind a segment. Every local check passes:
// the seam agrees because the tail really is unchanged, and the certificate
// verifies because its arithmetic is sound. Only recomputing the whole thing
// finds it — so that is what must find it, and what must happen next is that
// this renderer never splices again.
func TestShadowVerificationCatchesALyingRenderer(t *testing.T) {
	b := newBlobs(t)
	refs := session(t, b, sneaky{}, "session/liar", 4, 6)

	bus := observe.NewBus()
	var alarms []observe.Event
	var mu sync.Mutex
	bus.On(func(e observe.Event) {
		if e.Type == observe.DeltaDiverged {
			mu.Lock()
			alarms = append(alarms, e)
			mu.Unlock()
		}
	})
	s := newStore(t, b, Options{Renderer: sneaky{}, Bus: bus, Policy: Policy{Verify: 1}})

	for i, ref := range refs {
		m, err := s.Materialize(context.Background(), Attribution{Stage: "chat"}, ref)
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		// Whatever the store decides internally, the caller is handed the bytes
		// a full render would have produced. That is the promise, and it holds
		// on the round the lie is discovered as well as on every round after.
		if want := reference(t, b, sneaky{}, ref); m.Text != want {
			t.Fatalf("round %d: a wrong context reached the caller\n got: %q\nwant: %q",
				i, clip(m.Text), clip(want))
		}
	}

	st := s.Stats()
	if st.Diverged == 0 {
		t.Fatal("shadow verification did not notice a renderer rewriting the front of the context")
	}
	if !s.quarantined() {
		t.Fatal("a renderer caught diverging was left free to splice again")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(alarms) != 1 {
		t.Fatalf("%d divergence events, want exactly one — the alarm should not repeat", len(alarms))
	}
	if !strings.Contains(alarms[0].Err, "will not be spliced again") {
		t.Fatalf("the alarm does not say what was done about it: %q", alarms[0].Err)
	}

	// And from here on it is the slow path, forever, without being asked again.
	more := session(t, b, sneaky{}, "session/liar2", 4, 2)
	for _, ref := range more {
		m, _ := s.Materialize(context.Background(), Attribution{}, ref)
		if m.Route == RouteSplice {
			t.Fatal("a quarantined renderer spliced")
		}
	}
}

// TestRouterSendsLargeChangesToTheFullPath: incremental is not universally
// better, and the router is where that is admitted.
func TestRouterSendsLargeChangesToTheFullPath(t *testing.T) {
	b := newBlobs(t)
	c, _ := NewChain(b, Tags{}, "session/a")
	root, _ := c.Root(segs(20)...)
	big := Segment{Name: "dump", Body: strings.Repeat("y", 40<<10)}
	next, _ := c.Append(root, big)

	s := newStore(t, b, Options{Policy: Policy{MaxDelta: 8 << 10}})
	if _, err := s.Materialize(context.Background(), Attribution{}, root); err != nil {
		t.Fatal(err)
	}
	m, err := s.Materialize(context.Background(), Attribution{}, next)
	if err != nil {
		t.Fatal(err)
	}
	if m.Route != RouteRebuild {
		t.Fatalf("route %s for a 40 KiB append against an 8 KiB ceiling", m.Route)
	}
	if !strings.Contains(m.Cert.Reason, "ceiling") {
		t.Fatalf("reason %q does not name the ceiling", m.Cert.Reason)
	}
	if want := reference(t, b, Tags{}, next); m.Text != want {
		t.Fatal("the rebuild differs from a full render")
	}
}

func TestPolicyRoutes(t *testing.T) {
	p := Policy{MaxDelta: 1000, MaxRatio: 0.5}
	cases := []struct {
		name         string
		base, change int
		resident     bool
		want         Route
		reasonHas    string
	}{
		{"cold process", 10000, 10, false, RouteRebuild, "no state"},
		{"small append", 10000, 100, true, RouteSplice, "against"},
		{"oversized append", 100000, 5000, true, RouteRebuild, "ceiling"},
		{"append dwarfs the context", 100, 80, true, RouteRebuild, "% of the context"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := p.Route(tc.base, tc.change, tc.resident)
			if got != tc.want {
				t.Fatalf("route %s, want %s (%s)", got, tc.want, reason)
			}
			if !strings.Contains(reason, tc.reasonHas) {
				t.Fatalf("reason %q does not mention %q", reason, tc.reasonHas)
			}
		})
	}
}

// TestConcurrentMaterializationsCollapse: a round's tasks all want the same
// context, and it should be rendered once.
func TestConcurrentMaterializationsCollapse(t *testing.T) {
	b := newBlobs(t)
	refs := session(t, b, Tags{}, "session/a", 40, 1)
	r := &counting{inner: Tags{}}
	s := newStore(t, b, Options{Renderer: r})

	if _, err := s.Materialize(context.Background(), Attribution{}, refs[0]); err != nil {
		t.Fatal(err)
	}
	before, _ := r.totals()

	const tasks = 16
	var wg sync.WaitGroup
	texts := make([]string, tasks)
	errs := make([]error, tasks)
	for i := range tasks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m, err := s.Materialize(context.Background(), Attribution{}, refs[1])
			texts[i], errs[i] = m.Text, err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("task %d: %v", i, err)
		}
		if texts[i] != texts[0] {
			t.Fatalf("task %d got different bytes from task 0", i)
		}
	}
	after, _ := r.totals()
	if after-before != 1 {
		t.Fatalf("%d render calls for one revision wanted by %d tasks", after-before, tasks)
	}
	if st := s.Stats(); st.Splices != 1 || st.Hits != tasks-1 {
		t.Fatalf("%d splices and %d hits, want 1 and %d", st.Splices, st.Hits, tasks-1)
	}
}

func TestResidencyIsReportedAndBounded(t *testing.T) {
	b := newBlobs(t)
	a := session(t, b, Tags{}, "session/a", 10, 2)
	c := session(t, b, Tags{}, "session/c", 10, 2)
	s := newStore(t, b, Options{})

	for _, ref := range []Ref{a[2], c[2]} {
		if _, err := s.Materialize(context.Background(), Attribution{}, ref); err != nil {
			t.Fatal(err)
		}
	}
	got := s.Resident()
	if len(got) != 2 || got[0] != "session/a" || got[1] != "session/c" {
		t.Fatalf("resident keys %v, want both sessions sorted", got)
	}
	if !s.Holds(a[2]) {
		t.Fatal("Holds says no for a revision that is resident")
	}

	// A ceiling smaller than one context is not a failure mode: the state is
	// served and not kept.
	tiny := newStore(t, b, Options{MaxBytes: 16})
	m, err := tiny.Materialize(context.Background(), Attribution{}, a[2])
	if err != nil {
		t.Fatal(err)
	}
	if want := reference(t, b, Tags{}, a[2]); m.Text != want {
		t.Fatal("a store with no room produced the wrong bytes")
	}
	if len(tiny.Resident()) != 0 {
		t.Fatal("a context larger than the whole ceiling was admitted")
	}
	if tiny.Stats().Rejected != 1 {
		t.Fatal("the rejection was not counted")
	}
}

func TestEvictionKeepsTheCeiling(t *testing.T) {
	b := newBlobs(t)
	var refs []Ref
	for i := range 6 {
		refs = append(refs, session(t, b, Tags{}, fmt.Sprintf("session/%d", i), 20, 0)[0])
	}
	one, _, _ := Build(Tags{}, refs[0], Segments(segs(20)...))
	s := newStore(t, b, Options{MaxBytes: int64(3 * len(one.Text))})

	for _, ref := range refs {
		if _, err := s.Materialize(context.Background(), Attribution{}, ref); err != nil {
			t.Fatal(err)
		}
	}
	st := s.Stats()
	if st.States > 3 {
		t.Fatalf("holding %d states under a ceiling of three", st.States)
	}
	if st.Evicted == 0 {
		t.Fatal("nothing was evicted despite the ceiling being reached")
	}
	if st.Bytes > 3*int64(len(one.Text)) {
		t.Fatalf("holding %d bytes over the ceiling", st.Bytes)
	}
}

// TestMaterializeIsExactWithoutAChain: a zero ref is the ordinary case of a
// stage that has no continuation, and must cost nothing and mean nothing.
func TestMaterializeWithoutARef(t *testing.T) {
	s := newStore(t, newBlobs(t), Options{})
	m, err := s.Materialize(context.Background(), Attribution{}, Ref{})
	if err != nil {
		t.Fatal(err)
	}
	if !m.Zero() || m.Text != "" || !m.Hint().Zero() {
		t.Fatal("a zero ref materialized something")
	}
}

func TestMaterializeReportsMissingStorage(t *testing.T) {
	s := newStore(t, newBlobs(t), Options{})
	_, err := s.Materialize(context.Background(), Attribution{},
		Ref{Key: "session/a", Hash: "beefbeefbeefbeef"})
	if err == nil {
		t.Fatal("a revision that is not in storage materialized anyway")
	}
	if !strings.Contains(err.Error(), "shared") {
		t.Fatalf("error %q does not say the two sides are not sharing storage", err)
	}
}
