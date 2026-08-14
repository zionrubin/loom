package delta

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/zionrubin/loom/store"
)

// --- renderers the tests need -------------------------------------------

// counted is not append-stable: it puts the segment total in a header, so
// appending changes bytes at the very front and no repair window can reach
// them. The splicer must notice and rebuild — every time, forever.
type counted struct{}

func (counted) Version() string { return "test.counted/1" }
func (counted) Lookahead() int  { return -1 }
func (counted) Render(segs []Segment, from, total int) (string, []Span) {
	var b strings.Builder
	spans := make([]Span, len(segs))
	for i, s := range segs {
		start := b.Len()
		if from+i == 0 {
			fmt.Fprintf(&b, "[%d segments]\n", total)
		}
		b.WriteString(s.Body + "\n")
		spans[i] = Span{Start: start, End: b.Len()}
	}
	return b.String(), spans
}

// sneaky is the renderer this whole package is afraid of: one whose output
// changes far from the seam, in a way the boundary check cannot see.
//
// Past five segments it stamps a banner on the front. A splice re-renders only
// the tail, the overlap agrees because the tail is genuinely unchanged, the
// certificate verifies because its arithmetic is sound — and the result is
// missing the banner. Nothing short of recomputing the whole thing catches it,
// which is exactly what shadow verification does.
type sneaky struct{}

func (sneaky) Version() string { return "test.sneaky/1" }

// Lookahead is the lie. It claims a change reaches nothing behind it, which is
// true of every segment except the one it rewrites from four positions away.
func (sneaky) Lookahead() int { return 0 }
func (sneaky) Render(segs []Segment, from, total int) (string, []Span) {
	var b strings.Builder
	spans := make([]Span, len(segs))
	for i, s := range segs {
		start := b.Len()
		if from+i == 0 && total > 5 {
			b.WriteString("BANNER\n")
		}
		b.WriteString(s.Body + "\n")
		spans[i] = Span{Start: start, End: b.Len()}
	}
	return b.String(), spans
}

// broken violates the Renderer contract: its spans do not describe its output.
type broken struct{}

func (broken) Version() string { return "test.broken/1" }
func (broken) Lookahead() int  { return 0 }
func (broken) Render(segs []Segment, from, total int) (string, []Span) {
	text, spans := Plain{}.Render(segs, from, total)
	if len(spans) > 0 {
		spans[0].End++ // one byte too far: the spans no longer tile
	}
	return text, spans
}

// counting wraps a renderer and records how many segments it was asked to
// render, which is how a test says "this cost O(change)" rather than hoping.
type counting struct {
	inner Renderer

	mu    sync.Mutex
	calls int
	segs  int
}

func (c *counting) Version() string { return c.inner.Version() }
func (c *counting) Lookahead() int  { return c.inner.Lookahead() }
func (c *counting) Render(segs []Segment, from, total int) (string, []Span) {
	c.mu.Lock()
	c.calls++
	c.segs += len(segs)
	c.mu.Unlock()
	return c.inner.Render(segs, from, total)
}

func (c *counting) totals() (calls, segs int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.segs
}

// --- helpers ------------------------------------------------------------

func seg(i int) Segment {
	return Segment{
		Name: fmt.Sprintf("turn%02d", i),
		Body: fmt.Sprintf("turn %d: %s", i, strings.Repeat("x", 40+i%17)),
	}
}

func segs(n int) []Segment {
	out := make([]Segment, n)
	for i := range out {
		out[i] = seg(i)
	}
	return out
}

func ref(n int) Ref {
	return Ref{Key: "session/test", Hash: fmt.Sprintf("rev%03d", n), Segments: n}
}

func renderers() []Renderer { return []Renderer{Tags{}, Plain{}, counted{}} }

// --- the property that matters ------------------------------------------

// TestSpliceEqualsFullRender is the whole contract in one test: however a state
// is arrived at, its bytes are the bytes a full render would have produced.
//
// It runs every renderer through a random walk of appends, middle edits and
// truncations, splicing each revision onto the last, and compares against a
// from-scratch render every time. A renderer that is not append-stable is in
// the list on purpose: it must produce the same answer by rebuilding, which is
// the claim that makes the fast path safe to have at all.
func TestSpliceEqualsFullRender(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, r := range renderers() {
		for _, window := range []int{1, 2, 8} {
			t.Run(fmt.Sprintf("%s/window=%d", r.Version(), window), func(t *testing.T) {
				l := Segments(segs(3)...)
				prev, _, err := Build(r, ref(0), l)
				if err != nil {
					t.Fatalf("build: %v", err)
				}
				for round := 1; round <= 40; round++ {
					switch rng.Intn(10) {
					case 0: // a middle edit: the common prefix shortens
						if l.Len() > 2 {
							cp := append([]Segment(nil), l.Segs...)
							at := rng.Intn(len(cp)-1) + 1
							cp[at].Body += "!"
							l = Segments(cp...)
						}
					case 1: // a truncation: a window policy dropped the head
						if l.Len() > 4 {
							l = Segments(l.Segs[2:]...)
						}
					default: // the common case: append a turn or three
						add := make([]Segment, 1+rng.Intn(3))
						for i := range add {
							add[i] = seg(100*round + i)
						}
						l = l.Append(add...)
					}

					want, _, err := Build(r, ref(round), l)
					if err != nil {
						t.Fatalf("round %d: reference build: %v", round, err)
					}
					got, cert, err := Splice(prev, r, ref(round), l, window, 64)
					if err != nil {
						t.Fatalf("round %d: splice: %v", round, err)
					}
					if got.Text != want.Text {
						t.Fatalf("round %d (%s): spliced text differs from a full render\n got: %q\nwant: %q",
							round, cert.Route, clip(got.Text), clip(want.Text))
					}
					if got.Digest() != want.Digest() {
						t.Fatalf("round %d: identical text, different digest", round)
					}
					if err := cert.Verify(prev, got); err != nil {
						t.Fatalf("round %d: certificate did not verify: %v", round, err)
					}
					if cert.Route == RouteSplice && cert.Retained+cert.Repaired != len(got.Text) {
						t.Fatalf("round %d: certificate accounting does not add up", round)
					}
					prev = got
				}
			})
		}
	}
}

// TestSpliceRetainsTheCommonPrefix is the performance claim, stated as a
// correctness-shaped assertion: an append to an append-stable renderer must
// re-render the change and a bounded window, not the context.
func TestSpliceRetainsTheCommonPrefix(t *testing.T) {
	r := &counting{inner: Tags{}}
	l := Segments(segs(200)...)
	prev, _, err := Build(r, ref(0), l)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := r.totals()
	if base != 1 {
		t.Fatalf("build made %d render calls, want 1", base)
	}

	next := l.Append(seg(999))
	got, cert, err := Splice(prev, r, ref(1), next, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if cert.Route != RouteSplice {
		t.Fatalf("route %s (%s), want a splice", cert.Route, cert.Reason)
	}
	// Tags declares a lookahead of one, so the window is two already-rendered
	// segments: the one that used to be last, which legitimately changed, and
	// the one before it, which did not and is therefore the evidence.
	if cert.Window != 2 || cert.Agreed != 1 || cert.Boundary != 199 {
		t.Fatalf("window %d, %d agreed, boundary %d — want a window of 2 with one segment of evidence",
			cert.Window, cert.Agreed, cert.Boundary)
	}
	if cert.Segments != 201 {
		t.Fatalf("state holds %d segments, want 201", cert.Segments)
	}
	// Three segments rendered out of 201: the window of two, plus the new one.
	if _, rendered := r.totals(); rendered != 203 {
		t.Fatalf("splice rendered %d segments beyond the first build, want 3", rendered-200)
	}
	if cert.Retained == 0 || cert.Retained >= len(got.Text) {
		t.Fatalf("retained %d of %d bytes", cert.Retained, len(got.Text))
	}
	want, _, _ := Build(Tags{}, ref(1), next)
	if got.Text != want.Text {
		t.Fatal("spliced text differs from a full render")
	}
}

// TestUnstableRendererNeverSplices pins the other half of the family argument:
// a renderer whose output depends on segments far ahead is not made to work, it
// is made to rebuild.
func TestUnstableRendererNeverSplices(t *testing.T) {
	r := counted{}
	l := Segments(segs(20)...)
	prev, _, err := Build(r, ref(0), l)
	if err != nil {
		t.Fatal(err)
	}
	next := l.Append(seg(999))
	got, cert, err := Splice(prev, r, ref(1), next, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	if cert.Route != RouteRebuild {
		t.Fatalf("route %s, want a rebuild: this renderer changes its header on every append", cert.Route)
	}
	if !strings.Contains(cert.Reason, "does not bound") {
		t.Fatalf("reason %q does not say why: the renderer declared an unbounded lookahead", cert.Reason)
	}
	want, _, _ := Build(r, ref(1), next)
	if got.Text != want.Text {
		t.Fatal("the rebuild differs from a full render")
	}
}

// TestBrokenRendererIsRefused: a renderer whose spans do not describe its own
// output makes every claim in this package meaningless, so it is raised rather
// than absorbed.
func TestBrokenRendererIsRefused(t *testing.T) {
	if _, _, err := Build(broken{}, ref(0), Segments(segs(3)...)); err == nil {
		t.Fatal("a renderer with spans that do not tile its output was accepted")
	}
}

// --- certificates -------------------------------------------------------

func TestCertificateCatchesTampering(t *testing.T) {
	r := Tags{}
	l := Segments(segs(30)...)
	prev, _, _ := Build(r, ref(0), l)
	next := l.Append(seg(999))
	st, cert, err := Splice(prev, r, ref(1), next, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := cert.Verify(prev, st); err != nil {
		t.Fatalf("an honest splice failed verification: %v", err)
	}

	cases := []struct {
		name string
		bend func(*State, *Certificate)
	}{
		{"repaired bytes changed under the certificate", func(s *State, _ *Certificate) {
			s.Text = strings.Replace(s.Text, "turn 999", "turn XXX", 1)
		}},
		{"a retained region that is not the parent's", func(_ *State, c *Certificate) {
			c.Retained -= 4
			c.Repaired += 4
		}},
		{"a digest from somewhere else", func(_ *State, c *Certificate) {
			c.Digest = hash("not", "this")
		}},
		{"seam evidence that does not match the bytes", func(_ *State, c *Certificate) {
			c.Seam = hash("overlap", "something else")
		}},
		{"a parent it did not splice from", func(_ *State, c *Certificate) {
			c.Parent = hash("other", "parent")
		}},
		{"spans that no longer tile", func(s *State, _ *Certificate) {
			s.Spans[len(s.Spans)-1].End--
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bent := *st
			bent.Spans = append([]Span(nil), st.Spans...)
			bent.Seams = append([]string(nil), st.Seams...)
			bentCert := cert
			tc.bend(&bent, &bentCert)
			if err := bentCert.Verify(prev, &bent); err == nil {
				t.Fatal("verification accepted a state that had been altered")
			}
		})
	}
}

// TestCertificateDoesNotReadTheRetainedRegion pins the limit of what a
// certificate is, which matters more than what it catches.
//
// Verification is O(change) because it does not look at the bytes it did not
// touch: the retained region is accounted for by a seam hash carried forward
// from the parent, not by comparing hundreds of kilobytes. So a state whose
// retained bytes were altered *without* altering the chain still verifies. That
// is not a hole to plug — plugging it would mean reading the whole context on
// every splice, which is the rebuild this exists to avoid. It is the reason the
// ladder has a rung above this one.
func TestCertificateDoesNotReadTheRetainedRegion(t *testing.T) {
	r := Plain{}
	l := Segments(segs(500)...)
	prev, _, _ := Build(r, ref(0), l)
	next := l.Append(seg(999))
	st, cert, err := Splice(prev, r, ref(1), next, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	if cert.Route != RouteSplice || cert.Retained < 1000 {
		t.Fatalf("route %s retaining %d bytes of a %d byte context",
			cert.Route, cert.Retained, len(prev.Text))
	}
	if err := cert.Verify(prev, st); err != nil {
		t.Fatalf("an honest splice failed verification: %v", err)
	}

	bent := *st
	bent.Text = strings.Replace(bent.Text, "turn 0:", "turn X:", 1)
	if len(bent.Text) != len(st.Text) {
		t.Fatal("the test's own tampering changed the length")
	}
	if err := cert.Verify(prev, &bent); err != nil {
		t.Fatalf("verification read the retained region after all: %v", err)
	}
}

// --- the chain ----------------------------------------------------------

func newBlobs(t *testing.T) Blobs {
	t.Helper()
	cas, err := store.NewCAS("")
	if err != nil {
		t.Fatal(err)
	}
	return cas
}

func TestChainResolvesToWhatWasWritten(t *testing.T) {
	b := newBlobs(t)
	c, err := NewChain(b, Tags{}, "session/a")
	if err != nil {
		t.Fatal(err)
	}
	r0, err := c.Root(seg(0), seg(1))
	if err != nil {
		t.Fatal(err)
	}
	r1, _ := c.Append(r0, seg(2))
	r2, _ := c.Append(r1, seg(3), seg(4))

	if r2.Segments != 5 {
		t.Fatalf("revision holds %d segments, want 5", r2.Segments)
	}
	if r2.Parent != r1.Hash {
		t.Fatal("revision does not name its parent")
	}
	got, err := c.Resolve(r2)
	if err != nil {
		t.Fatal(err)
	}
	want := Segments(seg(0), seg(1), seg(2), seg(3), seg(4))
	if got.Len() != want.Len() {
		t.Fatalf("resolved %d segments, want %d", got.Len(), want.Len())
	}
	for i := range want.Segs {
		if got.Segs[i] != want.Segs[i] {
			t.Fatalf("segment %d differs after a round trip", i)
		}
	}

	// Appending nothing is the same revision, so a caller need not special-case
	// a turn that produced nothing.
	if same, _ := c.Append(r2); same.Hash != r2.Hash {
		t.Fatal("appending nothing produced a new revision")
	}
}

func TestChainTraceStopsAtWhatIsHeld(t *testing.T) {
	b := newBlobs(t)
	c, _ := NewChain(b, Tags{}, "session/a")
	r0, _ := c.Root(seg(0))
	r1, _ := c.Append(r0, seg(1))
	r2, _ := c.Append(r1, seg(2))
	r3, _ := c.Append(r2, seg(3))

	base, added, err := c.Trace(r3, func(h string) bool { return h == r1.Hash })
	if err != nil {
		t.Fatal(err)
	}
	if base != r1.Hash {
		t.Fatalf("stopped at %s, want the held revision", short(base))
	}
	if len(added) != 2 || added[0].Name != seg(2).Name || added[1].Name != seg(3).Name {
		t.Fatalf("traced %d segments, want the two added after the held revision", len(added))
	}

	// Nothing held: the walk reaches the root and returns everything, which is
	// the rebuild path arriving by the same road.
	base, added, err = c.Trace(r3, func(string) bool { return false })
	if err != nil || base != "" || len(added) != 4 {
		t.Fatalf("cold trace: base %q, %d segments, err %v", base, len(added), err)
	}
}

func TestChainRefusesAnotherRenderersRevisions(t *testing.T) {
	b := newBlobs(t)
	written, _ := NewChain(b, Tags{}, "session/a")
	r0, _ := written.Root(seg(0))

	read, _ := NewChain(b, Plain{}, "session/a")
	if _, err := read.Revision(r0.Hash); err == nil {
		t.Fatal("a revision written for one renderer was read by another")
	}
}

func TestChainMissingRevisionIsPermanent(t *testing.T) {
	b := newBlobs(t)
	c, _ := NewChain(b, Tags{}, "session/a")
	if _, err := c.Revision("0000000000000000"); err == nil {
		t.Fatal("a revision that is not in storage resolved anyway")
	}
}

func clip(s string) string {
	if len(s) > 120 {
		return s[:120] + "…"
	}
	return s
}

var _ = context.Background
