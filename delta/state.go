package delta

import (
	"fmt"
	"strings"
)

// State is a rendered context together with the evidence needed to extend it
// without re-reading it.
//
// Three of the five fields exist only for that second purpose, and they are
// what separates this from a cache of strings:
//
//	List    the segments themselves and their identities, so finding what two
//	        revisions share is a scan of hashes, and re-rendering a window is
//	        possible without going back to storage for the source
//	Spans   where each segment landed, so "the bytes before segment i" is a
//	        range rather than a search
//	Seams   a chained hash where Seams[i] covers Text[:Spans[i].Start], so
//	        "the first i segments rendered to exactly these bytes" is one
//	        string comparison instead of a memcmp over the whole prefix
//
// Seams is the load-bearing one. Without it a certificate asserting that a
// splice retained 400 KB unchanged could only be checked by comparing 400 KB,
// which would cost more than the rebuild it was meant to avoid. With it the
// check is O(1) at the boundary and O(change) over the repair, which is the
// only reason certifying every splice is affordable enough to do every time.
//
// Holding the source alongside the rendering is what costs: a resident state is
// roughly twice its context in memory, the bytes a model will see and the
// segments they came from. That is the price of extending it without a trip to
// storage, it is bounded by Options.MaxBytes, and it is why residency is a
// cache rather than a ledger — a state that has fallen out is a rebuild, not a
// failure.
//
// A State is immutable once built. Splice reads one and produces another,
// sharing nothing that either could mutate.
type State struct {
	// Ref is the revision this state renders.
	Ref Ref
	// Renderer is the pinned version that produced Text. A state is only ever
	// spliced onto by the same version.
	Renderer string
	// Text is the rendered context.
	Text string
	// List is the segments Text was rendered from, with their identities.
	List List
	// Spans tile Text, one per segment.
	Spans []Span
	// Seams is the chained hash, one entry longer than the list: Seams[i]
	// covers Text[:Spans[i].Start] and the last covers all of Text.
	Seams []string
}

// Segments returns how many segments this state holds.
func (s *State) Segments() int { return s.List.Len() }

// Bytes returns the rendered size.
func (s *State) Bytes() int { return len(s.Text) }

// Digest is the seam-chain hash of the whole rendered text: the state's
// identity as bytes, as opposed to Ref.Hash, which is its identity as a
// revision. Two states that agree here rendered identically; two states that
// agree on Ref.Hash and disagree here are a renderer that broke its version
// pin, which is what shadow verification exists to catch.
func (s *State) Digest() string {
	if len(s.Seams) == 0 {
		return ""
	}
	return s.Seams[len(s.Seams)-1]
}

// Route is how a materialization was served.
type Route string

const (
	// RouteHit is the revision already materialized in this process: no
	// rendering at all.
	RouteHit Route = "hit"
	// RouteSplice reused a parent state's bytes and re-rendered a repair
	// window, under a certificate.
	RouteSplice Route = "splice"
	// RouteRebuild rendered every segment. It is the reference path, and every
	// other path is defined as producing the same bytes it would have.
	RouteRebuild Route = "rebuild"
)

// Certificate is the per-splice record of what was assumed, what was checked,
// and what it cost.
//
// It is deliberately not a proof of correctness, and the distinction is the
// whole reason the shadow verifier exists. What a certificate establishes,
// checkably and in time proportional to the change rather than to the context,
// is this:
//
//	the resulting bytes are the parent's first Retained bytes followed by
//	Repaired bytes freshly rendered — and the Agreed segments immediately
//	before that boundary, which this call re-rendered from scratch under the
//	new list, came out byte-identical to what the parent already held.
//
// That second clause is the evidence, and it is worth being precise about what
// it does and does not support. It shows the change did not reach back as far
// as the boundary. It says nothing about segments further back than the window,
// and no check proportional to the change could: reaching them means reading
// them, and reading them is the rebuild this is trying to avoid.
//
// So the assumption that remains is the renderer's declared Lookahead — that
// bytes change only near the end. The window enforces the declared bound, and
// Store's sampled full recomputation is what tests the declaration itself. A
// divergence quarantines the renderer and serves the reference result.
type Certificate struct {
	Route    Route  `json:"route"`
	Key      string `json:"key,omitempty"`
	Hash     string `json:"hash,omitempty"`
	Parent   string `json:"parent,omitempty"`
	Renderer string `json:"renderer"`
	// Boundary is the segment index where reuse ended and repair began. Zero
	// on a rebuild, which retains nothing.
	Boundary int `json:"boundary"`
	// Retained is bytes kept from the parent; Repaired is bytes re-rendered.
	// They sum to the size of the result.
	Retained int `json:"retained"`
	Repaired int `json:"repaired"`
	// Window is how many already-rendered segments were re-rendered to find the
	// boundary, and Widenings how many times it had to grow before any of them
	// agreed. Widenings routinely above zero means the renderer understates its
	// Lookahead, and every splice is paying to rediscover the real one.
	Window    int `json:"window"`
	Widenings int `json:"widenings"`
	// Agreed is how many of those re-rendered segments came out byte-identical
	// to what the parent already held — the evidence that the change did not
	// reach back past the boundary. It is at least one on any splice: a window
	// where nothing agreed is a window that proved nothing, and widens instead.
	//
	// Overlap is those segments' bytes, OverlapAt where they sit in the result,
	// and Seam their hash, kept so a report can show the evidence and a
	// verifier can re-derive it without re-rendering anything.
	Agreed    int    `json:"agreed"`
	Overlap   int    `json:"overlap"`
	OverlapAt int    `json:"overlap_at"`
	Seam      string `json:"seam,omitempty"`
	// Digest is the seam-chain hash of the resulting text, and Segments its
	// segment count.
	Digest   string `json:"digest"`
	Segments int    `json:"segments"`
	// Reason says why this route was taken, in words, for the event stream. A
	// rebuild that says "no parent state" is a cold worker; one that says
	// "change is 62% of the context" is the router working as intended; one
	// that says "seam unstable past 64 segments" is a renderer that is not
	// append-stable, and the number to act on.
	Reason string `json:"reason,omitempty"`
}

// Saved reports the bytes this route did not have to render.
func (c Certificate) Saved() int { return c.Retained }

// String renders the certificate for logs.
func (c Certificate) String() string {
	switch c.Route {
	case RouteHit:
		return "hit " + short(c.Hash)
	case RouteSplice:
		return fmt.Sprintf("splice %s: kept %d B, repaired %d B over %d segment(s)",
			short(c.Hash), c.Retained, c.Repaired, c.Window)
	default:
		return fmt.Sprintf("rebuild %s: %d B (%s)", short(c.Hash), c.Repaired, c.Reason)
	}
}

// Verify re-derives the certificate's arithmetic against the states it names.
//
// It costs O(change), never O(context): the retained region is checked by one
// seam-hash comparison rather than by reading it, and everything else is a walk
// over the bytes that were re-rendered. That is what makes verifying *every*
// accepted splice affordable, which in turn is what makes the sampled
// full-recomputation check a backstop rather than the only line of defence.
//
// It does not, and cannot cheaply, establish that a full render would have
// produced these bytes. See the type's doc comment for what it does establish.
func (c Certificate) Verify(parent, out *State) error {
	if out == nil {
		return errorf("certificate %s: no state to verify", short(c.Hash))
	}
	if out.Renderer != c.Renderer {
		return errorf("certificate %s: renderer %q, state rendered by %q",
			short(c.Hash), c.Renderer, out.Renderer)
	}
	if n := out.List.Len(); len(out.Spans) != n || len(out.Seams) != n+1 || len(out.List.IDs) != n {
		return errorf("certificate %s: state is malformed (%d segments, %d ids, %d spans, %d seams)",
			short(c.Hash), n, len(out.List.IDs), len(out.Spans), len(out.Seams))
	}
	if c.Retained+c.Repaired != len(out.Text) {
		return errorf("certificate %s: retained %d + repaired %d ≠ %d bytes rendered",
			short(c.Hash), c.Retained, c.Repaired, len(out.Text))
	}
	if c.Boundary < 0 || c.Boundary > out.List.Len() {
		return errorf("certificate %s: boundary %d outside %d segments",
			short(c.Hash), c.Boundary, out.List.Len())
	}

	switch c.Route {
	case RouteRebuild:
		if c.Retained != 0 || c.Boundary != 0 {
			return errorf("certificate %s: a rebuild retains nothing, claims %d bytes at segment %d",
				short(c.Hash), c.Retained, c.Boundary)
		}
	case RouteSplice:
		if parent == nil {
			return errorf("certificate %s: splice from %s, but the parent state is gone",
				short(c.Hash), short(c.Parent))
		}
		if parent.Ref.Hash != c.Parent {
			return errorf("certificate %s: splice claims parent %s, was given %s",
				short(c.Hash), short(c.Parent), short(parent.Ref.Hash))
		}
		if parent.Renderer != c.Renderer {
			return errorf("certificate %s: parent rendered by %q, not %q",
				short(c.Hash), parent.Renderer, c.Renderer)
		}
		if c.Boundary == 0 {
			return errorf("certificate %s: a splice retaining no segment is a rebuild", short(c.Hash))
		}
		if c.Boundary > len(parent.Spans) {
			return errorf("certificate %s: boundary %d outside the parent's %d segments",
				short(c.Hash), c.Boundary, len(parent.Spans))
		}
		if at := boundary(parent.Spans, c.Boundary, len(parent.Text)); at != c.Retained {
			return errorf("certificate %s: retained %d bytes, but segment %d begins at %d in the parent",
				short(c.Hash), c.Retained, c.Boundary, at)
		}
		if c.Agreed < 1 {
			return errorf("certificate %s: a splice with no segment of evidence proves nothing",
				short(c.Hash))
		}
		// The O(1) statement that the retained bytes are the parent's: the seam
		// chain is a hash over exactly them, carried forward rather than
		// recomputed, so agreement here is agreement over the whole prefix.
		if out.Seams[c.Boundary] != parent.Seams[c.Boundary] {
			return errorf("certificate %s: the retained %d bytes do not match the parent's",
				short(c.Hash), c.Retained)
		}
		// The evidence sits inside the retained region, immediately before the
		// boundary, and re-hashing it costs the window rather than the context.
		end := c.OverlapAt + c.Overlap
		if c.OverlapAt < 0 || end > len(out.Text) || end != c.Retained {
			return errorf("certificate %s: evidence of %d bytes at %d does not end at the boundary %d",
				short(c.Hash), c.Overlap, c.OverlapAt, c.Retained)
		}
		if got := hash("overlap", out.Text[c.OverlapAt:end]); got != c.Seam {
			return errorf("certificate %s: the seam evidence does not match the bytes",
				short(c.Hash))
		}
	default:
		return errorf("certificate %s: unknown route %q", short(c.Hash), c.Route)
	}

	// Walk the repaired region and confirm the seam chain the state carries
	// really follows from the retained prefix plus these bytes. This is what
	// catches spans that do not tile, a digest copied from somewhere else, and
	// a state assembled by anything other than the splicer.
	at := c.Boundary
	if at < len(out.Spans) && out.Spans[at].Start != c.Retained {
		return errorf("certificate %s: repair begins at %d, segment %d at %d",
			short(c.Hash), c.Retained, at, out.Spans[at].Start)
	}
	chain := out.Seams[at]
	for i := at; i < len(out.Spans); i++ {
		s := out.Spans[i]
		if s.Start < 0 || s.End > len(out.Text) || s.End < s.Start {
			return errorf("certificate %s: segment %d has span [%d,%d) over %d bytes",
				short(c.Hash), i, s.Start, s.End, len(out.Text))
		}
		if i > at && s.Start != out.Spans[i-1].End {
			return errorf("certificate %s: segments %d and %d do not meet (%d ≠ %d)",
				short(c.Hash), i-1, i, out.Spans[i-1].End, s.Start)
		}
		chain = extend(chain, out.Text[s.Start:s.End])
		if chain != out.Seams[i+1] {
			return errorf("certificate %s: the seam chain breaks at segment %d",
				short(c.Hash), i)
		}
	}
	if n := len(out.Spans); n > 0 && out.Spans[n-1].End != len(out.Text) {
		return errorf("certificate %s: the last segment ends at %d, text is %d bytes",
			short(c.Hash), out.Spans[n-1].End, len(out.Text))
	}
	if c.Digest != out.Digest() {
		return errorf("certificate %s: digest %s, state digests to %s",
			short(c.Hash), short(c.Digest), short(out.Digest()))
	}
	if c.Segments != out.List.Len() {
		return errorf("certificate %s: claims %d segments, state holds %d",
			short(c.Hash), c.Segments, out.List.Len())
	}
	return nil
}

// --- building and splicing ----------------------------------------------

// Build renders a list from scratch. It is the reference path: every other
// route in this package is defined as producing the bytes Build would have.
func Build(r Renderer, ref Ref, l List) (*State, Certificate, error) {
	return build(r, ref, l, "full render")
}

func build(r Renderer, ref Ref, l List, reason string) (*State, Certificate, error) {
	ver := r.Version()
	text, spans := r.Render(l.Segs, 0, l.Len())
	if err := tiles(ver, text, spans, l.Len()); err != nil {
		return nil, Certificate{}, err
	}
	st := &State{Ref: ref, Renderer: ver, Text: text, List: l, Spans: spans}
	st.Seams = make([]string, l.Len()+1)
	st.Seams[0] = seed(ver)
	for i, s := range spans {
		st.Seams[i+1] = extend(st.Seams[i], text[s.Start:s.End])
	}
	return st, Certificate{
		Route: RouteRebuild, Key: ref.Key, Hash: ref.Hash, Renderer: ver,
		Repaired: len(text), Digest: st.Digest(), Segments: l.Len(),
		Reason: reason,
	}, nil
}

// Splice extends prev into the state for segs, reusing every byte it can
// certify and re-rendering the rest.
//
// It never fails to produce a result, and that is the design rather than a
// convenience. A parent that is missing, a renderer that does not match, a
// common prefix of nothing, a seam that will not hold at any window this policy
// allows — every one of them arrives at the same place, which is a full render
// of every segment. Widening the repair window far enough *is* that full
// render: there is one code path here and two regimes of it, so a fast path
// that fails costs work and cannot cost correctness.
//
// The error return is reserved for a Renderer that broke its own contract by
// returning spans that do not tile its output. That is a programming error in
// the renderer, it makes every span-based claim in this package meaningless,
// and there is nothing to fall back to — so it is raised rather than absorbed.
func Splice(prev *State, r Renderer, ref Ref, l List, window, maxWindow int) (*State, Certificate, error) {
	ver := r.Version()
	segs := l.Segs

	switch {
	case prev == nil:
		return build(r, ref, l, "no parent state")
	case prev.Renderer != ver:
		return build(r, ref, l, "parent rendered by "+prev.Renderer)
	case l.Len() == 0:
		return build(r, ref, l, "empty context")
	case r.Lookahead() < 0:
		return build(r, ref, l, "renderer "+ver+" does not bound how far a change reaches back")
	}
	k := common(prev.List.IDs, l.IDs)
	if k == 0 {
		return build(r, ref, l, "nothing in common with the parent")
	}

	// The window must exceed the renderer's lookahead: the segments within the
	// lookahead are expected to differ, and what makes a boundary *stable* is
	// finding at least one beyond them that does not.
	if window < r.Lookahead()+1 {
		window = r.Lookahead() + 1
	}
	if window < 1 {
		window = 1
	}
	if maxWindow < window {
		maxWindow = window
	}

	widenings := 0
	for w := window; ; w *= 2 {
		s := k - w
		if s <= 0 || w > maxWindow {
			// The window has reached the front of the context, or past what the
			// policy will pay for. Either way what is left is the reference
			// path, which this arrives at by rendering everything — the same
			// thing the loop was doing, one step further.
			reason := fmt.Sprintf("no stable boundary within %d segments", maxWindow)
			if s <= 0 {
				reason = "the repair window reached the start of the context"
			}
			st, cert, err := build(r, ref, l, reason)
			if err != nil {
				return nil, Certificate{}, err
			}
			cert.Widenings = widenings
			return st, cert, nil
		}

		text, spans := r.Render(segs[s:], s, len(segs))
		if err := tiles(ver, text, spans, len(segs)-s); err != nil {
			return nil, Certificate{}, err
		}

		// Segments [s,k) exist in both the parent's rendering and this fresh
		// one. Walk them in order and find how far the two agree. The first
		// disagreement is the change's reach: everything before it rendered the
		// same with and without what was appended, which is the evidence that
		// keeping the parent's bytes up to there is safe.
		j := s
		for i := s; i < k; i++ {
			ps, fs := prev.Spans[i], spans[i-s]
			if ps.Len() != fs.Len() || prev.Text[ps.Start:ps.End] != text[fs.Start:fs.End] {
				break
			}
			j++
		}
		if j == s {
			// Nothing agreed, so nothing was shown. Widening is the honest
			// response: either the renderer's lookahead is larger than declared,
			// or this is not a renderer that can be spliced at all, and the two
			// are told apart by whether a wider window finds a boundary.
			widenings++
			continue
		}

		cut := boundary(prev.Spans, j, len(prev.Text))
		off := boundary(spans, j-s, len(text))

		var b strings.Builder
		b.Grow(cut + len(text) - off)
		b.WriteString(prev.Text[:cut])
		b.WriteString(text[off:])

		st := &State{
			Ref: ref, Renderer: ver, Text: b.String(), List: l,
			Spans: make([]Span, len(segs)),
			Seams: make([]string, len(segs)+1),
		}
		copy(st.Spans, prev.Spans[:j])
		copy(st.Seams, prev.Seams[:j+1])
		for i := j; i < len(segs); i++ {
			sp := spans[i-s]
			st.Spans[i] = Span{Start: cut + sp.Start - off, End: cut + sp.End - off}
			st.Seams[i+1] = extend(st.Seams[i], st.Text[st.Spans[i].Start:st.Spans[i].End])
		}

		evidenceAt := prev.Spans[s].Start
		return st, Certificate{
			Route: RouteSplice, Key: ref.Key, Hash: ref.Hash, Parent: prev.Ref.Hash,
			Renderer: ver, Boundary: j, Retained: cut, Repaired: len(text) - off,
			Window: k - s, Widenings: widenings, Agreed: j - s,
			Overlap: cut - evidenceAt, OverlapAt: evidenceAt,
			Seam:   hash("overlap", st.Text[evidenceAt:cut]),
			Digest: st.Digest(), Segments: len(segs),
			Reason: fmt.Sprintf("kept %d of %d segments, %d agreed at the boundary",
				j, len(segs), j-s),
		}, nil
	}
}

// boundary is where segment i begins, with the end of the text standing in for
// the segment one past the last — so "everything up to segment n" is a range
// like any other rather than a special case at every call site.
func boundary(spans []Span, i, end int) int {
	if i >= len(spans) {
		return end
	}
	return spans[i].Start
}

// common returns the length of the two identity lists' shared prefix.
//
// A prefix rather than a diff, on purpose. A general diff would find more to
// reuse when a segment changes in the middle, and would make every certificate
// a claim about several disjoint ranges instead of one — more machinery, more
// to verify, and aimed at a case that barely happens: contexts grow at the end.
// A middle edit here simply shortens the prefix and re-renders the rest, which
// is correct and no worse than what happens today.
func common(a, b []string) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// tiles checks that a renderer's spans really describe its output.
func tiles(renderer, text string, spans []Span, want int) error {
	if len(spans) != want {
		return errorf("renderer %s returned %d spans for %d segments", renderer, len(spans), want)
	}
	at := 0
	for i, s := range spans {
		if s.Start != at || s.End < s.Start || s.End > len(text) {
			return errorf("renderer %s: span %d is [%d,%d), expected to start at %d within %d bytes",
				renderer, i, s.Start, s.End, at, len(text))
		}
		at = s.End
	}
	if at != len(text) {
		return errorf("renderer %s: spans cover %d of %d bytes", renderer, at, len(text))
	}
	return nil
}
