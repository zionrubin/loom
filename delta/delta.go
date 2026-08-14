// Package delta is Loom's incremental execution layer: a way to process an
// evolving object by what changed rather than by how big it is, and — the part
// that carries the design — to establish that the shortcut was safe before
// taking it.
//
// # The shape of the problem
//
// An agent's context grows by a turn at a time. A research loop's context grows
// by a finding at a time. A session that has been running for an hour carries
// hundreds of kilobytes of transcript and adds two more at each step. Loom
// already refuses to redo two kinds of work on that shape — a result whose
// inputs are unchanged comes from the cache, and a question somebody already
// researched comes from the findings commons — but between them sits a third
// kind nobody was avoiding:
//
//	result cache        same computation?         don't execute it again
//	findings commons    same question?            don't research it again
//	prompt prefix       same head of a prompt?    don't prefill it again
//	delta state         same evolving object?     don't *process* it again
//
// Processing here means everything Loom does to a context between deciding to
// use it and handing it to a provider: materializing it out of storage,
// rendering it into bytes, shipping those bytes to whichever process will make
// the call. Today all three are linear in the whole context and the change is
// 2% of it.
//
// # What this package does about it
//
// A context is modelled as an immutable chain of revisions in
// content-addressed storage:
//
//	Root(A) ──▶ +B ──▶ +C ──▶ +D
//	                             │
//	                             ▼
//	                 Ref{Hash: h(D), Parent: h(C)}
//
// An envelope carries the Ref — a couple of hundred bytes — rather than the
// transcript, which is the same indirection store.Broadcasts uses and for the
// same reason: a value referenced once is shipped once, however many tasks read
// it and however large it is.
//
// A process that has already materialized revision C holds its rendered State.
// Asked for D, it splices: keep C's bytes, render the change and a small repair
// window around the seam, and check that the window came out byte-identical
// where the two overlap. That check is the whole safety argument, and it is
// there because rendering is not compositional — appending a segment can change
// how the segments before it render, and a system that assumed otherwise would
// produce a subtly different context and never notice.
//
// # The guarantee ladder
//
// Nothing here trusts the optimization. Four things stand between a splice and
// a wrong answer, and they are deliberately of different kinds:
//
//	family argument      a Renderer is spliceable only if a segment's bytes
//	                     depend on a bounded number of segments after it. Tags
//	                     is; a renderer that prints a total at the top is not,
//	                     and is not made to be — it simply never splices.
//	per-splice evidence  the repair window is re-rendered and must agree, byte
//	                     for byte, with what the parent state already holds
//	                     across the overlap. Disagreement widens the window;
//	                     widening far enough *is* a full rebuild.
//	runtime sampling     a fraction of accepted splices are recomputed from
//	                     scratch and compared. A mismatch quarantines that
//	                     renderer version for the life of the process and the
//	                     reference result is returned.
//	exact fallback       a state miss, an oversized change, a poisoned renderer
//	                     and an unprovable seam all route to the same full
//	                     render every other path is measured against.
//
// The property that ties them together, and the one to hold on to when reading
// the rest of this package:
//
//	State may make execution faster. It may never be necessary to make
//	execution correct. Every path here fails toward recomputation.
//
// Which is why a killed worker costs latency and nothing else. Its successor
// has no state, takes the rebuild route, and produces the same bytes.
package delta

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Segment is one addressable piece of an evolving context: a named, immutable
// unit that a Renderer turns into bytes.
//
// Segments are the granularity of everything else here — of the chain's
// revisions, of the common prefix a splice reuses, and of the repair window it
// re-renders — so they want to be the natural unit of change in the workload.
// One turn of a conversation, one finding, one tool result. A single segment
// holding the whole transcript is legal and buys nothing.
type Segment struct {
	Name string `json:"name"`
	Body string `json:"body"`
}

// ID is the segment's content identity: two segments with the same ID render
// identically wherever they sit.
func (s Segment) ID() string { return hash("segment", s.Name, s.Body) }

// List is a segment sequence carrying each segment's content identity
// alongside it.
//
// The identities are what a splice compares to find how much of a revision it
// already holds, and hashing every body on every revision would be a cost
// linear in the context — the exact cost this package exists to avoid, hidden
// one level down where nobody would look for it. Append computes identities for
// what was added and carries the rest forward, so extending a list of a
// thousand segments by one hashes one body.
//
// The type exists to make that structural rather than a promise: there is no
// way to hold a segment list here without its identities, and no way to build
// one that recomputes them.
type List struct {
	Segs []Segment `json:"segs,omitempty"`
	IDs  []string  `json:"ids,omitempty"`
}

// Segments builds a list, computing every identity. It is the O(n) constructor,
// used once at a root and on any path that has no parent to carry forward.
func Segments(segs ...Segment) List {
	ids := make([]string, len(segs))
	for i, s := range segs {
		ids[i] = s.ID()
	}
	return List{Segs: segs, IDs: ids}
}

// Append extends the list, hashing only what was added. The receiver is not
// modified: both lists remain valid, which is what lets a state stay immutable
// while its successor is built.
func (l List) Append(segs ...Segment) List {
	if len(segs) == 0 {
		return l
	}
	out := List{
		Segs: make([]Segment, 0, len(l.Segs)+len(segs)),
		IDs:  make([]string, 0, len(l.IDs)+len(segs)),
	}
	out.Segs = append(append(out.Segs, l.Segs...), segs...)
	out.IDs = append(out.IDs, l.IDs...)
	for _, s := range segs {
		out.IDs = append(out.IDs, s.ID())
	}
	return out
}

// Len returns how many segments the list holds.
func (l List) Len() int { return len(l.Segs) }

// Size returns the source size of the list: the bytes of its segments' names
// and bodies.
//
// It is not the rendered size, which depends on the renderer and is not known
// until something renders. The router compares this one, because for deciding
// whether a change is small relative to a context a number that is exact and
// free beats one that is precise and costs a render.
func (l List) Size() int {
	n := 0
	for _, s := range l.Segs {
		n += len(s.Name) + len(s.Body)
	}
	return n
}

// Span is where one segment landed in the rendered bytes.
//
// Spans tile the text completely and without overlap: span i ends exactly where
// span i+1 begins, the first starts at zero and the last ends at the end. A
// segment therefore owns whatever separators, wrappers and punctuation the
// renderer emitted around it, which is what makes "the bytes before segment i"
// a well-defined thing to keep.
type Span struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// Len returns the span's byte length.
func (s Span) Len() int { return s.End - s.Start }

// Ref names one revision of one chain. It is what travels: an envelope carries
// a Ref where it would otherwise carry the transcript.
//
// Hash identifies the revision, Parent the one it extends, and the two counts
// let a router decide what to do before anything is fetched — which is the
// point of putting them here rather than making the decision cost a read.
type Ref struct {
	// Key is the evolving object's identity: "session/abc", "review/17".
	// Everything else in this package is content-addressed and therefore
	// anonymous; the key is what makes two revisions recognizably the same
	// thing at two moments, and it is what worker locality is scored on.
	Key string `json:"key,omitempty"`
	// Hash addresses this revision in shared storage.
	Hash string `json:"hash,omitempty"`
	// Parent addresses the revision this one extends, empty at a root.
	Parent string `json:"parent,omitempty"`
	// Segments is how many segments this revision holds in total, and Bytes
	// how many bytes they render to. Both are the whole revision, not the
	// change — the change is Bytes minus the parent's.
	Segments int `json:"segments,omitempty"`
	Bytes    int `json:"bytes,omitempty"`
}

// Zero reports whether the ref names nothing.
func (r Ref) Zero() bool { return r.Hash == "" }

// Unbound reports a ref that names a continuation but no revision of it.
//
// It is what a process holds when it knows a key exists and has not been told
// which revision to read — a worker compiling a pipeline it will be sent
// envelopes for, rather than one it is planning. Reaching an executor it is a
// configuration error and is refused, because the alternative is a stage
// running with no context at all and answering plausibly.
func (r Ref) Unbound() bool { return r.Key != "" && r.Hash == "" }

// String renders the ref for logs: key@short-hash.
func (r Ref) String() string {
	if r.Zero() {
		return "-"
	}
	h := r.Hash
	if len(h) > 8 {
		h = h[:8]
	}
	if r.Key == "" {
		return h
	}
	return r.Key + "@" + h
}

// Hint is what a request tells a provider about its place in a sequence.
//
// It is strictly an optimization channel and the doc comment on
// model.Request.Continuation says so twice, because the failure mode if anyone
// forgets is severe: a provider that reconstructed a prompt from Parent plus a
// change would be reading state Loom does not guarantee it still holds. The
// prompt on the request is always the whole prompt. This says only what a
// backend that keeps its own state — a local engine with a KV cache, a
// tokenizer service that stores token IDs and source spans — could exploit
// about it, and the interesting number is Stable.
//
// Stable is the count of leading bytes of the continuation text that this
// revision shares, byte for byte, with the parent revision's rendering. It is
// not an estimate and not a guess from a diff: it is the retained region of a
// certified splice, which is the same quantity a prefix cache needs to know
// how much of its work it may keep.
type Hint struct {
	Key    string `json:"key,omitempty"`
	Parent string `json:"parent,omitempty"`
	Hash   string `json:"hash,omitempty"`
	Stable int    `json:"stable,omitempty"`
}

// Zero reports whether the hint says nothing.
func (h Hint) Zero() bool { return h.Key == "" && h.Hash == "" }

// --- rendering ----------------------------------------------------------

// Renderer turns segments into the bytes a model sees.
//
// The window arguments are the whole of what makes incremental rendering
// possible. Render is never asked only for "these segments"; it is asked for
// "these segments, which are positions from..from+len of a list of total" — so
// a renderer that opens a wrapper at the start and closes it at the end can
// still render the middle of a list correctly, and a splice can re-render a
// suffix without the result silently acquiring a second opening tag.
//
// # Lookahead, and why it is declared rather than discovered
//
// Lookahead is the family-level correctness argument, written as a method. It
// answers: how many segments after segment i may change segment i's bytes?
//
//	Plain   0   nothing a segment renders depends on what follows it
//	Tags    1   only the last segment differs, because it carries the closing
//	            wrapper; appending moves that wrapper by one
//	a header numbering the segments, a running total, a table of contents:
//	        -1  a change at the end rewrites bytes at the *front*, and no
//	            window anchored at the seam can ever see it
//
// A renderer returning -1 declares that it cannot be spliced, and it never is —
// every revision is rendered in full. That is not a defect to be worked around.
// It is the one honest answer for a format whose output is not local, and the
// result is identical to the fast path's, which was the only thing ever
// promised.
//
// The declaration is a claim, and the splicer treats it as one. The repair
// window is sized from it, and every splice re-renders that window and checks
// it against what the parent already holds: a renderer that understates its
// lookahead is caught here, the window widens, and the reason is reported. What
// no local check can catch is a renderer that changes bytes *outside* any
// window — one that returns 0 and behaves like -1. That is what the sampled
// full recomputation in Store is for, and it is why a package that could stop
// at "the seam agreed" does not.
//
// # Versions are pinned
//
// Version identifies the renderer exactly, and a stored State carries it. Two
// renderers reporting the same version must produce identical bytes for
// identical input, because a cached state from one will be spliced onto by the
// other without hesitation. Change the output, change the version — the seam
// chain is seeded with it, so a mismatched state cannot be mistaken for a
// usable one rather than merely being wrong.
type Renderer interface {
	Version() string
	// Lookahead reports how many segments after a segment may affect that
	// segment's bytes. Negative means unbounded: this renderer is never
	// spliced.
	Lookahead() int
	// Render renders segs, which occupy positions [from, from+len(segs)) of a
	// list of total segments. The returned spans are relative to the returned
	// text and tile it completely.
	Render(segs []Segment, from, total int) (string, []Span)
}

// Tags is the default renderer: the tagged-fragment format Loom already puts
// context in, so a continuation and an inline context fragment reach a model
// looking the same.
//
//	<context>
//	<name>
//	body
//	</name>
//	…
//	</context>
//
// It is append-stable with a lookahead of one segment. Appending moves the
// closing tag, which changes the bytes of the segment that used to be last and
// nothing before it — so a repair window of one is always enough, and the seam
// check proves it rather than assuming it.
type Tags struct{}

// Version implements Renderer.
func (Tags) Version() string { return "delta.Tags/1" }

// Lookahead implements Renderer: appending moves the closing wrapper off the
// segment that used to be last, and reaches no further than that.
func (Tags) Lookahead() int { return 1 }

// Render implements Renderer.
func (Tags) Render(segs []Segment, from, total int) (string, []Span) {
	var b strings.Builder
	spans := make([]Span, len(segs))
	for i, s := range segs {
		start := b.Len()
		if from+i == 0 {
			b.WriteString("<context>\n")
		}
		name := s.Name
		if name == "" {
			name = "fragment"
		}
		b.WriteString("<" + name + ">\n")
		b.WriteString(s.Body)
		b.WriteString("\n</" + name + ">\n")
		if from+i == total-1 {
			b.WriteString("</context>\n\n")
		}
		spans[i] = Span{Start: start, End: b.Len()}
	}
	return b.String(), spans
}

// Plain is a renderer that joins segment bodies with a blank line and nothing
// else. It is append-stable with a lookahead of zero — no segment's bytes
// depend on any other — which makes it the cheapest thing to splice and the
// right choice when the context is prose rather than structure.
type Plain struct{}

// Version implements Renderer.
func (Plain) Version() string { return "delta.Plain/1" }

// Lookahead implements Renderer: a segment's bytes depend on the separator
// after it, which is decided by whether it is last — so appending changes the
// segment that used to be last.
func (Plain) Lookahead() int { return 1 }

// Render implements Renderer.
func (Plain) Render(segs []Segment, from, total int) (string, []Span) {
	var b strings.Builder
	spans := make([]Span, len(segs))
	for i, s := range segs {
		start := b.Len()
		b.WriteString(s.Body)
		if from+i < total-1 {
			b.WriteString("\n\n")
		}
		spans[i] = Span{Start: start, End: b.Len()}
	}
	return b.String(), spans
}

// --- hashing ------------------------------------------------------------

// hash returns the hex SHA-256 of parts joined by a separator that cannot
// appear in a hex digest, so no two distinct part lists collide by
// concatenation.
func hash(parts ...string) string {
	h := sha256.New()
	for i, p := range parts {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// seed starts a seam chain. Folding the renderer version in at the root is what
// makes a state hash a statement about bytes *and* about who produced them: two
// renderers that disagree can never produce the same chain, so a state cached
// under one is not reusable under the other by accident.
func seed(renderer string) string { return hash("delta/seam/1", renderer) }

// extend advances a seam chain by one segment's rendered bytes.
func extend(prev, segment string) string { return hash(prev, segment) }

// short abbreviates a hash for reports.
func short(h string) string {
	if len(h) > 8 {
		return h[:8]
	}
	return h
}

// errorf builds a package error with a consistent prefix.
func errorf(format string, args ...any) error {
	return fmt.Errorf("delta: "+format, args...)
}
