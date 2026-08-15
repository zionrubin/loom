package delta

import (
	"encoding/json"

	"github.com/zionrubin/loom/core"
)

// Blobs is the shared content-addressed storage a chain lives in: the two
// methods store.CAS already has, named as an interface so this package does not
// care whether the bytes are in a directory, in a bucket, or in memory.
//
// It is the same interface worker.Blobs is, for the same reason, and in a
// deployment it is usually the same store — which is the point. A revision
// written by the process that plans a run is readable by every worker that
// might execute one, without either side arranging anything, because a hash is
// a name any process can resolve.
type Blobs interface {
	// Put stores data and returns its content hash.
	Put(data []byte) (string, error)
	// Get returns the blob stored under hash.
	Get(hash string) ([]byte, bool)
}

// Revision is one link of a chain, as stored.
//
// What it holds is the *change*, not the state: the segments this revision
// added and the hash of the revision it added them to. Reconstructing the whole
// context means walking to the root, which is exactly the work a resident state
// avoids and exactly the work a cold process must do. That asymmetry is the
// design — writing is cheap and constant, reading is cheap when you have been
// following along and linear when you have not — and it is why losing a worker
// costs latency rather than an answer.
//
// The renderer is stored because a state is only ever spliced onto by the
// version that produced it. A chain written for one renderer and materialized
// by another is a configuration error, and it is refused on the first read
// rather than discovered as a subtly different prompt.
type Revision struct {
	Key      string    `json:"key"`
	Parent   string    `json:"parent,omitempty"`
	Renderer string    `json:"renderer"`
	Added    []Segment `json:"added,omitempty"`
	// Segments and Bytes describe the whole revision, not the change: how many
	// segments it holds in total and their source size. They are what a router
	// consults to decide how to materialize before it has materialized
	// anything, which is the only place that decision can be made cheaply.
	Segments int `json:"segments"`
	Bytes    int `json:"bytes"`
}

// Chain writes an evolving context into shared storage, one revision at a time.
//
// It is deliberately a writer's tool and not a session manager. Loom does not
// know what a session is, when one starts, or when it ends — the application
// does — so this type does one thing: it turns "here is what changed" into an
// immutable, content-addressed, resolvable Ref that an envelope can carry. What
// that Ref means is the caller's business.
//
// A Chain is safe for concurrent use, and two processes may extend the same
// logical key at once: the result is two refs with a common ancestor rather
// than a conflict, because nothing here is mutable and no revision is anybody's
// idea of "current". Which ref a run uses is the run's choice, made where the
// run is planned.
type Chain struct {
	blobs Blobs
	r     Renderer
	key   string
}

// NewChain returns a chain for key over blobs, writing revisions rendered by r.
func NewChain(b Blobs, r Renderer, key string) (*Chain, error) {
	switch {
	case b == nil:
		return nil, errorf("a chain needs shared storage")
	case r == nil:
		return nil, errorf("a chain needs a renderer")
	case key == "":
		return nil, errorf("a chain needs a key")
	}
	return &Chain{blobs: b, r: r, key: key}, nil
}

// Key returns the evolving object this chain describes.
func (c *Chain) Key() string { return c.key }

// Root starts a chain, returning the ref of its first revision.
func (c *Chain) Root(segs ...Segment) (Ref, error) { return c.write(Ref{}, segs) }

// Append extends parent by segs, returning the ref of the new revision.
//
// Appending nothing is not an error and not a no-op worth optimizing away: it
// returns the parent unchanged, so a caller looping over turns need not special
// case a turn that produced nothing.
func (c *Chain) Append(parent Ref, segs ...Segment) (Ref, error) {
	if len(segs) == 0 {
		return parent, nil
	}
	if parent.Zero() {
		return c.write(Ref{}, segs)
	}
	if parent.Key != "" && parent.Key != c.key {
		return Ref{}, core.Permanent(errorf("chain %q cannot extend a revision of %q", c.key, parent.Key))
	}
	return c.write(parent, segs)
}

// write stores one revision.
func (c *Chain) write(parent Ref, segs []Segment) (Ref, error) {
	rev := Revision{Key: c.key, Renderer: c.r.Version(), Added: segs}
	if !parent.Zero() {
		// The parent is read rather than trusted, which costs one small blob and
		// buys two things: the counts on the new revision cannot drift from what
		// the chain actually holds, and a parent hash naming nothing fails here,
		// where the writer can see it, rather than on a worker three stages
		// later.
		prev, err := c.Revision(parent.Hash)
		if err != nil {
			return Ref{}, err
		}
		rev.Parent = parent.Hash
		rev.Segments, rev.Bytes = prev.Segments, prev.Bytes
	}
	rev.Segments += len(segs)
	for _, s := range segs {
		rev.Bytes += len(s.Name) + len(s.Body)
	}

	blob, err := json.Marshal(rev)
	if err != nil {
		return Ref{}, core.Permanent(errorf("revision must be JSON-serializable: %w", err))
	}
	hash, err := c.blobs.Put(blob)
	if err != nil {
		return Ref{}, errorf("storing revision: %w", err)
	}
	return Ref{
		Key: c.key, Hash: hash, Parent: rev.Parent,
		Segments: rev.Segments, Bytes: rev.Bytes,
	}, nil
}

// Revision reads one revision by hash.
func (c *Chain) Revision(hash string) (Revision, error) { return read(c.blobs, c.r.Version(), hash) }

// Resolve walks a ref back to its root and returns every segment, in order.
//
// This is the reference path for reading, and it is O(the whole context) on
// purpose: it is what a process with no state does, what a killed worker's
// replacement does, and what the sampled verifier does to check that a splice
// told the truth. Everything faster in this package is measured against it.
func (c *Chain) Resolve(ref Ref) (List, error) {
	_, segs, err := c.Trace(ref, func(string) bool { return false })
	if err != nil {
		return List{}, err
	}
	return Segments(segs...), nil
}

// Trace walks from ref toward the root, stopping at the first revision held
// reports is already available, and returns the segments added after it.
//
// It is how a process asks "what has changed since something I have?" without
// knowing in advance how far back that is. A worker one revision behind reads
// one blob; a worker that missed five rounds reads five; a worker that has
// never seen this key walks to the root and gets everything, which is the
// rebuild path arriving by the same road as the fast one.
//
// The walk cannot cycle: a revision's hash covers its parent's hash, so a cycle
// would be a hash collision rather than a bug.
func (c *Chain) Trace(ref Ref, held func(hash string) bool) (base string, added []Segment, err error) {
	if ref.Zero() {
		return "", nil, nil
	}
	ver := c.r.Version()
	// Revisions are walked newest-first and their segments prepended, so the
	// result is in chain order. Collecting the slices and reversing at the end
	// keeps that one allocation rather than one per revision.
	var chunks [][]Segment
	total := 0
	for hash := ref.Hash; hash != ""; {
		rev, err := read(c.blobs, ver, hash)
		if err != nil {
			return "", nil, err
		}
		chunks = append(chunks, rev.Added)
		total += len(rev.Added)
		if rev.Parent == "" {
			break
		}
		if held != nil && held(rev.Parent) {
			base = rev.Parent
			break
		}
		hash = rev.Parent
	}
	added = make([]Segment, 0, total)
	for i := len(chunks) - 1; i >= 0; i-- {
		added = append(added, chunks[i]...)
	}
	return base, added, nil
}

// read fetches and validates one revision.
func read(b Blobs, renderer, hash string) (Revision, error) {
	blob, ok := b.Get(hash)
	if !ok {
		// A revision that is not there means the processes are not sharing
		// storage, which no retry fixes and which must not be mistaken for an
		// empty context — a task quietly running with no context would be a
		// correctness bug wearing a plausible result.
		return Revision{}, core.Permanent(errorf(
			"revision %s not found in shared storage (the process that wrote the "+
				"chain and the one reading it must share a content-addressed store)", short(hash)))
	}
	var rev Revision
	if err := json.Unmarshal(blob, &rev); err != nil {
		return Revision{}, core.Permanent(errorf("revision %s: %w", short(hash), err))
	}
	if rev.Renderer != renderer {
		return Revision{}, core.Permanent(errorf(
			"revision %s was written for renderer %q, this process renders with %q",
			short(hash), rev.Renderer, renderer))
	}
	return rev, nil
}
