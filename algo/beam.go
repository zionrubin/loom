package algo

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/zionrubin/loom/core"
)

// Beam is frontier search that keeps the best Width candidates per round.
//
// Each round, every surviving candidate proposes successors; all proposals
// across the whole round are ranked together, the best Width survive, and the
// rest are discarded. It is beam search — the decoding strategy, applied one
// level up, where the thing being expanded is a line of reasoning rather than
// a token and the thing scoring it is a model rather than a logit.
//
// It is the third shape this package's algorithms take, and the one that
// justifies the interface's asymmetry between vertices and messages. BSP
// diffuses information over a graph that exists; Refine loops one vertex on
// itself; Beam *grows* the graph, addressing vertices that do not exist until
// the search proposes them. Set pipeline.IterateSpec.Grow to let it, or the
// stage will drop every proposal and report it — an open-world computation
// inside a closed-world stage is a configuration error worth surfacing rather
// than a search that silently finds nothing.
//
// Two properties come from the engine rather than from here, and both matter
// more for search than for the other shapes. A candidate reached twice by
// different paths has the same content-derived ID, so the second path is a
// cache hit rather than a second bill. And pruning is where the money is: a
// beam of 4 over 5 rounds with 3 proposals each is 20 calls, while the tree it
// searches has 243 leaves.
type Beam struct {
	cfg BeamConfig
}

// BeamConfig configures beam search.
type BeamConfig struct {
	// Width is how many candidates survive each round. Required, and the
	// single number that sets the stage's cost: the frontier never exceeds it,
	// so the whole search costs at most Width calls per round.
	Width int

	// Expand returns the successors a candidate proposes, each addressed to
	// its own new vertex ID and ranked. Required.
	//
	// Use ExpandFromField when the step writes its proposals into a record
	// field, which is the usual case — the model that just evaluated a
	// candidate is the thing best placed to say what to try next.
	Expand func(core.Record) []Message

	// Terminal reports that a candidate is a finished answer and should not
	// be expanded further. Nil means nothing is terminal and the search runs
	// until the stage's round cap or budget stops it.
	Terminal func(core.Record) bool
}

// NewBeam builds a beam search.
func NewBeam(cfg BeamConfig) (*Beam, error) {
	if cfg.Width <= 0 {
		return nil, fmt.Errorf("algo: Beam needs a positive Width")
	}
	if cfg.Expand == nil {
		return nil, fmt.Errorf("algo: Beam needs Expand (try algo.ExpandFromField(\"next\", nil))")
	}
	return &Beam{cfg: cfg}, nil
}

// Name implements Algorithm.
func (b *Beam) Name() string { return "beam" }

// Seed implements Algorithm: every starting record is a root of the search.
// More than one root is legitimate — Width is a budget on the whole frontier,
// so several roots compete for it from the first round rather than each
// getting their own beam.
func (b *Beam) Seed(g Graph) ([]Message, error) {
	out := make([]Message, 0, g.Len())
	for _, id := range g.IDs() {
		out = append(out, Message{To: id})
	}
	return out, nil
}

// Route implements Algorithm: collect the round's proposals, keep the best
// Width, discard the rest.
func (b *Beam) Route(r Round) ([]Message, error) {
	var proposals []Message
	for _, st := range r.Steps {
		if b.cfg.Terminal != nil && b.cfg.Terminal(st.Vertex) {
			continue // a finished answer stops expanding but is kept
		}
		for _, m := range b.cfg.Expand(st.Vertex) {
			if m.To == "" {
				return nil, fmt.Errorf(
					"algo: beam successor from %q has no destination "+
						"(build proposals with algo.Successor)", st.ID())
			}
			if m.From == "" {
				m.From = st.ID()
			}
			proposals = append(proposals, m)
		}
	}

	// Prune by candidate, not by message. Two parents proposing the same
	// successor is a merge in the search tree, not two candidates, and
	// charging it two beam slots would narrow the search exactly where it
	// found agreement.
	best := map[string]Message{}
	for _, m := range proposals {
		if cur, ok := best[m.To]; !ok || m.Rank > cur.Rank {
			best[m.To] = m
		}
	}
	unique := make([]Message, 0, len(best))
	for _, m := range best {
		unique = append(unique, m)
	}
	slices.SortStableFunc(unique, func(a, c Message) int {
		if d := cmp.Compare(c.Rank, a.Rank); d != 0 {
			return d
		}
		return cmp.Compare(a.To, c.To) // ties broken by ID, so pruning is deterministic
	})
	kept, _ := Cap(unique, b.cfg.Width)
	return kept, nil
}

// Successor builds a proposal message addressed to a new vertex derived from
// its content.
//
// The ID is a content hash and nothing else, which is a deliberate choice
// between two things that cannot both be true. An ID carrying the path that
// produced it would be readable, but two parents proposing the same candidate
// would then be two vertices — the search would explore it twice and pay
// twice, exactly where it found agreement. An ID carrying only the content
// makes them one vertex, so the second parent is a cache hit and its
// contribution arrives as a second message rather than a second subtree.
//
// Provenance is not lost by that, it moves: the vertex is created from the
// messages that reached it, each naming its sender, and lineage records them.
// That representation can hold two parents; a path in a string cannot.
func Successor(parent core.Record, body string, rank float64) Message {
	sum := sha256.Sum256([]byte(body))
	return Message{
		To:   "c" + hex.EncodeToString(sum[:6]),
		From: parent.ID,
		Body: body,
		Rank: rank,
	}
}

// ExpandFromField turns each entry of a record's field into a successor
// proposal.
//
// score ranks each proposal; nil ranks them all by the parent's "score"
// field, which is the zero-Go-code case: the step is asked to score the
// candidate it just evaluated and to list what to try next, and the beam
// follows the model's own judgement of which lines are worth continuing.
func ExpandFromField(field string, score func(parent core.Record, body string) float64) func(core.Record) []Message {
	return func(r core.Record) []Message {
		bodies := Strings(r, field)
		out := make([]Message, 0, len(bodies))
		for _, body := range bodies {
			rank := Number(r, "score", 0)
			if score != nil {
				rank = score(r, body)
			}
			out = append(out, Successor(r, body, rank))
		}
		return out
	}
}
