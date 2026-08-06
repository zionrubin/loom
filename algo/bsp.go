package algo

import (
	"fmt"
	"slices"

	"github.com/zionrubin/loom/core"
)

// BSP is bulk-synchronous message passing: Pregel, with a model call as the
// vertex program.
//
// Every round, each vertex that received a message runs, then emits messages
// that travel along its edges to be delivered at the start of the next round.
// A vertex with an empty inbox does not run. That single rule is what bends
// the cost curve downward as the computation converges: an iterative model
// workload normally gets more expensive per round as context accumulates,
// while this one gets cheaper as vertices fall silent, and a vertex that falls
// silent stops costing anything at all rather than costing a cache lookup.
//
// The edges may be static (declared in the records, or in a table) while the
// messages are chosen by the model. That combination is the useful one: the
// pipeline's author declares what *can* be reached, and the vertex program
// decides what *is* reached — which is a graph traversal whose shape is
// discovered by the computation, inside an adjacency the author bounded.
type BSP struct {
	cfg BSPConfig
}

// BSPConfig configures bulk-synchronous message passing.
type BSPConfig struct {
	// Edges returns the vertex IDs a vertex may send to. Required.
	//
	// Use EdgesFromField for adjacency carried by the records themselves, or
	// EdgesFromTable for a graph known up front.
	Edges func(core.Record) []string

	// Emit returns the messages a vertex sends after running. A message with
	// no To is delivered to every vertex Edges names; one with a To is
	// delivered there directly, which is how a vertex program reaches a
	// vertex the author never declared an edge to.
	//
	// Defaults to MessagesFromField("messages").
	Emit func(core.Record) []Message

	// Seeds selects which vertices are active in round 0. Nil starts every
	// vertex, which is Pregel's rule and the right default for a computation
	// that diffuses; supply one to start from a frontier instead.
	Seeds func(core.Record) bool

	// MaxMessages caps how many messages one vertex may emit per round,
	// keeping the highest-ranked (0 = no cap).
	//
	// This is the per-vertex half of explosion control. The engine enforces
	// the other half — a cap on the whole round — because a thousand vertices
	// emitting a legal two messages each is just as expensive as one vertex
	// emitting two thousand illegal ones.
	MaxMessages int

	// Directed, when false, delivers along edges in both directions: a vertex
	// with an edge to another also hears from it. Undirected is usually what
	// a "related to" field means and rarely what a "cites" field means, so
	// this defaults to directed and has to be asked for.
	Directed bool
}

// NewBSP builds a bulk-synchronous algorithm, validating the parts that
// cannot be defaulted.
func NewBSP(cfg BSPConfig) (*BSP, error) {
	if cfg.Edges == nil {
		return nil, fmt.Errorf("algo: BSP needs Edges (try algo.EdgesFromField(\"cites\"))")
	}
	if cfg.Emit == nil {
		cfg.Emit = MessagesFromField("messages")
	}
	if cfg.MaxMessages < 0 {
		return nil, fmt.Errorf("algo: BSP MaxMessages must not be negative")
	}
	return &BSP{cfg: cfg}, nil
}

// Name implements Algorithm.
func (b *BSP) Name() string { return "bsp" }

// Seed implements Algorithm: one empty message per starting vertex, which
// makes it active without giving it anything to read.
func (b *BSP) Seed(g Graph) ([]Message, error) {
	var out []Message
	for _, id := range g.IDs() {
		v, ok := g.Vertex(id)
		if !ok {
			continue
		}
		if b.cfg.Seeds != nil && !b.cfg.Seeds(v) {
			continue
		}
		out = append(out, Message{To: id})
	}
	return out, nil
}

// Route implements Algorithm: each stepped vertex emits, and its emissions
// are addressed along its edges.
func (b *BSP) Route(r Round) ([]Message, error) {
	var out []Message
	for _, st := range r.Steps {
		v := st.Vertex
		emitted := b.cfg.Emit(v)
		if len(emitted) == 0 {
			continue // this vertex voted to halt
		}
		emitted, _ = Cap(emitted, b.cfg.MaxMessages)

		targets := b.targets(v, r.Graph)
		for _, m := range emitted {
			if m.From == "" {
				m.From = v.ID
			}
			// An unaddressed message with no edges to travel is not an error
			// — a leaf that has something to say and nowhere to say it is a
			// normal state — but it does not become a message.
			out = append(out, Fanout(m, targets)...)
		}
	}
	// A vertex never messages itself along an edge: BSP's fixpoint is
	// "nothing is moving", and a self-loop is a vertex that never stops
	// moving. Refine is the algorithm for a deliberate self-loop.
	out = slices.DeleteFunc(out, func(m Message) bool { return m.To == m.From })
	return out, nil
}

// targets is the set of vertices a vertex's unaddressed messages reach:
// its own edges, plus — when the graph is undirected — every vertex whose
// edges point back at it.
func (b *BSP) targets(v core.Record, g Graph) []string {
	out := b.cfg.Edges(v)
	if b.cfg.Directed {
		return out
	}
	seen := make(map[string]bool, len(out))
	for _, t := range out {
		seen[t] = true
	}
	for _, id := range g.IDs() {
		if id == v.ID || seen[id] {
			continue
		}
		other, ok := g.Vertex(id)
		if !ok {
			continue
		}
		if slices.Contains(b.cfg.Edges(other), v.ID) {
			out = append(out, id)
			seen[id] = true
		}
	}
	return out
}
