package algo

import (
	"fmt"
	"strings"

	"github.com/zionrubin/loom/core"
)

// Refine is draft, critique, revise: each record loops on itself until it is
// good enough or the stage's limits stop it.
//
// It is the degenerate graph — one vertex, one self-edge — and it is here for
// two reasons. It is the fourth workload one pass cannot express and the one
// people reach for first. And it is the shape that shows the interface is
// about control flow rather than about graphs: the same engine, the same
// budget, the same content-addressed cache, with a message that goes nowhere.
//
// Records refine independently. A hundred drafts in one stage are a hundred
// self-loops running concurrently, each halting when its own critique passes,
// so the stage's cost falls as the easy ones finish rather than being set by
// the hardest one.
type Refine struct {
	cfg RefineConfig
}

// RefineConfig configures the refine loop.
type RefineConfig struct {
	// Accept reports whether a record is finished. Required.
	//
	// It is an ordinary Go predicate and runs in the engine, not in a model,
	// which is deliberate: the thing deciding when to stop spending should not
	// itself be a call that costs money and can be talked out of its answer.
	// A model's judgement still drives it — have the step write a score or a
	// verdict field and test that here.
	Accept func(core.Record) bool

	// Note builds the message a rejected record sends itself: what the next
	// attempt should read. Defaults to the record's "critique" field.
	Note func(core.Record) string

	// Carry, when true, keeps every previous note in the inbox instead of
	// only the newest one. Off by default: the usual failure of a refine loop
	// is a prompt that grows until it drowns the draft, and the previous
	// critique is normally the only one that still applies.
	Carry bool
}

// NewRefine builds a refine loop.
func NewRefine(cfg RefineConfig) (*Refine, error) {
	if cfg.Accept == nil {
		return nil, fmt.Errorf("algo: Refine needs Accept (the predicate that stops the loop)")
	}
	if cfg.Note == nil {
		cfg.Note = func(r core.Record) string { return r.String("critique") }
	}
	return &Refine{cfg: cfg}, nil
}

// Name implements Algorithm.
func (f *Refine) Name() string { return "refine" }

// Seed implements Algorithm: every record starts a loop.
func (f *Refine) Seed(g Graph) ([]Message, error) {
	out := make([]Message, 0, g.Len())
	for _, id := range g.IDs() {
		out = append(out, Message{To: id})
	}
	return out, nil
}

// Route implements Algorithm: a record that fails Accept sends itself the
// note it should address next; one that passes sends nothing and goes quiet.
func (f *Refine) Route(r Round) ([]Message, error) {
	var out []Message
	for _, st := range r.Steps {
		if f.cfg.Accept(st.Vertex) {
			continue
		}
		note := strings.TrimSpace(f.cfg.Note(st.Vertex))
		if note == "" {
			// Rejected with nothing to say. Looping again would resend an
			// identical inbox, which the engine would recognize as a fixpoint
			// and stop anyway — so stop here, where the reason is legible.
			continue
		}
		if f.cfg.Carry {
			// Re-send what this attempt already read, so the inbox
			// accumulates rather than replaces.
			for _, prev := range st.Inbox {
				prev.To, prev.From = st.ID(), st.ID()
				out = append(out, prev)
			}
		}
		out = append(out, Message{
			To: st.ID(), From: st.ID(), Body: note,
			// Later notes outrank earlier ones, so a carried inbox reads
			// newest-first and a truncated one keeps what still applies.
			Rank: float64(r.N + 1),
		})
	}
	return out, nil
}
