// Package algo is Loom's algorithm seam: the interface an iterative
// computation plugs into, and the algorithms that ship with it.
//
// Loom has two extension points already. executor.Executor decides *where* a
// task runs, and executor.OpRunner decides *what* one task does. Neither says
// anything about the control flow *between* tasks — that was fixed, and it was
// "walk the DAG once, forward". Everything that is not one pass (a loop, a
// traversal, a search) had nowhere to live but inside a driver.
//
// An Algorithm is that missing piece: the policy that decides, round by round,
// which records run next and what information they carry with them. It is a
// pure function of the round that just finished, which is what makes it small
// enough to be worth writing and testable without a model:
//
//	Seed(graph)  → the messages that make round 0's frontier
//	Route(round) → the messages that make round N+1's frontier
//	no messages  → the computation has converged
//
// Everything expensive stays with the engine. The algorithm never schedules a
// task, never touches the budget, never calls a model, and never sees a
// provider. It moves messages. The engine turns a frontier into tasks, and
// from there it is ordinary Loom work: one admission-controlled, retried,
// escalated, content-addressed, audited task per active vertex per round.
//
// # The contract an algorithm may assume
//
// The vertex program is a function of (state, inbox). It is executed by a
// model, so it is a function only in the sense that its output is determined
// by its input — which is exactly the property caching and quiescence
// detection rest on, and the reason this package's algorithms deal in
// messages rather than in side effects.
//
// # Writing one
//
// Implement Algorithm, or start from one of the three here:
//
//	BSP     bulk-synchronous message passing along edges (Pregel)
//	Refine  a record critiquing itself until it is good enough
//	Beam    frontier search that keeps the best k candidates per round
//
// They are deliberately different shapes — diffusion over a fixed graph, a
// self-loop, and a tree that grows as it is searched — because an interface
// with one implementation has not been shown to be an interface.
package algo

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/zionrubin/loom/core"
)

// Reserved record fields. The engine writes a vertex's inbox into these keys
// before the step runs so prompt templates can read it, and strips them from
// the result afterwards so a round's inbox never becomes part of the next
// round's state.
//
// They are capitalized, unlike ordinary record data, because a template reads
// them as {{.Inbox}} beside the record's own {{.title}} and the distinction
// should be visible in the prompt. A vertex that already carries one of these
// keys is rejected at seed time rather than silently overwritten.
const (
	// FieldInbox holds the message bodies delivered to this vertex, in
	// canonical order, as []string.
	FieldInbox = "Inbox"
	// FieldSenders holds the sending vertex ID of each message in FieldInbox,
	// aligned by index.
	FieldSenders = "Senders"
)

// Reserved lists the record fields the engine owns during an iterative stage.
func Reserved() []string { return []string{FieldInbox, FieldSenders} }

// Message is one piece of information passed from one vertex to another
// between rounds. It is the only thing an algorithm produces.
//
// Body is text rather than a structured value, and that is deliberate: the
// receiving vertex's program is a model call, so a message's destination is a
// prompt. Rank is the numeric channel alongside it — a score, a weight, a
// priority — used to order an inbox and to prune a frontier, where a
// number is what an algorithm actually needs.
type Message struct {
	// To is the receiving vertex's ID. An empty To means "every vertex this
	// one has an edge to", which the algorithm expands during routing.
	To string `json:"to"`
	// From is the sending vertex's ID, delivered alongside the body so a
	// prompt can say who said what.
	From string `json:"from,omitempty"`
	// Body is what the receiving vertex reads.
	Body string `json:"body,omitempty"`
	// Rank orders and prunes. Higher is better: an inbox is delivered highest
	// first, and an algorithm that keeps only the best k keeps the highest k.
	Rank float64 `json:"rank,omitempty"`
}

// Graph is the read-only view of the vertex table an algorithm sees. The
// engine owns the table — vertices are created, updated, and retired by it —
// so an algorithm can read state it did not just compute (a neighbour's
// current value, say) without being able to corrupt it.
type Graph interface {
	// Len is the number of vertices.
	Len() int
	// IDs returns every vertex ID in sorted order.
	IDs() []string
	// Vertex returns a copy of one vertex's current state.
	Vertex(id string) (core.Record, bool)
}

// Step is one vertex's completed execution within a round: what it consumed,
// and what it became.
type Step struct {
	// Vertex is the vertex's state after the step — the record the model
	// produced, with the inbox fields stripped.
	Vertex core.Record
	// Before is the state the step started from. The pair is what lets an
	// algorithm test for convergence by delta ("this value moved less than
	// epsilon") rather than only by silence.
	Before core.Record
	// Inbox is what this vertex consumed, in the order it was delivered.
	Inbox []Message
}

// ID is the stepped vertex's identifier.
func (s Step) ID() string { return s.Vertex.ID }

// Round is a completed superstep, handed to Route so it can decide the next
// one. Every field describes work that has already happened and been paid
// for; the return value is the only thing that costs anything.
type Round struct {
	// N is the round index, starting at 0.
	N int
	// Steps holds one entry per vertex that ran, ordered by vertex ID.
	Steps []Step
	// Graph is the vertex table as it stands after this round.
	Graph Graph
}

// Algorithm is the pluggable control flow of an iterative stage.
//
// Implementations must be deterministic and free of side effects: the engine
// may call Route once per round on one goroutine, but the value it returns
// participates in what the next round's cache keys are, so an algorithm that
// consults a clock or a random source silently turns a resumable computation
// into an unrepeatable one.
type Algorithm interface {
	// Name identifies the algorithm in events, reports, and stage detail. It
	// is not part of any fingerprint — see the package documentation for why.
	Name() string

	// Seed returns the messages that define round 0's frontier. Returning one
	// empty-bodied message per vertex starts every vertex active, which is
	// Pregel's rule; returning fewer starts a partial frontier.
	Seed(g Graph) ([]Message, error)

	// Route consumes a completed round and returns the messages that define
	// the next one. Returning no messages halts the computation: the graph has
	// gone quiet, which is the only halt condition an algorithm controls.
	//
	// A message addressed to a vertex the graph does not contain is not an
	// error here. Whether it creates that vertex or is dropped is the stage's
	// decision, declared with pipeline.IterateSpec.Grow.
	Route(r Round) ([]Message, error)
}

// --- Message handling ---------------------------------------------------

// Sort orders messages canonically: destination, then rank descending, then
// sender, then body.
//
// This is not cosmetic. A vertex's cache key is its state plus its inbox, so
// two runs that deliver the same messages in a different order would miss the
// cache and pay again for identical work — and the economic argument for
// iterating at all is that convergence makes rounds cheaper. Canonical
// ordering is what makes an inbox a value rather than a sequence of arrivals.
func Sort(msgs []Message) {
	slices.SortStableFunc(msgs, func(a, b Message) int {
		if c := cmp.Compare(a.To, b.To); c != 0 {
			return c
		}
		if c := cmp.Compare(b.Rank, a.Rank); c != 0 {
			return c
		}
		if c := cmp.Compare(a.From, b.From); c != 0 {
			return c
		}
		return cmp.Compare(a.Body, b.Body)
	})
}

// GroupByTo buckets messages by destination, each bucket in canonical order.
func GroupByTo(msgs []Message) map[string][]Message {
	sorted := slices.Clone(msgs)
	Sort(sorted)
	out := make(map[string][]Message)
	for _, m := range sorted {
		out[m.To] = append(out[m.To], m)
	}
	return out
}

// Bodies extracts message bodies in order — the []string a prompt template
// ranges over as {{.Inbox}}.
func Bodies(msgs []Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Body
	}
	return out
}

// Senders extracts message senders in order, aligned with Bodies.
func Senders(msgs []Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.From
	}
	return out
}

// Fanout expands a message with no explicit destination into one message per
// target, and passes addressed messages through untouched. It is what lets an
// algorithm write "send this to my neighbours" without knowing them.
func Fanout(m Message, targets []string) []Message {
	if m.To != "" {
		return []Message{m}
	}
	out := make([]Message, 0, len(targets))
	for _, t := range targets {
		c := m
		c.To = t
		out = append(out, c)
	}
	return out
}

// Cap keeps at most n messages, highest-ranked first, and reports how many it
// dropped. A zero or negative n keeps everything.
//
// It exists because the failure mode of message passing over model calls is
// not a slow round, it is a geometric one: a vertex that emits fifty messages
// multiplies the next frontier by fifty, and the bill with it.
func Cap(msgs []Message, n int) (kept []Message, dropped int) {
	if n <= 0 || len(msgs) <= n {
		return msgs, 0
	}
	sorted := slices.Clone(msgs)
	Sort(sorted)
	slices.SortStableFunc(sorted, func(a, b Message) int { return cmp.Compare(b.Rank, a.Rank) })
	return sorted[:n], len(sorted) - n
}

// --- Record field readers -----------------------------------------------

// Strings reads a record field as a list of strings, accepting the three
// shapes a field can plausibly arrive in: a JSON array (what a model that was
// asked for a list produces), a comma-separated string (what one that was
// asked for a line produces), and a single value.
//
// Both are common enough that rejecting either would mean every pipeline
// writing the same coercion, and a model's output shape is not something a
// prompt can guarantee.
func Strings(r core.Record, field string) []string {
	v, ok := r.Data[field]
	if !ok || v == nil {
		return nil
	}
	switch t := v.(type) {
	case []string:
		return trimAll(t)
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s := fmt.Sprintf("%v", e); strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return out
	case string:
		if !strings.Contains(t, ",") {
			if s := strings.TrimSpace(t); s != "" {
				return []string{s}
			}
			return nil
		}
		return trimAll(strings.Split(t, ","))
	default:
		if s := strings.TrimSpace(fmt.Sprintf("%v", t)); s != "" {
			return []string{s}
		}
		return nil
	}
}

func trimAll(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Number reads a record field as a float, returning fallback when the field
// is absent or not numeric. Model output that made a JSON round trip arrives
// as float64; a hand-built record may hold an int.
func Number(r core.Record, field string, fallback float64) float64 {
	switch t := r.Data[field].(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return f
		}
	}
	return fallback
}

// EdgesFromField reads a vertex's adjacency out of one of its own fields —
// the simplest place for a graph to live, and the one that needs no store:
// a paper's "cites", a ticket's "duplicates", a person's "reports_to".
func EdgesFromField(field string) func(core.Record) []string {
	return func(r core.Record) []string { return Strings(r, field) }
}

// EdgesFromTable reads adjacency from a fixed table, for a graph that is
// known up front rather than carried by the records.
func EdgesFromTable(table map[string][]string) func(core.Record) []string {
	return func(r core.Record) []string { return table[r.ID] }
}

// MessagesFromField turns one of a vertex's fields into the messages it
// sends: each entry becomes an unaddressed message, so it reaches whichever
// vertices the algorithm's edges lead to.
//
// This is the model-produced case, and it is the interesting one — the field
// was written by the step that just ran, so what the graph does next was
// decided by a model rather than by the pipeline's author.
func MessagesFromField(field string) func(core.Record) []Message {
	return func(r core.Record) []Message {
		items := Strings(r, field)
		out := make([]Message, 0, len(items))
		for _, s := range items {
			out = append(out, Message{From: r.ID, Body: s})
		}
		return out
	}
}
