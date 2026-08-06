package algo_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/zionrubin/loom/algo"
	"github.com/zionrubin/loom/core"
)

// testGraph is the whole of what an algorithm needs to run: a vertex table and
// no engine at all. Every test in this file is a pure function call, which is
// the property the interface was shaped for — an algorithm decides control
// flow, so it should be testable without paying for a model to prove it.
type testGraph struct {
	verts map[string]core.Record
}

func graphOf(recs ...core.Record) *testGraph {
	g := &testGraph{verts: map[string]core.Record{}}
	for _, r := range recs {
		g.verts[r.ID] = r
	}
	return g
}

func (g *testGraph) Len() int { return len(g.verts) }

func (g *testGraph) IDs() []string {
	out := make([]string, 0, len(g.verts))
	for id := range g.verts {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

func (g *testGraph) Vertex(id string) (core.Record, bool) {
	r, ok := g.verts[id]
	return r, ok
}

func rec(id string, kv map[string]any) core.Record { return core.NewRecord(id, kv) }

// route is a shorthand for handing an algorithm one round's worth of steps.
func route(t *testing.T, a algo.Algorithm, n int, g algo.Graph, ids ...string) []algo.Message {
	t.Helper()
	var steps []algo.Step
	for _, id := range ids {
		v, ok := g.Vertex(id)
		if !ok {
			t.Fatalf("no vertex %q", id)
		}
		steps = append(steps, algo.Step{Vertex: v, Before: v})
	}
	msgs, err := a.Route(algo.Round{N: n, Steps: steps, Graph: g})
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	return msgs
}

func summarize(msgs []algo.Message) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, fmt.Sprintf("%s->%s:%s", m.From, m.To, m.Body))
	}
	return out
}

// --- message plumbing ---------------------------------------------------

// An inbox is part of a cache key, so its order has to be a function of its
// contents and nothing else.
func TestSortIsCanonical(t *testing.T) {
	in := []algo.Message{
		{To: "b", From: "z", Body: "one", Rank: 1},
		{To: "a", From: "y", Body: "two", Rank: 1},
		{To: "a", From: "x", Body: "three", Rank: 5},
		{To: "a", From: "y", Body: "four", Rank: 1},
	}
	shuffled := []algo.Message{in[3], in[1], in[0], in[2]}

	algo.Sort(in)
	algo.Sort(shuffled)
	if !slices.Equal(summarize(in), summarize(shuffled)) {
		t.Errorf("order depends on arrival:\n %v\n %v", summarize(in), summarize(shuffled))
	}
	want := []string{"x->a:three", "y->a:four", "y->a:two", "z->b:one"}
	if got := summarize(in); !slices.Equal(got, want) {
		t.Errorf("sorted = %v, want %v (destination, rank desc, sender, body)", got, want)
	}
}

func TestCapKeepsHighestRanked(t *testing.T) {
	in := []algo.Message{
		{To: "a", Body: "low", Rank: 1},
		{To: "a", Body: "high", Rank: 9},
		{To: "a", Body: "mid", Rank: 5},
	}
	kept, dropped := algo.Cap(in, 2)
	if dropped != 1 {
		t.Errorf("dropped = %d, want 1", dropped)
	}
	if got := algo.Bodies(kept); !slices.Equal(got, []string{"high", "mid"}) {
		t.Errorf("kept = %v, want [high mid]", got)
	}
	if _, dropped := algo.Cap(in, 0); dropped != 0 {
		t.Error("a cap of 0 must mean uncapped")
	}
}

func TestFanoutAddressesOnlyUnaddressedMessages(t *testing.T) {
	direct := algo.Fanout(algo.Message{To: "fixed", Body: "x"}, []string{"a", "b"})
	if len(direct) != 1 || direct[0].To != "fixed" {
		t.Errorf("an addressed message must pass through untouched, got %v", summarize(direct))
	}
	spread := algo.Fanout(algo.Message{Body: "x"}, []string{"a", "b"})
	if got := summarize(spread); !slices.Equal(got, []string{"->a:x", "->b:x"}) {
		t.Errorf("fanout = %v, want one per target", got)
	}
	if n := len(algo.Fanout(algo.Message{Body: "x"}, nil)); n != 0 {
		t.Errorf("a message with nowhere to go produced %d messages, want 0", n)
	}
}

// Model output arrives in whichever shape the model felt like; a list field is
// as likely to be a JSON array as a comma-separated line, and neither is
// something a prompt can guarantee.
func TestStringsAcceptsTheShapesModelsProduce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  []string
	}{
		{"json array", []any{"a", " b "}, []string{"a", "b"}},
		{"string slice", []string{"a", "b"}, []string{"a", "b"}},
		{"comma separated", "a, b ,c", []string{"a", "b", "c"}},
		{"single value", "solo", []string{"solo"}},
		{"empty", "", nil},
		{"absent", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := algo.Strings(rec("v", map[string]any{"f": tc.value}), "f")
			if !slices.Equal(got, tc.want) {
				t.Errorf("Strings(%v) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

// --- BSP ----------------------------------------------------------------

func citation() *testGraph {
	return graphOf(
		rec("a", map[string]any{"cites": []any{"b"}, "say": "from-a"}),
		rec("b", map[string]any{"cites": []any{"c"}, "say": "from-b"}),
		rec("c", map[string]any{"cites": []any{}, "say": "from-c"}),
	)
}

func TestBSPSeedsEveryVertexByDefault(t *testing.T) {
	a, err := algo.NewBSP(algo.BSPConfig{Edges: algo.EdgesFromField("cites")})
	if err != nil {
		t.Fatalf("NewBSP: %v", err)
	}
	msgs, err := a.Seed(citation())
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if len(msgs) != 3 {
		t.Errorf("seeded %d vertices, want 3 (Pregel starts them all)", len(msgs))
	}
	for _, m := range msgs {
		if m.Body != "" {
			t.Errorf("seed message to %s carries a body %q; a seed is a wake-up, not information", m.To, m.Body)
		}
	}
}

func TestBSPSeedsSelectedVerticesOnly(t *testing.T) {
	a, _ := algo.NewBSP(algo.BSPConfig{
		Edges: algo.EdgesFromField("cites"),
		Seeds: func(r core.Record) bool { return r.ID == "a" },
	})
	msgs, _ := a.Seed(citation())
	if len(msgs) != 1 || msgs[0].To != "a" {
		t.Errorf("seed = %v, want one message to a", summarize(msgs))
	}
}

func TestBSPRoutesAlongEdges(t *testing.T) {
	g := citation()
	a, _ := algo.NewBSP(algo.BSPConfig{
		Edges:    algo.EdgesFromField("cites"),
		Emit:     algo.MessagesFromField("say"),
		Directed: true,
	})
	got := summarize(route(t, a, 0, g, "a", "b", "c"))
	// c has something to say and no edge to say it on: not an error, but not
	// a message either.
	want := []string{"a->b:from-a", "b->c:from-b"}
	if !slices.Equal(got, want) {
		t.Errorf("routed = %v, want %v", got, want)
	}
}

func TestBSPUndirectedDeliversBothWays(t *testing.T) {
	g := citation()
	a, _ := algo.NewBSP(algo.BSPConfig{
		Edges: algo.EdgesFromField("cites"),
		Emit:  algo.MessagesFromField("say"),
	})
	got := summarize(route(t, a, 0, g, "b"))
	// b cites c, and a cites b, so undirected b reaches both.
	want := []string{"b->c:from-b", "b->a:from-b"}
	if !slices.Equal(got, want) {
		t.Errorf("undirected routing = %v, want %v", got, want)
	}
}

// BSP's fixpoint is "nothing is moving", so a vertex that keeps talking to
// itself would never let the computation reach one. Refine is the algorithm
// for a deliberate self-loop.
func TestBSPDropsSelfMessages(t *testing.T) {
	g := graphOf(rec("a", map[string]any{"cites": []any{"a", "b"}, "say": "hi"}),
		rec("b", map[string]any{"cites": []any{}}))
	a, _ := algo.NewBSP(algo.BSPConfig{
		Edges: algo.EdgesFromField("cites"), Emit: algo.MessagesFromField("say"),
		Directed: true,
	})
	if got := summarize(route(t, a, 0, g, "a")); !slices.Equal(got, []string{"a->b:hi"}) {
		t.Errorf("routed = %v, want the self-edge dropped", got)
	}
}

func TestBSPMaxMessagesCapsPerVertex(t *testing.T) {
	g := graphOf(
		rec("a", map[string]any{"cites": []any{"b"}, "say": []any{"one", "two", "three"}}),
		rec("b", map[string]any{"cites": []any{}}),
	)
	a, _ := algo.NewBSP(algo.BSPConfig{
		Edges: algo.EdgesFromField("cites"), Emit: algo.MessagesFromField("say"),
		MaxMessages: 2, Directed: true,
	})
	if got := route(t, a, 0, g, "a"); len(got) != 2 {
		t.Errorf("emitted %d messages, want 2 under MaxMessages", len(got))
	}
}

func TestBSPRequiresEdges(t *testing.T) {
	if _, err := algo.NewBSP(algo.BSPConfig{}); err == nil {
		t.Fatal("expected an error when Edges is missing")
	} else if !strings.Contains(err.Error(), "Edges") {
		t.Errorf("error = %v, want it to name Edges", err)
	}
}

// --- Refine -------------------------------------------------------------

func TestRefineStopsWhenAccepted(t *testing.T) {
	a, err := algo.NewRefine(algo.RefineConfig{
		Accept: func(r core.Record) bool { return r.String("verdict") == "good" },
	})
	if err != nil {
		t.Fatalf("NewRefine: %v", err)
	}
	g := graphOf(rec("d", map[string]any{"verdict": "good", "critique": "ignored"}))
	if got := route(t, a, 0, g, "d"); len(got) != 0 {
		t.Errorf("an accepted record emitted %v, want silence", summarize(got))
	}
}

func TestRefineSendsCritiqueToItself(t *testing.T) {
	a, _ := algo.NewRefine(algo.RefineConfig{
		Accept: func(r core.Record) bool { return false },
	})
	g := graphOf(rec("d", map[string]any{"critique": "too long"}))
	got := route(t, a, 2, g, "d")
	if len(got) != 1 {
		t.Fatalf("emitted %v, want one self-message", summarize(got))
	}
	if got[0].To != "d" || got[0].From != "d" || got[0].Body != "too long" {
		t.Errorf("message = %+v, want d->d carrying the critique", got[0])
	}
	if got[0].Rank != 3 {
		t.Errorf("rank = %v, want the round number so later notes outrank earlier ones", got[0].Rank)
	}
}

// A rejected record with nothing to say would re-send an identical inbox,
// which the engine would recognize as a fixpoint and stop anyway. Stopping
// here makes the reason legible instead.
func TestRefineStopsWhenTheCritiqueIsEmpty(t *testing.T) {
	a, _ := algo.NewRefine(algo.RefineConfig{Accept: func(core.Record) bool { return false }})
	g := graphOf(rec("d", map[string]any{"critique": "   "}))
	if got := route(t, a, 0, g, "d"); len(got) != 0 {
		t.Errorf("emitted %v, want silence", summarize(got))
	}
}

func TestRefineCarryAccumulatesTheInbox(t *testing.T) {
	a, _ := algo.NewRefine(algo.RefineConfig{
		Accept: func(core.Record) bool { return false },
		Carry:  true,
	})
	g := graphOf(rec("d", map[string]any{"critique": "second"}))
	msgs, err := a.Route(algo.Round{N: 1, Graph: g, Steps: []algo.Step{{
		Vertex: g.verts["d"],
		Inbox:  []algo.Message{{To: "d", From: "d", Body: "first", Rank: 1}},
	}}})
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if got := algo.Bodies(msgs); !slices.Equal(got, []string{"first", "second"}) {
		t.Errorf("carried inbox = %v, want both notes", got)
	}
}

// --- Beam ---------------------------------------------------------------

func TestBeamKeepsTheBestWidth(t *testing.T) {
	a, err := algo.NewBeam(algo.BeamConfig{
		Width:  2,
		Expand: algo.ExpandFromField("next", func(_ core.Record, body string) float64 { return float64(len(body)) }),
	})
	if err != nil {
		t.Fatalf("NewBeam: %v", err)
	}
	g := graphOf(rec("r", map[string]any{"next": []any{"a", "bbbb", "cc", "ddd"}}))
	got := route(t, a, 0, g, "r")
	if len(got) != 2 {
		t.Fatalf("kept %d candidates, want 2", len(got))
	}
	if bodies := algo.Bodies(got); !slices.Equal(bodies, []string{"bbbb", "ddd"}) {
		t.Errorf("kept = %v, want the two highest-scoring", bodies)
	}
}

// Two parents proposing the same successor is a merge in the search tree, not
// two candidates; charging it two beam slots would narrow the search exactly
// where it found agreement.
func TestBeamMergesDuplicateSuccessors(t *testing.T) {
	a, _ := algo.NewBeam(algo.BeamConfig{Width: 4, Expand: algo.ExpandFromField("next", nil)})
	g := graphOf(
		rec("p", map[string]any{"next": []any{"shared"}, "score": 1.0}),
		rec("q", map[string]any{"next": []any{"shared"}, "score": 2.0}),
	)
	got := route(t, a, 0, g, "p", "q")
	if len(got) != 1 {
		t.Fatalf("kept %d candidates, want 1 merged", len(got))
	}
	if got[0].Rank != 2 {
		t.Errorf("merged rank = %v, want the better of the two", got[0].Rank)
	}
}

func TestBeamDoesNotExpandTerminalCandidates(t *testing.T) {
	a, _ := algo.NewBeam(algo.BeamConfig{
		Width:    4,
		Expand:   algo.ExpandFromField("next", nil),
		Terminal: func(r core.Record) bool { return r.String("done") == "yes" },
	})
	g := graphOf(
		rec("p", map[string]any{"next": []any{"more"}, "done": "yes"}),
		rec("q", map[string]any{"next": []any{"more too"}}),
	)
	got := route(t, a, 0, g, "p", "q")
	if len(got) != 1 || got[0].From != "q" {
		t.Errorf("expanded = %v, want only q's proposal", summarize(got))
	}
}

// A candidate reached twice must land on the same vertex — including from two
// different parents, which is the case that decides whether agreement between
// two lines of search costs one call or two.
func TestSuccessorIDsAreContentDerived(t *testing.T) {
	first := algo.Successor(rec("root", nil), "an idea", 1)
	if second := algo.Successor(rec("root", nil), "an idea", 9); first.To != second.To {
		t.Errorf("same content gave different IDs: %q vs %q", first.To, second.To)
	}
	if elsewhere := algo.Successor(rec("other-parent", nil), "an idea", 1); elsewhere.To != first.To {
		t.Errorf("a different parent gave a different ID (%q vs %q): agreement "+
			"between two branches must merge, not fork", elsewhere.To, first.To)
	}
	if other := algo.Successor(rec("root", nil), "a different idea", 1); other.To == first.To {
		t.Error("different content gave the same ID")
	}
	// The sender still says where it came from; that is where provenance
	// lives, and unlike a path it can hold two parents.
	if first.From != "root" {
		t.Errorf("From = %q, want the proposing parent", first.From)
	}
}

func TestBeamRequiresWidthAndExpand(t *testing.T) {
	if _, err := algo.NewBeam(algo.BeamConfig{Expand: algo.ExpandFromField("n", nil)}); err == nil {
		t.Error("expected an error for a beam with no width")
	}
	if _, err := algo.NewBeam(algo.BeamConfig{Width: 2}); err == nil {
		t.Error("expected an error for a beam with no Expand")
	}
}

func TestBeamRejectsUnaddressedProposals(t *testing.T) {
	a, _ := algo.NewBeam(algo.BeamConfig{
		Width:  2,
		Expand: func(core.Record) []algo.Message { return []algo.Message{{Body: "nowhere"}} },
	})
	g := graphOf(rec("r", nil))
	_, err := a.Route(algo.Round{Graph: g, Steps: []algo.Step{{Vertex: g.verts["r"]}}})
	if err == nil || !strings.Contains(err.Error(), "destination") {
		t.Errorf("error = %v, want a complaint about the missing destination", err)
	}
}

// Every algorithm here must satisfy the interface the engine drives.
var (
	_ algo.Algorithm = (*algo.BSP)(nil)
	_ algo.Algorithm = (*algo.Refine)(nil)
	_ algo.Algorithm = (*algo.Beam)(nil)
)
