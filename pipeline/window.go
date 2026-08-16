package pipeline

import "github.com/zionrubin/loom/stream"

// FromStream starts a pipeline from an unbounded source bound at run time with
// loom.WithSource.
//
// It is the one authoring difference between a batch pipeline and a streaming
// one, and it is deliberately explicit. A pipeline that reads a set of records
// and a pipeline that reads a feed differ in what their aggregates mean, what
// their failures cost, and whether they ever finish; a source stage that
// quietly became unbounded depending on which entry point was called would hide
// all three. loom.Run refuses a pipeline with a stream source, and loom.Stream
// refuses one without.
func (p *Pipeline) FromStream(name string) Dataset {
	return p.add(&Stage{ID: name, Kind: KindSource, Stream: true})
}

// Window cuts an unbounded input into finite sets.
//
// It is the stage that makes the rest of Loom work on a stream. ReduceAI,
// Combine and Iterate all need to know that their input is complete, and on a
// finite dataset the end of the input is what tells them; on a stream there is
// no end, so a window supplies one. Everything downstream of a window is scoped
// to a pane — an aggregate folds the pane, not the stream — and everything
// upstream of it runs per record, as the records arrive, which is where a
// per-record Infer belongs when latency matters.
//
//	events := p.FromStream("incidents")
//
//	graded := events.Infer("grade", ...)          // runs per record, immediately
//
//	graded.
//	    Window("per-minute", stream.WindowSpec{   // the finite set
//	        Assigner: stream.Tumbling(time.Minute),
//	        Key:      func(r core.Record) string { return r.String("service") },
//	        Lateness: 30 * time.Second,
//	    }).
//	    ReduceAI("digest", ...)                   // once per pane, over its records
//
// A window fires once per pane by default, and a pane is a model call
// downstream, so the shape of the window is the shape of the bill: halving the
// interval doubles the aggregations, and a keyed window multiplies them by the
// key's cardinality. loom.Explain prices a pipeline per pane for exactly this
// reason.
func (d Dataset) Window(name string, spec stream.WindowSpec, opts ...Option) Dataset {
	return d.p.add(&Stage{
		ID: name, Kind: KindWindow, Upstream: d.stage,
		Window: &spec, Opts: applyOpts(opts),
	})
}

// StreamSource reports whether s is a source bound to an unbounded stream.
func StreamSource(s *Stage) bool { return s.Kind == KindSource && s.Stream }
