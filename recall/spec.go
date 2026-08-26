package recall

import (
	"fmt"
	"strings"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/stream"
)

// Spec is the four model operations a desk performs, and the whole of what it
// asks a model to do.
//
// They are ordinary Loom specs — same bindings, same escalation ladders, same
// validation, same prefix caching — because a desk is a pipeline and a query,
// not a new kind of thing. Replacing one is how a desk is taught a domain:
// Read decides what is worth remembering, Digest decides how a slice of
// conversation is written down, Compact decides what survives when the context
// outgrows its budget, and Answer decides how a question is answered against
// what survived.
//
// The split between them is a cost decision as much as a semantic one. Read
// runs once per message and belongs on a cheap tier; Digest runs once per pane;
// Compact runs once per budget's worth of entries and is the only one that ever
// reads the whole context; Answer runs once per question and is the only one a
// user waits for.
type Spec struct {
	// Read runs on every message as it arrives. It is the expensive half of
	// the design being expensive at the right time: the understanding is paid
	// for on ingest, once, rather than on every query that would have needed
	// it.
	Read pipeline.InferSpec
	// Digest folds one window of one conversation into the line the context
	// will carry. It aggregates over the "item" field, which is what the Keep
	// stage writes each surviving message's observation into — a replacement
	// that sets a different ItemField will find nothing to fold.
	Digest pipeline.ReduceAISpec
	// Compact folds the oldest entries into the standing brief when the
	// context outgrows Config.MaxBytes.
	Compact pipeline.ReduceAISpec
	// Answer answers a question against the maintained context, which reaches
	// it as the continuation at the front of its prompt.
	Answer pipeline.InferSpec
}

// Kinds are the classifications the default Read prompt asks for. Anything
// else is a semantic failure, which is what sends the record up the escalation
// ladder rather than into the context.
var Kinds = []string{"fact", "decision", "request", "problem", "preference", "noise"}

// DefaultSpec returns a desk that works out of the box on ordinary
// conversations: support threads, sales calls, standups, agent transcripts.
//
// The bindings are tiers rather than model IDs, so a registry decides what
// actually runs. Read is fast and escalates on invalid output; Digest and
// Compact are deep because their output is what every later query reads, and a
// bad line in the context is not a bad record — it is a bad answer to every
// question that touches it.
func DefaultSpec() Spec {
	return Spec{
		Read: pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			System: "You read one message from a conversation and say what, if anything, " +
				"is worth remembering about it a week from now.",
			// The instruction is identical on every message, so it is a prefix:
			// rendered once per task and served from the provider's prompt
			// cache on every call after the first.
			Prefix: "Reply with JSON: {\"kind\": one of " + strings.Join(Kinds, "|") +
				", \"note\": a single sentence in the third person, \"subject\": " +
				"two or three words naming what it is about}.\n" +
				"Use kind \"noise\" for greetings, acknowledgements, and small talk, " +
				"and leave note empty when you do.",
			Prompt:    "In conversation {{.conversation}}, {{.role}} said:\n{{.text}}",
			ParseJSON: true,
			MaxTokens: 256,
			Validate:  validRead,
		},
		Digest: pipeline.ReduceAISpec{
			Binding: model.Binding{Tier: model.TierDeep},
			System: "You keep the running record of a conversation. You write the one " +
				"entry that a colleague who reads nothing else should have.",
			Prefix: "Write at most three sentences. State what happened and what is " +
				"outstanding. Do not speculate, do not repeat the wording, and do not " +
				"write a preamble.",
			Prompt: "{{.Count}} observations from one stretch of one conversation, " +
				"timestamped and not necessarily in order:\n" +
				"{{range .Items}}- {{.}}\n{{end}}",
			FanIn:       12,
			ItemField:   itemField,
			OutputField: "entry",
			MaxTokens:   400,
		},
		Compact: pipeline.ReduceAISpec{
			Binding: model.Binding{Tier: model.TierDeep},
			System: "You compress a body of accumulated notes into a standing brief, " +
				"losing detail rather than losing subjects.",
			Prefix: "Keep every distinct subject, participant and open question, and " +
				"drop the detail behind them. Group by subject. Anything unresolved " +
				"survives compression intact.",
			Prompt: "Compress these {{.Count}} entries into a standing brief:\n" +
				"{{range .Items}}- {{.}}\n{{end}}",
			FanIn:       8,
			ItemField:   "entry",
			OutputField: "entry",
			MaxTokens:   1200,
		},
		Answer: pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierDeep},
			System: "You answer questions about a body of conversations. The context " +
				"you have been given is the whole of what you know about them: it is " +
				"maintained continuously and it is already a summary. Answer from it, " +
				"cite the conversations you used, and say plainly when it does not " +
				"contain the answer rather than inferring one.",
			Prompt:      "{{.history}}Question: {{.question}}",
			OutputField: "answer",
			MaxTokens:   1024,
		},
	}
}

// validRead is the Read stage's semantic gate: a record whose kind is not one
// of the declared ones has not failed to arrive, it has arrived wrong, and the
// difference is what the escalation ladder is for.
func validRead(r core.Record) error {
	kind := strings.ToLower(strings.TrimSpace(r.String("kind")))
	for _, k := range Kinds {
		if kind == k {
			if kind != "noise" && strings.TrimSpace(r.String("note")) == "" {
				return fmt.Errorf("kind %q with an empty note", kind)
			}
			return nil
		}
	}
	return fmt.Errorf("kind %q is not one of %s", r.String("kind"), strings.Join(Kinds, ", "))
}

// --- the ingest pipeline -------------------------------------------------

// Stage names. They are exported because they are what an event handler, a
// report row and a loom.WithSink binding name.
const (
	// StageMessages is the stream source every message arrives through.
	StageMessages = "messages"
	// StageLive drops the feed's idle heartbeats before they cost anything.
	StageLive = "live"
	// StageRead is the per-message inference.
	StageRead = "read"
	// StageKeep is the pure stage that drops what Read judged noise.
	StageKeep = "keep"
	// StageSlice is the window: one pane per conversation per interval.
	StageSlice = "slice"
	// StageDigest is the per-pane aggregation whose output becomes a context
	// entry.
	StageDigest = "digest"
)

// ingestPipeline builds the pipeline that turns a feed of messages into
// entries for the context.
//
// The shape is the whole argument of the package, so it is worth reading as
// one sentence: every message is understood as it arrives, what is not worth
// remembering is dropped before it costs anything downstream, what remains is
// cut into per-conversation slices by event time, and each slice becomes one
// line of a context that is never rebuilt.
//
// Only the window is stream-specific. Everything above it runs per record with
// full pipelining — a read call starts the moment a message lands — and
// everything below it runs once per pane, which is the unit that costs money.
func (d *Desk) ingestPipeline() *pipeline.Pipeline {
	p := pipeline.New("recall/" + d.name)

	spec := stream.WindowSpec{
		Assigner: stream.Tumbling(d.every),
		// Cut per conversation: a slice of one thread is a thing a model can
		// summarize, and a slice of every thread at once is not.
		Key: func(r core.Record) string { return r.String("conversation") },
		// Event time from the payload rather than from when the message was
		// read, so a backfill lands in the windows it happened in.
		Time:     stream.EventTime("at"),
		Lateness: d.lateness,
		// A busy conversation should not wait for its interval to end before
		// any of it is queryable, so a slice that fills fires early and purges.
		// Purging is what keeps this from doubling the bill: each message is
		// carried by exactly one pane, so the entries are disjoint rather than
		// each superseding the last.
		Trigger:    stream.AtCount(d.slice),
		MaxRecords: d.slice * 4,
	}

	messages := p.FromStream(StageMessages)

	// A heartbeat exists to move event time, which it has already done by the
	// time it reaches a stage: the watermark is taken at ingestion, before the
	// first stage sees a record. So this drops it immediately, and it costs one
	// pure task per idle period rather than a model call.
	//
	// It sits ahead of Read rather than sharing the Keep stage below for the
	// same reason: what a heartbeat must never reach is the inference.
	live := messages.Filter(StageLive, notHeartbeat, pipeline.WithBatchSize(16))

	live.
		Infer(StageRead, d.spec.Read).
		FlatMap(StageKeep, d.worthKeeping).
		Window(StageSlice, spec).
		ReduceAI(StageDigest, d.spec.Digest)

	return p
}

// notHeartbeat passes everything a caller actually posted.
func notHeartbeat(r core.Record) (bool, error) { return !heartbeat(r), nil }

// worthKeeping drops the messages Read judged not worth remembering, and writes what
// survives into the one line the digest will read.
//
// Dropping is the cheapest work in the pipeline and some of the most valuable:
// a context stays compact mainly by not containing things, and a greeting that
// never reaches the digest costs nothing to summarize and nothing to carry in
// front of every question thereafter.
//
// Writing the line is the other half, and it is here because of when records
// arrive rather than what is in them. A pane holds what the Read stage
// finished, in the order it finished — which for concurrent per-record
// inference is not the order things were said. Stamping each observation with
// its own event time is what lets the aggregation read an exchange as an
// exchange instead of as a bag of sentences, and it is cheaper than the
// alternative of making inference sequential to preserve an order.
//
// It also counts what it drops, which is not bookkeeping for its own sake. A
// dropped message never reaches a pane, so nothing downstream can ever account
// for it, and a caller waiting for "everything I posted" would wait forever on
// a greeting. Counted here, a message is accounted for exactly once: either it
// was dropped, or it was folded into the context.
//
// The stage is deliberately left uncacheable — no pipeline.WithVersion — so
// that the count is of messages processed rather than of cache misses.
func (d *Desk) worthKeeping(r core.Record) ([]core.Record, error) {
	note := strings.TrimSpace(r.String("note"))
	if note == "" || strings.EqualFold(strings.TrimSpace(r.String("kind")), "noise") {
		d.dropped.Add(1)
		return nil, nil
	}
	out := r.Clone()
	out.Data[itemField] = observation(r, note)
	return []core.Record{out}, nil
}

// itemField is the field the digest aggregates over.
const itemField = "item"

// observation renders one message's contribution as the digest reads it: when,
// from whom, of what kind, and what it amounts to.
func observation(r core.Record, note string) string {
	var b strings.Builder
	if at := said(r); !at.IsZero() {
		b.WriteString(at.UTC().Format("15:04:05"))
		b.WriteString(" ")
	}
	if who := speaker(r); who != "" {
		b.WriteString(who)
		b.WriteString(" ")
	}
	if kind := strings.TrimSpace(r.String("kind")); kind != "" {
		b.WriteString("(" + kind + ") ")
	}
	b.WriteString(note)
	return b.String()
}

// said reads a record's event time back out of the field the source wrote it
// to. A message whose time could not be parsed contributes an unstamped
// observation rather than a wrong one.
func said(r core.Record) time.Time {
	return stream.EventTime("at")(r)
}

// speaker is who a message is from, by whichever of the two names the feed
// supplied.
func speaker(r core.Record) string {
	if who := strings.TrimSpace(r.String("speaker")); who != "" {
		return who
	}
	return strings.TrimSpace(r.String("role"))
}

// entryBody composes what one pane contributes to the context: the digest the
// model wrote, under a header the framework knows without asking a model.
//
// The split matters. The conversation, the interval, and how many messages the
// slice held are facts a pane already carries, so paying a model to restate
// them would be paying for something already known — and getting it wrong
// occasionally.
func entryBody(pane stream.Pane, conversation, digest string) string {
	var b strings.Builder
	b.WriteString("[")
	if conversation != "" {
		b.WriteString(conversation)
		b.WriteString(" · ")
	}
	b.WriteString(paneSpan(pane))
	fmt.Fprintf(&b, " · %d %s]\n", pane.Count, plural(pane.Count, "message"))
	b.WriteString(strings.TrimSpace(digest))
	return b.String()
}

// paneSpan renders a pane's interval compactly, and a global window as the
// watermark it fired at.
func paneSpan(pane stream.Pane) string {
	w := pane.Window
	if w.Global() {
		return pane.Watermark.UTC().Format(time.RFC3339)
	}
	start, end := w.Start.UTC(), w.End.UTC()
	if start.YearDay() == end.YearDay() && start.Year() == end.Year() {
		return start.Format("2006-01-02 15:04") + "–" + end.Format("15:04")
	}
	return start.Format(time.RFC3339) + "–" + end.Format(time.RFC3339)
}

// withDefaults fills a partially-specified Spec from DefaultSpec, field by
// field, so replacing one operation does not mean restating the other three.
//
// The unit of substitution is a whole spec rather than its fields: an
// InferSpec whose Prompt was replaced and whose System was not is a prompt
// answering a different instruction, which is a subtler thing to debug than a
// spec that was simply not filled in.
func withDefaults(s Spec) Spec {
	def := DefaultSpec()
	if s.Read.Prompt == "" {
		s.Read = def.Read
	}
	if s.Digest.Prompt == "" {
		s.Digest = def.Digest
	}
	if s.Compact.Prompt == "" {
		s.Compact = def.Compact
	}
	if s.Answer.Prompt == "" {
		s.Answer = def.Answer
	}
	return s
}

// plural is the difference between a context that reads like prose and one that
// reads like a log line, which matters here because a model reads it.
func plural(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}
