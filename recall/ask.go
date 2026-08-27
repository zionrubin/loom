package recall

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/task"
)

// contextKey is the continuation key a query's Answer stage reads the
// maintained context through. It is per desk, so two desks on one fleet are two
// contexts rather than one.
func (d *Desk) contextKey() string { return "recall/" + d.name }

// Question is one turn of a conversation with the desk.
type Question struct {
	// Session threads questions together. Two questions with the same session
	// share history, so "and what about the other one?" means something. Empty
	// is a one-shot question with no history.
	Session string `json:"session,omitempty"`
	// Text is the question.
	Text string `json:"text"`
	// MinVersion refuses to answer against a context older than this, waiting
	// for ingestion to catch up instead.
	//
	// It is how a caller buys back the coupling the design gave away: a desk is
	// decoupled by default, and a question that must reflect a conversation
	// just posted says so here rather than hoping. Zero answers against
	// whatever is published now, which is what a serving layer should do.
	MinVersion int64 `json:"min_version,omitempty"`
	// Wait bounds how long MinVersion may block (default: until the question's
	// context ends).
	Wait time.Duration `json:"wait,omitempty"`
	// Fresh is MinVersion for callers who do not track versions: answer only
	// against a context updated within this long, waiting if it is staler.
	// It is a weaker request than MinVersion — a quiet feed produces no
	// updates, and a desk with nothing to fold in cannot become fresher — so
	// it gives up when Wait elapses and answers against what it has, saying so
	// in Answer.Stale.
	Fresh time.Duration `json:"fresh,omitempty"`
}

// Answer is what the desk says, and what it said it from.
//
// The provenance fields are not decoration. A serving layer whose read path is
// decoupled from its write path is answering from a snapshot, and the only
// honest way to present that is to say which snapshot: Version names it, Behind
// says how many ingested messages it does not yet cover, and Stale says how
// long ago it was published.
type Answer struct {
	// Text is the answer.
	Text string `json:"text"`
	// Session and Turn locate this answer in a conversation with the desk.
	Session string `json:"session,omitempty"`
	Turn    int    `json:"turn"`
	// Version is the context version the answer was computed against, and Ref
	// the revision that names it.
	Version int64  `json:"version"`
	Ref     string `json:"ref,omitempty"`
	// Entries and Bytes describe the context that was read.
	Entries int `json:"entries"`
	Bytes   int `json:"bytes"`
	// Behind is how many ingested messages had not been folded into that
	// context when the question was answered — the staleness the decoupling
	// buys, stated rather than hidden. Zero means the answer covers everything
	// the desk had been given.
	Behind int64 `json:"behind"`
	// Stale is how long before the answer the context was last published.
	Stale time.Duration `json:"stale"`
	// Waited is how long the question blocked for a fresher context.
	Waited time.Duration `json:"waited,omitempty"`
	// Empty marks an answer given without a model call because the desk has
	// ingested nothing yet.
	Empty bool `json:"empty,omitempty"`
	// Cached marks an answer served from the result cache: the same question
	// against the same context revision, which costs nothing and is exactly as
	// current as the answer it repeats.
	Cached bool `json:"cached,omitempty"`
	// Usage is what this question cost.
	Usage core.Usage `json:"usage"`
	// Latency is how long the question took, including any wait.
	Latency time.Duration `json:"latency"`
}

// String renders the answer with its provenance, for a terminal.
func (a Answer) String() string {
	var b strings.Builder
	b.WriteString(a.Text)
	b.WriteString(fmt.Sprintf("\n— v%d · %d entries · %s",
		a.Version, a.Entries, humanBytes(a.Bytes)))
	if a.Behind > 0 {
		fmt.Fprintf(&b, " · %d messages behind", a.Behind)
	}
	switch {
	case a.Empty:
		b.WriteString(" · empty")
	case a.Cached:
		b.WriteString(" · cached")
	}
	return b.String()
}

// Ask puts a question to the desk and answers it against the maintained
// context.
//
// The cost of an answer is one model call over a context bounded by
// Config.MaxBytes, and it does not grow with how much conversation the desk has
// ingested — that is the property the whole package is arranged to produce.
// Asking the same question again against an unchanged context costs nothing at
// all: the context's revision joins the stage fingerprint, so the second
// question is a cache hit, and the moment a conversation lands the revision
// moves and the next answer is computed afresh.
func (d *Desk) Ask(ctx context.Context, q Question) (Answer, error) {
	started := time.Now()
	if strings.TrimSpace(q.Text) == "" {
		return Answer{}, fmt.Errorf("recall: empty question")
	}
	d.queries.Add(1)

	view, waited, err := d.viewFor(ctx, q)
	if err != nil {
		return Answer{}, err
	}

	ans := Answer{
		Session: q.Session, Version: view.Version, Ref: view.Ref.Hash,
		Entries: view.Entries, Bytes: view.Bytes, Waited: waited,
	}
	if !view.Updated.IsZero() {
		ans.Stale = started.Add(waited).Sub(view.Updated)
	}
	if d.feed != nil {
		// What was given to the desk, less what has been accounted for: folded
		// into this view, or dropped as not worth remembering.
		if behind := d.feed.Posted() - view.Messages - d.dropped.Load(); behind > 0 {
			ans.Behind = behind
		}
	}

	// A desk that has ingested nothing has nothing to answer from, and asking a
	// model to answer from an empty context invites it to answer from something
	// else. Saying so costs nothing and is true.
	if view.Entries == 0 && !view.Brief {
		ans.Empty = true
		ans.Text = "The desk has not digested any conversations yet."
		ans.Latency = time.Since(started)
		return ans, nil
	}

	sess := d.session(q.Session)
	history := sess.render(d.turns)

	res, err := d.answer(ctx, view, history, q.Text)
	if err != nil {
		return ans, err
	}
	out := d.spec.Answer.OutputField
	if out == "" {
		out = "output"
	}
	if len(res.Output) == 0 {
		return ans, fmt.Errorf("recall: the answer stage produced no record")
	}
	ans.Text = strings.TrimSpace(res.Output[0].String(out))
	ans.Usage = res.Report.Totals()
	ans.Cached = cacheOnly(res.Report)
	if ans.Cached {
		d.served.Add(1)
	}
	d.answered.Add(1)

	ans.Turn = sess.append(q.Text, ans.Text)
	ans.Latency = time.Since(started)
	return ans, nil
}

// viewFor resolves which context version a question is answered against,
// waiting when the question asked for one fresher than the desk has published.
func (d *Desk) viewFor(ctx context.Context, q Question) (View, time.Duration, error) {
	view := d.curator.View()
	if q.MinVersion <= view.Version && (q.Fresh <= 0 || view.Fresh(q.Fresh)) {
		return view, 0, nil
	}

	d.waited.Add(1)
	started := time.Now()
	waitCtx := ctx
	if q.Wait > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, q.Wait)
		defer cancel()
	}

	for {
		view = d.curator.View()
		if q.MinVersion <= view.Version && (q.Fresh <= 0 || view.Fresh(q.Fresh)) {
			return view, time.Since(started), nil
		}
		updated := d.curator.updates()
		select {
		case <-updated:
		case <-waitCtx.Done():
			// A deadline that expires while waiting for freshness is not a
			// failure: the caller asked for the best available and gets it,
			// with Answer.Stale saying what "available" turned out to mean.
			// A version that will never arrive is a failure, because answering
			// against an older one is the thing MinVersion ruled out.
			view = d.curator.View()
			if q.MinVersion > view.Version {
				return view, time.Since(started), fmt.Errorf(
					"recall: context is at v%d, question asked for v%d: %w",
					view.Version, q.MinVersion, waitCtx.Err())
			}
			return view, time.Since(started), nil
		}
	}
}

// answer runs the query as an agent on the desk's fleet.
//
// It is a one-record pipeline, and the interesting part is what it does not
// carry: the context does not travel in the prompt template or the record, it
// travels as the revision hash of a continuation the stage declares. The
// executor materializes it — from a state this process already holds, by
// splicing, or from the chain if it holds nothing — and places it at the front
// of the prompt, where a provider's prefix cache can serve it across every
// query answered against the same revision.
func (d *Desk) answer(ctx context.Context, view View, history, question string) (*loom.RunResult, error) {
	rec := core.NewRecord(
		// A stable record ID, because the cache key is the fingerprint and the
		// input content: a random one here would mean no two identical
		// questions ever shared an answer.
		"question",
		map[string]any{"question": question, "history": history},
	)

	p := pipeline.New("recall/" + d.name + "/ask")
	src := p.FromRecords("question", []core.Record{rec})

	spec := d.spec.Answer
	var opts []pipeline.Option
	if !view.Ref.Zero() {
		opts = append(opts, pipeline.WithContinuation(d.contextKey()))
	} else {
		// No revision to reference — a desk restored from a chain it could not
		// resolve. The context still has to reach the model, so it goes inline
		// as a fragment: the same bytes, shipped rather than referenced.
		//
		// Built fresh rather than appended to, because spec is a copy of the
		// desk's whose Context slice is not: appending into spare capacity
		// would write into the shared one.
		spec.Context = append(append([]task.Fragment{}, spec.Context...),
			task.Fragment{Name: "context", Content: view.Text})
	}
	src.Infer("answer", spec, opts...)

	var runOpts []loom.Option
	if !view.Ref.Zero() {
		runOpts = append(runOpts, loom.WithContinuation(d.contextKey(), view.Ref))
	}
	return d.fleet.Run(ctx, p, runOpts...)
}

// cacheOnly reports whether every task in the run was served from the result
// cache — for a one-record query, that the same question was already answered
// against this same context revision.
func cacheOnly(rep observe.RunReport) bool {
	var tasks, hits int
	for _, s := range rep.Stages {
		tasks += s.Tasks
		hits += s.CacheHits
	}
	return tasks > 0 && hits == tasks
}

// --- sessions ------------------------------------------------------------

// Turn is one exchange with the desk: what was asked, what was answered, and
// when.
type Turn struct {
	Q  string    `json:"q"`
	A  string    `json:"a"`
	At time.Time `json:"at"`
}

// sessionState is one conversational thread with the desk.
//
// A session holds what was asked and answered, and nothing about the
// conversations themselves — those live in the context, which every session
// shares. That split is why a desk can hold many sessions cheaply: a session is
// a few turns of text, not a copy of what the desk knows.
type sessionState struct {
	mu    sync.Mutex
	turns []Turn
}

// append records an exchange and returns its turn number.
func (s *sessionState) append(q, a string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.turns = append(s.turns, Turn{Q: q, A: a, At: time.Now().UTC()})
	// Bounded because the prompt only ever carries the last few and the rest is
	// memory nobody reads.
	if len(s.turns) > maxSessionTurns {
		s.turns = s.turns[len(s.turns)-maxSessionTurns:]
	}
	return len(s.turns)
}

// render writes the last n turns as the history the next question carries.
func (s *sessionState) render(n int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n <= 0 || len(s.turns) == 0 {
		return ""
	}
	from := max(0, len(s.turns)-n)
	var b strings.Builder
	b.WriteString("Earlier in this conversation:\n")
	for _, t := range s.turns[from:] {
		fmt.Fprintf(&b, "Q: %s\nA: %s\n", t.Q, t.A)
	}
	b.WriteByte('\n')
	return b.String()
}

// Turns returns a session's exchanges, oldest first.
func (d *Desk) Turns(session string) []Turn {
	s := d.session(session)
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Turn(nil), s.turns...)
}

// Forget drops a session's history. The context is untouched — a session is
// what was asked, not what is known.
func (d *Desk) Forget(session string) {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()
	delete(d.sessions, session)
	for i, id := range d.sessLRU {
		if id == session {
			d.sessLRU = append(d.sessLRU[:i], d.sessLRU[i+1:]...)
			break
		}
	}
}

// maxSessionTurns bounds one session's retained history.
const maxSessionTurns = 64

// session returns a session's state, creating it and evicting the least
// recently used one when the desk is holding as many as it may.
func (d *Desk) session(id string) *sessionState {
	if id == "" {
		return &sessionState{} // a one-shot question remembers nothing
	}
	d.sessMu.Lock()
	defer d.sessMu.Unlock()

	if s, ok := d.sessions[id]; ok {
		d.touchLocked(id)
		return s
	}
	s := &sessionState{}
	d.sessions[id] = s
	d.sessLRU = append(d.sessLRU, id)
	for len(d.sessLRU) > d.maxSess {
		delete(d.sessions, d.sessLRU[0])
		d.sessLRU = d.sessLRU[1:]
	}
	return s
}

func (d *Desk) touchLocked(id string) {
	for i, s := range d.sessLRU {
		if s == id {
			d.sessLRU = append(append(d.sessLRU[:i:i], d.sessLRU[i+1:]...), id)
			return
		}
	}
}

// Sessions lists the query threads the desk is holding, most recent last.
func (d *Desk) Sessions() []string {
	d.sessMu.Lock()
	defer d.sessMu.Unlock()
	return append([]string(nil), d.sessLRU...)
}
