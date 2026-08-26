// Package recall is Loom's serving layer: conversations arrive on one side,
// questions arrive on the other, and between them sits a context that is
// maintained rather than rebuilt.
//
// # The problem it is arranged around
//
// A body of conversation that keeps growing is easy to store and expensive to
// use. The usual answer is to do the work at query time — retrieve, rerank,
// stuff a prompt, summarize — which puts the cost of understanding on the
// critical path of every question, pays it again for every question, and makes
// the answer only as good as what one retrieval happened to surface.
//
// A desk inverts that. The expensive work of understanding happens once, when a
// message arrives, on the ingest side. What it produces is a compact standing
// context, maintained continuously, which a query reads as it stands:
//
//	conversations → ingest → read · slice · digest → living context → answers
//	                └──────── continuous, expensive ────────┘ └── cheap ──┘
//
// The two sides are decoupled by a published revision. Ingestion appends to an
// immutable chain and swaps a pointer; a query loads the pointer and answers
// against the revision it names. Neither blocks the other, no query sees a
// half-applied update, and a query's cost does not grow with the history behind
// it — only with the size of the context, which is a budget rather than an
// accumulation.
//
// # What each half is made of
//
// Neither half is new machinery. The ingest side is loom.Stream: a stream
// source, a per-message Infer, a window that cuts each conversation into slices
// on event time, and a ReduceAI that folds a slice into one line. The living
// context is a delta.Chain — the same evolving-context mechanism a long agent
// session uses — so appending a line writes one revision, a query carries a
// couple of hundred bytes rather than the context, and a process that already
// materialized the previous revision splices onto it instead of re-reading the
// whole thing. And a query is an ordinary Infer stage that declares that chain
// as its continuation, which puts the maintained context at the front of its
// prompt where the provider's prefix cache can serve it.
//
// Three properties fall out of that arrangement rather than being implemented:
//
//   - Asking the same question twice against an unchanged context costs
//     nothing. The continuation's revision joins the stage fingerprint, so the
//     second query is a cache hit — and the moment a conversation lands, the
//     revision moves and the next answer is computed again.
//   - Queries do not queue behind ingestion. Both run as agents on one fleet,
//     and the pool admits by attained service: a query that has been served
//     nothing takes the next contended slot from an ingest job that has been
//     running for an hour.
//   - Losing the process costs latency, not answers. The chain is in the
//     content-addressed store and the desk's pointer is a small file beside it,
//     so a restart resumes the context; and a context that cannot be resumed
//     rebuilds from the chain rather than from the conversations.
//
// # What it costs
//
// Honestly: one model call per message, one per conversation slice, one per
// budget's worth of entries, and one per question. The first of those is the
// bill, and it is the deliberate one — it is what buys a query that does not
// pay for the history. A desk over a feed that is mostly noise should say so in
// Spec.Read, because the cheapest way to keep a context small is to keep things
// out of it, and the Keep stage drops what Read judged not worth remembering
// before it costs anything downstream.
//
// # Using it
//
//	desk, err := recall.Open(recall.Config{Name: "support", Every: time.Minute},
//	    loom.WithRegistry(reg),
//	    loom.WithStateDir("./state"),
//	    loom.WithFleetBudget(core.Budget{MaxCostUSD: 20}))
//	defer desk.Close()
//
//	desk.Start(ctx)                                  // ingestion runs from here
//	n, _ := desk.Post(ctx, messages...)              // conversations arrive
//	desk.Await(ctx, n)                               // read-your-writes, when wanted
//
//	ans, _ := desk.Ask(ctx, recall.Question{
//	    Session: "sam", Text: "what is Acme blocked on?"})
//	fmt.Println(ans.Text, ans.Version, ans.Behind)
//
// Desk.Handler serves the same thing over HTTP, including a console to talk to
// it in.
package recall

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/delta"
	"github.com/zionrubin/loom/stream"
)

// Defaults for a desk. They describe a conversational feed watched by a person:
// slices short enough that a thread becomes queryable while it is still
// happening, a context small enough to sit in front of every question, and a
// grace period for out-of-order delivery measured in seconds.
const (
	DefaultEvery    = time.Minute
	DefaultSlice    = 8
	DefaultLateness = 5 * time.Second
	DefaultMaxBytes = 32 << 10
	DefaultKeep     = 24
	// DefaultIdle is how long the feed may be silent before it stops holding
	// the watermark back. It is short because a desk's windows are cut on event
	// time and a quiet feed would otherwise leave the last slice of every
	// conversation open — queryable only once somebody said something else.
	DefaultIdle = 2 * time.Second
	// DefaultQuiet is how long the in-process feed may be silent before it
	// declares event time complete up to the present.
	DefaultQuiet = 2 * time.Second
	// DefaultTurns is how many turns of a query session travel with the next
	// question.
	DefaultTurns = 6
	// DefaultSessions bounds how many query sessions a desk remembers.
	DefaultSessions = 512
	// feedSplits is how many partitions an in-process feed is read through.
	// Conversations hash onto them, so it bounds how many can be read in
	// parallel and is why one long thread does not hold up the rest.
	feedSplits = 4
)

// Config describes a desk. Everything in it has a working default except the
// name, and the name only matters for a desk that will be restarted.
type Config struct {
	// Name identifies this desk. It is the ingest job's ID and the chain's
	// key, which together are what a restart resumes: the same name with the
	// same state directory picks the context back up, a different one starts
	// cold.
	Name string

	// Source is where conversations arrive from. Nil provisions an in-process
	// Feed, reachable through Desk.Post and Desk.Feed.
	//
	// Any stream.Source works, and a deployed desk should generally use one
	// that is durable — a directory of JSONL (stream/file), a topic
	// (stream/kafka) — because a Feed is memory and a desk that restarts finds
	// it empty.
	Source stream.Source

	// Every is how long a slice of one conversation covers (default one
	// minute). It is the shape of the bill as much as of the context: each
	// conversation active in each interval is one aggregation call and one
	// entry.
	Every time.Duration
	// Slice is how many messages fill a slice before it is digested early,
	// without waiting for its interval to end (default 8). The pane purges, so
	// entries stay disjoint: a message is digested once however many times its
	// window fires.
	Slice int
	// Lateness is the feed's bounded out-of-orderness (default 5s).
	Lateness time.Duration
	// Quiet is how long the in-process feed may be silent before it declares
	// event time complete up to the present, closing every window whose end has
	// passed (default 2s; negative disables it).
	//
	// It is what makes the last slice of a conversation queryable without
	// waiting for the next one. See Feed.Post for the claim it makes and when
	// that claim is not a desk's to make. It applies only to the in-process
	// feed: a bound Config.Source keeps whatever watermark behaviour it has.
	Quiet time.Duration

	// MaxBytes is the living context's budget, rendered (default 32 KiB).
	// Past it the oldest entries are folded into a standing brief.
	MaxBytes int
	// Keep is how many recent entries survive a compaction verbatim (default
	// 24). It is the dial between detail and summary: the newest Keep slices
	// are quoted, everything older is briefed.
	Keep int

	// Spec is the four model operations (default DefaultSpec).
	Spec Spec

	// Fleet runs the desk's agents. Nil provisions one from the options passed
	// to Open, and closes it in Desk.Close; supplying one puts the desk on a
	// fleet it shares with other work and leaves its lifetime to the caller.
	//
	// The options still matter when a fleet is supplied: they are how Open
	// learns where state lives. Pass the same loom.WithStateDir the fleet was
	// built with, or the chain will be durable and the pointer into it will
	// not, and the desk will start cold every time.
	Fleet *loom.Fleet

	// Limit stops ingestion once a bound is reached, which is what makes an
	// endless desk a test.
	Limit stream.Limit

	// Turns is how many turns of a query session travel with the next question
	// (default 6), and Sessions how many sessions the desk remembers (default
	// 512).
	Turns    int
	Sessions int

	// OnError receives the errors a desk absorbs rather than returns: a pane
	// that could not be appended, a compaction that failed, a state file that
	// could not be written. Nil keeps the most recent few, readable through
	// Desk.Errors.
	OnError func(error)
}

// Desk is a serving layer over a conversation feed: the ingest side, the
// maintained context, and the query side, with one fleet underneath all three.
//
// It is safe for concurrent use, and deliberately so on both sides at once —
// serving queries while ingesting is the entire point.
type Desk struct {
	name     string
	every    time.Duration
	slice    int
	lateness time.Duration
	maxBytes int
	keep     int
	turns    int
	spec     Spec
	limit    stream.Limit
	stateDir string
	renderer delta.Renderer

	fleet    *loom.Fleet
	ownFleet bool
	source   stream.Source
	feed     *Feed
	curator  *curator

	startOnce sync.Once
	started   atomic.Bool
	cancel    context.CancelFunc
	ingest    *loom.StreamAgent
	stopped   chan struct{}

	onError func(error)
	errMu   sync.Mutex
	errs    []error

	sessMu   sync.Mutex
	sessions map[string]*sessionState
	sessLRU  []string
	maxSess  int

	queries  atomic.Int64
	served   atomic.Int64
	answered atomic.Int64
	waited   atomic.Int64
	// dropped counts the messages the Keep stage discarded. Together with the
	// view's digested count it is what "everything posted has been processed"
	// means, since a dropped message reaches nothing that could account for it.
	dropped atomic.Int64
}

// Open provisions a desk. The options are the fleet's — registry, budget,
// secrets, state directory, tools, event handler — and are ignored when
// Config.Fleet supplies a fleet that already has them.
//
// Nothing runs until Start.
func Open(cfg Config, opts ...loom.Option) (*Desk, error) {
	if cfg.Name == "" {
		cfg.Name = "desk"
	}
	// The name becomes a filename and a chain key, so it is checked here rather
	// than discovered as a state file written somewhere surprising.
	if !simpleName(cfg.Name) {
		return nil, fmt.Errorf("recall: desk name %q must be letters, digits, "+
			"'-', '_' or '.', and not '.' or '..'", cfg.Name)
	}

	// The fleet's own settings are read back out of the options rather than
	// duplicated in Config, so there is one place to say where state lives and
	// one place a mismatch cannot happen.
	var probe loom.Config
	for _, o := range opts {
		o(&probe)
	}

	d := &Desk{
		name:     cfg.Name,
		every:    orDuration(cfg.Every, DefaultEvery),
		slice:    orInt(cfg.Slice, DefaultSlice),
		lateness: orDuration(cfg.Lateness, DefaultLateness),
		maxBytes: orInt(cfg.MaxBytes, DefaultMaxBytes),
		keep:     orInt(cfg.Keep, DefaultKeep),
		turns:    orInt(cfg.Turns, DefaultTurns),
		spec:     cfg.Spec,
		limit:    cfg.Limit,
		stateDir: probe.StateDir,
		renderer: probe.DeltaRenderer,
		source:   cfg.Source,
		onError:  cfg.OnError,
		sessions: map[string]*sessionState{},
		maxSess:  orInt(cfg.Sessions, DefaultSessions),
		stopped:  make(chan struct{}),
	}
	if d.renderer == nil {
		d.renderer = delta.Tags{}
	}
	d.spec = withDefaults(cfg.Spec)
	if err := d.validate(); err != nil {
		return nil, err
	}
	if d.source == nil {
		quiet := DefaultQuiet
		if cfg.Quiet != 0 {
			quiet = max(cfg.Quiet, 0)
		}
		d.feed = NewFeed(feedSplits, quiet)
		d.source = d.feed
	}

	d.fleet = cfg.Fleet
	if d.fleet == nil {
		f, err := loom.NewFleet(opts...)
		if err != nil {
			return nil, err
		}
		d.fleet, d.ownFleet = f, true
	}

	// The chain lives on the fleet's store, which is what makes the context a
	// reference rather than a copy: the curator writes revisions here and every
	// agent that reads one — a query on this fleet, a worker process sharing
	// the state directory — resolves it by hash.
	chain, err := d.fleet.Chain("recall/" + d.name)
	if err != nil {
		d.closeFleet()
		return nil, fmt.Errorf("recall: open chain: %w", err)
	}
	d.curator = newCurator(d, chain)
	if _, err := d.curator.restore(); err != nil {
		d.noteError(err)
	}
	return d, nil
}

// validate refuses the configurations that cannot mean anything, before a
// source is opened or a model is called.
func (d *Desk) validate() error {
	switch {
	case d.every <= 0:
		return fmt.Errorf("recall: Every must be positive, got %s", d.every)
	case d.slice <= 0:
		return fmt.Errorf("recall: Slice must be positive, got %d", d.slice)
	case d.maxBytes < 0:
		return fmt.Errorf("recall: MaxBytes must not be negative, got %d", d.maxBytes)
	case d.keep < 0:
		return fmt.Errorf("recall: Keep must not be negative, got %d", d.keep)
	}
	if d.spec.Answer.Prompt == "" {
		return errors.New("recall: Spec.Answer has no prompt template")
	}
	return nil
}

// Start begins ingestion and returns immediately.
//
// The desk runs until ctx is cancelled or Close is called; both stop the ingest
// job and the curator, and neither disturbs a query already in flight. Calling
// it twice is a no-op, so a caller that is not sure whether the desk is running
// may say so again.
func (d *Desk) Start(ctx context.Context) error {
	var err error
	d.startOnce.Do(func() { err = d.start(ctx) })
	return err
}

func (d *Desk) start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	d.cancel = cancel

	go d.curator.run(ctx)

	opts := []loom.Option{
		loom.WithSource(StageMessages, d.source),
		loom.WithSink(StageDigest, &sink{desk: d, ctx: ctx}),
		loom.WithJobID("recall/" + d.name),
		loom.WithLateness(d.lateness),
		// A desk's feed goes quiet between bursts, and a quiet split holds
		// every window open. Releasing it quickly is what makes the last slice
		// of a conversation queryable without waiting for the next one.
		loom.WithIdleTimeout(DefaultIdle),
		loom.WithStreamLimit(d.limit),
	}

	d.started.Store(true)
	d.ingest = d.fleet.Stream(ctx, d.ingestPipeline(), opts...)

	go func() {
		defer close(d.stopped)
		if _, err := d.ingest.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			d.noteError(fmt.Errorf("recall: ingest: %w", err))
		}
	}()
	return nil
}

// Wait blocks until ingestion has stopped and returns its report.
//
// A desk over a live feed stops when its context is cancelled, so this is what
// a process waits on before exiting rather than a completion to expect.
func (d *Desk) Wait() (*loom.StreamResult, error) {
	if !d.started.Load() {
		return nil, errors.New("recall: desk not started")
	}
	<-d.stopped
	return d.ingest.Wait()
}

// Close stops the desk and releases what it owns.
//
// A desk given a fleet does not close it — the caller who supplied it owns its
// lifetime — and a desk that provisioned its own does.
func (d *Desk) Close() error {
	if d.cancel != nil {
		d.cancel()
		<-d.stopped
	}
	if d.feed != nil {
		_ = d.feed.Close()
	}
	return d.closeFleet()
}

func (d *Desk) closeFleet() error {
	if d.ownFleet && d.fleet != nil {
		return d.fleet.Close()
	}
	return nil
}

// --- ingestion -----------------------------------------------------------

// ErrNoFeed is returned by Post on a desk bound to a source of its own.
var ErrNoFeed = errors.New("recall: this desk reads a bound source, not an in-process feed " +
	"(write to that source, or leave Config.Source nil)")

// Post hands messages to the desk's feed and returns how many messages the
// feed has accepted in its whole life.
//
// That number is the read-your-writes handle: Await(ctx, n) returns once every
// message counted by n has been folded into the context. Post itself does not
// wait for any of that — it is the write side, and it returns as soon as the
// messages are on the feed.
func (d *Desk) Post(ctx context.Context, msgs ...Message) (int64, error) {
	if d.feed == nil {
		return 0, ErrNoFeed
	}
	if err := ctx.Err(); err != nil {
		return d.feed.Posted(), err
	}
	return d.feed.Post(msgs...)
}

// Ingest posts whole conversations, filling in the conversation ID and
// metadata each message inherits.
func (d *Desk) Ingest(ctx context.Context, convs ...Conversation) (int64, error) {
	var msgs []Message
	for _, c := range convs {
		msgs = append(msgs, c.normalize()...)
	}
	return d.Post(ctx, msgs...)
}

// Feed returns the desk's in-process feed, or nil for a desk reading a bound
// source. Closing it ends ingestion the way a finished source does: the splits
// retire, the open windows drain, and the last entries reach the context.
func (d *Desk) Feed() *Feed { return d.feed }

// Await blocks until the first n posted messages have been accounted for, and
// returns the view that covers them.
//
// Accounted for means each one either reached the context or was dropped as not
// worth remembering — the two things that can become of a message, counted so
// that a caller waiting on a greeting is not waiting on something that will
// never arrive.
//
// It is the one place a caller may deliberately couple the two sides, and it
// exists because "post, then ask" is a thing people do and a decoupled system
// would otherwise answer from before the post. Everything it waits for is real
// work — the slice has to close, the digest has to run — so a desk whose
// windows are minutes long waits minutes. Passing a context with a deadline is
// the way to bound that.
//
// The count it waits on is of messages processed, so a source that redelivers
// after a restart can satisfy it with a message it has already seen. That is
// exact for the in-process Feed, which cannot redeliver, and best-effort for a
// durable source, which can.
func (d *Desk) Await(ctx context.Context, n int64) (View, error) {
	for {
		if v, ok := d.settled(n); ok {
			return v, nil
		}
		updated := d.curator.updates()
		select {
		case <-updated:
		case <-time.After(pollInterval):
			// A message dropped as noise moves the count without publishing a
			// revision, so a wait that only woke on publication would sleep
			// through a burst of pure small talk.
		case <-ctx.Done():
			return d.curator.View(), ctx.Err()
		case <-d.stopped:
			if v, ok := d.settled(n); ok {
				return v, nil
			}
			return d.curator.View(), errors.New(
				"recall: ingestion stopped before those messages were processed")
		}
	}
}

// settled reports whether n posted messages have been accounted for.
func (d *Desk) settled(n int64) (View, bool) {
	v := d.curator.View()
	return v, v.Messages+d.dropped.Load() >= n
}

// pollInterval is how often a wait re-checks a count that can move without a
// revision being published.
const pollInterval = 25 * time.Millisecond

// AwaitVersion blocks until the context has reached at least version v.
func (d *Desk) AwaitVersion(ctx context.Context, version int64) (View, error) {
	for {
		v := d.curator.View()
		if v.Version >= version {
			return v, nil
		}
		updated := d.curator.updates()
		select {
		case <-updated:
		case <-ctx.Done():
			return d.curator.View(), ctx.Err()
		}
	}
}

// --- reading -------------------------------------------------------------

// Context returns the maintained context as it stands: the living prompt, the
// revision that names it, and what it covers.
//
// It is a plain atomic load. A caller may take it as often as it likes without
// slowing ingestion down, which is what makes it reasonable to put on an HTTP
// endpoint.
func (d *Desk) Context() View { return d.curator.View() }

// Prompt returns just the rendered context — the bytes a model receives.
func (d *Desk) Prompt() string { return d.curator.View().Text }

// Stats is what a desk reports about itself.
type Stats struct {
	Name string `json:"name"`
	// Running reports whether ingestion is live.
	Running bool `json:"running"`
	// View is the published context.
	View View `json:"view"`
	// Posted is how many messages have reached the feed, Dropped how many the
	// Keep stage judged not worth remembering, and Pending how many digested
	// slices are queued for the curator.
	//
	// Posted minus Dropped minus View.Messages is the honest measure of how far
	// the query side is behind the write side: messages that have been given to
	// the desk and are not yet reflected in anything a question would read.
	Posted  int64 `json:"posted"`
	Dropped int64 `json:"dropped"`
	Pending int64 `json:"pending"`
	// Queries is how many questions have been asked, Cached how many were
	// answered without a model call because neither the question nor the
	// context had changed, and Waited how many blocked for a fresher context.
	Queries int64 `json:"queries"`
	Cached  int64 `json:"cached"`
	Waited  int64 `json:"waited"`
	// Sessions is how many query threads the desk is holding.
	Sessions int `json:"sessions"`
	// Spent is what the fleet has spent across ingestion, compaction and
	// answers together.
	Spent core.Usage `json:"spent"`
	// Errors is how many problems the desk absorbed rather than returned.
	Errors int `json:"errors"`
}

// Stats snapshots the desk.
func (d *Desk) Stats() Stats {
	d.sessMu.Lock()
	sessions := len(d.sessions)
	d.sessMu.Unlock()

	d.errMu.Lock()
	errs := len(d.errs)
	d.errMu.Unlock()

	var posted int64
	if d.feed != nil {
		posted = d.feed.Posted()
	}
	return Stats{
		Name: d.name, Running: d.started.Load() && !d.isStopped(),
		View: d.curator.View(), Posted: posted,
		Dropped:  d.dropped.Load(),
		Pending:  d.curator.pending.Load(),
		Queries:  d.queries.Load(),
		Cached:   d.served.Load(),
		Waited:   d.waited.Load(),
		Sessions: sessions, Spent: d.fleet.Spent(), Errors: errs,
	}
}

// Report returns the fleet's report: one row per agent, the ingest job among
// them, and what the whole desk has spent.
func (d *Desk) Report() loom.FleetReport { return d.fleet.Report() }

// Fleet returns the fleet the desk's agents run on.
func (d *Desk) Fleet() *loom.Fleet { return d.fleet }

// Updates returns a channel closed the next time the context is published. It
// is what a live view polls on instead of a timer.
func (d *Desk) Updates() <-chan struct{} { return d.curator.updates() }

func (d *Desk) isStopped() bool {
	select {
	case <-d.stopped:
		return true
	default:
		return false
	}
}

// --- errors --------------------------------------------------------------

// maxKeptErrors bounds the ring of absorbed errors.
const maxKeptErrors = 16

// noteError records a problem the desk absorbed. A desk keeps serving through
// these — a pane that failed to append is a missing entry, not a broken desk —
// so they are reported rather than returned, and a caller who wants them
// promptly supplies Config.OnError.
func (d *Desk) noteError(err error) {
	if err == nil {
		return
	}
	if d.onError != nil {
		d.onError(err)
		return
	}
	d.errMu.Lock()
	defer d.errMu.Unlock()
	d.errs = append(d.errs, err)
	if len(d.errs) > maxKeptErrors {
		d.errs = d.errs[len(d.errs)-maxKeptErrors:]
	}
}

// Errors returns the problems the desk absorbed, oldest first.
func (d *Desk) Errors() []error {
	d.errMu.Lock()
	defer d.errMu.Unlock()
	return append([]error(nil), d.errs...)
}

// --- small helpers -------------------------------------------------------

func orInt(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

func orDuration(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}

// humanBytes renders a byte count the way a report should read.
func humanBytes(n int) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%d B", n)
	case n < 1024*1024:
		return fmt.Sprintf("%.1f KiB", float64(n)/1024)
	default:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1024*1024))
	}
}

// simpleName reports whether a desk's name is safe to use as a path element.
func simpleName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}
