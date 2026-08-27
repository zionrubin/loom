package recall

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/delta"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/stream"
)

// Segment names in the maintained context. They are the tags a Tags-rendered
// context carries, so they are what the model actually reads.
const (
	segBrief = "brief"
	segEntry = "entry"
)

// View is the maintained context at one moment: an immutable, self-consistent
// snapshot that a query reads without coordinating with ingestion.
//
// It is the whole of the handoff between the two sides of a desk, and it is a
// value rather than a handle for exactly that reason. Ingestion publishes a new
// one by swapping a pointer; a query takes the current one and holds it for the
// duration of an answer. Neither waits for the other, and no query ever sees a
// context halfway through an update — the alternative, a lock held across a
// model call, would be a serving layer whose read latency was set by its write
// path.
type View struct {
	// Version is the desk's monotonic context version, incremented once per
	// published revision. It is what Question.MinVersion names and what an
	// answer reports having been computed against.
	Version int64 `json:"version"`
	// Ref addresses this revision in the fleet's content-addressed store. It
	// is what a query carries — a couple of hundred bytes — instead of the
	// context itself.
	Ref delta.Ref `json:"ref"`
	// Text is the rendered context: the living prompt, exactly as a model
	// receives it.
	//
	// Rendering it here costs a pass over a context bounded by Config.MaxBytes,
	// once per published revision, and it is deliberately the only place that
	// pass happens. A query does not read this field: it carries Ref, and the
	// executor materializes the same bytes by splicing onto the revision it
	// already holds. This copy exists so that a human, an HTTP client, or a
	// test can read what the model reads.
	Text string `json:"text"`
	// Entries is how many conversation slices the context holds verbatim, and
	// Brief reports whether a compacted summary stands in front of them.
	Entries int  `json:"entries"`
	Brief   bool `json:"brief"`
	// Bytes is the rendered size — the budget Config.MaxBytes governs.
	Bytes int `json:"bytes"`
	// Messages is how many ingested messages this revision was built from, and
	// Conversations how many distinct threads it covers. Messages the Keep
	// stage dropped are not among them — nothing in the context came from one —
	// so this is smaller than what was posted by exactly the noise.
	Messages      int64 `json:"messages"`
	Conversations int   `json:"conversations"`
	// Compactions is how many times the context has been folded down to fit.
	Compactions int64 `json:"compactions"`
	// Updated is when this revision was published, and Watermark how far event
	// time had advanced in the ingest job when it was.
	Updated   time.Time `json:"updated"`
	Watermark time.Time `json:"watermark,omitempty"`
}

// Fresh reports whether this view is younger than d.
func (v View) Fresh(d time.Duration) bool {
	return !v.Updated.IsZero() && time.Since(v.Updated) < d
}

// String renders the view's headline for a log line.
func (v View) String() string {
	s := fmt.Sprintf("v%d · %d entries · %s", v.Version, v.Entries, humanBytes(v.Bytes))
	if v.Brief {
		s += " · briefed"
	}
	return s
}

// --- the curator ---------------------------------------------------------

// entry is one conversation slice as the curator holds it, alongside what the
// pane it came from told us about it.
type entry struct {
	Key          string    `json:"key"`
	Conversation string    `json:"conversation"`
	Body         string    `json:"body"`
	Messages     int       `json:"messages"`
	At           time.Time `json:"at"`
}

// curator owns the maintained context, and is the only thing that writes it.
//
// Single ownership is the design rather than an implementation detail. The
// context is an append-only chain with a compaction that rewrites its root,
// and two writers would need a lock held across a model call to keep those two
// operations from interleaving. One writer, fed by a channel, needs none: panes
// queue behind each other, compaction runs beside the queue rather than in it,
// and every reader is served from a pointer that only ever moves forward.
type curator struct {
	desk  *Desk
	chain *delta.Chain

	// in carries panes from the ingest job's sink. Its capacity is the
	// backpressure: a desk whose curator cannot keep up eventually stalls the
	// sink, which stalls the window stage, which is where a stream job is
	// supposed to feel pressure.
	in chan pending

	// view is the published snapshot. Readers load it without a lock; the
	// curator is the only writer.
	view atomic.Pointer[View]

	// Everything below is the curator's alone, touched on its goroutine only.
	head     delta.Ref
	brief    string
	entries  []entry
	applied  map[string]bool
	order    []string
	messages int64
	convs    map[string]bool
	version  int64

	// compacting is set while a compaction agent is in flight, and folding is
	// how many entries it was given, so its result can be spliced in front of
	// whatever arrived meanwhile.
	compacting bool
	folding    int
	// stuck records that the context is over budget with nothing left to fold,
	// so the desk says so once per episode rather than once per entry.
	stuck       bool
	compactions atomic.Int64
	compacted   chan compaction

	// waiters are the read-your-writes waits: each is woken on every publish.
	waitMu  sync.Mutex
	waitCh  chan struct{}
	pending atomic.Int64
}

// pending is one pane on its way into the context.
type pending struct {
	key          string
	conversation string
	body         string
	messages     int
	watermark    time.Time
}

// compaction is a finished fold of the oldest entries.
type compaction struct {
	brief  string
	folded int
	err    error
}

func newCurator(d *Desk, chain *delta.Chain) *curator {
	c := &curator{
		desk:      d,
		chain:     chain,
		in:        make(chan pending, 256),
		applied:   map[string]bool{},
		convs:     map[string]bool{},
		compacted: make(chan compaction, 1),
		waitCh:    make(chan struct{}),
	}
	c.view.Store(&View{Updated: time.Now()})
	return c
}

// View returns the published context. It is a plain atomic load: the read path
// of a desk never takes a lock and never waits for ingestion.
func (c *curator) View() View { return *c.view.Load() }

// run is the curator's loop. It ends when the desk's context does, which is
// what makes Close a cancellation rather than a protocol.
func (c *curator) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-c.in:
			c.pending.Add(-1)
			c.apply(ctx, p)
		case done := <-c.compacted:
			c.absorb(ctx, done)
		}
	}
}

// submit hands a pane to the curator, blocking only if the queue is full.
func (c *curator) submit(ctx context.Context, p pending) error {
	c.pending.Add(1)
	select {
	case c.in <- p:
		return nil
	case <-ctx.Done():
		c.pending.Add(-1)
		return ctx.Err()
	}
}

// apply folds one pane into the context and publishes the result.
//
// The append is the cheap operation this whole package is arranged around: it
// writes one revision holding one segment, and every process that already held
// the previous revision extends its rendering by that segment instead of
// re-reading the context.
func (c *curator) apply(ctx context.Context, p pending) {
	// At-least-once delivery is the stream layer's contract, so a pane can
	// arrive twice — after a restart, or after a checkpoint that was taken
	// between the write and the commit. The pane key is stable across both, so
	// recognizing it is all the idempotence the context needs.
	if c.applied[p.key] {
		return
	}
	c.remember(p.key)

	e := entry{
		Key: p.key, Conversation: p.conversation, Body: p.body,
		Messages: p.messages, At: time.Now().UTC(),
	}
	ref, err := c.chain.Append(c.head, delta.Segment{Name: segEntry, Body: p.body})
	if err != nil {
		c.desk.noteError(fmt.Errorf("recall: append entry: %w", err))
		return
	}
	c.head = ref
	c.entries = append(c.entries, e)
	c.messages += int64(p.messages)
	if p.conversation != "" {
		c.convs[p.conversation] = true
	}
	c.publish(p.watermark)
	c.maybeCompact(ctx)
}

// maybeCompact starts a compaction when the context has outgrown its budget.
//
// It runs as its own agent on the desk's fleet rather than inside this loop,
// and that is the point: compaction is a model call over the whole context, and
// a curator blocked on one would stop folding in new conversations for its
// duration. Started beside the loop, ingestion continues, queries continue
// against the context as it stands, and the fold is spliced in when it lands.
func (c *curator) maybeCompact(ctx context.Context) {
	if c.compacting || c.desk.maxBytes <= 0 {
		return
	}
	if c.rendered() <= c.desk.maxBytes {
		c.stuck = false
		return
	}
	keep := c.desk.keep
	if keep >= len(c.entries) {
		// Every entry is one the policy says to keep, and the context is still
		// over budget: there is nothing left to fold that folding would help,
		// and a compaction that cannot shrink is a model call spent to learn
		// that again. Carry on oversized, and say so once per episode rather
		// than once per entry.
		if !c.stuck {
			c.stuck = true
			c.desk.noteError(fmt.Errorf(
				"recall: context is %s against a %s budget with %d entries, all within "+
					"Keep=%d: raise MaxBytes, lower Keep, or shorten Spec.Digest's output",
				humanBytes(c.rendered()), humanBytes(c.desk.maxBytes), len(c.entries), keep))
		}
		return
	}
	c.stuck = false
	fold := c.entries[:len(c.entries)-keep]
	items := make([]string, 0, len(fold)+1)
	if c.brief != "" {
		items = append(items, c.brief)
	}
	for _, e := range fold {
		items = append(items, e.Body)
	}

	c.compacting, c.folding = true, len(fold)
	go func() {
		brief, err := c.desk.compact(ctx, items)
		select {
		case c.compacted <- compaction{brief: brief, folded: len(fold), err: err}:
		case <-ctx.Done():
		}
	}()
}

// absorb installs a finished compaction.
//
// It is the one operation that does not append. A brief that replaces the front
// of the context changes bytes that every later segment sits behind, so the
// chain gets a new root rather than another link — which costs the next query a
// full render, once, and every query after it a splice again. That trade is the
// reason compaction is rare and the budget is a byte count rather than a
// count of entries.
func (c *curator) absorb(ctx context.Context, done compaction) {
	c.compacting = false
	if done.err != nil {
		c.desk.noteError(fmt.Errorf("recall: compaction: %w", done.err))
		return
	}
	if done.folded > len(c.entries) {
		return // cannot happen: entries only grow between start and finish
	}
	// A fold that produced nothing is not a fold that produced an empty
	// summary: installing it would drop the entries it was supposed to replace
	// and lose them, since the entries are the only place their content lives.
	// Keep them, and let the next append try again.
	brief := strings.TrimSpace(done.brief)
	if brief == "" {
		c.desk.noteError(fmt.Errorf(
			"recall: compaction of %d entries produced an empty brief; keeping them",
			done.folded))
		return
	}

	// Written before anything is committed to, because a state whose Ref and
	// whose rendered text describe different contexts is worse than a state
	// that is merely oversized: a query reads the revision and everything else
	// reads the text, and they must be the same context.
	kept := append([]entry(nil), c.entries[done.folded:]...)
	ref, err := c.chain.Root(segmentsOf(brief, kept)...)
	if err != nil {
		c.desk.noteError(fmt.Errorf("recall: compaction: %w", err))
		return
	}
	c.brief, c.entries, c.head = brief, kept, ref
	c.compactions.Add(1)

	c.publish(c.View().Watermark)
	// The fold may not have been enough — a burst can arrive while it runs —
	// so the budget is checked again rather than assumed.
	c.maybeCompact(ctx)
}

// segments is the context as the renderer sees it: the brief, then the entries
// oldest first.
//
// Everything that renders or writes the context goes through here, so that
// c.head and the published text can never be two different arrangements of the
// same pieces.
func (c *curator) segments() []delta.Segment { return segmentsOf(c.brief, c.entries) }

func segmentsOf(brief string, entries []entry) []delta.Segment {
	segs := make([]delta.Segment, 0, len(entries)+1)
	if brief != "" {
		segs = append(segs, delta.Segment{Name: segBrief, Body: brief})
	}
	for _, e := range entries {
		segs = append(segs, delta.Segment{Name: segEntry, Body: e.Body})
	}
	return segs
}

// rendered is the context's size in bytes as a model would receive it.
//
// The published view is authoritative whenever it is current, which it is
// everywhere this is called from: rendering twice to learn the same number
// would be the cost this package exists to avoid, paid on the write path.
func (c *curator) rendered() int {
	if v := c.view.Load(); v != nil && v.Version == c.version {
		return v.Bytes
	}
	segs := c.segments()
	text, _ := c.desk.renderer.Render(segs, 0, len(segs))
	return len(text)
}

// publish renders the context and swaps it in as the new view.
func (c *curator) publish(watermark time.Time) {
	segs := c.segments()
	text, _ := c.desk.renderer.Render(segs, 0, len(segs))
	c.version++

	v := &View{
		Version: c.version, Ref: c.head, Text: text,
		Entries: len(c.entries), Brief: c.brief != "", Bytes: len(text),
		Messages: c.messages, Conversations: len(c.convs),
		Compactions: c.compactions.Load(),
		Updated:     time.Now().UTC(), Watermark: watermark,
	}
	c.view.Store(v)
	c.save()

	// Waking every waiter on every publish, rather than tracking which version
	// each is waiting for, is right because the wait is rare and the wake is a
	// channel close: a waiter that is still unsatisfied re-reads the view and
	// waits again.
	c.waitMu.Lock()
	close(c.waitCh)
	c.waitCh = make(chan struct{})
	c.waitMu.Unlock()

}

// updates returns a channel closed on the next publish.
func (c *curator) updates() <-chan struct{} {
	c.waitMu.Lock()
	defer c.waitMu.Unlock()
	return c.waitCh
}

// remember records a pane key as applied, keeping the most recent
// appliedKeys of them.
//
// The set is bounded because it is a guard against replay, and replay is
// bounded: a restart re-reads from the last checkpoint, not from the beginning
// of time. Keeping every key a long-lived desk ever saw would be a leak in
// service of a case that cannot arise.
func (c *curator) remember(key string) {
	c.applied[key] = true
	c.order = append(c.order, key)
	if len(c.order) > appliedKeys {
		drop := c.order[0]
		c.order = c.order[1:]
		delete(c.applied, drop)
	}
}

// appliedKeys bounds the replay guard. It is generously larger than the number
// of panes a checkpoint interval can produce.
const appliedKeys = 4096

// --- durability ----------------------------------------------------------

// state is what a desk writes down so that a restart resumes its context
// rather than rebuilding it from the conversations.
//
// The entries themselves are not in here: they are in the chain, in the
// content-addressed store, under a hash this file names. What this file is, is
// the pointer — the one piece of a content-addressed system that has to be
// mutable, kept as small as a pointer should be.
type state struct {
	Version       int64     `json:"version"`
	Ref           delta.Ref `json:"ref"`
	Brief         bool      `json:"brief"`
	Entries       []entry   `json:"entries"`
	Messages      int64     `json:"messages"`
	Conversations []string  `json:"conversations"`
	Compactions   int64     `json:"compactions"`
	Applied       []string  `json:"applied,omitempty"`
	Saved         time.Time `json:"saved"`
}

// statePath is where a desk keeps its pointer, or "" for a desk with no state
// directory — which is a desk that starts cold every time, and is the right
// configuration for a test and the wrong one for a deployment.
func (d *Desk) statePath() string {
	if d.stateDir == "" {
		return ""
	}
	return filepath.Join(d.stateDir, "recall", d.name+".json")
}

// save writes the pointer. A failure here is reported and not fatal: the desk
// is still correct, it has just lost its ability to resume.
func (c *curator) save() {
	path := c.desk.statePath()
	if path == "" {
		return
	}
	convs := make([]string, 0, len(c.convs))
	for k := range c.convs {
		convs = append(convs, k)
	}
	sort.Strings(convs)

	st := state{
		Version: c.version, Ref: c.head, Brief: c.brief != "",
		Entries: c.entries, Messages: c.messages, Conversations: convs,
		Compactions: c.compactions.Load(), Applied: c.order, Saved: time.Now().UTC(),
	}
	blob, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		c.desk.noteError(fmt.Errorf("recall: save state: %w", err))
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		c.desk.noteError(fmt.Errorf("recall: save state: %w", err))
		return
	}
	// Written beside and renamed over, so a desk killed mid-write resumes from
	// the previous pointer rather than from half of this one.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, blob, 0o644); err != nil {
		c.desk.noteError(fmt.Errorf("recall: save state: %w", err))
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		c.desk.noteError(fmt.Errorf("recall: save state: %w", err))
	}
}

// restore reads the pointer and rebuilds the curator's view of the context.
//
// The entries come back from the file and the brief from the chain, because the
// brief is the one segment the file does not hold: it can be large, it is
// already in the store, and resolving the revision is how we check that the
// store the desk was pointed at is the store the chain was written to.
func (c *curator) restore() (bool, error) {
	path := c.desk.statePath()
	if path == "" {
		return false, nil
	}
	blob, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("recall: restore: %w", err)
	}
	var st state
	if err := json.Unmarshal(blob, &st); err != nil {
		return false, fmt.Errorf("recall: restore %s: %w", path, err)
	}
	if st.Ref.Zero() {
		return false, nil
	}
	list, err := c.chain.Resolve(st.Ref)
	if err != nil {
		// A pointer into a store that no longer holds the revision is a desk
		// that was moved without its state directory. Starting cold is the only
		// thing it can do, and saying so is better than doing it quietly.
		return false, fmt.Errorf("recall: restore %s (%s): %w", path, st.Ref, err)
	}
	if st.Brief && list.Len() > 0 && list.Segs[0].Name == segBrief {
		c.brief = list.Segs[0].Body
	}
	c.head = st.Ref
	c.version = st.Version
	c.entries = st.Entries
	c.messages = st.Messages
	c.compactions.Store(st.Compactions)
	for _, k := range st.Conversations {
		c.convs[k] = true
	}
	for _, k := range st.Applied {
		c.remember(k)
	}

	segs := c.segments()
	text, _ := c.desk.renderer.Render(segs, 0, len(segs))
	c.view.Store(&View{
		Version: c.version, Ref: c.head, Text: text,
		Entries: len(c.entries), Brief: c.brief != "", Bytes: len(text),
		Messages: c.messages, Conversations: len(c.convs),
		Compactions: st.Compactions, Updated: st.Saved,
	})
	return true, nil
}

// --- the sink ------------------------------------------------------------

// sink is the ingest job's terminal stage: where a fired pane stops being
// stream-mode output and becomes a line of the maintained context.
//
// It is a stream.Sink like any other, which is what keeps the two halves of the
// desk honestly separate — the ingest job knows it is writing to a destination,
// not that the destination is a prompt.
type sink struct {
	desk *Desk
	ctx  context.Context
}

// Write implements stream.Sink.
func (s *sink) Write(_ context.Context, b stream.Batch) error {
	if len(b.Records) == 0 {
		return nil
	}
	digest := digestOf(b.Records, s.desk.spec.Digest.OutputField)
	if strings.TrimSpace(digest) == "" {
		return nil
	}
	conversation := b.Pane.Window.Key
	if conversation == "" {
		conversation = b.Records[0].String("conversation")
	}
	return s.desk.curator.submit(s.ctx, pending{
		key:          b.Key(),
		conversation: conversation,
		body:         entryBody(b.Pane, conversation, digest),
		messages:     b.Pane.Count,
		watermark:    b.Pane.Watermark,
	})
}

// Commit implements stream.Sink. The context is not staged: an entry is
// published the moment it is folded in, because a serving layer whose answers
// waited for the next checkpoint would be a serving layer with a checkpoint
// interval of staleness built in. The cost is at-least-once — a pane replayed
// after a crash is recognized by its key rather than by a transaction.
func (s *sink) Commit(context.Context, int64) error { return nil }

// Close implements stream.Sink.
func (s *sink) Close() error { return nil }

// digestOf pulls the aggregation's text out of a pane's output records.
func digestOf(recs []core.Record, field string) string {
	if field == "" {
		field = "output"
	}
	var parts []string
	for _, r := range recs {
		if v := strings.TrimSpace(r.String(field)); v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, "\n")
}

// --- compaction ----------------------------------------------------------

// compact folds a set of entries into one standing brief, as an ordinary agent
// on the desk's fleet.
//
// It is a pipeline rather than a model call because everything a pipeline gives
// it is wanted here: the tree reduce handles a context of any size, the result
// cache means a compaction repeated after a crash is free, the budget governor
// counts it against the same ceiling as everything else, and the fairness
// policy stops it from starving the queries running beside it.
func (d *Desk) compact(ctx context.Context, items []string) (string, error) {
	field := d.spec.Compact.ItemField
	if field == "" {
		field = "entry"
	}
	recs := make([]core.Record, 0, len(items))
	for i, it := range items {
		recs = append(recs, core.NewRecord(fmt.Sprintf("fold-%d", i),
			map[string]any{field: it}))
	}

	p := pipeline.New("recall/" + d.name + "/compact")
	p.FromRecords("entries", recs).ReduceAI("brief", d.spec.Compact)

	res, err := d.fleet.Run(ctx, p)
	if err != nil {
		return "", err
	}
	out := d.spec.Compact.OutputField
	if out == "" {
		out = "output"
	}
	if len(res.Output) == 0 {
		return "", fmt.Errorf("compaction produced nothing")
	}
	return res.Output[0].String(out), nil
}

// The sink is bound by loom.WithSink, which takes the interface rather than
// this type, so the check belongs here.
var _ stream.Sink = (*sink)(nil)
