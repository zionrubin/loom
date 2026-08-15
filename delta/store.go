package delta

import (
	"container/list"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
)

// Policy is the router: given how big a context is and how much of it changed,
// which path materializes it.
//
// The premise is that incremental is not universally better, and a system that
// believed it was would be slower than the one it replaced on exactly the
// workloads people notice. Splicing costs a re-render of the change plus a
// repair window plus one copy of the retained bytes; rebuilding costs a render
// of everything. Below some ratio the first is obviously cheaper and above it
// obviously is not, and the interesting property of the crossover is that it is
// a property of the deployment — the renderer, the machine, the size of a
// typical turn — rather than of this package. So it is a policy, with defaults
// that are a starting point and not a claim.
//
// The defaults come from the published shape of agent traces (a median call
// appending on the order of a kilobyte to a context of tens of thousands of
// tokens, with rebuilds becoming competitive somewhere in the tens of
// kilobytes of append) and they are worth re-measuring rather than trusting.
// The one thing they cannot get wrong is the answer: both routes produce
// identical bytes, so a badly tuned policy costs time and nothing else.
type Policy struct {
	// Window is the smallest repair window a splice may start with, in
	// already-rendered segments (default 1).
	//
	// It is a floor rather than the number itself: the window actually used is
	// at least the renderer's declared Lookahead plus one, because the segments
	// inside the lookahead are expected to differ and the extra one is the
	// evidence. Raising it buys more evidence per splice at the cost of
	// re-rendering more, which is worth doing only for a renderer whose
	// lookahead is a claim nobody is confident in.
	Window int
	// MaxWindow is how far the window may widen before the splice gives up and
	// rebuilds (default 64). Widening is how an unstable seam is answered, and
	// this bounds how much re-rendering that answer may cost before the
	// reference path becomes the cheaper one.
	MaxWindow int
	// MaxDelta is the source bytes of change above which a rebuild is routed
	// without trying to splice (default 32 KiB).
	MaxDelta int
	// MaxRatio is the change-to-context ratio above which a rebuild is routed
	// (default 0.5). It catches the case MaxDelta cannot: a small append to a
	// small context, where there is nothing worth retaining.
	MaxRatio float64
	// Verify is the fraction of accepted splices recomputed from scratch and
	// compared (default 0.02). It is the runtime half of the correctness
	// argument: the certificate checks a splice's arithmetic every time, and
	// this checks the assumption underneath it now and then, on real traffic,
	// against the reference path.
	//
	// Zero means never, which is a choice a deployment may make and this
	// package will not make for it. One means always, which is what a test
	// wants.
	Verify float64
}

// DefaultPolicy is the zero value filled in.
var DefaultPolicy = Policy{
	Window: 1, MaxWindow: 64, MaxDelta: 32 << 10, MaxRatio: 0.5, Verify: 0.02,
}

func (p Policy) normalize() Policy {
	if p.Window <= 0 {
		p.Window = DefaultPolicy.Window
	}
	if p.MaxWindow < p.Window {
		p.MaxWindow = max(DefaultPolicy.MaxWindow, p.Window)
	}
	if p.MaxDelta <= 0 {
		p.MaxDelta = DefaultPolicy.MaxDelta
	}
	if p.MaxRatio <= 0 {
		p.MaxRatio = DefaultPolicy.MaxRatio
	}
	if p.Verify < 0 {
		p.Verify = 0
	}
	if p.Verify > 1 {
		p.Verify = 1
	}
	return p
}

// Route picks a path for a change of `change` source bytes against a context of
// `base` source bytes, and says why in words.
//
// The reason is returned rather than logged because a rebuild is not a failure
// and a report that cannot distinguish "this worker has never seen this
// session" from "this append was half the context" cannot tell an operator
// whether the fleet is misrouting work or the policy is doing its job.
func (p Policy) Route(base, change int, resident bool) (Route, string) {
	p = p.normalize()
	switch {
	case !resident:
		return RouteRebuild, "no state for this revision's ancestry in this process"
	case change > p.MaxDelta:
		return RouteRebuild, fmt.Sprintf("change of %d B is above the %d B splice ceiling",
			change, p.MaxDelta)
	case base > 0 && float64(change)/float64(base) > p.MaxRatio:
		return RouteRebuild, fmt.Sprintf("change is %.0f%% of the context, above %.0f%%",
			100*float64(change)/float64(base), 100*p.MaxRatio)
	default:
		return RouteSplice, fmt.Sprintf("change of %d B against %d B", change, base)
	}
}

// Materialization is one context, made available to one task.
//
// Route and Cert answer two different questions and the difference matters when
// reading a report. Route is how *this call* was served — from a state this
// process already had, by splicing one, or by rendering everything. Cert is the
// certificate the state was created under, which for a state served from memory
// is the certificate of whichever call created it, possibly some rounds ago.
type Materialization struct {
	Ref   Ref
	Route Route
	Text  string
	Cert  Certificate
	// Base is the rendered size of the state this one was built from, and
	// Delta the source bytes added since it. Both are zero when there was
	// nothing to build on, which is what a cold process reports.
	Base  int
	Delta int
	Took  time.Duration
}

// Zero reports whether nothing was materialized.
func (m Materialization) Zero() bool { return m.Text == "" && m.Ref.Zero() }

// Hint is what a provider is told about this context's place in a sequence.
//
// Stable is the certificate's retained region and nothing else. After a rebuild
// it is zero even though the bytes are, necessarily, the same bytes the
// previous revision's rendering ended with — because "the same" was not
// established here, and a number that means "certified identical" must not
// quietly also mean "probably fine".
func (m Materialization) Hint() Hint {
	if m.Ref.Zero() {
		return Hint{}
	}
	return Hint{
		Key: m.Ref.Key, Parent: m.Ref.Parent, Hash: m.Ref.Hash,
		Stable: m.Cert.Retained,
	}
}

// Attribution names the work a materialization is being done for, so the events
// it publishes land on the same run, stage and task as everything else in the
// stream. It is the same reason executor.ModelClient.Call is handed a task ID:
// a component that publishes has to be told who it is publishing for, because
// it cannot see the schedule.
type Attribution struct {
	RunID  string
	Stage  string
	TaskID string
}

// Stats is what a store did.
type Stats struct {
	// Materialize calls, split by how they were served.
	Calls    int `json:"calls"`
	Hits     int `json:"hits"`
	Splices  int `json:"splices"`
	Rebuilds int `json:"rebuilds"`
	// Widenings totals the times a repair window had to grow before its seam
	// held. Persistently above zero means Policy.Window is smaller than the
	// renderer's real lookahead, and every splice is paying to rediscover it.
	Widenings int `json:"widenings"`
	// Retained totals the bytes served without rendering them again, and
	// Rendered the bytes that were rendered. Their ratio is the whole claim of
	// this package, measured rather than asserted.
	Retained int64 `json:"retained"`
	Rendered int64 `json:"rendered"`
	// Verified counts splices recomputed from scratch and compared; Diverged
	// counts those that disagreed. Diverged above zero is a correctness alarm
	// and quarantines the renderer for the life of the process.
	Verified int `json:"verified"`
	Diverged int `json:"diverged"`
	// Residency.
	States   int   `json:"states"`
	Keys     int   `json:"keys"`
	Bytes    int64 `json:"bytes"`
	Evicted  int   `json:"evicted"`
	Rejected int   `json:"rejected"`
}

// Saved reports the bytes this store did not have to render.
func (s Stats) Saved() int64 { return s.Retained }

// Options configure a Store.
type Options struct {
	// Renderer turns segments into bytes (default Tags).
	Renderer Renderer
	// Policy routes materializations (the zero value is DefaultPolicy).
	Policy Policy
	// MaxBytes bounds the rendered bytes held resident (default 64 MiB). A
	// state costs roughly twice its context, so this is a ceiling on about half
	// as much context as it says.
	MaxBytes int64
	// Bus publishes what the store does.
	Bus *observe.Bus
	// Rand draws the shadow-verification sample (default math/rand).
	Rand func() float64
}

// Store materializes contexts for one process, keeping what it has already
// rendered so the next revision costs the change rather than the context.
//
// It is the piece a worker holds, and everything about it is shaped by the fact
// that a worker can vanish. Nothing here is authoritative: the chain in shared
// storage is the state, this is a cache of renderings of it, and every question
// a caller can ask has an answer that does not need the cache to be warm. A
// worker that has followed a session for an hour splices; the worker that
// replaces it when that one is killed rebuilds; the bytes they hand to a model
// are identical, and the only difference is how long it took.
//
// A Store is safe for concurrent use. Concurrent materializations of the same
// revision — the ordinary case, since a round's tasks all want the same context
// — collapse into one: the first renders and the rest wait for it.
type Store struct {
	blobs Blobs
	r     Renderer
	pol   Policy
	max   int64
	bus   *observe.Bus
	rnd   func() float64

	mu      sync.Mutex
	states  map[string]*list.Element // revision hash → LRU element
	lru     *list.List               // *entry, most recently used at the front
	bytes   int64
	keys    map[string]int // locality key → resident states
	bad     map[string]bool
	pending map[string]*call
	stats   Stats
}

// entry is one resident state and the certificate it was created under.
type entry struct {
	state *State
	cert  Certificate
}

// call is one in-flight materialization others may wait on.
type call struct {
	done chan struct{}
	m    Materialization
	err  error
}

// NewStore returns a store over shared storage.
func NewStore(b Blobs, opts Options) (*Store, error) {
	if b == nil {
		return nil, errorf("a state store needs shared storage")
	}
	if opts.Renderer == nil {
		opts.Renderer = Tags{}
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 64 << 20
	}
	if opts.Rand == nil {
		opts.Rand = rand.Float64
	}
	return &Store{
		blobs: b, r: opts.Renderer, pol: opts.Policy.normalize(),
		max: opts.MaxBytes, bus: opts.Bus, rnd: opts.Rand,
		states: map[string]*list.Element{}, lru: list.New(),
		keys: map[string]int{}, bad: map[string]bool{}, pending: map[string]*call{},
	}, nil
}

// Renderer returns the version this store renders with.
func (s *Store) Renderer() Renderer { return s.r }

// Chain returns a chain for key over this store's storage and renderer, so a
// process that materializes contexts can also write them without arranging to
// agree with itself about either.
func (s *Store) Chain(key string) (*Chain, error) { return NewChain(s.blobs, s.r, key) }

// Materialize renders the context ref names, as cheaply as it can prove is
// safe.
//
// The five paths it can take, in the order it tries them:
//
//	already held      this exact revision is resident: no rendering at all
//	being built       another goroutine is materializing it: wait for that one
//	splice            an ancestor is resident and the change is small enough
//	rebuild           anything else: render every segment, which is the
//	                  reference result the other paths are defined against
//	quarantine        a renderer that has been caught diverging never splices
//	                  again in this process, and every call rebuilds
//
// A zero ref materializes nothing and is not an error: a stage with no
// continuation is the ordinary case.
func (s *Store) Materialize(ctx context.Context, at Attribution, ref Ref) (Materialization, error) {
	if ref.Zero() {
		return Materialization{}, nil
	}
	if err := ctx.Err(); err != nil {
		return Materialization{}, err
	}
	start := time.Now()

	s.mu.Lock()
	s.stats.Calls++
	if e, ok := s.touch(ref.Hash); ok {
		s.stats.Hits++
		s.stats.Retained += int64(len(e.state.Text))
		s.mu.Unlock()
		return Materialization{
			Ref: ref, Route: RouteHit, Text: e.state.Text, Cert: e.cert,
			Base: len(e.state.Text), Took: time.Since(start),
		}, nil
	}
	if c, ok := s.pending[ref.Hash]; ok {
		s.mu.Unlock()
		select {
		case <-c.done:
		case <-ctx.Done():
			return Materialization{}, ctx.Err()
		}
		if c.err != nil {
			return Materialization{}, c.err
		}
		// This call did none of the work, and says so: a round of eight tasks
		// should report one render and seven states served, not eight splices.
		m := c.m
		m.Route, m.Took = RouteHit, time.Since(start)
		s.count(func(st *Stats) { st.Hits++; st.Retained += int64(len(m.Text)) })
		return m, nil
	}
	c := &call{done: make(chan struct{})}
	s.pending[ref.Hash] = c
	s.mu.Unlock()

	m, err := s.materialize(ctx, at, ref, start)

	c.m, c.err = m, err
	close(c.done)
	s.mu.Lock()
	delete(s.pending, ref.Hash)
	s.mu.Unlock()
	return m, err
}

// materialize is Materialize's slow half: everything that touches storage or
// renders anything.
func (s *Store) materialize(ctx context.Context, at Attribution, ref Ref, start time.Time) (Materialization, error) {
	chain, err := NewChain(s.blobs, s.r, ref.Key)
	if err != nil {
		return Materialization{}, err
	}
	base, added, err := chain.Trace(ref, s.holds)
	if err != nil {
		return Materialization{}, err
	}

	var prev *State
	if base != "" {
		s.mu.Lock()
		if e, ok := s.touch(base); ok {
			prev = e.state
		}
		s.mu.Unlock()
	}

	// The list to render. With a parent state the segments before the change
	// come from memory, which is what keeps the read proportional to the
	// change; without one they came from the chain, and this is the whole
	// context.
	var l List
	if prev != nil {
		l = prev.List.Append(added...)
	} else {
		l = Segments(added...)
	}

	change := 0
	for _, seg := range added {
		change += len(seg.Name) + len(seg.Body)
	}
	route, reason := s.pol.Route(baseSize(prev), change, prev != nil)
	if route == RouteSplice && s.quarantined() {
		route, reason = RouteRebuild, "this renderer has been quarantined after a divergence"
	}

	var st *State
	var cert Certificate
	if route == RouteSplice {
		st, cert, err = Splice(prev, s.r, ref, l, s.pol.Window, s.pol.MaxWindow)
	} else {
		st, cert, err = Build(s.r, ref, l)
		if cert.Reason == "full render" {
			cert.Reason = reason
		}
	}
	if err != nil {
		return Materialization{}, core.Permanent(err)
	}

	// Every splice is certified before it is used, not sampled. The check is
	// proportional to what was repaired, so doing it always costs a fraction of
	// what was already spent rendering, and a splice that cannot certify itself
	// is a bug in this package rather than a slow path — which is why it is
	// answered by rebuilding and by saying so loudly.
	if cert.Route == RouteSplice {
		if verr := cert.Verify(prev, st); verr != nil {
			s.diverge(at, ref, verr.Error())
			st, cert, err = Build(s.r, ref, l)
			if err != nil {
				return Materialization{}, core.Permanent(err)
			}
			cert.Reason = "the splice failed its own certificate"
		}
	}

	if cert.Route == RouteSplice && s.sample() {
		st, cert = s.shadow(at, chain, ref, l, st, cert)
	}

	m := Materialization{
		Ref: ref, Route: cert.Route, Text: st.Text, Cert: cert,
		Base: baseRendered(prev), Delta: change, Took: time.Since(start),
	}
	s.admit(ref, st, cert)
	s.publish(at, m)
	return m, nil
}

// shadow recomputes a splice the long way and compares.
//
// It reads the chain back to the root rather than reusing the list the splice
// was given, which makes it a check on two things at once: that the renderer's
// family assumption held, and that the walk which produced the segment list
// produced the right one. Reusing the list would only ever have tested the
// first, and the second is the half a caller cannot inspect.
//
// A divergence is not recoverable and is not treated as one. The renderer is
// quarantined for the life of the process — every later call rebuilds — the
// reference result is returned to this caller, and an event says so. Being
// wrong quietly is the only outcome this package is designed to make
// impossible.
func (s *Store) shadow(at Attribution, chain *Chain, ref Ref, l List, spliced *State, cert Certificate) (*State, Certificate) {
	s.count(func(st *Stats) { st.Verified++ })

	ref2 := ref
	want, err := chain.Resolve(ref2)
	if err != nil {
		// The chain could not be read back. That says nothing about the splice,
		// so it is not a divergence; it is a verification that did not happen.
		return spliced, cert
	}
	reference, refCert, err := Build(s.r, ref2, want)
	if err != nil {
		return spliced, cert
	}
	if reference.Digest() == spliced.Digest() && reference.Text == spliced.Text {
		return spliced, cert
	}

	s.diverge(at, ref, fmt.Sprintf(
		"a certified splice disagreed with a full render: %d B spliced digesting to %s, "+
			"%d B rendered digesting to %s",
		len(spliced.Text), short(spliced.Digest()), len(reference.Text), short(reference.Digest())))
	refCert.Reason = "shadow verification found the splice wrong; this is the full render"
	return reference, refCert
}

// diverge records a correctness alarm and stops this process splicing.
func (s *Store) diverge(at Attribution, ref Ref, detail string) {
	s.mu.Lock()
	first := !s.bad[s.r.Version()]
	s.bad[s.r.Version()] = true
	s.stats.Diverged++
	s.mu.Unlock()
	if s.bus != nil && first {
		s.bus.Publish(observe.Event{
			Type: observe.DeltaDiverged, RunID: at.RunID, Stage: at.Stage, TaskID: at.TaskID,
			Continuation: ref.Key, Artifact: ref.Hash, Detail: s.r.Version(),
			Err: "delta: " + detail + " — this renderer will not be spliced again in " +
				"this process; every context will be rendered in full",
		})
	}
}

// admit puts a state in residence, evicting the least recently used until the
// ceiling is respected.
func (s *Store) admit(ref Ref, st *State, cert Certificate) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cert.Route {
	case RouteSplice:
		s.stats.Splices++
		s.stats.Retained += int64(cert.Retained)
		s.stats.Rendered += int64(cert.Repaired)
		s.stats.Widenings += cert.Widenings
	default:
		s.stats.Rebuilds++
		s.stats.Rendered += int64(cert.Repaired)
	}

	size := int64(len(st.Text))
	if size > s.max {
		// A context larger than the whole ceiling is not worth evicting
		// everything else for, and holding it would guarantee it is evicted
		// before its own next revision arrives. It is served and forgotten.
		s.stats.Rejected++
		return
	}
	if _, dup := s.states[ref.Hash]; dup {
		return
	}
	s.states[ref.Hash] = s.lru.PushFront(&entry{state: st, cert: cert})
	s.bytes += size
	s.keys[ref.Key]++
	for s.bytes > s.max {
		back := s.lru.Back()
		if back == nil {
			break
		}
		s.evictLocked(back)
	}
	s.stats.States, s.stats.Keys, s.stats.Bytes = len(s.states), len(s.keys), s.bytes
}

// evictLocked drops one state. Callers hold s.mu.
func (s *Store) evictLocked(el *list.Element) {
	e := el.Value.(*entry)
	s.lru.Remove(el)
	delete(s.states, e.state.Ref.Hash)
	s.bytes -= int64(len(e.state.Text))
	if k := e.state.Ref.Key; s.keys[k] > 1 {
		s.keys[k]--
	} else {
		delete(s.keys, k)
	}
	s.stats.Evicted++
}

// touch returns a resident state and marks it most recently used. Callers hold
// s.mu.
func (s *Store) touch(hash string) (*entry, bool) {
	el, ok := s.states[hash]
	if !ok {
		return nil, false
	}
	s.lru.MoveToFront(el)
	return el.Value.(*entry), true
}

// holds reports whether a revision is resident, which is what Chain.Trace asks
// on the way back toward the root.
func (s *Store) holds(hash string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.states[hash]
	return ok
}

func (s *Store) quarantined() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bad[s.r.Version()]
}

func (s *Store) sample() bool {
	if s.pol.Verify <= 0 {
		return false
	}
	if s.pol.Verify >= 1 {
		return true
	}
	return s.rnd() < s.pol.Verify
}

// Resident lists the continuation keys this process holds state for, sorted.
//
// It is what a worker advertises so the queue can prefer it for work on those
// keys, and it is deliberately a soft signal: the list changes on every
// eviction, it is a snapshot by the time anyone reads it, and nothing goes
// wrong when it is wrong. A task routed to a worker that has since evicted the
// state rebuilds.
func (s *Store) Resident() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.keys))
	for k := range s.keys {
		if k != "" {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// Holds reports whether this process could splice the given revision — that it
// is resident, or one of its ancestors is. It reads storage, so it is for
// reports and tests rather than for the claim path, which learns the same thing
// as a side effect of tracing.
func (s *Store) Holds(ref Ref) bool {
	if ref.Zero() {
		return false
	}
	if s.holds(ref.Hash) {
		return true
	}
	chain, err := NewChain(s.blobs, s.r, ref.Key)
	if err != nil {
		return false
	}
	base, _, err := chain.Trace(ref, s.holds)
	return err == nil && base != ""
}

// Stats reports what this store has done.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	st.States, st.Keys, st.Bytes = len(s.states), len(s.keys), s.bytes
	return st
}

// Forget drops every resident state, which is what a process does when it is
// told the world moved on under it. It cannot lose anything: the chain is in
// shared storage and every dropped state is a rebuild away.
func (s *Store) Forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states = map[string]*list.Element{}
	s.lru.Init()
	s.keys = map[string]int{}
	s.bytes = 0
}

func (s *Store) count(fn func(*Stats)) {
	s.mu.Lock()
	fn(&s.stats)
	s.mu.Unlock()
}

// publish reports one materialization that did work.
//
// A state served from residence publishes nothing, and that is the useful
// granularity rather than an omission: a round whose eight tasks share one
// context should read as one render, not as one render and seven notifications
// that nothing happened.
func (s *Store) publish(at Attribution, m Materialization) {
	if s.bus == nil {
		return
	}
	e := observe.Event{
		Type: observe.DeltaRebuilt, RunID: at.RunID, Stage: at.Stage, TaskID: at.TaskID,
		Continuation: m.Ref.Key, Artifact: m.Ref.Hash,
		Base: m.Base, Delta: m.Delta, Retained: m.Cert.Retained,
		Repaired: m.Cert.Repaired, Window: m.Cert.Window,
		Latency: m.Took, Note: m.Cert.Reason, Detail: m.Cert.Renderer,
	}
	if m.Route == RouteSplice {
		e.Type = observe.DeltaSpliced
	}
	s.bus.Publish(e)
}

// baseSize is the parent state's source size, for the router.
func baseSize(prev *State) int {
	if prev == nil {
		return 0
	}
	return prev.List.Size()
}

// baseRendered is the parent state's rendered size, for reporting.
func baseRendered(prev *State) int {
	if prev == nil {
		return 0
	}
	return len(prev.Text)
}
