package findings

// The distributed half of the commons.
//
// The local ledger makes one process's agents share their research. It cannot
// make two processes share anything, and by the time a fleet is worth running
// it is usually more than one process: a queue worker per machine, a scheduled
// batch beside a long-lived service, ten pods of the same executor started by
// the same deployment. Each of them holds a ledger the others cannot see, so
// the duplication the local gate removed inside a process comes straight back
// at the process boundary — n executors researching one subject n times, and
// n executors calling one source at the same instant because none of them can
// see the others' flights.
//
// This file adds the layer that closes that gap, and the whole of its design
// is in where it sits:
//
//	L1  the in-process ledger        map lookups, no I/O, unchanged
//	L2  the shared backend           one round trip, consulted only on an L1 miss
//	    the source                   what both layers exist to avoid
//
// L1 stays exactly what it was. It is not a write-through cache in front of
// L2, it is the first rung of the same ladder: a local hit answers without
// touching the network, which is the property that lets the gate stand in
// front of every task rather than only the ones a human guessed would collide.
// L2 is reached only when L1 has nothing, and what it returns is *adopted* into
// L1 and then run through the ordinary sufficiency ladder — the same
// reachability, freshness, coverage, corroboration and adjudication rules,
// applied to a finding that happens to have arrived over a socket. A remotely
// stored finding gets no privileges a local one does not have.
//
// Three interfaces are all the gate knows about the backend: findings go in
// and come out of a Store, similarity search happens in a VectorStore, and
// mutual exclusion between executors is a Lease. The PostgreSQL adapter in
// findings/pgstore implements all three over one database and is the intended
// production backend; findings/filestore implements them over a shared
// directory and needs no server at all. Neither is named anywhere in the gate,
// which is the point: the gate is written against the three contracts, so
// replacing the database is an import change.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

// --- The three contracts ------------------------------------------------

// Store is the durable, cross-executor half of the commons: where a finding
// goes so an executor that has never run before can be served it.
//
// It is deliberately the smallest surface the gate actually needs. There is no
// query language, no scan, no cursor — the gate asks for the candidates for one
// question and writes back what it learned, because everything else it does is
// a decision it must make itself. A store that answered "is this finding
// sufficient" would be a store that had to know about grants, freshness
// horizons and coverage, and the moment it knows those, replacing it means
// reimplementing them.
//
// Implementations must be safe for concurrent use by many executors, and they
// must treat findings as immutable: Put appends revisions, and the only fields
// that change after a revision lands are the ones about the *recording* —
// corroboration, threshold, citations, verdicts.
type Store interface {
	// Put records a finding revision and returns the entry the store now holds
	// for it.
	//
	// It carries the same three outcomes as the local ledger's Append, and for
	// the same reasons: a claim nobody has stored becomes a new revision; a
	// claim already stored — same knowledge hash, same subject — corroborates
	// the revision holding it and is returned in place of the input, so two
	// executors that independently learn one thing converge on one entry
	// instead of racing to own it; a finding naming an ID the store has a head
	// for becomes the next revision of that claim.
	Put(ctx context.Context, e Entry) (Entry, error)

	// Candidates returns the live entries that could answer a question: those
	// recorded under its exact key, and those about its subject class. One call
	// covers both tiers because they are one round trip and the caller is going
	// to walk both anyway.
	Candidates(ctx context.Context, q CandidateQuery) ([]Entry, error)

	// Fetch resolves entries by content hash, including superseded and
	// retracted revisions. Lineage names hashes, so a hash must stay resolvable
	// however the claim has since been revised.
	Fetch(ctx context.Context, hashes []string) ([]Entry, error)

	// Retract withdraws a claim across every executor and returns everything
	// that had already been served one of its revisions.
	Retract(ctx context.Context, id, reason string, now time.Time) ([]Dependent, error)

	// Cite records that a finding was served to a task on some executor. It is
	// the justification edge a retraction walks, and it is the one write on the
	// serve path — which is why the gate makes it off the caller's goroutine.
	Cite(ctx context.Context, hash string, d Dependent) error

	// Dependents returns everything that was served a finding, by hash, from
	// every executor.
	Dependents(ctx context.Context, hash string) ([]Dependent, error)

	// Verdicts returns the adjudications recorded against a set of findings,
	// newest first and bounded by limit. They are read when entries are
	// adopted — in one call, because adoption arrives in batches — so an
	// expensive judgement made on one executor is not paid for again on the
	// next.
	Verdicts(ctx context.Context, hashes []string, limit int) ([]Judgement, error)

	// RecordVerdict memoizes an adjudication for every executor.
	RecordVerdict(ctx context.Context, j Judgement) error

	// SetThreshold persists an entry's learned near-match boundary.
	SetThreshold(ctx context.Context, hash string, threshold float64) error

	// Topics summarizes what the store holds, by topic: live claims, negative
	// results, corroborations, and what learning them cost.
	Topics(ctx context.Context) ([]TopicStat, error)

	Close() error
}

// CandidateQuery asks a store for the entries that might answer one question.
// Key is the exact question key, Class the subject class; an implementation
// returns the union, newest first, and never returns retracted or superseded
// revisions.
type CandidateQuery struct {
	Topic string
	Key   string
	Class string
	Limit int
}

// Judgement is one memoized adjudication: a model's answer to "does this
// finding answer this question", recorded so no pairing is judged twice on any
// executor.
type Judgement struct {
	QuestionKey string    `json:"question_key"`
	Hash        string    `json:"hash"`
	OK          bool      `json:"ok"`
	Similarity  float64   `json:"similarity,omitempty"`
	Decided     time.Time `json:"decided"`
	Executor    string    `json:"executor,omitempty"`
}

// VectorStore is similarity search over the commons, and nothing else.
//
// The operations are the four findings needs — upsert by finding hash, top-K
// cosine search filtered to a topic and, when the question has one, a subject
// class, and removal when a claim is withdrawn. Everything a general vector
// database offers beyond that (hybrid ranking, payload filtering, re-indexing
// policy) is deliberately absent, because a wider contract is a contract more
// backends fail to satisfy, and this one is satisfied by a table with an index
// on it.
//
// What the interface cannot express is as important as what it can: it returns
// *matches*, never answers. A match is a candidate the sufficiency ladder has
// not looked at yet, and the gate treats it as one.
type VectorStore interface {
	// Upsert indexes a finding's question embedding under its content hash.
	Upsert(ctx context.Context, v Vector) error
	// Nearest returns the top-K matches over the configured similarity floor,
	// most similar first.
	Nearest(ctx context.Context, q VectorQuery) ([]VectorMatch, error)
	// Remove deactivates a finding's vector, so a retracted claim stops being a
	// candidate for anyone.
	Remove(ctx context.Context, hash string) error
}

// Vector is one indexed question embedding.
type Vector struct {
	Hash      string
	Topic     string
	Class     string
	Embedding []float32
}

// VectorQuery is a similarity search. Class narrows the search to one subject
// when the question has facets to pin it with; with no facets it is empty and
// the search covers the topic, which is the widest a findings search ever gets.
type VectorQuery struct {
	Embedding []float32
	Topic     string
	Class     string
	TopK      int
	// MinSimilarity is the floor below which a match is not worth returning.
	// It is not a promotion rule: every match returned is still checked against
	// the entry's own learned threshold and then against the sufficiency
	// ladder.
	MinSimilarity float64
}

// VectorMatch is one candidate and its cosine similarity.
type VectorMatch struct {
	Hash       string
	Similarity float64
}

// Leases is distributed mutual exclusion: the mechanism that makes n executors
// missing the same question at the same instant produce one call to the source
// instead of n.
//
// It is a lease rather than a lock, which is the distinction that matters when
// the holder is a process that can die: a lock is held until released and an
// owner that crashes holds it forever, while a lease expires and the work
// continues without it. Everything else here follows from that — the owner ID
// so a lease can be identified, the expiry so a crash is survivable, the
// renewal so slow-but-alive research is not mistaken for a crash, and the
// fencing token so an owner that *was* expired and has since been replaced
// cannot act as though it still holds anything.
type Leases interface {
	// Acquire takes the lease for key, reporting whether it was granted. On
	// refusal the returned lease describes the current holder, so a follower
	// knows how long it might reasonably wait.
	Acquire(ctx context.Context, key, owner string, ttl time.Duration) (Lease, bool, error)
	// Renew extends a held lease. It reports false when the lease has been
	// taken over, which is how a slow owner learns it has been fenced.
	Renew(ctx context.Context, l Lease, ttl time.Duration) (Lease, bool, error)
	// Release ends a lease, waking every follower immediately rather than
	// letting them wait out the expiry. It must verify the fencing token: a
	// fenced owner releasing the lease would release the *new* owner's
	// followers, onto a finding nobody has contributed yet.
	Release(ctx context.Context, l Lease) error
	// Peek reports the current state of a lease without taking it.
	Peek(ctx context.Context, key string) (Lease, bool, error)
}

// Lease is one distributed single-flight grant.
//
// Token is the fencing token: it increases every time the lease changes hands,
// so a holder that stalled past its expiry and woke to find itself replaced can
// be told apart from the live owner by comparing numbers rather than by trusting
// clocks. It is the standard fix for the problem that makes lease-based mutual
// exclusion subtle — the owner cannot tell "I am slow" from "I am dead" — and
// the reason renewal and release are token-checked rather than owner-checked.
type Lease struct {
	Key      string    `json:"key"`
	Owner    string    `json:"owner"`
	Token    int64     `json:"token"`
	Acquired time.Time `json:"acquired"`
	Expires  time.Time `json:"expires"`
	Released bool      `json:"released"`
	// Takeover reports that this lease was granted over a holder that had
	// expired without releasing — an executor that crashed, was killed, or was
	// paused past its horizon. It is counted rather than logged away, because a
	// backend seeing many takeovers is a backend whose lease TTL is shorter
	// than its research.
	Takeover bool `json:"takeover,omitempty"`
}

// Done reports whether a lease no longer stands in anyone's way: released by
// its owner, or expired underneath it.
func (l Lease) Done(now time.Time) bool {
	return l.Released || l.Expires.IsZero() || !l.Expires.After(now)
}

// Backend is a store, a vector index and a lease table together. Adapters
// usually implement all three over one system — the PostgreSQL adapter does,
// which is why leases need no Redis and vectors need no second database — but
// Compose exists for the setups that split them.
type Backend interface {
	Store
	VectorStore
	Leases
}

// Compose builds a Backend from three independent implementations, for a
// deployment that wants (say) findings in PostgreSQL and vectors in something
// purpose-built. Any of them may be nil: a nil VectorStore disables L2
// similarity search, a nil Leases disables cross-executor single-flight, and
// the gate degrades to the layer below rather than failing.
func Compose(s Store, v VectorStore, l Leases) Backend {
	return composed{Store: s, VectorStore: v, Leases: l}
}

type composed struct {
	Store
	VectorStore
	Leases
}

// --- Configuration ------------------------------------------------------

// SharedConfig declares this executor's connection to the distributed commons.
// The zero value beyond Backend is a working configuration; every duration has
// a default chosen to be right for a backend on the same network.
type SharedConfig struct {
	// Backend is where findings, vectors and leases live. Required.
	Backend Backend

	// Executor identifies this process to the others. It is the lease owner ID
	// and the executor recorded on every finding this process contributes.
	// Default: host:pid plus entropy, which is unique enough for a lease and
	// legible enough for a report.
	Executor string

	// LeaseTTL is how long a research lease is valid without renewal (default
	// 30s), and Heartbeat how often the owner renews it (default LeaseTTL/3).
	//
	// The TTL is the bound on how long a crashed executor can stall the
	// question it was researching, so it wants to be short; renewal is what
	// lets it be short without punishing research that is legitimately slower
	// than the TTL.
	LeaseTTL  time.Duration
	Heartbeat time.Duration

	// Poll and PollCeiling bound the backoff a follower waits with (defaults
	// 20ms and 400ms). Polling is the whole coordination mechanism: it needs no
	// pub/sub, no message broker and no connection the backend has to hold
	// open, and the cost of it is one indexed primary-key read per poll.
	Poll        time.Duration
	PollCeiling time.Duration

	// Timeout bounds any single backend call (default 3s). It is what keeps a
	// wedged backend from turning into wedged tasks: the call fails, the gate
	// counts a failure, and research proceeds as though the layer were not
	// configured.
	Timeout time.Duration

	// Fanout is how many candidates a lookup pulls (default 32) and TopK how
	// many the vector search returns (default 8).
	Fanout int
	TopK   int

	// Refresh is how long a finding copied out of the shared backend is trusted
	// locally before the gate re-consults L2 for it (default 60s).
	//
	// It is the staleness window for cross-executor invalidation, and it exists
	// because the alternative is worse. An adopted finding is a copy; a
	// retraction or revision on another executor cannot reach into this
	// process's memory. Re-validating on every local hit would put the network
	// back on the path this layer exists to keep off it, so instead a copy has
	// a horizon: past it, the local hit misses, L2 is consulted, and whatever
	// the commons now holds — a revision, nothing at all — is what gets served.
	// Locally learned findings are unaffected; only copies expire this way.
	Refresh time.Duration

	// Strict fails a research call when the backend is unavailable, instead of
	// falling back to researching without it.
	//
	// The default is to fail open, and that is the right default for a layer
	// whose job is to *avoid* calls: a backend outage should cost money, not
	// correctness. Strict is for the deployment where an uncoordinated executor
	// is worse than a stalled one — a metered source with a hard quota, say —
	// and it is deliberately explicit, because turning it on converts an
	// optimization into a dependency.
	Strict bool
}

const (
	defaultLeaseTTL    = 30 * time.Second
	defaultPoll        = 20 * time.Millisecond
	defaultPollCeiling = 400 * time.Millisecond
	defaultTimeout     = 3 * time.Second
	defaultFanout      = 32
	defaultTopK        = 8
	defaultRefresh     = 60 * time.Second
	// citeQueue bounds the citation write-behind. Citations are the one write
	// on the serve path, and a serve is supposed to cost microseconds, so they
	// go through a queue that drops rather than blocks: a lost justification
	// edge is a retraction that reports one fewer dependent, which is a real
	// cost and still much smaller than putting a network write in front of
	// every hit.
	citeQueue = 1024
	// verdictFanout bounds how many adjudications an adoption pulls across. A
	// finding accumulates one verdict per question it was ever considered for,
	// and a reader only needs the recent ones — the pairing it is about to
	// consider is either among them or has never been judged anywhere.
	verdictFanout = 64
)

func (c SharedConfig) normalize() SharedConfig {
	if c.Executor == "" {
		c.Executor = ExecutorID()
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = defaultLeaseTTL
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = c.LeaseTTL / 3
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = time.Second
	}
	if c.Poll <= 0 {
		c.Poll = defaultPoll
	}
	if c.PollCeiling < c.Poll {
		c.PollCeiling = max(defaultPollCeiling, c.Poll)
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
	if c.Fanout <= 0 {
		c.Fanout = defaultFanout
	}
	if c.TopK <= 0 {
		c.TopK = defaultTopK
	}
	if c.Refresh <= 0 {
		c.Refresh = defaultRefresh
	}
	return c
}

// ExecutorID mints an identifier for this process: host, pid and entropy.
//
// The entropy is not decoration. Two containers of one deployment share a
// hostname pattern and can share a pid, and a lease owner that collides is a
// lease two processes both believe they hold.
func ExecutorID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "executor"
	}
	if i := strings.IndexByte(host, '.'); i > 0 {
		host = host[:i]
	}
	return fmt.Sprintf("%s-%d-%04x", host, os.Getpid(), rand.Int31n(1<<16))
}

// --- The shared layer ---------------------------------------------------

// Shared is the gate's connection to the distributed commons: the backend, the
// policy for talking to it, the accounting for what it did, and the small
// amount of machinery — heartbeats, bounded waits, a citation queue — that
// makes a database into a coordination layer.
//
// One per executor, held by the gate. It is safe for concurrent use.
type Shared struct {
	cfg SharedConfig

	mu    sync.Mutex
	stats Stats

	cites   chan citation
	closing chan struct{}
	wg      sync.WaitGroup
	once    sync.Once
}

type citation struct {
	hash string
	dep  Dependent
}

// NewShared connects an executor to a distributed commons.
//
//	backend, err := pgstore.Open(ctx, dsn, pgstore.Options{Dimensions: 1536})
//	loom.NewFleet(
//	    loom.WithFindings(findings.Config{
//	        Gate:   []string{"mcp/web/search"},
//	        Shared: findings.NewShared(findings.SharedConfig{Backend: backend}),
//	    }),
//	)
//
// A nil return is impossible; a nil Backend simply produces a layer that never
// finds anything, which is the same as not configuring one.
func NewShared(cfg SharedConfig) *Shared {
	s := &Shared{
		cfg:     cfg.normalize(),
		cites:   make(chan citation, citeQueue),
		closing: make(chan struct{}),
	}
	s.wg.Add(1)
	go s.writeBehind()
	return s
}

// Backend returns the store this executor is sharing through.
func (s *Shared) Backend() Backend {
	if s == nil {
		return nil
	}
	return s.cfg.Backend
}

// Executor returns this process's identity in the commons.
func (s *Shared) Executor() string {
	if s == nil {
		return ""
	}
	return s.cfg.Executor
}

// Close drains the citation queue and closes the backend. A fleet does this for
// the layer it provisioned; a caller wiring a gate by hand should do it too,
// because the queue is where the serve path's writes are waiting.
func (s *Shared) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.once.Do(func() {
		close(s.closing)
		s.wg.Wait()
		if s.cfg.Backend != nil {
			err = s.cfg.Backend.Close()
		}
	})
	return err
}

// Flush waits for the citation queue to drain without closing anything. Tests
// and reports want it; the hot path never does.
func (s *Shared) Flush(ctx context.Context) {
	if s == nil {
		return
	}
	for {
		if len(s.cites) == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond):
		}
	}
}

func (s *Shared) ok() bool { return s != nil && s.cfg.Backend != nil }

// call bounds one backend operation and folds its outcome into the accounting.
// Every path into the backend goes through it, so "how long did L2 cost" and
// "how often did L2 fail" are measurements rather than estimates.
func (s *Shared) call(ctx context.Context, op string, fn func(context.Context) error) error {
	start := time.Now()
	cctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	err := fn(cctx)
	took := time.Since(start)

	s.mu.Lock()
	s.stats.RemoteLatency += took
	if err != nil {
		s.stats.BackendFailures++
	}
	s.mu.Unlock()

	if err != nil {
		return fmt.Errorf("findings: shared backend %s: %w", op, err)
	}
	return nil
}

func (s *Shared) count(fn func(*Stats)) {
	s.mu.Lock()
	fn(&s.stats)
	s.mu.Unlock()
}

// snapshot returns the L2 accounting, which the gate merges into its own.
func (s *Shared) snapshot() Stats {
	if s == nil {
		return Stats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// --- Lookup -------------------------------------------------------------

// candidates pulls the exact and class candidates for a question in one round
// trip. A backend failure returns nothing and is counted, so the caller's next
// move is the same one an empty commons would produce.
func (s *Shared) candidates(ctx context.Context, q Question) ([]Entry, error) {
	if !s.ok() {
		return nil, nil
	}
	var out []Entry
	err := s.call(ctx, "candidates", func(ctx context.Context) error {
		var err error
		out, err = s.cfg.Backend.Candidates(ctx, CandidateQuery{
			Topic: q.Topic, Key: q.Key(), Class: q.Class(), Limit: s.cfg.Fanout,
		})
		return err
	})
	return out, err
}

// nearest runs the vector search and resolves the matches into entries.
//
// Two round trips, deliberately: the vector store returns hashes and the store
// returns findings, so a deployment can put similarity search somewhere else
// without the gate learning a third shape. It is also the coldest path in the
// ladder — reached only when exact and class both came back with nothing — so
// the extra hop is paid exactly when the alternative is an external call.
func (s *Shared) nearest(ctx context.Context, q Question, vec []float32, floor float64) ([]Entry, error) {
	if !s.ok() || len(vec) == 0 {
		return nil, nil
	}
	start := time.Now()
	var matches []VectorMatch
	err := s.call(ctx, "nearest", func(ctx context.Context) error {
		var err error
		matches, err = s.cfg.Backend.Nearest(ctx, VectorQuery{
			Embedding: vec, Topic: q.Topic, Class: q.Class(),
			TopK: s.cfg.TopK, MinSimilarity: floor,
		})
		return err
	})
	s.count(func(st *Stats) { st.VectorLatency += time.Since(start) })
	if err != nil || len(matches) == 0 {
		return nil, err
	}
	hashes := make([]string, 0, len(matches))
	for _, m := range matches {
		hashes = append(hashes, m.Hash)
	}
	var out []Entry
	err = s.call(ctx, "fetch", func(ctx context.Context) error {
		var err error
		out, err = s.cfg.Backend.Fetch(ctx, hashes)
		return err
	})
	return out, err
}

// verdicts returns the adjudications recorded against a set of findings, so
// adopting them adopts what has already been decided about them.
func (s *Shared) verdicts(ctx context.Context, hashes []string) []Judgement {
	if !s.ok() || len(hashes) == 0 {
		return nil
	}
	var out []Judgement
	_ = s.call(ctx, "verdicts", func(ctx context.Context) error {
		var err error
		out, err = s.cfg.Backend.Verdicts(ctx, hashes, verdictFanout)
		return err
	})
	return out
}

// dependents returns everything the commons has recorded as resting on a
// finding, from every executor.
func (s *Shared) dependents(ctx context.Context, hash string) ([]Dependent, error) {
	if !s.ok() {
		return nil, nil
	}
	var out []Dependent
	err := s.call(ctx, "dependents", func(ctx context.Context) error {
		var err error
		out, err = s.cfg.Backend.Dependents(ctx, hash)
		return err
	})
	return out, err
}

// refresh is how long a copy taken from the backend is trusted locally.
func (s *Shared) refresh() time.Duration {
	if !s.ok() {
		return 0
	}
	return s.cfg.Refresh
}

// --- Contribution -------------------------------------------------------

// publish writes a finding to the shared backend and indexes its vector.
//
// It returns the entry the store settled on, which is not always the one that
// went in: two executors that independently learn the same thing converge on
// one entry, and the second one's Put comes back holding the first one's hash.
// The caller adopts that, so the executors' local ledgers converge too.
//
// A failure here is counted and returned, never raised: the research succeeded
// and the answer is correct. All that is lost is another executor's chance to
// reuse it.
func (s *Shared) publish(ctx context.Context, e Entry) (Entry, bool) {
	if !s.ok() {
		return Entry{}, false
	}
	e.Executor = s.cfg.Executor
	var stored Entry
	if err := s.call(ctx, "put", func(ctx context.Context) error {
		var err error
		stored, err = s.cfg.Backend.Put(ctx, e)
		return err
	}); err != nil {
		return Entry{}, false
	}
	s.count(func(st *Stats) { st.Published++ })

	if len(e.Vector) > 0 && stored.Hash != "" {
		_ = s.call(ctx, "upsert", func(ctx context.Context) error {
			return s.cfg.Backend.Upsert(ctx, Vector{
				Hash: stored.Hash, Topic: stored.Finding.Topic,
				Class: stored.Class, Embedding: e.Vector,
			})
		})
	}
	if len(stored.Vector) == 0 {
		stored.Vector = e.Vector
	}
	return stored, true
}

// cite queues a justification edge for the backend. It never blocks the caller
// and never fails it; an overflowing queue is counted and dropped.
func (s *Shared) cite(hash string, d Dependent) {
	if !s.ok() || hash == "" {
		return
	}
	select {
	case s.cites <- citation{hash: hash, dep: d}:
	default:
		s.count(func(st *Stats) { st.CitesDropped++ })
	}
}

func (s *Shared) writeBehind() {
	defer s.wg.Done()
	for {
		select {
		case c := <-s.cites:
			_ = s.call(context.Background(), "cite", func(ctx context.Context) error {
				return s.cfg.Backend.Cite(ctx, c.hash, c.dep)
			})
		case <-s.closing:
			// Drain what is already queued: these are serves that have happened,
			// and a retraction that cannot find them is a retraction that
			// under-reports.
			for {
				select {
				case c := <-s.cites:
					_ = s.call(context.Background(), "cite", func(ctx context.Context) error {
						return s.cfg.Backend.Cite(ctx, c.hash, c.dep)
					})
				default:
					return
				}
			}
		}
	}
}

// recordVerdict shares an adjudication and the threshold it moved, so the next
// executor to consider this pairing does not pay a model to reach the same
// conclusion.
func (s *Shared) recordVerdict(ctx context.Context, j Judgement, threshold float64) {
	if !s.ok() {
		return
	}
	j.Executor = s.cfg.Executor
	if j.Decided.IsZero() {
		j.Decided = time.Now()
	}
	_ = s.call(ctx, "verdict", func(ctx context.Context) error {
		return s.cfg.Backend.RecordVerdict(ctx, j)
	})
	if threshold > 0 {
		_ = s.call(ctx, "threshold", func(ctx context.Context) error {
			return s.cfg.Backend.SetThreshold(ctx, j.Hash, threshold)
		})
	}
}

// retract withdraws a claim for every executor and deactivates its vector.
func (s *Shared) retract(ctx context.Context, id, reason string, now time.Time) ([]Dependent, error) {
	if !s.ok() {
		return nil, nil
	}
	var deps []Dependent
	err := s.call(ctx, "retract", func(ctx context.Context) error {
		var err error
		deps, err = s.cfg.Backend.Retract(ctx, id, reason, now)
		return err
	})
	return deps, err
}

// forget deactivates a retracted finding's vector, so a withdrawn claim stops
// being a similarity candidate rather than merely failing the ladder.
func (s *Shared) forget(ctx context.Context, hash string) {
	if !s.ok() || hash == "" {
		return
	}
	_ = s.call(ctx, "forget", func(ctx context.Context) error {
		return s.cfg.Backend.Remove(ctx, hash)
	})
}

// topics summarizes what the whole commons holds, across executors.
func (s *Shared) topics(ctx context.Context) []TopicStat {
	if !s.ok() {
		return nil
	}
	var out []TopicStat
	_ = s.call(ctx, "topics", func(ctx context.Context) error {
		var err error
		out, err = s.cfg.Backend.Topics(ctx)
		return err
	})
	return out
}

// --- Distributed single flight ------------------------------------------

// acquire takes the research lease for a key, or reports who holds it.
func (s *Shared) acquire(ctx context.Context, key string) (Lease, bool, error) {
	if !s.ok() {
		return Lease{}, false, nil
	}
	var l Lease
	var held bool
	err := s.call(ctx, "acquire", func(ctx context.Context) error {
		var err error
		l, held, err = s.cfg.Backend.Acquire(ctx, key, s.cfg.Executor, s.cfg.LeaseTTL)
		return err
	})
	if err != nil {
		return Lease{}, false, err
	}
	s.count(func(st *Stats) {
		if held {
			st.Leader++
			if l.Takeover {
				st.LeaseTakeovers++
			}
			return
		}
		st.Follower++
	})
	return l, held, nil
}

// heartbeat renews a held lease until the returned stop function is called.
//
// It is what lets the TTL be short. Without renewal the TTL has to exceed the
// slowest research anyone will ever do — which means a crashed executor stalls
// the question for that long — and with it the TTL only has to exceed the time
// between two heartbeats.
func (s *Shared) heartbeat(ctx context.Context, l Lease) (stop func() Lease) {
	if !s.ok() {
		return func() Lease { return l }
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	current := l

	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(s.cfg.Heartbeat)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				mu.Lock()
				held := current
				mu.Unlock()
				var next Lease
				var still bool
				if err := s.call(ctx, "renew", func(ctx context.Context) error {
					var err error
					next, still, err = s.cfg.Backend.Renew(ctx, held, s.cfg.LeaseTTL)
					return err
				}); err != nil {
					continue
				}
				if !still {
					// Fenced: someone else owns this key now. The research in
					// flight is still valid and still worth contributing — what
					// is no longer ours is the right to release anyone.
					s.count(func(st *Stats) { st.LeaseLost++ })
					return
				}
				mu.Lock()
				current = next
				mu.Unlock()
			}
		}
	}()

	return func() Lease {
		close(done)
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		return current
	}
}

// release ends a lease, waking every follower at once.
func (s *Shared) release(ctx context.Context, l Lease) {
	if !s.ok() {
		return
	}
	_ = s.call(ctx, "release", func(ctx context.Context) error {
		return s.cfg.Backend.Release(ctx, l)
	})
}

// await waits for a lease to clear, polling with bounded exponential backoff.
//
// It reports whether the lease is done — released by its owner or expired
// underneath it — as opposed to the wait running out. The caller's next move
// differs: a lease that cleared means there is probably a finding to read,
// while a wait that ran out means the leader is slower than this caller is
// willing to be, and correctness now matters more than deduplication.
func (s *Shared) await(ctx context.Context, key string, deadline time.Time) bool {
	if !s.ok() {
		return false
	}
	wait := s.cfg.Poll
	for {
		now := time.Now()
		if !now.Before(deadline) {
			s.count(func(st *Stats) { st.LeaseTimeouts++ })
			return false
		}
		sleep := min(wait, time.Until(deadline))
		select {
		case <-ctx.Done():
			return false
		case <-time.After(sleep):
		}
		wait = min(wait*2, s.cfg.PollCeiling)

		l, found, err := s.peek(ctx, key)
		if err != nil {
			return false
		}
		if !found || l.Done(time.Now()) {
			return true
		}
	}
}

func (s *Shared) peek(ctx context.Context, key string) (Lease, bool, error) {
	var l Lease
	var found bool
	err := s.call(ctx, "peek", func(ctx context.Context) error {
		var err error
		l, found, err = s.cfg.Backend.Peek(ctx, key)
		return err
	})
	return l, found, err
}

// --- Failure policy -----------------------------------------------------

// failOpen decides what an unavailable backend means. It returns the error the
// caller should raise, which is nil unless strict mode was asked for — a layer
// that exists to avoid calls should cost money when it breaks, not
// correctness.
func (s *Shared) failOpen(err error) error {
	if err == nil {
		return nil
	}
	if s.cfg.Strict {
		return err
	}
	s.count(func(st *Stats) { st.FailedOpen++ })
	return nil
}

// ErrStrict wraps a backend failure raised because strict mode is on. It is a
// distinct error so a caller can tell "the commons is down" from "the research
// failed".
var ErrStrict = errors.New("findings: shared backend unavailable (strict mode)")
