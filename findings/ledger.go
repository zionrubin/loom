package findings

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/store"
)

// Entry is one finding as the ledger holds it: the immutable claim, plus
// everything about *this* recording of it that the claim itself must not carry.
//
// The split is load-bearing. Learned is a wall clock, and a wall clock inside
// the hashed body would make identical knowledge hash differently on every
// fleet — so it lives here, on the entry, exactly as Fleet.Post keeps timing out
// of a blackboard post and in the event stream. Everything else here is
// likewise about the recording rather than the claim: who learned it, how many
// independent agents have since reached the same conclusion, and where this
// entry's own near-match boundary currently sits.
type Entry struct {
	Seq     int     `json:"seq"`
	Hash    string  `json:"hash"`
	Finding Finding `json:"finding"`
	Key     string  `json:"key"`   // the question key it was learned under
	Class   string  `json:"class"` // topic + facets: the subject it is about
	// Knowledge is the hash of the claim alone. Entries sharing one are the
	// same conclusion reached independently; entries in one class with
	// different ones are a contradiction the ledger can see.
	Knowledge string    `json:"knowledge"`
	Learned   time.Time `json:"learned"`
	Learner   string    `json:"learner,omitempty"` // the run that contributed it
	// Executor names the process that learned it. On a single-process fleet it
	// is noise; across executors it is what makes a report able to say that a
	// finding this machine served was researched by another one, which is the
	// whole claim the distributed layer makes.
	Executor string `json:"executor,omitempty"`

	// Remote marks an entry copied out of the shared backend rather than
	// learned here, and Adopted when the copy was taken.
	//
	// The pair is what bounds cross-executor staleness. A copy cannot be
	// reached by a retraction on the executor that owns the claim, so it is
	// trusted for SharedConfig.Refresh and then re-consulted — the local hit
	// misses, L2 answers with whatever the commons now holds, and the copy is
	// refreshed or replaced. Locally learned entries carry neither field and
	// are unaffected.
	Remote  bool      `json:"remote,omitempty"`
	Adopted time.Time `json:"adopted,omitempty"`

	// Corroborations counts the independent rediscoveries of this claim. It
	// costs nothing to maintain — an agent that re-learns something the ledger
	// already knows produces a matching knowledge hash — and it is what
	// TopicPolicy.MinSources is checked against.
	Corroborations int `json:"corroborations,omitempty"`

	// Threshold is this entry's own near-match decision boundary, seeded from
	// the topic policy and moved by adjudication. One global similarity
	// threshold is the documented failure mode of embedding-similarity caching:
	// too low and it serves answers to questions nobody asked, too high and it
	// serves almost nothing. A boundary per entry, adjusted by the verdicts
	// that entry has actually received, is the cheap version of the same fix.
	Threshold float64 `json:"threshold,omitempty"`

	// Latency is how long the research that produced this entry took. It is
	// the saving that matters when the source is a tool rather than a model:
	// a search call costs no tokens and a great deal of wall clock, so a layer
	// that reported only dollars would report nothing at all about it.
	Latency time.Duration `json:"latency,omitempty"`

	// Vector is the embedded question text, present only when an Embedder was
	// configured. It is kept out of the finding so a ledger can be replayed by
	// a process with a different embedder, or none.
	Vector []float32 `json:"vector,omitempty"`
}

// Live reports whether the entry may still be served: not retracted, and not
// superseded by a later revision of the same claim.
func (e *Entry) Live() bool { return !e.Finding.Retracted }

// Support and Threshold are the two fields of an entry that change after it is
// indexed — corroboration as agents rediscover a claim, and the near-match
// boundary as adjudications move it — so both are read through the ledger's
// lock rather than off the struct.
//
// Everything else about an entry is written once, before it is reachable from
// any index, and can be read directly. These two cannot: a fleet has readers
// walking the class index while another agent's contribution is incrementing a
// corroboration count on an entry in it.

// Support is how many independent sources stand behind an entry: its own,
// plus every later agent that reached the same conclusion.
func (l *Ledger) Support(e *Entry) int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return max(len(e.Finding.Sources), 1) + e.Corroborations
}

// Threshold is the entry's current near-match boundary.
func (l *Ledger) Threshold(e *Entry) float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return e.Threshold
}

// Age is how long ago the entry was learned, against the given clock.
func (e *Entry) Age(now time.Time) time.Duration { return now.Sub(e.Learned) }

// Dependent is one place a finding was served: the run, stage, and task that
// built an answer on it.
//
// Keeping this index is what makes retraction mean something. Justification-
// based truth maintenance (Doyle, 1979) records which conclusions rest on which
// premises so that withdrawing a premise can find everything that has to be
// reconsidered; Loom already records exactly that for artifacts, in lineage, and
// this is the same relation for findings. The ledger reports the dependents — it
// does not re-run them, because whether stale conclusions are worth
// recomputing is a question about the caller's budget, not about the ledger.
type Dependent struct {
	RunID  string `json:"run_id,omitempty"`
	Stage  string `json:"stage,omitempty"`
	TaskID string `json:"task_id,omitempty"`
}

// Ledger is the shared, append-only store of findings, with the three indices
// the lookup ladder walks: exact question key, subject class, and — when an
// embedder is configured — the vectors of a class's entries.
//
// It is safe for concurrent use by every agent on a fleet, and with a state
// directory it survives the process: an append is a line of JSON, and reopening
// replays the log. That is the same shape the result cache uses, for the same
// reason — the durable form of an append-only structure is its log.
type Ledger struct {
	cas *store.CAS

	mu          sync.RWMutex
	entries     []*Entry
	byKey       map[string][]*Entry
	byClass     map[string][]*Entry
	byID        map[string][]*Entry
	byKnowledge map[string][]*Entry
	byHash      map[string]*Entry
	heads       map[string]*Entry
	deps        map[string][]Dependent
	verdicts    map[string]bool
	spend       map[string]topicSpend

	file *os.File
}

type topicSpend struct {
	n     int
	total float64
}

// NewLedger opens a ledger over cas. With dir non-empty the entry log persists
// at dir/findings.jsonl and is replayed on open, so a fleet restarted tomorrow
// starts with everything yesterday's fleet learned.
func NewLedger(cas *store.CAS, dir string) (*Ledger, error) {
	l := &Ledger{
		cas:         cas,
		byKey:       map[string][]*Entry{},
		byClass:     map[string][]*Entry{},
		byID:        map[string][]*Entry{},
		byKnowledge: map[string][]*Entry{},
		byHash:      map[string]*Entry{},
		heads:       map[string]*Entry{},
		deps:        map[string][]Dependent{},
		verdicts:    map[string]bool{},
		spend:       map[string]topicSpend{},
	}
	if dir == "" {
		return l, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "findings.jsonl")
	if data, err := os.ReadFile(path); err == nil {
		dec := json.NewDecoder(bytes.NewReader(data))
		for dec.More() {
			var e Entry
			if err := dec.Decode(&e); err != nil {
				break
			}
			l.index(&e)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l.file = f
	return l, nil
}

// Close flushes the persistent log, if any.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		err := l.file.Close()
		l.file = nil
		return err
	}
	return nil
}

// Append records a finding and returns the entry holding it.
//
// Three outcomes, and only the first spends anything. A claim the ledger has
// never seen becomes a new entry. A claim it already holds — same knowledge
// hash, same subject — corroborates the entry that holds it, which is how
// independent rediscovery strengthens a finding instead of duplicating it. A
// finding carrying an ID the ledger has a head for becomes the next revision of
// that claim, naming the hash it supersedes.
//
// Nothing is ever overwritten. The bytes behind a hash never change, so a task
// holding one — or a lineage entry naming one — is never disturbed by a later
// contribution.
func (l *Ledger) Append(in Entry) (*Entry, error) {
	f, key, class := in.Finding, in.Key, in.Class
	if f.Topic == "" {
		return nil, fmt.Errorf("findings: a finding must name a topic")
	}
	knowledge := f.Knowledge()

	l.mu.Lock()
	// Rediscovery: the same claim about the same subject, learned again.
	// Corroboration is a property of the recording, not of the claim, so it is
	// the only field an append may change on an existing entry — and it changes
	// the entry, never the finding, whose hash stays the address it always was.
	for _, e := range l.byKnowledge[knowledge] {
		if e.Class == class && e.Live() {
			e.Corroborations++
			if key != "" && key != e.Key {
				// A second phrasing of a question that reached a claim we
				// already hold is worth indexing exactly, so the next asker
				// with those words never reaches the near tier at all.
				l.byKey[key] = append(l.byKey[key], e)
			}
			l.creditLocked(f)
			e2 := e
			l.mu.Unlock()
			return e2, nil
		}
	}

	if f.ID == "" {
		f.ID = core.NewID("finding")
		f.Rev = 1
	} else if head, ok := l.heads[f.ID]; ok {
		f.Rev = head.Finding.Rev + 1
		f.Supersedes = head.Hash
	} else if f.Rev == 0 {
		f.Rev = 1
	}
	f = f.canonical()

	blob, err := json.Marshal(f)
	if err != nil {
		l.mu.Unlock()
		return nil, fmt.Errorf("findings: finding must be JSON-serializable: %w", err)
	}
	hash, err := l.cas.Put(blob)
	if err != nil {
		l.mu.Unlock()
		return nil, err
	}

	e := &in
	e.Hash, e.Finding, e.Knowledge = hash, f, knowledge
	l.index(e)
	l.creditLocked(f)
	l.persistLocked(e)
	l.mu.Unlock()
	return e, nil
}

// Adopt copies an entry out of the shared backend into this process's ledger,
// reporting whether it was new here.
//
// It is Append's sibling for findings that arrive rather than happen, and the
// differences are all in what it must *not* do. It does not corroborate: a copy
// of a claim is not an independent rediscovery of it, and counting it as one
// would let a finding bootstrap its own support by being read. It does not
// re-derive identity: the shared entry's hash, ID and revision are the ones
// every executor already agrees on. And it does not overwrite a claim's local
// history — a hash the ledger already holds is refreshed in place, so a task
// holding a pointer to it sees the newer corroboration count rather than a
// second copy of its own entry.
//
// The hash is checked against the bytes. A shared store is another process's
// memory reached over a socket, and content addressing is only worth anything
// if somebody verifies the address; a mismatch means the entry did not survive
// the round trip intact, and it is refused rather than indexed under a name it
// does not have.
func (l *Ledger) Adopt(e Entry, now time.Time) (*Entry, bool, error) {
	if e.Hash == "" {
		return nil, false, fmt.Errorf("findings: a shared entry must carry its hash")
	}
	if got := e.Finding.Hash(); got != e.Hash {
		return nil, false, fmt.Errorf("findings: shared entry %s does not hash to its bytes (%s)", e.Hash, got)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if held, ok := l.byHash[e.Hash]; ok {
		// Refresh the parts that are about the recording rather than the claim.
		// Corroboration only ever rises: another executor's count includes
		// rediscoveries this one has not seen, and this one's includes
		// rediscoveries it has not yet published.
		held.Corroborations = max(held.Corroborations, e.Corroborations)
		if e.Threshold > 0 {
			held.Threshold = e.Threshold
		}
		if len(held.Vector) == 0 {
			held.Vector = e.Vector
		}
		if held.Remote {
			held.Adopted = now
		}
		return held, false, nil
	}

	blob, err := json.Marshal(e.Finding.canonical())
	if err != nil {
		return nil, false, err
	}
	if _, err := l.cas.Put(blob); err != nil {
		return nil, false, err
	}

	adopted := e
	adopted.Remote, adopted.Adopted = true, now
	l.index(&adopted)
	l.creditLocked(adopted.Finding)
	l.persistLocked(&adopted)
	return &adopted, true, nil
}

// SeedVerdict installs an adjudication decided elsewhere, without moving the
// entry's own threshold — the executor that made the call already moved it, and
// the value came across with the entry.
func (l *Ledger) SeedVerdict(qKey, hash string, ok bool) {
	if qKey == "" || hash == "" {
		return
	}
	l.mu.Lock()
	l.verdicts[qKey+"|"+hash] = ok
	l.mu.Unlock()
}

// Revisions returns every recorded revision of a claim, oldest first.
func (l *Ledger) Revisions(id string) []*Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]*Entry(nil), l.byID[id]...)
}

// Entry returns the entry holding a content hash, if this ledger holds it.
func (l *Ledger) Entry(hash string) (*Entry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	e, ok := l.byHash[hash]
	return e, ok
}

// index wires an entry into every lookup structure. Callers hold the lock.
func (l *Ledger) index(e *Entry) {
	e.Seq = len(l.entries)
	l.entries = append(l.entries, e)
	if e.Hash != "" {
		l.byHash[e.Hash] = e
	}
	if e.Key != "" {
		l.byKey[e.Key] = append(l.byKey[e.Key], e)
	}
	if e.Class != "" {
		l.byClass[e.Class] = append(l.byClass[e.Class], e)
	}
	if e.Knowledge != "" {
		l.byKnowledge[e.Knowledge] = append(l.byKnowledge[e.Knowledge], e)
	}
	if id := e.Finding.ID; id != "" {
		l.byID[id] = append(l.byID[id], e)
		if head, ok := l.heads[id]; !ok || e.Finding.Rev >= head.Finding.Rev {
			l.heads[id] = e
		}
	}
}

// creditLocked folds a finding's research cost into its topic's running mean,
// which is what the gate's break-even rule reads before it is willing to spend
// a model call adjudicating a near match. Callers hold the lock.
func (l *Ledger) creditLocked(f Finding) {
	s := l.spend[f.Topic]
	s.n++
	s.total += f.Cost.CostUSD
	l.spend[f.Topic] = s
}

func (l *Ledger) persistLocked(e *Entry) {
	if l.file == nil {
		return
	}
	line, err := json.Marshal(e)
	if err != nil {
		return
	}
	_, _ = l.file.Write(append(line, '\n'))
}

// Exact returns the live entries learned under a question key, newest first.
func (l *Ledger) Exact(key string) []*Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return liveHeads(l.byKey[key], l.heads)
}

// Class returns the live entries about one subject, newest first. It is the
// second tier: everything anyone has learned about this topic and these facets,
// however the question was worded.
func (l *Ledger) Class(class string) []*Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return liveHeads(l.byClass[class], l.heads)
}

// liveHeads filters a candidate list down to current, unretracted revisions,
// newest first. A superseded revision stays resolvable by hash — lineage names
// it — but it is never a candidate. Callers hold at least the read lock.
func liveHeads(in []*Entry, heads map[string]*Entry) []*Entry {
	out := make([]*Entry, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, e := range in {
		if !e.Live() {
			continue
		}
		if id := e.Finding.ID; id != "" {
			if head, ok := heads[id]; ok && head != e {
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
		}
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq > out[j].Seq })
	return out
}

// Head returns the current revision of a claim.
func (l *Ledger) Head(id string) (*Entry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	e, ok := l.heads[id]
	return e, ok
}

// Get resolves a finding by content hash, including superseded and retracted
// revisions. Lineage names hashes, so a hash must stay resolvable forever
// however the claim has since been revised.
func (l *Ledger) Get(hash string) (Finding, bool) {
	blob, ok := l.cas.Get(hash)
	if !ok {
		return Finding{}, false
	}
	var f Finding
	if err := json.Unmarshal(blob, &f); err != nil {
		return Finding{}, false
	}
	return f, true
}

// Cite records that a finding was served to a task. It is the justification
// edge retraction later walks.
func (l *Ledger) Cite(hash string, d Dependent) {
	if hash == "" || (d.RunID == "" && d.TaskID == "") {
		return
	}
	l.mu.Lock()
	l.deps[hash] = append(l.deps[hash], d)
	l.mu.Unlock()
}

// Dependents returns everything that was served a finding, by hash.
func (l *Ledger) Dependents(hash string) []Dependent {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]Dependent(nil), l.deps[hash]...)
}

// Retract withdraws a claim and returns everything that had already been served
// one of its revisions.
//
// It appends a retracted revision rather than deleting anything: the head stops
// being servable, every prior hash stays resolvable, and the reason is on the
// record. That is the same choice temporal knowledge graphs make in writing an
// invalidation timestamp instead of removing an edge — a ledger that forgets
// cannot answer what it believed when it produced a conclusion, which is
// precisely the question a retraction makes urgent.
func (l *Ledger) Retract(id, reason string, now time.Time) ([]Dependent, error) {
	l.mu.Lock()
	head, ok := l.heads[id]
	if !ok {
		l.mu.Unlock()
		return nil, fmt.Errorf("findings: no finding %q", id)
	}
	f := head.Finding
	f.Rev++
	f.Retracted = true
	f.Note = reason
	f.Supersedes = head.Hash

	blob, err := json.Marshal(f)
	if err != nil {
		l.mu.Unlock()
		return nil, err
	}
	hash, err := l.cas.Put(blob)
	if err != nil {
		l.mu.Unlock()
		return nil, err
	}
	e := &Entry{
		Seq: len(l.entries), Hash: hash, Finding: f, Key: head.Key,
		Class: head.Class, Knowledge: head.Knowledge, Learned: now,
		Learner: head.Learner, Threshold: head.Threshold,
	}
	l.index(e)
	l.persistLocked(e)

	var out []Dependent
	for _, rev := range l.byID[id] {
		out = append(out, l.deps[rev.Hash]...)
	}
	l.mu.Unlock()
	return out, nil
}

// Conflicts reports the live claims about one subject that disagree: entries in
// the class whose knowledge hashes differ while their coverage overlaps.
//
// Two findings about one subject that answer different questions are not a
// conflict, which is why coverage overlap is part of the test. Two that answer
// the same question differently are one, and the ledger surfaces it rather than
// silently serving whichever was indexed first.
func (l *Ledger) Conflicts(class string) [][]*Entry {
	entries := l.Class(class)
	byKnowledge := map[string][]*Entry{}
	var order []string
	for _, e := range entries {
		if _, seen := byKnowledge[e.Knowledge]; !seen {
			order = append(order, e.Knowledge)
		}
		byKnowledge[e.Knowledge] = append(byKnowledge[e.Knowledge], e)
	}
	if len(order) < 2 {
		return nil
	}
	var out [][]*Entry
	for i := 0; i < len(order); i++ {
		for j := i + 1; j < len(order); j++ {
			a, b := byKnowledge[order[i]][0], byKnowledge[order[j]][0]
			if overlaps(a.Finding.covers(), b.Finding.covers()) {
				out = append(out, []*Entry{a, b})
			}
		}
	}
	return out
}

func overlaps(a, b []string) bool {
	set := make(map[string]struct{}, len(a))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := set[s]; ok {
			return true
		}
	}
	return false
}

// Verdict returns a memoized adjudication for a (question, finding) pair.
//
// Memoizing it is what makes adjudication affordable at all: the expensive
// check runs once per pairing however many tasks reach it, so a fleet of a
// hundred agents asking one near-miss question pays for one judgement rather
// than a hundred.
func (l *Ledger) Verdict(qKey, hash string) (bool, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	v, ok := l.verdicts[qKey+"|"+hash]
	return v, ok
}

// RecordVerdict memoizes an adjudication and moves the entry's own near-match
// boundary: a rejection at similarity s raises the boundary above s so the same
// mistake is not re-made, an acceptance lowers it toward s so the same success
// is reachable without a second judgement.
func (l *Ledger) RecordVerdict(qKey string, e *Entry, similarity float64, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.verdicts[qKey+"|"+e.Hash] = ok
	if similarity <= 0 || similarity > 1 {
		return
	}
	if ok {
		if similarity < e.Threshold {
			e.Threshold = math.Max(similarity, nearFloor)
		}
		return
	}
	if similarity >= e.Threshold {
		e.Threshold = math.Min(similarity+nearStep, 1)
	}
}

const (
	// nearFloor is how far an entry's own boundary may be lowered by
	// accumulated acceptances. Learning per entry is worth doing; learning all
	// the way down to "any two questions about this topic are the same
	// question" is not.
	nearFloor = 0.80
	nearStep  = 0.01
)

// MeanCost is the average research cost of a topic's findings, and the number
// the gate's break-even rule weighs before spending a model call on
// adjudication. It is a measurement rather than an estimate because the ledger
// records what each contribution cost.
func (l *Ledger) MeanCost(topic string) float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	s := l.spend[topic]
	if s.n == 0 {
		return 0
	}
	return s.total / float64(s.n)
}

// Len returns the number of entries the ledger holds, including superseded and
// retracted revisions.
func (l *Ledger) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// Entries returns every entry in the order it was appended.
func (l *Ledger) Entries() []*Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]*Entry(nil), l.entries...)
}

// Topics summarizes the ledger by topic: how many live claims each holds and
// what their research cost. It is what a report prints.
func (l *Ledger) Topics() []TopicStat {
	l.mu.RLock()
	byTopic := map[string]*TopicStat{}
	for _, e := range l.entries {
		t := e.Finding.Topic
		st, ok := byTopic[t]
		if !ok {
			st = &TopicStat{Topic: t}
			byTopic[t] = st
		}
		st.Entries++
		if !e.Live() {
			st.Retracted++
			continue
		}
		if head, ok := l.heads[e.Finding.ID]; ok && head != e {
			continue
		}
		st.Live++
		st.Corroborations += e.Corroborations
		if e.Finding.NoEvidence {
			st.Negative++
		}
		st.Cost.Add(e.Finding.Cost)
	}
	l.mu.RUnlock()

	out := make([]TopicStat, 0, len(byTopic))
	for _, st := range byTopic {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}

// TopicStat is one topic's row in a ledger summary.
type TopicStat struct {
	Topic          string
	Entries        int // every revision ever appended
	Live           int // current, unretracted claims
	Retracted      int
	Negative       int // searched-and-found-nothing results
	Corroborations int
	Cost           core.Usage // what learning this topic cost, once
}
