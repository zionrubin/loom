// Package backendtest is the conformance suite every findings backend must
// pass: one set of tests, run against PostgreSQL, against a shared directory,
// and against whatever anybody writes next.
//
// It exists because "replaceable through clean interfaces" is a claim, and the
// only way to hold an interface to it is to have more than one implementation
// and one definition of what they both owe. Everything the gate assumes about a
// backend is written down here as a test rather than as a paragraph: that
// rediscovery corroborates instead of duplicating, that a retracted claim stops
// being a candidate while its bytes stay resolvable, that a lease is granted to
// exactly one of two racing executors, that an expired lease can be taken over
// and a fenced owner cannot release the taker's.
//
// A new backend is finished when this passes:
//
//	func TestConformance(t *testing.T) {
//	    backendtest.Run(t, func(t *testing.T) findings.Backend { return open(t) })
//	}
package backendtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/findings"
)

// Open builds a backend for one subtest. Each call must produce an empty
// commons, and the implementation is responsible for cleaning it up (t.Cleanup
// is the natural place).
type Open func(t *testing.T) findings.Backend

// Run executes the conformance suite against a backend.
func Run(t *testing.T, open Open) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(t *testing.T, b findings.Backend)
	}{
		{"PutAndLookUpByKeyAndClass", testPutAndLookup},
		{"RediscoveryCorroborates", testRediscovery},
		{"RevisionsSupersede", testRevisions},
		{"RetractionHidesAndReportsDependents", testRetraction},
		{"CitationsAccumulate", testCitations},
		{"VerdictsAndThresholdsPersist", testVerdicts},
		{"TopicsSummarize", testTopics},
		{"VectorSearchFiltersAndRanks", testVectors},
		{"EntriesCarryTheirVector", testVectorRoundTrip},
		{"LeaseIsGrantedToOne", testLeaseExclusion},
		{"LeaseExpiryAllowsTakeover", testLeaseTakeover},
		{"FencedOwnerCannotReleaseOrRenew", testLeaseFencing},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := open(t)
			tc.fn(t, b)
		})
	}
}

// --- helpers ------------------------------------------------------------

// Entry builds a well-formed entry: the finding, its content address, and the
// three indices a store files it under. Backends are entitled to assume the
// hash matches the bytes, because the gate refuses entries where it does not.
func Entry(topic, text string, facets map[string]string, answer string, fields map[string]any) findings.Entry {
	q := findings.Question{Topic: topic, Text: text, Facets: facets}.Normalize()
	f := findings.Finding{
		ID:      "finding_" + topic + "_" + answer,
		Rev:     1,
		Topic:   q.Topic,
		Asked:   q,
		Answer:  answer,
		Fields:  fields,
		Sources: []findings.Source{{Tool: "search", Host: "api.example.com"}},
		Cost:    core.Usage{Requests: 1, CostUSD: 0.01},
	}
	return findings.Entry{
		Hash: f.Hash(), Finding: f, Key: q.Key(), Class: q.Class(),
		Knowledge: f.Knowledge(), Learned: time.Now().UTC().Truncate(time.Millisecond),
		Learner: "run_test", Executor: "executor-a", Threshold: 0.92,
		Latency: 900 * time.Millisecond,
	}
}

func put(t *testing.T, b findings.Backend, e findings.Entry) findings.Entry {
	t.Helper()
	stored, err := b.Put(context.Background(), e)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if stored.Hash == "" {
		t.Fatalf("put returned an entry with no hash")
	}
	return stored
}

func candidates(t *testing.T, b findings.Backend, q findings.Question) []findings.Entry {
	t.Helper()
	out, err := b.Candidates(context.Background(), findings.CandidateQuery{
		Topic: q.Topic, Key: q.Key(), Class: q.Class(), Limit: 16,
	})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	return out
}

func hashes(entries []findings.Entry) map[string]findings.Entry {
	out := make(map[string]findings.Entry, len(entries))
	for _, e := range entries {
		out[e.Hash] = e
	}
	return out
}

// --- findings -----------------------------------------------------------

func testPutAndLookup(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	e := Entry("company", "northwind revenue", map[string]string{"co": "northwind"},
		"Revenue was $4.2bn", map[string]any{"revenue": "$4.2bn"})
	stored := put(t, b, e)

	if stored.Hash != e.Hash {
		t.Fatalf("a new finding must keep its content address: %s vs %s", stored.Hash, e.Hash)
	}

	// The exact tier: the same question, however it is re-asked.
	q := e.Finding.Asked
	got := candidates(t, b, q)
	if _, ok := hashes(got)[e.Hash]; !ok {
		t.Fatalf("the exact key must find the finding recorded under it (%d candidates)", len(got))
	}

	// The class tier: a different phrasing of the same subject.
	other := findings.Question{Topic: "company", Text: "how much did northwind earn",
		Facets: map[string]string{"co": "northwind"}}
	if _, ok := hashes(candidates(t, b, other))[e.Hash]; !ok {
		t.Fatalf("the subject class must find a differently worded question's finding")
	}

	// A different subject must not.
	elsewhere := findings.Question{Topic: "company", Text: "contoso revenue",
		Facets: map[string]string{"co": "contoso"}}
	if _, ok := hashes(candidates(t, b, elsewhere))[e.Hash]; ok {
		t.Fatalf("a different subject must not be a candidate")
	}

	// Fetch resolves by hash, and what comes back must survive the round trip
	// intact — the gate checks the address against the bytes and refuses
	// anything that does not.
	back, err := b.Fetch(ctx, []string{e.Hash})
	if err != nil || len(back) != 1 {
		t.Fatalf("fetch by hash: %v (%d entries)", err, len(back))
	}
	if got := back[0].Finding.Hash(); got != e.Hash {
		t.Fatalf("a finding must hash to its address after a round trip: %s != %s", got, e.Hash)
	}
	if back[0].Finding.Answer != e.Finding.Answer {
		t.Fatalf("answer did not survive: %q", back[0].Finding.Answer)
	}
	if back[0].Learner != "run_test" || back[0].Executor != "executor-a" {
		t.Fatalf("provenance did not survive: learner=%q executor=%q", back[0].Learner, back[0].Executor)
	}
	if back[0].Latency != 900*time.Millisecond {
		t.Fatalf("research latency did not survive: %s", back[0].Latency)
	}
	if back[0].Finding.Cost.CostUSD != 0.01 {
		t.Fatalf("research cost did not survive: %v", back[0].Finding.Cost)
	}
}

func testRediscovery(t *testing.T, b findings.Backend) {
	first := Entry("company", "when was northwind founded", map[string]string{"co": "northwind"},
		"Founded in 1996", map[string]any{"founded": 1996})
	put(t, b, first)

	// The same claim, reached independently and worded differently. It is one
	// finding with two exact keys and a corroboration, not two rival claims.
	second := Entry("company", "founding year of northwind", map[string]string{"co": "northwind"},
		"Founded in 1996", map[string]any{"founded": 1996})
	second.Finding.ID = "finding_from_another_executor"
	second.Executor = "executor-b"
	second.Hash = second.Finding.Hash()
	stored := put(t, b, second)

	if stored.Hash != first.Hash {
		t.Fatalf("rediscovery must converge on the entry already stored, got %s want %s",
			stored.Hash, first.Hash)
	}
	if stored.Corroborations != 1 {
		t.Fatalf("corroborations = %d, want 1", stored.Corroborations)
	}
	// And the second phrasing is now an exact-tier key for the first entry, so
	// the next executor asking it that way never reaches similarity search.
	if _, ok := hashes(candidates(t, b, second.Finding.Asked))[first.Hash]; !ok {
		t.Fatalf("a phrasing that reached a known claim must be indexed exactly")
	}
	all := candidates(t, b, first.Finding.Asked)
	if len(all) != 1 {
		t.Fatalf("two rediscoveries of one claim = 1 live entry, got %d", len(all))
	}
	if all[0].Corroborations != 1 {
		t.Fatalf("corroboration must be readable, got %d", all[0].Corroborations)
	}
}

func testRevisions(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	first := Entry("company", "northwind headcount", map[string]string{"co": "northwind"},
		"12,000 staff", map[string]any{"headcount": 12000})
	put(t, b, first)

	second := first
	second.Finding.Rev = 2
	second.Finding.Answer = "12,400 staff"
	second.Finding.Fields = map[string]any{"headcount": 12400}
	second.Finding.Supersedes = first.Hash
	second.Knowledge = second.Finding.Knowledge()
	second.Hash = second.Finding.Hash()
	put(t, b, second)

	live := candidates(t, b, first.Finding.Asked)
	if len(live) != 1 || live[0].Hash != second.Hash {
		t.Fatalf("only the head revision may be a candidate, got %d", len(live))
	}
	// The superseded revision stays resolvable: lineage names it.
	back, err := b.Fetch(ctx, []string{first.Hash})
	if err != nil || len(back) != 1 {
		t.Fatalf("a superseded revision must stay resolvable by hash: %v (%d)", err, len(back))
	}
}

func testRetraction(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	e := Entry("company", "northwind litigation", map[string]string{"co": "northwind"},
		"Two open matters", map[string]any{"litigation": "two"})
	put(t, b, e)

	dep := findings.Dependent{RunID: "run_1", Stage: "research", TaskID: "task_9"}
	if err := b.Cite(ctx, e.Hash, dep); err != nil {
		t.Fatalf("cite: %v", err)
	}

	deps, err := b.Retract(ctx, e.Finding.ID, "the source published a correction", time.Now().UTC())
	if err != nil {
		t.Fatalf("retract: %v", err)
	}
	if len(deps) != 1 || deps[0] != dep {
		t.Fatalf("retraction must report what rested on the claim, got %v", deps)
	}
	if got := candidates(t, b, e.Finding.Asked); len(got) != 0 {
		t.Fatalf("a retracted claim must not be a candidate, got %d", len(got))
	}
	if back, err := b.Fetch(ctx, []string{e.Hash}); err != nil || len(back) != 1 {
		t.Fatalf("a retracted revision must stay resolvable by hash: %v (%d)", err, len(back))
	}
	// And its vector stops being a similarity candidate.
	if err := b.Upsert(ctx, findings.Vector{Hash: e.Hash, Topic: "company", Class: e.Class,
		Embedding: []float32{1, 0, 0}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	matches, err := b.Nearest(ctx, findings.VectorQuery{
		Embedding: []float32{1, 0, 0}, Topic: "company", TopK: 4})
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	for _, m := range matches {
		if m.Hash == e.Hash {
			t.Fatalf("a retracted claim must not be a similarity candidate")
		}
	}
}

func testCitations(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	e := Entry("company", "contoso revenue", map[string]string{"co": "contoso"},
		"$880m", map[string]any{"revenue": "$880m"})
	put(t, b, e)

	want := []findings.Dependent{
		{RunID: "run_1", Stage: "research", TaskID: "task_1"},
		{RunID: "run_2", Stage: "research", TaskID: "task_2"},
	}
	for _, d := range want {
		if err := b.Cite(ctx, e.Hash, d); err != nil {
			t.Fatalf("cite: %v", err)
		}
	}
	// The same serve recorded twice is one dependent: a retraction reports what
	// rests on a claim, not how many times it was asked.
	if err := b.Cite(ctx, e.Hash, want[0]); err != nil {
		t.Fatalf("cite: %v", err)
	}
	got, err := b.Dependents(ctx, e.Hash)
	if err != nil {
		t.Fatalf("dependents: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("dependents = %d, want 2 (%v)", len(got), got)
	}
}

func testVerdicts(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	e := Entry("company", "fabrikam profile", map[string]string{"co": "fabrikam"},
		"$12.6bn revenue", map[string]any{"revenue": "$12.6bn"})
	put(t, b, e)

	j := findings.Judgement{
		QuestionKey: "qkey_1", Hash: e.Hash, OK: true, Similarity: 0.88,
		Decided: time.Now().UTC(), Executor: "executor-a",
	}
	if err := b.RecordVerdict(ctx, j); err != nil {
		t.Fatalf("record verdict: %v", err)
	}
	if err := b.SetThreshold(ctx, e.Hash, 0.85); err != nil {
		t.Fatalf("set threshold: %v", err)
	}

	got, err := b.Verdicts(ctx, []string{e.Hash}, 16)
	if err != nil {
		t.Fatalf("verdicts: %v", err)
	}
	if len(got) != 1 || got[0].QuestionKey != "qkey_1" || !got[0].OK {
		t.Fatalf("the adjudication must survive: %v", got)
	}
	// The learned threshold rides with the entry, so an executor that adopts it
	// inherits the boundary the judgement moved rather than the topic default.
	back := candidates(t, b, e.Finding.Asked)
	if len(back) != 1 {
		t.Fatalf("expected the entry back, got %d", len(back))
	}
	if back[0].Threshold != 0.85 {
		t.Fatalf("threshold = %v, want 0.85", back[0].Threshold)
	}

	// Re-deciding a pairing replaces the verdict rather than accumulating.
	j.OK = false
	if err := b.RecordVerdict(ctx, j); err != nil {
		t.Fatalf("record verdict: %v", err)
	}
	got, err = b.Verdicts(ctx, []string{e.Hash}, 16)
	if err != nil || len(got) != 1 || got[0].OK {
		t.Fatalf("a re-decided pairing must hold one current verdict: %v (%v)", got, err)
	}
}

func testTopics(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	put(t, b, Entry("company", "northwind revenue", map[string]string{"co": "northwind"},
		"$4.2bn", map[string]any{"revenue": "$4.2bn"}))
	put(t, b, Entry("company", "contoso revenue", map[string]string{"co": "contoso"},
		"$880m", map[string]any{"revenue": "$880m"}))

	negative := Entry("filings", "litware sec filings", map[string]string{"co": "litware"}, "", nil)
	negative.Finding.NoEvidence = true
	negative.Finding.Covers = []string{"filings"}
	negative.Knowledge = negative.Finding.Knowledge()
	negative.Hash = negative.Finding.Hash()
	put(t, b, negative)

	stats, err := b.Topics(ctx)
	if err != nil {
		t.Fatalf("topics: %v", err)
	}
	byTopic := map[string]findings.TopicStat{}
	for _, s := range stats {
		byTopic[s.Topic] = s
	}
	if got := byTopic["company"].Live; got != 2 {
		t.Fatalf("company live = %d, want 2", got)
	}
	if got := byTopic["filings"].Negative; got != 1 {
		t.Fatalf("negative results must be counted, got %d", got)
	}
	if got := byTopic["company"].Cost.CostUSD; got < 0.019 || got > 0.021 {
		t.Fatalf("topic cost = %v, want ~0.02", got)
	}
}

// --- vectors ------------------------------------------------------------

func testVectors(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	near := Entry("company", "northwind revenue", map[string]string{"co": "northwind"},
		"$4.2bn", map[string]any{"revenue": "$4.2bn"})
	far := Entry("company", "contoso litigation", map[string]string{"co": "contoso"},
		"none disclosed", map[string]any{"litigation": "none"})
	other := Entry("weather", "rain in seattle", map[string]string{"city": "seattle"},
		"wet", map[string]any{"rain": true})
	put(t, b, near)
	put(t, b, far)
	put(t, b, other)

	vecs := []findings.Vector{
		{Hash: near.Hash, Topic: "company", Class: near.Class, Embedding: []float32{1, 0, 0}},
		{Hash: far.Hash, Topic: "company", Class: far.Class, Embedding: []float32{0, 1, 0}},
		{Hash: other.Hash, Topic: "weather", Class: other.Class, Embedding: []float32{1, 0, 0}},
	}
	for _, v := range vecs {
		if err := b.Upsert(ctx, v); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	// Ranked by cosine similarity, filtered to the topic.
	matches, err := b.Nearest(ctx, findings.VectorQuery{
		Embedding: []float32{0.99, 0.01, 0}, Topic: "company", TopK: 4})
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	if len(matches) == 0 || matches[0].Hash != near.Hash {
		t.Fatalf("the most similar vector must rank first, got %v", matches)
	}
	if matches[0].Similarity < 0.9 {
		t.Fatalf("similarity = %v, want ≈1 for a near-identical vector", matches[0].Similarity)
	}
	for _, m := range matches {
		if m.Hash == other.Hash {
			t.Fatalf("a different topic must not be searched")
		}
	}

	// Narrowed to one subject when the question has facets to pin it with.
	scoped, err := b.Nearest(ctx, findings.VectorQuery{
		Embedding: []float32{0, 1, 0}, Topic: "company", Class: far.Class, TopK: 4})
	if err != nil {
		t.Fatalf("nearest by class: %v", err)
	}
	for _, m := range scoped {
		if m.Hash != far.Hash {
			t.Fatalf("a class filter must not return other subjects, got %v", scoped)
		}
	}

	// The floor is a floor: a vector below it is not worth returning.
	orthogonal, err := b.Nearest(ctx, findings.VectorQuery{
		Embedding: []float32{0, 0, 1}, Topic: "company", TopK: 4, MinSimilarity: 0.5})
	if err != nil {
		t.Fatalf("nearest with floor: %v", err)
	}
	if len(orthogonal) != 0 {
		t.Fatalf("nothing similar must return nothing, got %v", orthogonal)
	}

	// Removal deactivates.
	if err := b.Remove(ctx, near.Hash); err != nil {
		t.Fatalf("remove: %v", err)
	}
	after, err := b.Nearest(ctx, findings.VectorQuery{
		Embedding: []float32{1, 0, 0}, Topic: "company", TopK: 4})
	if err != nil {
		t.Fatalf("nearest after remove: %v", err)
	}
	for _, m := range after {
		if m.Hash == near.Hash {
			t.Fatalf("a removed vector must not be a candidate")
		}
	}
}

// testVectorRoundTrip is the requirement that is easy to miss and expensive to
// miss: an entry must come back carrying the embedding it went in with.
//
// A backend that stores vectors perfectly and returns entries without them
// passes every similarity search and still breaks the layer. The reader adopts
// the finding into its own ledger, its own near tier cannot score a vectorless
// entry, and every paraphrase it is asked afterwards goes back over the network
// to rediscover a finding it is already holding.
func testVectorRoundTrip(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	e := Entry("company", "northwind revenue", map[string]string{"co": "northwind"},
		"$4.2bn", map[string]any{"revenue": "$4.2bn"})
	e.Vector = []float32{0.25, -0.5, 0.75}
	put(t, b, e)

	check := func(what string, got []findings.Entry) {
		t.Helper()
		if len(got) != 1 {
			t.Fatalf("%s: expected one entry, got %d", what, len(got))
		}
		if len(got[0].Vector) != len(e.Vector) {
			t.Fatalf("%s: entry came back without its vector (%d dims, want %d)",
				what, len(got[0].Vector), len(e.Vector))
		}
		for i := range e.Vector {
			if diff := got[0].Vector[i] - e.Vector[i]; diff > 1e-6 || diff < -1e-6 {
				t.Fatalf("%s: vector[%d] = %v, want %v", what, i, got[0].Vector[i], e.Vector[i])
			}
		}
	}
	check("candidates", candidates(t, b, e.Finding.Asked))
	fetched, err := b.Fetch(ctx, []string{e.Hash})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	check("fetch", fetched)
}

// --- leases -------------------------------------------------------------

func testLeaseExclusion(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	key := "lease_exclusion"

	a, held, err := b.Acquire(ctx, key, "executor-a", 5*time.Second)
	if err != nil || !held {
		t.Fatalf("the first acquirer must win: held=%v err=%v", held, err)
	}
	if a.Owner != "executor-a" || a.Token == 0 {
		t.Fatalf("a granted lease must name its owner and carry a token: %+v", a)
	}

	other, held, err := b.Acquire(ctx, key, "executor-b", 5*time.Second)
	if err != nil {
		t.Fatalf("contending acquire: %v", err)
	}
	if held {
		t.Fatalf("two executors must not hold one lease")
	}
	if other.Owner != "executor-a" {
		t.Fatalf("a refused acquire must describe the holder, got %+v", other)
	}

	// Peek sees the same thing without taking anything.
	seen, found, err := b.Peek(ctx, key)
	if err != nil || !found || seen.Owner != "executor-a" {
		t.Fatalf("peek: %+v found=%v err=%v", seen, found, err)
	}
	if seen.Done(time.Now()) {
		t.Fatalf("a live lease must not read as done")
	}

	// Renewal extends it and keeps the token.
	renewed, still, err := b.Renew(ctx, a, 5*time.Second)
	if err != nil || !still {
		t.Fatalf("the owner must be able to renew: still=%v err=%v", still, err)
	}
	if renewed.Token != a.Token {
		t.Fatalf("renewal must not change the fencing token: %d → %d", a.Token, renewed.Token)
	}
	if !renewed.Expires.After(a.Expires.Add(-time.Second)) {
		t.Fatalf("renewal must extend the expiry")
	}

	// Release frees it immediately, and the next acquirer is not a takeover:
	// nobody died, the work finished.
	if err := b.Release(ctx, renewed); err != nil {
		t.Fatalf("release: %v", err)
	}
	after, found, err := b.Peek(ctx, key)
	if err != nil || !found {
		t.Fatalf("peek after release: found=%v err=%v", found, err)
	}
	if !after.Done(time.Now()) {
		t.Fatalf("a released lease must read as done")
	}
	next, held, err := b.Acquire(ctx, key, "executor-b", 5*time.Second)
	if err != nil || !held {
		t.Fatalf("a released lease must be acquirable: held=%v err=%v", held, err)
	}
	if next.Takeover {
		t.Fatalf("acquiring a cleanly released lease is not a takeover")
	}
	if next.Token <= a.Token {
		t.Fatalf("the fencing token must increase when the lease changes hands: %d → %d",
			a.Token, next.Token)
	}
}

func testLeaseTakeover(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	key := "lease_takeover"

	// An executor takes the lease and dies: no release, no renewal.
	dead, held, err := b.Acquire(ctx, key, "executor-dead", 150*time.Millisecond)
	if err != nil || !held {
		t.Fatalf("first acquire: held=%v err=%v", held, err)
	}

	if _, held, _ := b.Acquire(ctx, key, "executor-live", time.Second); held {
		t.Fatalf("a live lease must not be takeable")
	}

	deadline := time.Now().Add(5 * time.Second)
	var taken findings.Lease
	for {
		var ok bool
		taken, ok, err = b.Acquire(ctx, key, "executor-live", time.Second)
		if err != nil {
			t.Fatalf("acquire after expiry: %v", err)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("an expired lease must become acquirable: a crashed executor cannot block a question forever")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !taken.Takeover {
		t.Fatalf("acquiring an expired, unreleased lease is a takeover and must say so")
	}
	if taken.Token <= dead.Token {
		t.Fatalf("a takeover must advance the fencing token: %d → %d", dead.Token, taken.Token)
	}
}

func testLeaseFencing(t *testing.T, b findings.Backend) {
	ctx := context.Background()
	key := "lease_fencing"

	stale, held, err := b.Acquire(ctx, key, "executor-slow", 100*time.Millisecond)
	if err != nil || !held {
		t.Fatalf("first acquire: held=%v err=%v", held, err)
	}

	var taken findings.Lease
	deadline := time.Now().Add(5 * time.Second)
	for {
		var ok bool
		taken, ok, err = b.Acquire(ctx, key, "executor-fast", 5*time.Second)
		if err != nil {
			t.Fatalf("takeover: %v", err)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the expired lease never became acquirable")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The fenced owner wakes up and tries to carry on. Neither of its calls may
	// touch the lease the new owner holds: a release would wake the new owner's
	// followers onto a finding nobody has contributed yet, and a renewal would
	// extend a lease it does not have.
	if _, still, err := b.Renew(ctx, stale, 5*time.Second); err != nil || still {
		t.Fatalf("a fenced owner must not be able to renew: still=%v err=%v", still, err)
	}
	if err := b.Release(ctx, stale); err != nil {
		t.Fatalf("a fenced release must be a no-op, not an error: %v", err)
	}
	after, found, err := b.Peek(ctx, key)
	if err != nil || !found {
		t.Fatalf("peek: found=%v err=%v", found, err)
	}
	if after.Done(time.Now()) || after.Owner != "executor-fast" || after.Token != taken.Token {
		t.Fatalf("the live lease must be untouched by a fenced owner: %+v", after)
	}
}

// Describe renders a backend's identity for a test failure message.
func Describe(b findings.Backend) string { return fmt.Sprintf("%T", b) }
