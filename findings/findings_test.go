package findings

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
)

// --- helpers ------------------------------------------------------------

func newGate(t *testing.T, p Policy) *Gate {
	t.Helper()
	cas, err := store.NewCAS("")
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	l, err := NewLedger(cas, "")
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return NewGate(l, p)
}

// openRequest is a reader granted everything a test's findings require, which
// is the uninteresting case for containment and the interesting one for
// everything else.
func openRequest(q Question) Request {
	return Request{
		Question: q,
		Grants:   security.NewGrantSet(security.ToolCap("search")),
		Egress:   security.EgressPolicy{}.With("api.example.com"),
		RunID:    "run_a",
		TaskID:   "task_a",
	}
}

func webResult(text string, fields map[string]any) Result {
	return Result{
		Text:    text,
		Fields:  fields,
		Sources: []Source{{Tool: "search", Host: "api.example.com", URI: "https://example.com/x"}},
		Cost:    core.Usage{OutputTokens: 400, Requests: 1, CostUSD: 0.02},
		Latency: 900 * time.Millisecond,
	}
}

func counted(res Result, n *int32) Fetch {
	return func(ctx context.Context, q Question) (Result, error) {
		atomic.AddInt32(n, 1)
		return res, nil
	}
}

// --- questions ----------------------------------------------------------

func TestQuestionKeyIgnoresWordingNoise(t *testing.T) {
	a := Question{Topic: "Company", Text: "  What is  Northwind's  revenue? ", Facets: map[string]string{"Year": "2024"}}
	b := Question{Topic: "company", Text: "what is northwind's revenue", Facets: map[string]string{"year": "2024"}}
	if a.Key() != b.Key() {
		t.Fatalf("case and whitespace must not split the exact key")
	}
	// Different words about the same subject are deliberately NOT the same
	// exact key: proving them equal is the near tier's job, not the key's.
	c := Question{Topic: "company", Text: "how much did northwind earn", Facets: map[string]string{"year": "2024"}}
	if a.Key() == c.Key() {
		t.Fatalf("the exact key must not merge questions it cannot prove identical")
	}
	if a.Class() != c.Class() {
		t.Fatalf("same topic and facets must land in one class")
	}
}

func TestNeedsDoNotSplitTheKey(t *testing.T) {
	a := Question{Topic: "company", Text: "profile", Needs: []string{"founded"}}
	b := Question{Topic: "company", Text: "profile", Needs: []string{"revenue", "hq"}}
	if a.Key() != b.Key() {
		t.Fatalf("two callers wanting different fields are asking one question")
	}
}

// --- the knowledge hash -------------------------------------------------

func TestKnowledgeHashExcludesTheClock(t *testing.T) {
	// The property the whole append-only design rests on: identical knowledge
	// learned at different times, by different agents, through different
	// sources, is one claim.
	a := Finding{Topic: "company", Answer: "Founded in 1996", Fields: map[string]any{"founded": 1996}}
	b := Finding{
		Topic: "company", Answer: "  founded in 1996  ",
		Fields:  map[string]any{"founded": 1996},
		Sources: []Source{{Tool: "other", Host: "elsewhere.example"}},
		Cost:    core.Usage{CostUSD: 9.99},
	}
	if a.Knowledge() != b.Knowledge() {
		t.Fatalf("the same claim must hash the same however and whenever it was learned")
	}
	if a.Hash() == b.Hash() {
		t.Fatalf("different recordings must still have distinct content addresses")
	}
	c := Finding{Topic: "company", Answer: "Founded in 1997", Fields: map[string]any{"founded": 1997}}
	if a.Knowledge() == c.Knowledge() {
		t.Fatalf("different claims must not converge")
	}
}

func TestRediscoveryCorroboratesRatherThanDuplicates(t *testing.T) {
	g := newGate(t, Policy{})
	q := Question{Topic: "company", Text: "when was northwind founded", Facets: map[string]string{"co": "northwind"}}

	for i := 0; i < 3; i++ {
		req := openRequest(q)
		req.RunID = fmt.Sprintf("run_%d", i)
		// Each agent phrases it differently but reaches the same conclusion.
		asked := q
		asked.Text = fmt.Sprintf("founding year of northwind (asked %d ways)", i)
		if _, err := g.Contribute(context.Background(), req, asked,
			webResult("Founded in 1996", map[string]any{"founded": 1996})); err != nil {
			t.Fatalf("contribute: %v", err)
		}
	}
	live := g.Ledger.Class(q.Class())
	if len(live) != 1 {
		t.Fatalf("three rediscoveries of one claim = 1 entry, got %d", len(live))
	}
	if live[0].Corroborations != 2 {
		t.Fatalf("corroborations = %d, want 2", live[0].Corroborations)
	}
	// And the alternative phrasings are now exact-tier hits.
	alt := q
	alt.Text = "founding year of northwind (asked 1 ways)"
	if got := g.Ledger.Exact(alt.Key()); len(got) != 1 {
		t.Fatalf("a phrasing that reached a known claim should be indexed exactly, got %d", len(got))
	}
}

// --- the lookup ladder --------------------------------------------------

func TestExactHitServesWithoutResearching(t *testing.T) {
	g := newGate(t, Policy{})
	q := Question{Topic: "company", Text: "northwind revenue", Facets: map[string]string{"co": "northwind"}}
	var calls int32
	fetch := counted(webResult("Revenue was $4.2bn", map[string]any{"revenue": "4.2bn"}), &calls)

	first, err := g.Research(context.Background(), openRequest(q), fetch)
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if first.Origin != OriginFresh {
		t.Fatalf("first ask origin = %s, want fresh", first.Origin)
	}

	second, err := g.Research(context.Background(), openRequest(q), fetch)
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if second.Origin != OriginExact {
		t.Fatalf("second ask origin = %s, want exact", second.Origin)
	}
	if calls != 1 {
		t.Fatalf("external calls = %d, want 1", calls)
	}
	if second.Text != first.Text {
		t.Fatalf("a hit must be substitutable for the call it replaced")
	}
	if second.Avoided.CostUSD != 0.02 {
		t.Fatalf("avoided cost = %v, want the original research cost", second.Avoided.CostUSD)
	}
	if second.AvoidedTime != 900*time.Millisecond {
		t.Fatalf("avoided time = %v, want the original research latency", second.AvoidedTime)
	}
}

func TestClassHitServesADifferentlyWordedQuestion(t *testing.T) {
	g := newGate(t, Policy{})
	facets := map[string]string{"co": "northwind", "year": "2024"}
	var calls int32
	fetch := counted(webResult("Revenue was $4.2bn", map[string]any{"revenue": "4.2bn"}), &calls)

	learn := Question{Topic: "company", Text: "what was northwind's revenue", Facets: facets}
	if _, err := g.Research(context.Background(), openRequest(learn), fetch); err != nil {
		t.Fatalf("research: %v", err)
	}

	// Same subject, no shared words at all, and no embedder configured — this
	// is the tier that costs nothing and still catches the case.
	ask := Question{Topic: "company", Text: "how much did they earn", Facets: facets}
	ans, err := g.Research(context.Background(), openRequest(ask), fetch)
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if ans.Origin != OriginClass {
		t.Fatalf("origin = %s, want class", ans.Origin)
	}
	if calls != 1 {
		t.Fatalf("external calls = %d, want 1", calls)
	}
}

func TestCoverageGapForcesResearchAndNarrowsIt(t *testing.T) {
	g := newGate(t, Policy{})
	facets := map[string]string{"co": "northwind"}
	ctx := context.Background()

	// Learned for one purpose: revenue only.
	learn := Question{Topic: "company", Text: "northwind revenue", Facets: facets}
	if _, err := g.Research(ctx, openRequest(learn), counted(
		webResult("Revenue $4.2bn", map[string]any{"revenue": "4.2bn"}), new(int32))); err != nil {
		t.Fatalf("research: %v", err)
	}

	// Asked for another: revenue and headcount. The revenue half is known, so
	// the external call must be narrowed to the half that is not.
	var asked []Question
	fetch := func(ctx context.Context, q Question) (Result, error) {
		asked = append(asked, q)
		return webResult("Headcount 12,000", map[string]any{"headcount": 12000}), nil
	}
	ask := Question{Topic: "company", Text: "northwind profile", Facets: facets,
		Needs: []string{"revenue", "headcount"}}
	ans, err := g.Research(ctx, openRequest(ask), fetch)
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if ans.Origin != OriginToppedUp {
		t.Fatalf("origin = %s, want topped-up", ans.Origin)
	}
	if len(asked) != 1 {
		t.Fatalf("fetches = %d, want 1", len(asked))
	}
	if got := asked[0].Needs; len(got) != 1 || got[0] != "headcount" {
		t.Fatalf("narrowed needs = %v, want [headcount]", got)
	}
	if !strings.Contains(asked[0].Text, "headcount") {
		t.Fatalf("narrowed text should name the gap, got %q", asked[0].Text)
	}
	// The answer the caller gets is both halves.
	if ans.Fields["revenue"] != "4.2bn" || ans.Fields["headcount"] != 12000 {
		t.Fatalf("topped-up answer lost a half: %v", ans.Fields)
	}
}

// --- freshness ----------------------------------------------------------

func TestVolatilityExpiresAndStaticDoesNot(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	g := newGate(t, Policy{
		Now: clock,
		Topics: map[string]TopicPolicy{
			"price":   {Volatility: Hourly},
			"history": {Volatility: Static},
		},
	})
	ctx := context.Background()
	var priceCalls, histCalls int32

	price := Question{Topic: "price", Text: "share price", Facets: map[string]string{"t": "nw"}}
	hist := Question{Topic: "history", Text: "founding year", Facets: map[string]string{"co": "nw"}}
	_, _ = g.Research(ctx, openRequest(price), counted(webResult("$41", nil), &priceCalls))
	_, _ = g.Research(ctx, openRequest(hist), counted(webResult("1996", nil), &histCalls))

	now = now.Add(90 * time.Minute)

	if ans, _ := g.Research(ctx, openRequest(price), counted(webResult("$44", nil), &priceCalls)); ans.Origin != OriginFresh {
		t.Fatalf("an hourly answer 90 minutes later must be re-researched, got %s", ans.Origin)
	}
	if ans, _ := g.Research(ctx, openRequest(hist), counted(webResult("1996", nil), &histCalls)); ans.Origin != OriginExact {
		t.Fatalf("a static answer must not expire, got %s", ans.Origin)
	}
	if priceCalls != 2 || histCalls != 1 {
		t.Fatalf("calls: price=%d (want 2), history=%d (want 1)", priceCalls, histCalls)
	}
	if s := g.Stats(); s.Stale != 1 {
		t.Fatalf("stale = %d, want 1", s.Stale)
	}
}

func TestLiveTopicIsNeverConsultedOrStored(t *testing.T) {
	g := newGate(t, Policy{Topics: map[string]TopicPolicy{"ticker": {Volatility: Live}}})
	q := Question{Topic: "ticker", Text: "price now", Facets: map[string]string{"t": "nw"}}
	var calls int32
	fetch := counted(webResult("$41", nil), &calls)

	for i := 0; i < 3; i++ {
		ans, err := g.Research(context.Background(), openRequest(q), fetch)
		if err != nil {
			t.Fatalf("research: %v", err)
		}
		if ans.Origin != OriginBypass {
			t.Fatalf("origin = %s, want bypass", ans.Origin)
		}
	}
	if calls != 3 {
		t.Fatalf("a live topic must always call out: calls = %d, want 3", calls)
	}
	if g.Ledger.Len() != 0 {
		t.Fatalf("a live topic must store nothing, ledger holds %d", g.Ledger.Len())
	}
	if s := g.Stats(); s.Bypassed != 3 {
		t.Fatalf("bypassed = %d, want 3", s.Bypassed)
	}
}

// --- negative results ---------------------------------------------------

func TestNegativeResultsAreServedLikeAnyOther(t *testing.T) {
	g := newGate(t, Policy{})
	q := Question{Topic: "filings", Text: "any SEC enforcement against northwind",
		Facets: map[string]string{"co": "northwind"}, Needs: []string{"enforcement"}}
	var calls int32
	fetch := func(ctx context.Context, q Question) (Result, error) {
		atomic.AddInt32(&calls, 1)
		return Result{
			NoEvidence: true,
			Text:       "Searched; no enforcement actions found.",
			Sources:    []Source{{Tool: "search", Host: "api.example.com"}},
			Cost:       core.Usage{CostUSD: 0.02},
		}, nil
	}
	ctx := context.Background()
	if _, err := g.Research(ctx, openRequest(q), fetch); err != nil {
		t.Fatalf("research: %v", err)
	}
	ans, err := g.Research(ctx, openRequest(q), fetch)
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if ans.Origin != OriginExact {
		t.Fatalf("a dead end must be reusable: origin = %s", ans.Origin)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 — nobody should search a known dead end twice", calls)
	}
	if !ans.Finding.NoEvidence {
		t.Fatalf("the served finding should still say it found nothing")
	}
}

// --- capability containment ---------------------------------------------

func TestLedgerNeverServesResearchTheReaderCouldNotHaveDone(t *testing.T) {
	g := newGate(t, Policy{})
	q := Question{Topic: "internal", Text: "customer roster", Facets: map[string]string{"acct": "42"}}
	ctx := context.Background()

	privileged := openRequest(q) // holds tool:search and egress to api.example.com
	var calls int32
	if _, err := g.Research(ctx, privileged, counted(webResult("Roster: …", nil), &calls)); err != nil {
		t.Fatalf("research: %v", err)
	}

	// A reader with no grants must not be handed the answer. The whole point:
	// waiting for a privileged sibling must not be a way around the envelope.
	unprivileged := Request{Question: q, RunID: "run_b", TaskID: "task_b"}
	if _, ok := g.Lookup(ctx, unprivileged); ok {
		t.Fatalf("a reader without the tool grant was served someone else's research")
	}

	// Granted the tool but not the host: still no.
	toolOnly := Request{
		Question: q, RunID: "run_c", TaskID: "task_c",
		Grants: security.NewGrantSet(security.ToolCap("search")),
	}
	if _, ok := g.Lookup(ctx, toolOnly); ok {
		t.Fatalf("a reader that cannot reach the host was served its contents")
	}

	// And a reader who could have done the research itself is served.
	if _, ok := g.Lookup(ctx, openRequest(q)); !ok {
		t.Fatalf("a reader holding every capability the research used must be served")
	}
	if s := g.Stats(); s.Denied != 2 {
		t.Fatalf("denied = %d, want 2", s.Denied)
	}
}

func TestPrivateScopeKeepsAFindingWithItsLearner(t *testing.T) {
	g := newGate(t, Policy{Topics: map[string]TopicPolicy{"case": {Scope: ScopePrivate}}})
	q := Question{Topic: "case", Text: "claimant history", Facets: map[string]string{"id": "9"}}
	ctx := context.Background()

	mine := openRequest(q)
	if _, err := g.Research(ctx, mine, counted(webResult("…", nil), new(int32))); err != nil {
		t.Fatalf("research: %v", err)
	}
	if _, ok := g.Lookup(ctx, mine); !ok {
		t.Fatalf("the agent that learned it must still be served it")
	}
	other := openRequest(q)
	other.RunID = "run_other"
	if _, ok := g.Lookup(ctx, other); ok {
		t.Fatalf("a private finding must not leave the agent that learned it")
	}
}

// --- single flight ------------------------------------------------------

func TestConcurrentIdenticalQuestionsCollapseToOneCall(t *testing.T) {
	g := newGate(t, Policy{})
	q := Question{Topic: "company", Text: "northwind profile", Facets: map[string]string{"co": "northwind"}}

	release := make(chan struct{})
	var calls int32
	fetch := func(ctx context.Context, _ Question) (Result, error) {
		atomic.AddInt32(&calls, 1)
		<-release // hold the leader until every follower is queued behind it
		return webResult("Profile …", map[string]any{"revenue": "4.2bn"}), nil
	}

	const askers = 32
	var wg sync.WaitGroup
	origins := make([]Origin, askers)
	errs := make([]error, askers)
	for i := 0; i < askers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := openRequest(q)
			req.TaskID = fmt.Sprintf("task_%d", i)
			ans, err := g.Research(context.Background(), req, fetch)
			origins[i], errs[i] = ans.Origin, err
		}(i)
	}
	// Give the followers time to queue on the leader's flight, then release it.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("asker %d: %v", i, err)
		}
	}
	if calls != 1 {
		t.Fatalf("external calls = %d, want 1 — this is the saving a result cache cannot make", calls)
	}
	var fresh, coalesced int
	for _, o := range origins {
		switch o {
		case OriginFresh:
			fresh++
		case OriginCoalesced:
			coalesced++
		default:
			t.Fatalf("unexpected origin %s", o)
		}
	}
	if fresh != 1 || coalesced != askers-1 {
		t.Fatalf("fresh=%d coalesced=%d, want 1 and %d", fresh, coalesced, askers-1)
	}
	if s := g.Stats(); s.Coalesced != askers-1 {
		t.Fatalf("stats coalesced = %d, want %d", s.Coalesced, askers-1)
	}
}

// The case the whole layer exists for: several agents want the same thing at
// the same moment and each asks for it in its own words. A cache keyed on the
// request cannot collapse these — the keys differ and nothing has finished
// writing yet — so the flight key is the subject, not the sentence.
func TestConcurrentParaphrasesOfOneSubjectCollapseToOneCall(t *testing.T) {
	g := newGate(t, Policy{})
	facets := map[string]string{"co": "northwind"}
	phrasings := []string{
		"what is northwind's annual revenue",
		"revenue figures for northwind",
		"how much money did northwind make",
		"northwind earnings last year",
		"northwind: revenue?",
	}

	release := make(chan struct{})
	var calls int32
	fetch := func(ctx context.Context, _ Question) (Result, error) {
		atomic.AddInt32(&calls, 1)
		<-release
		return webResult("Revenue $4.2bn", map[string]any{"revenue": "4.2bn"}), nil
	}

	var wg sync.WaitGroup
	origins := make([]Origin, len(phrasings))
	for i, text := range phrasings {
		wg.Add(1)
		go func(i int, text string) {
			defer wg.Done()
			req := openRequest(Question{Topic: "company", Text: text, Facets: facets})
			req.TaskID = fmt.Sprintf("task_%d", i)
			ans, err := g.Research(context.Background(), req, fetch)
			if err != nil {
				t.Errorf("asker %d: %v", i, err)
				return
			}
			origins[i] = ans.Origin
		}(i, text)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if calls != 1 {
		t.Fatalf("external calls = %d, want 1 — five wordings of one question", calls)
	}
	var fresh int
	for i, o := range origins {
		switch o {
		case OriginFresh:
			fresh++
		case OriginCoalesced:
		default:
			t.Fatalf("asker %d origin = %s", i, o)
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh = %d, want exactly one researcher", fresh)
	}
}

// A follower must never be fobbed off with an answer that does not cover what
// it asked. Collapsing askers is a cost optimization; it may not change what
// anyone is told.
func TestFollowerWithADifferentNeedIsNotFobbedOff(t *testing.T) {
	g := newGate(t, Policy{})
	facets := map[string]string{"co": "northwind"}

	release := make(chan struct{})
	var calls int32
	fetch := func(ctx context.Context, q Question) (Result, error) {
		atomic.AddInt32(&calls, 1)
		if len(q.Needs) > 0 && q.Needs[0] == "revenue" {
			<-release // the leader, held until the follower is queued
			return webResult("Revenue $4.2bn", map[string]any{"revenue": "4.2bn"}), nil
		}
		return webResult("Litigation: two open matters", map[string]any{"litigation": "2 open"}), nil
	}

	started := make(chan struct{})
	go func() {
		close(started)
		req := openRequest(Question{Topic: "company", Text: "revenue", Facets: facets,
			Needs: []string{"revenue"}})
		_, _ = g.Research(context.Background(), req, fetch)
	}()
	<-started
	time.Sleep(20 * time.Millisecond)

	// Same subject, different need. It shares the leader's subject but not its
	// question, so it must end up with its own answer.
	done := make(chan Answer, 1)
	go func() {
		req := openRequest(Question{Topic: "company", Text: "litigation", Facets: facets,
			Needs: []string{"litigation"}})
		ans, err := g.Research(context.Background(), req, fetch)
		if err != nil {
			t.Errorf("follower: %v", err)
		}
		done <- ans
	}()

	time.Sleep(20 * time.Millisecond)
	close(release)
	ans := <-done

	if ans.Fields["litigation"] != "2 open" {
		t.Fatalf("follower got %v, want its own answer", ans.Fields)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 — two different questions about one subject", calls)
	}
}

func TestFollowersShareTheLeadersFailure(t *testing.T) {
	g := newGate(t, Policy{})
	q := Question{Topic: "company", Text: "unreachable", Facets: map[string]string{"co": "x"}}
	release := make(chan struct{})
	var calls int32
	fetch := func(ctx context.Context, _ Question) (Result, error) {
		atomic.AddInt32(&calls, 1)
		<-release
		return Result{}, fmt.Errorf("source down")
	}

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = g.Research(context.Background(), openRequest(q), fetch)
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err == nil {
			t.Fatalf("asker %d saw no error", i)
		}
	}
	// Releasing the herd onto a source that just failed is what the lease
	// exists to prevent.
	if calls != 1 {
		t.Fatalf("calls = %d, want 1: a failed flight must not release the herd", calls)
	}
}

func TestFollowerOvertakesAStuckLeader(t *testing.T) {
	g := newGate(t, Policy{MaxWait: 20 * time.Millisecond})
	q := Question{Topic: "company", Text: "slow", Facets: map[string]string{"co": "x"}}
	release := make(chan struct{})
	defer close(release)

	var calls int32
	leaderStarted := make(chan struct{})
	fetch := func(ctx context.Context, _ Question) (Result, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(leaderStarted)
			<-release
		}
		return webResult("eventually", nil), nil
	}
	go func() { _, _ = g.Research(context.Background(), openRequest(q), fetch) }()
	<-leaderStarted

	ans, err := g.Research(context.Background(), openRequest(q), fetch)
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if ans.Origin != OriginFresh {
		t.Fatalf("origin = %s, want fresh — a follower past the wait bound researches itself", ans.Origin)
	}
	if s := g.Stats(); s.Overtaken != 1 {
		t.Fatalf("overtaken = %d, want 1", s.Overtaken)
	}
}

// --- revision, retraction, conflict -------------------------------------

func TestRetractionReportsWhatRestedOnTheClaim(t *testing.T) {
	g := newGate(t, Policy{})
	q := Question{Topic: "company", Text: "northwind ceo", Facets: map[string]string{"co": "northwind"}}
	ctx := context.Background()

	first, err := g.Research(ctx, openRequest(q), counted(webResult("CEO is A. Vance", nil), new(int32)))
	if err != nil {
		t.Fatalf("research: %v", err)
	}

	// Two more agents build on it.
	for i := 0; i < 2; i++ {
		req := openRequest(q)
		req.RunID, req.TaskID = fmt.Sprintf("run_%d", i), fmt.Sprintf("task_%d", i)
		if _, ok := g.Lookup(ctx, req); !ok {
			t.Fatalf("reader %d should have been served", i)
		}
	}

	deps, err := g.Ledger.Retract(first.Finding.ID, "superseded by an announcement", time.Now())
	if err != nil {
		t.Fatalf("retract: %v", err)
	}
	if len(deps) != 2 {
		t.Fatalf("dependents = %d, want the 2 readers that were served it", len(deps))
	}
	// The claim stops being servable...
	if _, ok := g.Lookup(ctx, openRequest(q)); ok {
		t.Fatalf("a retracted claim must not be served")
	}
	// ...but the bytes any lineage entry names stay resolvable.
	if _, ok := g.Ledger.Get(first.Hash); !ok {
		t.Fatalf("a retracted finding's hash must stay resolvable")
	}
}

func TestRevisionSupersedesWithoutLosingHistory(t *testing.T) {
	g := newGate(t, Policy{})
	ctx := context.Background()
	q := Question{Topic: "company", Text: "hq", Facets: map[string]string{"co": "nw"}}
	req := openRequest(q)

	first, err := g.Contribute(ctx, req, q, webResult("HQ in Leeds", map[string]any{"hq": "Leeds"}))
	if err != nil {
		t.Fatalf("contribute: %v", err)
	}
	corrected := webResult("HQ in Manchester", map[string]any{"hq": "Manchester"})
	corrected.ID = first.Finding.ID
	second, err := g.Contribute(ctx, req, q, corrected)
	if err != nil {
		t.Fatalf("contribute: %v", err)
	}
	if second.Finding.Rev != 2 || second.Finding.Supersedes != first.Hash {
		t.Fatalf("rev=%d supersedes=%q, want 2 and %q", second.Finding.Rev, second.Finding.Supersedes, first.Hash)
	}
	live := g.Ledger.Class(q.Class())
	if len(live) != 1 || live[0].Finding.Fields["hq"] != "Manchester" {
		t.Fatalf("only the head should be live, got %d entries", len(live))
	}
	if _, ok := g.Ledger.Get(first.Hash); !ok {
		t.Fatalf("the superseded revision must stay resolvable")
	}
}

func TestConflictingClaimsAboutOneSubjectAreVisible(t *testing.T) {
	g := newGate(t, Policy{})
	ctx := context.Background()
	facets := map[string]string{"co": "nw"}
	a := Question{Topic: "company", Text: "revenue per filing", Facets: facets}
	b := Question{Topic: "company", Text: "revenue per press release", Facets: facets}

	if _, err := g.Contribute(ctx, openRequest(a), a, webResult("4.2bn", map[string]any{"revenue": "4.2bn"})); err != nil {
		t.Fatalf("contribute: %v", err)
	}
	if _, err := g.Contribute(ctx, openRequest(b), b, webResult("4.8bn", map[string]any{"revenue": "4.8bn"})); err != nil {
		t.Fatalf("contribute: %v", err)
	}
	conflicts := g.Ledger.Conflicts(a.Class())
	if len(conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1 — two answers to one question is a contradiction", len(conflicts))
	}
}

func TestNonOverlappingClaimsAreNotConflicts(t *testing.T) {
	g := newGate(t, Policy{})
	ctx := context.Background()
	facets := map[string]string{"co": "nw"}
	a := Question{Topic: "company", Text: "revenue", Facets: facets}
	b := Question{Topic: "company", Text: "headcount", Facets: facets}
	_, _ = g.Contribute(ctx, openRequest(a), a, webResult("4.2bn", map[string]any{"revenue": "4.2bn"}))
	_, _ = g.Contribute(ctx, openRequest(b), b, webResult("12000", map[string]any{"headcount": 12000}))
	if c := g.Ledger.Conflicts(a.Class()); len(c) != 0 {
		t.Fatalf("two findings answering different questions are not a conflict, got %d", len(c))
	}
}

// --- corroboration and confidence gates ---------------------------------

func TestMinSourcesWithholdsAnUncorroboratedClaim(t *testing.T) {
	g := newGate(t, Policy{Topics: map[string]TopicPolicy{"claim": {MinSources: 2}}})
	ctx := context.Background()
	q := Question{Topic: "claim", Text: "did it happen", Facets: map[string]string{"e": "1"}}

	one := webResult("Yes", map[string]any{"happened": true})
	if _, err := g.Contribute(ctx, openRequest(q), q, one); err != nil {
		t.Fatalf("contribute: %v", err)
	}
	if _, ok := g.Lookup(ctx, openRequest(q)); ok {
		t.Fatalf("a single-source claim must be withheld while MinSources is 2")
	}
	// A second agent reaching the same conclusion corroborates it into use.
	req := openRequest(q)
	req.RunID = "run_b"
	if _, err := g.Contribute(ctx, req, q, one); err != nil {
		t.Fatalf("contribute: %v", err)
	}
	if _, ok := g.Lookup(ctx, openRequest(q)); !ok {
		t.Fatalf("a corroborated claim must be served")
	}
}

// --- the near tier ------------------------------------------------------

// bagOfWords is a deterministic stand-in for a real embedder: it maps text to a
// sparse vector over a fixed vocabulary, so "revenue last year" and "revenue in
// the last year" score high and unrelated text scores low. It keeps the near
// tier testable without a model.
type bagOfWords struct{ vocab []string }

func (b bagOfWords) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		v := make([]float32, len(b.vocab))
		for _, w := range strings.Fields(strings.ToLower(t)) {
			for j, term := range b.vocab {
				if term == w {
					v[j] = 1
				}
			}
		}
		out[i] = v
	}
	return out, nil
}

func TestNearTierServesAParaphraseAndRejectsAnUnrelatedQuestion(t *testing.T) {
	emb := bagOfWords{vocab: []string{"revenue", "headcount", "northwind", "earn", "staff", "annual"}}
	g := newGate(t, Policy{
		Embedder: emb,
		// One topic, no facets: the class tier cannot separate these, so the
		// near tier is what has to decide.
		Topics: map[string]TopicPolicy{"web": {Near: 0.8}},
	})
	ctx := context.Background()
	var calls int32
	fetch := counted(webResult("Revenue $4.2bn", map[string]any{"revenue": "4.2bn"}), &calls)

	learn := Question{Topic: "web", Text: "northwind annual revenue"}
	if _, err := g.Research(ctx, openRequest(learn), fetch); err != nil {
		t.Fatalf("research: %v", err)
	}

	near := Question{Topic: "web", Text: "annual revenue northwind"}
	ans, err := g.Research(ctx, openRequest(near), fetch)
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if ans.Origin != OriginNear {
		t.Fatalf("origin = %s, want near", ans.Origin)
	}
	if ans.Similarity < 0.8 {
		t.Fatalf("similarity = %v, want >= the threshold that admitted it", ans.Similarity)
	}

	far := Question{Topic: "web", Text: "northwind headcount staff"}
	if ans, err := g.Research(ctx, openRequest(far), fetch); err != nil {
		t.Fatalf("research: %v", err)
	} else if ans.Origin != OriginFresh {
		t.Fatalf("an unrelated question must not be served a near match, got %s", ans.Origin)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (the paraphrase reused, the unrelated one researched)", calls)
	}
}

func TestAdjudicationIsMemoizedAndMovesTheEntryThreshold(t *testing.T) {
	emb := bagOfWords{vocab: []string{"revenue", "northwind", "annual", "quarterly"}}
	var judged int32
	g := newGate(t, Policy{
		Embedder: emb,
		Judge: func(context.Context, Question, Finding) (bool, error) {
			atomic.AddInt32(&judged, 1)
			return false, nil // this near match does not actually answer it
		},
		JudgeCostUSD: 0.0001,
		Topics:       map[string]TopicPolicy{"web": {Near: 0.5, Adjudicate: true}},
	})
	ctx := context.Background()
	fetch := counted(webResult("Annual revenue $4.2bn", map[string]any{"revenue": "4.2bn"}), new(int32))

	learn := Question{Topic: "web", Text: "northwind annual revenue"}
	if _, err := g.Research(ctx, openRequest(learn), fetch); err != nil {
		t.Fatalf("research: %v", err)
	}
	entry := g.Ledger.Class(learn.Class())[0]
	before := entry.Threshold

	ask := Question{Topic: "web", Text: "northwind quarterly revenue"}
	for i := 0; i < 5; i++ {
		if ans, err := g.Research(ctx, openRequest(ask), fetch); err != nil {
			t.Fatalf("research: %v", err)
		} else if ans.Origin == OriginNear {
			t.Fatalf("a rejected candidate must not be served")
		}
	}
	if judged != 1 {
		t.Fatalf("judged = %d, want 1 — a verdict is memoized per (question, finding)", judged)
	}
	if entry.Threshold <= before {
		t.Fatalf("a rejection should raise the entry's own boundary: %v → %v", before, entry.Threshold)
	}
}

func TestBreakEvenDeclinesToJudgeCheapResearch(t *testing.T) {
	emb := bagOfWords{vocab: []string{"revenue", "northwind", "annual", "quarterly"}}
	var judged int32
	g := newGate(t, Policy{
		Embedder: emb,
		Judge: func(context.Context, Question, Finding) (bool, error) {
			atomic.AddInt32(&judged, 1)
			return false, nil
		},
		// Judging costs more than three times what this topic's research costs,
		// so the gate must not spend it.
		JudgeCostUSD: 1.0,
		BreakEven:    3,
		Topics:       map[string]TopicPolicy{"web": {Near: 0.5, Adjudicate: true}},
	})
	ctx := context.Background()
	fetch := counted(webResult("Annual revenue $4.2bn", map[string]any{"revenue": "4.2bn"}), new(int32))
	learn := Question{Topic: "web", Text: "northwind annual revenue"}
	if _, err := g.Research(ctx, openRequest(learn), fetch); err != nil {
		t.Fatalf("research: %v", err)
	}
	ask := Question{Topic: "web", Text: "northwind quarterly revenue"}
	ans, err := g.Research(ctx, openRequest(ask), fetch)
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if judged != 0 {
		t.Fatalf("the gate must not spend more looking than the lookup could save (judged %d)", judged)
	}
	if ans.Origin != OriginNear {
		t.Fatalf("declining to judge means serving the structural verdict, got %s", ans.Origin)
	}
}

// A fleet has readers walking the class index while other agents' contributions
// are incrementing corroboration counts and adjudications are moving near-match
// boundaries on the very same entries. Run under -race, this is the test that
// says those mutable fields are read through the lock that guards them.
func TestConcurrentReadersAndWritersOnLiveEntries(t *testing.T) {
	emb := bagOfWords{vocab: []string{"revenue", "headcount", "northwind", "annual", "quarterly", "staff"}}
	g := newGate(t, Policy{
		Embedder: emb,
		Judge: func(context.Context, Question, Finding) (bool, error) {
			return true, nil
		},
		JudgeCostUSD: 0.000001,
		Topics: map[string]TopicPolicy{
			"web": {Near: 0.4, Adjudicate: true, MinSources: 2},
		},
	})
	ctx := context.Background()
	fetch := counted(webResult("Annual revenue $4.2bn", map[string]any{"revenue": "4.2bn"}), new(int32))

	texts := []string{
		"northwind annual revenue", "annual revenue northwind",
		"northwind quarterly revenue", "northwind staff headcount",
		"headcount northwind annual",
	}

	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q := Question{Topic: "web", Text: texts[i%len(texts)]}
			req := openRequest(q)
			req.RunID = fmt.Sprintf("run_%d", i)
			req.TaskID = fmt.Sprintf("task_%d", i)
			if _, err := g.Research(ctx, req, fetch); err != nil {
				t.Errorf("asker %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if s := g.Stats(); s.Asked != 40 {
		t.Fatalf("asked = %d, want 40", s.Asked)
	}
	if g.Ledger.Len() == 0 {
		t.Fatalf("nothing was learned")
	}
}

// --- persistence --------------------------------------------------------

func TestLedgerSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cas, err := store.NewCAS(dir + "/cas")
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	l1, err := NewLedger(cas, dir)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	g1 := NewGate(l1, Policy{})
	q := Question{Topic: "company", Text: "northwind revenue", Facets: map[string]string{"co": "nw"}}
	var calls int32
	fetch := counted(webResult("$4.2bn", map[string]any{"revenue": "4.2bn"}), &calls)
	if _, err := g1.Research(context.Background(), openRequest(q), fetch); err != nil {
		t.Fatalf("research: %v", err)
	}
	if err := l1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A new process, a new ledger over the same directory.
	cas2, err := store.NewCAS(dir + "/cas")
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	l2, err := NewLedger(cas2, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	g2 := NewGate(l2, Policy{})
	ans, err := g2.Research(context.Background(), openRequest(q), fetch)
	if err != nil {
		t.Fatalf("research: %v", err)
	}
	if ans.Origin != OriginExact {
		t.Fatalf("origin = %s, want exact — yesterday's fleet already learned this", ans.Origin)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 across both processes", calls)
	}
}

// --- overhead -----------------------------------------------------------

func TestGateOverheadIsMicrosecondsOnTheCommonPath(t *testing.T) {
	g := newGate(t, Policy{})
	q := Question{Topic: "company", Text: "northwind revenue", Facets: map[string]string{"co": "nw"}}
	ctx := context.Background()
	fetch := counted(webResult("$4.2bn", map[string]any{"revenue": "4.2bn"}), new(int32))
	if _, err := g.Research(ctx, openRequest(q), fetch); err != nil {
		t.Fatalf("research: %v", err)
	}

	const asks = 2000
	for i := 0; i < asks; i++ {
		if _, err := g.Research(ctx, openRequest(q), fetch); err != nil {
			t.Fatalf("research: %v", err)
		}
	}
	s := g.Stats()
	if s.Exact != asks {
		t.Fatalf("exact hits = %d, want %d", s.Exact, asks)
	}
	// The claim the design makes is that the gate is cheap enough to sit in
	// front of every task. A hundred microseconds per question is a very loose
	// bound on a map lookup and still two orders below the network round trip
	// it replaces — loose on purpose, so the test measures the design rather
	// than the machine it runs on.
	if per := s.Overshoot(); per > 100*time.Microsecond {
		t.Fatalf("gate overhead %s per question is too high to sit in front of every task", per)
	}
}
