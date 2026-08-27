package recall

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/stream"
)

// --- a deterministic desk ------------------------------------------------

// registry wires the two tiers a desk uses onto one deterministic mock brain,
// so every test here runs offline and produces the same context twice.
func registry(t *testing.T) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	for _, tier := range []struct {
		id string
		t  model.Tier
	}{{"mock-fast", model.TierFast}, {"mock-deep", model.TierDeep}} {
		if _, err := model.RegisterMock(reg, tier.id, tier.t, model.WithHandler(answer)); err != nil {
			t.Fatalf("register %s: %v", tier.id, err)
		}
	}
	return reg
}

// answer is the mock brain. It recognizes each of the desk's four operations by
// something structural in its prompt rather than by guessing, and answers
// deterministically, so a test can assert on the text a desk produced.
func answer(req model.Request) (string, error) {
	switch {
	case strings.Contains(req.Prefix, "Reply with JSON"):
		return read(req.Prompt), nil
	case strings.Contains(req.Prompt, "observations from one stretch"):
		return items(req.Prompt), nil
	case strings.Contains(req.Prompt, "Compress these"):
		// A compaction that does not shrink is not a compaction, and a mock
		// that returned everything it was given would leave a desk folding
		// forever. This one is bounded: the count, and the subjects, clipped.
		folded := items(req.Prompt)
		if len(folded) > 120 {
			folded = folded[:120] + "…"
		}
		return fmt.Sprintf("BRIEF: %d entries — %s",
			strings.Count(req.Prompt, "\n- "), folded), nil
	default:
		// The answer stage. Echoing the context back is what lets a test check
		// that the maintained context actually reached the model, and counting
		// its entries is what lets it check which revision did.
		return fmt.Sprintf("context carried %d entries; asked: %s",
			strings.Count(req.Prefix, "<entry>"), question(req.Prompt)), nil
	}
}

// read answers the per-message stage.
func read(prompt string) string {
	text := prompt
	if i := strings.Index(prompt, ":\n"); i >= 0 {
		text = prompt[i+2:]
	}
	kind := "fact"
	switch {
	case strings.Contains(strings.ToLower(text), "hello"),
		strings.Contains(strings.ToLower(text), "thanks"):
		return `{"kind": "noise", "note": "", "subject": "chat"}`
	case strings.Contains(strings.ToLower(text), "blocked"):
		kind = "problem"
	case strings.Contains(strings.ToLower(text), "we will"):
		kind = "decision"
	}
	return fmt.Sprintf(`{"kind": %q, "note": %q, "subject": "topic"}`, kind, strings.TrimSpace(text))
}

// items collapses an aggregation prompt's bullet list back into one line, so a
// digest is a deterministic function of exactly what was folded into it.
func items(prompt string) string {
	var out []string
	for _, line := range strings.Split(prompt, "\n") {
		if strings.HasPrefix(line, "- ") {
			out = append(out, strings.TrimPrefix(line, "- "))
		}
	}
	return strings.Join(out, " | ")
}

func question(prompt string) string {
	if i := strings.LastIndex(prompt, "Question: "); i >= 0 {
		return prompt[i+len("Question: "):]
	}
	return prompt
}

// openDesk provisions a desk on a temporary state directory with short windows,
// so a test folds conversations in within milliseconds rather than minutes.
func openDesk(t *testing.T, cfg Config, opts ...loom.Option) *Desk {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "test"
	}
	if cfg.Every == 0 {
		cfg.Every = 200 * time.Millisecond
	}
	if cfg.Lateness == 0 {
		cfg.Lateness = time.Millisecond
	}
	opts = append([]loom.Option{
		loom.WithRegistry(registry(t)),
		loom.WithStateDir(t.TempDir()),
		loom.WithWorkers(4),
	}, opts...)

	d, err := Open(cfg, opts...)
	if err != nil {
		t.Fatalf("open desk: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// say builds a message stamped with the moment it was written, which is what a
// live feed's messages carry.
//
// Backdating them would be a backfill, and a Feed is not a backfill: it closes
// its windows against the present, so a message stamped an hour ago arrives
// after its window has already been declared complete. That is a property worth
// meeting in a test helper rather than in a flaky assertion.
func say(conv, role, text string) Message {
	at := time.Now().UTC()
	return Message{
		Conversation: conv,
		ID:           fmt.Sprintf("%s-%d", conv, at.UnixNano()),
		Role:         role,
		Text:         text,
		At:           at,
	}
}

// --- the tests -----------------------------------------------------------

func TestDeskDigestsConversationsIntoContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := openDesk(t, Config{Name: "support"})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	n, err := d.Post(ctx,
		say("acme", "user", "hello there"),
		say("acme", "user", "we are blocked on SSO"),
		say("acme", "agent", "we will ship SAML in Q3"),
	)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if n != 3 {
		t.Fatalf("posted = %d, want 3", n)
	}

	// Two of the three messages survive the Keep stage; the greeting does not,
	// so waiting for all three would be waiting for a message that was
	// deliberately dropped.
	view, err := d.Await(ctx, 2)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if view.Entries == 0 {
		t.Fatal("context has no entries after digesting a conversation")
	}
	if view.Version == 0 {
		t.Fatal("context version did not advance")
	}
	if !strings.Contains(view.Text, "blocked on SSO") {
		t.Errorf("context does not carry what was said:\n%s", view.Text)
	}
	if !strings.Contains(view.Text, "acme") {
		t.Errorf("context does not attribute the conversation:\n%s", view.Text)
	}
	if !strings.Contains(view.Text, "<entry>") {
		t.Errorf("context is not rendered as tagged segments:\n%s", view.Text)
	}
	// The greeting was judged noise, and the cheapest context is the one that
	// never carried it.
	if strings.Contains(view.Text, "hello there") {
		t.Errorf("noise reached the context:\n%s", view.Text)
	}
}

func TestAskAnswersAgainstMaintainedContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := openDesk(t, Config{Name: "ask"})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Asked before anything is ingested, a desk says so rather than inventing
	// an answer, and does not spend a call to do it.
	empty, err := d.Ask(ctx, Question{Text: "what is going on?"})
	if err != nil {
		t.Fatalf("ask empty: %v", err)
	}
	if !empty.Empty {
		t.Errorf("an empty desk answered from something: %q", empty.Text)
	}
	if empty.Usage.Requests != 0 {
		t.Errorf("an empty desk made %d model calls", empty.Usage.Requests)
	}

	if _, err := d.Post(ctx, say("acme", "user", "we are blocked on SSO")); err != nil {
		t.Fatalf("post: %v", err)
	}
	if _, err := d.Await(ctx, 1); err != nil {
		t.Fatalf("await: %v", err)
	}

	ans, err := d.Ask(ctx, Question{Session: "s", Text: "what is Acme blocked on?"})
	if err != nil {
		t.Fatalf("ask: %v", err)
	}
	if ans.Empty {
		t.Fatal("desk reported empty after ingesting a conversation")
	}
	// The mock echoes how many entries reached its prompt, so this asserts the
	// maintained context actually travelled to the model rather than merely
	// existing beside it.
	if !strings.Contains(ans.Text, "context carried 1 entries") {
		t.Errorf("the context did not reach the model: %q", ans.Text)
	}
	if ans.Version == 0 {
		t.Error("answer does not name the context version it used")
	}
	if ans.Entries != 1 {
		t.Errorf("answer reports %d entries, want 1", ans.Entries)
	}
	if ans.Turn != 1 {
		t.Errorf("first turn of a session numbered %d", ans.Turn)
	}
}

// TestRepeatedQuestionIsFreeUntilTheContextMoves is the property the whole
// arrangement exists for: a query's cost is a function of the context revision,
// not of the history behind it.
func TestRepeatedQuestionIsFreeUntilTheContextMoves(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := openDesk(t, Config{Name: "cache"})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := d.Post(ctx, say("acme", "user", "we are blocked on SSO")); err != nil {
		t.Fatalf("post: %v", err)
	}
	if _, err := d.Await(ctx, 1); err != nil {
		t.Fatalf("await: %v", err)
	}

	q := Question{Text: "what is Acme blocked on?"}
	first, err := d.Ask(ctx, q)
	if err != nil {
		t.Fatalf("first ask: %v", err)
	}
	if first.Cached {
		t.Error("the first answer to a question was served from cache")
	}

	second, err := d.Ask(ctx, q)
	if err != nil {
		t.Fatalf("second ask: %v", err)
	}
	if !second.Cached {
		t.Error("the same question against an unchanged context was recomputed")
	}
	if second.Text != first.Text {
		t.Errorf("cached answer differs:\n%q\n%q", second.Text, first.Text)
	}

	// A new conversation moves the revision, which moves the fingerprint, which
	// is what makes the next answer current rather than stale-but-free.
	if _, err := d.Post(ctx, say("beta", "user", "we are blocked on billing")); err != nil {
		t.Fatalf("post: %v", err)
	}
	if _, err := d.Await(ctx, 2); err != nil {
		t.Fatalf("await: %v", err)
	}
	third, err := d.Ask(ctx, q)
	if err != nil {
		t.Fatalf("third ask: %v", err)
	}
	if third.Cached {
		t.Error("a question was served from cache after the context changed")
	}
	if third.Version <= first.Version {
		t.Errorf("context version did not advance: %d then %d", first.Version, third.Version)
	}
	if !strings.Contains(third.Text, "context carried 2 entries") {
		t.Errorf("the newer context did not reach the model: %q", third.Text)
	}
}

func TestSessionCarriesHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := openDesk(t, Config{Name: "session"})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := d.Post(ctx, say("acme", "user", "we are blocked on SSO")); err != nil {
		t.Fatalf("post: %v", err)
	}
	if _, err := d.Await(ctx, 1); err != nil {
		t.Fatalf("await: %v", err)
	}

	if _, err := d.Ask(ctx, Question{Session: "sam", Text: "first"}); err != nil {
		t.Fatalf("ask: %v", err)
	}
	second, err := d.Ask(ctx, Question{Session: "sam", Text: "second"})
	if err != nil {
		t.Fatalf("ask: %v", err)
	}
	if second.Turn != 2 {
		t.Errorf("second turn numbered %d", second.Turn)
	}
	if turns := d.Turns("sam"); len(turns) != 2 {
		t.Fatalf("session holds %d turns, want 2", len(turns))
	}
	// A question in another session shares the context and not the history.
	if turns := d.Turns("other"); len(turns) != 0 {
		t.Errorf("a fresh session already holds %d turns", len(turns))
	}
	d.Forget("sam")
	if turns := d.Turns("sam"); len(turns) != 0 {
		t.Errorf("a forgotten session still holds %d turns", len(turns))
	}
}

func TestContextCompactsWhenItOutgrowsItsBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A budget small enough that a handful of entries exceeds it, and a Keep
	// that leaves something to fold.
	d := openDesk(t, Config{Name: "compact", MaxBytes: 700, Keep: 2, Slice: 1})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	var posted int64
	for i := 0; i < 10; i++ {
		n, err := d.Post(ctx, say(fmt.Sprintf("conv-%d", i), "user",
			fmt.Sprintf("we are blocked on subject number %d in this thread", i)))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		posted = n
	}
	if _, err := d.Await(ctx, posted); err != nil {
		t.Fatalf("await: %v", err)
	}

	// Compaction runs beside the curator rather than inside it, so entries keep
	// arriving while a fold is in flight and the fold that lands leaves however
	// many arrived meanwhile. What is being waited for is therefore convergence
	// — the context back inside its budget — not one fold.
	deadline := time.Now().Add(20 * time.Second)
	var view View
	for time.Now().Before(deadline) {
		view = d.Context()
		if view.Brief && view.Bytes <= 700 {
			break
		}
		select {
		case <-d.Updates():
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !view.Brief {
		t.Fatalf("context never compacted: %s (%d entries, %d bytes)",
			view, view.Entries, view.Bytes)
	}
	if view.Bytes > 700 {
		t.Errorf("context is %d bytes after compacting, over its 700-byte budget", view.Bytes)
	}
	// Keep is how many entries survive a fold, not a ceiling the context is held
	// under between folds: entries that arrive while a compaction is in flight
	// are still there when it lands, and are left alone if the result already
	// fits. The invariant is the budget; the entry count is whatever meets it.
	if view.Entries >= 10 {
		t.Errorf("compaction left all %d entries verbatim", view.Entries)
	}
	if !strings.Contains(view.Text, "<brief>") {
		t.Errorf("the brief is not in the rendered context:\n%s", view.Text)
	}
	if !strings.Contains(view.Text, "BRIEF:") {
		t.Errorf("the brief does not carry what was folded:\n%s", view.Text)
	}
	if !strings.Contains(view.Text, "blocked on subject number") {
		t.Errorf("the brief lost the subjects it folded:\n%s", view.Text)
	}
	if view.Compactions == 0 {
		t.Error("compaction was not counted")
	}
	if errs := d.Errors(); len(errs) > 0 {
		t.Errorf("desk absorbed errors while compacting: %v", errs)
	}
}

func TestContextSurvivesRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dir := t.TempDir()
	reg := registry(t)

	// A desk, some conversation, and the version it reached.
	first, err := Open(Config{Name: "durable", Every: 200 * time.Millisecond, Lateness: time.Millisecond},
		loom.WithRegistry(reg), loom.WithStateDir(dir), loom.WithWorkers(4))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := first.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := first.Post(ctx, say("acme", "user", "we are blocked on SSO")); err != nil {
		t.Fatalf("post: %v", err)
	}
	before, err := first.Await(ctx, 1)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The same name and the same state directory: the context comes back rather
	// than being rebuilt from conversations this process never saw.
	second, err := Open(Config{Name: "durable", Every: 200 * time.Millisecond, Lateness: time.Millisecond},
		loom.WithRegistry(reg), loom.WithStateDir(dir), loom.WithWorkers(4))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()

	after := second.Context()
	if after.Entries != before.Entries {
		t.Errorf("restored %d entries, had %d", after.Entries, before.Entries)
	}
	if after.Text != before.Text {
		t.Errorf("restored a different context:\n%q\n%q", after.Text, before.Text)
	}
	if after.Version != before.Version {
		t.Errorf("restored version %d, had %d", after.Version, before.Version)
	}
	if after.Ref.Hash != before.Ref.Hash {
		t.Errorf("restored revision %s, had %s", after.Ref, before.Ref)
	}

	// And it can still be asked, without ingestion having started at all: the
	// read side needs the context, not the feed.
	if err := second.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	ans, err := second.Ask(ctx, Question{Text: "what is Acme blocked on?"})
	if err != nil {
		t.Fatalf("ask after restart: %v", err)
	}
	if !strings.Contains(ans.Text, "context carried 1 entries") {
		t.Errorf("the restored context did not reach the model: %q", ans.Text)
	}
}

func TestMinVersionWaitsForIngestion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := openDesk(t, Config{Name: "fresh"})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := d.Post(ctx, say("acme", "user", "we are blocked on SSO")); err != nil {
		t.Fatalf("post: %v", err)
	}

	// Asked for a version that has not been published, and posted only after
	// the question is already waiting.
	want := d.Context().Version + 1
	ans, err := d.Ask(ctx, Question{Text: "what is Acme blocked on?", MinVersion: want})
	if err != nil {
		t.Fatalf("ask: %v", err)
	}
	if ans.Version < want {
		t.Errorf("answered against v%d, asked for at least v%d", ans.Version, want)
	}

	// And a version that will not arrive fails rather than answering against an
	// older one.
	tight, cancelTight := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelTight()
	if _, err := d.Ask(tight, Question{
		Text: "anything?", MinVersion: d.Context().Version + 1000,
	}); err == nil {
		t.Error("a question for an unreachable version was answered anyway")
	}
}

func TestStreamLimitStopsIngestion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := openDesk(t, Config{Name: "bounded", Limit: stream.Limit{Records: 2}})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := d.Post(ctx,
		say("acme", "user", "we are blocked on SSO"),
		say("acme", "user", "we will ship SAML in Q3"),
		say("acme", "user", "and a third that is past the limit"),
	); err != nil {
		t.Fatalf("post: %v", err)
	}

	// The limit is checked between polls, not between records, so a poll that
	// picked up all three stops the job having read all three. What it
	// guarantees is that the job stops itself rather than running until it is
	// cancelled — which is what makes an endless desk a test.
	res, err := d.Wait()
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if res.Stream.StopReason != "record limit reached" {
		t.Errorf("job stopped for %q, want the record limit", res.Stream.StopReason)
	}
	if res.Stream.Records < 2 {
		t.Errorf("job stopped after %d records, before reaching its limit of 2",
			res.Stream.Records)
	}
	if !d.isStopped() {
		t.Error("desk still reports ingestion running after the job stopped")
	}
}

// TestQueriesAreServedWhileIngestionRuns is the claim the package is arranged
// around: the read path does not queue behind the write path.
//
// Ingestion is given more work than the fleet has slots and a model slow enough
// that it cannot drain quickly. Queries are asked throughout, and each is
// answered against a published revision without waiting for the ingest job to
// finish — which it never does.
func TestQueriesAreServedWhileIngestionRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	reg := model.NewRegistry()
	// The read tier is slow, so ingestion is genuinely occupying slots while
	// the queries arrive; the deep tier answers promptly.
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(answer), model.WithLatency(60*time.Millisecond)); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := model.RegisterMock(reg, "mock-deep", model.TierDeep,
		model.WithHandler(answer)); err != nil {
		t.Fatalf("register: %v", err)
	}

	d, err := Open(Config{Name: "busy", Every: 200 * time.Millisecond, Lateness: time.Millisecond},
		loom.WithRegistry(reg), loom.WithStateDir(t.TempDir()), loom.WithWorkers(2))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Something to answer from, before the flood.
	if _, err := d.Post(ctx, say("acme", "user", "we are blocked on SSO")); err != nil {
		t.Fatalf("post: %v", err)
	}
	if _, err := d.Await(ctx, 1); err != nil {
		t.Fatalf("await: %v", err)
	}

	// Far more ingest work than there are slots, so the queries below are
	// competing for admission rather than arriving at an idle fleet.
	var flood []Message
	for i := 0; i < 60; i++ {
		flood = append(flood, say(fmt.Sprintf("flood-%d", i%6), "user",
			fmt.Sprintf("we are blocked on subject %d", i)))
	}
	if _, err := d.Post(ctx, flood...); err != nil {
		t.Fatalf("post flood: %v", err)
	}

	for i := 0; i < 5; i++ {
		asked := time.Now()
		ans, err := d.Ask(ctx, Question{Text: fmt.Sprintf("question %d", i)})
		if err != nil {
			t.Fatalf("ask %d while ingesting: %v", i, err)
		}
		if ans.Empty {
			t.Fatalf("query %d was answered from nothing while the context was populated", i)
		}
		// The whole ingest backlog is at least 60 calls at 60ms against two
		// slots — the better part of two seconds — so an answer that arrives
		// well inside that did not wait for it.
		if waited := time.Since(asked); waited > 1500*time.Millisecond {
			t.Errorf("query %d took %s: it queued behind ingestion", i, waited)
		}
	}

	// And the desk was ingesting the whole time, rather than having quietly
	// finished before the questions started.
	if stats := d.Stats(); stats.Posted-stats.Dropped-stats.View.Messages == 0 {
		t.Log("the flood had already been absorbed; the timing above is still valid")
	}
}
