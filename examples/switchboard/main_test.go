package main

import (
	"context"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/recall"
)

// open provisions the example's own desk on a temporary state directory.
func open(t *testing.T, cfg recall.Config) *recall.Desk {
	t.Helper()
	if cfg.Name == "" {
		cfg.Name = "switchboard-test"
	}
	if cfg.Every == 0 {
		cfg.Every = 3 * time.Minute
	}
	if cfg.Quiet == 0 {
		cfg.Quiet = 200 * time.Millisecond
	}
	if cfg.Slice == 0 {
		cfg.Slice = 6
	}
	d, err := recall.Open(cfg,
		loom.WithRegistry(registry()),
		loom.WithStateDir(t.TempDir()),
		loom.WithWorkers(6),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 5}))
	if err != nil {
		t.Fatalf("open desk: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// ingest posts the whole shift, stamped as having ended a window ago, and waits
// for the desk to account for all of it.
func ingest(t *testing.T, ctx context.Context, d *recall.Desk, every time.Duration) recall.View {
	t.Helper()
	posted, err := d.Post(ctx, shift(time.Now().UTC().Add(-every-2*time.Second), transcript)...)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	view, err := d.Await(ctx, posted)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	return view
}

// TestTheShiftBecomesAContext is the example end to end: a shift of support
// traffic in, one queryable context out.
func TestTheShiftBecomesAContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	d := open(t, recall.Config{})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	view := ingest(t, ctx, d, 3*time.Minute)

	if view.Conversations != conversations(transcript) {
		t.Errorf("context covers %d conversations, the shift had %d",
			view.Conversations, conversations(transcript))
	}
	// The whole point of the arrangement: what a question reads is far smaller
	// than what was said, and it is smaller because it was summarized rather
	// than because it was truncated.
	if view.Entries >= len(transcript) {
		t.Errorf("context holds %d entries for %d messages: nothing was folded",
			view.Entries, len(transcript))
	}
	if s := d.Stats(); s.Dropped == 0 {
		t.Error("the shift's pleasantries were all carried into the context")
	}
	for _, want := range []string{"northwind", "acme", "meridian", "orbit"} {
		if !strings.Contains(view.Text, want) {
			t.Errorf("context lost conversation %q:\n%s", want, view.Text)
		}
	}
	if errs := d.Errors(); len(errs) > 0 {
		t.Errorf("desk absorbed errors: %v", errs)
	}
}

// TestAnswersComeFromTheContext checks the thing the example's mock exists to
// make checkable: an answer can only contain what the context contained.
func TestAnswersComeFromTheContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	d := open(t, recall.Config{})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	ingest(t, ctx, d, 3*time.Minute)

	blocked, err := d.Ask(ctx, recall.Question{Text: "which customers are blocked, and on what?"})
	if err != nil {
		t.Fatalf("ask: %v", err)
	}
	if !strings.Contains(blocked.Text, "SSO") {
		t.Errorf("the desk did not surface the blocker it was told about:\n%s", blocked.Text)
	}

	promised, err := d.Ask(ctx, recall.Question{Text: "what has been promised, and by when?"})
	if err != nil {
		t.Fatalf("ask: %v", err)
	}
	// A different question against the same context selects different entries.
	// That is the property worth asserting: the answer is a function of the
	// question and the maintained context, and of nothing else.
	if promised.Text == blocked.Text {
		t.Errorf("two different questions produced the same answer:\n%s", promised.Text)
	}
	if promised.Version != blocked.Version {
		t.Errorf("the two questions read different revisions (%d, %d)",
			blocked.Version, promised.Version)
	}
	if view := d.Context(); !strings.Contains(view.Text, "Decision:") {
		t.Errorf("the context did not record any commitment to answer from:\n%s", view.Text)
	}

	// Repeated against an unchanged context, in a session with the same (empty)
	// history: the same computation, and therefore no computation.
	again, err := d.Ask(ctx, recall.Question{Text: "which customers are blocked, and on what?"})
	if err != nil {
		t.Fatalf("ask again: %v", err)
	}
	if !again.Cached {
		t.Error("the same question against an unchanged context was recomputed")
	}
	if again.Text != blocked.Text {
		t.Errorf("the repeat answered differently:\n%q\n%q", again.Text, blocked.Text)
	}
}

// TestATighterBudgetFolds covers -budget: the context stays within its budget by
// folding its oldest entries rather than by dropping them.
func TestATighterBudgetFolds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	d := open(t, recall.Config{MaxBytes: 1200, Keep: 4})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	ingest(t, ctx, d, 3*time.Minute)

	deadline := time.Now().Add(20 * time.Second)
	var view recall.View
	for time.Now().Before(deadline) {
		if view = d.Context(); view.Brief && view.Bytes <= 1200 {
			break
		}
		select {
		case <-d.Updates():
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !view.Brief {
		t.Fatalf("context never folded: %d entries, %d bytes", view.Entries, view.Bytes)
	}
	if view.Bytes > 1200 {
		t.Errorf("context is %d bytes against a 1200-byte budget", view.Bytes)
	}
	// Folded, not truncated: the earliest conversations still have to be
	// findable, in less detail.
	if !strings.Contains(view.Text, "northwind") {
		t.Errorf("the oldest conversation was lost rather than briefed:\n%s", view.Text)
	}
}

// TestTheBrainReadsAndDropsDeterministically keeps the example honest twice
// over: the report's cache numbers only mean something if the same message
// produces the same note, and the "dropped" number only means something if the
// pleasantries are actually recognized.
func TestTheBrainReadsAndDropsDeterministically(t *testing.T) {
	read := func(role, text string) string {
		t.Helper()
		req := model.Request{
			Prefix: "Reply with JSON: …",
			Prompt: "In conversation acme, " + role + " said:\n" + text,
		}
		first, err := brain(req)
		if err != nil {
			t.Fatalf("brain: %v", err)
		}
		second, err := brain(req)
		if err != nil {
			t.Fatalf("brain: %v", err)
		}
		if first != second {
			t.Errorf("the mock answered %q twice differently:\n%q\n%q", text, first, second)
		}
		return first
	}

	if got := read("user", "Hi there, are you around?"); !strings.Contains(got, `"noise"`) {
		t.Errorf("a greeting was filed as %s, want noise", got)
	}
	if got := read("user", "We are blocked on SSO — the upload keeps failing."); !strings.Contains(got, `"problem"`) {
		t.Errorf("a blocker was filed as %s, want a problem", got)
	}
	if got := read("agent", "We will ship a fix this week."); !strings.Contains(got, `"decision"`) {
		t.Errorf("a commitment was filed as %s, want a decision", got)
	}
}
