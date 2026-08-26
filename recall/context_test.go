package recall

import (
	"context"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

func TestOpenRefusesUnusableConfigurations(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		want string
	}{
		{"a name that is a path", Config{Name: "../escape"}, "must be letters"},
		{"a name with a separator", Config{Name: "a/b"}, "must be letters"},
		{"a negative window", Config{Name: "d", Every: -time.Second}, "Every must be positive"},
		{"a zero slice", Config{Name: "d", Slice: -1}, "Slice must be positive"},
		{"a negative budget", Config{Name: "d", MaxBytes: -1}, "MaxBytes must not be negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := Open(tc.cfg, loom.WithRegistry(registry(t)), loom.WithStateDir(t.TempDir()))
			if err == nil {
				_ = d.Close()
				t.Fatalf("Open accepted %+v", tc.cfg)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// TestAFailedCompactionKeepsTheEntriesItWouldHaveFolded covers the one way a
// fold could lose data: a model that answers with nothing. Installing that as a
// brief would drop the entries it was supposed to replace, and the entries are
// the only place their content lives.
func TestAFailedCompactionKeepsTheEntriesItWouldHaveFolded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(answer)); err != nil {
		t.Fatalf("register: %v", err)
	}
	// The deep tier answers compactions with whitespace and everything else
	// normally, so the desk meets a fold that produced nothing.
	if _, err := model.RegisterMock(reg, "mock-deep", model.TierDeep,
		model.WithHandler(func(req model.Request) (string, error) {
			if strings.Contains(req.Prompt, "Compress these") {
				return "   \n  ", nil
			}
			return answer(req)
		})); err != nil {
		t.Fatalf("register: %v", err)
	}

	d, err := Open(Config{
		Name: "hollow", Every: 200 * time.Millisecond, Lateness: time.Millisecond,
		MaxBytes: 500, Keep: 1, Slice: 1,
	}, loom.WithRegistry(reg), loom.WithStateDir(t.TempDir()), loom.WithWorkers(4))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	var posted int64
	for i := 0; i < 6; i++ {
		n, err := d.Post(ctx, say("conv", "user",
			"we are blocked on a subject that will not fit in the budget"))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		posted = n
	}
	if _, err := d.Await(ctx, posted); err != nil {
		t.Fatalf("await: %v", err)
	}

	// Give the fold a chance to land and be refused.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(d.Errors()) == 0 {
		select {
		case <-d.Updates():
		case <-time.After(100 * time.Millisecond):
		}
	}

	view := d.Context()
	if view.Brief {
		t.Error("an empty brief was installed in the context")
	}
	if view.Entries == 0 {
		t.Fatal("the entries a failed fold would have replaced were dropped")
	}
	// The context is over budget and says so, which is the right failure: it is
	// still complete, and the desk keeps serving from it.
	if errs := d.Errors(); len(errs) == 0 {
		t.Error("the desk did not report the compaction that produced nothing")
	} else if !strings.Contains(errs[0].Error(), "empty brief") {
		t.Errorf("reported %v, want the empty brief", errs[0])
	}
	if _, err := d.Ask(ctx, Question{Text: "what is blocked?"}); err != nil {
		t.Errorf("the desk stopped answering after a failed fold: %v", err)
	}
}

// TestTheContextAndItsRevisionAgree checks the invariant that keeps a query and
// an inspector looking at the same thing: what /context renders and what a
// query resolves by hash are the same segments.
func TestTheContextAndItsRevisionAgree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := openDesk(t, Config{Name: "agree", MaxBytes: 800, Keep: 2, Slice: 1})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	var posted int64
	for i := 0; i < 8; i++ {
		n, err := d.Post(ctx, say("conv", "user", "we are blocked on another distinct subject here"))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		posted = n
	}
	if _, err := d.Await(ctx, posted); err != nil {
		t.Fatalf("await: %v", err)
	}

	// Checked across a compaction, because that is the operation that rewrites
	// the chain's root rather than appending to it.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !d.Context().Brief {
		select {
		case <-d.Updates():
		case <-time.After(100 * time.Millisecond):
		}
	}

	view := d.Context()
	chain, err := d.Fleet().Chain(d.contextKey())
	if err != nil {
		t.Fatalf("chain: %v", err)
	}
	list, err := chain.Resolve(view.Ref)
	if err != nil {
		t.Fatalf("resolve %s: %v", view.Ref, err)
	}
	text, _ := d.renderer.Render(list.Segs, 0, list.Len())
	if text != view.Text {
		t.Errorf("the published text and the revision it names differ:\n%q\n%q", view.Text, text)
	}
	if view.Ref.Segments != list.Len() {
		t.Errorf("revision claims %d segments, holds %d", view.Ref.Segments, list.Len())
	}
}

// TestASpecIsFilledInFieldByField covers the substitution rule: replacing one
// operation leaves the other three alone.
func TestASpecIsFilledInFieldByField(t *testing.T) {
	mine := pipeline.ReduceAISpec{Prompt: "mine: {{.Count}}", ItemField: "x"}
	got := withDefaults(Spec{Digest: mine})

	if got.Digest.Prompt != mine.Prompt {
		t.Errorf("a supplied Digest was overwritten: %q", got.Digest.Prompt)
	}
	def := DefaultSpec()
	if got.Read.Prompt != def.Read.Prompt {
		t.Error("Read was not filled in from the default")
	}
	if got.Compact.Prompt != def.Compact.Prompt {
		t.Error("Compact was not filled in from the default")
	}
	if got.Answer.Prompt != def.Answer.Prompt {
		t.Error("Answer was not filled in from the default")
	}
}

func TestValidReadIsTheSemanticGate(t *testing.T) {
	for _, tc := range []struct {
		name string
		data map[string]any
		ok   bool
	}{
		{"a known kind with a note", map[string]any{"kind": "fact", "note": "something"}, true},
		{"noise needs no note", map[string]any{"kind": "noise", "note": ""}, true},
		{"case is not the model's problem", map[string]any{"kind": "Decision", "note": "n"}, true},
		{"an invented kind", map[string]any{"kind": "vibes", "note": "n"}, false},
		{"a kind with nothing behind it", map[string]any{"kind": "problem", "note": "  "}, false},
		{"no kind at all", map[string]any{"note": "n"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validRead(core.NewRecord("r", tc.data))
			if tc.ok && err != nil {
				t.Errorf("rejected a valid read: %v", err)
			}
			if !tc.ok && err == nil {
				t.Error("accepted output that would have gone into the context wrong")
			}
		})
	}
}
