package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The example's headline claim, checked rather than printed: two independent
// fleets — separate ledgers, separate gates, separate everything except the
// backend — call the source once per subject between them.
//
// The process boundary itself is covered exhaustively in
// findings/distributed_test.go, which runs real executor processes. What this
// asserts is the wiring above it: that loom.WithFindings with a shared backend
// makes one fleet's research reachable by another's.
func TestIndependentFleetsShareOneCommons(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	commons := filepath.Join(dir, "commons")
	calls := filepath.Join(dir, "calls.log")

	run := func(name string, offset int, shared bool) report {
		t.Helper()
		out := filepath.Join(dir, name+".json")
		if err := runExecutor(ctx, config{
			Name: name, Commons: commons, CallLog: calls, Out: out,
			Offset: offset, Shared: shared, Latency: time.Millisecond,
		}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return read(t, out)
	}

	first := run("executor-1", 0, true)
	if first.Stats.Fresh != len(subjects) {
		t.Fatalf("the first executor researches every subject: %d of %d",
			first.Stats.Fresh, len(subjects))
	}
	if got := countLines(calls); got != len(subjects) {
		t.Fatalf("calls to the source = %d, want %d", got, len(subjects))
	}

	// A second fleet, asking about the same subjects in a different house style.
	second := run("executor-2", 1, true)
	if got := countLines(calls); got != len(subjects) {
		t.Fatalf("calls to the source = %d after the second executor, want %d — "+
			"it must have been served the first's research", got, len(subjects))
	}
	if second.Stats.SharedReuse() != len(subjects) {
		t.Fatalf("the second executor reused %d findings across executors, want %d (%+v)",
			second.Stats.SharedReuse(), len(subjects), second.Stats)
	}
	if second.Stats.Fresh != 0 {
		t.Fatalf("the second executor researched %d subjects it did not have to", second.Stats.Fresh)
	}

	// The answers are the same answers, which is the property that makes the
	// saving safe rather than merely cheap.
	for id, brief := range first.Briefs {
		if other, ok := second.Briefs[id]; ok && other != brief {
			t.Fatalf("%s: served answer differs from the researched one:\n  %q\n  %q", id, other, brief)
		}
	}
}

// Without the shared commons the same two fleets pay twice, which is the
// baseline the example prints beside the shared column.
func TestWithoutTheCommonsEachFleetPaysAgain(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls.log")

	for i, name := range []string{"executor-1", "executor-2"} {
		if err := runExecutor(ctx, config{
			Name: name, Commons: filepath.Join(dir, "commons"), CallLog: calls,
			Out: filepath.Join(dir, name+".json"), Offset: i, Shared: false,
			Latency: time.Millisecond,
		}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if got, want := countLines(calls), 2*len(subjects); got != want {
		t.Fatalf("calls to the source = %d, want %d — with no shared commons "+
			"each executor researches every subject itself", got, want)
	}
}

func read(t *testing.T, path string) report {
	t.Helper()
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var r report
	if err := json.Unmarshal(blob, &r); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return r
}
