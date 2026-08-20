package route

import (
	"os"
	"path/filepath"
	"testing"
)

// TestProfileSurvivesTheRun: the property that makes the second run over
// similar input start where the first one left off.
func TestProfileSurvivesTheRun(t *testing.T) {
	dir := t.TempDir()

	first := New(Config{Features: fixedBucket("b")})
	feed(first, "b", 0, 40, 4)
	if err := SaveProfile(dir, first.Learned()); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadProfile(dir)
	if err != nil {
		t.Fatal(err)
	}
	rate, n := loaded.Rate("classify", "b", 0)
	if n != 40 || rate != 0.1 {
		t.Fatalf("loaded %.2f over %d, want 0.10 over 40", rate, n)
	}
}

// TestContributionsAccumulate: several processes calibrate one pipeline at
// once, so the file has to sum rather than let the last writer win.
func TestContributionsAccumulate(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		a := New(Config{Features: fixedBucket("b")})
		feed(a, "b", 0, 10, 2)
		if err := SaveProfile(dir, a.Learned()); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := LoadProfile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if rate, n := loaded.Rate("classify", "b", 0); n != 30 || rate != 0.2 {
		t.Fatalf("loaded %.2f over %d, want 0.20 over 30 — contributions must sum", rate, n)
	}
}

// TestSeededRunDoesNotDoubleCountItsSeed: the reason SaveProfile is handed
// Learned rather than Profile.
func TestSeededRunDoesNotDoubleCountItsSeed(t *testing.T) {
	dir := t.TempDir()
	first := New(Config{Features: fixedBucket("b")})
	feed(first, "b", 0, 20, 5)
	if err := SaveProfile(dir, first.Learned()); err != nil {
		t.Fatal(err)
	}

	seed, err := LoadProfile(dir)
	if err != nil {
		t.Fatal(err)
	}
	second := New(Config{Features: fixedBucket("b"), Profile: seed})
	feed(second, "b", 0, 20, 15)
	if err := SaveProfile(dir, second.Learned()); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadProfile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if rate, n := loaded.Rate("classify", "b", 0); n != 40 || rate != 0.5 {
		t.Fatalf("loaded %.2f over %d, want 0.50 over 40", rate, n)
	}
}

// TestMissingStateIsAColdPipeline, not an error: a run must not fail to start
// because it has nothing to route with.
func TestMissingStateIsAColdPipeline(t *testing.T) {
	p, err := LoadProfile(filepath.Join(t.TempDir(), "never-written"))
	if err != nil {
		t.Fatalf("a missing profile should not be an error: %v", err)
	}
	if _, n := p.Rate("classify", "b", 0); n != 0 {
		t.Errorf("samples = %d on an empty profile", n)
	}
	if err := SaveProfile("", NewProfile()); err != nil {
		t.Errorf("saving with no state dir should be a no-op: %v", err)
	}
}

// TestATornWriteCostsOnlyWhatFollowsIt: a profile is a cache of decisions, so
// a half-written trailing line must lose that line and nothing before it.
func TestATornWriteCostsOnlyWhatFollowsIt(t *testing.T) {
	dir := t.TempDir()
	a := New(Config{Features: fixedBucket("b")})
	feed(a, "b", 0, 12, 3)
	if err := SaveProfile(dir, a.Learned()); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, ProfileFile)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(good, []byte(`{"version":1,"stag`)...), 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadProfile(dir)
	if err != nil {
		t.Fatalf("a torn line should not fail the load: %v", err)
	}
	if _, n := loaded.Rate("classify", "b", 0); n != 12 {
		t.Fatalf("samples = %d, want the 12 written before the tear", n)
	}
}

// TestALearnerThatLearnedNothingWritesNothing, so a pipeline run a thousand
// times with routing on but no verdicts to record does not grow a file of
// empty lines.
func TestALearnerThatLearnedNothingWritesNothing(t *testing.T) {
	dir := t.TempDir()
	if err := SaveProfile(dir, New(Config{}).Learned()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ProfileFile)); !os.IsNotExist(err) {
		t.Errorf("an empty contribution created a file: %v", err)
	}
}
