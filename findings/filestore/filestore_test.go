package filestore_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/findings"
	"github.com/zionrubin/loom/findings/backendtest"
	"github.com/zionrubin/loom/findings/filestore"
)

// The whole conformance suite, against a directory.
func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) findings.Backend {
		s, err := filestore.Open(t.TempDir(), filestore.Options{})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}

// Two Store values over one directory are two executors: the point of the
// backend is that neither of them holds the state, the log does.
func TestSeparateHandlesShareOneCommons(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	a := open(t, dir)
	b := open(t, dir)

	e := backendtest.Entry("company", "northwind revenue", map[string]string{"co": "northwind"},
		"$4.2bn", map[string]any{"revenue": "$4.2bn"})
	if _, err := a.Put(ctx, e); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := b.Candidates(ctx, findings.CandidateQuery{
		Topic: "company", Key: e.Key, Class: e.Class, Limit: 8})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(got) != 1 || got[0].Hash != e.Hash {
		t.Fatalf("a second handle must see the first's contribution, got %d", len(got))
	}

	// And a mutation from the other side is visible back on the first.
	if err := b.Cite(ctx, e.Hash, findings.Dependent{RunID: "run_b", TaskID: "task_b"}); err != nil {
		t.Fatalf("cite: %v", err)
	}
	deps, err := a.Dependents(ctx, e.Hash)
	if err != nil || len(deps) != 1 {
		t.Fatalf("dependents across handles: %v (%d)", err, len(deps))
	}
}

// The lease is the mechanism the whole distributed layer rests on, so it is
// worth hammering: many contenders, one winner, and the winner is the only one
// that can release.
func TestLeaseUnderContention(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	const contenders = 12
	var wg sync.WaitGroup
	won := make([]bool, contenders)
	for i := range contenders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := open(t, dir)
			_, held, err := s.Acquire(ctx, "hot-question", "executor-"+string(rune('a'+i)), 5*time.Second)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			won[i] = held
		}(i)
	}
	wg.Wait()

	winners := 0
	for _, w := range won {
		if w {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("%d executors acquired one lease; exactly one must", winners)
	}
}

// A log with a half-written trailing record is a writer mid-append, not a
// corrupt commons: the reader must fold what is complete and leave the rest.
func TestPartialTrailingRecordIsIgnoredUntilComplete(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := open(t, dir)

	e := backendtest.Entry("company", "contoso revenue", map[string]string{"co": "contoso"},
		"$880m", map[string]any{"revenue": "$880m"})
	if _, err := s.Put(ctx, e); err != nil {
		t.Fatalf("put: %v", err)
	}

	path := filepath.Join(dir, "commons.jsonl")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := f.WriteString(`{"kind":"cite","hash":"abc","dep":{"run_i`); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = f.Close()

	reader := open(t, dir)
	got, err := reader.Candidates(ctx, findings.CandidateQuery{
		Topic: "company", Key: e.Key, Class: e.Class, Limit: 8})
	if err != nil {
		t.Fatalf("candidates over a partial log: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("the complete records must still fold, got %d", len(got))
	}
}

func open(t *testing.T, dir string) *filestore.Store {
	t.Helper()
	s, err := filestore.Open(dir, filestore.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
