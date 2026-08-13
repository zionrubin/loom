package pgstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/findings"
	"github.com/zionrubin/loom/findings/backendtest"
	"github.com/zionrubin/loom/findings/pgstore"
)

// DSN is the environment variable these tests read. Without it they skip:
// a test that silently passes because no database was reachable is worse than
// one that says so.
const DSN = "LOOM_FINDINGS_PG_DSN"

//	createdb loomtest
//	LOOM_FINDINGS_PG_DSN='postgres://localhost/loomtest?sslmode=disable' go test ./findings/pgstore/

func dsn(t *testing.T) string {
	t.Helper()
	v := strings.TrimSpace(os.Getenv(DSN))
	if v == "" {
		t.Skipf("set %s to run the PostgreSQL backend tests "+
			"(e.g. postgres://localhost/loomtest?sslmode=disable)", DSN)
	}
	return v
}

// open builds a store with its own table prefix, so every subtest gets an empty
// commons in one database and the suite can run in parallel with itself.
func open(t *testing.T, opts pgstore.Options) *pgstore.Store {
	t.Helper()
	ctx := context.Background()
	if opts.Prefix == "" {
		opts.Prefix = fmt.Sprintf("t%d_%s", time.Now().UnixNano()%1e9, sanitize(t.Name()))
	}
	s, err := pgstore.Open(ctx, dsn(t), opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		drop(t, s, opts.Prefix)
		_ = s.Close()
	})
	return s
}

func sanitize(name string) string {
	name = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r >= 'A' && r <= 'Z':
			return r + 32
		default:
			return '_'
		}
	}, name)
	if len(name) > 40 {
		name = name[:40]
	}
	return name
}

func drop(t *testing.T, s *pgstore.Store, prefix string) {
	t.Helper()
	for _, table := range []string{"alias", "vector", "dependent", "verdict", "lease", "revision"} {
		if _, err := s.DB().Exec(`drop table if exists ` + prefix + `_` + table + ` cascade`); err != nil {
			t.Logf("drop %s_%s: %v", prefix, table, err)
		}
	}
}

// The conformance suite, against PostgreSQL — with pgvector if the extension is
// installed, and with the scan fallback if it is not.
func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) findings.Backend {
		return open(t, pgstore.Options{Dimensions: 3})
	})
}

// And again with pgvector explicitly out of the picture, because the fallback
// is a real code path that real managed databases will take.
func TestConformanceWithoutPgvector(t *testing.T) {
	if !hasPgvector(t) {
		t.Skip("pgvector is not installed; the default suite already covers the scan path")
	}
	backendtest.Run(t, func(t *testing.T) findings.Backend {
		return openScanOnly(t)
	})
}

func TestReportsWhichVectorModeItIsUsing(t *testing.T) {
	s := open(t, pgstore.Options{Dimensions: 3})
	mode := s.Vectors()
	if mode != pgstore.VectorPgvector && mode != pgstore.VectorScan {
		t.Fatalf("unexpected vector mode %q", mode)
	}
	if hasPgvector(t) && mode != pgstore.VectorPgvector {
		t.Fatalf("pgvector is installed but the store is in %q mode", mode)
	}
	t.Logf("vector search mode: %s", mode)
}

// Two stores are two executors. The lease has to be exclusive across
// connections, not merely across goroutines — that is the whole reason it is a
// row in a database rather than a mutex.
func TestLeaseIsExclusiveAcrossConnections(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("t%d_lease", time.Now().UnixNano()%1e9)
	a := open(t, pgstore.Options{Prefix: prefix, Dimensions: 3})
	b, err := pgstore.Open(ctx, dsn(t), pgstore.Options{Prefix: prefix, Dimensions: 3})
	if err != nil {
		t.Fatalf("second connection: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	const contenders = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	won := 0
	start := make(chan struct{})
	for i := range contenders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := a
			if i%2 == 1 {
				store = b
			}
			<-start
			_, held, err := store.Acquire(ctx, "hot-question", fmt.Sprintf("executor-%d", i), 10*time.Second)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			if held {
				mu.Lock()
				won++
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if won != 1 {
		t.Fatalf("%d executors acquired one lease; exactly one must", won)
	}
}

// Two stores over one database see each other's findings, which is the only
// property the whole package exists to provide.
func TestSeparateConnectionsShareOneCommons(t *testing.T) {
	ctx := context.Background()
	prefix := fmt.Sprintf("t%d_share", time.Now().UnixNano()%1e9)
	a := open(t, pgstore.Options{Prefix: prefix, Dimensions: 3})
	b, err := pgstore.Open(ctx, dsn(t), pgstore.Options{Prefix: prefix, Dimensions: 3})
	if err != nil {
		t.Fatalf("second connection: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	e := backendtest.Entry("company", "northwind revenue", map[string]string{"co": "northwind"},
		"$4.2bn", map[string]any{"revenue": "$4.2bn"})
	e.Vector = []float32{1, 0, 0}
	if _, err := a.Put(ctx, e); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, err := b.Candidates(ctx, findings.CandidateQuery{
		Topic: "company", Key: e.Key, Class: e.Class, Limit: 8})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(got) != 1 || got[0].Hash != e.Hash {
		t.Fatalf("the second executor must see the first's contribution, got %d", len(got))
	}
	// The finding must hash to its address after crossing the database: the
	// gate refuses anything that does not, so a lossy round trip is not a
	// cosmetic problem, it is a backend that cannot be used.
	if h := got[0].Finding.Hash(); h != e.Hash {
		t.Fatalf("finding did not survive the round trip: %s != %s", h, e.Hash)
	}
	// Contributing a finding indexes its vector in the same transaction.
	matches, err := b.Nearest(ctx, findings.VectorQuery{
		Embedding: []float32{1, 0, 0}, Topic: "company", TopK: 4})
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	if len(matches) != 1 || matches[0].Hash != e.Hash {
		t.Fatalf("a contributed vector must be searchable at once, got %v", matches)
	}
}

// hasPgvector reports whether the extension is installed on the test database.
func hasPgvector(t *testing.T) bool {
	t.Helper()
	db, err := sql.Open("pgx", dsn(t))
	if err != nil {
		return false
	}
	defer db.Close()
	var present bool
	if err := db.QueryRow(
		`select exists (select 1 from pg_extension where extname = 'vector')`).Scan(&present); err != nil {
		return false
	}
	return present
}

// openScanOnly builds a store that ignores pgvector, so the fallback is
// exercised on machines that have the extension as well as on those that do
// not.
func openScanOnly(t *testing.T) findings.Backend {
	t.Helper()
	s := open(t, pgstore.Options{ForceScan: true})
	if s.Vectors() != pgstore.VectorScan {
		t.Fatalf("expected the scan fallback, got %q", s.Vectors())
	}
	return s
}
