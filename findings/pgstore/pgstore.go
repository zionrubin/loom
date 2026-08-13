// Package pgstore is the production findings backend: PostgreSQL for the
// findings, pgvector for similarity search, and one table for the leases that
// make concurrent executors research a question once between them.
//
// # Why one database rather than three systems
//
// The obvious architecture for this layer has three moving parts — a database
// for the findings, a vector database for the embeddings, and Redis (or
// ZooKeeper, or etcd) for the locks. This has one, and the reason is not
// frugality. Every one of those parts is a system that can be up while the
// others are down, and a commons split across three of them has failure modes
// that are nobody's fault and everybody's problem: a finding written but not
// indexed, a lease released against a store that has not yet committed the
// research it was protecting, a vector pointing at a claim that was retracted
// an hour ago. Putting all three in one database makes those states impossible
// by construction, because contributing a finding and indexing it is one
// transaction and releasing a lease after a contribution is an ordering the
// same connection can guarantee.
//
// PostgreSQL is also, unglamorously, already there. A lease is a row with an
// expiry and a fencing token; the atomic compare-and-set it needs is SELECT ...
// FOR UPDATE, which is a primitive the database has had for thirty years and
// which no operator has to be persuaded to deploy.
//
// # What is stored
//
//	findings_revision   immutable revisions: the finding, its content address,
//	                    exact question key, subject class, knowledge hash,
//	                    coverage, provenance, cost and latency, corroboration
//	                    count, learned threshold, retraction and supersession
//	findings_alias      the other question keys that reached one claim
//	findings_vector     embeddings, active flag, topic and class filters
//	findings_dependent  what was served each finding, from every executor
//	findings_verdict    memoized adjudications
//	findings_lease      distributed single-flight, with fencing tokens
//
// # Types stay inside
//
// Nothing PostgreSQL-specific crosses this package's boundary. The exported
// surface is Open, New, Options and a Store implementing findings.Backend; the
// adapter speaks database/sql and plain SQL text, and the vector column is
// written and read as pgvector's literal format rather than through a driver
// type. Replacing this with another database means writing another package
// against the same three interfaces, and the gate cannot tell.
//
// # pgvector is preferred, not required
//
// Open probes for the extension and uses it when it is there: a vector column,
// an optional HNSW index, and cosine distance evaluated in the database. Where
// the extension cannot be installed — a managed instance that does not offer
// it — the same table holds embeddings as JSON and similarity is scored in
// Go over a bounded candidate set. The interface is identical, the results are
// identical, and Store.Vectors reports which mode is in use so an operator can
// see whether they are getting the index they think they are.
package pgstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver: "pgx"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/findings"
)

// Store is a findings backend over PostgreSQL. It implements findings.Backend
// and is safe for concurrent use by any number of goroutines, processes and
// machines.
type Store struct {
	db   *sql.DB
	opts Options
	own  bool // this store opened the pool and must close it

	vectors VectorMode
}

// VectorMode is how similarity search is being served.
type VectorMode string

const (
	// VectorPgvector is the intended mode: a vector column, cosine distance in
	// the database, and an index when the dimension is declared.
	VectorPgvector VectorMode = "pgvector"
	// VectorScan is the fallback where the extension is unavailable:
	// embeddings as JSON, scored in Go over a bounded candidate set.
	VectorScan VectorMode = "scan"
)

// Options tunes the adapter. The zero value works against any PostgreSQL 12+.
type Options struct {
	// Dimensions is the embedding width. Declaring it lets the column be typed
	// vector(n) and indexed; leaving it zero keeps the column untyped, which
	// works and cannot be indexed. Set it to whatever the configured Embedder
	// produces.
	Dimensions int

	// Index names the pgvector index to build on the embedding column: "hnsw"
	// (default when Dimensions is set), "ivfflat", or "none".
	//
	// It is worth being deliberate here. An approximate index makes search
	// sub-linear and makes recall less than one — the layer will miss
	// candidates it would have found — which is acceptable precisely because a
	// vector match is a candidate and not an answer: a missed candidate costs
	// one external call, while a wrong hit would cost correctness. That
	// asymmetry is why approximation is safe here and would not be in the exact
	// tier.
	Index string

	// Schema places the tables in a named schema (default: the connection's
	// search path).
	Schema string

	// Prefix names the tables (default "findings"). Two commons can share a
	// database by differing here.
	Prefix string

	// MaxOpen and MaxIdle bound the pool when Open creates it (defaults 16 and
	// 4). They are ignored by New, where the pool is the caller's.
	MaxOpen int
	MaxIdle int

	// Scan bounds the candidate set the VectorScan fallback scores (default
	// 2048). Ignored when pgvector is available.
	Scan int

	// SkipMigrate leaves the schema alone, for deployments that manage
	// migrations themselves. Schema returns the DDL such a deployment needs.
	SkipMigrate bool

	// ForceScan ignores pgvector even where it is installed, storing embeddings
	// as JSON and scoring them in Go. It exists so the fallback path can be run
	// deliberately — by an operator who does not want the extension, and by the
	// tests, which would otherwise only ever exercise whichever mode the
	// machine they ran on happened to offer.
	ForceScan bool

	// Now is the clock for values this adapter writes itself. Rows the database
	// timestamps — leases, above all — use the database's clock on purpose:
	// mutual exclusion between machines cannot rest on the machines' clocks
	// agreeing.
	Now func() time.Time
}

const (
	defaultPrefix  = "findings"
	defaultMaxOpen = 16
	defaultMaxIdle = 4
	defaultScan    = 2048
)

func (o Options) normalize() Options {
	if o.Prefix == "" {
		o.Prefix = defaultPrefix
	}
	if o.MaxOpen <= 0 {
		o.MaxOpen = defaultMaxOpen
	}
	if o.MaxIdle <= 0 {
		o.MaxIdle = defaultMaxIdle
	}
	if o.Scan <= 0 {
		o.Scan = defaultScan
	}
	if o.Index == "" {
		if o.Dimensions > 0 {
			o.Index = "hnsw"
		} else {
			o.Index = "none"
		}
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Open connects to PostgreSQL and prepares the schema.
//
//	backend, err := pgstore.Open(ctx, os.Getenv("FINDINGS_DSN"), pgstore.Options{
//	    Dimensions: 1536,
//	})
//	shared := findings.NewShared(findings.SharedConfig{Backend: backend})
func Open(ctx context.Context, dsn string, opts Options) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("pgstore: a connection string is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: %w", err)
	}
	o := opts.normalize()
	db.SetMaxOpenConns(o.MaxOpen)
	db.SetMaxIdleConns(o.MaxIdle)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pgstore: %w", err)
	}
	s, err := New(ctx, db, opts)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s.own = true
	return s, nil
}

// New builds a store over a pool the caller owns and keeps.
func New(ctx context.Context, db *sql.DB, opts Options) (*Store, error) {
	if db == nil {
		return nil, errors.New("pgstore: a database is required")
	}
	s := &Store{db: db, opts: opts.normalize(), vectors: VectorScan}
	if err := s.prepare(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// Close releases the pool, if this store opened it.
func (s *Store) Close() error {
	if s.own {
		return s.db.Close()
	}
	return nil
}

// DB returns the pool, for a caller that wants to run its own queries against
// the same connection — a migration runner, a dashboard, a nightly report.
func (s *Store) DB() *sql.DB { return s.db }

// Vectors reports how similarity search is being served.
func (s *Store) Vectors() VectorMode { return s.vectors }

// --- Schema -------------------------------------------------------------

func (s *Store) table(name string) string {
	t := s.opts.Prefix + "_" + name
	if s.opts.Schema != "" {
		return s.opts.Schema + "." + t
	}
	return t
}

// prepare probes for pgvector and creates the schema.
func (s *Store) prepare(ctx context.Context) error {
	if !s.opts.ForceScan {
		// Asking for the extension is worth a try and never worth failing over:
		// a role without CREATE EXTENSION on a managed instance is the normal
		// case, not an error, and the scan fallback exists for exactly it.
		_, _ = s.db.ExecContext(ctx, `create extension if not exists vector`)
		var present bool
		if err := s.db.QueryRowContext(ctx,
			`select exists (select 1 from pg_extension where extname = 'vector')`).Scan(&present); err == nil && present {
			s.vectors = VectorPgvector
		}
	}
	if s.opts.SkipMigrate {
		return nil
	}
	// One transaction, behind an advisory lock. Ten executors of one deployment
	// start at the same instant and all run this, and concurrent CREATE TABLE IF
	// NOT EXISTS on one table is not the no-op it looks like — it races on the
	// catalog and one of the ten gets "tuple concurrently updated" instead of a
	// commons. Serializing the migration costs a lock nobody holds for longer
	// than a schema check.
	return s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `select pg_advisory_xact_lock(hashtext($1))`,
			"findings.migrate."+s.opts.Prefix); err != nil {
			return err
		}
		for _, stmt := range s.schema() {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("pgstore: migrate: %w\n%s", err, stmt)
			}
		}
		return nil
	})
}

// Schema returns the DDL this store needs, for a deployment that runs its own
// migrations (Options.SkipMigrate).
func (s *Store) Schema() []string { return s.schema() }

func (s *Store) schema() []string {
	rev, alias := s.table("revision"), s.table("alias")
	vec, dep := s.table("vector"), s.table("dependent")
	verdict, lease := s.table("verdict"), s.table("lease")

	embedding := "jsonb"
	if s.vectors == VectorPgvector {
		if s.opts.Dimensions > 0 {
			embedding = fmt.Sprintf("vector(%d)", s.opts.Dimensions)
		} else {
			embedding = "vector"
		}
	}

	stmts := []string{}
	if s.opts.Schema != "" {
		stmts = append(stmts, `create schema if not exists `+s.opts.Schema)
	}
	stmts = append(stmts,
		`create table if not exists `+rev+` (
			hash           text primary key,
			seq            bigserial,
			finding_id     text not null,
			rev            integer not null default 1,
			topic          text not null default '',
			question_key   text not null default '',
			class          text not null default '',
			knowledge      text not null default '',
			head           boolean not null default true,
			retracted      boolean not null default false,
			no_evidence    boolean not null default false,
			supersedes     text not null default '',
			confidence     double precision not null default 0,
			cost_usd       double precision not null default 0,
			latency_ms     bigint not null default 0,
			learned        timestamptz not null default now(),
			learner        text not null default '',
			executor       text not null default '',
			corroborations integer not null default 0,
			threshold      double precision not null default 0,
			finding        jsonb not null,
			usage          jsonb not null default '{}'::jsonb,
			created        timestamptz not null default now()
		)`,
		`create index if not exists `+idx(rev, "class")+` on `+rev+` (class) where head and not retracted`,
		`create index if not exists `+idx(rev, "knowledge")+` on `+rev+` (knowledge, class) where head and not retracted`,
		`create index if not exists `+idx(rev, "claim")+` on `+rev+` (finding_id)`,
		`create index if not exists `+idx(rev, "topic")+` on `+rev+` (topic)`,

		`create table if not exists `+alias+` (
			question_key text not null,
			hash         text not null references `+rev+` (hash) on delete cascade,
			primary key (question_key, hash)
		)`,

		`create table if not exists `+vec+` (
			hash      text primary key references `+rev+` (hash) on delete cascade,
			topic     text not null default '',
			class     text not null default '',
			active    boolean not null default true,
			embedding `+embedding+`
		)`,
		`create index if not exists `+idx(vec, "topic")+` on `+vec+` (topic) where active`,

		`create table if not exists `+dep+` (
			hash    text not null,
			run_id  text not null default '',
			stage   text not null default '',
			task_id text not null default '',
			seen    timestamptz not null default now(),
			primary key (hash, run_id, stage, task_id)
		)`,

		`create table if not exists `+verdict+` (
			question_key text not null,
			hash         text not null,
			ok           boolean not null,
			similarity   double precision not null default 0,
			decided      timestamptz not null default now(),
			executor     text not null default '',
			primary key (question_key, hash)
		)`,

		`create table if not exists `+lease+` (
			key      text primary key,
			owner    text not null default '',
			token    bigint not null default 0,
			acquired timestamptz not null default now(),
			expires  timestamptz not null default now(),
			released boolean not null default true
		)`,
	)
	if s.vectors == VectorPgvector && s.opts.Dimensions > 0 {
		switch strings.ToLower(s.opts.Index) {
		case "hnsw":
			stmts = append(stmts, `create index if not exists `+idx(vec, "hnsw")+
				` on `+vec+` using hnsw (embedding vector_cosine_ops)`)
		case "ivfflat":
			stmts = append(stmts, `create index if not exists `+idx(vec, "ivfflat")+
				` on `+vec+` using ivfflat (embedding vector_cosine_ops)`)
		}
	}
	return stmts
}

// idx names an index after the table it is on, with any schema qualification
// stripped: an index name is not schema-qualified in CREATE INDEX.
func idx(table, suffix string) string {
	if i := strings.LastIndexByte(table, '.'); i >= 0 {
		table = table[i+1:]
	}
	return table + "_" + suffix + "_idx"
}

// --- findings.Store: writing --------------------------------------------

// revisionColumns is what an entry is read back from. The embedding rides along
// with it, because an entry adopted without its vector is an entry the reader's
// own near tier can never score — the finding would be copied into a local
// ledger and be invisible to the tier that most wanted it, and every paraphrase
// would go back to the backend to rediscover what this process already held.
const revisionColumns = `r.hash, r.seq, r.finding_id, r.rev, r.topic, r.question_key, r.class,
	r.knowledge, r.retracted, r.no_evidence, r.supersedes, r.confidence, r.cost_usd,
	r.latency_ms, r.learned, r.learner, r.executor, r.corroborations, r.threshold,
	r.finding, r.usage, v.embedding::text`

// revisionSource is the revision table with its vectors attached.
func (s *Store) revisionSource() string {
	return s.table("revision") + ` r left join ` + s.table("vector") + ` v
		on v.hash = r.hash and v.active`
}

// Put records a finding revision, folding rediscovery into corroboration.
//
// The whole operation is one transaction, and it opens by taking an advisory
// lock on (knowledge, class). That lock is what makes "have we already been
// told this?" a decision rather than a race: without it two executors that
// learn the same thing at the same instant both look, both find nothing, and
// both insert — which is the duplication this layer exists to remove,
// reappearing inside the mechanism meant to remove it.
func (s *Store) Put(ctx context.Context, e findings.Entry) (findings.Entry, error) {
	if e.Hash == "" {
		return findings.Entry{}, errors.New("pgstore: an entry must carry its hash")
	}
	var out findings.Entry
	err := s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`select pg_advisory_xact_lock(hashtext($1))`, e.Knowledge+"|"+e.Class); err != nil {
			return err
		}

		// Rediscovery: the same claim about the same subject, learned again.
		var held string
		err := tx.QueryRowContext(ctx,
			`select hash from `+s.table("revision")+`
			  where knowledge = $1 and class = $2 and head and not retracted
			  order by seq limit 1`, e.Knowledge, e.Class).Scan(&held)
		switch {
		case err == nil && held != e.Hash:
			if _, err := tx.ExecContext(ctx,
				`update `+s.table("revision")+` set corroborations = corroborations + 1 where hash = $1`,
				held); err != nil {
				return err
			}
			if e.Key != "" {
				// A second phrasing that reached a claim we hold is worth
				// indexing exactly, so the next executor asking it that way
				// never reaches similarity search.
				if _, err := tx.ExecContext(ctx,
					`insert into `+s.table("alias")+` (question_key, hash) values ($1, $2)
					 on conflict do nothing`, e.Key, held); err != nil {
					return err
				}
			}
			stored, err := s.fetchOne(ctx, tx, held)
			if err != nil {
				return err
			}
			out = stored
			return nil
		case err == nil && held == e.Hash:
			// Byte-identical rediscovery of the entry itself.
			if _, err := tx.ExecContext(ctx,
				`update `+s.table("revision")+` set corroborations = corroborations + 1 where hash = $1`,
				held); err != nil {
				return err
			}
			stored, err := s.fetchOne(ctx, tx, held)
			if err != nil {
				return err
			}
			out = stored
			return nil
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}

		if err := s.insert(ctx, tx, e); err != nil {
			return err
		}
		stored, err := s.fetchOne(ctx, tx, e.Hash)
		if err != nil {
			return err
		}
		out = stored
		return nil
	})
	if err != nil {
		return findings.Entry{}, fmt.Errorf("pgstore: put: %w", err)
	}
	return out, nil
}

// insert writes one revision and re-elects the claim's head.
func (s *Store) insert(ctx context.Context, tx *sql.Tx, e findings.Entry) error {
	body, err := json.Marshal(e.Finding)
	if err != nil {
		return err
	}
	usage, err := json.Marshal(e.Finding.Cost)
	if err != nil {
		return err
	}
	learned := e.Learned
	if learned.IsZero() {
		learned = s.opts.Now()
	}
	if _, err := tx.ExecContext(ctx,
		`insert into `+s.table("revision")+`
		 (hash, finding_id, rev, topic, question_key, class, knowledge, retracted,
		  no_evidence, supersedes, confidence, cost_usd, latency_ms, learned, learner,
		  executor, corroborations, threshold, finding, usage)
		 values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
		 on conflict (hash) do nothing`,
		e.Hash, e.Finding.ID, max(e.Finding.Rev, 1), e.Finding.Topic, e.Key, e.Class,
		e.Knowledge, e.Finding.Retracted, e.Finding.NoEvidence, e.Finding.Supersedes,
		e.Finding.Confidence, e.Finding.Cost.CostUSD, e.Latency.Milliseconds(), learned,
		e.Learner, e.Executor, e.Corroborations, e.Threshold, body, usage,
	); err != nil {
		return err
	}
	if e.Key != "" {
		if _, err := tx.ExecContext(ctx,
			`insert into `+s.table("alias")+` (question_key, hash) values ($1,$2)
			 on conflict do nothing`, e.Key, e.Hash); err != nil {
			return err
		}
	}
	if len(e.Vector) > 0 {
		if err := s.upsertVector(ctx, tx, findings.Vector{
			Hash: e.Hash, Topic: e.Finding.Topic, Class: e.Class, Embedding: e.Vector,
		}); err != nil {
			return err
		}
	}
	return s.elect(ctx, tx, e.Finding.ID)
}

// elect makes the newest revision of a claim its head. Two statements rather
// than one clever one: a claim with two revisions at the same number — two
// executors correcting it at once — must still end with exactly one head, and
// "clear, then set the newest" says that unambiguously.
func (s *Store) elect(ctx context.Context, tx *sql.Tx, id string) error {
	if id == "" {
		return nil
	}
	rev := s.table("revision")
	if _, err := tx.ExecContext(ctx, `update `+rev+` set head = false where finding_id = $1 and head`, id); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx,
		`update `+rev+` set head = true where hash = (
			select hash from `+rev+` where finding_id = $1 order by rev desc, seq desc limit 1)`, id)
	return err
}

// Retract withdraws a claim: a retracted revision at the head, every vector
// deactivated, and the dependents reported back.
func (s *Store) Retract(ctx context.Context, id, reason string, now time.Time) ([]findings.Dependent, error) {
	var deps []findings.Dependent
	err := s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `select pg_advisory_xact_lock(hashtext($1))`, "claim|"+id); err != nil {
			return err
		}
		var head string
		err := tx.QueryRowContext(ctx,
			`select hash from `+s.table("revision")+` where finding_id = $1 and head limit 1`, id).Scan(&head)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("no finding %q", id)
		}
		if err != nil {
			return err
		}
		prev, err := s.fetchOne(ctx, tx, head)
		if err != nil {
			return err
		}

		f := prev.Finding
		f.Rev++
		f.Retracted = true
		f.Note = reason
		f.Supersedes = prev.Hash
		e := findings.Entry{
			Hash: f.Hash(), Finding: f, Key: prev.Key, Class: prev.Class,
			Knowledge: prev.Knowledge, Learned: now, Learner: prev.Learner,
			Executor: prev.Executor, Threshold: prev.Threshold,
		}
		if err := s.insert(ctx, tx, e); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`update `+s.table("vector")+` set active = false where hash in (
				select hash from `+s.table("revision")+` where finding_id = $1)`, id); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx,
			`select d.run_id, d.stage, d.task_id from `+s.table("dependent")+` d
			  join `+s.table("revision")+` r on r.hash = d.hash
			 where r.finding_id = $1`, id)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var d findings.Dependent
			if err := rows.Scan(&d.RunID, &d.Stage, &d.TaskID); err != nil {
				return err
			}
			deps = append(deps, d)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("pgstore: retract: %w", err)
	}
	return deps, nil
}

// Cite records that a finding was served to a task on some executor.
func (s *Store) Cite(ctx context.Context, hash string, d findings.Dependent) error {
	if hash == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`insert into `+s.table("dependent")+` (hash, run_id, stage, task_id) values ($1,$2,$3,$4)
		 on conflict do nothing`, hash, d.RunID, d.Stage, d.TaskID)
	if err != nil {
		return fmt.Errorf("pgstore: cite: %w", err)
	}
	return nil
}

// RecordVerdict memoizes an adjudication for every executor.
func (s *Store) RecordVerdict(ctx context.Context, j findings.Judgement) error {
	if j.QuestionKey == "" || j.Hash == "" {
		return nil
	}
	decided := j.Decided
	if decided.IsZero() {
		decided = s.opts.Now()
	}
	_, err := s.db.ExecContext(ctx,
		`insert into `+s.table("verdict")+` (question_key, hash, ok, similarity, decided, executor)
		 values ($1,$2,$3,$4,$5,$6)
		 on conflict (question_key, hash) do update
		    set ok = excluded.ok, similarity = excluded.similarity,
		        decided = excluded.decided, executor = excluded.executor`,
		j.QuestionKey, j.Hash, j.OK, j.Similarity, decided, j.Executor)
	if err != nil {
		return fmt.Errorf("pgstore: verdict: %w", err)
	}
	return nil
}

// SetThreshold persists an entry's learned near-match boundary.
func (s *Store) SetThreshold(ctx context.Context, hash string, threshold float64) error {
	if hash == "" || threshold <= 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`update `+s.table("revision")+` set threshold = $2 where hash = $1`, hash, threshold)
	if err != nil {
		return fmt.Errorf("pgstore: threshold: %w", err)
	}
	return nil
}

// --- findings.Store: reading --------------------------------------------

// Candidates returns the live entries recorded under a question key or about a
// subject class, newest first — the exact and class tiers in one round trip.
func (s *Store) Candidates(ctx context.Context, q findings.CandidateQuery) ([]findings.Entry, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 32
	}
	rows, err := s.db.QueryContext(ctx,
		`select `+revisionColumns+` from `+s.revisionSource()+`
		  where r.head and not r.retracted
		    and ( ($1 <> '' and exists (
		            select 1 from `+s.table("alias")+` a
		             where a.question_key = $1 and a.hash = r.hash))
		       or ($2 <> '' and r.class = $2) )
		  order by r.seq desc limit $3`, q.Key, q.Class, limit)
	if err != nil {
		return nil, fmt.Errorf("pgstore: candidates: %w", err)
	}
	return scanEntries(rows)
}

// Fetch resolves entries by hash, including superseded and retracted ones —
// lineage names hashes, so a hash stays resolvable however the claim was later
// revised.
func (s *Store) Fetch(ctx context.Context, hashes []string) ([]findings.Entry, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	blob, err := json.Marshal(hashes)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`select `+revisionColumns+` from `+s.revisionSource()+`
		  where r.hash in (select jsonb_array_elements_text($1::jsonb))`, blob)
	if err != nil {
		return nil, fmt.Errorf("pgstore: fetch: %w", err)
	}
	return scanEntries(rows)
}

func (s *Store) fetchOne(ctx context.Context, tx *sql.Tx, hash string) (findings.Entry, error) {
	rows, err := tx.QueryContext(ctx,
		`select `+revisionColumns+` from `+s.revisionSource()+` where r.hash = $1`, hash)
	if err != nil {
		return findings.Entry{}, err
	}
	out, err := scanEntries(rows)
	if err != nil {
		return findings.Entry{}, err
	}
	if len(out) == 0 {
		return findings.Entry{}, sql.ErrNoRows
	}
	return out[0], nil
}

func scanEntries(rows *sql.Rows) ([]findings.Entry, error) {
	defer rows.Close()
	var out []findings.Entry
	for rows.Next() {
		var (
			e         findings.Entry
			seq       int64
			id        string
			rev       int
			topic     string
			retracted bool
			noEv      bool
			supers    string
			conf      float64
			costUSD   float64
			latencyMS int64
			body      []byte
			usage     []byte
			embedding sql.NullString
		)
		if err := rows.Scan(&e.Hash, &seq, &id, &rev, &topic, &e.Key, &e.Class, &e.Knowledge,
			&retracted, &noEv, &supers, &conf, &costUSD, &latencyMS, &e.Learned,
			&e.Learner, &e.Executor, &e.Corroborations, &e.Threshold, &body, &usage,
			&embedding); err != nil {
			return nil, err
		}
		e.Vector = parseVector(embedding.String)
		if err := json.Unmarshal(body, &e.Finding); err != nil {
			return nil, err
		}
		// The denormalized columns exist to be queried and indexed, not to be
		// believed: the finding itself is the record, and it arrived as one
		// JSON document that hashes to the address it is filed under.
		e.Seq = int(seq)
		e.Latency = time.Duration(latencyMS) * time.Millisecond
		out = append(out, e)
	}
	return out, rows.Err()
}

// Dependents returns everything that was served a finding, from every executor.
func (s *Store) Dependents(ctx context.Context, hash string) ([]findings.Dependent, error) {
	rows, err := s.db.QueryContext(ctx,
		`select run_id, stage, task_id from `+s.table("dependent")+` where hash = $1 order by seen`, hash)
	if err != nil {
		return nil, fmt.Errorf("pgstore: dependents: %w", err)
	}
	defer rows.Close()
	var out []findings.Dependent
	for rows.Next() {
		var d findings.Dependent
		if err := rows.Scan(&d.RunID, &d.Stage, &d.TaskID); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Verdicts returns the adjudications recorded against a set of findings.
func (s *Store) Verdicts(ctx context.Context, hashes []string, limit int) ([]findings.Judgement, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 64
	}
	blob, err := json.Marshal(hashes)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`select question_key, hash, ok, similarity, decided, executor
		   from `+s.table("verdict")+`
		  where hash in (select jsonb_array_elements_text($1::jsonb))
		  order by decided desc limit $2`, blob, limit)
	if err != nil {
		return nil, fmt.Errorf("pgstore: verdicts: %w", err)
	}
	defer rows.Close()
	var out []findings.Judgement
	for rows.Next() {
		var j findings.Judgement
		if err := rows.Scan(&j.QuestionKey, &j.Hash, &j.OK, &j.Similarity, &j.Decided, &j.Executor); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// Topics summarizes what the commons holds, by topic, across every executor.
func (s *Store) Topics(ctx context.Context) ([]findings.TopicStat, error) {
	live := `head and not retracted`
	rows, err := s.db.QueryContext(ctx,
		`select topic,
		        count(*),
		        count(*) filter (where `+live+`),
		        count(*) filter (where retracted),
		        count(*) filter (where `+live+` and no_evidence),
		        coalesce(sum(corroborations) filter (where `+live+`), 0),
		        coalesce(sum(cost_usd) filter (where `+live+`), 0),
		        coalesce(sum(coalesce((usage->>'input_tokens')::bigint, 0)) filter (where `+live+`), 0),
		        coalesce(sum(coalesce((usage->>'output_tokens')::bigint, 0)) filter (where `+live+`), 0),
		        coalesce(sum(coalesce((usage->>'requests')::bigint, 0)) filter (where `+live+`), 0)
		   from `+s.table("revision")+`
		  group by topic order by topic`)
	if err != nil {
		return nil, fmt.Errorf("pgstore: topics: %w", err)
	}
	defer rows.Close()
	var out []findings.TopicStat
	for rows.Next() {
		var (
			st                     findings.TopicStat
			in, outTok, requests   int64
			cost                   float64
			entries, liveN, retrac int64
			negative, corr         int64
		)
		if err := rows.Scan(&st.Topic, &entries, &liveN, &retrac, &negative, &corr,
			&cost, &in, &outTok, &requests); err != nil {
			return nil, err
		}
		st.Entries, st.Live, st.Retracted = int(entries), int(liveN), int(retrac)
		st.Negative, st.Corroborations = int(negative), int(corr)
		st.Cost = core.Usage{
			InputTokens: int(in), OutputTokens: int(outTok),
			Requests: int(requests), CostUSD: cost,
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// --- findings.VectorStore -----------------------------------------------

// Upsert indexes a finding's question embedding.
func (s *Store) Upsert(ctx context.Context, v findings.Vector) error {
	if v.Hash == "" || len(v.Embedding) == 0 {
		return nil
	}
	return s.tx(ctx, func(tx *sql.Tx) error { return s.upsertVector(ctx, tx, v) })
}

func (s *Store) upsertVector(ctx context.Context, tx *sql.Tx, v findings.Vector) error {
	value := s.vectorLiteral(v.Embedding)
	_, err := tx.ExecContext(ctx,
		`insert into `+s.table("vector")+` (hash, topic, class, active, embedding)
		 values ($1,$2,$3,true,`+s.vectorCast("$4")+`)
		 on conflict (hash) do update
		    set topic = excluded.topic, class = excluded.class,
		        active = true, embedding = excluded.embedding`,
		v.Hash, v.Topic, v.Class, value)
	if err != nil {
		return fmt.Errorf("pgstore: upsert vector: %w", err)
	}
	return nil
}

// Remove deactivates a finding's vector, so a withdrawn claim stops being a
// similarity candidate. The row stays: what it was is part of the record.
func (s *Store) Remove(ctx context.Context, hash string) error {
	if hash == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`update `+s.table("vector")+` set active = false where hash = $1`, hash)
	if err != nil {
		return fmt.Errorf("pgstore: remove vector: %w", err)
	}
	return nil
}

// Nearest returns the top-K matches over the similarity floor, most similar
// first, filtered to a topic and — when the question has facets to pin it with
// — to one subject class.
func (s *Store) Nearest(ctx context.Context, q findings.VectorQuery) ([]findings.VectorMatch, error) {
	if len(q.Embedding) == 0 {
		return nil, nil
	}
	if s.vectors == VectorPgvector {
		return s.nearestPgvector(ctx, q)
	}
	return s.nearestScan(ctx, q)
}

func (s *Store) nearestPgvector(ctx context.Context, q findings.VectorQuery) ([]findings.VectorMatch, error) {
	topK := q.TopK
	if topK <= 0 {
		topK = 8
	}
	value := s.vectorLiteral(q.Embedding)
	rows, err := s.db.QueryContext(ctx,
		`select v.hash, 1 - (v.embedding <=> $1::vector) as similarity
		   from `+s.table("vector")+` v
		   join `+s.table("revision")+` r on r.hash = v.hash
		  where v.active and r.head and not r.retracted
		    and ($2 = '' or v.topic = $2)
		    and ($3 = '' or v.class = $3)
		    and 1 - (v.embedding <=> $1::vector) >= $4
		  order by v.embedding <=> $1::vector
		  limit $5`, value, q.Topic, q.Class, q.MinSimilarity, topK)
	if err != nil {
		return nil, fmt.Errorf("pgstore: nearest: %w", err)
	}
	defer rows.Close()
	var out []findings.VectorMatch
	for rows.Next() {
		var m findings.VectorMatch
		if err := rows.Scan(&m.Hash, &m.Similarity); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// nearestScan is the fallback for a database without pgvector: pull a bounded
// candidate set and score it here. It is linear and it is honest about being
// linear — the alternative is pretending an unindexed column is a vector index.
func (s *Store) nearestScan(ctx context.Context, q findings.VectorQuery) ([]findings.VectorMatch, error) {
	rows, err := s.db.QueryContext(ctx,
		`select v.hash, v.embedding
		   from `+s.table("vector")+` v
		   join `+s.table("revision")+` r on r.hash = v.hash
		  where v.active and r.head and not r.retracted
		    and ($1 = '' or v.topic = $1)
		    and ($2 = '' or v.class = $2)
		  order by r.seq desc limit $3`, q.Topic, q.Class, s.opts.Scan)
	if err != nil {
		return nil, fmt.Errorf("pgstore: nearest (scan): %w", err)
	}
	defer rows.Close()

	var out []findings.VectorMatch
	for rows.Next() {
		var hash string
		var blob []byte
		if err := rows.Scan(&hash, &blob); err != nil {
			return nil, err
		}
		var vec []float32
		if err := json.Unmarshal(blob, &vec); err != nil {
			continue
		}
		sim := cosine(q.Embedding, vec)
		if sim < q.MinSimilarity {
			continue
		}
		out = append(out, findings.VectorMatch{Hash: hash, Similarity: sim})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Similarity > out[j].Similarity })
	topK := q.TopK
	if topK <= 0 {
		topK = 8
	}
	if len(out) > topK {
		out = out[:topK]
	}
	return out, nil
}

// vectorLiteral renders an embedding for the column in use: pgvector's own
// text format, or JSON in the fallback. Both are plain strings, which is what
// keeps the driver out of the interface — nothing crossing this boundary is a
// pgx type, so the adapter is replaceable and so is the driver under it.
func (s *Store) vectorLiteral(v []float32) string {
	if s.vectors != VectorPgvector {
		blob, _ := json.Marshal(v)
		return string(blob)
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(f), 'f', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// vectorCast is the type a literal has to be read back as.
func (s *Store) vectorCast(placeholder string) string {
	if s.vectors != VectorPgvector {
		return placeholder + "::jsonb"
	}
	return placeholder + "::vector"
}

// parseVector reads an embedding back out of the database. Both column types
// render as a bracketed list — pgvector's own literal, and jsonb's array — so
// one parser covers both modes and neither needs a driver type to do it.
func parseVector(s string) []float32 {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil
	}
	s = s[1 : len(s)-1]
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]float32, 0, len(parts))
	for _, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil
		}
		out = append(out, float32(f))
	}
	return out
}

func cosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// --- findings.Leases ----------------------------------------------------

// Acquire takes the research lease for a key, or reports the current holder.
//
// The row is created before it is locked, and then read FOR UPDATE. That order
// matters: SELECT ... FOR UPDATE cannot lock a row that does not exist, so two
// executors racing for a lease nobody has ever taken would both find nothing
// and both insert. Inserting a released placeholder first turns "does this
// lease exist" into a question the primary key answers, and every decision
// after it happens with the row held.
func (s *Store) Acquire(ctx context.Context, key, owner string, ttl time.Duration) (findings.Lease, bool, error) {
	var out findings.Lease
	var held bool
	err := s.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`insert into `+s.table("lease")+` (key, owner, token, expires, released)
			 values ($1, '', 0, now(), true) on conflict (key) do nothing`, key); err != nil {
			return err
		}
		var cur findings.Lease
		var expired bool
		if err := tx.QueryRowContext(ctx,
			`select owner, token, acquired, expires, released, expires <= now()
			   from `+s.table("lease")+` where key = $1 for update`, key).
			Scan(&cur.Owner, &cur.Token, &cur.Acquired, &cur.Expires, &cur.Released, &expired); err != nil {
			return err
		}
		cur.Key = key
		if !cur.Released && !expired {
			out = cur
			return nil
		}
		// A lease that expired without being released belonged to an executor
		// that did not finish: crashed, killed, or paused past its horizon.
		takeover := !cur.Released && expired && cur.Token > 0
		if err := tx.QueryRowContext(ctx,
			`update `+s.table("lease")+`
			    set owner = $2, token = token + 1, acquired = now(),
			        expires = now() + make_interval(secs => $3), released = false
			  where key = $1
			 returning owner, token, acquired, expires, released`,
			key, owner, ttl.Seconds()).
			Scan(&out.Owner, &out.Token, &out.Acquired, &out.Expires, &out.Released); err != nil {
			return err
		}
		out.Key, out.Takeover, held = key, takeover, true
		return nil
	})
	if err != nil {
		return findings.Lease{}, false, fmt.Errorf("pgstore: acquire: %w", err)
	}
	return out, held, nil
}

// Renew extends a held lease, reporting false when it has been taken over.
func (s *Store) Renew(ctx context.Context, l findings.Lease, ttl time.Duration) (findings.Lease, bool, error) {
	out := l
	err := s.db.QueryRowContext(ctx,
		`update `+s.table("lease")+` set expires = now() + make_interval(secs => $4)
		  where key = $1 and owner = $2 and token = $3 and not released
		 returning owner, token, acquired, expires, released`,
		l.Key, l.Owner, l.Token, ttl.Seconds()).
		Scan(&out.Owner, &out.Token, &out.Acquired, &out.Expires, &out.Released)
	if errors.Is(err, sql.ErrNoRows) {
		return l, false, nil
	}
	if err != nil {
		return l, false, fmt.Errorf("pgstore: renew: %w", err)
	}
	return out, true, nil
}

// Release ends a lease, waking every follower at once.
//
// The owner and token are in the WHERE clause, which is the fencing: an
// executor whose lease expired and was taken over holds a stale token, so its
// release matches nothing. Without that check a slow executor waking up after
// its lease was reassigned would release the *new* owner's lease, and every
// follower waiting behind it would be sent to look for a finding that has not
// been contributed yet.
func (s *Store) Release(ctx context.Context, l findings.Lease) error {
	_, err := s.db.ExecContext(ctx,
		`update `+s.table("lease")+` set released = true, expires = now()
		  where key = $1 and owner = $2 and token = $3 and not released`,
		l.Key, l.Owner, l.Token)
	if err != nil {
		return fmt.Errorf("pgstore: release: %w", err)
	}
	return nil
}

// Peek reports the current state of a lease without taking it.
func (s *Store) Peek(ctx context.Context, key string) (findings.Lease, bool, error) {
	out := findings.Lease{Key: key}
	err := s.db.QueryRowContext(ctx,
		`select owner, token, acquired, expires, released from `+s.table("lease")+` where key = $1`, key).
		Scan(&out.Owner, &out.Token, &out.Acquired, &out.Expires, &out.Released)
	if errors.Is(err, sql.ErrNoRows) {
		return findings.Lease{Key: key}, false, nil
	}
	if err != nil {
		return findings.Lease{}, false, fmt.Errorf("pgstore: peek: %w", err)
	}
	return out, true, nil
}

// --- plumbing -----------------------------------------------------------

func (s *Store) tx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Store implements the whole backend contract.
var _ findings.Backend = (*Store)(nil)
