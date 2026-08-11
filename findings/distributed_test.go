package findings_test

// Multi-process tests for the distributed commons.
//
// Every test here launches real executor processes — separate `os.Exec`
// children, separate address spaces, separate in-process ledgers — against one
// shared backend, and asserts on what the *source* saw. That last part is the
// point. A test that asserted on hit counters would be asking the layer to
// grade its own homework; these count lines in a file that only an external
// call appends to, so "the second executor did not call the source" is a
// measurement rather than a claim.
//
// The suite runs against every backend that is available: the file backend
// always, and PostgreSQL whenever LOOM_FINDINGS_PG_DSN is set. Both are exercised
// through the same gate, which is the check that the interfaces are really the
// seam they claim to be.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/findings"
	"github.com/zionrubin/loom/findings/filestore"
	"github.com/zionrubin/loom/findings/pgstore"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
)

// --- the executor child -------------------------------------------------

// childEnv carries the spec to a child process. Its presence is what turns a
// test binary into an executor.
const childEnv = "LOOM_FINDINGS_EXECUTOR_SPEC"

// spec is one executor process: which commons to join, what to ask, what the
// source would say, and what to do with it.
type spec struct {
	Backend string `json:"backend"` // "file" or "pg"
	Addr    string `json:"addr"`    // directory, or DSN
	Prefix  string `json:"prefix"`  // pg table prefix
	Name    string `json:"name"`    // executor identity
	Out     string `json:"out"`     // where to write the result
	Action  string `json:"action"`

	Topic      string            `json:"topic"`
	Text       string            `json:"text"`
	Facets     map[string]string `json:"facets,omitempty"`
	Needs      []string          `json:"needs,omitempty"`
	Volatility string            `json:"volatility,omitempty"`

	// The scripted source.
	Answer  string         `json:"answer,omitempty"`
	Fields  map[string]any `json:"fields,omitempty"`
	Covers  []string       `json:"covers,omitempty"`
	CallLog string         `json:"call_log,omitempty"`
	Delay   time.Duration  `json:"delay,omitempty"`
	Fail    bool           `json:"fail,omitempty"`

	// The reader's envelope. Empty grants mean "everything this test needs".
	Grants []string `json:"grants,omitempty"`
	Hosts  []string `json:"hosts,omitempty"`

	// Coordination with the parent.
	Ready string `json:"ready,omitempty"` // touch this when set up
	Start string `json:"start,omitempty"` // wait for this before asking

	// Knobs.
	Skew       time.Duration `json:"skew,omitempty"` // this executor's clock offset
	LeaseTTL   time.Duration `json:"lease_ttl,omitempty"`
	MaxWait    time.Duration `json:"max_wait,omitempty"`
	Refresh    time.Duration `json:"refresh,omitempty"`
	Embed      bool          `json:"embed,omitempty"`
	Near       float64       `json:"near,omitempty"`
	Adjudicate bool          `json:"adjudicate,omitempty"`
	Judgement  bool          `json:"judgement,omitempty"` // what the judge says
	Strict     bool          `json:"strict,omitempty"`
	ClaimID    string        `json:"claim_id,omitempty"`
	Hash       string        `json:"hash,omitempty"`
}

// result is what a child reports back.
type result struct {
	Origin     string               `json:"origin"`
	Text       string               `json:"text"`
	Executor   string               `json:"executor"`
	Hash       string               `json:"hash"`
	ClaimID    string               `json:"claim_id"`
	Found      bool                 `json:"found"`
	Err        string               `json:"err"`
	Judged     int                  `json:"judged"`
	Stats      findings.Stats       `json:"stats"`
	Dependents []findings.Dependent `json:"dependents,omitempty"`
	Topics     []findings.TopicStat `json:"topics,omitempty"`
	Verdicts   []findings.Judgement `json:"verdicts,omitempty"`
	Threshold  float64              `json:"threshold,omitempty"`
	Corrob     int                  `json:"corroborations,omitempty"`
}

// TestExecutorProcess is the executor a spawned child runs. It is a test
// function because a test binary has no other entry point; without the spec in
// its environment it does nothing at all.
func TestExecutorProcess(t *testing.T) {
	blob := os.Getenv(childEnv)
	if blob == "" {
		t.Skip("not an executor child")
	}
	var s spec
	if err := json.Unmarshal([]byte(blob), &s); err != nil {
		t.Fatalf("spec: %v", err)
	}
	res := s.execute()
	out, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	if err := os.WriteFile(s.Out, out, 0o644); err != nil {
		t.Fatalf("write result: %v", err)
	}
}

// execute is the whole of an executor: join the commons, ask one question,
// report what happened.
func (s spec) execute() result {
	ctx := context.Background()
	backend, err := s.open()
	if err != nil {
		return result{Err: err.Error()}
	}
	shared := findings.NewShared(findings.SharedConfig{
		Backend:  backend,
		Executor: s.Name,
		LeaseTTL: orDuration(s.LeaseTTL, 2*time.Second),
		Poll:     10 * time.Millisecond,
		Refresh:  orDuration(s.Refresh, time.Minute),
		Strict:   s.Strict,
	})

	cas, err := store.NewCAS("")
	if err != nil {
		return result{Err: err.Error()}
	}
	// A fresh ledger every time: these are independent executors, so nothing
	// may reach one of them except through the commons.
	ledger, err := findings.NewLedger(cas, "")
	if err != nil {
		return result{Err: err.Error()}
	}
	judged := 0
	policy := findings.Policy{
		Default: findings.TopicPolicy{},
		Topics: map[string]findings.TopicPolicy{
			s.Topic: {
				Volatility: findings.Volatility(s.Volatility),
				Near:       s.Near,
				Adjudicate: s.Adjudicate,
			},
		},
		MaxWait: orDuration(s.MaxWait, 15*time.Second),
	}
	if s.Skew != 0 {
		skew := s.Skew
		policy.Now = func() time.Time { return time.Now().Add(skew) }
	}
	if s.Embed {
		policy.Embedder = bagOfWords{vocab: testVocab}
	}
	if s.Adjudicate {
		policy.JudgeCostUSD = 0.0001
		policy.Judge = func(context.Context, findings.Question, findings.Finding) (bool, error) {
			judged++
			return s.Judgement, nil
		}
	}
	gate := findings.NewGate(ledger, policy).Share(shared)
	defer func() { _ = gate.Close(); _ = ledger.Close() }()

	req := findings.Request{
		Question: findings.Question{
			Topic: s.Topic, Text: s.Text, Facets: s.Facets, Needs: s.Needs,
		},
		Grants: s.grants(),
		Egress: s.egress(),
		RunID:  "run_" + s.Name,
		Stage:  "research",
		TaskID: "task_" + s.Name,
	}

	s.touch(s.Ready)
	s.await(s.Start)

	res := result{Judged: judged}
	switch s.Action {
	case "lookup":
		ans, ok := gate.Lookup(ctx, req)
		res.Found, res.Origin, res.Text = ok, string(ans.Origin), ans.Text
		res.Hash, res.Executor = ans.Hash, ans.Executor

	case "retract":
		deps, err := gate.Retract(ctx, s.ClaimID, "the source published a correction")
		if err != nil {
			res.Err = err.Error()
		}
		res.Dependents = deps

	case "inspect":
		// A fresh process reading what the commons remembers: the thing every
		// piece of mutable metadata has to survive.
		shared.Flush(ctx)
		res.Dependents = gate.Dependents(ctx, s.Hash)
		res.Topics = gate.Commons(ctx)
		if b := shared.Backend(); b != nil {
			res.Verdicts, _ = b.Verdicts(ctx, []string{s.Hash}, 16)
			if entries, err := b.Fetch(ctx, []string{s.Hash}); err == nil && len(entries) == 1 {
				res.Threshold = entries[0].Threshold
				res.Corrob = entries[0].Corroborations
			}
		}
		res.Found = true

	default: // "research"
		ans, err := gate.Research(ctx, req, s.fetch())
		if err != nil {
			res.Err = err.Error()
			break
		}
		res.Found = true
		res.Origin, res.Text = string(ans.Origin), ans.Text
		res.Hash, res.Executor = ans.Hash, ans.Executor
		res.ClaimID = ans.Finding.ID
	}

	// The citation queue is write-behind, so a report has to drain it before it
	// can say what rested on a finding.
	shared.Flush(ctx)
	res.Stats = gate.Stats()
	res.Judged = judged
	return res
}

// fetch is the scripted public source: it records that it was called, takes its
// time, and answers.
func (s spec) fetch() findings.Fetch {
	return func(ctx context.Context, q findings.Question) (findings.Result, error) {
		if s.CallLog != "" {
			f, err := os.OpenFile(s.CallLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err == nil {
				fmt.Fprintf(f, "%s\t%s\n", s.Name, q.Text)
				_ = f.Close()
			}
		}
		if s.Delay > 0 {
			select {
			case <-time.After(s.Delay):
			case <-ctx.Done():
				return findings.Result{}, ctx.Err()
			}
		}
		if s.Fail {
			return findings.Result{}, errors.New("the source is down")
		}
		return findings.Result{
			Text:   s.Answer,
			Fields: s.Fields,
			Covers: s.Covers,
			Sources: []findings.Source{{
				Tool: "search", Host: "api.example.com", URI: "https://example.com/x",
			}},
			Cost:    core.Usage{Requests: 1, CostUSD: 0.004},
			Latency: s.Delay,
		}, nil
	}
}

func (s spec) open() (findings.Backend, error) {
	switch s.Backend {
	case "pg":
		return pgstore.Open(context.Background(), s.Addr, pgstore.Options{
			Prefix: s.Prefix, Dimensions: len(testVocab),
		})
	default:
		return filestore.Open(s.Addr, filestore.Options{})
	}
}

func (s spec) grants() security.GrantSet {
	if len(s.Grants) == 0 {
		return security.NewGrantSet(security.ToolCap("search"))
	}
	caps := make([]security.Capability, 0, len(s.Grants))
	for _, c := range s.Grants {
		caps = append(caps, security.Capability(c))
	}
	return security.NewGrantSet(caps...)
}

func (s spec) egress() security.EgressPolicy {
	if len(s.Hosts) == 0 {
		return security.EgressPolicy{}.With("api.example.com")
	}
	return security.EgressPolicy{}.With(s.Hosts...)
}

func (s spec) touch(path string) {
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err == nil {
		_ = f.Close()
	}
}

// await blocks until the parent's start file appears — the barrier that makes
// "at the same instant" mean something across processes.
func (s spec) await(path string) {
	if path == "" {
		return
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func orDuration(d, fallback time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return fallback
}

// testVocab is the fixed vocabulary the deterministic embedder scores over.
var testVocab = []string{
	"revenue", "earnings", "headcount", "staff", "litigation", "legal",
	"northwind", "contoso", "annual", "quarterly", "weather", "rain",
}

// bagOfWords is a deterministic stand-in for a real embedder, so the vector
// tier is testable without a model and identical in every process.
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

// --- the harness --------------------------------------------------------

// commons is one shared backend under test, and the parent's handle on the
// evidence: where the executors are pointed, and what the source recorded.
type commons struct {
	t       *testing.T
	backend string
	addr    string
	prefix  string
	dir     string
	calls   string
	seq     int
}

// backends returns every commons this machine can run the suite against.
func backends(t *testing.T) []func(t *testing.T) *commons {
	out := []func(t *testing.T) *commons{fileCommons}
	if strings.TrimSpace(os.Getenv("LOOM_FINDINGS_PG_DSN")) != "" {
		out = append(out, pgCommons)
	}
	return out
}

func fileCommons(t *testing.T) *commons {
	dir := t.TempDir()
	return &commons{
		t: t, backend: "file",
		addr:  filepath.Join(dir, "commons"),
		dir:   dir,
		calls: filepath.Join(dir, "calls.log"),
	}
}

func pgCommons(t *testing.T) *commons {
	dsn := os.Getenv("LOOM_FINDINGS_PG_DSN")
	dir := t.TempDir()
	prefix := fmt.Sprintf("x%d", time.Now().UnixNano()%1e9)
	c := &commons{
		t: t, backend: "pg", addr: dsn, prefix: prefix,
		dir: dir, calls: filepath.Join(dir, "calls.log"),
	}
	t.Cleanup(func() {
		s, err := pgstore.Open(context.Background(), dsn, pgstore.Options{Prefix: prefix, SkipMigrate: true})
		if err != nil {
			return
		}
		defer s.Close()
		for _, table := range []string{"alias", "vector", "dependent", "verdict", "lease", "revision"} {
			_, _ = s.DB().Exec(`drop table if exists ` + prefix + `_` + table + ` cascade`)
		}
	})
	return c
}

func (c *commons) name() string { return c.backend }

// spawn starts an executor process and waits for it.
func (c *commons) spawn(s spec) result {
	c.t.Helper()
	cmd := c.start(&s)
	if err := cmd.Wait(); err != nil {
		c.t.Fatalf("executor %s: %v", s.Name, err)
	}
	return c.collect(s)
}

// start launches an executor without waiting, for the tests that need two of
// them in flight at once.
func (c *commons) start(s *spec) *exec.Cmd {
	c.t.Helper()
	c.seq++
	s.Backend, s.Addr, s.Prefix = c.backend, c.addr, c.prefix
	if s.CallLog == "" {
		s.CallLog = c.calls
	}
	if s.Name == "" {
		s.Name = fmt.Sprintf("executor-%d", c.seq)
	}
	if s.Out == "" {
		s.Out = filepath.Join(c.dir, fmt.Sprintf("result-%d-%s.json", c.seq, s.Name))
	}
	blob, err := json.Marshal(s)
	if err != nil {
		c.t.Fatalf("spec: %v", err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestExecutorProcess$", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), childEnv+"="+string(blob))
	cmd.Stdout, cmd.Stderr = &strings.Builder{}, os.Stderr
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", s.Name, err)
	}
	return cmd
}

func (c *commons) collect(s spec) result {
	c.t.Helper()
	blob, err := os.ReadFile(s.Out)
	if err != nil {
		c.t.Fatalf("executor %s produced no result: %v", s.Name, err)
	}
	var res result
	if err := json.Unmarshal(blob, &res); err != nil {
		c.t.Fatalf("executor %s result: %v", s.Name, err)
	}
	return res
}

// sourceCalls is how many times the public source was actually reached, by
// every executor, counted from the file only the source writes to.
func (c *commons) sourceCalls() int {
	c.t.Helper()
	blob, err := os.ReadFile(c.calls)
	if err != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(string(blob), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

func (c *commons) waitForFile(path string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// ask is the question most of these tests are about.
func ask(text string) spec {
	return spec{
		Topic: "company", Text: text,
		Facets: map[string]string{"co": "northwind"},
		Answer: "Northwind — revenue $4.2bn, headcount 12,000.",
		Fields: map[string]any{"revenue": "$4.2bn", "headcount": 12000},
	}
}

// forEachBackend runs a test against every backend available on this machine.
func forEachBackend(t *testing.T, fn func(t *testing.T, c *commons)) {
	t.Helper()
	for _, open := range backends(t) {
		c := open(t)
		t.Run(c.name(), func(t *testing.T) {
			inner := open(t)
			inner.t = t
			fn(t, inner)
		})
	}
}

// --- 1 & 2: one executor researches, the next reuses --------------------

func TestSecondExecutorReusesTheFirstsResearchWithoutCallingTheSource(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		first := ask("what is northwind's revenue and headcount")
		first.Name = "executor-a"
		a := c.spawn(first)
		if a.Err != "" {
			t.Fatalf("executor-a: %s", a.Err)
		}
		if a.Origin != string(findings.OriginFresh) {
			t.Fatalf("the first executor must research: origin = %s", a.Origin)
		}
		if got := c.sourceCalls(); got != 1 {
			t.Fatalf("source calls after the first executor = %d, want 1", got)
		}

		second := ask("what is northwind's revenue and headcount")
		second.Name = "executor-b"
		b := c.spawn(second)
		if b.Err != "" {
			t.Fatalf("executor-b: %s", b.Err)
		}
		if b.Origin != string(findings.OriginRemoteExact) {
			t.Fatalf("the second executor must be served from the commons: origin = %s", b.Origin)
		}
		if got := c.sourceCalls(); got != 1 {
			t.Fatalf("source calls after two executors = %d, want 1 — "+
				"the second must not have reached the source", got)
		}
		if b.Text != a.Text {
			t.Fatalf("a reused answer must be the answer it replaced:\n  %q\n  %q", b.Text, a.Text)
		}
		if b.Executor != "executor-a" {
			t.Fatalf("the answer must name the executor whose research it was, got %q", b.Executor)
		}
		if b.Stats.SharedReuse() != 1 || b.Stats.Adopted == 0 {
			t.Fatalf("cross-executor reuse must be counted as such: %+v", b.Stats)
		}
		if b.Stats.LocalReuse() != 0 {
			t.Fatalf("a fresh executor has nothing local to reuse: %+v", b.Stats)
		}
	})
}

// --- 3: different words, one subject ------------------------------------

func TestDifferentlyWordedQuestionsAboutOneSubjectShareAFinding(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		a := c.spawn(ask("what are northwind's revenue and headcount"))
		if a.Err != "" {
			t.Fatalf("executor-a: %s", a.Err)
		}

		// Same facets, entirely different sentence: one subject, two phrasings,
		// and no exact key in common.
		b := c.spawn(ask("northwind: earnings and staff count please"))
		if b.Err != "" {
			t.Fatalf("executor-b: %s", b.Err)
		}
		if b.Origin != string(findings.OriginRemoteClass) {
			t.Fatalf("a differently worded question about one subject must hit the class tier: %s", b.Origin)
		}
		if got := c.sourceCalls(); got != 1 {
			t.Fatalf("source calls = %d, want 1", got)
		}
		if b.Hash != a.Hash {
			t.Fatalf("both executors must be talking about one finding: %s vs %s", b.Hash, a.Hash)
		}
	})
}

// --- 4: concurrent misses collapse to one call --------------------------

func TestConcurrentExecutorsMakeExactlyOneExternalCall(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		start := filepath.Join(c.dir, "start")
		readyA := filepath.Join(c.dir, "ready-a")
		readyB := filepath.Join(c.dir, "ready-b")

		specA := ask("what are northwind's revenue and headcount")
		specA.Name, specA.Ready, specA.Start = "executor-a", readyA, start
		specA.Delay = 400 * time.Millisecond

		specB := ask("northwind earnings and staff count")
		specB.Name, specB.Ready, specB.Start = "executor-b", readyB, start
		specB.Delay = 400 * time.Millisecond

		cmdA := c.start(&specA)
		cmdB := c.start(&specB)
		if !c.waitForFile(readyA, 30*time.Second) || !c.waitForFile(readyB, 30*time.Second) {
			t.Fatal("executors did not come up")
		}
		// Both processes are connected and waiting: release them together, which
		// is the case a content-addressed cache structurally cannot help with.
		if f, err := os.Create(start); err == nil {
			_ = f.Close()
		}
		if err := cmdA.Wait(); err != nil {
			t.Fatalf("executor-a: %v", err)
		}
		if err := cmdB.Wait(); err != nil {
			t.Fatalf("executor-b: %v", err)
		}

		a, b := c.collect(specA), c.collect(specB)
		if a.Err != "" || b.Err != "" {
			t.Fatalf("executors failed: %q / %q", a.Err, b.Err)
		}
		if got := c.sourceCalls(); got != 1 {
			t.Fatalf("source calls = %d, want exactly 1 — two executors asking at once "+
				"must produce one call between them", got)
		}

		leader, follower := a, b
		if b.Origin == string(findings.OriginFresh) {
			leader, follower = b, a
		}
		if leader.Origin != string(findings.OriginFresh) {
			t.Fatalf("one executor must have researched: %s / %s", a.Origin, b.Origin)
		}
		if !findings.Origin(follower.Origin).Remote() {
			t.Fatalf("the other must have been served from the commons, got %s", follower.Origin)
		}
		if follower.Text != leader.Text {
			t.Fatalf("the follower must get the leader's answer:\n  %q\n  %q", follower.Text, leader.Text)
		}
		if leader.Stats.Leader != 1 {
			t.Fatalf("the leader must record taking the lease: %+v", leader.Stats)
		}
		if follower.Stats.Follower == 0 {
			t.Fatalf("the follower must record waiting for one: %+v", follower.Stats)
		}
	})
}

// --- 5: a crashed lease owner does not block the question ---------------

func TestCrashedLeaseOwnerDoesNotBlockFutureResearch(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		ready := filepath.Join(c.dir, "ready-dead")

		// An executor that takes the lease and dies mid-research: no release, no
		// heartbeat, nothing to tell anyone it is gone.
		dying := ask("what are northwind's revenue and headcount")
		dying.Name = "executor-doomed"
		dying.Ready = ready
		dying.Delay = 30 * time.Second
		dying.LeaseTTL = 400 * time.Millisecond
		cmd := c.start(&dying)
		if !c.waitForFile(ready, 30*time.Second) {
			t.Fatal("the doomed executor did not come up")
		}
		// Give it time to take the lease and enter its fetch, then kill it
		// outright — the lease is now held by a process that no longer exists.
		time.Sleep(300 * time.Millisecond)
		if err := cmd.Process.Kill(); err != nil {
			t.Fatalf("kill: %v", err)
		}
		_ = cmd.Wait()

		survivor := ask("what are northwind's revenue and headcount")
		survivor.Name = "executor-live"
		survivor.LeaseTTL = 2 * time.Second
		survivor.MaxWait = 20 * time.Second

		done := make(chan result, 1)
		go func() { done <- c.spawn(survivor) }()
		select {
		case res := <-done:
			if res.Err != "" {
				t.Fatalf("executor-live: %s", res.Err)
			}
			if res.Origin != string(findings.OriginFresh) {
				t.Fatalf("the surviving executor must research the question: %s", res.Origin)
			}
			if res.Stats.LeaseTakeovers == 0 {
				t.Fatalf("taking over an expired lease must be counted: %+v", res.Stats)
			}
		case <-time.After(45 * time.Second):
			t.Fatal("a crashed lease owner blocked the question")
		}
	})
}

// --- 6: stale, retracted and insufficient findings are not served -------

func TestStaleFindingsAreNotServedAcrossExecutors(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		first := ask("northwind incident status")
		first.Topic, first.Volatility = "status", string(findings.Hourly)
		first.Facets = map[string]string{"co": "northwind"}
		if res := c.spawn(first); res.Err != "" {
			t.Fatalf("executor-a: %s", res.Err)
		}

		// The same question on an executor whose clock is two hours further on:
		// the finding is inside the commons and outside its topic's horizon.
		later := first
		later.Name = "executor-late"
		later.Skew = 2 * time.Hour
		res := c.spawn(later)
		if res.Err != "" {
			t.Fatalf("executor-late: %s", res.Err)
		}
		if res.Origin != string(findings.OriginFresh) {
			t.Fatalf("a stale finding must not be served: %s", res.Origin)
		}
		if res.Stats.Stale == 0 {
			t.Fatalf("a stale candidate must be counted as one: %+v", res.Stats)
		}
		if got := c.sourceCalls(); got != 2 {
			t.Fatalf("source calls = %d, want 2 — the stale finding must be re-researched", got)
		}
	})
}

func TestRetractedFindingsAreNotServedAcrossExecutors(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		a := c.spawn(ask("what are northwind's revenue and headcount"))
		if a.Err != "" {
			t.Fatalf("executor-a: %s", a.Err)
		}

		retract := spec{Action: "retract", Name: "executor-retractor", Topic: "company", ClaimID: a.ClaimID}
		if res := c.spawn(retract); res.Err != "" {
			t.Fatalf("retract: %s", res.Err)
		}

		b := c.spawn(ask("what are northwind's revenue and headcount"))
		if b.Err != "" {
			t.Fatalf("executor-b: %s", b.Err)
		}
		if b.Origin != string(findings.OriginFresh) {
			t.Fatalf("a retracted claim must not be served: %s", b.Origin)
		}
		if got := c.sourceCalls(); got != 2 {
			t.Fatalf("source calls = %d, want 2 — the retracted claim must be re-researched", got)
		}
	})
}

func TestInsufficientCoverageIsNotServedAcrossExecutors(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		// The first executor learns only the revenue.
		first := ask("northwind revenue")
		first.Fields = map[string]any{"revenue": "$4.2bn"}
		first.Covers = []string{"revenue"}
		first.Needs = []string{"revenue"}
		if res := c.spawn(first); res.Err != "" {
			t.Fatalf("executor-a: %s", res.Err)
		}

		// The second needs the litigation too, which nothing in the commons
		// covers.
		second := ask("northwind revenue")
		second.Name = "executor-b"
		second.Needs = []string{"revenue", "litigation"}
		second.Fields = map[string]any{"revenue": "$4.2bn", "litigation": "two open matters"}
		second.Covers = []string{"revenue", "litigation"}
		res := c.spawn(second)
		if res.Err != "" {
			t.Fatalf("executor-b: %s", res.Err)
		}
		if findings.Origin(res.Origin).Remote() {
			t.Fatalf("a finding that does not cover the question must not be served: %s", res.Origin)
		}
		if got := c.sourceCalls(); got != 2 {
			t.Fatalf("source calls = %d, want 2", got)
		}
	})
}

// --- 7: containment applies to remotely stored findings -----------------

func TestFindingsAreNotServedToExecutorsWithoutTheGrants(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		if res := c.spawn(ask("what are northwind's revenue and headcount")); res.Err != "" {
			t.Fatalf("executor-a: %s", res.Err)
		}

		// An executor that could not have done this research itself: it holds no
		// grant for the tool the finding's research consumed.
		ungranted := ask("what are northwind's revenue and headcount")
		ungranted.Name = "executor-ungranted"
		ungranted.Grants = []string{"tool:something-else"}
		res := c.spawn(ungranted)
		if res.Err != "" {
			t.Fatalf("executor-ungranted: %s", res.Err)
		}
		if findings.Origin(res.Origin).Remote() {
			t.Fatalf("the commons must not serve research the reader was not allowed to do: %s", res.Origin)
		}
		if res.Stats.Denied == 0 {
			t.Fatalf("a denial must be counted as a denial, not a miss: %+v", res.Stats)
		}

		// And one that cannot reach the host the research came from.
		walled := ask("what are northwind's revenue and headcount")
		walled.Name = "executor-walled"
		walled.Hosts = []string{"intranet.example"}
		res = c.spawn(walled)
		if res.Err != "" {
			t.Fatalf("executor-walled: %s", res.Err)
		}
		if findings.Origin(res.Origin).Remote() {
			t.Fatalf("egress containment must apply to shared findings too: %s", res.Origin)
		}
		if res.Stats.Denied == 0 {
			t.Fatalf("an egress denial must be counted: %+v", res.Stats)
		}
	})
}

// --- 8: vector candidates still face the ladder -------------------------

func TestVectorCandidatesFromTheCommonsPassSufficiencyAndAdjudication(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		// No facets: the class tier has nothing to be certain about, so
		// similarity search over the shared backend is the only tier that can
		// find this.
		first := spec{
			Topic: "web", Text: "northwind annual revenue", Embed: true, Near: 0.5,
			Answer: "Revenue $4.2bn", Fields: map[string]any{"revenue": "$4.2bn"},
			Name: "executor-a",
		}
		if res := c.spawn(first); res.Err != "" {
			t.Fatalf("executor-a: %s", res.Err)
		}

		paraphrase := first
		paraphrase.Name = "executor-b"
		paraphrase.Text = "annual revenue northwind"
		res := c.spawn(paraphrase)
		if res.Err != "" {
			t.Fatalf("executor-b: %s", res.Err)
		}
		if res.Origin != string(findings.OriginRemoteNear) {
			t.Fatalf("a paraphrase must be served through the shared vector tier: %s", res.Origin)
		}
		if got := c.sourceCalls(); got != 1 {
			t.Fatalf("source calls = %d, want 1", got)
		}

		// Now the same shape with an adjudicator that rejects the candidate: a
		// vector match is a candidate, and a candidate the judge refuses is not
		// served however similar it was.
		judged := first
		judged.Name = "executor-judge"
		judged.Text = "quarterly revenue northwind"
		judged.Adjudicate, judged.Judgement = true, false
		res = c.spawn(judged)
		if res.Err != "" {
			t.Fatalf("executor-judge: %s", res.Err)
		}
		if findings.Origin(res.Origin).Remote() {
			t.Fatalf("an adjudicated rejection must not be served: %s", res.Origin)
		}

		// And the verdict is now the commons': another executor asking the same
		// thing inherits the judgement instead of paying for it again.
		inheritor := judged
		inheritor.Name = "executor-inheritor"
		res = c.spawn(inheritor)
		if res.Err != "" {
			t.Fatalf("executor-inheritor: %s", res.Err)
		}
		if res.Judged != 0 {
			t.Fatalf("an adjudication recorded by another executor must not be paid for twice (judged %d)", res.Judged)
		}
	})
}

// --- 10: mutable metadata survives every restart ------------------------

func TestMutableMetadataSurvivesRestart(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		// One executor researches it. A learner is not a dependent — nothing
		// rested on this finding when it was made, which is why the ledger
		// records the two facts separately.
		a := c.spawn(ask("what are northwind's revenue and headcount"))
		if a.Err != "" {
			t.Fatalf("executor-a: %s", a.Err)
		}
		// A second cannot be served it — it holds no grant for the tool the
		// research consumed — so it researches, reaches the same conclusion, and
		// the commons records that as corroboration rather than a rival claim.
		second := ask("northwind revenue and headcount, restated")
		second.Name = "executor-b"
		second.Grants = []string{"tool:something-else"}
		if res := c.spawn(second); res.Err != "" {
			t.Fatalf("executor-b: %s", res.Err)
		}
		// Two more are served it, from two more processes: two citations.
		for _, name := range []string{"executor-c", "executor-d"} {
			served := ask("what are northwind's revenue and headcount")
			served.Name = name
			if res := c.spawn(served); res.Err != "" {
				t.Fatalf("%s: %s", name, res.Err)
			}
		}

		// A fresh process, holding nothing: everything it can say about this
		// finding came out of the commons.
		res := c.spawn(spec{Action: "inspect", Name: "executor-restarted", Topic: "company", Hash: a.Hash})
		if res.Err != "" {
			t.Fatalf("inspect: %s", res.Err)
		}
		if len(res.Dependents) < 2 {
			t.Fatalf("citations must survive: %v", res.Dependents)
		}
		runs := map[string]bool{}
		for _, d := range res.Dependents {
			runs[d.RunID] = true
		}
		if !runs["run_executor-c"] || !runs["run_executor-d"] {
			t.Fatalf("every executor's citation must survive: %v", res.Dependents)
		}
		if res.Corrob == 0 {
			t.Fatalf("corroboration must survive: %+v", res)
		}
		var live int
		for _, topic := range res.Topics {
			if topic.Topic == "company" {
				live = topic.Live
			}
		}
		if live == 0 {
			t.Fatalf("the commons summary must survive: %v", res.Topics)
		}
	})
}

func TestAdjudicationVerdictsAndThresholdsSurviveRestart(t *testing.T) {
	forEachBackend(t, func(t *testing.T, c *commons) {
		first := spec{
			Topic: "web", Text: "northwind annual revenue", Embed: true, Near: 0.5,
			Answer: "Revenue $4.2bn", Fields: map[string]any{"revenue": "$4.2bn"},
			Name: "executor-a",
		}
		a := c.spawn(first)
		if a.Err != "" {
			t.Fatalf("executor-a: %s", a.Err)
		}

		rejecting := first
		rejecting.Name = "executor-judge"
		rejecting.Text = "quarterly revenue northwind"
		rejecting.Adjudicate, rejecting.Judgement = true, false
		if res := c.spawn(rejecting); res.Err != "" {
			t.Fatalf("executor-judge: %s", res.Err)
		}

		res := c.spawn(spec{Action: "inspect", Name: "executor-restarted", Topic: "web", Hash: a.Hash})
		if res.Err != "" {
			t.Fatalf("inspect: %s", res.Err)
		}
		if len(res.Verdicts) == 0 {
			t.Fatalf("an adjudication must survive restart: %+v", res)
		}
		if res.Verdicts[0].OK {
			t.Fatalf("the verdict must survive as the verdict it was: %+v", res.Verdicts[0])
		}
		// A rejection raises the entry's own near-match boundary, and that
		// learning belongs to the commons rather than to the process that did it.
		if res.Threshold <= 0.5 {
			t.Fatalf("the learned threshold must survive restart, got %v", res.Threshold)
		}
	})
}
