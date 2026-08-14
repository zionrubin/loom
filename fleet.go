package loom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/delta"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/findings"
	"github.com/zionrubin/loom/mcp"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/ops"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/worker"
)

// Fleet runs many pipelines at once as one engine.
//
// A single Run provisions everything it needs and tears it down again: a rate
// limiter, a budget governor, a result cache, a pool of execution slots. That
// is right for one pipeline and wrong for several, because the things it
// provisions are not properties of a pipeline — they are properties of the
// account, the wallet, and the machine. Two Runs in one process each believe
// they own the provider's whole quota, so together they exceed it. Each
// enforces its own dollar ceiling, so neither enforces yours. Neither can
// replay the other's completed work. And nothing schedules them against each
// other, so the short one waits on the long one for no reason.
//
// A Fleet holds those things once and lends them to every agent on it:
//
//   - One rate limiter, so a fleet of ten agents respects one quota.
//   - One budget governor, so the ceiling is the fleet's, not each agent's.
//   - One content-addressed store and result cache, so work an agent already
//     paid for is free to the next agent that needs it.
//   - One pool of execution slots, admitted fairly across agents rather than
//     first-come-first-served, so a short agent's completion time is set by
//     its own size and not by whatever large agent got there first (see
//     runtime.Pool).
//   - One event bus, so a single observer — the constellation view's universe,
//     say — holds every agent side by side as it runs.
//
// Agents also need to reach each other's conclusions, and for that a Fleet
// carries a blackboard: append-only topics an agent posts to and later agents
// read (see Post). Coordination happens between agents rather than inside a
// task, which is deliberate — a task that could publish mid-run would make its
// own cached result depend on execution order, which is exactly what
// content-addressed replay assumes away.
//
// A Fleet is safe for concurrent use. Agents run pipelined (the driver
// loom.WithStreaming selects for a single run), because a fleet's whole
// purpose is one continuously-fed engine rather than a barrier per stage.
type Fleet struct {
	*host

	pool *runtime.Pool

	mu      sync.Mutex
	cond    *sync.Cond
	board   map[string][]Post
	topics  []string
	agents  []*Agent
	started time.Time
	closed  bool
}

// Post is one entry on a fleet's blackboard: a value an agent published for
// later agents to read.
//
// A post carries no timestamp, and that is not an omission. A topic's
// snapshot is content-addressed, and its hash joins the fingerprint of every
// stage that reads it — so a wall clock in the payload would make the same
// knowledge hash differently on every fleet, and a rerun that should have been
// free would pay for itself again. Seq orders posts; the audit log and the
// event stream carry the timing.
type Post struct {
	Seq   int    `json:"seq"`
	Agent string `json:"agent,omitempty"`
	Value any    `json:"value"`
}

// Version identifies one state of a blackboard topic: how many posts it holds
// and the content hash of that snapshot. It is the hash agents pin, and the
// hash their cache keys are built from.
type Version struct {
	Topic string
	Posts int
	Hash  string
}

// String renders the version as topic@n.
func (v Version) String() string { return fmt.Sprintf("%s@%d", v.Topic, v.Posts) }

// NewFleet provisions a fleet. The options are the fleet's, not an agent's:
// slots (WithWorkers), the shared budget (WithFleetBudget), registry, secrets,
// state directory, tools, broadcasts, and event handler are fixed here and
// shared by every agent. Close it when done.
func NewFleet(opts ...Option) (*Fleet, error) {
	cfg := Config{Workers: 8, Retry: runtime.DefaultRetry}
	for _, o := range opts {
		o(&cfg)
	}
	h, err := newHost(cfg)
	if err != nil {
		return nil, err
	}
	pool := runtime.NewPool(cfg.Workers)
	if cfg.AdmissionAging > 0 {
		pool.Aging(cfg.AdmissionAging)
	}
	f := &Fleet{
		host:    h,
		pool:    pool,
		board:   map[string][]Post{},
		started: time.Now(),
	}
	f.cond = sync.NewCond(&f.mu)
	for _, name := range slices.Sorted(maps.Keys(cfg.Topics)) {
		if err := f.declare(name); err != nil {
			h.close()
			return nil, err
		}
	}
	return f, nil
}

// Slots returns the fleet's concurrency ceiling — the number of tasks that can
// be in flight across every agent at once.
func (f *Fleet) Slots() int { return f.pool.Slots() }

// Spent returns what the fleet has spent so far, across every agent.
func (f *Fleet) Spent() core.Usage { return f.gov.Spent() }

// Close releases the fleet's shared state. Wait for the agents first; Close
// does not stop work in flight.
func (f *Fleet) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	f.mu.Unlock()
	return f.host.close()
}

// Agent is one pipeline running on a fleet, and the fleet's unit of
// scheduling: what the pool calls a program, and what a caller waits on.
type Agent struct {
	// Name is the pipeline's name, and RunID the run it was given. Both are
	// set before Go returns, so a caller can correlate an agent with the
	// events it is about to publish.
	Name  string
	RunID string

	done chan struct{}
	res  *RunResult
	err  error
}

// Wait blocks until the agent finishes and returns its result. As with Run, a
// non-nil error still comes with whatever partial results the agent produced.
func (a *Agent) Wait() (*RunResult, error) {
	<-a.done
	return a.res, a.err
}

// Done is closed when the agent finishes, for callers selecting over several.
func (a *Agent) Done() <-chan struct{} { return a.done }

// Go starts p as an agent on the fleet and returns immediately. Options
// refine this agent only — retry policy, extra egress, continue-on-error,
// batch wait, and its model registry. The fleet-wide settings (slots, budget,
// secrets, state directory, tools, broadcasts, event handler) belong to
// NewFleet and passing them here fails the agent rather than being silently
// ignored.
func (f *Fleet) Go(ctx context.Context, p *pipeline.Pipeline, opts ...Option) *Agent {
	a := &Agent{Name: p.Name, RunID: core.NewID("run"), done: make(chan struct{})}

	cfg, err := f.agentConfig(opts)
	if err != nil {
		a.err = err
		close(a.done)
		return a
	}

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		a.err = fmt.Errorf("fleet: closed")
		close(a.done)
		return a
	}
	f.agents = append(f.agents, a)
	f.mu.Unlock()

	go func() {
		defer close(a.done)
		a.res, a.err = f.host.launch(ctx, a.RunID, p, cfg, f.pool)
	}()
	return a
}

// Run starts p as an agent and waits for it. It is Go followed by Wait, for
// the agents a caller wants to sequence — the one that reads a topic the
// previous agent posted to, typically.
func (f *Fleet) Run(ctx context.Context, p *pipeline.Pipeline, opts ...Option) (*RunResult, error) {
	return f.Go(ctx, p, opts...).Wait()
}

// Wait blocks until every agent has finished and returns the first error any
// of them reported. It walks the roster by index rather than snapshotting it,
// so an agent launched while Wait is blocked is waited on too.
func (f *Fleet) Wait() error {
	var firstErr error
	for i := 0; ; i++ {
		f.mu.Lock()
		if i >= len(f.agents) {
			f.mu.Unlock()
			return firstErr
		}
		a := f.agents[i]
		f.mu.Unlock()

		<-a.done
		if firstErr == nil && a.err != nil {
			firstErr = a.err
		}
	}
}

// agentConfig merges per-agent options over the fleet's, rejecting the ones
// that describe shared machinery an agent cannot have its own copy of.
func (f *Fleet) agentConfig(opts []Option) (Config, error) {
	var probe Config
	for _, o := range opts {
		o(&probe)
	}
	shared := []struct {
		set  bool
		name string
	}{
		{probe.Workers != 0, "WithWorkers (a fleet's slots are set by NewFleet)"},
		{probe.RunBudget != core.Budget{}, "WithFleetBudget/WithRunBudget (one governor covers the fleet)"},
		{probe.Secrets != nil, "WithSecrets"},
		{probe.StateDir != "", "WithStateDir"},
		{probe.Tools != nil, "WithTools"},
		{probe.MCPServers != nil, "WithMCPServer (a fleet holds one set of connections)"},
		{probe.MCPResources != nil, "WithMCPResource"},
		{probe.Broadcasts != nil, "WithBroadcast"},
		{probe.Topics != nil, "WithTopic"},
		{probe.Findings != nil, "WithFindings (a fleet shares one commons)"},
		{probe.EventHandler != nil, "WithEventHandler"},
	}
	for _, s := range shared {
		if s.set {
			return Config{}, fmt.Errorf(
				"fleet: loom.%s is fleet-wide — pass it to NewFleet, not to an agent", s.name)
		}
	}

	cfg := f.cfg
	cfg.EgressAllow = slices.Clone(cfg.EgressAllow)
	for _, o := range opts {
		o(&cfg)
	}
	// Restore what the agent may not override, in case an option touched it.
	cfg.Workers, cfg.RunBudget = f.cfg.Workers, f.cfg.RunBudget
	cfg.Secrets, cfg.StateDir = f.cfg.Secrets, f.cfg.StateDir
	cfg.Tools, cfg.Broadcasts, cfg.Topics = f.cfg.Tools, f.cfg.Broadcasts, f.cfg.Topics
	cfg.MCPServers, cfg.MCPResources = f.cfg.MCPServers, f.cfg.MCPResources
	cfg.Findings = f.cfg.Findings
	cfg.EventHandler = f.cfg.EventHandler
	return cfg, nil
}

// --- Blackboard ---------------------------------------------------------

// Post appends value to a blackboard topic and returns the version the topic
// reached. Agents started afterwards that declare the topic with
// pipeline.WithBroadcast read exactly this snapshot.
//
// The mechanism is the broadcast mechanism, which is what makes a mutable
// shared log safe here. A topic's snapshot is serialized into the
// content-addressed store and agents carry its 64-byte hash, so:
//
//   - Posting cannot disturb a running agent. Its envelopes already name a
//     hash, and the bytes behind a hash never change; a later post writes a
//     new snapshot under a new hash and leaves the old one resolvable.
//   - The cache stays honest. The snapshot's hash is part of the fingerprint
//     of every stage that reads the topic, so an agent reading findings@3 and
//     an agent reading findings@7 have different cache keys, while a rerun
//     against findings@3 replays for free.
//   - Reads stay least-privilege. A topic on the board is not a topic an agent
//     can see: it reads only what it declared, checked against its grants and
//     audited like any other capability.
//
// The value must be JSON-serializable, which Post checks now rather than at
// the launch of whatever agent would have read it.
func (f *Fleet) Post(topic string, value any) (Version, error) {
	return f.post(topic, "", value)
}

// PostFrom is Post, attributing the entry to an agent by name.
func (f *Fleet) PostFrom(agent, topic string, value any) (Version, error) {
	return f.post(topic, agent, value)
}

func (f *Fleet) post(topic, agent string, value any) (Version, error) {
	if topic == "" {
		return Version{}, fmt.Errorf("fleet: empty topic")
	}
	if _, err := json.Marshal(value); err != nil {
		return Version{}, fmt.Errorf("fleet: topic %q: value must be JSON-serializable: %w", topic, err)
	}

	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return Version{}, fmt.Errorf("fleet: closed")
	}
	posts, known := f.board[topic]
	if !known {
		f.topics = append(f.topics, topic)
		sort.Strings(f.topics)
	}
	posts = append(posts, Post{Seq: len(posts), Agent: agent, Value: value})
	f.board[topic] = posts
	snapshot := slices.Clone(posts)

	// Registered while still holding the lock, so the topic's current hash
	// always names its longest snapshot. Two concurrent posts that appended in
	// order but registered out of order would leave the name pointing at the
	// shorter of the two, and the later post would be invisible to every agent
	// launched afterwards.
	hash, err := f.shared.Register(topic, snapshot)
	f.mu.Unlock()
	if err != nil {
		return Version{}, err
	}
	f.cond.Broadcast()

	v := Version{Topic: topic, Posts: len(snapshot), Hash: hash}
	blob, _ := json.Marshal(snapshot)
	f.bus.Publish(observe.Event{
		Type: observe.BlackboardPosted, Topic: topic, Posts: v.Posts,
		Artifact: hash, Bytes: len(blob), Pipeline: agent,
		Detail: observe.Clip(postDetail(snapshot[len(snapshot)-1])),
	})
	return v, nil
}

// Posts returns a copy of a topic's entries in the order they were posted.
func (f *Fleet) Posts(topic string) []Post {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.board[topic])
}

// Values returns just the posted values of a topic, for the common case where
// who posted and in what order is not what the caller needs.
func (f *Fleet) Values(topic string) []any {
	posts := f.Posts(topic)
	out := make([]any, 0, len(posts))
	for _, p := range posts {
		out = append(out, p.Value)
	}
	return out
}

// Topics lists the declared topics in name order.
func (f *Fleet) Topics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.topics)
}

// Await blocks until a topic holds at least n posts and returns them. It is
// the fan-in a fleet needs when several agents feed one: launch the producers,
// await the count, then launch the consumer that reads the snapshot.
func (f *Fleet) Await(ctx context.Context, topic string, n int) ([]Post, error) {
	stop := context.AfterFunc(ctx, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.cond.Broadcast()
	})
	defer stop()

	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.board[topic]) < n {
		if err := ctx.Err(); err != nil {
			return slices.Clone(f.board[topic]), err
		}
		if f.closed {
			return slices.Clone(f.board[topic]), fmt.Errorf("fleet: closed")
		}
		f.cond.Wait()
	}
	return slices.Clone(f.board[topic]), nil
}

// declare registers an empty topic so an agent can read a board nobody has
// posted to yet — an agent that runs first and finds no findings should see
// none, not fail to compile.
func (f *Fleet) declare(topic string) error {
	f.mu.Lock()
	if _, known := f.board[topic]; known {
		f.mu.Unlock()
		return nil
	}
	f.board[topic] = []Post{}
	f.topics = append(f.topics, topic)
	sort.Strings(f.topics)
	f.mu.Unlock()
	_, err := f.shared.Register(topic, []Post{})
	return err
}

func postDetail(p Post) string {
	blob, err := json.MarshalIndent(p.Value, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", p.Value)
	}
	return string(blob)
}

// Findings returns the fleet's shared research gate, or nil when none was
// configured. It is the handle for the operations that belong to the commons
// rather than to any agent: retracting a claim that turned out to be wrong, and
// asking what rested on it.
func (f *Fleet) Findings() *findings.Gate { return f.commons }

// commonsTopics summarizes the commons, or nil when there is none. With a
// shared backend configured it summarizes what every executor holds, not only
// what this one has learned — a report about a commons that stopped at the
// process boundary would be reporting the wrong thing.
func (h *host) commonsTopics() []findings.TopicStat {
	if h.commons == nil {
		if h.ledger == nil {
			return nil
		}
		return h.ledger.Topics()
	}
	ctx, cancel := context.WithTimeout(context.Background(), commonsReportTimeout)
	defer cancel()
	return h.commons.Commons(ctx)
}

// commonsReportTimeout bounds the backend query a report makes. A report is
// the one place it is always acceptable to say less rather than wait.
const commonsReportTimeout = 5 * time.Second

// Explain projects what an agent would cost on this fleet without making a
// call, with the board's current snapshots in scope — so a stage that reads a
// topic is priced against the bytes it would actually read.
func (f *Fleet) Explain(p *pipeline.Pipeline, opts ...Option) (*Projection, error) {
	f.mu.Lock()
	board := make([]Option, 0, len(f.topics))
	for _, topic := range f.topics {
		board = append(board, WithBroadcast(topic, slices.Clone(f.board[topic])))
	}
	f.mu.Unlock()

	base := []Option{WithRegistry(f.cfg.Registry), WithRunBudget(f.cfg.RunBudget), WithStreaming()}
	for name, v := range f.cfg.Broadcasts {
		base = append(base, WithBroadcast(name, v))
	}
	return Explain(p, slices.Concat(base, board, opts)...)
}

// --- Reporting ----------------------------------------------------------

// AgentReport is one agent's contribution to a fleet: its own run report plus
// the numbers only the fleet can see — how long it took end to end, how much
// slot-time it was given, and how long its tasks spent queued for one.
type AgentReport struct {
	Name    string
	RunID   string
	Report  observe.RunReport
	JCT     time.Duration // start to finish: what a caller waited
	Service time.Duration // slot-time the pool charged this agent
	Wait    time.Duration // total time this agent's tasks spent queued
	MaxWait time.Duration
	Tasks   int
	Err     error
}

// FleetReport is the aggregate view of a fleet: every agent, the pool they
// shared, and what the whole thing spent.
type FleetReport struct {
	Slots    int
	Started  time.Time
	Finished time.Time
	Agents   []AgentReport
	Pool     runtime.PoolStats
	Spent    core.Usage
	Budget   core.Budget
	Topics   int
	Posts    int
	// MCP is the fleet's connection accounting — one row per server, shared by
	// every agent, which is the point of holding them here.
	MCP []mcp.Stats
	// Findings is what the shared research layer did across the whole fleet:
	// how many questions were answered from what another agent already learned,
	// what that avoided, and what the gate itself cost to run.
	Findings findings.Stats
	// Commons summarizes the ledger by topic — what the fleet now knows, as
	// distinct from what it saved by knowing it.
	Commons []findings.TopicStat
}

// Duration is the fleet's wall-clock span.
func (r FleetReport) Duration() time.Duration {
	if r.Started.IsZero() || r.Finished.IsZero() {
		return 0
	}
	return r.Finished.Sub(r.Started)
}

// Occupancy is the share of the fleet's slot-time that was actually occupied:
// completed slot-time over slots × elapsed. It is the number that says whether
// more slots would have helped.
func (r FleetReport) Occupancy() float64 {
	span := r.Duration()
	if span <= 0 || r.Slots == 0 {
		return 0
	}
	return float64(r.Pool.Service) / float64(span) / float64(r.Slots)
}

// Report snapshots the fleet: one row per agent started so far, plus the pool
// and budget totals. Safe to call while agents are still running.
func (f *Fleet) Report() FleetReport {
	f.mu.Lock()
	agents := slices.Clone(f.agents)
	topics, posts := len(f.topics), 0
	for _, ps := range f.board {
		posts += len(ps)
	}
	started := f.started
	f.mu.Unlock()

	stats := f.pool.Stats()
	byProgram := make(map[string]runtime.ProgramStats, len(stats.Programs))
	for _, ps := range stats.Programs {
		byProgram[ps.Program] = ps
	}

	rep := FleetReport{
		Slots: f.pool.Slots(), Started: started, Pool: stats,
		Spent: f.gov.Spent(), Budget: f.cfg.RunBudget,
		Topics: topics, Posts: posts, MCP: f.mcpStats(),
		Findings: f.findingsStats(), Commons: f.commonsTopics(),
	}
	for _, a := range agents {
		ar := AgentReport{Name: a.Name, RunID: a.RunID}
		// An agent's error is written before its done channel closes, so the
		// close is the only edge that makes it safe to read. Reporting on a
		// fleet still running must not race the agents it is describing.
		select {
		case <-a.done:
			ar.Err = a.err
		default:
		}
		if tr := f.host.trace(a.RunID); tr != nil {
			ar.Report = tr.collector.Report()
			ar.JCT = ar.Report.Duration()
			for _, s := range ar.Report.Stages {
				ar.Tasks += s.Tasks
				if s.Finished.After(rep.Finished) {
					rep.Finished = s.Finished
				}
			}
		}
		ps := byProgram[a.RunID]
		ar.Service, ar.Wait, ar.MaxWait = ps.Service, ps.Wait, ps.MaxWait
		rep.Agents = append(rep.Agents, ar)
	}
	if rep.Finished.IsZero() {
		rep.Finished = time.Now()
	}
	return rep
}

// String renders the fleet as a table: one line per agent, then the totals and
// what sharing bought.
func (r FleetReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "fleet  %d agents · %d slots · %s\n",
		len(r.Agents), r.Slots, r.Duration().Round(time.Millisecond))
	fmt.Fprintf(&b, "%-20s %-14s %6s %6s %8s %10s %9s %9s %9s\n",
		"agent", "run", "stages", "tasks", "tokens", "cost($)", "service", "wait", "jct")

	var totTasks, totStages, totTokens int
	var totCost float64
	var totService, totWait time.Duration
	for _, a := range r.Agents {
		u := a.Report.Totals()
		fmt.Fprintf(&b, "%-20s %-14s %6d %6d %8d %10.4f %9s %9s %9s\n",
			clip(a.Name, 20), clip(a.RunID, 14), len(a.Report.Stages), a.Tasks,
			u.TotalTokens(), u.CostUSD,
			a.Service.Round(time.Millisecond), a.Wait.Round(time.Millisecond),
			a.JCT.Round(time.Millisecond))
		totStages += len(a.Report.Stages)
		totTasks += a.Tasks
		totTokens += u.TotalTokens()
		totCost += u.CostUSD
		totService += a.Service
		totWait += a.Wait
	}
	fmt.Fprintf(&b, "%-20s %-14s %6d %6d %8d %10.4f %9s %9s %9s\n",
		"TOTAL", "", totStages, totTasks, totTokens, totCost,
		totService.Round(time.Millisecond), totWait.Round(time.Millisecond),
		r.Duration().Round(time.Millisecond))

	fmt.Fprintf(&b, "slots %d occupied %.0f%% of %s · %d tasks admitted",
		r.Slots, 100*r.Occupancy(), r.Duration().Round(time.Millisecond), r.Pool.Admitted)
	if r.Pool.Waiting > 0 {
		fmt.Fprintf(&b, " · %d queued", r.Pool.Waiting)
	}
	b.WriteByte('\n')

	if r.Budget.MaxCostUSD > 0 {
		fmt.Fprintf(&b, "fleet budget $%.4f, spent $%.4f (%.0f%%) across every agent\n",
			r.Budget.MaxCostUSD, r.Spent.CostUSD, 100*r.Spent.CostUSD/r.Budget.MaxCostUSD)
	}
	if r.Topics > 0 {
		fmt.Fprintf(&b, "blackboard: %d topic(s), %d post(s), read by reference\n", r.Topics, r.Posts)
	}
	if r.Findings.Asked > 0 {
		b.WriteString(r.Findings.String())
		for _, t := range r.Commons {
			fmt.Fprintf(&b, "  %-24s %d live", clip(t.Topic, 24), t.Live)
			if t.Negative > 0 {
				fmt.Fprintf(&b, " (%d negative)", t.Negative)
			}
			if t.Corroborations > 0 {
				fmt.Fprintf(&b, " · %d corroboration(s)", t.Corroborations)
			}
			if t.Retracted > 0 {
				fmt.Fprintf(&b, " · %d retracted", t.Retracted)
			}
			b.WriteByte('\n')
		}
	}
	for _, m := range r.MCP {
		fmt.Fprintf(&b, "mcp %s: %d session(s) shared by every agent, %d call(s)",
			m.Server, m.Sessions, m.Calls)
		if m.Errors > 0 {
			fmt.Fprintf(&b, ", %d failed", m.Errors)
		}
		if m.Dials > m.Sessions {
			fmt.Fprintf(&b, ", %d reconnect(s)", m.Dials-m.Sessions)
		}
		fmt.Fprintf(&b, ", %s busy", m.Busy.Round(time.Millisecond))
		// Queue time is the only tuning signal a call slot has: it says the
		// ceiling, not the server, is what the agents were waiting on.
		if m.Waited > 0 {
			fmt.Fprintf(&b, ", %s queued for a slot (peak %d of %d)",
				m.Waited.Round(time.Millisecond), m.Peak, m.Slots)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

// --- Shared host --------------------------------------------------------

// host is the machinery a fleet provisions once and every agent on it borrows:
// the rate limiter that stands for one provider quota, the governor that holds
// one ceiling, the store that holds one cache, and the bus that carries one
// stream of events. A single Run is a fleet of one and is built the same way,
// so what a run and an agent share cannot drift apart.
type host struct {
	cfg     Config
	bus     *observe.Bus
	audit   *security.AuditLog
	broker  security.SecretBroker
	lineage *store.Lineage
	cas     *store.CAS
	cache   *store.Cache
	shared  *store.Broadcasts
	gov     *runtime.Governor
	limiter *runtime.RateLimiter
	client  *executor.ModelClient
	tools   *executor.ToolSet
	// mcp holds the host's connections to MCP servers, alongside the limiter
	// and the governor and for the same reason: a connection belongs to an
	// account and a server process rather than to a pipeline, so every agent
	// on this host shares one set of them and one bound on their use.
	mcp      *mcp.Catalog
	manifest mcp.Manifest
	// commons is the shared research layer, held here for the same reason the
	// cache is: what an agent has already learned about the world is a property
	// of the work, not of the pipeline that learned it. Nil when no findings
	// config was given, which leaves every tool exactly as it was.
	commons *findings.Gate
	ledger  *findings.Ledger
	// remote is the executor that puts tasks on a worker queue instead of
	// running them here. Nil unless WithWorkerService was given, which is what
	// keeps local execution the default: distribution is an adapter installed
	// at provisioning, not a mode the scheduler knows about.
	remote *worker.Client
	// state materializes the evolving contexts task envelopes reference, and
	// keeps what it has rendered so the next revision costs the change rather
	// than the context.
	//
	// It is held on the host beside the cache and the commons because it is the
	// same kind of thing: a property of the process rather than of a pipeline.
	// Every agent on this host shares one, which is what lets two pipelines
	// working the same session share the rendering rather than each keeping
	// their own copy of it.
	state *delta.Store

	mu     sync.Mutex
	traces map[string]*agentTrace
}

// agentTrace is the per-agent slice of a shared event stream: its own
// collector, and the task IDs that let a fleet-wide audit log be attributed
// back to the agent that produced each line.
type agentTrace struct {
	collector *observe.Collector

	mu    sync.Mutex
	tasks map[string]struct{}
}

func (t *agentTrace) handle(e observe.Event) {
	t.collector.Handle(e)
	if e.TaskID == "" {
		return
	}
	t.mu.Lock()
	t.tasks[e.TaskID] = struct{}{}
	t.mu.Unlock()
}

func (t *agentTrace) owns(taskID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.tasks[taskID]
	return ok
}

func newHost(cfg Config) (*host, error) {
	if cfg.Registry == nil {
		cfg.Registry = model.NewRegistry()
	}
	audit := &security.AuditLog{}
	h := &host{
		cfg:     cfg,
		bus:     observe.NewBus(),
		audit:   audit,
		broker:  security.NewStaticBroker(cfg.Secrets, audit),
		lineage: &store.Lineage{},
		gov:     runtime.NewGovernor(cfg.RunBudget),
		limiter: runtime.NewRateLimiter(),
		tools:   executor.NewToolSet(cfg.Tools...),
		traces:  map[string]*agentTrace{},
	}

	// One handler demultiplexes the shared stream into each agent's own
	// collector. A handler per agent would mean a fleet of a hundred agents
	// calling a hundred handlers on every event, and stages of the same name in
	// different pipelines folded into one row.
	h.bus.On(func(e observe.Event) {
		if tr := h.trace(e.RunID); tr != nil {
			tr.handle(e)
		}
	})
	if cfg.EventHandler != nil {
		h.bus.On(cfg.EventHandler)
	}

	casDir, cacheDir := "", ""
	if cfg.StateDir != "" {
		casDir, cacheDir = cfg.StateDir+"/cas", cfg.StateDir
	}
	cas, err := store.NewCAS(casDir)
	if err != nil {
		return nil, err
	}
	cache, err := store.NewCache(cas, cacheDir)
	if err != nil {
		return nil, err
	}
	h.cas, h.cache = cas, cache
	h.shared = store.NewBroadcasts(cas)
	h.state, err = delta.NewStore(cas, delta.Options{
		Renderer: cfg.DeltaRenderer, Policy: cfg.DeltaPolicy,
		MaxBytes: cfg.DeltaBytes, Bus: h.bus,
	})
	if err != nil {
		return nil, err
	}
	h.client = &executor.ModelClient{
		Registry: cfg.Registry, Broker: h.broker, Audit: audit, Bus: h.bus,
	}
	if cfg.Queue != nil {
		// The workers read and write through the same CAS this host holds, which
		// is why the client is built here rather than by the caller: a queue
		// client pointed at a different store would be a fleet that cannot
		// resolve its own inputs.
		h.remote = worker.NewClient(worker.ClientConfig{
			Queue: cfg.Queue, Blobs: h.cas, Bus: h.bus, Wait: cfg.QueueWait,
			Affinity: cfg.Affinity,
		})
	}

	// MCP servers are connected before anything else, because everything else
	// depends on what they answer: the tool set an executor can dispatch, the
	// manifest a plan is compiled against, and any resource registered as a
	// broadcast.
	if err := h.connectMCP(); err != nil {
		_ = cache.Close()
		return nil, err
	}

	// The research gate is provisioned after the MCP tools exist, because the
	// tools it stands in front of are usually theirs — and before anything
	// runs, because a tool that were gated halfway through a run would produce
	// a fleet whose savings depended on when the wrapping happened.
	if err := h.provisionFindings(); err != nil {
		_ = h.closeMCP()
		_ = cache.Close()
		return nil, err
	}

	// Broadcasts are stored before anything runs: from here on tasks carry
	// content hashes, not copies.
	for _, name := range slices.Sorted(maps.Keys(cfg.Broadcasts)) {
		if _, err := h.shared.Register(name, cfg.Broadcasts[name]); err != nil {
			_ = h.closeMCP()
			_ = cache.Close()
			return nil, err
		}
	}
	if err := h.readMCPResources(); err != nil {
		_ = h.closeMCP()
		_ = cache.Close()
		return nil, err
	}
	return h, nil
}

// connectMCP provisions the host's MCP connections: credentials resolved once
// through the broker, sessions dialed, tools discovered, and the resulting
// contract published as a manifest for the planner.
//
// Doing it here — at host construction, before a single task exists — is the
// whole of the design. A connection made lazily inside a task would be made
// once per task under load, would put a handshake on the critical path of a
// record, and would leave a broken server to surface as a scattering of failed
// records instead of a run that never started.
func (h *host) connectMCP() error {
	if len(h.cfg.MCPServers) == 0 {
		return nil
	}
	// Provisioning resolves exactly the credentials the configured servers
	// name, and nothing else. The grant set is built from the descriptors
	// themselves, so the broker's check is real rather than ceremonial and the
	// audit log records precisely which secrets a connection consumed.
	var caps []security.Capability
	for _, s := range h.cfg.MCPServers {
		if s.AuthSecret != "" {
			caps = append(caps, security.SecretCap(s.AuthSecret))
		}
		for _, ref := range s.EnvSecrets {
			caps = append(caps, security.SecretCap(ref))
		}
	}
	grants := security.NewGrantSet(caps...)

	cat, err := mcp.NewCatalog(mcp.Options{
		Resolve: func(ref security.SecretRef) (string, error) {
			return h.broker.Resolve("provisioning", ref, grants)
		},
		Audit: h.audit, Bus: h.bus, Slots: h.cfg.Workers,
	}, h.cfg.MCPServers...)
	if err != nil {
		return err
	}
	if err := cat.Connect(context.Background()); err != nil {
		_ = cat.Close()
		return err
	}
	h.mcp, h.manifest = cat, cat.Manifest()

	// The discovered tools join the same tool set local tools live in, under
	// qualified names ("mcp/<server>/<tool>"), so a grant on an MCP tool is an
	// ordinary capability and the executor needed no new dispatch path.
	for _, t := range cat.Tools() {
		h.tools.Add(t)
	}
	return nil
}

// readMCPResources turns each configured resource into a broadcast: read once
// here, stored by content hash, and from then on an ordinary shared value.
func (h *host) readMCPResources() error {
	for _, r := range h.cfg.MCPResources {
		if h.mcp == nil {
			return fmt.Errorf("mcp resource %q: no MCP servers registered (loom.WithMCPServer)", r.Name)
		}
		if _, taken := h.cfg.Broadcasts[r.Name]; taken {
			return fmt.Errorf("mcp resource %q: a broadcast of that name is already registered", r.Name)
		}
		val, err := h.mcp.ReadResource(context.Background(), r.Server, r.URI)
		if err != nil {
			return fmt.Errorf("mcp resource %q: %w", r.Name, err)
		}
		if _, err := h.shared.Register(r.Name, val); err != nil {
			return err
		}
	}
	return nil
}

// provisionFindings opens the host's ledger and wraps the tools the config
// named, so a call to one of them passes the commons before it reaches a
// public source.
//
// Wrapping the registered tool rather than adding a new one is what makes this
// a gate: the stage still declares the tool it always declared, the planner
// still grants exactly that name, and the executor still checks the capability
// and the egress allowlist before the guard is reached. Nothing about the
// pipeline says the commons exists, which is the property that lets it be
// turned on for an existing fleet.
func (h *host) provisionFindings() error {
	cfg := h.cfg.Findings
	if cfg == nil || !cfg.Enabled() {
		return nil
	}
	// The ledger persists beside the result cache, under the same state dir and
	// for the same reason: an append-only log whose durable form is its log.
	ledger, err := findings.NewLedger(h.cas, h.cfg.StateDir)
	if err != nil {
		return fmt.Errorf("findings: %w", err)
	}
	gate := findings.NewGate(ledger, cfg.Policy)
	gate.Bus = h.bus
	// The shared backend, when there is one. It is wired here rather than
	// inside the gate because the host owns every connection this process
	// makes and closes them together — the commons is not special.
	gate.Shared = cfg.Shared
	h.ledger, h.commons = ledger, gate

	for _, name := range cfg.Gate {
		tool, ok := h.tools.Get(name)
		if !ok {
			_ = ledger.Close()
			return fmt.Errorf("findings: tool %q is not registered "+
				"(loom.WithTools, or an MCP server's \"mcp/<server>/<tool>\")", name)
		}
		h.tools.Add(findings.Guard(gate, tool, cfg.Specs[name]))
	}
	if cfg.Recall {
		h.tools.Add(findings.Recall(gate))
	}
	return nil
}

func (h *host) closeMCP() error {
	if h.mcp == nil {
		return nil
	}
	return h.mcp.Close()
}

// findingsStats reports what the shared research layer did, or the zero value
// when none was configured.
//
// It is host-wide rather than per-agent, deliberately. A finding one agent
// learned and three others reused belongs to the fleet; attributing the saving
// to any one of them would be picking an owner for something whose whole point
// is that it has none.
func (h *host) findingsStats() findings.Stats {
	if h.commons == nil {
		return findings.Stats{}
	}
	return h.commons.Stats()
}

// mcpStats reports the host's connection accounting, or nil when no servers
// were configured.
func (h *host) mcpStats() []mcp.Stats {
	if h.mcp == nil {
		return nil
	}
	return h.mcp.Stats()
}

func (h *host) close() error {
	var ledger, shared error
	if h.ledger != nil {
		ledger = h.ledger.Close()
	}
	if h.commons != nil {
		// Draining first: the citation queue holds serves that have already
		// happened, and a retraction that cannot find them under-reports.
		shared = h.commons.Close()
	}
	err := errors.Join(h.closeMCP(), h.cache.Close(), ledger, shared)
	h.bus.Close()
	return err
}

func (h *host) trace(runID string) *agentTrace {
	if runID == "" {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.traces[runID]
}

// open registers an agent's trace before any of its events can be published.
func (h *host) open(runID string) *agentTrace {
	tr := &agentTrace{collector: observe.NewCollector(), tasks: map[string]struct{}{}}
	h.mu.Lock()
	h.traces[runID] = tr
	h.mu.Unlock()
	return tr
}

// launch compiles p and drives it as one program on this host. A non-nil pool
// makes it an agent on a shared engine; nil leaves the driver choice to the
// config, which is how Run keeps its barrier default.
func (h *host) launch(ctx context.Context, runID string, p *pipeline.Pipeline,
	cfg Config, pool *runtime.Pool) (*RunResult, error) {

	// Compiling against the current snapshot of every shared value is where an
	// agent pins the board: from here its envelopes name hashes, and later
	// posts cannot move underneath it.
	snapshot := h.shared.Hashes()
	pl, err := plan.Compile(p, cfg.Registry,
		plan.WithBroadcasts(snapshot), plan.WithContinuations(cfg.Continuations),
		plan.WithMCP(h.manifest))
	if err != nil {
		return nil, err
	}
	runners, err := ops.BuildRunners(pl)
	if err != nil {
		return nil, err
	}

	// Local is the default and the fallback both: a fleet of workers is one
	// more implementation of the same one method, chosen here and nowhere else,
	// so nothing downstream of this line can tell which it got.
	var exec executor.Executor = &executor.Local{
		Runners: runners, Client: h.client, Tools: h.tools,
		Broadcasts: h.shared, State: h.state,
		Audit: h.audit, Cache: h.cache, Lineage: h.lineage, Bus: h.bus,
	}
	if h.remote != nil {
		exec = h.remote
	}
	sched := runtime.Scheduler{
		Workers: cfg.Workers, Retry: cfg.Retry, Limiter: h.limiter,
		Governor: h.gov, Registry: cfg.Registry, Exec: exec, Bus: h.bus,
		ContinueOnError: cfg.ContinueOnError,
	}

	driverName := "barrier"
	switch {
	case pool != nil:
		driverName = "fleet"
	case cfg.Streaming:
		driverName = "streaming"
	}

	tr := h.open(runID)
	// The driver is part of what a viewer is looking at: the same pipeline
	// under streaming shows overlapping stages and shared execution slots, and
	// that is only legible if the view knows which one ran. The pipeline's name
	// rides along for the same reason: a process that runs several needs them
	// told apart by something other than a random run ID.
	h.bus.Publish(observe.Event{
		Type: observe.RunStarted, RunID: runID, Pipeline: p.Name, Kind: driverName})

	// Announce the shared values after the run header (which opens the run in
	// an observer) and before any task runs, so a viewer sees what this agent
	// agreed to share before it sees anything read it.
	//
	// Announced from the pinned snapshot rather than from the store's current
	// state: on a fleet a topic can gain a post between this agent compiling and
	// this loop running, and an event naming a hash the agent's envelopes do not
	// carry would be a lie about what it read.
	for _, name := range slices.Sorted(maps.Keys(snapshot)) {
		hash := snapshot[name]
		e := observe.Event{
			Type: observe.BroadcastRegistered, RunID: runID,
			Broadcast: name, Artifact: hash,
		}
		if blob, ok := h.cas.Get(hash); ok {
			e.Bytes = len(blob)
			e.Detail = observe.Clip(string(blob))
		}
		h.bus.Publish(e)
	}

	d := &driver{
		plan: pl, runID: runID, cfg: cfg, sched: sched, bus: h.bus, pool: pool,
		outputs: map[string][]core.Record{},
	}
	run := d.barrier
	if pool != nil || cfg.Streaming {
		run = d.stream
	}
	runErr := run(ctx)

	h.bus.Publish(observe.Event{Type: observe.RunFinished, RunID: runID, Pipeline: p.Name})

	res := &RunResult{
		RunID:        runID,
		StageOutputs: d.outputs,
		Report:       tr.collector.Report(),
		Failures:     d.failures,
		Iterations:   d.iterations,
		Lineage:      lineageOf(h.lineage.Entries(), runID),
		Audit:        auditOf(h.audit.Entries(), tr),
		Broadcasts:   snapshot,
		MCP:          h.mcpStats(),
		Findings:     h.findingsStats(),
		Spent:        h.gov.Spent(),
	}
	if term := pl.Terminal(); len(term) == 1 {
		res.Output = d.outputs[term[0]]
	}
	return res, runErr
}

// lineageOf narrows a fleet-wide lineage log to one agent's entries.
func lineageOf(all []store.LineageEntry, runID string) []store.LineageEntry {
	out := make([]store.LineageEntry, 0, len(all))
	for _, e := range all {
		if e.RunID == runID {
			out = append(out, e)
		}
	}
	return out
}

// auditOf narrows a fleet-wide audit log to one agent's decisions. Audit
// entries name a task rather than a run — a secret is resolved by the broker,
// which knows nothing of pipelines — so the agent's own task IDs are what
// attribute them.
func auditOf(all []security.AuditEntry, tr *agentTrace) []security.AuditEntry {
	out := make([]security.AuditEntry, 0, len(all))
	for _, e := range all {
		if tr.owns(e.TaskID) {
			out = append(out, e)
		}
	}
	return out
}
