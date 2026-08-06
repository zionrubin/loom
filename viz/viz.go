// Package viz serves Loom's constellation view: a live, in-browser
// visualization of a run in which every task and executor is a star-like
// node. Queued tasks sit dim and unlit; running tasks illuminate and gently
// pulse; tasks that run long grow brighter and gain a rotating activity
// ring; completed tasks flash and settle into a stable star (cache replays
// settle in a distinct hue); failures burn as a clearly-marked red cross.
// Clicking any node opens its full detail: stage, executor, model, input,
// runtime, token usage, cost, retries, event log, and errors.
//
// Two kinds of node are not tasks, and they sit in bands on either side of the
// stage clusters because that is the direction each one points. Above are the
// run's shared values, feeding down into the stages that read them. Below are
// the MCP servers, which the run reaches out to — drawn as rings whose filled
// arc is the peak calls in flight against the server's ceiling, because a task
// leases a call slot rather than a connection. Servers belong to the host
// rather than to any run, so the same one appears in every sky, carrying that
// run's own traffic.
//
// Wire it into a run with two lines:
//
//	v := viz.New()
//	url, _ := v.Start("localhost:8077")
//	res, err := loom.Run(ctx, p, loom.WithEventHandler(v.Handle), ...)
//
// A process usually runs more than one pipeline — a fan-out run and the run
// that fuses its results, a retry, an A/B — so the server keeps a *universe*
// of runs rather than only the latest. Each run.started opens a new sky and
// the previous one is retained, whole and inspectable, alongside it: the
// overview names every run in the process, and any of them can be entered,
// compared, and drilled into after it has finished. Events are routed by run
// ID, so pipelines running concurrently on one handler land in their own
// universes instead of interleaving into one unreadable sky.
//
// It serves the embedded single-file UI at /, a JSON snapshot of one run at
// /api/state (optionally ?run=…), the roster of every retained run at
// /api/runs, a live delta stream at /api/events (server-sent events), and one
// task's full detail at /api/task?id=…. It has no dependencies beyond the
// standard library.
//
// Snapshots and deltas carry only what the constellation draws. A task's
// heavy payloads — rendered prompts and responses, full record JSON — are
// fetched per node, because a viewer reads one node at a time and shipping
// every node's prompts on every event is what makes a large run unwatchable.
package viz

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/zionrubin/loom/observe"
)

//go:embed ui.html
var uiFS embed.FS

// Usage is per-node token/cost accounting (JSON-shaped for the UI).
type Usage struct {
	InputTokens  int `json:"in"`
	OutputTokens int `json:"out"`
	// CachedTokens are prompt tokens the provider served from the shared
	// prefix cache rather than reprocessing.
	CachedTokens int     `json:"cached"`
	Requests     int     `json:"req"`
	CostUSD      float64 `json:"cost"`
}

// LogEntry is one line of a node's event log.
type LogEntry struct {
	At  int64  `json:"at"` // unix milliseconds
	Msg string `json:"msg"`
}

// Call is one model invocation made by a task: the full rendered request
// and response, with its economics.
type Call struct {
	At        int64   `json:"at"` // unix milliseconds
	Model     string  `json:"model,omitempty"`
	Prompt    string  `json:"prompt,omitempty"`
	Response  string  `json:"response,omitempty"`
	Err       string  `json:"err,omitempty"`
	In        int     `json:"in"`
	Out       int     `json:"out"`
	CostUSD   float64 `json:"cost"`
	LatencyMS int64   `json:"latencyMs"`
}

// Node is the visualized state of one task.
//
// A node holds two very different kinds of data. The light fields — status,
// stage, timings, counters — are what the constellation draws, and they are
// small enough to broadcast on every change. The heavy fields (Input, Output,
// CallLog, Log) hold full rendered prompts, responses, and record JSON, and
// can run to hundreds of kilobytes per task.
//
// Only the light fields go on the wire in snapshots and deltas; see
// MarshalJSON. The heavy ones are served per task from /api/task, because a
// viewer can only read one node's prompts at a time and paying to ship every
// node's prompts on every event is what made large runs unusable.
type Node struct {
	ID        string   `json:"id"`
	Stage     string   `json:"stage"`
	Status    string   `json:"status"` // pending | running | retrying | completed | failed
	Worker    string   `json:"worker,omitempty"`
	Model     string   `json:"model,omitempty"`
	Attempts  int      `json:"attempts"`
	Retries   int      `json:"retries"`
	Records   int      `json:"records"`
	Input     string   `json:"input,omitempty"`     // full input records JSON
	Output    string   `json:"output,omitempty"`    // full output records JSON
	InputIDs  []string `json:"inputIds,omitempty"`  // lineage: records consumed
	OutputIDs []string `json:"outputIds,omitempty"` // lineage: records produced
	StartedAt int64    `json:"startedAt,omitempty"` // unix ms of first attempt
	EndedAt   int64    `json:"endedAt,omitempty"`
	LatencyMS int64    `json:"latencyMs,omitempty"` // final attempt latency
	Calls     int      `json:"calls"`               // model calls (all attempts)
	CallLog   []Call   `json:"callLog,omitempty"`   // full request/response per call
	Usage     Usage    `json:"usage"`               // accumulated across attempts
	CacheHit  bool     `json:"cacheHit,omitempty"`
	// Broadcasts names the run-level shared values this task actually read.
	Broadcasts []string `json:"broadcasts,omitempty"`
	// MCPCalls names the MCP tools this task invoked, as "server/tool". They
	// are the one kind of work a task does that costs no tokens, so without
	// this the node's own numbers would not explain its runtime.
	MCPCalls []string `json:"mcpCalls,omitempty"`
	// Round is the superstep this task belongs to, 1-based, on an iterative
	// stage (0 everywhere else). It is what lets the view draw a stage that
	// ran more than once as something other than one undifferentiated cluster.
	Round int `json:"round,omitempty"`
	// Error intentionally has no omitempty: it clears when a retry succeeds,
	// and clients that merge deltas must see the transition back to "".
	Error string     `json:"error"`
	Log   []LogEntry `json:"log,omitempty"`

	// rev increments whenever a heavy field changes. It rides along on the
	// light wire form so a client displaying one node's detail can tell
	// whether its copy is stale without refetching to find out.
	rev int
}

// MarshalJSON emits the light form of a node: everything the constellation
// needs to draw and rank it, plus the size of the detail it is not carrying.
// The heavy payloads are fetched per node from /api/task.
func (n *Node) MarshalJSON() ([]byte, error) {
	type light Node // sheds the method, so this does not recurse
	out := struct {
		light
		InputBytes  int `json:"inBytes,omitempty"`
		OutputBytes int `json:"outBytes,omitempty"`
		CallCount   int `json:"callN,omitempty"`
		LogCount    int `json:"logN,omitempty"`
		Rev         int `json:"rev,omitempty"`
	}{
		light:       light(*n),
		InputBytes:  len(n.Input),
		OutputBytes: len(n.Output),
		CallCount:   len(n.CallLog),
		LogCount:    len(n.Log),
		Rev:         n.rev,
	}
	out.Input, out.Output, out.CallLog, out.Log = "", "", nil, nil
	return json.Marshal(out)
}

// TaskDetail is the heavy half of a node, served on demand for the one task
// a viewer is actually inspecting.
type TaskDetail struct {
	ID     string     `json:"id"`
	Rev    int        `json:"rev"`
	Input  string     `json:"input,omitempty"`
	Output string     `json:"output,omitempty"`
	Calls  []Call     `json:"callLog,omitempty"`
	Log    []LogEntry `json:"log,omitempty"`
}

// StageInfo is the visualized state of one stage.
type StageInfo struct {
	ID        string `json:"id"`
	Upstream  string `json:"upstream,omitempty"`
	Kind      string `json:"kind,omitempty"`   // infer, fused, reduce_ai, …
	Detail    string `json:"detail,omitempty"` // human-readable stage spec
	Status    string `json:"status"`           // running | done
	StartedAt int64  `json:"startedAt,omitempty"`
	EndedAt   int64  `json:"endedAt,omitempty"`
	// Task tallies, maintained as tasks settle. The viewed run's counts are
	// derivable from its tasks, but the overview shows the shape of runs whose
	// tasks the client is not holding, so they are kept here too.
	Tasks  int `json:"tasks,omitempty"`
	Done   int `json:"done,omitempty"`
	Failed int `json:"failed,omitempty"`
	// Prefix-cache accounting for the stage's shared prompt head: tokens the
	// provider served from it, tokens spent establishing it, and what the
	// difference was worth. A stage is where these belong — the prefix is
	// shared by the stage's tasks, so the saving is a property of the stage
	// rather than of any one task.
	CachedTokens  int     `json:"cachedTokens,omitempty"`
	WrittenTokens int     `json:"writtenTokens,omitempty"`
	SavedUSD      float64 `json:"savedUsd,omitempty"`

	// Proj* is this stage's pre-flight projection, present when loom.Explain
	// published to the same handler as the run. It is what lets a stage be read
	// against what it was expected to cost while it is still running, rather
	// than only in hindsight.
	Proj *StageProjection `json:"proj,omitempty"`

	// Rounds is the per-superstep shape of an iterative stage (pipeline.Iterate),
	// empty for every other kind. Halt is why the loop stopped, once it has.
	//
	// A loop's output cannot be read on its own: the same records come back
	// whether it settled in three rounds or was cut off mid-argument by the
	// round cap. The rounds are that difference, and they are a list rather
	// than a total because the signal in an iterative workload is a slope —
	// a frontier that narrows is converging, one that does not is the case the
	// cap exists for.
	Rounds []RoundInfo `json:"rounds,omitempty"`
	Halt   string      `json:"halt,omitempty"`
}

// RoundInfo is one superstep of an iterative stage.
type RoundInfo struct {
	N         int     `json:"n"`                  // 1-based superstep number
	Active    int     `json:"active"`             // vertices that ran
	Messages  int     `json:"messages,omitempty"` // messages delivered into the round
	Done      int     `json:"done,omitempty"`     // vertices that completed
	Tokens    int     `json:"tokens,omitempty"`
	CostUSD   float64 `json:"costUsd,omitempty"`
	StartedAt int64   `json:"startedAt,omitempty"`
	EndedAt   int64   `json:"endedAt,omitempty"`
}

// StageProjection is one stage's forecast, published before the run by
// loom.Explain. ExpectedUSD carries an assumption about response length;
// CeilingUSD carries none, because MaxTokens is a cap the provider enforces.
type StageProjection struct {
	Calls       int     `json:"calls"`
	Records     int     `json:"records"`
	Tokens      int     `json:"tokens"`
	ExpectedUSD float64 `json:"expectedUsd"`
	CeilingUSD  float64 `json:"ceilingUsd"`
	FloorMS     int64   `json:"floorMs,omitempty"`
	// Estimated marks a stage whose record count was guessed rather than
	// computed, because a ParseJSON stage upstream invents the fields the
	// stages below it filter on. Its cost is a guess in both columns.
	Estimated bool `json:"estimated,omitempty"`
}

// ProjectionInfo is the run-level forecast: the totals a run was expected to
// hit, and the budget it was measured against.
type ProjectionInfo struct {
	Driver        string  `json:"driver,omitempty"`
	Calls         int     `json:"calls"`
	Tokens        int     `json:"tokens"`
	ExpectedUSD   float64 `json:"expectedUsd"`
	CeilingTokens int     `json:"ceilingTokens"`
	CeilingUSD    float64 `json:"ceilingUsd"`
	FloorMS       int64   `json:"floorMs,omitempty"`
	BudgetUSD     float64 `json:"budgetUsd,omitempty"`
	FitsBudget    bool    `json:"fitsBudget"`
	// Partial means at least one stage was estimated, so CeilingUSD covers
	// only the stages that could be computed and is not a cap on the run.
	Partial  bool     `json:"partial,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// BroadcastInfo is the visualized state of one run-level shared value: what
// it is (name, content hash, size, the value itself) and where it reached
// (the stages and tasks observed reading it).
type BroadcastInfo struct {
	ID      string   `json:"id"` // the broadcast's name
	Hash    string   `json:"hash,omitempty"`
	Bytes   int      `json:"bytes"`
	Preview string   `json:"preview,omitempty"` // serialized value (clipped)
	Stages  []string `json:"stages,omitempty"`  // stages observed reading it
	Readers int      `json:"readers"`           // tasks that read it
	Tasks   []string `json:"tasks,omitempty"`   // reader task IDs (capped)
}

// MCPInfo is the visualized state of one MCP server: what it is, what the
// host's connection to it is doing, and where in this run its tools were
// called.
//
// A server is not a task and not an executor. It is outside the engine — a
// process or an endpoint the run reaches out to — and it belongs to the *host*
// rather than to any run: one connection serves every agent on a fleet, made
// before the first run started and still open after the last one finishes. So
// its identity fields (transport, tools, digest, sessions, dials) are the same
// in every sky that shows it, while its call counters are that run's own.
// The inspector says so, because "why does this server appear in both runs"
// is exactly the question the architecture answers.
type MCPInfo struct {
	ID string `json:"id"` // the server name stages declare
	// Kind is the transport ("stdio" or "http") and Endpoint what it reaches:
	// a command line, or a URL.
	Kind     string `json:"kind,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
	Detail   string `json:"detail,omitempty"` // full description, for the inspector
	Digest   string `json:"digest,omitempty"` // tool-descriptor contract
	Tools    int    `json:"tools"`
	Sessions int    `json:"sessions"` // transports the host holds open
	Dials    int    `json:"dials"`    // connections made, reconnects included
	DialMS   int64  `json:"dialMs,omitempty"`

	// This run's activity. Times are microseconds, not the milliseconds the
	// rest of this file uses: a local stdio server answers in a few hundred
	// microseconds, and a panel reporting "0ms average" for six real calls
	// reads as broken rather than as fast.
	Calls   int   `json:"calls"`
	Errors  int   `json:"errors"`
	BusyUS  int64 `json:"busyUs"`
	QueueUS int64 `json:"queueUs"` // total time this run's calls waited for a slot
	// Slots is the ceiling on concurrent calls and Peak the most ever seen in
	// flight at once. The pair is the whole of what this design rations: a task
	// leases a call slot, not a connection, so occupancy against the ceiling is
	// the number that says whether the bound is the bottleneck.
	Slots int `json:"slots,omitempty"`
	Peak  int `json:"peak"`
	// LastAt is when the most recent call landed, which is what lets the view
	// show a server as live rather than merely present.
	LastAt   int64      `json:"lastAt,omitempty"`
	Stages   []string   `json:"stages,omitempty"` // stages observed calling it
	Tasks    []string   `json:"tasks,omitempty"`  // caller task IDs (capped)
	ByTool   []ToolStat `json:"byTool,omitempty"`
	Recent   []MCPCall  `json:"recent,omitempty"` // most recent calls, newest last
	LastErr  string     `json:"lastErr,omitempty"`
	Connects int        `json:"connects,omitempty"` // announcements seen
}

// ToolStat is one tool's share of a server's traffic.
type ToolStat struct {
	Name    string `json:"name"`
	Calls   int    `json:"calls"`
	Errors  int    `json:"errors"`
	TotalUS int64  `json:"totalUs"`
	MaxUS   int64  `json:"maxUs"`
}

// MCPCall is one tool invocation, kept as a short tail so the inspector can
// show what a server has actually been asked to do rather than only how often.
type MCPCall struct {
	At      int64  `json:"at"`
	Tool    string `json:"tool"`
	Stage   string `json:"stage,omitempty"`
	TaskID  string `json:"taskId,omitempty"`
	US      int64  `json:"us"`
	QueueUS int64  `json:"queueUs,omitempty"`
	Bytes   int    `json:"bytes,omitempty"`
	Err     string `json:"err,omitempty"`
}

// WorkerInfo is the visualized state of one scheduler worker (executor slot).
type WorkerInfo struct {
	ID string `json:"id"`
	// Current intentionally has no omitempty: it clears when the worker goes
	// idle, and clients that merge deltas must see the transition back to "".
	Current string `json:"current"` // task ID being executed
	Done    int    `json:"done"`
	Failed  int    `json:"failed"`
	BusyMS  int64  `json:"busyMs"`

	busyStart int64
}

type runHeader struct {
	RunID string `json:"runId"`
	// Pipeline is the name the run's pipeline was built with. It is what makes
	// a universe of runs readable: run IDs are random, pipeline names are not.
	Pipeline  string `json:"pipeline,omitempty"`
	Index     int    `json:"index,omitempty"` // 1-based order of appearance
	StartedAt int64  `json:"startedAt,omitempty"`
	EndedAt   int64  `json:"endedAt,omitempty"`
	Done      bool   `json:"done"`
	Note      string `json:"note,omitempty"`
	// Driver names the execution strategy: "barrier" runs a stage at a time,
	// "streaming" pipelines them across one shared pool of executors.
	Driver string `json:"driver,omitempty"`
}

// StageBrief is one stage reduced to what the overview draws for a run the
// viewer is not currently inside.
type StageBrief struct {
	ID     string `json:"id"`
	Kind   string `json:"kind,omitempty"`
	Status string `json:"status,omitempty"`
	Tasks  int    `json:"tasks"`
	Done   int    `json:"done"`
	Failed int    `json:"failed"`
}

// RunSummary is one run as the universe overview sees it: enough to rank,
// compare, and pick between runs without holding any of their tasks.
//
// Stages is carried only by roster views (snapshots and /api/runs). Deltas
// ride the hot path and send the counters alone.
type RunSummary struct {
	runHeader
	Tasks      int          `json:"tasks"`
	Completed  int          `json:"completed"`
	Failed     int          `json:"failed"`
	Running    int          `json:"running"`
	Pending    int          `json:"pending"`
	Retries    int          `json:"retries"`
	CacheHits  int          `json:"cacheHits"`
	StageCount int          `json:"stageCount"`
	Tokens     int          `json:"tokens"`
	CostUSD    float64      `json:"cost"`
	Projected  float64      `json:"projectedUsd,omitempty"`
	Stages     []StageBrief `json:"stages,omitempty"`
}

// Snapshot is the complete state of one run, plus the roster of every run
// the server is holding so a viewer can see where it is in the universe.
type Snapshot struct {
	runHeader
	Now int64 `json:"now"`
	// Live names the run currently receiving events, which is not necessarily
	// the run this snapshot describes — the point of keeping a universe is
	// that a finished run stays watchable while the next one runs.
	Live       string           `json:"live,omitempty"`
	Runs       []*RunSummary    `json:"runs"`
	Stages     []*StageInfo     `json:"stages"`
	Tasks      []*Node          `json:"tasks"`
	Workers    []*WorkerInfo    `json:"workers"`
	Broadcasts []*BroadcastInfo `json:"broadcasts"`
	MCP        []*MCPInfo       `json:"mcp"`
	Projection *ProjectionInfo  `json:"projection,omitempty"`
}

// delta is one incremental update: which run it belongs to, that run's header
// and summary, and whichever entities the triggering event changed. A client
// watching a different run applies the summary and drops the rest.
type delta struct {
	Now int64 `json:"now"`
	// Reset marks the delta that opened a new run. Clients following the live
	// run switch to it; clients pinned to an older one stay where they are.
	Reset bool   `json:"reset,omitempty"`
	RunID string `json:"runId,omitempty"`
	// Runs is the full roster, sent when it changes shape (a run started, an
	// old one was evicted) rather than on every event.
	Runs      []*RunSummary  `json:"runs,omitempty"`
	Live      string         `json:"live,omitempty"`
	Run       runHeader      `json:"run"`
	Summary   *RunSummary    `json:"summary,omitempty"`
	Stage     *StageInfo     `json:"stage,omitempty"`
	Task      *Node          `json:"task,omitempty"`
	Worker    *WorkerInfo    `json:"worker,omitempty"`
	Broadcast *BroadcastInfo `json:"broadcast,omitempty"`
	// MCP carries one server's state. It is the only entity in a delta that
	// can arrive with no run ID: a connection is the host's, made before any
	// run exists, so a client applies it to whatever sky it is showing.
	MCP        *MCPInfo        `json:"mcp,omitempty"`
	Projection *ProjectionInfo `json:"projection,omitempty"`
}

const (
	maxLogEntries  = 80
	maxCallEntries = 24
	// Reader lists are for showing *where* a value landed, not for
	// enumerating every task; Readers stays exact regardless.
	maxBroadcastTasks = 24
	// An MCP server's caller list and call tail are for showing what a server
	// has been asked to do, not for enumerating every call; Calls and ByTool
	// stay exact regardless.
	maxMCPTasks = 24
	maxMCPCalls = 32
	// defaultRetainedRuns bounds the universe. Runs are held whole — every
	// task, prompt, and response — so the history is finite by construction:
	// the oldest run is dropped when a new one pushes past the limit.
	defaultRetainedRuns = 12
)

type subscriber struct {
	ch   chan []byte
	lost bool // a send was dropped; the writer heals with a full snapshot
}

// runState is one run's whole sky: its stages, tasks, executors, shared
// values, and the forecast it was measured against. A universe is a list of
// these, and a run keeps its own indexes so the runs never bleed into one
// another — the same task ID in two runs is two different stars.
type runState struct {
	hdr      runHeader
	stages   []*StageInfo
	stageIx  map[string]*StageInfo
	tasks    []*Node
	taskIx   map[string]*Node
	workers  []*WorkerInfo
	workIx   map[string]*WorkerInfo
	shared   []*BroadcastInfo
	sharedIx map[string]*BroadcastInfo
	// servers is this run's view of the host's MCP connections: the same
	// identity in every sky, this run's own call counters.
	servers []*MCPInfo
	serveIx map[string]*MCPInfo

	// round is the superstep each iterative stage is currently in, so the
	// tasks it schedules can be attributed to it.
	//
	// A round is a barrier — every task of superstep N is built, submitted and
	// settled between that stage's round.started and round.finished — so the
	// stage's current round is the round of any task it schedules. Keyed by
	// stage rather than held as one value because a streaming run can have two
	// iterative stages in flight at once.
	round map[string]int

	// The forecast this run was measured against (loom.Explain), and the
	// run-level half of it. Held per run so a second pipeline's prediction
	// never lands on the first one's stages.
	fc   *forecast
	proj *ProjectionInfo

	// Totals, kept incrementally. The overview shows every run at once, and
	// re-tallying each run's tasks on every event is how a view that watches
	// several runs stops keeping up with any of them.
	byStatus  map[string]int
	retries   int
	cacheHits int
	tokens    int
	costUSD   float64
}

func newRunState(hdr runHeader) *runState {
	return &runState{
		hdr:      hdr,
		stageIx:  map[string]*StageInfo{},
		taskIx:   map[string]*Node{},
		workIx:   map[string]*WorkerInfo{},
		sharedIx: map[string]*BroadcastInfo{},
		serveIx:  map[string]*MCPInfo{},
		byStatus: map[string]int{},
		round:    map[string]int{},
	}
}

// forecast is one pipeline's pre-flight projection, held by pipeline name so
// the run it predicted can claim it whenever it starts.
type forecast struct {
	run    *ProjectionInfo
	stages map[string]*StageProjection
}

// Server folds observe events into a universe of runs and serves the
// constellation UI.
type Server struct {
	mu    sync.Mutex
	runs  []*runState // oldest first
	runIx map[string]*runState
	cur   *runState // most recently started run
	seq   int       // runs seen, ever — the source of Index
	// retain bounds how many runs are held at once; see Retain.
	retain int
	subs   map[*subscriber]struct{}

	// Projections outlive the runs they describe. A forecast is published
	// before its run starts (loom.Explain), and the run that follows is
	// precisely the thing it predicted, so it is held by pipeline name and
	// handed to that pipeline's runs as they open — including a re-run, which
	// is the same prediction a second time.
	forecasts map[string]*forecast

	// servers is the host's MCP connections, held outside the universe
	// because that is where they actually live: connected before the first
	// run, shared by every agent, still open after the last one finishes. Each
	// new run's sky is seeded from this, so a server looks the same in every
	// sky and accumulates its own call counters in each.
	servers    []*MCPInfo
	serverIx   map[string]*MCPInfo
	viewer     chan struct{}
	viewerOnce sync.Once

	mux  *http.ServeMux
	http *http.Server
}

// Option configures a Server.
type Option func(*Server)

// Retain bounds the number of runs the universe holds at once (default 12).
// Runs are kept whole — every task, prompt, and response — so this is the
// knob that trades history for memory; the oldest run is dropped when a new
// one pushes past the limit. Values below 1 are ignored.
func Retain(n int) Option {
	return func(s *Server) {
		if n >= 1 {
			s.retain = n
		}
	}
}

// New returns a Server with an empty universe.
func New(opts ...Option) *Server {
	s := &Server{
		runIx:     map[string]*runState{},
		forecasts: map[string]*forecast{},
		serverIx:  map[string]*MCPInfo{},
		retain:    defaultRetainedRuns,
		subs:      map[*subscriber]struct{}{},
		viewer:    make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.serveUI)
	mux.HandleFunc("/api/state", s.serveState)
	mux.HandleFunc("/api/runs", s.serveRuns)
	mux.HandleFunc("/api/events", s.serveEvents)
	mux.HandleFunc("/api/task", s.serveTask)
	s.mux = mux
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Start listens on addr (e.g. "localhost:8077", or ":0" for an ephemeral
// port) and serves in a background goroutine, returning the URL to open.
func (s *Server) Start(addr string) (string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.http = &http.Server{Handler: s}
	srv := s.http
	s.mu.Unlock()
	go func() { _ = srv.Serve(ln) }()

	host, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return "http://" + ln.Addr().String(), nil
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port), nil
}

// Close stops the HTTP server started by Start.
func (s *Server) Close() error {
	s.mu.Lock()
	srv := s.http
	s.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Close()
}

// AwaitViewer blocks until a browser connects to the event stream (or ctx
// ends), reporting whether a viewer arrived. Useful in examples: print the
// URL, wait for the user to open it, then start the run.
func (s *Server) AwaitViewer(ctx context.Context) bool {
	select {
	case <-s.viewer:
		return true
	case <-ctx.Done():
		return false
	}
}

// Handle consumes one observe event; attach with loom.WithEventHandler(v.Handle).
func (s *Server) Handle(e observe.Event) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	now := e.Time.UnixMilli()

	s.mu.Lock()
	defer s.mu.Unlock()

	d := delta{Now: now}
	// Every event but a projection belongs to exactly one run, and routing by
	// run ID is what keeps two pipelines on one handler in two skies.
	var r *runState
	switch e.Type {
	case observe.StageProjected, observe.RunProjected:
		// Resolved below: a forecast belongs to a pipeline, not to a run.
	case observe.MCPConnected:
		// Nor does a connection. It is made before any run starts and outlives
		// every one of them, so it must not conjure a sky to live in — the
		// empty universe should show the servers it is about to use, not a
		// phantom run that never happened.
	case observe.RunStarted:
		// A new sky. The roster rides along because this is the one event
		// that changes its shape — a run appears, and the oldest may retire.
		r = s.startRunLocked(e, now)
		d.Reset = true
		d.Runs = s.rosterLocked()
		d.Projection = r.proj
	default:
		r = s.runForLocked(e.RunID, now)
	}

	switch e.Type {
	case observe.RunFinished:
		r.hdr.Done = true
		r.hdr.EndedAt = now

	case observe.StageProjected:
		p := &StageProjection{
			Calls: e.Usage.Requests, Records: e.Records,
			Tokens: e.Usage.TotalTokens(), ExpectedUSD: e.Usage.CostUSD,
			CeilingUSD: e.Ceiling.CostUSD, FloorMS: e.Latency.Milliseconds(),
			Estimated: e.Note == "estimated",
		}
		fc := s.forecastLocked(e.Pipeline)
		fc.stages[e.Stage] = p
		// A projection usually arrives before its run, in which case it waits
		// for it. When the run is already open it attaches now.
		if r = s.forecastRunLocked(e.Pipeline); r != nil {
			st := r.stageLocked(e.Stage)
			st.Proj = p
			// Seed what the forecast knows; stage.started overwrites it with
			// the real thing.
			if st.Kind == "" {
				st.Kind = e.Kind
			}
			d.Stage = st
		}

	case observe.RunProjected:
		pi := &ProjectionInfo{
			Driver: e.Kind, Calls: e.Usage.Requests,
			Tokens: e.Usage.TotalTokens(), ExpectedUSD: e.Usage.CostUSD,
			CeilingTokens: e.Ceiling.TotalTokens(), CeilingUSD: e.Ceiling.CostUSD,
			FloorMS:   e.Latency.Milliseconds(),
			BudgetUSD: e.Budget.MaxCostUSD,
			Partial:   e.Note == "partial",
			FitsBudget: (e.Budget.MaxCostUSD == 0 || e.Ceiling.CostUSD <= e.Budget.MaxCostUSD) &&
				(e.Budget.MaxTokens == 0 || e.Ceiling.TotalTokens() <= e.Budget.MaxTokens),
		}
		if e.Detail != "" {
			pi.Warnings = strings.Split(e.Detail, "\n")
		}
		s.forecastLocked(e.Pipeline).run = pi
		if r = s.forecastRunLocked(e.Pipeline); r != nil {
			r.proj = pi
		}
		// Sent whether or not a run has claimed it: before the first run there
		// is nothing else to show, and the price is the one thing still
		// actionable at that moment.
		d.Projection = pi

	case observe.StageStarted:
		st := r.stageLocked(e.Stage)
		st.Upstream = e.Upstream
		st.Kind = e.Kind
		st.Detail = e.Detail
		st.Status = "running"
		st.StartedAt = now
		d.Stage = st

	case observe.StageFinished:
		st := r.stageLocked(e.Stage)
		st.Status = "done"
		st.EndedAt = now
		delete(r.round, e.Stage)
		d.Stage = st

	case observe.RoundStarted:
		st := r.stageLocked(e.Stage)
		r.round[e.Stage] = e.Round
		st.Rounds = append(st.Rounds, RoundInfo{
			N: e.Round, Active: e.Records, Messages: e.Messages, StartedAt: now,
		})
		d.Stage = st

	case observe.RoundFinished:
		st := r.stageLocked(e.Stage)
		if ri := roundOf(st, e.Round); ri != nil {
			ri.Done = e.Records
			ri.EndedAt = now
			ri.Tokens = e.Usage.TotalTokens()
			ri.CostUSD = e.Usage.CostUSD
		}
		d.Stage = st

	case observe.StageConverged:
		// Why the loop stopped. Converging and running out of budget return
		// the same records, so this is the only thing that tells them apart —
		// which makes it the one field of an iterative stage a viewer must not
		// have to infer.
		st := r.stageLocked(e.Stage)
		st.Halt = e.Note
		delete(r.round, e.Stage)
		d.Stage = st

	case observe.TaskScheduled:
		n := r.nodeLocked(e.TaskID, e.Stage)
		n.Records = e.Records
		n.Input = e.Input
		n.InputIDs = e.InputIDs
		n.Round = r.round[e.Stage]
		if n.Round > 0 {
			logf(n, now, "scheduled in round %d (%d vertex%s)", n.Round,
				e.Records, map[bool]string{true: "", false: "es"}[e.Records == 1])
		} else {
			logf(n, now, "scheduled (%d record%s)", e.Records, plural(e.Records))
		}
		d.Task = n

	case observe.TaskStarted:
		n := r.nodeLocked(e.TaskID, e.Stage)
		r.setStatus(n, "running")
		n.Attempts = e.Attempt
		n.Worker = e.Worker
		if e.Model != "" {
			n.Model = e.Model
		}
		if n.StartedAt == 0 {
			n.StartedAt = now
		}
		logf(n, now, "attempt %d started on %s (%s)", e.Attempt, e.Worker, orDash(e.Model))
		d.Task = n
		d.Worker = r.workerStartLocked(e.Worker, e.TaskID, now)

	case observe.ModelCalled:
		n := r.nodeLocked(e.TaskID, e.Stage)
		n.Calls++
		n.CallLog = append(n.CallLog, Call{
			At: now, Model: e.Model, Prompt: e.Prompt, Response: e.Response,
			Err: e.Err, In: e.Usage.InputTokens, Out: e.Usage.OutputTokens,
			CostUSD: e.Usage.CostUSD, LatencyMS: e.Latency.Milliseconds(),
		})
		if len(n.CallLog) > maxCallEntries {
			n.CallLog = n.CallLog[len(n.CallLog)-maxCallEntries:]
		}
		if e.Err != "" {
			logf(n, now, "model call failed on %s after %s: %s",
				orDash(e.Model), e.Latency.Round(time.Millisecond), e.Err)
		} else {
			n.Usage.InputTokens += e.Usage.InputTokens
			n.Usage.OutputTokens += e.Usage.OutputTokens
			n.Usage.CachedTokens += e.Usage.CacheReadTokens
			n.Usage.Requests += e.Usage.Requests
			n.Usage.CostUSD += e.Usage.CostUSD
			r.tokens += e.Usage.InputTokens + e.Usage.OutputTokens
			r.costUSD += e.Usage.CostUSD
			shared := ""
			if e.Usage.CacheReadTokens > 0 {
				shared = fmt.Sprintf(" (+%d from shared prefix)", e.Usage.CacheReadTokens)
			} else if e.Usage.CacheWriteTokens > 0 {
				shared = fmt.Sprintf(" (+%d written to shared prefix)", e.Usage.CacheWriteTokens)
			}
			logf(n, now, "model call %s: %d in%s / %d out tokens, $%.5f, %s",
				orDash(e.Model), e.Usage.InputTokens, shared, e.Usage.OutputTokens,
				e.Usage.CostUSD, e.Latency.Round(time.Millisecond))
			// Roll the prefix-cache economics up to the stage that owns the
			// shared prompt head.
			if e.Stage != "" && (e.Usage.CacheReadTokens > 0 || e.Usage.CacheWriteTokens > 0) {
				st := r.stageLocked(e.Stage)
				st.CachedTokens += e.Usage.CacheReadTokens
				st.WrittenTokens += e.Usage.CacheWriteTokens
				st.SavedUSD += e.Saved
				d.Stage = st
			}
		}
		d.Task = n

	case observe.CacheHit:
		n := r.nodeLocked(e.TaskID, e.Stage)
		if !n.CacheHit {
			n.CacheHit = true
			r.cacheHits++
		}
		logf(n, now, "cache hit — result replayed, zero model calls")
		d.Task = n

	case observe.TaskRetried:
		n := r.nodeLocked(e.TaskID, e.Stage)
		r.setStatus(n, "retrying")
		n.Retries++
		r.retries++
		n.Attempts = e.Attempt
		n.Error = e.Err
		logf(n, now, "attempt %d failed: %s → retrying (%s)", e.Attempt, e.Err, e.Note)
		d.Task = n

	case observe.TaskCompleted:
		n := r.nodeLocked(e.TaskID, e.Stage)
		r.setStatus(n, "completed")
		n.Attempts = e.Attempt
		if e.Model != "" {
			n.Model = e.Model
		}
		n.EndedAt = now
		n.LatencyMS = e.Latency.Milliseconds()
		n.Output = e.Output
		n.OutputIDs = e.OutIDs
		n.Error = ""
		logf(n, now, "completed in %s (attempt %d)", e.Latency.Round(time.Millisecond), e.Attempt)
		d.Task = n
		d.Worker = r.workerEndLocked(e.Worker, e.TaskID, now, false)

	case observe.TaskFailed:
		n := r.nodeLocked(e.TaskID, e.Stage)
		r.setStatus(n, "failed")
		n.Attempts = e.Attempt
		n.EndedAt = now
		n.Error = e.Err
		logf(n, now, "failed after %d attempt%s: %s", e.Attempt, plural(e.Attempt), e.Err)
		d.Task = n
		d.Worker = r.workerEndLocked(e.Worker, e.TaskID, now, true)

	case observe.BroadcastRegistered:
		bc := r.sharedLocked(e.Broadcast)
		bc.Hash = e.Artifact
		bc.Bytes = e.Bytes
		bc.Preview = e.Detail
		d.Broadcast = bc

	case observe.BroadcastRead:
		bc := r.sharedLocked(e.Broadcast)
		bc.Readers++
		if e.Stage != "" && !contains(bc.Stages, e.Stage) {
			bc.Stages = append(bc.Stages, e.Stage)
		}
		if len(bc.Tasks) < maxBroadcastTasks {
			bc.Tasks = append(bc.Tasks, e.TaskID)
		}
		n := r.nodeLocked(e.TaskID, e.Stage)
		if !contains(n.Broadcasts, e.Broadcast) {
			n.Broadcasts = append(n.Broadcasts, e.Broadcast)
		}
		logf(n, now, "read shared value %q (%s) — referenced, not copied",
			e.Broadcast, shortHash(e.Artifact))
		d.Task = n
		d.Broadcast = bc

	case observe.MCPConnected:
		host := s.hostServerLocked(e.Server)
		host.Kind, host.Endpoint, host.Detail = e.Kind, e.Note, e.Detail
		host.Digest, host.Tools, host.Slots = e.Artifact, e.Records, e.Slots
		host.DialMS = e.Latency.Milliseconds()
		host.Connects++
		host.Sessions, host.Dials = 1, host.Connects
		// A run already open gains the server too: a reconnect mid-run is
		// exactly the moment a viewer wants to see it.
		if s.cur != nil {
			d.MCP = s.cur.adoptServerLocked(host)
			r = s.cur
		} else {
			cp := *host
			d.MCP = &cp
		}

	case observe.MCPCalled:
		m := r.serverLocked(e.Server)
		if seed := s.serverIx[e.Server]; seed != nil && m.Tools == 0 {
			m.Kind, m.Endpoint, m.Detail = seed.Kind, seed.Endpoint, seed.Detail
			m.Digest, m.Tools, m.Sessions, m.Dials = seed.Digest, seed.Tools, seed.Sessions, seed.Dials
		}
		m.Calls++
		m.LastAt = now
		m.BusyUS += e.Latency.Microseconds()
		m.QueueUS += e.Queued.Microseconds()
		if e.Slots > 0 {
			m.Slots = e.Slots
		}
		if e.InFlight > m.Peak {
			m.Peak = e.InFlight
		}
		if e.Stage != "" && !contains(m.Stages, e.Stage) {
			m.Stages = append(m.Stages, e.Stage)
		}
		if e.TaskID != "" && len(m.Tasks) < maxMCPTasks && !contains(m.Tasks, e.TaskID) {
			m.Tasks = append(m.Tasks, e.TaskID)
		}
		m.tally(e.Tool, e.Latency.Microseconds(), e.Err != "")
		m.Recent = append(m.Recent, MCPCall{
			At: now, Tool: e.Tool, Stage: e.Stage, TaskID: e.TaskID,
			US: e.Latency.Microseconds(), QueueUS: e.Queued.Microseconds(),
			Bytes: e.Bytes, Err: e.Err,
		})
		if len(m.Recent) > maxMCPCalls {
			m.Recent = m.Recent[len(m.Recent)-maxMCPCalls:]
		}
		if e.Err != "" {
			m.Errors++
			m.LastErr = e.Err
		}

		// The call also belongs to the task that made it: a star's log is
		// where "why did this task take four seconds" gets answered, and a
		// tool call is the one kind of work that costs no tokens and so leaves
		// no trace in the cost column.
		if e.TaskID != "" {
			n := r.nodeLocked(e.TaskID, e.Stage)
			qualified := e.Server + "/" + e.Tool
			if !contains(n.MCPCalls, qualified) {
				n.MCPCalls = append(n.MCPCalls, qualified)
			}
			if e.Err != "" {
				logf(n, now, "mcp %s failed after %s: %s",
					qualified, e.Latency.Round(time.Millisecond), e.Err)
			} else {
				logf(n, now, "mcp %s: %s%s, %d byte%s back (no tokens, no cost)",
					qualified, e.Latency.Round(time.Millisecond),
					queuedNote(e.Queued), e.Bytes, plural(e.Bytes))
			}
			d.Task = n
		}
		d.MCP = m

	case observe.BudgetExceeded:
		r.hdr.Note = "budget exceeded: " + e.Note
		if e.TaskID != "" {
			n := r.nodeLocked(e.TaskID, e.Stage)
			logf(n, now, "budget exceeded: %s", e.Note)
			d.Task = n
		}
	}

	if r != nil {
		d.RunID = r.hdr.RunID
		d.Run = r.hdr
		d.Summary = s.summaryLocked(r, false)
	}
	if s.cur != nil {
		d.Live = s.cur.hdr.RunID
	}
	s.broadcastLocked(d)
}

// startRunLocked opens a new sky for a run, retiring the oldest one if the
// universe is full. The run inherits its pipeline's forecast, because the run
// starting is exactly when that comparison becomes worth something.
func (s *Server) startRunLocked(e observe.Event, now int64) *runState {
	s.seq++
	r := newRunState(runHeader{
		RunID: e.RunID, Pipeline: e.Pipeline, Index: s.seq,
		StartedAt: now, Driver: e.Kind,
	})
	if fc := s.matchForecastLocked(e.Pipeline); fc != nil {
		r.fc = fc
		r.proj = fc.run
	}
	// The host's connections predate this run and will outlive it, so the sky
	// opens already knowing which servers it can reach.
	r.seedServersLocked(s.servers)
	// A run ID repeated (a reconnected producer, a hand-fed stream) replaces
	// the run it names rather than shadowing it in the index.
	if old, ok := s.runIx[e.RunID]; ok && e.RunID != "" {
		s.dropLocked(old)
	}
	s.runs = append(s.runs, r)
	if r.hdr.RunID != "" {
		s.runIx[r.hdr.RunID] = r
	}
	s.cur = r
	// Oldest first, and never the run still receiving events — it is the one
	// thing a viewer cannot recover by looking somewhere else.
	for len(s.runs) > s.retain && s.runs[0] != s.cur {
		s.dropLocked(s.runs[0])
	}
	return r
}

// dropLocked forgets a run entirely.
func (s *Server) dropLocked(victim *runState) {
	if victim == nil {
		return
	}
	for i, r := range s.runs {
		if r == victim {
			s.runs = append(s.runs[:i], s.runs[i+1:]...)
			break
		}
	}
	if victim.hdr.RunID != "" && s.runIx[victim.hdr.RunID] == victim {
		delete(s.runIx, victim.hdr.RunID)
	}
}

// runForLocked returns the run an event belongs to: the one it names, the
// current run when it names none, and a fresh universe for a run whose start
// was never seen — a handler attached mid-run, or a producer that began
// before the view did.
func (s *Server) runForLocked(id string, now int64) *runState {
	if id != "" {
		if r, ok := s.runIx[id]; ok {
			return r
		}
		// A run whose events arrive before its run.started (or without one)
		// adopts the unnamed placeholder rather than splitting in two.
		if s.cur != nil && s.cur.hdr.RunID == "" {
			s.cur.hdr.RunID = id
			s.runIx[id] = s.cur
			return s.cur
		}
		return s.startRunLocked(observe.Event{RunID: id}, now)
	}
	if s.cur == nil {
		return s.startRunLocked(observe.Event{}, now)
	}
	return s.cur
}

// forecastLocked returns the forecast slot for a pipeline name, creating it.
func (s *Server) forecastLocked(pipeline string) *forecast {
	fc, ok := s.forecasts[pipeline]
	if !ok {
		fc = &forecast{stages: map[string]*StageProjection{}}
		s.forecasts[pipeline] = fc
	}
	return fc
}

// matchForecastLocked finds the forecast a starting run should claim: the one
// published for its pipeline, or an unnamed one (a projection from a producer
// that predates pipeline names, which can only mean this run).
func (s *Server) matchForecastLocked(pipeline string) *forecast {
	if fc, ok := s.forecasts[pipeline]; ok {
		return fc
	}
	if fc, ok := s.forecasts[""]; ok {
		return fc
	}
	return nil
}

// pendingForecastLocked returns the forecast to show while the universe is
// still empty: the only one published, or the unnamed one when several are.
func (s *Server) pendingForecastLocked() *ProjectionInfo {
	if len(s.forecasts) == 1 {
		for _, fc := range s.forecasts {
			return fc.run
		}
	}
	if fc, ok := s.forecasts[""]; ok {
		return fc.run
	}
	return nil
}

// forecastRunLocked returns the open run a just-published projection describes,
// if any. Projections normally precede their run, in which case there is none
// and the forecast simply waits.
func (s *Server) forecastRunLocked(pipeline string) *runState {
	if s.cur == nil {
		return nil
	}
	if s.cur.hdr.Pipeline == pipeline || pipeline == "" || s.cur.hdr.Pipeline == "" {
		return s.cur
	}
	return nil
}

// setStatus moves a node between states, keeping the run's tallies true. The
// overview reads those tallies for runs whose tasks nobody is holding.
func (r *runState) setStatus(n *Node, status string) {
	if n.Status == status {
		return
	}
	r.byStatus[n.Status]--
	n.Status = status
	r.byStatus[status]++
	if st := r.stageIx[n.Stage]; st != nil {
		switch status {
		case "completed":
			st.Done++
		case "failed":
			st.Failed++
		}
	}
}

// sharedLocked returns the entry for a broadcast name, creating it if a read
// arrives before (or without) its registration event.
func (r *runState) sharedLocked(name string) *BroadcastInfo {
	if bc, ok := r.sharedIx[name]; ok {
		return bc
	}
	bc := &BroadcastInfo{ID: name}
	r.sharedIx[name] = bc
	r.shared = append(r.shared, bc)
	return bc
}

// hostServerLocked returns the host-level entry for an MCP server, creating it
// on first announcement.
func (s *Server) hostServerLocked(name string) *MCPInfo {
	if m, ok := s.serverIx[name]; ok {
		return m
	}
	m := &MCPInfo{ID: name}
	s.serverIx[name] = m
	s.servers = append(s.servers, m)
	return m
}

// serverLocked returns this run's entry for a server, creating it if a call
// arrives before (or without) the connection announcement.
func (r *runState) serverLocked(name string) *MCPInfo {
	if m, ok := r.serveIx[name]; ok {
		return m
	}
	m := &MCPInfo{ID: name}
	r.serveIx[name] = m
	r.servers = append(r.servers, m)
	return m
}

// adoptServerLocked copies the host's view of a server's identity into this
// run, leaving the run's own call counters alone. Identity is the host's —
// one connection, however many runs look at it — and traffic is the run's.
func (r *runState) adoptServerLocked(host *MCPInfo) *MCPInfo {
	m := r.serverLocked(host.ID)
	m.Kind, m.Endpoint, m.Detail = host.Kind, host.Endpoint, host.Detail
	m.Digest, m.Tools, m.DialMS = host.Digest, host.Tools, host.DialMS
	m.Sessions, m.Dials, m.Connects = host.Sessions, host.Dials, host.Connects
	if host.Slots > 0 {
		m.Slots = host.Slots
	}
	return m
}

// seedServersLocked gives a new run the host's connections, so a sky opens
// showing the servers it may reach rather than discovering them one call at a
// time.
func (r *runState) seedServersLocked(hosts []*MCPInfo) {
	for _, h := range hosts {
		r.adoptServerLocked(h)
	}
}

// tally folds one call into a server's per-tool breakdown. Which tool is being
// called is usually the answer to "why is this server slow", and a single
// aggregate for the whole server hides it.
func (m *MCPInfo) tally(tool string, us int64, failed bool) {
	for i := range m.ByTool {
		if m.ByTool[i].Name != tool {
			continue
		}
		m.ByTool[i].Calls++
		m.ByTool[i].TotalUS += us
		if us > m.ByTool[i].MaxUS {
			m.ByTool[i].MaxUS = us
		}
		if failed {
			m.ByTool[i].Errors++
		}
		return
	}
	st := ToolStat{Name: tool, Calls: 1, TotalUS: us, MaxUS: us}
	if failed {
		st.Errors = 1
	}
	m.ByTool = append(m.ByTool, st)
}

// queuedNote renders slot-wait time only when there was any, so the common
// case — a slot was free — says nothing rather than saying "0s".
func queuedNote(d time.Duration) string {
	if d < time.Millisecond {
		return ""
	}
	return fmt.Sprintf(" after %s queued for a slot", d.Round(time.Millisecond))
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// shortHash abbreviates a content hash for display.
func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12] + "…"
	}
	if h == "" {
		return "—"
	}
	return h
}

func (r *runState) stageLocked(id string) *StageInfo {
	if st, ok := r.stageIx[id]; ok {
		return st
	}
	// A re-run of the same pipeline is the same prediction a second time, so
	// the stage claims its forecast as it appears.
	st := &StageInfo{ID: id}
	if r.fc != nil {
		st.Proj = r.fc.stages[id]
	}
	r.stageIx[id] = st
	r.stages = append(r.stages, st)
	return st
}

// roundOf finds a stage's record of one superstep.
func roundOf(st *StageInfo, n int) *RoundInfo {
	for i := range st.Rounds {
		if st.Rounds[i].N == n {
			return &st.Rounds[i]
		}
	}
	return nil
}

func (r *runState) nodeLocked(id, stage string) *Node {
	if n, ok := r.taskIx[id]; ok {
		return n
	}
	n := &Node{ID: id, Stage: stage, Status: "pending"}
	r.taskIx[id] = n
	r.tasks = append(r.tasks, n)
	r.byStatus["pending"]++
	r.stageLocked(stage).Tasks++
	return n
}

func (r *runState) workerStartLocked(id, taskID string, now int64) *WorkerInfo {
	if id == "" {
		return nil
	}
	w, ok := r.workIx[id]
	if !ok {
		w = &WorkerInfo{ID: id}
		r.workIx[id] = w
		r.workers = append(r.workers, w)
	}
	w.Current = taskID
	if w.busyStart == 0 {
		w.busyStart = now
	}
	return w
}

func (r *runState) workerEndLocked(id, taskID string, now int64, failed bool) *WorkerInfo {
	if id == "" {
		return nil
	}
	w, ok := r.workIx[id]
	if !ok {
		return nil
	}
	if w.Current == taskID {
		w.Current = ""
	}
	if w.busyStart > 0 {
		w.BusyMS += now - w.busyStart
		w.busyStart = 0
	}
	if failed {
		w.Failed++
	} else {
		w.Done++
	}
	return w
}

func logf(n *Node, at int64, format string, args ...any) {
	n.Log = append(n.Log, LogEntry{At: at, Msg: fmt.Sprintf(format, args...)})
	if len(n.Log) > maxLogEntries {
		n.Log = n.Log[len(n.Log)-maxLogEntries:]
	}
	n.rev++
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// summaryLocked reduces a run to the line the overview shows for it. Stage
// briefs are for roster views only: deltas ride the hot path and send the
// counters alone.
func (s *Server) summaryLocked(r *runState, withStages bool) *RunSummary {
	sum := &RunSummary{
		runHeader:  r.hdr,
		Tasks:      len(r.tasks),
		Completed:  r.byStatus["completed"],
		Failed:     r.byStatus["failed"],
		Running:    r.byStatus["running"] + r.byStatus["retrying"],
		Pending:    r.byStatus["pending"],
		Retries:    r.retries,
		CacheHits:  r.cacheHits,
		StageCount: len(r.stages),
		Tokens:     r.tokens,
		CostUSD:    r.costUSD,
	}
	if r.proj != nil {
		sum.Projected = r.proj.ExpectedUSD
	}
	if withStages {
		sum.Stages = make([]StageBrief, 0, len(r.stages))
		for _, st := range r.stages {
			sum.Stages = append(sum.Stages, StageBrief{
				ID: st.ID, Kind: st.Kind, Status: st.Status,
				Tasks: st.Tasks, Done: st.Done, Failed: st.Failed,
			})
		}
	}
	return sum
}

// rosterLocked is the universe: every run held, oldest first, each with the
// shape of its pipeline attached.
func (s *Server) rosterLocked() []*RunSummary {
	out := make([]*RunSummary, 0, len(s.runs))
	for _, r := range s.runs {
		out = append(out, s.summaryLocked(r, true))
	}
	return out
}

// snapshotLocked serializes one run plus the roster it sits in. A nil run
// means the universe is empty — the view has connected before anything ran.
func (s *Server) snapshotLocked(r *runState) []byte {
	snap := Snapshot{
		Now:  time.Now().UnixMilli(),
		Runs: s.rosterLocked(),
	}
	if s.cur != nil {
		snap.Live = s.cur.hdr.RunID
	}
	if r != nil {
		snap.runHeader = r.hdr
		snap.Stages = r.stages
		snap.Tasks = r.tasks
		snap.Workers = r.workers
		snap.Broadcasts = r.shared
		snap.MCP = r.servers
		snap.Projection = r.proj
	} else {
		// Nothing has run yet. If a forecast has been published it is the only
		// thing there is to show, and this is the one moment when knowing the
		// price is still actionable. The host's MCP connections are the other
		// half of that: they exist before any run does, so an empty universe
		// can still say what this process is wired to.
		snap.Projection = s.pendingForecastLocked()
		snap.MCP = s.servers
	}
	if snap.Stages == nil {
		snap.Stages = []*StageInfo{}
	}
	if snap.Tasks == nil {
		snap.Tasks = []*Node{}
	}
	if snap.Workers == nil {
		snap.Workers = []*WorkerInfo{}
	}
	if snap.Broadcasts == nil {
		snap.Broadcasts = []*BroadcastInfo{}
	}
	if snap.MCP == nil {
		snap.MCP = []*MCPInfo{}
	}
	b, err := json.Marshal(snap)
	if err != nil {
		b = []byte(`{"error":"marshal failed"}`)
	}
	return b
}

func (s *Server) broadcastLocked(d delta) {
	if len(s.subs) == 0 {
		return
	}
	b, err := json.Marshal(d)
	if err != nil {
		return
	}
	for sub := range s.subs {
		select {
		case sub.ch <- b:
		default:
			sub.lost = true
		}
	}
}

// --- HTTP handlers ---------------------------------------------------------

func (s *Server) serveUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	page, err := uiFS.ReadFile("ui.html")
	if err != nil {
		http.Error(w, "ui not embedded", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(page)
}

// serveState returns one run's full state: the live run by default, or the
// run named by ?run= — which is how a viewer walks back into a pipeline that
// finished while the next one was starting.
func (s *Server) serveState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	want := r.URL.Query().Get("run")
	s.mu.Lock()
	target := s.cur
	missing := false
	if want != "" {
		target, missing = s.runIx[want], s.runIx[want] == nil
	}
	var b []byte
	if !missing {
		b = s.snapshotLocked(target)
	}
	s.mu.Unlock()
	if missing {
		http.Error(w, "unknown run", http.StatusNotFound)
		return
	}
	writeJSON(w, b)
}

// serveRuns returns the universe: every run being held, with the shape of its
// pipeline, so the overview can be built without loading any run's tasks.
func (s *Server) serveRuns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	out := struct {
		Now  int64         `json:"now"`
		Live string        `json:"live,omitempty"`
		Runs []*RunSummary `json:"runs"`
	}{Now: time.Now().UnixMilli(), Runs: s.rosterLocked()}
	if s.cur != nil {
		out.Live = s.cur.hdr.RunID
	}
	b, err := json.Marshal(out)
	s.mu.Unlock()
	if err != nil {
		http.Error(w, "marshal failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, b)
}

func writeJSON(w http.ResponseWriter, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(b)
}

// serveTask returns one task's heavy detail: full input and output records,
// every rendered model call, and the event log. This is the lazy half of the
// constellation — the UI asks for it when a node is opened, not before.
//
// ?run= scopes the lookup to one run; without it the search walks the
// universe newest-first, so a link to a task still resolves after the run
// that produced it has been left behind.
func (s *Server) serveTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	n := s.findTaskLocked(id, r.URL.Query().Get("run"))
	var b []byte
	ok := n != nil
	if ok {
		var err error
		b, err = json.Marshal(TaskDetail{
			ID: n.ID, Rev: n.rev, Input: n.Input, Output: n.Output,
			Calls: n.CallLog, Log: n.Log,
		})
		if err != nil {
			ok = false
		}
	}
	s.mu.Unlock()

	if !ok {
		http.Error(w, "unknown task", http.StatusNotFound)
		return
	}
	writeJSON(w, b)
}

func (s *Server) findTaskLocked(id, run string) *Node {
	if run != "" {
		if r, ok := s.runIx[run]; ok {
			return r.taskIx[id]
		}
		return nil
	}
	for i := len(s.runs) - 1; i >= 0; i-- {
		if n, ok := s.runs[i].taskIx[id]; ok {
			return n
		}
	}
	return nil
}

func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	sub := &subscriber{ch: make(chan []byte, 512)}
	s.mu.Lock()
	s.subs[sub] = struct{}{}
	first := s.snapshotLocked(s.cur)
	s.mu.Unlock()
	s.viewerOnce.Do(func() { close(s.viewer) })
	defer func() {
		s.mu.Lock()
		delete(s.subs, sub)
		s.mu.Unlock()
	}()

	writeSSE(w, "snapshot", first)
	fl.Flush()

	heal := time.NewTicker(2 * time.Second)
	defer heal.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case b := <-sub.ch:
			writeSSE(w, "delta", b)
			fl.Flush()
		case <-heal.C:
			// If any send was dropped, re-sync the client with a full
			// snapshot; otherwise just keep the connection alive.
			s.mu.Lock()
			lost := sub.lost
			sub.lost = false
			var snap []byte
			if lost {
				snap = s.snapshotLocked(s.cur)
			}
			s.mu.Unlock()
			if lost {
				writeSSE(w, "snapshot", snap)
			} else {
				_, _ = fmt.Fprint(w, ": ping\n\n")
			}
			fl.Flush()
		}
	}
}

func writeSSE(w http.ResponseWriter, event string, data []byte) {
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
}
