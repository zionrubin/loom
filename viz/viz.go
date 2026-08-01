// Package viz serves Loom's constellation view: a live, in-browser
// visualization of a run in which every task and executor is a star-like
// node. Queued tasks sit dim and unlit; running tasks illuminate and gently
// pulse; tasks that run long grow brighter and gain a rotating activity
// ring; completed tasks flash and settle into a stable star (cache replays
// settle in a distinct hue); failures burn as a clearly-marked red cross.
// Clicking any node opens its full detail: stage, executor, model, input,
// runtime, token usage, cost, retries, event log, and errors.
//
// Wire it into a run with two lines:
//
//	v := viz.New()
//	url, _ := v.Start("localhost:8077")
//	res, err := loom.Run(ctx, p, loom.WithEventHandler(v.Handle), ...)
//
// The server folds the event stream into the state of the current run (a
// new run.started event resets it), serves the embedded single-file UI at
// /, a JSON snapshot at /api/state, a live delta stream at /api/events
// (server-sent events), and one task's full detail at /api/task?id=…. It has
// no dependencies beyond the standard library.
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
	RunID     string `json:"runId"`
	StartedAt int64  `json:"startedAt,omitempty"`
	EndedAt   int64  `json:"endedAt,omitempty"`
	Done      bool   `json:"done"`
	Note      string `json:"note,omitempty"`
	// Driver names the execution strategy: "barrier" runs a stage at a time,
	// "streaming" pipelines them across one shared pool of executors.
	Driver string `json:"driver,omitempty"`
}

// Snapshot is the complete state of the current run.
type Snapshot struct {
	runHeader
	Now        int64            `json:"now"`
	Stages     []*StageInfo     `json:"stages"`
	Tasks      []*Node          `json:"tasks"`
	Workers    []*WorkerInfo    `json:"workers"`
	Broadcasts []*BroadcastInfo `json:"broadcasts"`
	Projection *ProjectionInfo  `json:"projection,omitempty"`
}

// delta is one incremental update: the run header plus whichever entities
// the triggering event changed.
type delta struct {
	Now        int64           `json:"now"`
	Reset      bool            `json:"reset,omitempty"`
	Run        runHeader       `json:"run"`
	Stage      *StageInfo      `json:"stage,omitempty"`
	Task       *Node           `json:"task,omitempty"`
	Worker     *WorkerInfo     `json:"worker,omitempty"`
	Broadcast  *BroadcastInfo  `json:"broadcast,omitempty"`
	Projection *ProjectionInfo `json:"projection,omitempty"`
}

const (
	maxLogEntries  = 80
	maxCallEntries = 24
	// Reader lists are for showing *where* a value landed, not for
	// enumerating every task; Readers stays exact regardless.
	maxBroadcastTasks = 24
)

type subscriber struct {
	ch   chan []byte
	lost bool // a send was dropped; the writer heals with a full snapshot
}

// Server folds observe events into run state and serves the constellation UI.
type Server struct {
	mu       sync.Mutex
	run      runHeader
	stages   []*StageInfo
	stageIx  map[string]*StageInfo
	tasks    []*Node
	taskIx   map[string]*Node
	workers  []*WorkerInfo
	workIx   map[string]*WorkerInfo
	shared   []*BroadcastInfo
	sharedIx map[string]*BroadcastInfo
	subs     map[*subscriber]struct{}

	// The projection outlives a run reset. It describes the pipeline, and the
	// run that follows is precisely the thing it predicted, so clearing it at
	// run.started would discard the comparison exactly when it starts being
	// worth something. projIx keeps the per-stage forecasts so they can be
	// reattached to StageInfos the reset threw away.
	projection *ProjectionInfo
	projIx     map[string]*StageProjection

	viewer     chan struct{}
	viewerOnce sync.Once

	mux  *http.ServeMux
	http *http.Server
}

// New returns a Server with empty state.
func New() *Server {
	s := &Server{
		stageIx:  map[string]*StageInfo{},
		taskIx:   map[string]*Node{},
		workIx:   map[string]*WorkerInfo{},
		sharedIx: map[string]*BroadcastInfo{},
		projIx:   map[string]*StageProjection{},
		subs:     map[*subscriber]struct{}{},
		viewer:   make(chan struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.serveUI)
	mux.HandleFunc("/api/state", s.serveState)
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

	switch e.Type {
	case observe.RunStarted:
		s.resetLocked()
		s.run = runHeader{RunID: e.RunID, StartedAt: now, Driver: e.Kind}
		d.Reset = true

	case observe.RunFinished:
		s.run.Done = true
		s.run.EndedAt = now

	case observe.StageProjected:
		p := &StageProjection{
			Calls: e.Usage.Requests, Records: e.Records,
			Tokens: e.Usage.TotalTokens(), ExpectedUSD: e.Usage.CostUSD,
			CeilingUSD: e.Ceiling.CostUSD, FloorMS: e.Latency.Milliseconds(),
			Estimated: e.Note == "estimated",
		}
		s.projIx[e.Stage] = p
		st := s.stageLocked(e.Stage)
		st.Proj = p
		// A projection arrives before the run, so it is usually the event that
		// creates the stage. Seed what it knows; stage.started overwrites it
		// with the real thing.
		if st.Kind == "" {
			st.Kind = e.Kind
		}
		d.Stage = st

	case observe.RunProjected:
		s.projection = &ProjectionInfo{
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
			s.projection.Warnings = strings.Split(e.Detail, "\n")
		}
		d.Projection = s.projection

	case observe.StageStarted:
		st := s.stageLocked(e.Stage)
		st.Upstream = e.Upstream
		st.Kind = e.Kind
		st.Detail = e.Detail
		st.Status = "running"
		st.StartedAt = now
		d.Stage = st

	case observe.StageFinished:
		st := s.stageLocked(e.Stage)
		st.Status = "done"
		st.EndedAt = now
		d.Stage = st

	case observe.TaskScheduled:
		n := s.nodeLocked(e.TaskID, e.Stage)
		n.Records = e.Records
		n.Input = e.Input
		n.InputIDs = e.InputIDs
		logf(n, now, "scheduled (%d record%s)", e.Records, plural(e.Records))
		d.Task = n

	case observe.TaskStarted:
		n := s.nodeLocked(e.TaskID, e.Stage)
		n.Status = "running"
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
		d.Worker = s.workerStartLocked(e.Worker, e.TaskID, now)

	case observe.ModelCalled:
		n := s.nodeLocked(e.TaskID, e.Stage)
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
				st := s.stageLocked(e.Stage)
				st.CachedTokens += e.Usage.CacheReadTokens
				st.WrittenTokens += e.Usage.CacheWriteTokens
				st.SavedUSD += e.Saved
				d.Stage = st
			}
		}
		d.Task = n

	case observe.CacheHit:
		n := s.nodeLocked(e.TaskID, e.Stage)
		n.CacheHit = true
		logf(n, now, "cache hit — result replayed, zero model calls")
		d.Task = n

	case observe.TaskRetried:
		n := s.nodeLocked(e.TaskID, e.Stage)
		n.Status = "retrying"
		n.Retries++
		n.Attempts = e.Attempt
		n.Error = e.Err
		logf(n, now, "attempt %d failed: %s → retrying (%s)", e.Attempt, e.Err, e.Note)
		d.Task = n

	case observe.TaskCompleted:
		n := s.nodeLocked(e.TaskID, e.Stage)
		n.Status = "completed"
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
		d.Worker = s.workerEndLocked(e.Worker, e.TaskID, now, false)

	case observe.TaskFailed:
		n := s.nodeLocked(e.TaskID, e.Stage)
		n.Status = "failed"
		n.Attempts = e.Attempt
		n.EndedAt = now
		n.Error = e.Err
		logf(n, now, "failed after %d attempt%s: %s", e.Attempt, plural(e.Attempt), e.Err)
		d.Task = n
		d.Worker = s.workerEndLocked(e.Worker, e.TaskID, now, true)

	case observe.BroadcastRegistered:
		bc := s.sharedLocked(e.Broadcast)
		bc.Hash = e.Artifact
		bc.Bytes = e.Bytes
		bc.Preview = e.Detail
		d.Broadcast = bc

	case observe.BroadcastRead:
		bc := s.sharedLocked(e.Broadcast)
		bc.Readers++
		if e.Stage != "" && !contains(bc.Stages, e.Stage) {
			bc.Stages = append(bc.Stages, e.Stage)
		}
		if len(bc.Tasks) < maxBroadcastTasks {
			bc.Tasks = append(bc.Tasks, e.TaskID)
		}
		n := s.nodeLocked(e.TaskID, e.Stage)
		if !contains(n.Broadcasts, e.Broadcast) {
			n.Broadcasts = append(n.Broadcasts, e.Broadcast)
		}
		logf(n, now, "read shared value %q (%s) — referenced, not copied",
			e.Broadcast, shortHash(e.Artifact))
		d.Task = n
		d.Broadcast = bc

	case observe.BudgetExceeded:
		s.run.Note = "budget exceeded: " + e.Note
		if e.TaskID != "" {
			n := s.nodeLocked(e.TaskID, e.Stage)
			logf(n, now, "budget exceeded: %s", e.Note)
			d.Task = n
		}
	}

	d.Run = s.run
	s.broadcastLocked(d)
}

// resetLocked clears the run. The projection deliberately survives: it
// describes the pipeline rather than any one run, and the run starting is
// exactly when the comparison it enables becomes worth something.
func (s *Server) resetLocked() {
	s.run = runHeader{}
	s.stages = nil
	s.stageIx = map[string]*StageInfo{}
	s.tasks = nil
	s.taskIx = map[string]*Node{}
	s.workers = nil
	s.workIx = map[string]*WorkerInfo{}
	s.shared = nil
	s.sharedIx = map[string]*BroadcastInfo{}
}

// sharedLocked returns the entry for a broadcast name, creating it if a read
// arrives before (or without) its registration event.
func (s *Server) sharedLocked(name string) *BroadcastInfo {
	if bc, ok := s.sharedIx[name]; ok {
		return bc
	}
	bc := &BroadcastInfo{ID: name}
	s.sharedIx[name] = bc
	s.shared = append(s.shared, bc)
	return bc
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

func (s *Server) stageLocked(id string) *StageInfo {
	if st, ok := s.stageIx[id]; ok {
		return st
	}
	// Reattach the forecast a reset dropped, so a re-run against the same
	// projection still shows expected beside actual.
	st := &StageInfo{ID: id, Proj: s.projIx[id]}
	s.stageIx[id] = st
	s.stages = append(s.stages, st)
	return st
}

func (s *Server) nodeLocked(id, stage string) *Node {
	if n, ok := s.taskIx[id]; ok {
		return n
	}
	n := &Node{ID: id, Stage: stage, Status: "pending"}
	s.taskIx[id] = n
	s.tasks = append(s.tasks, n)
	return n
}

func (s *Server) workerStartLocked(id, taskID string, now int64) *WorkerInfo {
	if id == "" {
		return nil
	}
	w, ok := s.workIx[id]
	if !ok {
		w = &WorkerInfo{ID: id}
		s.workIx[id] = w
		s.workers = append(s.workers, w)
	}
	w.Current = taskID
	if w.busyStart == 0 {
		w.busyStart = now
	}
	return w
}

func (s *Server) workerEndLocked(id, taskID string, now int64, failed bool) *WorkerInfo {
	if id == "" {
		return nil
	}
	w, ok := s.workIx[id]
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

func (s *Server) snapshotLocked() []byte {
	snap := Snapshot{
		runHeader:  s.run,
		Now:        time.Now().UnixMilli(),
		Stages:     s.stages,
		Tasks:      s.tasks,
		Workers:    s.workers,
		Broadcasts: s.shared,
		Projection: s.projection,
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

func (s *Server) serveState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	b := s.snapshotLocked()
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(b)
}

// serveTask returns one task's heavy detail: full input and output records,
// every rendered model call, and the event log. This is the lazy half of the
// constellation — the UI asks for it when a node is opened, not before.
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
	n, ok := s.taskIx[id]
	var b []byte
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
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(b)
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
	first := s.snapshotLocked()
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
				snap = s.snapshotLocked()
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
