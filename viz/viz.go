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
// /, a JSON snapshot at /api/state, and a live delta stream at /api/events
// (server-sent events). It has no dependencies beyond the standard library.
package viz

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
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
	Log   []LogEntry `json:"log"`
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
}

// Snapshot is the complete state of the current run.
type Snapshot struct {
	runHeader
	Now        int64            `json:"now"`
	Stages     []*StageInfo     `json:"stages"`
	Tasks      []*Node          `json:"tasks"`
	Workers    []*WorkerInfo    `json:"workers"`
	Broadcasts []*BroadcastInfo `json:"broadcasts"`
}

// delta is one incremental update: the run header plus whichever entities
// the triggering event changed.
type delta struct {
	Now       int64          `json:"now"`
	Reset     bool           `json:"reset,omitempty"`
	Run       runHeader      `json:"run"`
	Stage     *StageInfo     `json:"stage,omitempty"`
	Task      *Node          `json:"task,omitempty"`
	Worker    *WorkerInfo    `json:"worker,omitempty"`
	Broadcast *BroadcastInfo `json:"broadcast,omitempty"`
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
		subs:     map[*subscriber]struct{}{},
		viewer:   make(chan struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.serveUI)
	mux.HandleFunc("/api/state", s.serveState)
	mux.HandleFunc("/api/events", s.serveEvents)
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
		s.run = runHeader{RunID: e.RunID, StartedAt: now}
		d.Reset = true

	case observe.RunFinished:
		s.run.Done = true
		s.run.EndedAt = now

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
	st := &StageInfo{ID: id}
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
