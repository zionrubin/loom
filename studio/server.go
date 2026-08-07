package studio

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

//go:embed ui.html
var uiFS embed.FS

// Server serves the studio: the single-file UI at /, and a small JSON API
// beside it.
//
// It holds one document. Every edit the browser makes is a POST of the whole
// document, which the server validates, compiles, prices and — if a file was
// given — saves, before answering with the new state. That round trip is the
// point: the price in the header is never computed in the browser, because the
// browser cannot compile a pipeline, read the source records, or know what a
// model costs. The canvas draws; the server is the one that knows.
type Server struct {
	mu      sync.Mutex
	doc     *Doc
	est     *Estimate
	savedAt time.Time
	// records caches source reads by the content hash of the source spec, so
	// editing a prompt does not re-read a folder of thousands of files.
	records map[string][]core.Record

	registry      *model.Registry
	file          string
	assistant     Assistant
	runner        RunFunc
	constellation string

	run *RunState

	mux  *http.ServeMux
	http *http.Server
}

// RunState is what the studio knows about the run it started.
type RunState struct {
	Running  bool   `json:"running"`
	Started  int64  `json:"started,omitempty"` // unix milliseconds
	Finished int64  `json:"finished,omitempty"`
	Err      string `json:"err,omitempty"`
	// URL is where to watch it, when a constellation view was attached.
	URL string `json:"url,omitempty"`
}

// RunFunc starts a run of the document. The studio compiles and prices; what
// it means to *run* — which secrets, which state directory, which event
// handler, whether a constellation view is watching — belongs to the program
// embedding the studio, so it is a function rather than a pile of options.
type RunFunc func(ctx context.Context, r RunRequest) error

// RunRequest is the compiled document, handed to the runner.
type RunRequest struct {
	Doc      *Doc
	Estimate *Estimate
	// Pipeline is the first pass, compiled and ready for loom.Run.
	Pipeline *pipeline.Pipeline
	// Second compiles the second pass from the first one's stage outputs
	// (loom.RunResult.StageOutputs). It returns nil when the document has no
	// merging fold, so a runner can always call it.
	Second func(out map[string][]core.Record) (*pipeline.Pipeline, error)
}

// Option configures a Server.
type Option func(*Server)

// Models supplies the registry the document's steps bind to. Without one the
// studio still edits and exports, and says plainly that it cannot price.
func Models(reg *model.Registry) Option { return func(s *Server) { s.registry = reg } }

// File makes the studio autosave the document to path after every accepted
// edit, and is what puts "saved just now" in the header.
func File(path string) Option { return func(s *Server) { s.file = path } }

// Assist replaces the built-in [Insight] assistant — with a model-backed one,
// for instance, which can answer the questions a projection cannot.
func Assist(a Assistant) Option {
	return func(s *Server) {
		if a != nil {
			s.assistant = a
		}
	}
}

// Runner attaches what the Run button does. Without one the button explains
// that this studio builds and prices but does not run, and points at the Go
// export.
func Runner(fn RunFunc) Option { return func(s *Server) { s.runner = fn } }

// Constellation is the URL of the viz server watching runs, which is where the
// RUN tab goes. The studio is the build half; watching a run is the
// constellation view's job, and it already does it.
func Constellation(url string) Option { return func(s *Server) { s.constellation = url } }

// New returns a studio serving doc.
func New(doc *Doc, opts ...Option) *Server {
	if doc == nil {
		doc = &Doc{Name: "untitled", Steps: nil}
	}
	doc.Layout()
	s := &Server{
		doc:       doc,
		assistant: Insight{},
		records:   map[string][]core.Record{},
		run:       &RunState{},
	}
	for _, o := range opts {
		o(s)
	}
	s.est = s.priceLocked(doc)

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.serveUI)
	mux.HandleFunc("/api/state", s.serveState)
	mux.HandleFunc("/api/doc", s.serveDoc)
	mux.HandleFunc("/api/ask", s.serveAsk)
	mux.HandleFunc("/api/accept", s.serveAccept)
	mux.HandleFunc("/api/export", s.serveExport)
	mux.HandleFunc("/api/sample", s.serveSample)
	mux.HandleFunc("/api/run", s.serveRun)
	s.mux = mux
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Start listens on addr (e.g. "localhost:8078", or ":0" for an ephemeral
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

// Doc returns the current document. The returned value is a copy: the studio's
// own document is only ever replaced under its lock.
func (s *Server) Doc() *Doc {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.doc.Clone()
}

// Estimate returns the current projection.
func (s *Server) Estimate() *Estimate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.est
}

// SetDoc replaces the document, revalidating and repricing it.
func (s *Server) SetDoc(d *Doc) error {
	if err := d.Validate(); err != nil {
		return err
	}
	d.Layout()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.doc = d
	s.est = s.priceLocked(d)
	return s.saveLocked()
}

// --- state -----------------------------------------------------------------

// state is what the UI renders from: the document, its price, and the few
// things about the studio itself the canvas has to know.
type state struct {
	Doc      *Doc      `json:"doc"`
	Estimate *Estimate `json:"estimate"`
	Intents  []Intent  `json:"intents"`
	// Fields lists the record fields visible at each step, which is what fills
	// the prompt chips in the inspector.
	Fields map[string][]string `json:"fields"`
	// Ops and Kinds are the vocabularies the inspector offers, served from the
	// same place that validates them so the two cannot drift.
	Ops   []string `json:"ops"`
	Kinds []Kind   `json:"kinds"`

	SavedAt       int64     `json:"saved_at,omitempty"` // unix milliseconds
	File          string    `json:"file,omitempty"`
	Constellation string    `json:"constellation,omitempty"`
	CanRun        bool      `json:"can_run"`
	Run           *RunState `json:"run"`
}

func (s *Server) stateLocked() *state {
	run := *s.run // a value, so the response can be written after the unlock
	st := &state{
		Doc: s.doc, Estimate: s.est,
		Intents: s.assistant.Intents(s.doc, s.est),
		Fields:  map[string][]string{},
		Ops:     Ops(), Kinds: Kinds(),
		File: s.file, Constellation: s.constellation,
		CanRun: s.runner != nil, Run: &run,
	}
	for i := range s.doc.Steps {
		id := s.doc.Steps[i].ID
		st.Fields[id] = s.doc.Fields(id)
	}
	if !s.savedAt.IsZero() {
		st.SavedAt = s.savedAt.UnixMilli()
	}
	return st
}

// priceLocked prices d and keeps the source records it read.
func (s *Server) priceLocked(d *Doc) *Estimate {
	est, fresh := priceDoc(d, s.registry, s.records)
	// Keep only what the current document reads, so editing a folder path does
	// not pin the old folder's records in memory for the rest of the session.
	s.records = fresh
	return est
}

// priceDoc prices d, reading each source once and reusing anything cache
// already holds. It returns the cache the document actually needs, which is
// how a studio that has been edited for an hour still holds one folder's
// records rather than every folder ever typed into the inspector.
//
// It touches no server state, so it is safe to hand to an assistant that may
// take its time: the records it reads are local files, and pricing issues no
// model calls.
func priceDoc(d *Doc, reg *model.Registry, cache map[string][]core.Record) (*Estimate, map[string][]core.Record) {
	opts := []PriceOption{WithRegistry(reg)}
	fresh := map[string][]core.Record{}
	for i := range d.Steps {
		st := &d.Steps[i]
		if st.Kind != KindSource {
			continue
		}
		key := sourceKey(st.Source)
		recs, ok := cache[key]
		if !ok {
			var err error
			recs, err = LoadRecords(context.Background(), st.Source)
			if err != nil {
				return &Estimate{Pipeline: d.Name, CapUSD: d.CapUSD, Error: err.Error()}, fresh
			}
		}
		fresh[key] = recs
		opts = append(opts, WithRecords(st.ID, recs))
	}
	return Price(d, opts...), fresh
}

// sourceKey is the content hash of a source declaration: two steps reading the
// same folder the same way share one read.
func sourceKey(spec *SourceSpec) string {
	b, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8])
}

func (s *Server) saveLocked() error {
	if s.file == "" {
		return nil
	}
	if err := s.doc.Save(s.file); err != nil {
		return err
	}
	s.savedAt = time.Now()
	return nil
}

// --- handlers --------------------------------------------------------------

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
	if !get(w, r) {
		return
	}
	s.mu.Lock()
	st := s.stateLocked()
	s.mu.Unlock()
	writeJSON(w, st)
}

// serveDoc replaces the document. The browser posts the whole thing rather
// than a patch: a document is small, a patch protocol is not, and the server
// has to revalidate everything either way.
func (s *Server) serveDoc(w http.ResponseWriter, r *http.Request) {
	if !post(w, r) {
		return
	}
	var d Doc
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<20)).Decode(&d); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	if err := d.Validate(); err != nil {
		// A half-edited document is the normal state of an editor, so this is
		// a 422 carrying the reason, not a 500. The canvas shows it and keeps
		// the last good document on screen.
		httpError(w, http.StatusUnprocessableEntity, err)
		return
	}
	d.Layout()
	s.mu.Lock()
	s.doc = &d
	s.est = s.priceLocked(&d)
	saveErr := s.saveLocked()
	st := s.stateLocked()
	s.mu.Unlock()
	if saveErr != nil {
		httpError(w, http.StatusInternalServerError, saveErr)
		return
	}
	writeJSON(w, st)
}

func (s *Server) serveAsk(w http.ResponseWriter, r *http.Request) {
	if !post(w, r) {
		return
	}
	var body struct {
		Q        string `json:"q"`
		Selected string `json:"selected"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	s.mu.Lock()
	doc, est, a, reg := s.doc.Clone(), s.est, s.assistant, s.registry
	cache := maps.Clone(s.records)
	s.mu.Unlock()

	// The assistant runs outside the lock, on a copy of the document, with a
	// pricing function that reads local files and nothing else. A proposal is
	// allowed to take a moment; the studio is not.
	p, err := a.Ask(Request{Doc: doc, Estimate: est, Query: body.Q, Selected: body.Selected,
		Price: func(d *Doc) *Estimate {
			e, _ := priceDoc(d, reg, cache)
			return e
		}})
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, p)
}

// serveAccept applies a proposal's edits. The browser sends back the edits it
// was given rather than a new document, so the diff that was shown is exactly
// the diff that is applied.
func (s *Server) serveAccept(w http.ResponseWriter, r *http.Request) {
	if !post(w, r) {
		return
	}
	var body struct {
		Edits []Edit `json:"edits"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&body); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	s.mu.Lock()
	next, err := s.doc.Apply(body.Edits)
	if err != nil {
		s.mu.Unlock()
		httpError(w, http.StatusUnprocessableEntity, err)
		return
	}
	s.doc = next
	s.est = s.priceLocked(next)
	saveErr := s.saveLocked()
	st := s.stateLocked()
	s.mu.Unlock()
	if saveErr != nil {
		httpError(w, http.StatusInternalServerError, saveErr)
		return
	}
	writeJSON(w, st)
}

func (s *Server) serveExport(w http.ResponseWriter, r *http.Request) {
	if !get(w, r) {
		return
	}
	s.mu.Lock()
	doc := s.doc.Clone()
	s.mu.Unlock()
	src, err := doc.Go()
	if err != nil {
		httpError(w, http.StatusUnprocessableEntity, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(src))
}

// serveSample returns the first records of a source, which is the "first
// record" panel in the source inspector — the one place a person can check
// that the redactions did what they say before anything is sent anywhere.
func (s *Server) serveSample(w http.ResponseWriter, r *http.Request) {
	if !get(w, r) {
		return
	}
	id := r.URL.Query().Get("step")
	n, _ := strconv.Atoi(r.URL.Query().Get("n"))
	if n <= 0 || n > 20 {
		n = 1
	}
	s.mu.Lock()
	step := s.doc.Find(id)
	var recs []core.Record
	var err error
	if step != nil && step.Kind == KindSource {
		key := sourceKey(step.Source)
		var ok bool
		recs, ok = s.records[key]
		if !ok {
			recs, err = LoadRecords(context.Background(), step.Source)
			if err == nil {
				s.records[key] = recs
			}
		}
	}
	s.mu.Unlock()
	if step == nil || step.Kind != KindSource {
		httpError(w, http.StatusNotFound, fmt.Errorf("no source step %q", id))
		return
	}
	if err != nil {
		httpError(w, http.StatusUnprocessableEntity, err)
		return
	}
	if len(recs) > n {
		recs = recs[:n]
	}
	writeJSON(w, struct {
		Total   int           `json:"total"`
		Records []core.Record `json:"records"`
	}{len(recs), recs})
}

// serveRun starts the run, if a runner was attached.
func (s *Server) serveRun(w http.ResponseWriter, r *http.Request) {
	if !post(w, r) {
		return
	}
	s.mu.Lock()
	if s.runner == nil {
		s.mu.Unlock()
		httpError(w, http.StatusNotImplemented, fmt.Errorf(
			"this studio builds and prices but does not run: export the Go and run it, "+
				"or attach a runner with studio.Runner"))
		return
	}
	if s.run.Running {
		st := s.stateLocked()
		s.mu.Unlock()
		writeJSON(w, st)
		return
	}
	doc, est, runner := s.doc.Clone(), s.est, s.runner
	p, err := doc.Build()
	if err != nil {
		s.mu.Unlock()
		httpError(w, http.StatusUnprocessableEntity, err)
		return
	}
	s.run = &RunState{Running: true, Started: time.Now().UnixMilli(), URL: s.constellation}
	st := s.stateLocked()
	s.mu.Unlock()

	go func() {
		err := runner(context.Background(), RunRequest{
			Doc: doc, Estimate: est, Pipeline: p, Second: doc.BuildSecond,
		})
		s.mu.Lock()
		defer s.mu.Unlock()
		s.run.Running = false
		s.run.Finished = time.Now().UnixMilli()
		if err != nil {
			s.run.Err = err.Error()
		}
	}()
	writeJSON(w, st)
}

// --- http plumbing ---------------------------------------------------------

func get(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

// post gates every mutating request on the method and on the request's origin.
//
// The studio writes files and reads folders on the machine it runs on, so a
// page on some other origin must not be able to drive it through the browser
// that has it open. A same-origin check is the cheap half of that: requests
// from a page carry an Origin, and one that is not this server's is refused.
// Requests with no Origin at all — curl, a test, the studio's own fetches —
// are allowed through.
func post(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host != r.Host {
		http.Error(w, "cross-origin request refused", http.StatusForbidden)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "marshal failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(b)
}

// httpError answers with a JSON error, because everything that goes wrong here
// is something the canvas has to show a human: a document that does not
// validate, a folder that is not there, a model that is not registered.
func httpError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{err.Error()})
}
