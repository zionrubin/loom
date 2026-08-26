package recall

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

//go:embed console.html
var consoleHTML []byte

// Handler serves the desk over HTTP.
//
//	GET  /             a console to talk to the desk in
//	POST /ask          {"session":…,"text":…,"min_version":…} → Answer
//	GET  /context      the living prompt, as the model receives it
//	GET  /stats        Stats
//	POST /ingest       {"conversation":…,"messages":[…]} or a bare message array
//	GET  /updates      long-poll: returns when the context passes ?version=
//
// The split between them is the package's split. /ingest is the write path and
// returns as soon as the messages are on the feed; /ask is the read path and
// answers from whatever is published. Neither waits for the other, and
// /updates is how a client that wants them coupled couples them itself.
func (d *Desk) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", d.serveConsole)
	mux.HandleFunc("/ask", d.serveAsk)
	mux.HandleFunc("/context", d.serveContext)
	mux.HandleFunc("/stats", d.serveStats)
	mux.HandleFunc("/ingest", d.serveIngest)
	mux.HandleFunc("/updates", d.serveUpdates)
	return mux
}

// Serve listens on addr (":0" for an ephemeral port) and serves the desk in a
// background goroutine, returning the URL and a function that stops it.
func (d *Desk) Serve(addr string) (string, func() error, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{Handler: d.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()

	host, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return "http://" + ln.Addr().String(), srv.Close, nil
	}
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port), srv.Close, nil
}

func (d *Desk) serveConsole(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(consoleHTML)
}

// serveAsk is the read path.
func (d *Desk) serveAsk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST a question", http.StatusMethodNotAllowed)
		return
	}
	var q Question
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&q); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	// A question that asked to wait for a version that may never come would
	// otherwise hold a connection open indefinitely.
	if q.Wait <= 0 && (q.MinVersion > 0 || q.Fresh > 0) {
		q.Wait = 30 * time.Second
	}
	ans, err := d.Ask(r.Context(), q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, ans)
}

// serveContext returns the living prompt itself, which is the endpoint worth
// having on a serving layer whose product is a prompt: what a model is being
// told is not an implementation detail, and a desk that would not show you is
// one you cannot debug.
func (d *Desk) serveContext(w http.ResponseWriter, r *http.Request) {
	v := d.Context()
	w.Header().Set("X-Recall-Version", strconv.FormatInt(v.Version, 10))
	w.Header().Set("X-Recall-Entries", strconv.Itoa(v.Entries))
	if v.Ref.Hash != "" {
		w.Header().Set("X-Recall-Revision", v.Ref.Hash)
	}
	if r.URL.Query().Get("format") == "json" {
		writeJSON(w, v)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprint(w, v.Text)
}

func (d *Desk) serveStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, d.Stats())
}

// ingestRequest accepts either a conversation or a bare list of messages, so a
// caller with one turn does not have to wrap it in a thread it does not have.
type ingestRequest struct {
	Conversation string         `json:"conversation,omitempty"`
	Meta         map[string]any `json:"meta,omitempty"`
	Messages     []Message      `json:"messages"`
}

// serveIngest is the write path.
func (d *Desk) serveIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST messages", http.StatusMethodNotAllowed)
		return
	}
	body := http.MaxBytesReader(w, r.Body, 8<<20)
	var req ingestRequest
	dec := json.NewDecoder(body)
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Messages) == 0 {
		http.Error(w, "no messages", http.StatusBadRequest)
		return
	}
	n, err := d.Ingest(r.Context(), Conversation{
		ID: req.Conversation, Messages: req.Messages, Meta: req.Meta,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]any{"accepted": len(req.Messages), "posted": n})
}

// serveUpdates long-polls for the next context revision past ?version=.
//
// It is how a client keeps a live view without polling: ask for one past what
// you have, and the request returns when ingestion publishes it. A bounded wait
// keeps it a poll rather than a subscription, because a desk that has nothing
// to say should not hold a connection open forever to say it.
func (d *Desk) serveUpdates(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("version"), 10, 64)
	wait := 25 * time.Second
	if s := r.URL.Query().Get("wait"); s != "" {
		if parsed, err := time.ParseDuration(s); err == nil && parsed > 0 && parsed <= time.Minute {
			wait = parsed
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()

	v, err := d.AwaitVersion(ctx, after+1)
	if err != nil {
		// A timeout is not an error here: the context simply has not moved, and
		// the current version is the answer to "what is it now?".
		v = d.Context()
	}
	writeJSON(w, v)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		// The header is already written; there is nowhere left to report this
		// but the server's own logs, and the client will see a short body.
		_ = err
	}
}
