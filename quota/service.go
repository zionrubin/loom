package quota

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// This file is the other backend, and it exists for two reasons.
//
// The first is the honest test of an interface: a set of interfaces with one
// implementation is not a set of interfaces. The conformance suite in
// quota/quotatest runs against both, so "the store is replaceable" is a
// property something checks rather than a claim the package makes about itself.
//
// The second is that a shared directory is a machine. The moment a fleet spans
// hosts — workers on three boxes, a job per pod, a serving tier behind a load
// balancer — the directory stops being shared and every process is back to
// believing it owns the whole account. What those fleets need is one process
// that holds the state and answers questions about it, which is what a
// [Handler] is: five operations over HTTP, no state of its own, wrapping any
// [Store].
//
//	// the one process that holds the quota
//	q, _ := quota.Open("/var/lib/loom/quota", quota.Options{})
//	http.ListenAndServe(":9095", quota.Handler(q))
//
//	// every process that spends it
//	loom.Run(ctx, p, loom.WithSharedQuota(quota.Dial("http://quota:9095", quota.ClientOptions{}),
//	    core.Budget{MaxCostUSD: 500}, 24*time.Hour))
//
// The service is deliberately thin. It does not decide anything, hold a
// ceiling, or know what a pipeline is; it is the directory store with a socket
// in front of it, which is what keeps the two backends interchangeable rather
// than merely similar.

// Handler serves a [Store] over HTTP. Mount it wherever you like; the paths it
// answers are relative to the mount point.
func Handler(s Store) http.Handler {
	mux := http.NewServeMux()
	h := &server{store: s}
	mux.HandleFunc("POST /draw", h.draw)
	mux.HandleFunc("POST /return", h.give)
	mux.HandleFunc("POST /charge", h.charge)
	mux.HandleFunc("GET /ledger", h.ledger)
	mux.HandleFunc("GET /models", h.models)
	return mux
}

type server struct{ store Store }

func (h *server) draw(w http.ResponseWriter, r *http.Request) {
	var d Draw
	if !decode(w, r, &d) {
		return
	}
	g, err := h.store.Draw(r.Context(), d)
	reply(w, g, err)
}

func (h *server) give(w http.ResponseWriter, r *http.Request) {
	var d Draw
	if !decode(w, r, &d) {
		return
	}
	reply(w, struct{}{}, h.store.Return(r.Context(), d))
}

func (h *server) charge(w http.ResponseWriter, r *http.Request) {
	var c Charge
	if !decode(w, r, &c) {
		return
	}
	l, err := h.store.Charge(r.Context(), c)
	reply(w, l, err)
}

func (h *server) ledger(w http.ResponseWriter, r *http.Request) {
	window, err := parseWindow(r.URL.Query().Get("window"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	l, err := h.store.Ledger(r.Context(), window)
	reply(w, l, err)
}

func (h *server) models(w http.ResponseWriter, r *http.Request) {
	m, err := h.store.Models(r.Context())
	if m == nil {
		m = []ModelState{}
	}
	reply(w, m, err)
}

func parseWindow(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("quota: bad window %q", s)
	}
	return time.Duration(n), nil
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(v); err != nil {
		http.Error(w, fmt.Sprintf("quota: bad request: %v", err), http.StatusBadRequest)
		return false
	}
	return true
}

// reply writes the answer, or the error as a 500 with its text as the body.
//
// A quota service's errors are not the caller's to classify: whatever went
// wrong, the caller's only correct response is to treat the draw as refused and
// retry, which is what a transport error already produces. So the wire carries
// the message and nothing else.
func reply(w http.ResponseWriter, v any, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// --- client --------------------------------------------------------------

// ClientOptions tunes a [Client]. The zero value works.
type ClientOptions struct {
	// HTTP is the client to use (default: one with Timeout set below).
	HTTP *http.Client
	// Timeout bounds one request (default 10s). Ignored when HTTP is given,
	// because a caller who supplied a client supplied its timeout too.
	Timeout time.Duration
}

// Client is a [Store] served by a [Handler] on another host.
type Client struct {
	base string
	http *http.Client
}

// Dial returns a store backed by the quota service at base.
//
// It opens no connection: there is nothing to hand shake, and a store that
// failed at construction because the service was restarting would take a fleet
// down for a reason a retry would have fixed. The first draw is the first
// contact, and a draw that cannot reach the service is a transient failure the
// scheduler already knows how to wait out.
func Dial(base string, opts ClientOptions) *Client {
	c := opts.HTTP
	if c == nil {
		t := opts.Timeout
		if t <= 0 {
			t = 10 * time.Second
		}
		c = &http.Client{Timeout: t}
	}
	return &Client{base: strings.TrimSuffix(base, "/"), http: c}
}

// Close releases the client. The state is the service's.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

func (c *Client) Draw(ctx context.Context, d Draw) (Grant, error) {
	if !d.Metered() || len(d.Tokens) == 0 {
		return Grant{Admitted: len(d.Tokens)}, nil
	}
	var g Grant
	err := c.post(ctx, "/draw", d, &g)
	return g, err
}

func (c *Client) Return(ctx context.Context, d Draw) error {
	if !d.Metered() || len(d.Tokens) == 0 {
		return nil
	}
	return c.post(ctx, "/return", d, nil)
}

func (c *Client) Charge(ctx context.Context, ch Charge) (Ledger, error) {
	if ch.Window > Retention {
		return Ledger{}, fmt.Errorf("%w: %s > %s", ErrWindowTooLong, ch.Window, Retention)
	}
	var l Ledger
	err := c.post(ctx, "/charge", ch, &l)
	return l, err
}

func (c *Client) Ledger(ctx context.Context, window time.Duration) (Ledger, error) {
	if window > Retention {
		return Ledger{}, fmt.Errorf("%w: %s > %s", ErrWindowTooLong, window, Retention)
	}
	var l Ledger
	err := c.get(ctx, "/ledger?window="+strconv.FormatInt(int64(window), 10), &l)
	return l, err
}

func (c *Client) Models(ctx context.Context) ([]ModelState, error) {
	var m []ModelState
	err := c.get(ctx, "/models", &m)
	return m, err
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("quota: encode %s: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("quota: %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	return c.do(req, path, out)
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return fmt.Errorf("quota: %s: %w", path, err)
	}
	return c.do(req, path, out)
}

func (c *Client) do(req *http.Request, path string, out any) error {
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("quota: %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("quota: %s: %s: %s", path, resp.Status, strings.TrimSpace(string(msg)))
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("quota: %s: decode: %w", path, err)
	}
	return nil
}
