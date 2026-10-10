// Package typesafetest is a minimal in-process System One server for testing
// Loom pipelines that make decisions with Jev, and for examples that must run
// offline.
//
// It is to package typesafe what llamacpptest is to package llamacpp: a real
// implementation of the wire protocol — POST /v1/systemone, bearer auth,
// typed questions in, typed answers with distributions and confidences out,
// 400 for a malformed schema, scripted 429s and 5xxs — with the judgment
// itself supplied by a deterministic function. It listens on a genuine
// loopback socket, so the provider's HTTP client, its error classification
// and the egress allowlist a Jev binding produces are all exercised rather
// than bypassed.
//
// The helpers Choice, Score and Noul turn raw weights into answers the way a
// decision model reports them: normalized probabilities, and a confidence
// derived from the shape of the distribution — concentrated is high, spread
// out is low — so an offline judge produces answers with the same statistics
// a confidence gate will see in production.
package typesafetest

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/providers/typesafe"
)

// Judge answers one request: the state, and the questions asked about it.
type Judge func(state string, qs model.Questions) (map[string]model.Answer, error)

// Request is one decision as the server received it.
type Request struct {
	Model     string
	State     string
	Questions model.Questions
}

// Server is a scriptable System One server.
type Server struct {
	// Judge produces the answers. Nil answers every question with its first
	// option (or lowest level, or 0.9 yes) at high confidence.
	Judge Judge
	// APIKey, when set, is required as a bearer token.
	APIKey string
	// Served is the model version reported in responses (default
	// "jev-1.13.0-test").
	Served string
	// Delay sleeps inside every request, to give a run realistic latency.
	Delay time.Duration
	// Failures are HTTP statuses returned by successive requests before the
	// judge runs; each request consumes one.
	Failures []int

	mu       sync.Mutex
	calls    int
	requests []Request
}

// Start listens on a loopback address and returns the base URL to hand
// typesafe.WithBaseURL, plus the function that shuts the server down.
func (s *Server) Start() (baseURL string, stop func()) {
	srv := httptest.NewServer(s.Handler())
	return srv.URL, srv.Close
}

// Handler returns the server's routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(typesafe.Path, s.handle)
	return mux
}

// Calls returns how many decision requests the server has received,
// including scripted failures.
func (s *Server) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// Requests returns every well-formed request served, in arrival order.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	if s.APIKey != "" && r.Header.Get("Authorization") != "Bearer "+s.APIKey {
		writeError(w, http.StatusUnauthorized, "invalid API key")
		return
	}
	s.mu.Lock()
	s.calls++
	var fail int
	if len(s.Failures) > 0 {
		fail, s.Failures = s.Failures[0], s.Failures[1:]
	}
	s.mu.Unlock()
	if s.Delay > 0 {
		time.Sleep(s.Delay)
	}
	if fail != 0 {
		writeError(w, fail, "scripted failure")
		return
	}

	var body typesafe.WireRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Model == "" {
		writeError(w, http.StatusBadRequest, "model is required")
		return
	}
	qs, err := typesafe.FromWire(body.Questions)
	if err == nil {
		err = qs.Validate()
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	state, ok := body.State.(string)
	if !ok {
		b, _ := json.Marshal(body.State)
		state = string(b)
	}
	s.mu.Lock()
	s.requests = append(s.requests, Request{Model: body.Model, State: state, Questions: qs})
	s.mu.Unlock()

	judge := s.Judge
	if judge == nil {
		judge = defaultJudge
	}
	answers, err := judge(state, qs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	served := s.Served
	if served == "" {
		served = "jev-1.13.0-test"
	}
	resp := typesafe.WireResponse{Model: served, Answers: answers}
	// Input is what is billed; count it the way every offline provider in
	// Loom does, plus the preamble the real API bills on every call, so a
	// projection and a run agree.
	resp.Usage.InputTokens = typesafe.RequestOverheadTokens + model.EstimateTokens(state)
	for _, id := range qs.IDs() {
		resp.Usage.InputTokens += model.EstimateTokens(qs[id].Instructions)
	}
	resp.Usage.OutputTokens = 6 * len(answers)
	writeJSON(w, http.StatusOK, resp)
}

func defaultJudge(_ string, qs model.Questions) (map[string]model.Answer, error) {
	out := make(map[string]model.Answer, len(qs))
	for id, q := range qs {
		switch q.Type {
		case model.QuestionNoul:
			out[id] = Noul(0.9)
		case model.QuestionChoice:
			w := map[string]float64{}
			for i, name := range q.OptionNames() {
				if i == 0 {
					w[name] = 10
				} else {
					w[name] = 0.2
				}
			}
			out[id] = Choice(w)
		case model.QuestionScore:
			w := make([]float64, len(q.Levels))
			w[0] = 10
			for i := 1; i < len(w); i++ {
				w[i] = 0.2
			}
			out[id] = Score(q.Levels, w)
		}
	}
	return out, nil
}

// Noul answers a yes/no question with probability p of yes (clamped to 0..1).
func Noul(p float64) model.Answer {
	return model.Answer{Type: model.QuestionNoul, Noul: round(math.Min(1, math.Max(0, p)))}
}

// Choice answers a pick-one question from unnormalized weights per option:
// the highest-weighted option is chosen, and the confidence is one minus the
// distribution's normalized entropy.
func Choice(weights map[string]float64) model.Answer {
	names := make([]string, 0, len(weights))
	for n := range weights {
		names = append(names, n)
	}
	sort.Strings(names)
	ps := normalize(names, weights)
	best := ""
	for _, n := range names {
		if best == "" || ps[n] > ps[best] {
			best = n
		}
	}
	return model.Answer{
		Type:          model.QuestionChoice,
		Choice:        best,
		Probabilities: ps,
		Confidence:    confidence(ps),
	}
}

// Score answers a rubric question from unnormalized weights per level: the
// score is the probability-weighted level index.
func Score(levels []string, weights []float64) model.Answer {
	keys := make([]string, len(levels))
	byKey := make(map[string]float64, len(levels))
	legend := make(map[string]string, len(levels))
	for i, lvl := range levels {
		k := strconv.Itoa(i)
		keys[i] = k
		legend[k] = lvl
		if i < len(weights) {
			byKey[k] = weights[i]
		}
	}
	ps := normalize(keys, byKey)
	score := 0.0
	for i, k := range keys {
		score += float64(i) * ps[k]
	}
	return model.Answer{
		Type:          model.QuestionScore,
		Score:         round(score),
		Legend:        legend,
		Probabilities: ps,
		Confidence:    confidence(ps),
	}
}

func normalize(keys []string, w map[string]float64) map[string]float64 {
	total := 0.0
	for _, k := range keys {
		total += math.Max(0, w[k])
	}
	out := make(map[string]float64, len(keys))
	for _, k := range keys {
		if total == 0 {
			out[k] = round(1 / float64(len(keys)))
		} else {
			out[k] = round(math.Max(0, w[k]) / total)
		}
	}
	return out
}

// confidence is one minus normalized entropy: 1 for a certain answer, 0 for a
// uniform distribution.
func confidence(ps map[string]float64) float64 {
	if len(ps) < 2 {
		return 1
	}
	h := 0.0
	for _, p := range ps {
		if p > 0 {
			h -= p * math.Log(p)
		}
	}
	return round(math.Max(0, 1-h/math.Log(float64(len(ps)))))
}

func round(v float64) float64 { return math.Round(v*1000) / 1000 }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{
		"code": status, "message": fmt.Sprint(msg),
	}})
}
