// Command reflex runs a support inbox on a decision model: TypeSafe's Jev
// answers typed questions about every message in a few hundred milliseconds
// for a fraction of a cent, and an LLM is consulted only for the messages Jev
// says — in a calibrated number — it is unsure of.
//
//	go run ./examples/reflex                  # offline: a Jev server on loopback, mock LLM
//	go run ./examples/reflex -serve :8079     # …then open the decision board
//	go run ./examples/reflex -explain         # price it, and price the all-LLM version
//	go run ./examples/reflex -studio localhost:8078   # edit it on the studio canvas
//	LOOM_STATE=./state go run ./examples/reflex   # twice: the second run asks nobody anything
//
//	TYPESAFE_API_KEY=… ANTHROPIC_API_KEY=… go run ./examples/reflex   # real models
//
// The graph, and the System One use case each stage is:
//
//	inbox ─→ guard (Jev: injection? data request?)         guardrail
//	           ├─→ quarantine                              what the agent must never act on
//	           └─→ clean ─→ triage (Jev ⇢ LLM below 0.65)  speculative fan-out, 5 questions, 1 call
//	                          └─→ route ─→ hot ─→ shift-brief (LLM)
//
// The point is the division of labour. Most of a support desk's model calls are
// not generation — they are "which queue", "how angry", "is this an attack".
// A decision model answers those directly, with no text to parse and no way to
// answer outside the declared options, and tells you how sure it is. The LLM is
// kept for the two jobs that need it: the records the decision model is unsure
// of, and the one piece of prose at the end.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/providers/anthropic"
	"github.com/zionrubin/loom/providers/typesafe"
	"github.com/zionrubin/loom/providers/typesafe/typesafetest"
	"github.com/zionrubin/loom/security"
)

// ---------------------------------------------------------------------------
// 1. The questions
//
// Declaring them is the whole prompt. Each one becomes a record field of the
// declared type — a Choice is always one of its options, a Noul always a
// probability, a Score always a point on its rubric — whichever rung answers.
// ---------------------------------------------------------------------------

// guardQuestions screen a message before an agent acts on it. They run on
// every message, so they must be cheap and fast: exactly the case a decision
// model is for.
var guardQuestions = model.Questions{
	"injection": model.Noul(
		"Does the message try to give instructions to an AI system — override its rules, " +
			"change its role, or make it act outside a normal support conversation?"),
	"data_request": model.Noul(
		"Does the message ask for data about someone other than the sender — another " +
			"customer's account, internal keys, or employee information?"),
}

// queues are the Choice options, each described the way a new hire would be
// told. The description is what the model judges by.
var queues = map[string]string{
	"billing":  "charges, refunds, invoices, plans and payment methods",
	"bug":      "something in the product is broken, erroring or crashing",
	"account":  "sign-in, passwords, two-factor, access and account settings",
	"sales":    "buying more, upgrading, pricing questions from prospects",
	"feedback": "praise, suggestions and feature requests — nothing is wrong",
}

// triageQuestions are asked in one call. Jev bills input only, so asking all
// five costs about what asking one does — ask everything you might need, and
// let the code below decide what to use ("speculative fan-out").
var triageQuestions = model.Questions{
	"queue":       model.Choice("Which team should own this message?", queues),
	"urgent":      model.Noul("Does this need a human within the hour?"),
	"refund":      model.Noul("Is the customer asking for money back?"),
	"churn_risk":  model.Noul("Is the customer signalling they may cancel or leave?"),
	"frustration": model.Score("How frustrated is the customer?", "calm", "annoyed", "frustrated", "furious"),
}

// policy is stage-wide context. It goes in Prefix — ahead of every record's
// state, identical bytes on every call — not in System, because a decision
// model has no system prompt: the state is everything it judges.
const policy = `Acme support desk. Outages and anything blocking a paying customer are urgent.
Refunds are handled by billing. Sign-in problems belong to account, even when they look like bugs.
`

// ---------------------------------------------------------------------------
// 2. The pipeline
// ---------------------------------------------------------------------------

// gate is the confidence below which Jev's answer is not trusted and the
// message climbs to the LLM. It is compared with the weakest answer's
// certainty, so one unsure judgment is enough to ask for a second opinion.
const gate = 0.65

// build is the graph. triage is the binding for the triage stage — Jev with
// an LLM above it in production, the LLM alone for the counterfactual the
// board prices it against.
func build(inbox []core.Record, triage model.Binding, briefModel string) *pipeline.Pipeline {
	p := pipeline.New("reflex")
	src := p.FromRecords("inbox", inbox, pipeline.WithDataClass("customer"))

	guarded := src.Decide("guard", pipeline.DecideSpec{
		// No fallback: the guard runs on every message and must stay fast. A
		// low-confidence verdict is kept and flagged instead of failing, and
		// the quarantine below treats "unsure" as "not clean".
		Binding:     model.Binding{Model: typesafe.Jev},
		State:       "From: {{.from}}\nSubject: {{.subject}}\n\n{{.body}}",
		Questions:   guardQuestions,
		OutputField: "guard",
	})

	guarded.Filter("quarantine", func(r core.Record) (bool, error) {
		return !clean(r), nil
	}, pipeline.WithVersion("v1"))

	routed := guarded.
		Filter("clean", func(r core.Record) (bool, error) {
			return clean(r), nil
		}, pipeline.WithVersion("v1")).
		Decide("triage", pipeline.DecideSpec{
			Binding:       triage,
			System:        "You are a senior support lead triaging the inbox.",
			Prefix:        policy,
			State:         "Customer plan: {{.plan}}\nSubject: {{.subject}}\n\n{{.body}}",
			Questions:     triageQuestions,
			MinConfidence: gate,
		}).
		Map("route", route, pipeline.WithVersion("v1"))

	// A free fold, no model: how many messages each lane got. It is also the
	// route stage's second consumer, which keeps route from fusing into the
	// filter below — so its output, every triaged message with its lane, is
	// there to read when the run is done.
	routed.Combine("lanes", func(a, b core.Record) (core.Record, error) {
		n := laneCounts(a)
		for lane, c := range laneCounts(b) {
			n[lane] += c
		}
		return core.NewRecord("lanes", map[string]any{"lanes": n}), nil
	})

	routed.
		Filter("hot", func(r core.Record) (bool, error) {
			return r.String("lane") == "page-oncall", nil
		}, pipeline.WithVersion("v1")).
		ReduceAI("shift-brief", pipeline.ReduceAISpec{
			Binding:   model.Binding{Model: briefModel},
			System:    "You brief the on-call support lead. Be concrete and short.",
			Prompt:    "{{.Count}} messages need a human within the hour:\n{{range .Items}}- {{.}}\n{{end}}\nWrite a three-sentence brief: what is happening, who is affected, what to do first.",
			ItemField: "line",
			MaxTokens: 300,
		})
	return p
}

// clean reads the guard's typed answers. Plain field reads: the decision
// wrote a probability into each question's field.
func clean(r core.Record) bool {
	inj, _ := r.Data["injection"].(float64)
	data, _ := r.Data["data_request"].(float64)
	g, _ := r.Data["guard"].(map[string]any)
	return inj < 0.5 && data < 0.5 && g["low_confidence"] != true
}

// route turns typed answers into a lane. This is ordinary code branching on
// ordinary values — the part a free-text answer would have made a parser.
func route(r core.Record) (core.Record, error) {
	num := func(k string) float64 { v, _ := r.Data[k].(float64); return v }
	lane := r.String("queue")
	switch {
	case num("urgent") >= 0.7:
		lane = "page-oncall"
	case num("churn_risk") >= 0.6:
		lane = "retention"
	case num("refund") >= 0.6:
		lane = "refunds"
	}
	r.Data["lane"] = lane
	r.Data["line"] = fmt.Sprintf("[%s, %s plan, frustration %.1f/3] %s | %s",
		r.String("queue"), r.String("plan"), num("frustration"), r.String("subject"), r.String("body"))
	return r, nil
}

// laneCounts reads a lane tally from a folded record, or starts one from a
// routed message.
func laneCounts(r core.Record) map[string]float64 {
	out := map[string]float64{}
	if m, ok := r.Data["lanes"].(map[string]float64); ok {
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	if m, ok := r.Data["lanes"].(map[string]any); ok { // after a trip through JSON
		for k, v := range m {
			f, _ := v.(float64)
			out[k] = f
		}
		return out
	}
	out[r.String("lane")] = 1
	return out
}

// ---------------------------------------------------------------------------
// 3. The models
// ---------------------------------------------------------------------------

// setup is the one function that changes between offline and production.
type setup struct {
	reg      *model.Registry
	secrets  map[security.SecretRef]string
	fallback string // the triage escalation rung
	brief    string // the shift-brief model
	mode     string
	stop     func()
	// Offline only: the Jev server and the mock LLM, for counting calls.
	jev *typesafetest.Server
	llm *model.Mock
}

func models(realistic bool) (*setup, error) {
	s := &setup{reg: model.NewRegistry(), secrets: map[security.SecretRef]string{}, stop: func() {}}

	if key := os.Getenv("TYPESAFE_API_KEY"); key != "" {
		if err := typesafe.RegisterDefaults(s.reg, model.Limits{RequestsPerMinute: 600}); err != nil {
			return nil, err
		}
		s.secrets[typesafe.DefaultSecretRef] = key
		s.mode = "Jev (api.typesafe.ai)"
	} else {
		// A System One server on loopback, speaking the real wire protocol, so
		// the provider's HTTP path, error handling and egress rule all run.
		srv := &typesafetest.Server{APIKey: "offline", Judge: offlineJev, Served: "jev-offline"}
		if realistic {
			srv.Delay = 15 * time.Millisecond
		}
		url, stop := srv.Start()
		s.stop, s.jev = stop, srv
		p := typesafe.New(typesafe.WithBaseURL(url))
		if err := typesafe.Register(s.reg, p, typesafe.Jev, model.Limits{}); err != nil {
			return nil, err
		}
		s.secrets[typesafe.DefaultSecretRef] = "offline"
		s.mode = "Jev (offline server on " + p.Endpoint() + ")"
	}

	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		if err := anthropic.RegisterDefaults(s.reg, model.Limits{RequestsPerMinute: 50}); err != nil {
			return nil, err
		}
		s.secrets[anthropic.DefaultSecretRef] = key
		s.fallback, s.brief = "claude-sonnet-5", "claude-opus-4-8"
		s.mode += " + Claude"
		return s, nil
	}
	var opts []model.MockOption
	if realistic {
		opts = append(opts, model.WithLatency(400*time.Millisecond))
	}
	// Priced like the model it stands in for, so the board's dollar figures
	// compare what they would in production.
	llm := model.NewMock("mock-llm", append(opts, model.WithHandler(offlineLLM))...)
	s.llm = llm
	for _, m := range []struct {
		id      string
		pricing model.Pricing
	}{
		{"mock-sonnet", model.Pricing{InputPerMTok: 3, OutputPerMTok: 15}},
		{"mock-opus", model.Pricing{InputPerMTok: 5, OutputPerMTok: 25}},
	} {
		if err := s.reg.Register(model.Info{ID: m.id, Provider: llm, Pricing: m.pricing}); err != nil {
			return nil, err
		}
	}
	s.fallback, s.brief = "mock-sonnet", "mock-opus"
	s.mode += " + mock LLM"
	return s, nil
}

func (s *setup) opts() []loom.Option {
	return []loom.Option{loom.WithRegistry(s.reg), loom.WithSecrets(s.secrets)}
}

// ---------------------------------------------------------------------------
// 4. Running it
// ---------------------------------------------------------------------------

func main() {
	explainOnly := flag.Bool("explain", false, "price the pipeline and the all-LLM alternative without running")
	serve := flag.String("serve", "", "after the run, serve the decision board on this address (e.g. :8079)")
	studioAddr := flag.String("studio", "", "instead of running, open the pipeline in Loom Studio on this address (e.g. localhost:8078)")
	budget := flag.Float64("budget", 1.0, "hard USD ceiling for the run")
	flag.Parse()

	s, err := models(true)
	if err != nil {
		log.Fatal(err)
	}
	defer s.stop()
	fmt.Printf("models: %s\n\n", s.mode)
	if *studioAddr != "" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		if err := openStudio(ctx, s, *studioAddr); err != nil {
			log.Fatal(err)
		}
		return
	}

	jevFirst := model.Binding{Model: typesafe.Jev, Escalation: []string{s.fallback}}
	p := build(inbox(), jevFirst, s.brief)
	opts := append(s.opts(),
		loom.WithRunBudget(core.Budget{MaxCostUSD: *budget}),
		loom.WithContinueOnError(),
		// A decision declares its fields, so every template below it renders
		// in the projection; what no plan can know is the answers. Name the
		// ones that make the most downstream work so the ceiling stays a
		// bound: every message clean, every message urgent enough to reach
		// the shift brief.
		loom.WithStageSample("guard", map[string]any{"injection": 0.0, "data_request": 0.0}),
		loom.WithStageSample("triage", map[string]any{
			"queue": "bug", "urgent": 1.0, "refund": 1.0, "churn_risk": 1.0, "frustration": 3.0,
		}),
	)
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		opts = append(opts, loom.WithStateDir(dir))
	}

	proj, err := loom.Explain(p, opts...)
	if err != nil {
		log.Fatalf("explain: %v", err)
	}
	fmt.Print(proj.String())
	llmOnly, err := loom.Explain(build(inbox(), model.Binding{Model: s.fallback}, s.brief), opts...)
	if err != nil {
		log.Fatalf("explain all-LLM: %v", err)
	}
	fmt.Printf("\nthe same triage on %s alone would project $%.4f; on Jev first, $%.4f before escalations\n\n",
		s.fallback, stageCost(llmOnly, "triage"), stageCost(proj, "triage"))
	if *explainOnly {
		return
	}
	if !proj.FitsBudget() {
		log.Fatalf("projected ceiling $%.4f exceeds the $%.2f budget", proj.Ceiling().CostUSD, *budget)
	}

	calls := &ledger{}
	start := time.Now()
	res, err := loom.Run(context.Background(), p, append(opts, loom.WithEventHandler(calls.observe))...)
	if err != nil {
		log.Fatalf("run: %v (spent $%.4f)", err, res.Spent.CostUSD)
	}
	b := newBoard(res, calls, stageCost(llmOnly, "triage"), s, time.Since(start))
	fmt.Print(b.String())

	if *serve != "" {
		if err := b.serve(*serve); err != nil {
			log.Fatal(err)
		}
	}
}

func stageCost(p *loom.Projection, stage string) float64 {
	for _, s := range p.Stages {
		if s.Stage == stage {
			return s.Usage.CostUSD
		}
	}
	return 0
}

// ledger tallies every model call by model — count, dollars, latency — and
// keeps the first answer each triage task got, so the board can show what Jev
// said about a message before the gate sent it up the ladder. Those answers
// are in the telemetry stream, not the record: a record holds the decision
// that was accepted, and an escalated record's first decision was not.
type ledger struct {
	mu      sync.Mutex
	byModel map[string]*modelTally
	first   map[string]string   // triage task ID → first response (answers JSON)
	outputs map[string][]string // task ID → record IDs it produced
}

type modelTally struct {
	Model     string          `json:"model"`
	Calls     int             `json:"calls"`
	CostUSD   float64         `json:"cost_usd"`
	Latencies []time.Duration `json:"-"`
	P50ms     float64         `json:"p50_ms"`
}

func (l *ledger) observe(e observe.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.byModel == nil {
		l.byModel, l.first, l.outputs = map[string]*modelTally{}, map[string]string{}, map[string][]string{}
	}
	if e.Type == observe.TaskCompleted && e.Stage == "triage" {
		l.outputs[e.TaskID] = e.OutIDs
	}
	if e.Type != observe.ModelCalled || e.Err != "" {
		return
	}
	if _, seen := l.first[e.TaskID]; !seen && e.Stage == "triage" {
		l.first[e.TaskID] = e.Response
	}
	t := l.byModel[e.Model]
	if t == nil {
		t = &modelTally{Model: e.Model}
		l.byModel[e.Model] = t
	}
	t.Calls++
	t.CostUSD += e.Usage.CostUSD
	t.Latencies = append(t.Latencies, e.Latency)
}

// firstAnswers returns, per record ID, the answers its triage task's first
// call returned.
func (l *ledger) firstAnswers() map[string]model.Decision {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]model.Decision{}
	for task, ids := range l.outputs {
		var answers map[string]model.Answer
		if json.Unmarshal([]byte(l.first[task]), &answers) != nil {
			continue
		}
		for _, id := range ids {
			out[id] = model.Decision{Answers: answers, Calibrated: true}
		}
	}
	return out
}

func (l *ledger) tallies() []*modelTally {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*modelTally, 0, len(l.byModel))
	for _, t := range l.byModel {
		lat := append([]time.Duration(nil), t.Latencies...)
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		if len(lat) > 0 {
			t.P50ms = float64(lat[len(lat)/2].Microseconds()) / 1000
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

// ---------------------------------------------------------------------------
// 5. The decision board
//
// Everything the UI shows is read off the run result: the typed answers and
// their distributions from each record's decision field, the lanes from the
// route stage, the money from the call ledger.
// ---------------------------------------------------------------------------

//go:embed board.html
var boardHTML []byte

type board struct {
	Mode     string        `json:"mode"`
	Gate     float64       `json:"gate"`
	Took     string        `json:"took"`
	Models   []*modelTally `json:"models"`
	SpentUSD float64       `json:"spent_usd"`
	// LLMOnlyUSD is what the projection says the triage stage alone would
	// cost with the LLM answering every message.
	LLMOnlyUSD float64 `json:"llm_only_usd"`
	TriageUSD  float64 `json:"triage_usd"`
	Escalated  int     `json:"escalated"`
	Triaged    int     `json:"triaged"`
	Questions  struct {
		Guard  model.Questions `json:"guard"`
		Triage model.Questions `json:"triage"`
	} `json:"questions"`
	Messages []message          `json:"messages"`
	Lanes    map[string]float64 `json:"lanes"`
	Brief    string             `json:"brief"`
}

type message struct {
	ID      string         `json:"id"`
	From    string         `json:"from"`
	Plan    string         `json:"plan"`
	Subject string         `json:"subject"`
	Body    string         `json:"body"`
	Status  string         `json:"status"` // "quarantined" or "triaged"
	Lane    string         `json:"lane,omitempty"`
	Guard   map[string]any `json:"guard"`
	Triage  map[string]any `json:"triage,omitempty"`
	// First is what the bottom rung answered, for a message the gate sent
	// up the ladder.
	First *firstTry `json:"first,omitempty"`
}

type firstTry struct {
	Answers    map[string]model.Answer `json:"answers"`
	Confidence float64                 `json:"confidence"`
	Weakest    string                  `json:"weakest"`
}

func newBoard(res *loom.RunResult, calls *ledger, llmOnly float64, s *setup, took time.Duration) *board {
	b := &board{Mode: s.mode, Gate: gate, Took: took.Round(time.Millisecond).String(),
		Models: calls.tallies(), SpentUSD: res.Report.Totals().CostUSD, LLMOnlyUSD: llmOnly}
	b.Questions.Guard, b.Questions.Triage = guardQuestions, triageQuestions
	for _, st := range res.Report.Stages {
		if st.Stage == "triage" {
			b.TriageUSD = st.Usage.CostUSD
		}
	}
	first := calls.firstAnswers()
	add := func(r core.Record, status string) {
		m := message{ID: r.ID, From: r.String("from"), Plan: r.String("plan"),
			Subject: r.String("subject"), Body: r.String("body"), Status: status,
			Lane: r.String("lane")}
		m.Guard, _ = r.Data["guard"].(map[string]any)
		if t, ok := r.Data["decision"].(map[string]any); ok {
			m.Triage = t
			b.Triaged++
			if cal, _ := t["calibrated"].(bool); !cal {
				b.Escalated++
				if d, ok := first[r.ID]; ok {
					m.First = &firstTry{Answers: d.Answers, Confidence: d.Confidence(), Weakest: d.Weakest()}
				}
			}
		}
		b.Messages = append(b.Messages, m)
	}
	for _, r := range res.StageOutputs["route"] {
		add(r, "triaged")
	}
	for _, r := range res.StageOutputs["quarantine"] {
		add(r, "quarantined")
	}
	sort.Slice(b.Messages, func(i, j int) bool { return b.Messages[i].ID < b.Messages[j].ID })
	if out := res.StageOutputs["lanes"]; len(out) > 0 {
		b.Lanes = laneCounts(out[0])
	}
	if out := res.StageOutputs["shift-brief"]; len(out) > 0 {
		b.Brief = out[0].String("output")
	}
	return b
}

func (b *board) String() string {
	var w strings.Builder
	fmt.Fprintf(&w, "\n%-4s %-12s %-10s %6s %6s %6s  %-26s %s\n",
		"id", "lane", "queue", "urgent", "refund", "angry", "answered by", "subject")
	for _, m := range b.Messages {
		if m.Status == "quarantined" {
			fmt.Fprintf(&w, "%-4s %-12s %-10s %6s %6s %6s  %-26s %s\n",
				m.ID, "QUARANTINE", "-", "-", "-", "-", fmt.Sprint(m.Guard["model"]), m.Subject)
			continue
		}
		who := fmt.Sprint(m.Triage["model"])
		if cal, _ := m.Triage["calibrated"].(bool); !cal {
			who = "↑ " + who
		} else {
			who = fmt.Sprintf("%s @%.2f", who, m.Triage["confidence"])
		}
		answers, _ := m.Triage["answers"].(map[string]any)
		val := func(id, k string) any {
			a, _ := answers[id].(map[string]any)
			return a[k]
		}
		num := func(v any) string { f, _ := v.(float64); return fmt.Sprintf("%.2f", f) }
		fmt.Fprintf(&w, "%-4s %-12s %-10s %6s %6s %6s  %-26s %s\n", m.ID, m.Lane,
			val("queue", "choice"), num(val("urgent", "noul")), num(val("refund", "noul")),
			num(val("frustration", "score")), who, m.Subject)
	}
	fmt.Fprintf(&w, "\n%d triaged, %d escalated below confidence %.2f, %d quarantined by the guard\n",
		b.Triaged, b.Escalated, b.Gate, len(b.Messages)-b.Triaged)
	for _, t := range b.Models {
		fmt.Fprintf(&w, "  %-24s %3d calls  $%.5f  p50 %6.1f ms\n", t.Model, t.Calls, t.CostUSD, t.P50ms)
	}
	fmt.Fprintf(&w, "triage cost $%.5f against $%.5f projected for the LLM alone; run total $%.5f in %s\n",
		b.TriageUSD, b.LLMOnlyUSD, b.SpentUSD, b.Took)
	if b.Brief != "" {
		fmt.Fprintf(&w, "\nshift brief:\n%s\n", b.Brief)
	}
	return w.String()
}

// handler serves the board page and its data.
func (b *board) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(boardHTML)
	})
	mux.HandleFunc("/api/board", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(b)
	})
	return mux
}

func (b *board) serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Printf("\ndecision board: http://%s  (ctrl-c to stop)\n", displayAddr(ln.Addr()))
	return http.Serve(ln, b.handler())
}

func displayAddr(a net.Addr) string {
	host, port, _ := net.SplitHostPort(a.String())
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = "localhost"
	}
	return net.JoinHostPort(host, port)
}
