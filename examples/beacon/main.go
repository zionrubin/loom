// Command beacon runs a pipeline with telemetry attached and then shows what
// a monitoring system would have seen.
//
//	go run ./examples/beacon              # run, scrape, print the trace
//	go run ./examples/beacon -serve       # leave the ops endpoint up to curl
//	go run ./examples/beacon -otlp URL    # send spans to a real collector
//
// Loom already observes itself: an event bus, a run report, a constellation
// view. All of it is in-process and all of it ends when the process does,
// which is the right shape for building a pipeline and the wrong one for
// running a deployment. This example is the other half — the boring half, the
// one a platform team needs before it will put anything on call.
//
// The run itself is small and deliberately imperfect: four support tickets
// through a stage bound to a cheap model with an escalation ladder, and a
// validator that rejects what the cheap model returns for two of them. So the
// run climbs, pays twice for those records, and finishes inside its budget.
// That is the ordinary shape of an AI workload and it is exactly what the
// exposition has to be able to describe.
//
// What to look for:
//
//   - **the unit is the dollar.** A classic framework's metrics are records
//     per second, because cores are what it runs out of. The first families
//     here are cost, tokens and wallet headroom, because those are what a run
//     runs out of. `loom_budget_headroom_usd` is the series to page on.
//   - **cost is counted at the call, not at the task.** The two records that
//     escalated were billed twice, and the counters say so. A cost metric fed
//     from task completions would report the run as cheaper than the invoice.
//   - **the run ID is the trace ID.** The trace printed at the bottom is found
//     by pasting the run ID from the report into a tracing backend. Nothing
//     had to be propagated, logged or correlated: the identifier is derived.
//   - **sampling keeps what somebody would open.** The ratio here is zero, and
//     the two escalated tasks are traced anyway — the decision is made when a
//     task settles, so it can see the failure a head sampler could not.
//   - **the collector is real.** This program stands up an OTLP/HTTP endpoint
//     on loopback and posts to it over the wire, so what is printed came back
//     off a socket in the format a collector accepts, not out of a mock.
//
// It runs entirely offline against mock models, with no keys and no network.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/telemetry"
)

func main() {
	serve := flag.Bool("serve", false, "leave the ops endpoint running until interrupted")
	addr := flag.String("addr", "127.0.0.1:9464", "address for /metrics, /healthz and /readyz")
	otlp := flag.String("otlp", "", "post spans to this OTLP/HTTP collector instead of the built-in one")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// The collector. A real one is an address in another process; this is the
	// same thing on loopback, so the example exercises the encoding rather
	// than describing it.
	sink := &spanSink{}
	endpoint := *otlp
	if endpoint == "" {
		var shutdown func()
		endpoint, shutdown = sink.listen()
		defer shutdown()
	}

	tel := telemetry.New(telemetry.Options{
		Service:  "beacon",
		Instance: "beacon-1",
		Attrs:    map[string]string{"deployment.environment": "example"},
		Endpoint: endpoint,
		// Zero ordinary tasks, and every task that retried, escalated or
		// failed. On a run of four records that is a curiosity; on a run of
		// four hundred thousand it is the difference between a trace bill and
		// a trace.
		Sample: telemetry.Sampling{Ratio: 0, Slow: 5 * time.Second},
	})

	// The ops surface. A worker, a stream job or a serving desk starts this
	// once and is operable for the rest of its life.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("ops endpoint: %v", err)
	}
	srv := &http.Server{Handler: tel.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	reg, err := registry()
	if err != nil {
		log.Fatal(err)
	}
	// A readiness check that is about this process's dependencies, not about
	// whether it is alive: the two are different questions and a load
	// balancer asks the second one.
	tel.Ready("model-registry", func(context.Context) error {
		if len(reg.All()) == 0 {
			return fmt.Errorf("no models registered")
		}
		return nil
	})

	budget := core.Budget{MaxCostUSD: 0.50}
	res, err := loom.Run(ctx, triage(),
		loom.WithRegistry(reg),
		loom.WithRunBudget(budget),
		loom.WithTelemetry(tel))
	if err != nil {
		log.Fatal(err)
	}
	if err := tel.Flush(ctx); err != nil {
		log.Printf("flush: %v", err)
	}

	fmt.Printf("\n── the run ─────────────────────────────────────────────\n")
	fmt.Printf("   run %s\n", res.RunID)
	fmt.Printf("   %d tickets classified, %d model calls, $%.4f of a $%.2f budget\n",
		len(res.StageOutputs["classify"]), res.Spent.Requests, res.Spent.CostUSD, budget.MaxCostUSD)

	base := "http://" + ln.Addr().String()
	fmt.Printf("\n── the scrape (%s/metrics) ───────────\n", base)
	scrape, err := fetch(ctx, base+"/metrics")
	if err != nil {
		log.Fatal(err)
	}
	printScrape(scrape)

	fmt.Printf("\n── liveness and readiness ──────────────────────────────\n")
	for _, path := range []string{"/healthz", "/readyz"} {
		body, err := fetch(ctx, base+path)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   GET %-9s → %s", path, body)
	}

	if *otlp == "" {
		fmt.Printf("\n── the trace, off the wire (trace %s) ──\n",
			telemetry.TraceIDFor(res.RunID)[:16]+"…")
		sink.printTree()
	} else {
		fmt.Printf("\n   spans posted to %s, trace %s\n", *otlp, telemetry.TraceIDFor(res.RunID))
	}

	fmt.Printf(`
── what to put on call ─────────────────────────────────
   the wallet     loom_budget_headroom_usd < 1
   the burn rate  rate(loom_cost_usd_total[5m]) * 3600
   a ladder that  rate(loom_escalations_total[15m])
     started      / rate(loom_tasks_total{outcome="completed"}[15m])
     climbing
   throttling     histogram_quantile(0.95,
                    rate(loom_task_queue_seconds_bucket[5m]))
   replay value   rate(loom_cache_hits_total[1h])
`)

	if *serve {
		fmt.Printf("\n   ops endpoint still up on %s — ^C to stop\n", base)
		<-ctx.Done()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tel.Close(shutdown)
}

// triage is the work: four tickets, a cheap model with a rung above it, and a
// validator that rejects what the cheap model produces for the hard ones.
func triage() *pipeline.Pipeline {
	p := pipeline.New("beacon")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding: model.Binding{
				Tier:       model.TierFast,
				Escalation: []string{"mock-deep"},
			},
			System:    "You classify support tickets.",
			Prompt:    "Classify this ticket. subject: {{.subject}}\nbody: {{.body}}",
			ParseJSON: true,
			Validate: func(r core.Record) error {
				if r.String("category") == "" {
					return fmt.Errorf("no category")
				}
				return nil
			},
		})
	return p
}

func tickets() []core.Record {
	return []core.Record{
		core.NewRecord("t1", map[string]any{
			"subject": "refund not processed", "body": "charged twice last week"}),
		core.NewRecord("t2", map[string]any{
			"subject": "login loop", "body": "the app returns to the login screen"}),
		core.NewRecord("t3", map[string]any{
			"subject": "escalate: contract terms", "body": "our legal team disputes clause 4"}),
		core.NewRecord("t4", map[string]any{
			"subject": "escalate: data residency", "body": "where is this stored"}),
	}
}

func registry() (*model.Registry, error) {
	reg := model.NewRegistry()

	// The cheap model answers the ordinary tickets and returns nothing usable
	// for the two marked "escalate:", which is what the validator is for.
	fast := model.NewMock("mock-fast", model.WithLatency(4*time.Millisecond),
		model.WithHandler(func(req model.Request) (string, error) {
			if strings.Contains(req.Prompt, "escalate:") {
				return `{"category": "", "urgent": false}`, nil
			}
			return `{"category": "billing", "urgent": true}`, nil
		}))
	if err := reg.Register(model.Info{
		ID: "mock-fast", Provider: fast, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 0.80, OutputPerMTok: 4},
		Limits:  model.Limits{RequestsPerMinute: 100000},
	}); err != nil {
		return nil, err
	}

	deep := model.NewMock("mock-deep", model.WithLatency(12*time.Millisecond),
		model.WithHandler(func(model.Request) (string, error) {
			return `{"category": "legal", "urgent": true}`, nil
		}))
	if err := reg.Register(model.Info{
		ID: "mock-deep", Provider: deep, Tier: model.TierDeep,
		Pricing: model.Pricing{InputPerMTok: 3, OutputPerMTok: 15},
		Limits:  model.Limits{RequestsPerMinute: 100000},
	}); err != nil {
		return nil, err
	}
	return reg, nil
}

// --- printing -----------------------------------------------------------

// interesting is the handful of families worth putting on the terminal. The
// exposition holds several dozen; a scrape is for a scraper, and a demo that
// printed all of it would say nothing.
var interesting = []string{
	"loom_cost_usd_total",
	"loom_budget_headroom_usd",
	"loom_model_calls_total",
	"loom_tokens_total",
	"loom_tasks_total",
	"loom_escalations_total",
	"loom_task_retries_total",
	"loom_cache_hits_total",
}

func printScrape(body string) {
	want := map[string]bool{}
	for _, f := range interesting {
		want[f] = true
	}
	for _, line := range strings.Split(body, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, _, _ := strings.Cut(line, " ")
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		if want[name] {
			fmt.Printf("   %s\n", line)
		}
	}
	fmt.Printf("   … and %d more families a scraper would take\n", families(body)-len(interesting))
	fmt.Printf("\n   The wallet reads whole because the run has finished: headroom is\n" +
		"   what is left of the tightest ceiling *in flight*, and nothing is.\n")
}

func families(body string) int {
	n := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			n++
		}
	}
	return n
}

func fetch(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// --- a collector on loopback --------------------------------------------

// spanSink is an OTLP/HTTP endpoint: it accepts what the exporter posts and
// keeps it, so the spans printed below arrived over a socket in the encoding
// a real collector reads.
type spanSink struct {
	spans []wireSpan
}

type wireSpan struct {
	TraceID string `json:"traceId"`
	SpanID  string `json:"spanId"`
	Parent  string `json:"parentSpanId"`
	Name    string `json:"name"`
	Start   string `json:"startTimeUnixNano"`
	End     string `json:"endTimeUnixNano"`
	Attrs   []struct {
		Key   string `json:"key"`
		Value struct {
			String *string  `json:"stringValue"`
			Int    *string  `json:"intValue"`
			Double *float64 `json:"doubleValue"`
		} `json:"value"`
	} `json:"attributes"`
	Events []struct {
		Name string `json:"name"`
	} `json:"events"`
	Status struct {
		Code int `json:"code"`
	} `json:"status"`
}

func (s *spanSink) listen() (endpoint string, shutdown func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("collector: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/traces", func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			ResourceSpans []struct {
				ScopeSpans []struct {
					Spans []wireSpan `json:"spans"`
				} `json:"scopeSpans"`
			} `json:"resourceSpans"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, rs := range payload.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				s.spans = append(s.spans, ss.Spans...)
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return "http://" + ln.Addr().String(), func() { _ = srv.Close() }
}

// printTree renders what arrived as the tree a tracing backend would draw.
func (s *spanSink) printTree() {
	byParent := map[string][]wireSpan{}
	var roots []wireSpan
	for _, sp := range s.spans {
		if sp.Parent == "" {
			roots = append(roots, sp)
			continue
		}
		byParent[sp.Parent] = append(byParent[sp.Parent], sp)
	}
	if len(roots) == 0 {
		fmt.Println("   (no spans)")
		return
	}
	var walk func(sp wireSpan, depth int)
	walk = func(sp wireSpan, depth int) {
		line := fmt.Sprintf("   %s%-28s %8s  %s", strings.Repeat("  ", depth),
			sp.Name, duration(sp), annotate(sp))
		fmt.Println(strings.TrimRight(line, " "))
		kids := byParent[sp.SpanID]
		sort.Slice(kids, func(i, j int) bool {
			if kids[i].Start != kids[j].Start {
				return kids[i].Start < kids[j].Start
			}
			return kids[i].Name < kids[j].Name
		})
		for _, k := range kids {
			walk(k, depth+1)
		}
	}
	for _, r := range roots {
		walk(r, 0)
	}
	fmt.Printf("\n   %d spans. Two tasks answered on the first rung and were sampled\n"+
		"   away; the two that escalated were kept whatever the ratio said.\n", len(s.spans))
}

func duration(sp wireSpan) string {
	start, end := nanos(sp.Start), nanos(sp.End)
	if start == 0 || end < start {
		return ""
	}
	return time.Duration(end - start).Round(time.Millisecond).String()
}

func nanos(s string) int64 {
	var v int64
	_, _ = fmt.Sscanf(s, "%d", &v)
	return v
}

// annotate picks the attributes worth putting beside a span on a terminal:
// what it cost, and whether it went wrong.
func annotate(sp wireSpan) string {
	var parts []string
	for _, a := range sp.Attrs {
		switch a.Key {
		case "loom.cost_usd":
			if a.Value.Double != nil && *a.Value.Double > 0 {
				parts = append(parts, fmt.Sprintf("$%.4f", *a.Value.Double))
			}
		case "loom.attempts":
			if a.Value.Int != nil && *a.Value.Int != "1" {
				parts = append(parts, "attempts="+*a.Value.Int)
			}
		case "loom.ladder_rung":
			if a.Value.Int != nil {
				parts = append(parts, "rung="+*a.Value.Int)
			}
		case "loom.model":
			if a.Value.String != nil && strings.HasPrefix(sp.Name, "loom.task") {
				parts = append(parts, *a.Value.String)
			}
		}
	}
	for _, e := range sp.Events {
		parts = append(parts, "•"+e.Name)
	}
	return strings.Join(parts, " ")
}
