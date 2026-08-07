// Command on-device runs a Loom pipeline whose models are on this machine:
// two llama.cpp servers, no API key, no dollars, and no record that leaves
// the box.
//
//	go run ./examples/on-device                       # offline, no llama.cpp needed
//	go run ./examples/on-device -state /tmp/loom-dev  # then again: replayed free
//	go run ./examples/on-device -view localhost:8077  # watch it
//
//	# against real servers: llama-server -m small.gguf --port 8080 --parallel 2
//	#                       llama-server -m large.gguf --port 8081
//	go run ./examples/on-device -fast http://127.0.0.1:8080 -deep http://127.0.0.1:8081
//
// The pipeline is an on-call incident desk: reports arrive carrying customer
// names, addresses and account numbers, a small model triages every one of
// them, and a larger model writes the brief. Both models run here.
//
// Without a -fast or -deep address the example starts its own
// llama.cpp-compatible servers in-process, so it runs offline with nothing
// installed and no model file downloaded. The pipeline does not know the
// difference; a binding names a model, not a machine.
//
// Four things are on show, and each is a number at the end rather than a
// claim:
//
//   - Cost is zero, so the ceiling that binds is not the budget governor's
//     dollars but the device's decode width. llamacpp.Register discovers each
//     server's slot count and the scheduler admits exactly that many calls at
//     once — the servers report the peak they actually saw.
//   - Nothing egresses. The triage stage's envelope allows loopback and
//     nothing else, and the executor checks it before every call, so the
//     records above cannot reach a vendor even by mistake.
//   - No credential exists to leak. The stage is planned with model grants
//     and no secret grant at all, because a local server needs none.
//   - The shared rubric is served from the KV cache. Loom's prompt prefix and
//     llama.cpp's cache are the same mechanism here, and reuse costs nothing
//     to write, so it is asked for on every call.
//
// The escalation ladder is the ordinary one, with both rungs local: the small
// model triages, and an incident whose output fails validation is retried on
// the large model. Speculative decoding, one altitude up, on hardware you own.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/providers/llamacpp"
	"github.com/zionrubin/loom/providers/llamacpp/llamacpptest"
	"github.com/zionrubin/loom/task"
	"github.com/zionrubin/loom/viz"
)

func main() {
	fast := flag.String("fast", "", "base URL of the small model's llama.cpp server (default: one in-process)")
	deep := flag.String("deep", "", "base URL of the large model's llama.cpp server (default: one in-process)")
	state := flag.String("state", os.Getenv("LOOM_STATE"), "state directory for cache/resume")
	view := flag.String("view", "", "serve the constellation view on this address")
	slow := flag.Duration("slow", 0, "add latency to the in-process servers, to watch the run")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// --- Two servers, because a llama.cpp server is one loaded model. -----
	//
	// A deployment that wants a fast model and a strong one runs two of them
	// on two ports, which is exactly what makes an escalation ladder ordinary
	// here: two registrations, two IDs, and the binding below names both.
	fastURL, fastSrv, closeFast := server(*fast, triageModel(), 2, *slow)
	defer closeFast()
	deepURL, deepSrv, closeDeep := server(*deep, briefModel(), 1, *slow)
	defer closeDeep()

	// --- Registration asks the hardware what it can do. -------------------
	//
	// The slot count is not a number somebody typed into a config: it is what
	// the server answers when asked, and it becomes the model's
	// MaxConcurrent. Contacting the server here also means a server that is
	// not running fails while the pipeline is being wired rather than on the
	// first record.
	reg := model.NewRegistry()
	fastProps := register(ctx, reg, llamacpp.New(fastURL, llamacpp.WithName("llamacpp-fast")), "local-fast", model.TierFast)
	deepProps := register(ctx, reg, llamacpp.New(deepURL, llamacpp.WithName("llamacpp-deep")), "local-deep", model.TierDeep)

	fmt.Println("--- models on this machine ---")
	fmt.Printf("%-11s %-34s %d slot(s)  %5d ctx  %s\n", "local-fast", fastProps.Model, fastProps.Slots, fastProps.ContextSize, fastURL)
	fmt.Printf("%-11s %-34s %d slot(s)  %5d ctx  %s\n\n", "local-deep", deepProps.Model, deepProps.Slots, deepProps.ContextSize, deepURL)

	p := desk()

	opts := []loom.Option{
		loom.WithRegistry(reg),
		// More workers than the two servers have slots between them, on
		// purpose: the run is oversubscribed and admission control is what
		// keeps the device from being. Without it the excess would queue
		// inside llama-server, where Loom could neither see it nor schedule
		// around it.
		loom.WithWorkers(8),
		// A dollar ceiling that will never be reached, kept to make the point
		// that it is no longer the bound that matters.
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1.00}),
	}
	if *state != "" {
		opts = append(opts, loom.WithStateDir(*state))
	}

	// Which model actually answered each record. The ladder is invisible in
	// the output records — an escalated result looks exactly like a first-try
	// one, which is the point — so it is read off the event bus instead.
	answered := newLedger()
	handle := answered.Handle
	if *view != "" {
		v := viz.New()
		url, err := v.Start(*view)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("constellation view: %s\n\n", url)
		waitCtx, cancelWait := context.WithTimeout(ctx, 30*time.Second)
		if v.AwaitViewer(waitCtx) {
			time.Sleep(800 * time.Millisecond)
		}
		cancelWait()
		handle = func(e observe.Event) { answered.Handle(e); v.Handle(e) }
	}
	opts = append(opts, loom.WithEventHandler(handle))

	res, err := loom.Run(ctx, p, opts...)
	if err != nil {
		log.Fatalf("run: %v", err)
	}

	fmt.Println("--- triage ---")
	for _, r := range res.StageOutputs["triage"] {
		paging, _ := r.Data["paging"].(bool)
		line := fmt.Sprintf("%-6s %-5s %-12s paging=%-5v %-53s",
			r.ID, r.String("severity"), r.String("component"), paging, clip(r.String("text"), 52))
		if answered.model(r.ID) == "local-deep" {
			line += "  ← the small model would not place it; escalated"
		}
		fmt.Println(strings.TrimRight(line, " "))
	}

	fmt.Println("\n--- incident brief ---")
	for _, r := range res.Output {
		fmt.Println(strings.TrimSpace(r.String("output")))
	}

	fmt.Println("\n--- report ---")
	fmt.Print(res.Report)

	fmt.Println()
	summarize(res, reg, p, fastSrv, deepSrv, fastProps, deepProps)

	if *view != "" {
		fmt.Println("\nview still serving; press s for the summary; ctrl-c to exit")
		<-ctx.Done()
	}
}

// --- The pipeline ---------------------------------------------------------

// severityRubric is the stage-stable head of every triage prompt: the same
// bytes on all eight calls, which is what makes it worth keeping out of the
// per-record half.
//
// Against a hosted provider this is a prompt-cache prefix and the planner
// weighs a cache write against the calls that will read it. Against a
// llama.cpp server there is nothing to weigh: the KV cache is a byproduct of
// the forward pass the model was making anyway, so reuse is free to write and
// free to read, and the adapter asks for it every time.
const severityRubric = `Severity rubric, applied in order:

sev1 — money or data is moving wrongly, or not at all: failed payments,
       corrupted writes, an authentication path that admits the wrong person.
       Always pages.
sev2 — the product works but visibly badly for many people: elevated errors,
       latency past the published objective, a queue that is not draining.
       Pages during business hours.
sev3 — a bounded fault with a workaround, or one customer affected. Does not
       page; goes on the next standup.
sev4 — cosmetic, editorial, or a request dressed as an incident. Does not page.

Rules that override the ladder:
  - A report you cannot place with confidence is not a sev3. Say so.
  - "Customer says" is evidence, not severity. Weigh the described symptom.
  - Duplicate reports of one fault share its severity; do not inflate.
  - A component name is one lowercase word, drawn from the service named in
    the report.`

// incidents are the records, and the reason the pipeline is worth running
// locally at all: names, addresses, account numbers, and a customer's
// description of what they were doing when it broke.
var incidents = []core.Record{
	core.NewRecord("inc-1", map[string]any{
		"reporter": "dana.okoye@example.com", "service": "payments",
		"text": "Dana Okoye (acct 88121) has had every card declined since 09:40. Three retries, all 502 from the payments gateway. Her renewal fails tonight.",
	}),
	core.NewRecord("inc-2", map[string]any{
		"reporter": "ops@example.com", "service": "checkout",
		"text": "Checkout p95 is 4.2s against a 900ms objective, sustained for 40 minutes. Cart abandonment is up. No errors, just slow.",
	}),
	core.NewRecord("inc-3", map[string]any{
		"reporter": "marco.reis@example.com", "service": "search",
		"text": "Marco Reis reports the search box drops the last character he types on Firefox 141. Works in Chrome. He has been pasting instead.",
	}),
	core.NewRecord("inc-4", map[string]any{
		"reporter": "ops@example.com", "service": "payments",
		"text": "Second report of declines at the payments gateway, this time from acct 90441. Same 502, same window as the earlier one.",
	}),
	core.NewRecord("inc-5", map[string]any{
		"reporter": "priya.n@example.com", "service": "billing",
		"text": "The invoice footer still says 2025 and the VAT line is mislabelled. Priya Nair asked whether this affects what she owes; it does not.",
	}),
	core.NewRecord("inc-6", map[string]any{
		"reporter": "ops@example.com", "service": "auth",
		"text": "Our status page is green, the third-party gateway status page is amber, and our own dashboards disagree with both. Something is wrong with auth but I cannot say what.",
	}),
	core.NewRecord("inc-7", map[string]any{
		"reporter": "sam.whitlock@example.com", "service": "checkout",
		"text": "Sam Whitlock (acct 11902, 14 Bellhaven Row) says the address form rejects his postcode. He completed the order by phone. One customer, workaround exists.",
	}),
	core.NewRecord("inc-8", map[string]any{
		"reporter": "ops@example.com", "service": "search",
		"text": "Search error rate went from 0.2% to 6% after the 08:00 index rebuild and is holding there. Results still return, just wrong ones for 1 in 16 queries.",
	}),
}

// desk builds the pipeline: triage every incident, keep the pageable ones,
// write one brief.
func desk() *pipeline.Pipeline {
	p := pipeline.New("on-device-desk")
	src := p.FromRecords("incidents", incidents)

	triaged := src.Infer("triage", pipeline.InferSpec{
		// Both rungs are local. The small model answers; an incident whose
		// output fails the validator below is retried on the large one, which
		// is the same escalation a hosted ladder performs and costs the same
		// nothing here.
		Binding:   model.Binding{Tier: model.TierFast, Escalation: []string{"local-deep"}},
		System:    "You are an on-call triage assistant. Reply with a single JSON object and nothing else.",
		Prefix:    severityRubric,
		Prompt:    "Incident reported by {{.reporter}} against service {{.service}}:\n{{.text}}\n\nReply with JSON {\"severity\": \"sev1|sev2|sev3|sev4\", \"component\": \"<one lowercase word>\", \"paging\": true|false}.",
		MaxTokens: 120,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			switch r.String("severity") {
			case "sev1", "sev2", "sev3", "sev4":
				return nil
			}
			return fmt.Errorf("unusable severity %q", r.String("severity"))
		},
	})

	triaged.
		Filter("pageworthy", func(r core.Record) (bool, error) {
			paging, _ := r.Data["paging"].(bool)
			return paging, nil
		}).
		ReduceAI("brief", pipeline.ReduceAISpec{
			Binding:   model.Binding{Model: "local-deep"},
			System:    "You write the paragraph the on-call engineer reads first.",
			Prompt:    "Write one paragraph covering these {{.Count}} pageable incidents, leading with the one that costs money:\n{{range .Items}}- {{.}}\n{{end}}",
			FanIn:     4,
			MaxTokens: 300,
			ItemField: "text",
		})

	return p
}

// ledger records which model produced each record, from the event bus.
//
// Handlers run on worker goroutines, so it guards its map — the same care any
// observer of a concurrent run needs.
type ledger struct {
	mu sync.Mutex
	by map[string]string // record ID → model that produced it
}

func newLedger() *ledger { return &ledger{by: map[string]string{}} }

func (l *ledger) Handle(e observe.Event) {
	if e.Type != observe.TaskCompleted || e.Model == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, id := range e.OutIDs {
		l.by[id] = e.Model
	}
}

func (l *ledger) model(recordID string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.by[recordID]
}

// --- The closing accounting -----------------------------------------------

// summarize prints what running on your own hardware changed, in numbers the
// run produced rather than claims about it.
func summarize(res *loom.RunResult, reg *model.Registry, p *pipeline.Pipeline,
	fastSrv, deepSrv *llamacpptest.Server, fastProps, deepProps llamacpp.Props) {

	total := res.Report.Totals()
	fmt.Println("--- what running on your own hardware changed ---")

	// 1. Cost. The interesting number is not the zero; it is the zero next to
	// what the identical pipeline would have cost billed by the token.
	fmt.Printf("cost         $%.4f over %d model call(s) — the run budget never came near binding\n",
		total.CostUSD, total.Requests)
	if hosted, ok := hostedProjection(p, reg); ok {
		fmt.Printf("             the same pipeline at hosted rates: $%.4f expected, $%.4f ceiling (projected, no calls made)\n",
			hosted.Expected().CostUSD, hosted.Ceiling().CostUSD)
	}
	if total.Requests == 0 {
		fmt.Println("             (and not one call was made: every task replayed from the cache)")
	}

	// 2. The ceiling that did bind. Eight workers, three slots between the two
	// servers: the scheduler is what kept the device from oversubscription,
	// and the servers counted what actually arrived.
	if fastSrv != nil && deepSrv != nil {
		fmt.Printf("ceiling      local-fast %d slot(s), peak %d in flight · local-deep %d slot(s), peak %d in flight\n",
			fastProps.Slots, fastSrv.Peak(), deepProps.Slots, deepSrv.Peak())
	} else {
		fmt.Printf("ceiling      local-fast %d slot(s) · local-deep %d slot(s), discovered from the servers\n",
			fastProps.Slots, deepProps.Slots)
	}

	// 3 and 4. The envelope the planner produced for the triage stage — the
	// artifact itself, recompiled here so the printout cannot drift from what
	// the run enforced.
	if env, ok := triageEnvelope(p, reg, res.RunID); ok {
		fmt.Printf("egress       %v — the executor checks this before every call\n", env.Egress.Hosts)
		var secrets int
		caps := make([]string, 0, 4)
		for _, c := range env.Grants.List() {
			caps = append(caps, string(c))
			if strings.HasPrefix(string(c), "secret:") {
				secrets++
			}
		}
		fmt.Printf("credentials  %d secret grant(s): %s\n", secrets, strings.Join(caps, " "))
	}

	// 5. The prefix cache, which locally is the KV cache.
	if total.CacheReadTokens > 0 {
		fmt.Printf("kv cache     %d of %d prompt tokens served from the shared rubric, recomputed for nobody\n",
			total.CacheReadTokens, total.PromptTokens())
	}

	for _, f := range res.Failures {
		fmt.Printf("dead letter  %s: %v\n", f.Task.ID, f.Err)
	}
}

// hostedProjection asks what this pipeline would cost billed by the token.
//
// It is loom.Explain over the same pipeline against a registry where the two
// local IDs carry first-party rates instead of nothing — the same providers,
// priced as though somebody else owned the hardware. No call is made and no
// key is needed, which is what makes the comparison free to print.
func hostedProjection(p *pipeline.Pipeline, local *model.Registry) (*loom.Projection, bool) {
	// Prevailing per-MTok rates for a small and a large hosted model.
	rates := map[string]model.Pricing{
		"local-fast": {InputPerMTok: 1, OutputPerMTok: 5},
		"local-deep": {InputPerMTok: 5, OutputPerMTok: 25},
	}
	hosted := model.NewRegistry()
	for _, info := range local.All() {
		info.Pricing = rates[info.ID]
		info.Limits = model.Limits{}
		if hosted.Register(info) != nil {
			return nil, false
		}
	}
	proj, err := loom.Explain(p,
		loom.WithRegistry(hosted),
		// triage's fields come out of the model, so the filter under it would
		// drop every record during projection. Naming the value that makes the
		// most downstream work turns the guess back into a count.
		loom.WithStageSample("triage", map[string]any{"paging": true}),
	)
	if err != nil {
		return nil, false
	}
	return proj, true
}

// triageEnvelope recompiles the pipeline to read the envelope the planner
// assembles for the triage stage. It is the same call the run made, so the
// printout below cannot drift from what was enforced.
func triageEnvelope(p *pipeline.Pipeline, reg *model.Registry, runID string) (task.Envelope, bool) {
	compiled, err := plan.Compile(p, reg)
	if err != nil {
		return task.Envelope{}, false
	}
	sp, ok := compiled.ByID["triage"]
	if !ok {
		return task.Envelope{}, false
	}
	return sp.Envelope(runID, nil), true
}

// --- The servers ----------------------------------------------------------

// server resolves one model's address: the one given on the command line, or
// an in-process llama.cpp-compatible server started here so the example runs
// with nothing installed. It returns the base URL, the in-process server when
// there is one, and the shutdown.
func server(addr string, gen func(llamacpptest.Prompt) (string, error), slots int, slow time.Duration) (string, *llamacpptest.Server, func()) {
	if addr != "" {
		return addr, nil, func() {}
	}
	srv := &llamacpptest.Server{
		Model:       fmt.Sprintf("models/example-%s.gguf", map[int]string{1: "14b-q4", 2: "4b-q4"}[slots]),
		Slots:       slots,
		ContextSize: 8192,
		Delay:       slow,
		Generate:    gen,
	}
	base, stop := srv.Start()
	return base, srv, stop
}

// register adds one server's model, failing loudly if it is not there.
func register(ctx context.Context, reg *model.Registry, p *llamacpp.Provider, id string, tier model.Tier) llamacpp.Props {
	props, err := llamacpp.Register(ctx, reg, p, id, tier)
	if err != nil {
		log.Fatalf("%v\n(start one with: llama-server -m model.gguf --port 8080 --parallel 2)", err)
	}
	return props
}

// triageModel is the small model: it reads the incident and answers with the
// JSON the stage asked for. Like a real small model it is confident about
// the clear cases and, on the one report that contradicts itself, declines to
// place it — which is what sends that record up the ladder.
func triageModel() func(llamacpptest.Prompt) (string, error) {
	return func(p llamacpptest.Prompt) (string, error) {
		text := strings.ToLower(p.User)
		component := componentOf(p.User)
		switch {
		case strings.Contains(text, "cannot say what"):
			// Contradictory evidence. The rubric says not to call this a sev3,
			// and this model has nothing better to offer.
			return `{"severity": "unclear", "component": "auth", "paging": false}`, nil
		case strings.Contains(text, "declined") || strings.Contains(text, "502"):
			return fmt.Sprintf(`{"severity": "sev1", "component": %q, "paging": true}`, component), nil
		case strings.Contains(text, "objective") || strings.Contains(text, "error rate"):
			return fmt.Sprintf(`{"severity": "sev2", "component": %q, "paging": true}`, component), nil
		case strings.Contains(text, "footer") || strings.Contains(text, "mislabelled"):
			return fmt.Sprintf(`{"severity": "sev4", "component": %q, "paging": false}`, component), nil
		default:
			return fmt.Sprintf(`{"severity": "sev3", "component": %q, "paging": false}`, component), nil
		}
	}
}

// briefModel is the large model. It answers two kinds of prompt: the triage
// the small model could not place, and the reduce that writes the brief.
func briefModel() func(llamacpptest.Prompt) (string, error) {
	return func(p llamacpptest.Prompt) (string, error) {
		if strings.Contains(p.User, "Reply with JSON") {
			// The escalated incident: an auth path that may be admitting the
			// wrong person is a sev1 until it is shown not to be.
			return `{"severity": "sev1", "component": "auth", "paging": true}`, nil
		}
		services := []string{}
		for _, s := range []string{"payments", "checkout", "auth", "search"} {
			if strings.Contains(strings.ToLower(p.User), s) {
				services = append(services, s)
			}
		}
		return fmt.Sprintf(
			"Payments is the one that costs money: card declines against the gateway have been failing since 09:40 "+
				"and at least two accounts are affected, with a renewal due tonight. Alongside it, %s are pageable — "+
				"checkout is serving well past its latency objective, search has held a 6%% error rate since the 08:00 "+
				"index rebuild, and auth cannot be placed from the evidence on hand and is being treated as sev1 until "+
				"it is ruled out.",
			strings.Join(services, ", ")), nil
	}
}

// componentOf picks the service name out of the rendered prompt, which is
// where the stage template put it.
func componentOf(prompt string) string {
	_, rest, ok := strings.Cut(prompt, "against service ")
	if !ok {
		return "unknown"
	}
	name, _, _ := strings.Cut(rest, ":")
	return strings.TrimSpace(name)
}

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
