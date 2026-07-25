// Command support-desk runs a real Loom pipeline against the OpenAI API,
// built so the framework's two sharing advantages are visible rather than
// implied:
//
//   - Broadcast values: the product catalog, the support policy, and the
//     brand-voice rubric are registered once per run with loom.WithBroadcast.
//     Every task that needs them reads them by content-hash reference — one
//     stored copy for the whole run instead of a copy in every task envelope.
//     The run prints exactly how many bytes that avoided, and the
//     constellation view draws each shared value as a ⬡ node wired to its
//     readers (press `s` there for the run-summary overlay).
//
//   - Multi-task executors: a small pool of executors (◇ diamonds in the
//     view) each runs many tasks, and each task is provisioned with only the
//     capabilities its stage declared — the classify tasks can read the
//     policy but not the voice rubric; the reply tasks can read both.
//
// The pipeline: tickets are enriched from the catalog in a pure Go stage
// (capability-checked broadcast read, no model call), classified against the
// support policy on GPT-5.4 nano (escalating to mini on invalid output),
// routed to queues in a fused pure stage, then branch — problem tickets get
// a reply drafted on mini in the brand voice, and every ticket feeds an
// operations digest synthesized on GPT-5.4.
//
// Requires OPENAI_API_KEY. The run is capped at $0.50 by the budget
// governor.
//
//	OPENAI_API_KEY=sk-... go run ./examples/support-desk
//	# then open http://localhost:8077
//
// To see the caching story (this is the part worth doing twice):
//
//	LOOM_STATE=/tmp/loom-desk OPENAI_API_KEY=sk-... go run ./examples/support-desk
//	LOOM_STATE=/tmp/loom-desk OPENAI_API_KEY=sk-... go run ./examples/support-desk
//	# second run: every task replays from cache, zero model calls, $0
//
//	LOOM_STATE=/tmp/loom-desk OPENAI_API_KEY=sk-... go run ./examples/support-desk -policy v2
//	# the policy broadcast changed, so exactly the stages that read it
//	# (classify, draft-reply) and their downstream recompute; enrich, which
//	# reads only the catalog, replays from cache untouched.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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
	"github.com/zionrubin/loom/providers/openai"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/viz"
)

// ----------------------------------------------------------------------------
// Shared knowledge — registered once per run, referenced by every reader.
// ----------------------------------------------------------------------------

// catalog is the product knowledge base: the kind of side table every
// support task needs and none should carry a private copy of.
var catalog = map[string]map[string]string{
	"AUR-100": {"name": "Aurora Buds", "category": "earbuds", "warranty": "12 months",
		"known_issues": "left bud may fail to pair after firmware 2.3; fixed in 2.4"},
	"AUR-200": {"name": "Aurora Buds Pro", "category": "earbuds", "warranty": "24 months",
		"known_issues": "case hinge can loosen with heavy use"},
	"NIM-055": {"name": "Nimbus Speaker", "category": "speaker", "warranty": "12 months",
		"known_issues": "none"},
	"NIM-075": {"name": "Nimbus Speaker XL", "category": "speaker", "warranty": "12 months",
		"known_issues": "Wi-Fi setup fails on mesh networks with band steering; workaround: temporary 2.4 GHz-only SSID"},
	"VEL-310": {"name": "Velo Watch", "category": "wearable", "warranty": "24 months",
		"known_issues": "battery drain on always-on display with firmware < 5.1"},
	"VEL-320": {"name": "Velo Watch Lite", "category": "wearable", "warranty": "12 months",
		"known_issues": "strap pins on early batches (serial < VL2100) can slip; free replacement program active"},
	"TER-410": {"name": "Terra Router", "category": "networking", "warranty": "36 months",
		"known_issues": "companion app crashes on Android 15; fix rolling out"},
	"TER-450": {"name": "Terra Mesh Kit", "category": "networking", "warranty": "36 months",
		"known_issues": "none"},
	"SOL-500": {"name": "Solis Charger", "category": "power", "warranty": "12 months",
		"known_issues": "recalled units from batch 2025-11 overheat; recall lookup at solis.example/recall"},
	"SOL-520": {"name": "Solis Power Bank", "category": "power", "warranty": "12 months",
		"known_issues": "none"},
}

// Two policy versions: rerunning with -policy v2 changes the broadcast's
// content hash, which invalidates exactly the cached stages that read it.
var policies = map[string]string{
	"v1": `SUPPORT POLICY (v1, authoritative)
- Refunds: full refund within 30 days of delivery, no questions asked.
- Warranty: free repair or replacement for hardware faults inside the
  product's warranty window; shipping damage is always covered.
- Recalled units: immediate free replacement plus a $20 goodwill credit.
- Escalation: safety issues (overheating, battery swelling) are P1 and go
  to the safety desk within 4 hours. Angry customers with a valid claim
  get a $10 goodwill credit at the agent's discretion.
- Never promise firmware release dates.`,

	"v2": `SUPPORT POLICY (v2, authoritative — supersedes v1)
- Refunds: full refund within 60 days of delivery, no questions asked.
- Warranty: free repair or replacement for hardware faults inside the
  product's warranty window; shipping damage is always covered.
- Recalled units: immediate free replacement plus a $40 goodwill credit.
- Escalation: safety issues (overheating, battery swelling) are P1 and go
  to the safety desk within 2 hours. Angry customers with a valid claim
  get a $15 goodwill credit at the agent's discretion.
- Never promise firmware release dates.`,
}

var voiceRubric = `BRAND VOICE
- Warm and plain-spoken; no corporate filler ("we apologize for any
  inconvenience" is banned).
- Lead with the fix, not the apology.
- One concrete next step per reply, with a timeframe.
- Mention the known-issue fix or workaround when one exists.
- Maximum four sentences.`

var tickets = []core.Record{
	core.NewRecord("t01", map[string]any{"sku": "SOL-500", "customer": "Dana",
		"subject": "charger got REALLY hot",
		"body":    "My Solis charger from November got too hot to touch last night. I unplugged it. This seems dangerous?"}),
	core.NewRecord("t02", map[string]any{"sku": "AUR-100", "customer": "Miguel",
		"subject": "left earbud won't pair anymore",
		"body":    "Since the last update the left bud just blinks and never connects. Tried resetting twice."}),
	core.NewRecord("t03", map[string]any{"sku": "NIM-075", "customer": "Priya",
		"subject": "can't finish Wi-Fi setup",
		"body":    "The app never finds the speaker during setup. I have an eero mesh at home. About to return this."}),
	core.NewRecord("t04", map[string]any{"sku": "VEL-320", "customer": "Jonas",
		"subject": "watch fell off — strap pin slipped",
		"body":    "Serial VL2043. The strap pin slipped and the watch hit the pavement. Screen is fine but I don't trust the strap now."}),
	core.NewRecord("t05", map[string]any{"sku": "TER-410", "customer": "Amara",
		"subject": "app crashes on my new phone",
		"body":    "Upgraded to a Pixel on Android 15 and the Terra app crashes on launch every time. Router still works."}),
	core.NewRecord("t06", map[string]any{"sku": "NIM-055", "customer": "Ken",
		"subject": "love this thing",
		"body":    "Just wanted to say the Nimbus sounds fantastic on the patio. Zero complaints after three months."}),
	core.NewRecord("t07", map[string]any{"sku": "AUR-200", "customer": "Sofia",
		"subject": "case hinge feels loose after 5 weeks",
		"body":    "The lid flops open in my bag now. Bought it 5 weeks ago. Can I get a refund or a new case?"}),
	core.NewRecord("t08", map[string]any{"sku": "VEL-310", "customer": "Tomás",
		"subject": "battery dies by 3pm",
		"body":    "Always-on display, firmware 4.9. Battery used to last two days, now it's dead by mid-afternoon. Furious — this watch was not cheap."}),
	core.NewRecord("t09", map[string]any{"sku": "SOL-520", "customer": "Elif",
		"subject": "arrived with a cracked shell",
		"body":    "The power bank works but the casing arrived cracked at the corner. Box was crushed on one side."}),
	core.NewRecord("t10", map[string]any{"sku": "TER-450", "customer": "Ravi",
		"subject": "one node keeps dropping",
		"body":    "The upstairs mesh node drops offline every couple of days until I power-cycle it. Two months old."}),
	core.NewRecord("t11", map[string]any{"sku": "AUR-100", "customer": "Lena",
		"subject": "refund window question",
		"body":    "Delivered 41 days ago, barely used, sound isn't for me. Am I still inside the refund window?"}),
	core.NewRecord("t12", map[string]any{"sku": "NIM-055", "customer": "Marcus",
		"subject": "speaker just... died",
		"body":    "Ten months in, the Nimbus won't power on at all. Different cables, different outlets, nothing."}),
}

// ----------------------------------------------------------------------------
// Advantage tracker — folds run events into the numbers the demo is about.
// ----------------------------------------------------------------------------

type broadcastStat struct {
	bytes  int
	reads  int
	stages map[string]bool
}

type tracker struct {
	mu        sync.Mutex
	shared    map[string]*broadcastStat
	workers   map[string]bool
	tasks     int
	cacheHits int
}

func newTracker() *tracker {
	return &tracker{shared: map[string]*broadcastStat{}, workers: map[string]bool{}}
}

func (tr *tracker) stat(name string) *broadcastStat {
	s, ok := tr.shared[name]
	if !ok {
		s = &broadcastStat{stages: map[string]bool{}}
		tr.shared[name] = s
	}
	return s
}

func (tr *tracker) handle(e observe.Event) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	switch e.Type {
	case observe.BroadcastRegistered:
		tr.stat(e.Broadcast).bytes = e.Bytes
	case observe.BroadcastRead:
		s := tr.stat(e.Broadcast)
		s.reads++
		if e.Stage != "" {
			s.stages[e.Stage] = true
		}
	case observe.TaskStarted:
		if e.Worker != "" {
			tr.workers[e.Worker] = true
		}
	case observe.TaskCompleted:
		tr.tasks++
	case observe.CacheHit:
		tr.cacheHits++
	}
}

func fmtKB(n int) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.1f KB", float64(n)/1024)
}

func (tr *tracker) summary() string {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	var b strings.Builder
	fmt.Fprintf(&b, "--- why this run was cheap to share ---\n")
	fmt.Fprintf(&b, "%-10s %9s %6s %8s   %-24s %s\n",
		"broadcast", "size", "reads", "stages", "if copied per task", "actually shipped")
	names := make([]string, 0, len(tr.shared))
	for n := range tr.shared {
		names = append(names, n)
	}
	sort.Strings(names)
	totalAvoided := 0
	for _, n := range names {
		s := tr.shared[n]
		copied := s.bytes * s.reads
		// Envelopes carry a 64-byte content hash per declared broadcast; the
		// value itself is stored once for the whole run.
		shipped := s.bytes + 64*s.reads
		if s.reads > 0 {
			totalAvoided += copied - shipped
		}
		fmt.Fprintf(&b, "%-10s %9s %6d %8d   %-24s %s\n",
			n, fmtKB(s.bytes), s.reads, len(s.stages),
			fmtKB(copied), fmt.Sprintf("%s (1 copy + hashes)", fmtKB(shipped)))
	}
	if totalAvoided > 0 {
		fmt.Fprintf(&b, "payload avoided across all task envelopes: %s\n", fmtKB(totalAvoided))
	}
	fmt.Fprintf(&b, "\n%d executors ran %d tasks (avg %.1f tasks/executor); each task was\n",
		len(tr.workers), tr.tasks, float64(tr.tasks)/float64(max(len(tr.workers), 1)))
	fmt.Fprintf(&b, "provisioned with only its stage's grants — classify tasks can read the\n")
	fmt.Fprintf(&b, "policy but not the voice rubric; reply tasks can read both; enrich reads\n")
	fmt.Fprintf(&b, "only the catalog and never touches the API key.\n")
	if tr.cacheHits > 0 {
		fmt.Fprintf(&b, "\n%d of %d tasks replayed from cache — paid for once, never again.\n", tr.cacheHits, tr.tasks)
	}
	return b.String()
}

// ----------------------------------------------------------------------------

// buildPipeline declares the dataflow. It is separated from main so the
// offline smoke test can run the identical pipeline on mock models.
func buildPipeline() *pipeline.Pipeline {
	p := pipeline.New("support-desk")
	src := p.FromRecords("tickets", tickets)

	// Pure Go enrichment from the shared catalog: a capability-checked
	// broadcast read, no model call, cached like everything else. This stage
	// declares only "catalog" — it could not read the policy if it tried.
	enriched := src.MapTools("enrich", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		cat, err := core.BroadcastAs[map[string]map[string]string](ctx, s, "catalog")
		if err != nil {
			return core.Record{}, err
		}
		out := r.Clone()
		if prod, ok := cat[r.String("sku")]; ok {
			out.Data["product"] = prod["name"]
			out.Data["warranty"] = prod["warranty"]
			out.Data["known_issues"] = prod["known_issues"]
		} else {
			out.Data["product"] = "unknown product"
			out.Data["warranty"] = "unknown"
			out.Data["known_issues"] = "none on file"
		}
		return out, nil
	}, pipeline.WithBroadcast("catalog"), pipeline.WithVersion("v1"))

	// Classification runs on the fast tier and escalates to mini when the
	// output fails parsing or validation. The policy arrives via
	// {{broadcast "policy"}} — the prompt template references the shared
	// value; the task envelope carries its hash, not its bytes.
	classified := enriched.Infer("classify", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"gpt-5.4-mini"}},
		System:  "You triage consumer-electronics support tickets. Respond with a single JSON object and nothing else.",
		Prompt: `{{broadcast "policy"}}

Product: {{.product}} (sku {{.sku}}, warranty {{.warranty}})
Known issues: {{.known_issues}}

Ticket from {{.customer}} — {{.subject}}
{{.body}}

Apply the policy above literally. Respond with JSON:
{"category": "safety|hardware|software|shipping|billing|praise|other",
 "sentiment": "angry|neutral|happy",
 "priority": "P1|P2|P3",
 "refund_due": true|false,
 "issue": "<one-line issue summary, or 'none'>"}`,
		MaxTokens: 300,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			switch r.String("priority") {
			case "P1", "P2", "P3":
			default:
				return fmt.Errorf("bad priority %q", r.String("priority"))
			}
			switch r.String("sentiment") {
			case "angry", "neutral", "happy":
				return nil
			}
			return fmt.Errorf("bad sentiment %q", r.String("sentiment"))
		},
	}, pipeline.WithBroadcast("policy"))

	// Pure routing: P1 anywhere goes to the safety desk, everything else to
	// its category queue. Fused and cached; no model involved.
	routed := classified.Map("route", func(r core.Record) (core.Record, error) {
		out := r.Clone()
		if r.String("priority") == "P1" {
			out.Data["queue"] = "safety-desk"
		} else {
			out.Data["queue"] = r.String("category") + "-queue"
		}
		return out, nil
	}, pipeline.WithVersion("v1"))

	// Branch 1: anything that isn't praise gets a drafted reply, written in
	// the brand voice against the same policy the classifier used. This
	// stage declares two broadcasts; its envelope grants exactly those.
	problems := routed.Filter("needs-reply", func(r core.Record) (bool, error) {
		return r.String("category") != "praise", nil
	}, pipeline.WithVersion("v1"))
	problems.Infer("draft-reply", pipeline.InferSpec{
		Binding: model.Binding{Model: "gpt-5.4-mini"},
		System:  "You write customer-support replies. Follow the voice rubric exactly.",
		Prompt: `{{broadcast "voice"}}

{{broadcast "policy"}}

Product: {{.product}} — known issues: {{.known_issues}}
Ticket ({{.category}}, {{.priority}}, refund due: {{.refund_due}}) from {{.customer}}:
{{.subject}} — {{.body}}

Draft the reply.`,
		MaxTokens:   300,
		OutputField: "reply",
	}, pipeline.WithBroadcast("voice", "policy"))

	// Branch 2: every routed ticket — praise included — feeds the operations
	// digest, synthesized hierarchically on the deep model.
	routed.ReduceAI("ops-digest", pipeline.ReduceAISpec{
		Binding:   model.Binding{Model: "gpt-5.4"},
		System:    "You write crisp support-operations digests.",
		Prompt:    "Synthesize an operations digest from {{.Count}} triaged ticket issues. Group recurring problems, flag anything safety-related first, and end with one recommended action.\n{{range .Items}}- {{.}}\n{{end}}",
		FanIn:     6,
		MaxTokens: 400,
		ItemField: "issue",
	})

	return p
}

func main() {
	addr := flag.String("addr", "localhost:8077", "address for the constellation view")
	policyVersion := flag.String("policy", "v1", "support policy version to broadcast (v1|v2)")
	flag.Parse()

	key := os.Getenv("OPENAI_API_KEY")
	if key == "" {
		log.Fatal("set OPENAI_API_KEY to run this example")
	}
	policy, ok := policies[*policyVersion]
	if !ok {
		log.Fatalf("unknown -policy %q (want v1 or v2)", *policyVersion)
	}

	reg := model.NewRegistry()
	// gpt-5.4 (deep), gpt-5.4-mini (balanced), gpt-5.4-nano (fast), with real
	// pricing; admission control at 50 req/min per model.
	if err := openai.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 50}); err != nil {
		log.Fatal(err)
	}

	p := buildPipeline()

	// --- Serve the constellation view and run -------------------------------
	v := viz.New()
	url, err := v.Start(*addr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("constellation view: %s\n", url)
	fmt.Println("waiting up to 60s for a browser to connect (Ctrl-C to abort)…")
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 60*time.Second)
	if v.AwaitViewer(waitCtx) {
		fmt.Println("viewer connected — starting the run")
		time.Sleep(800 * time.Millisecond) // a beat, so the empty sky is visible first
	} else {
		fmt.Println("no viewer yet — running anyway (the page replays state on connect)")
	}
	cancelWait()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	adv := newTracker()
	opts := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithSecrets(map[security.SecretRef]string{openai.DefaultSecretRef: key}),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 0.50}),
		loom.WithWorkers(6),
		// Shared knowledge, registered once. Stages opt in with
		// pipeline.WithBroadcast and read by content-hash reference.
		loom.WithBroadcast("catalog", catalog),
		loom.WithBroadcast("policy", policy),
		loom.WithBroadcast("voice", voiceRubric),
		loom.WithEventHandler(func(e observe.Event) {
			v.Handle(e)
			adv.handle(e)
		}),
	}
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		opts = append(opts, loom.WithStateDir(dir))
	} else {
		fmt.Println("tip: set LOOM_STATE=/tmp/loom-desk to make reruns free (and try -policy v2 after)")
	}

	res, err := loom.Run(ctx, p, opts...)
	if err != nil {
		spent := 0.0
		if res != nil {
			spent = res.Spent.CostUSD
		}
		fmt.Printf("\nrun ended with error: %v (spent $%.4f)\n", err, spent)
	}
	if res != nil {
		fmt.Println("\n--- triaged tickets ---")
		for _, r := range res.StageOutputs["route"] {
			fmt.Printf("%s  %-12s %-8s %-18s refund=%v  %s\n",
				r.ID, r.String("priority")+"/"+r.String("sentiment"), r.String("sku"),
				r.String("queue"), r.Data["refund_due"], r.String("issue"))
		}
		fmt.Println("\n--- drafted replies (everything but praise) ---")
		for _, r := range res.StageOutputs["draft-reply"] {
			fmt.Printf("%s → %s: %s\n\n", r.ID, r.String("customer"), r.String("reply"))
		}
		fmt.Println("--- operations digest ---")
		for _, r := range res.StageOutputs["ops-digest"] {
			fmt.Println(r.String("output"))
		}
		fmt.Println("\n--- report ---")
		fmt.Print(res.Report.String())
		fmt.Println()
		fmt.Print(adv.summary())
		if os.Getenv("LOOM_STATE") != "" && adv.cacheHits == 0 {
			fmt.Println("\nrerun the same command: every task above replays from cache for $0.")
			fmt.Println("then rerun with -policy v2: only classify and draft-reply (the stages")
			fmt.Println("that read the policy broadcast) recompute; enrich stays cached.")
		}
	}

	fmt.Printf("\nrun finished — still serving %s (press `s` there for the run summary; Ctrl-C to exit)\n", url)
	<-ctx.Done()
	_ = v.Close()
}
