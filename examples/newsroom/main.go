// Command newsroom is Loom's fleet demo: five agents running at once on one
// engine, sharing its slots, its provider quota, its dollar ceiling, and its
// cache — and reaching each other's conclusions through a blackboard.
// Everything runs on scripted mock models, so it needs no keys, no network,
// and costs nothing.
//
// The agents:
//
//	wire-desk    60 wire reports → classify (fast tier).      The long agent.
//	beat-markets  ┐
//	beat-policy   ├ 6 briefs each → summarize (balanced),     The short agents.
//	beat-tech     ┘ then post one finding to the blackboard.
//	front-page   reads the board → writes the page (deep).    The expensive agent.
//	wire-recheck the same 60 reports again: all cached, $0.
//
// Four properties of a fleet, in one run:
//
//   - The short agents overtake the long one. wire-desk is launched first and
//     fills every slot; the beats are launched a moment later and still finish
//     well before it, because a contended slot goes to the agent that has been
//     served least rather than the one that queued first. The report prints
//     each agent's completion time next to the slot-time it was given.
//
//   - The beats hand their findings to front-page through the blackboard. A
//     loom DAG fans out but does not fan back in, so this synthesis could only
//     ever be another run — and now it is another run that can *read* what the
//     first ones concluded, pinned to a content hash so its cache stays honest.
//
//   - One ceiling covers all five agents, and one quota. Two Runs in a process
//     each enforce their own; a fleet enforces yours.
//
//   - wire-recheck replays wire-desk's 60 classifications for nothing, because
//     the cache belongs to the fleet rather than to a run.
//
// Running it:
//
//	go run ./examples/newsroom           # then open http://localhost:8077
//
//	# squeeze the ceiling and watch one governor stop the whole newsroom
//	go run ./examples/newsroom -budget 0.02
//
//	# tune the fairness bound: a higher aging rate protects an agent that has
//	# already been served heavily from ones that keep arriving fresh
//	go run ./examples/newsroom -aging 4
//
//	# cache as checkpoint, across the fleet and across processes
//	LOOM_STATE=/tmp/loom-newsroom go run ./examples/newsroom
//	LOOM_STATE=/tmp/loom-newsroom go run ./examples/newsroom
//
// In the constellation view every agent is its own sky in one universe (press
// `u`), so the wire desk stays whole and inspectable while the front page is
// still being written.
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/viz"
)

// beats are the desks that report in parallel and then hand up their findings.
var beats = []string{"markets", "policy", "tech"}

// The system prompt of each stage, named because the scripted models dispatch
// on them — see respond.
const (
	sysWire       = "You classify incoming wire reports."
	sysReporter   = "You are a beat reporter."
	sysBeatEditor = "You are the beat editor."
	sysEditor     = "You are the editor."
)

func main() {
	addr := flag.String("addr", "localhost:8077", "address for the constellation view")
	budget := flag.Float64("budget", 5.00, "fleet budget in USD, across every agent (try 0.02)")
	slots := flag.Int("slots", 6, "execution slots the whole fleet shares")
	aging := flag.Float64("aging", 0, "priority credit a queued task earns per unit wait (0 = default)")
	wait := flag.Duration("wait", 60*time.Second, "how long to wait for a browser before starting anyway")
	serve := flag.Bool("serve", true, "keep the constellation view up after the newsroom finishes")
	flag.Parse()

	reg, fast, err := buildRegistry()
	if err != nil {
		log.Fatal(err)
	}

	v := viz.New()
	url, err := v.Start(*addr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("constellation view: %s\n", url)
	fmt.Printf("waiting up to %s for a browser to connect (Ctrl-C to abort)…\n", *wait)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), *wait)
	if v.AwaitViewer(waitCtx) {
		fmt.Println("viewer connected — opening the newsroom")
		time.Sleep(800 * time.Millisecond)
	} else {
		fmt.Println("no viewer yet — running anyway (the page replays state on connect)")
	}
	cancelWait()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := []loom.Option{
		loom.WithRegistry(reg),
		// Slots, budget, cache and quota belong to the fleet, not to an agent.
		loom.WithWorkers(*slots),
		loom.WithFleetBudget(core.Budget{MaxCostUSD: *budget}),
		loom.WithContinueOnError(),
		loom.WithEventHandler(v.Handle),
		// The board an agent reads before anything has been posted to it.
		loom.WithTopic("findings"),
		// An ordinary broadcast: read-only for the fleet's whole life, unlike a
		// topic, which grows.
		loom.WithBroadcast("style-guide", styleGuide),
	}
	if *aging > 0 {
		opts = append(opts, loom.WithAdmissionAging(*aging))
	}
	if dir := os.Getenv("LOOM_STATE"); dir != "" {
		opts = append(opts, loom.WithStateDir(dir))
	}

	fleet, err := loom.NewFleet(opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer fleet.Close()

	fmt.Printf("fleet open: %d slots, $%.2f ceiling, one quota and one cache\n\n",
		fleet.Slots(), *budget)

	// --- The long agent, launched first and filling every slot -----------
	desk := fleet.Go(ctx, wireDesk("wire-desk"))
	fmt.Printf("launched %-14s 60 wire reports, fast tier\n", desk.Name)

	// --- The short agents, launched a moment later ------------------------
	// They are behind the desk in arrival order and ahead of it in attained
	// service, which is the whole difference a fleet makes.
	time.Sleep(150 * time.Millisecond)
	beatAgents := map[string]*loom.Agent{}
	for _, beat := range beats {
		a := fleet.Go(ctx, beatPipeline(beat))
		beatAgents[beat] = a
		fmt.Printf("launched %-14s 6 briefs, balanced tier\n", a.Name)
	}

	// Each beat posts its finding as it lands, so front-page can be launched
	// on the count rather than on a list of agents.
	for _, beat := range beats {
		go func(beat string) {
			res, err := beatAgents[beat].Wait()
			if err != nil || res == nil || len(res.Output) == 0 {
				return
			}
			v, err := fleet.PostFrom("beat-"+beat, "findings", map[string]any{
				"beat":    beat,
				"finding": res.Output[0].String("output"),
			})
			if err == nil {
				fmt.Printf("posted   %-14s → %s\n", "beat-"+beat, v)
			}
		}(beat)
	}

	// --- Fan-in: wait for the board to fill, then synthesize --------------
	// This is the shape a loom DAG cannot express. The board is how it becomes
	// expressible without giving up reproducibility: front-page pins the
	// snapshot it read, and its cache key is built from that hash.
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 2*time.Minute)
	posts, err := fleet.Await(awaitCtx, "findings", len(beats))
	cancelAwait()
	if err != nil {
		fmt.Printf("board never filled (%v); writing the page from %d findings\n", err, len(posts))
	} else {
		fmt.Printf("\nboard has %d findings — launching front-page\n", len(posts))
	}

	page, err := fleet.Run(ctx, frontPage())
	if err != nil {
		fmt.Printf("front-page ended with error: %v\n", err)
	}

	// --- The same work again, for free ------------------------------------
	// Identical stage spec, identical records: the fleet's cache answers every
	// task without a model call. On a sequence of Runs this would cost twice.
	if _, err := desk.Wait(); err != nil {
		fmt.Printf("wire-desk ended with error: %v\n", err)
	}
	before := fast.Calls()
	if _, err := fleet.Run(ctx, wireDesk("wire-recheck")); err != nil {
		fmt.Printf("wire-recheck ended with error: %v\n", err)
	}
	recheckCalls := fast.Calls() - before

	if err := fleet.Wait(); err != nil {
		fmt.Printf("\nfleet ended with error: %v\n", err)
	}

	// --- What the newsroom produced --------------------------------------
	if page != nil && len(page.Output) > 0 {
		fmt.Printf("\n--- front page ---\n%s\n", page.Output[0].String("output"))
	}
	fmt.Printf("\n%s", fleet.Report())
	fmt.Printf("wire-recheck cost %d model calls (the fleet's cache had all 60)\n", recheckCalls)

	if !*serve {
		return
	}
	fmt.Printf("\nconstellation view still serving at %s — press `u` for the universe, Ctrl-C to exit\n", url)
	<-ctx.Done()
}

// --- Pipelines ----------------------------------------------------------

// wireDesk classifies the incoming wire, one call per report. It is the long
// agent: 60 tasks that would otherwise monopolise every slot they can reach.
//
// The pipeline is named per agent but its records and stage spec are not, so
// two desks over the same wire share cache keys — which is what makes
// wire-recheck free.
func wireDesk(name string) *pipeline.Pipeline {
	p := pipeline.New(name)
	p.FromRecords("wire", wireReports()).
		Infer("classify", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast, Escalation: []string{"mock-balanced"}},
			System:    sysWire,
			Prefix:    `Style guide:\n{{broadcast "style-guide"}}`,
			Prompt:    "Classify this report: {{.headline}}",
			ParseJSON: true,
			MaxTokens: 200,
		}, pipeline.WithBroadcast("style-guide"))
	return p
}

// beatPipeline is one beat desk: six briefs, summarized, then tree-reduced to
// the single finding the beat hands up to the board.
func beatPipeline(beat string) *pipeline.Pipeline {
	p := pipeline.New("beat-" + beat)
	p.FromRecords("briefs", briefs(beat)).
		Infer("read", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierBalanced},
			System:    sysReporter,
			Prompt:    "Summarize this brief for the " + beat + " beat: {{.brief}}",
			MaxTokens: 300,
		}).
		ReduceAI("finding", pipeline.ReduceAISpec{
			Binding:   model.Binding{Tier: model.TierBalanced},
			System:    sysBeatEditor,
			Prompt:    "State the one thing the " + beat + " beat should lead with, from {{.Count}} summaries:\n{{range .Items}}- {{.}}\n{{end}}",
			FanIn:     6,
			MaxTokens: 300,
		})
	return p
}

// frontPage reads the blackboard and writes the page. One call, on the
// expensive model — the agent whose completion time a person is waiting on,
// and the reason the fleet's scheduling policy is worth having.
func frontPage() *pipeline.Pipeline {
	p := pipeline.New("front-page")
	p.FromRecords("slot", []core.Record{
		core.NewRecord("edition", map[string]any{"edition": "evening"}),
	}).
		Infer("write", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierDeep},
			System:  sysEditor,
			// The board arrives by reference: the envelope carries a hash, the
			// bytes come from content-addressed storage, and the hash joins this
			// stage's fingerprint so a new finding invalidates exactly this.
			Prefix: `Style guide:\n{{broadcast "style-guide"}}`,
			Prompt: `Findings from the beats:
{{range broadcast "findings"}}- [{{.value.beat}}] {{.value.finding}}
{{end}}
Write the {{.edition}} front page.`,
			MaxTokens: 600,
		}, pipeline.WithBroadcast("style-guide", "findings"))
	return p
}

// --- Data ---------------------------------------------------------------

const styleGuide = `House style: lead with the consequence, name the actor, no adjectives
in headlines, one claim per sentence, always attribute.`

var headlines = []string{
	"Central bank holds rates, signals two cuts", "Chip fab breaks ground in Dresden",
	"Port strike enters third week", "Regulator opens inquiry into ad auctions",
	"Grid operator reports record solar share", "Bond yields slip on soft payrolls",
	"Carrier cancels regional routes", "Housing starts fall for fourth month",
	"Antitrust suit narrowed by judge", "Battery plant delays first output",
	"Freight volumes rebound in the north", "Insurer raises catastrophe reserves",
	"Data-centre buildout strains water permits", "Copper hits eleven-month high",
	"Pension fund shifts to private credit", "Border tariff exemption expires",
	"Rail merger clears review", "Semiconductor export rules tightened",
	"Retail vacancies at decade low", "Farm subsidy overhaul advances",
}

// wireReports returns the 60 reports the desk classifies. Deterministic, so
// the cache and the projection agree with the run.
func wireReports() []core.Record {
	recs := make([]core.Record, 0, 60)
	for i := 0; i < 60; i++ {
		h := headlines[i%len(headlines)]
		recs = append(recs, core.NewRecord(fmt.Sprintf("wire-%02d", i), map[string]any{
			"headline": fmt.Sprintf("%s (dispatch %d)", h, i/len(headlines)+1),
		}))
	}
	return recs
}

// briefs returns one beat's six briefs. Each beat draws from a different slice
// of the headline pool, so the three findings that reach the board differ —
// which is the whole reason the editor reads all three.
func briefs(beat string) []core.Record {
	offset := 0
	for i, b := range beats {
		if b == beat {
			offset = i * 6
		}
	}
	recs := make([]core.Record, 0, 6)
	for i := 0; i < 6; i++ {
		recs = append(recs, core.NewRecord(fmt.Sprintf("%s-brief-%d", beat, i), map[string]any{
			"brief": fmt.Sprintf("%s desk note %d: %s", beat, i,
				headlines[(offset+i)%len(headlines)]),
		}))
	}
	return recs
}

// --- Mock models --------------------------------------------------------

// buildRegistry wires three scripted tiers with plausible prices and
// latencies. The deep model is slow and dear, which is what makes the front
// page's completion time the number worth protecting.
func buildRegistry() (*model.Registry, *model.Mock, error) {
	reg := model.NewRegistry()
	var fast *model.Mock
	tiers := []struct {
		id      string
		tier    model.Tier
		latency time.Duration
		pricing model.Pricing
	}{
		{"mock-fast", model.TierFast, 120 * time.Millisecond,
			model.Pricing{InputPerMTok: 0.80, OutputPerMTok: 4.00}},
		{"mock-balanced", model.TierBalanced, 400 * time.Millisecond,
			model.Pricing{InputPerMTok: 3.00, OutputPerMTok: 15.00}},
		{"mock-deep", model.TierDeep, 1200 * time.Millisecond,
			model.Pricing{InputPerMTok: 15.00, OutputPerMTok: 75.00}},
	}
	for _, t := range tiers {
		m := model.NewMock(t.id,
			model.WithLatency(t.latency),
			model.WithHandler(respond(t.id)))
		if err := reg.Register(model.Info{
			ID: t.id, Provider: m, Tier: t.tier, Pricing: t.pricing,
			Limits: model.Limits{RequestsPerMinute: 6000, TokensPerMinute: 4_000_000},
		}); err != nil {
			return nil, nil, err
		}
		if t.tier == model.TierFast {
			fast = m
		}
	}
	return reg, fast, nil
}

// respond returns a deterministic handler per model: same prompt, same answer,
// every time, so caching and reruns behave as they would in production.
//
// It dispatches on the system prompt rather than on the user prompt, because
// the user prompt of a later stage contains the *output* of an earlier one —
// and a mock that pattern-matches text a model produced will eventually match
// the wrong branch. The system prompt is written by the pipeline and can never
// be contaminated that way.
func respond(id string) func(model.Request) (string, error) {
	return func(req model.Request) (string, error) {
		// Jitter is derived from the prompt rather than a clock, so it varies
		// between calls without making the run non-reproducible.
		seed := int64(hash(req.Prompt))
		jitter := time.Duration(rand.New(rand.NewSource(seed)).Intn(180)) * time.Millisecond
		time.Sleep(jitter)

		switch req.System {
		case sysWire:
			headline := strings.TrimSpace(strings.TrimPrefix(req.Prompt, "Classify this report:"))
			return fmt.Sprintf(`{"beat": %q, "urgency": %d, "headline": %q}`,
				beats[hash(headline)%uint32(len(beats))], hash(headline)%5+1, headline), nil
		case sysReporter:
			brief := lastSegment(strings.TrimSpace(req.Prompt))
			return fmt.Sprintf("%s — %d sources agree", brief, hash(brief)%4+2), nil
		case sysBeatEditor:
			items := promptItems(req.Prompt)
			if len(items) == 0 {
				return "nothing to lead with", nil
			}
			return fmt.Sprintf("%s (over %d others)", items[0], len(items)-1), nil
		case sysEditor:
			return frontPageText(req.Prompt), nil
		}
		return "OK", nil
	}
}

// frontPageText lays out the page from the findings the editor read off the
// blackboard, which arrive in the prompt as "- [beat] finding" lines.
func frontPageText(prompt string) string {
	var b strings.Builder
	b.WriteString("EVENING EDITION")
	n := 0
	for _, line := range promptItems(prompt) {
		n++
		b.WriteString("\n  " + line)
	}
	fmt.Fprintf(&b, "\n(%d findings, read off the fleet's blackboard by reference)", n)
	return b.String()
}

// promptItems returns the "- " list items of a rendered prompt: the reduce
// tree's summaries, or the editor's findings.
func promptItems(prompt string) []string {
	var out []string
	for _, line := range strings.Split(prompt, "\n") {
		if item := strings.TrimPrefix(line, "- "); item != line && strings.TrimSpace(item) != "" {
			out = append(out, strings.TrimSpace(item))
		}
	}
	return out
}

// lastSegment returns the text after the final ": ", which is how the briefs
// and summaries in this demo carry their payload.
func lastSegment(s string) string {
	if i := strings.LastIndex(s, ": "); i >= 0 {
		return strings.TrimSpace(s[i+2:])
	}
	return s
}

func hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
