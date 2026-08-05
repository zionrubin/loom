// Command multi-hop is the proving example for Loom's algorithm seam: a
// research question answered by walking a citation graph, where which paper
// gets read next is decided by the model that just read the previous one.
//
// It is the workload one forward pass cannot express. A pipeline can read
// every paper once; it cannot read a paper, learn that the claim it needs is
// in a paper that one cites, and go get it. That is a fixpoint over a graph,
// and it is what pipeline.Iterate plus an algo.Algorithm are for.
//
// The pipeline is three stages and only the middle one is new:
//
//	seed        the two entry papers the question names
//	explore     Iterate: bulk-synchronous message passing over citations,
//	            where each active paper reports what it contributes and names
//	            the papers worth following, and those become the next round
//	synthesize  ReduceAI over everything the walk reached
//
// Four things are worth watching in the output, because each is a claim the
// design makes and this run either shows or does not:
//
//   - The frontier per round. It grows while the walk is discovering and
//     shrinks as papers stop having anywhere new to send. That shrinking is
//     convergence, and it is why the last round is the cheapest rather than
//     the most expensive.
//   - The projection printed before the run. A loop's cost is not knowable —
//     the round count is a property of the data — so Explain prices the round
//     cap instead, which is the number HaltWhen.Budget should be set from. The
//     run comes in under it.
//   - The graph growing. One paper cites something outside the corpus; with
//     Grow set, the walk creates that vertex rather than dropping the edge.
//   - The rerun. With -state, running twice replays the entire loop from the
//     content-addressed cache at zero model calls, because a vertex's cache
//     key is its state and its inbox rather than the round it is in.
//
// The model is a deterministic mock, so this runs offline with no key and no
// network:
//
//	go run ./examples/multi-hop
//	go run ./examples/multi-hop -state /tmp/loom-hop   # then again: 0 calls
//
// The corpus is synthetic. The papers, their abstracts, and their citations
// are fixtures invented for this example, not a real bibliography.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/algo"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

const question = "Does sparse retrieval actually reduce end-to-end latency, " +
	"or does it move the cost somewhere else?"

// corpus is a synthetic citation graph. It has the three shapes that make a
// walk interesting: a diamond (two routes converge on p7), a dead end (p5),
// and a reference to a paper the corpus does not contain (p9, cited by p7).
var corpus = []core.Record{
	paper("p1", "Sparse retrieval for long-context serving",
		"Reports a 4x reduction in retrieved tokens with no measured accuracy loss.",
		"p2", "p3"),
	paper("p2", "Index construction costs in sparse retrieval",
		"Finds index build time dominates when the corpus changes hourly.",
		"p4"),
	paper("p3", "Latency budgets in retrieval-augmented serving",
		"Breaks end-to-end latency into retrieval, prefill, and decode.",
		"p4", "p5"),
	paper("p4", "Where the time actually goes",
		"Measures that retrieval savings are offset by reranking in 3 of 5 systems.",
		"p7"),
	paper("p5", "A survey of retrieval strategies",
		"Catalogues approaches without new measurements.",
		""),
	paper("p6", "Unrelated work on tokenizer design",
		"Nothing to do with retrieval latency.",
		""),
	paper("p7", "Reranking is the hidden cost",
		"Shows reranking consumes the latency sparse retrieval saves, citing p9 for the model.",
		"p9"),
	paper("p8", "Sparse retrieval at small corpus sizes",
		"Finds no benefit below 10k documents.",
		""),
}

func paper(id, title, abstract string, cites ...string) core.Record {
	var edges []any
	for _, c := range cites {
		if c != "" {
			edges = append(edges, c)
		}
	}
	return core.NewRecord(id, map[string]any{
		"title": title, "abstract": abstract, "cites": edges,
	})
}

func main() {
	state := flag.String("state", "", "directory for the persistent cache (rerun to replay for free)")
	rounds := flag.Int("rounds", 5, "maximum supersteps")
	budget := flag.Float64("budget", 2.00, "stage budget in USD")
	flag.Parse()

	p, err := build(*rounds, *budget)
	if err != nil {
		log.Fatal(err)
	}

	opts := []loom.Option{
		loom.WithRegistry(registry()),
		loom.WithBroadcast("question", question),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 5}),
		// The step parses its output as JSON, so the fields it adds are chosen
		// by the model and no plan can know them — which would leave the
		// filter below it dropping every record during projection and the
		// synthesis priced at zero. Naming the fields once makes the whole
		// projection a bound again instead of a floor.
		loom.WithStageSample("explore", map[string]any{
			"finding": strings.Repeat("x", 120), "follow": []any{"p4"},
		}),
	}
	if *state != "" {
		opts = append(opts, loom.WithStateDir(*state))
	}

	// Price the loop before running it. The round count is not knowable, so
	// this prices the cap — deliberately an over-estimate, and the only
	// direction it is safe to be wrong in.
	proj, err := loom.Explain(p, opts...)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(proj)
	fmt.Println()

	res, err := loom.Run(context.Background(), p, opts...)
	if err != nil {
		log.Fatalf("run: %v", err)
	}

	report(res, proj)
}

func build(rounds int, budget float64) (*pipeline.Pipeline, error) {
	// The algorithm. Edges come from the records — a paper's own "cites"
	// field, so the corpus carries its own graph — while the messages come
	// from the model: "follow" is a field the step writes, which is what makes
	// the walk's shape discovered rather than declared.
	walk, err := algo.NewBSP(algo.BSPConfig{
		Edges: algo.EdgesFromField("cites"),
		Emit:  algo.MessagesFromField("follow"),
		Seeds: func(r core.Record) bool { return r.ID == "p1" || r.ID == "p3" },
		// One paper cannot drag more than three others in behind it.
		MaxMessages: 3,
		Directed:    true,
	})
	if err != nil {
		return nil, err
	}

	p := pipeline.New("multi-hop")
	reached := p.FromRecords("seed", corpus).Iterate("explore", pipeline.IterateSpec{
		Step: pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast,
				Escalation: []string{"deep"}},
			System: "You are assessing papers against a research question.",
			// The question is a broadcast, so it is stored once and referenced
			// by hash rather than copied into every task — and it is in the
			// shared prefix, so the provider caches it across every call this
			// stage makes, in every round.
			Prefix: `Research question:
{{broadcast "question"}}`,
			Prompt: `Paper {{.title}}
{{.abstract}}
{{if .Inbox}}
Passed to you by papers that cite this one:
{{range .Inbox}}- {{.}}
{{end}}{{end}}
State what this paper contributes to the question in one line ("finding"), and
list the papers worth following ("follow").`,
			ParseJSON: true,
			MaxTokens: 256,
		},
		Algorithm: walk,
		// The open world: p7 cites p9, which the corpus does not contain.
		// Without this the edge is dropped and counted; with it the walk
		// creates the vertex from the messages that reached it.
		Grow: func(id string, msgs []algo.Message) (core.Record, error) {
			return core.NewRecord(id, map[string]any{
				"title":    id + " (discovered)",
				"abstract": "Not in the corpus. Reached via: " + strings.Join(algo.Bodies(msgs), "; "),
				"cites":    []any{},
			}), nil
		},
		// All three bounds, which is the design rather than an abundance of
		// options: quiet is implicit, and these two are what stop a loop that
		// does not converge.
		Halt:        pipeline.HaltWhen{MaxRounds: rounds, Budget: core.Budget{MaxCostUSD: budget}},
		MaxInbox:    4,
		MaxFrontier: 6,
	}, pipeline.WithBroadcast("question"))

	reached.
		Filter("relevant", func(r core.Record) (bool, error) {
			return strings.TrimSpace(r.String("finding")) != "", nil
		}).
		ReduceAI("synthesize", pipeline.ReduceAISpec{
			Binding:   model.Binding{Tier: model.TierDeep},
			Prefix:    "Research question:\n{{broadcast \"question\"}}",
			Prompt:    "Findings:\n{{range .Items}}- {{.}}\n{{end}}\nAnswer the question.",
			ItemField: "finding",
			FanIn:     4,
			MaxTokens: 512,
		}, pipeline.WithBroadcast("question"))

	return p, nil
}

func report(res *loom.RunResult, proj *loom.Projection) {
	it, ok := res.Iteration("explore")
	if !ok {
		log.Fatal("no iteration report")
	}

	fmt.Print(it)
	fmt.Println()

	// The claim the design rests on: rounds get cheaper as the walk converges,
	// because a paper that has nothing new to say stops being scheduled at all.
	fmt.Println("papers the walk reached, in the order it reached them:")
	for _, r := range res.StageOutputs["explore"] {
		finding := r.String("finding")
		if finding == "" {
			finding = "(never activated)"
		}
		fmt.Printf("  %-4s %-46s %s\n", r.ID, clipTo(r.String("title"), 46), clipTo(finding, 60))
	}
	fmt.Println()

	if len(res.Output) > 0 {
		fmt.Println("synthesis:")
		fmt.Println(" ", res.Output[0].String("output"))
		fmt.Println()
	}

	spent := res.Spent
	ceiling := proj.Ceiling()
	fmt.Printf("projected ceiling %d tokens / $%.4f at the round cap\n",
		ceiling.TotalTokens(), ceiling.CostUSD)
	fmt.Printf("actually spent    %d tokens / $%.4f over %d round(s), halted: %s\n",
		spent.TotalTokens(), spent.CostUSD, it.Rounds, it.Halt)
	if spent.CacheReadTokens > 0 {
		fmt.Printf("prompt prefix served %d tokens from the provider's cache\n",
			spent.CacheReadTokens)
	}
	if hits := cacheHits(res); hits > 0 {
		fmt.Printf("%d task(s) replayed from the result cache: rerunning a converged "+
			"loop costs nothing\n", hits)
	}
	fmt.Println()
	fmt.Print(res.Report)
}

func cacheHits(res *loom.RunResult) int {
	var n int
	for _, s := range res.Report.Stages {
		n += s.CacheHits
	}
	return n
}

func clipTo(s string, n int) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// --- the simulated model -------------------------------------------------

// registry wires a deterministic mock in place of a provider. It reads the
// prompt the way a model would and answers as a model would: papers that
// measure something contribute a finding and point at what to read next;
// papers that do not, say so and point nowhere, which is how the walk goes
// quiet without being told to.
//
// The mocks are priced and rate-limited like real models, because the numbers
// this example is about — the projection, the stage budget, the admission
// floor — are all computed from the registry. A free model would make every
// one of them zero and the demonstration vacuous.
func registry() *model.Registry {
	reg := model.NewRegistry()
	for _, m := range []struct {
		id      string
		tier    model.Tier
		pricing model.Pricing
		limits  model.Limits
	}{
		{"fast", model.TierFast,
			model.Pricing{InputPerMTok: 1.00, OutputPerMTok: 5.00},
			model.Limits{RequestsPerMinute: 1000, TokensPerMinute: 200_000}},
		{"deep", model.TierDeep,
			model.Pricing{InputPerMTok: 15.00, OutputPerMTok: 75.00},
			model.Limits{RequestsPerMinute: 200, TokensPerMinute: 80_000}},
	} {
		if err := reg.Register(model.Info{
			ID: m.id, Tier: m.tier, Pricing: m.pricing, Limits: m.limits,
			Provider: model.NewMock(m.id, model.WithHandler(answer)),
		}); err != nil {
			log.Fatal(err)
		}
	}
	return reg
}

func answer(req model.Request) (string, error) {
	if strings.Contains(req.Prompt, "Findings:") {
		return synthesis(req.Prompt), nil
	}

	id := paperID(req.Prompt)
	rec, known := lookup(id)
	if !known {
		return `{"finding": "Reached by citation but not in the corpus.", "follow": []}`, nil
	}

	abstract := rec.String("abstract")
	// A paper with no measurement has nothing to pass on: no follow, so it
	// votes to halt. This is the rule that makes the frontier shrink.
	if !strings.Contains(abstract, "Finds") && !strings.Contains(abstract, "Reports") &&
		!strings.Contains(abstract, "Measures") && !strings.Contains(abstract, "Shows") &&
		!strings.Contains(abstract, "Breaks") {
		return fmt.Sprintf(`{"finding": %q, "follow": []}`,
			"No measurement bearing on the question."), nil
	}

	var follow []string
	for _, c := range algo.Strings(rec, "cites") {
		follow = append(follow, c)
	}
	sort.Strings(follow)
	quoted := make([]string, len(follow))
	for i, f := range follow {
		quoted[i] = fmt.Sprintf("%q", f)
	}
	return fmt.Sprintf(`{"finding": %q, "follow": [%s]}`,
		abstract, strings.Join(quoted, ", ")), nil
}

// paperID recovers which paper a rendered prompt is about. A real provider
// would just read it; this is the mock standing in for comprehension.
func paperID(prompt string) string {
	for _, r := range corpus {
		if strings.Contains(prompt, r.String("title")) {
			return r.ID
		}
	}
	for _, line := range strings.Split(prompt, "\n") {
		if title, ok := strings.CutPrefix(line, "Paper "); ok {
			return strings.TrimSpace(strings.TrimSuffix(title, " (discovered)"))
		}
	}
	return ""
}

func lookup(id string) (core.Record, bool) {
	for _, r := range corpus {
		if r.ID == id {
			return r, true
		}
	}
	return core.Record{}, false
}

// synthesis answers the question only when p7's finding is among the items.
//
// That is the point of the example rather than a detail of the mock. p7 is
// three hops from the seeds — p3 → p4 → p7 — and it is the only paper that
// closes the question. A walk cut off at two rounds gathers findings from four
// papers and still cannot answer, which is what makes this a multi-hop
// workload rather than a fan-out with extra steps.
func synthesis(prompt string) string {
	if strings.Contains(prompt, "consumes the latency") {
		// The answer repeats the phrase it turned on, which is not a trick:
		// the reduce is a tree, so a level-1 aggregate is the input to level 2,
		// and a summarizer that drops the finding the question turns on has
		// not summarized. Carrying it is what lets the signal survive the
		// climb.
		return "Sparse retrieval reduces retrieved tokens, but reranking " +
			"consumes the latency it saves, so end-to-end latency improves far " +
			"less than retrieval latency does. That rests on p7, three hops " +
			"from anything the question named."
	}
	return "Retrieval cost falls. Whether that reaches end-to-end latency is " +
		"not settled by the papers reached so far."
}
