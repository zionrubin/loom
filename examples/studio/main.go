// Command studio opens Loom Studio on the vertical-digest pipeline: the same
// pipeline examples/vertical-digest writes in Go, held as a document you can
// edit on a canvas, priced from the records on disk before anything is spent,
// and exported back to Go.
//
//	go run ./examples/studio
//	# then open http://localhost:8078
//
// With no flags it invents a small archive of daily chat files in a temp
// directory and registers deterministic mock models, so everything works
// offline and with no API key: the price on the header is real arithmetic over
// real records at made-up per-token rates. Point it at your own archive with
// -messages, and give it real models with -openai.
//
// The Run button runs what the canvas draws — both passes — and streams the
// run into the constellation view next door, which is what the RUN tab opens.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/providers/openai"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/studio"
	"github.com/zionrubin/loom/viz"
)

func main() {
	messages := flag.String("messages", "", "archive root: <vertical>/<date>.jsonl (empty invents one)")
	docPath := flag.String("doc", "", "JSON document to open and autosave (empty starts from the built-in one)")
	addr := flag.String("addr", "localhost:8078", "address for the studio")
	vizAddr := flag.String("viz", "localhost:8077", "address for the constellation view (empty to disable)")
	out := flag.String("out", "reports", "directory the write step puts reports in")
	budget := flag.Float64("budget", 15, "hard cost cap in USD")
	useOpenAI := flag.Bool("openai", false, "use real OpenAI models (needs OPENAI_API_KEY)")
	state := flag.String("state", os.Getenv("LOOM_STATE"), "state dir for cache/resume")
	flag.Parse()

	root := *messages
	if root == "" {
		var err error
		root, err = inventArchive(24)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("invented an archive at %s (3 verticals × 24 days)\n", root)
	}

	reg, secrets, err := registry(*useOpenAI)
	if err != nil {
		log.Fatal(err)
	}

	doc := digestDoc(root, *out, *budget, *useOpenAI)
	if *docPath != "" {
		if loaded, err := studio.Load(*docPath); err == nil {
			doc = loaded
			fmt.Printf("opened %s\n", *docPath)
		} else if !os.IsNotExist(err) {
			log.Fatal(err)
		}
	}

	// The constellation view is the other half of the pair: the studio builds
	// and prices, the constellation watches. They are two servers because they
	// are two jobs, and the RUN tab is a link from one to the other.
	var v *viz.Server
	vizURL := ""
	if *vizAddr != "" {
		v = viz.New()
		if vizURL, err = v.Start(*vizAddr); err != nil {
			log.Fatal(err)
		}
		defer v.Close()
	}

	opts := []studio.Option{
		studio.Models(reg),
		studio.Constellation(vizURL),
		studio.Runner(runner(reg, secrets, v, *state)),
	}
	if *docPath != "" {
		opts = append(opts, studio.File(*docPath))
	}
	s := studio.New(doc, opts...)
	url, err := s.Start(*addr)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	fmt.Printf("\nloom studio:        %s\n", url)
	if vizURL != "" {
		fmt.Printf("constellation view: %s\n", vizURL)
	}
	fmt.Printf("\n%s\n", s.Estimate())
	fmt.Println("\nedit the canvas, watch the price move, press ⌘K to ask, Run ▸ to run it. Ctrl-C to exit.")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
}

// runner is what the Run button does: run the first pass, then — if the
// document merges branches — the second one, both against the same registry
// and the same constellation view.
func runner(reg *model.Registry, secrets map[security.SecretRef]string, v *viz.Server, stateDir string) studio.RunFunc {
	return func(ctx context.Context, r studio.RunRequest) error {
		opts := []loom.Option{
			loom.WithRegistry(reg),
			loom.WithRunBudget(core.Budget{MaxCostUSD: r.Doc.CapUSD}),
			loom.WithWorkers(max(1, r.Doc.Workers)),
		}
		if r.Doc.KeepGoing {
			opts = append(opts, loom.WithContinueOnError())
		}
		if len(secrets) > 0 {
			opts = append(opts, loom.WithSecrets(secrets))
		}
		if stateDir != "" {
			opts = append(opts, loom.WithStateDir(stateDir))
		}
		if v != nil {
			opts = append(opts, loom.WithEventHandler(func(e observe.Event) { v.Handle(e) }))
		}

		fmt.Printf("\nrunning %q — priced at %s\n", r.Doc.Name, r.Estimate)
		res, err := loom.Run(ctx, r.Pipeline, opts...)
		if err != nil && res == nil {
			return err
		}
		if err != nil {
			fmt.Printf("first pass ended with %v (spent $%.4f)\n", err, res.Spent.CostUSD)
		}
		fmt.Print(res.Report)

		second, err := r.Second(res.StageOutputs)
		if err != nil {
			return err
		}
		if second == nil {
			return nil
		}
		res2, err := loom.Run(ctx, second, opts...)
		if err != nil && res2 == nil {
			return err
		}
		fmt.Print(res2.Report)
		for _, rec := range res2.Output {
			fmt.Printf("\n--- %s ---\n%s\n", second.Name, rec.String("output"))
		}
		return nil
	}
}

// digestDoc is the vertical-digest pipeline as a studio document: the same
// shape examples/vertical-digest builds in Go, one card per step.
func digestDoc(root, out string, budget float64, real bool) *studio.Doc {
	fast, mid, deep := "mock-fast", "mock-mid", "mock-deep"
	if real {
		fast, mid, deep = "", "gpt-5.4-mini", "gpt-5.4"
	}
	digest := &studio.InferSpec{
		Tier: "cheapest", Escalate: []string{mid},
		System: "You are a business analyst digesting one day of an internal team chat channel. " +
			"Messages may be in English or Hebrew; always respond in English.",
		Prompt: "One day of internal chat from the \"{{.vertical}}\" vertical, {{.date}} " +
			"({{.count}} messages). Senders are anonymized as S1, S2, …\n\n{{.messages}}",
		MaxTokens: 400, Workers: 8,
		Answer: []studio.Answer{
			{Name: "summary", Note: "3-5 sentences: what happened, decisions made, problems raised", Required: true},
			{Name: "topics", Kind: "list", Note: "up to 5 short topic labels"},
			{Name: "signals", Kind: "list", Note: "up to 3 notable risks, wins or blockers; empty if none"},
		},
	}
	if !real {
		digest.Model, digest.Tier = fast, ""
	}

	steps := []studio.Step{
		{ID: "load-days", Kind: studio.KindSource, Title: "Load days",
			Note: "One file per vertical per day",
			Source: &studio.SourceSpec{
				From: "folder", Root: root, Match: "*.jsonl",
				Line:   "{{clock .createTime}} {{.sender}}: {{.text}}",
				Scrub:  []string{"speakers", "emails"},
				Fields: map[string]string{"group": "vertical", "name": "date", "text": "messages"},
			}},
		{ID: "daily-digest", Kind: studio.KindInfer, Title: "Daily digest",
			Note: "Summary, topics, signals per day", From: "load-days", Infer: digest},
		{ID: "digest-line", Kind: studio.KindDerive, Title: "One line each",
			Note: "Date, summary, topics, signals", From: "daily-digest",
			Field: &studio.FieldSpec{Name: "digest_line",
				Template: "[{{.date}}] {{.summary}} | topics: {{join .topics}} | signals: {{join .signals}}"}},
	}

	verticals := []string{"payments", "logistics", "retail"}
	for _, v := range verticals {
		steps = append(steps,
			studio.Step{ID: "only-" + v, Kind: studio.KindFilter, Title: titleCase(v) + " only",
				From: "digest-line", Keep: &studio.Cond{Field: "vertical", Op: "is", Value: v}},
			studio.Step{ID: "rollup-" + v, Kind: studio.KindReduce, Title: titleCase(v) + " rollup",
				Note: "12 days at a time, in a tree", From: "only-" + v,
				Reduce: &studio.ReduceSpec{
					Model:  mid,
					System: "You roll up daily digests from one business vertical into a concise report.",
					Cover: []string{
						"Overall status and trajectory of the vertical",
						"Main themes discussed",
						"Recurring patterns",
						"Notable subjects that deserve attention",
					},
					Words: 400, FanIn: 12, MaxTokens: 900, ItemField: "digest_line",
				}})
	}

	steps = append(steps,
		studio.Step{ID: "business-overview", Kind: studio.KindReduce, Title: "Business overview",
			Note: "One page for leadership", Merge: []string{"rollup-payments", "rollup-logistics", "rollup-retail"},
			Reduce: &studio.ReduceSpec{
				Model:  deep,
				System: "You write crisp one-page executive summaries for company leadership.",
				Cover: []string{
					"Headline status across all verticals",
					"Status by vertical",
					"What teams are talking about",
					"Common patterns",
					"Subjects that deserve attention",
				},
				MaxTokens: 2000,
			}},
		studio.Step{ID: "write-overview", Kind: studio.KindWrite, Title: "Write the one-pager",
			From:  "business-overview",
			Write: &studio.WriteSpec{Dir: out, Name: "business-overview.md", Body: "{{.output}}"}},
	)

	doc := &studio.Doc{
		Name: "vertical-digest", CapUSD: budget, Workers: 8, KeepGoing: true,
		Note:  "Daily chat, digested per day, rolled up per vertical, fused into one page.",
		Steps: steps,
	}
	doc.Layout()
	return doc
}

// titleCase capitalizes the first letter, which is all the card titles need.
func titleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// registry wires either three priced mock models or the real OpenAI ones.
func registry(real bool) (*model.Registry, map[security.SecretRef]string, error) {
	reg := model.NewRegistry()
	if real {
		key := os.Getenv("OPENAI_API_KEY")
		if key == "" {
			return nil, nil, fmt.Errorf("-openai needs OPENAI_API_KEY")
		}
		if err := openai.RegisterDefaults(reg, model.Limits{RequestsPerMinute: 200}); err != nil {
			return nil, nil, err
		}
		return reg, map[security.SecretRef]string{openai.DefaultSecretRef: key}, nil
	}
	for _, m := range []struct {
		id       string
		tier     model.Tier
		in, out  float64
		latencyM int
	}{
		{"mock-fast", model.TierFast, 0.10, 0.40, 120},
		{"mock-mid", model.TierBalanced, 0.60, 2.40, 300},
		{"mock-deep", model.TierDeep, 3.00, 12.00, 700},
	} {
		wait := m.latencyM
		p := model.NewMock(m.id, model.WithHandler(func(req model.Request) (string, error) {
			time.Sleep(time.Duration(wait+rand.Intn(wait)) * time.Millisecond)
			return respond(req), nil
		}))
		err := reg.Register(model.Info{
			ID: m.id, Provider: p, Tier: m.tier,
			Pricing: model.Pricing{InputPerMTok: m.in, OutputPerMTok: m.out},
			Limits:  model.Limits{RequestsPerMinute: 600},
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return reg, nil, nil
}

// respond is the offline "model": the JSON the digest step asks for, prose for
// the folds.
func respond(req model.Request) string {
	if strings.Contains(req.Prompt, `"summary"`) {
		topic := "throughput"
		switch {
		case strings.Contains(req.Prompt, "chargeback"):
			topic = "chargebacks"
		case strings.Contains(req.Prompt, "carrier"):
			topic = "carriers"
		case strings.Contains(req.Prompt, "inventory"):
			topic = "inventory"
		}
		return fmt.Sprintf(`{"summary": "The team worked through %s and agreed on the next step. `+
			`Two decisions were made and one blocker was raised.", "topics": [%q, "planning"], `+
			`"signals": [%q]}`, topic, topic, topic+" under pressure")
	}
	n := strings.Count(req.Prompt, "- ")
	return fmt.Sprintf("Status is steady across %d digests. Themes: throughput, carriers, "+
		"chargebacks. Recurring pattern: issues raised in the morning, resolved by the "+
		"afternoon. Worth attention: the contract renewal.", n)
}

// inventArchive writes a small chat archive of days days per vertical, so the
// example runs with nothing installed and nothing configured.
func inventArchive(days int) (string, error) {
	root, err := os.MkdirTemp("", "loom-studio-*")
	if err != nil {
		return "", err
	}
	people := map[string][]string{
		"payments":  {"Dana", "Roi", "Yael", "Amir"},
		"logistics": {"Noa", "Tom", "Lior"},
		"retail":    {"Maya", "Gil", "Shira", "Eitan", "Adi"},
	}
	lines := map[string][]string{
		"payments": {
			"chargebacks are up %d%% week over week",
			"opened a ticket with the PSP, ping finance@example.com if it stalls",
			"the retry budget is the thing biting us, not the gateway",
			"decision: hold the rollout until the chargeback dashboard is right",
		},
		"logistics": {
			"carrier API is throttling us again",
			"down to %d retries, queue is holding",
			"contract renewal is the real blocker here",
			"decision: move the nightly sync an hour earlier",
		},
		"retail": {
			"store %d shows inventory drift again",
			"recount scheduled for tomorrow morning",
			"the promo ran out of stock in two regions",
			"decision: freeze the promo until the recount lands",
		},
	}
	rnd := rand.New(rand.NewSource(7))
	start := time.Date(2026, 7, 8, 0, 0, 0, 0, time.UTC)
	for vertical, names := range people {
		dir := filepath.Join(root, vertical)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", err
		}
		for d := 0; d < days; d++ {
			day := start.AddDate(0, 0, d)
			var out []string
			for i := 0; i < 8+rnd.Intn(14); i++ {
				at := day.Add(time.Duration(8+i/2)*time.Hour + time.Duration(rnd.Intn(59))*time.Minute)
				text := lines[vertical][rnd.Intn(len(lines[vertical]))]
				if strings.Contains(text, "%d") {
					text = fmt.Sprintf(text, 3+rnd.Intn(20))
				}
				out = append(out, fmt.Sprintf(`{"sender":%q,"createTime":%q,"text":%q}`,
					names[rnd.Intn(len(names))], at.Format(time.RFC3339), text))
			}
			path := filepath.Join(dir, day.Format("2006-01-02")+".jsonl")
			if err := os.WriteFile(path, []byte(strings.Join(out, "\n")+"\n"), 0o644); err != nil {
				return "", err
			}
		}
	}
	return root, nil
}
