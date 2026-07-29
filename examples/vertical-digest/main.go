// Command vertical-digest analyzes exported Google Chat messages, one JSONL
// file per vertical per day, and produces a one-page executive summary of the
// state of the business plus a rollup report per vertical.
//
// Stage plan (run 1, one branch per vertical):
//
//	load-days → daily-digest (nano→mini) → digest-line ─┬→ only-<v> → rollup-<v> (mini)
//	                                                    └→ ... one branch per vertical
//
// Run 2 fuses the per-vertical rollups into the final one-pager (gpt-5.4);
// loom DAGs fan out but do not fan back in, so the synthesis is a second run.
//
//	OPENAI_API_KEY=sk-... go run ./examples/vertical-digest \
//	    -messages /path/to/messages -out reports -budget 15
//
// Sender IDs are anonymized to stable per-day labels (S1, S2, ...) and email
// addresses in message bodies are redacted before anything leaves the machine.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
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

var emailRe = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)

// chatMsg is the subset of the Google Chat export schema the digest needs.
type chatMsg struct {
	Sender struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"sender"`
	CreateTime string `json:"createTime"`
	Text       string `json:"text"`
}

// dayFile is one vertical+date JSONL file.
type dayFile struct {
	Vertical string
	Date     string // YYYY-MM-DD, from the filename
	Path     string
}

// discover walks root and returns every per-day file (optionally clamped to
// [since, until], and to the `last` most recent days per vertical) plus the
// sorted list of verticals that have at least one.
func discover(root, since, until string, last int) ([]dayFile, []string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, err
	}
	var files []dayFile
	seen := map[string]bool{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		vertical := e.Name()
		days, err := os.ReadDir(filepath.Join(root, vertical))
		if err != nil {
			return nil, nil, err
		}
		for _, d := range days {
			if d.IsDir() || !strings.HasSuffix(d.Name(), ".jsonl") {
				continue
			}
			date := strings.TrimSuffix(d.Name(), ".jsonl")
			if since != "" && date < since {
				continue
			}
			if until != "" && date > until {
				continue
			}
			files = append(files, dayFile{Vertical: vertical, Date: date, Path: filepath.Join(root, vertical, d.Name())})
			seen[vertical] = true
		}
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Vertical != files[j].Vertical {
			return files[i].Vertical < files[j].Vertical
		}
		return files[i].Date < files[j].Date
	})
	if last > 0 {
		// Keep only the `last` most recent days per vertical. Files are
		// sorted (vertical, date), so each vertical's tail is what stays.
		byVertical := map[string]int{}
		for _, f := range files {
			byVertical[f.Vertical]++
		}
		kept := files[:0]
		seenSoFar := map[string]int{}
		for _, f := range files {
			seenSoFar[f.Vertical]++
			if seenSoFar[f.Vertical] > byVertical[f.Vertical]-last {
				kept = append(kept, f)
			}
		}
		files = kept
	}
	verticals := make([]string, 0, len(seen))
	for v := range seen {
		verticals = append(verticals, v)
	}
	sort.Strings(verticals)
	return files, verticals, nil
}

// loadDay parses one JSONL file into an anonymized chronological transcript.
// Sender IDs become stable per-day labels (S1, S2, ...) in order of first
// appearance; emails in message bodies are redacted.
func loadDay(f dayFile) (core.Record, error) {
	raw, err := os.ReadFile(f.Path)
	if err != nil {
		return core.Record{}, err
	}
	labels := map[string]string{}
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var m chatMsg
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			continue // tolerate the odd malformed line
		}
		text := strings.TrimSpace(m.Text)
		if text == "" {
			continue
		}
		text = emailRe.ReplaceAllString(text, "<email>")
		if len(text) > 2000 {
			text = text[:2000] + " …"
		}
		who, ok := labels[m.Sender.Name]
		if !ok {
			who = fmt.Sprintf("S%d", len(labels)+1)
			labels[m.Sender.Name] = who
		}
		clock := m.CreateTime
		if len(clock) >= 16 {
			clock = clock[11:16] // HH:MM
		}
		lines = append(lines, fmt.Sprintf("%s %s: %s", clock, who, text))
	}
	return core.NewRecord(f.Vertical+"/"+f.Date, map[string]any{
		"vertical": f.Vertical,
		"date":     f.Date,
		"count":    len(lines),
		"messages": strings.Join(lines, "\n"),
	}), nil
}

func joinAny(v any) string {
	items, _ := v.([]any)
	parts := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok && s != "" {
			parts = append(parts, s)
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

// buildPipeline assembles run 1: daily digests, then one rollup branch per
// vertical. The final cross-vertical synthesis happens in a second run.
func buildPipeline(files []dayFile, verticals []string) *pipeline.Pipeline {
	p := pipeline.New("vertical-digest")

	src := p.FromFunc("load-days", func(ctx context.Context) ([]core.Record, error) {
		recs := make([]core.Record, 0, len(files))
		for _, f := range files {
			r, err := loadDay(f)
			if err != nil {
				return nil, err
			}
			if r.Data["count"].(int) == 0 {
				continue
			}
			recs = append(recs, r)
		}
		return recs, nil
	})

	digested := src.Infer("daily-digest", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"gpt-5.4-mini"}},
		System: "You are a business analyst digesting one day of an internal team chat channel. " +
			"Messages may be in English or Hebrew; always respond in English. " +
			"Respond with a single JSON object and nothing else.",
		Prompt: `One day of internal chat from the "{{.vertical}}" vertical, {{.date}} ({{.count}} messages). Senders are anonymized as S1, S2, ...

{{.messages}}

Respond with JSON:
{"summary": "<3-5 sentences: what happened, decisions made, problems raised, metrics mentioned>",
 "topics": ["<up to 5 short topic labels>"],
 "signals": ["<up to 3 notable risks, wins, blockers, or metric changes; empty if none>"]}`,
		MaxTokens: 400,
		ParseJSON: true,
		Validate: func(r core.Record) error {
			if strings.TrimSpace(r.String("summary")) == "" {
				return fmt.Errorf("empty summary")
			}
			return nil
		},
	}, pipeline.WithParallelism(8))

	// Pure Go: compress each digest into one line the reducers consume.
	lined := digested.Map("digest-line", func(r core.Record) (core.Record, error) {
		out := r.Clone()
		out.Data["digest_line"] = fmt.Sprintf("[%s] %s | topics: %s | signals: %s",
			r.String("date"), r.String("summary"), joinAny(r.Data["topics"]), joinAny(r.Data["signals"]))
		return out, nil
	}, pipeline.WithVersion("v1"))

	for _, v := range verticals {
		v := v
		lined.
			Filter("only-"+v, func(r core.Record) (bool, error) {
				return r.String("vertical") == v, nil
			}, pipeline.WithVersion("v1")).
			ReduceAI("rollup-"+v, pipeline.ReduceAISpec{
				Binding: model.Binding{Model: "gpt-5.4-mini"},
				System:  "You are a senior business analyst rolling up daily digests from one business vertical into a concise report.",
				Prompt: fmt.Sprintf(`Below are {{.Count}} daily digests (or partial rollups) from the %q vertical, in chronological order:
{{range .Items}}- {{.}}
{{end}}
Write a concise markdown rollup covering:
1. Overall status and trajectory of the vertical.
2. Main themes discussed.
3. Recurring patterns.
4. Notable subjects that deserve attention.
Reference dates where meaningful. Maximum ~400 words.`, v),
				FanIn:     12,
				MaxTokens: 900,
				ItemField: "digest_line",
			})
	}
	return p
}

// buildOverview assembles run 2: one ReduceAI over the per-vertical rollups.
func buildOverview(rollups []core.Record) *pipeline.Pipeline {
	p := pipeline.New("business-overview")
	p.FromRecords("vertical-rollups", rollups).
		ReduceAI("one-pager", pipeline.ReduceAISpec{
			Binding: model.Binding{Model: "gpt-5.4"},
			System:  "You write crisp one-page executive summaries for company leadership.",
			Prompt: `Below are rollup reports from {{.Count}} business verticals:

{{range .Items}}{{.}}

---

{{end}}
Write a one-page executive summary in markdown:

# Business Overview
<headline status across all verticals>

## Status by Vertical
<1-2 sentences per vertical>

## What Teams Are Talking About
<the dominant discussion themes>

## Common Patterns
<patterns that repeat across verticals>

## Subjects That Deserve Attention
<the most interesting or urgent subjects leadership should look into>

Keep it to roughly one page.`,
			MaxTokens: 2000,
			ItemField: "item",
		})
	return p
}

func main() {
	messages := flag.String("messages", "/Users/zion.rubin/Desktop/industryos/scripts/output/messages", "root directory: <vertical>/<date>.jsonl")
	out := flag.String("out", "reports", "output directory for the generated reports")
	budget := flag.Float64("budget", 15, "hard cost cap in USD for the digest run")
	workers := flag.Int("workers", 8, "concurrent workers")
	rpm := flag.Int("rpm", 200, "per-model requests-per-minute admission limit")
	since := flag.String("since", "", "only include dates >= this (YYYY-MM-DD)")
	until := flag.String("until", "", "only include dates <= this (YYYY-MM-DD)")
	last := flag.Int("last", 0, "only include the N most recent days per vertical (0 = all)")
	state := flag.String("state", os.Getenv("LOOM_STATE"), "state dir for cache/resume (recommended for large runs)")
	addr := flag.String("addr", "localhost:8077", "address for the constellation view (empty to disable)")
	flag.Parse()

	key := os.Getenv("OPENAI_API_KEY")
	if key == "" {
		log.Fatal("set OPENAI_API_KEY to run this pipeline")
	}

	files, verticals, err := discover(*messages, *since, *until, *last)
	if err != nil {
		log.Fatal(err)
	}
	if len(files) == 0 {
		log.Fatalf("no <vertical>/<date>.jsonl files under %s", *messages)
	}
	fmt.Printf("found %d day-files across %d verticals: %s\n", len(files), len(verticals), strings.Join(verticals, ", "))

	reg := model.NewRegistry()
	if err := openai.RegisterDefaults(reg, model.Limits{RequestsPerMinute: *rpm}); err != nil {
		log.Fatal(err)
	}

	// Lightweight progress: handlers run on worker goroutines, so guard state.
	var mu sync.Mutex
	var calls int
	var cost float64
	progress := func(e observe.Event) {
		if e.Type != observe.ModelCalled && e.Type != observe.CacheHit {
			return
		}
		mu.Lock()
		calls++
		cost += e.Usage.CostUSD
		if calls%100 == 0 {
			fmt.Printf("  %d model calls, $%.4f so far (stage %s)\n", calls, cost, e.Stage)
		}
		mu.Unlock()
	}

	// Constellation view: events from both runs stream to the same page.
	handle := progress
	var v *viz.Server
	var vizURL string
	if *addr != "" {
		v = viz.New()
		url, err := v.Start(*addr)
		if err != nil {
			log.Fatal(err)
		}
		vizURL = url
		fmt.Printf("constellation view: %s\n", vizURL)
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
		if v.AwaitViewer(waitCtx) {
			time.Sleep(800 * time.Millisecond) // a beat, so the empty sky is visible first
		}
		cancelWait()
		handle = func(e observe.Event) {
			v.Handle(e)
			progress(e)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithSecrets(map[security.SecretRef]string{openai.DefaultSecretRef: key}),
		loom.WithRunBudget(core.Budget{MaxCostUSD: *budget}),
		loom.WithWorkers(*workers),
		loom.WithContinueOnError(),
		loom.WithEventHandler(handle),
	}
	if *state != "" {
		opts = append(opts, loom.WithStateDir(*state))
	}

	res, err := loom.Run(ctx, buildPipeline(files, verticals), opts...)
	if err != nil && res == nil {
		log.Fatal(err)
	}
	if err != nil {
		fmt.Printf("digest run ended with error: %v (spent $%.4f) — writing what completed\n", err, res.Spent.CostUSD)
	}
	if n := len(res.Failures); n > 0 {
		fmt.Printf("  %d task(s) dead-lettered; see run report\n", n)
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}

	// Collect per-vertical rollups; write each as its own report.
	var rollups []core.Record
	for _, v := range verticals {
		recs := res.StageOutputs["rollup-"+v]
		if len(recs) == 0 {
			fmt.Printf("  no rollup produced for %s (skipping)\n", v)
			continue
		}
		text := recs[0].String("output")
		path := filepath.Join(*out, v+".md")
		if err := os.WriteFile(path, []byte(fmt.Sprintf("# %s — vertical rollup\n\n%s\n", v, text)), 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("wrote %s\n", path)
		rollups = append(rollups, core.NewRecord("rollup-"+v, map[string]any{
			"item": fmt.Sprintf("## Vertical: %s\n\n%s", v, text),
		}))
	}
	if len(rollups) == 0 {
		log.Fatal("no vertical rollups produced; cannot synthesize the overview")
	}

	// Run 2: fuse the rollups into the one-page overview.
	res2, err := loom.Run(ctx, buildOverview(rollups),
		loom.WithRegistry(reg),
		loom.WithSecrets(map[security.SecretRef]string{openai.DefaultSecretRef: key}),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 2}),
		loom.WithWorkers(2),
		loom.WithEventHandler(handle),
	)
	if err != nil {
		log.Fatalf("overview run failed: %v", err)
	}
	overview := res2.Output[0].String("output")
	overviewPath := filepath.Join(*out, "business-overview.md")
	if err := os.WriteFile(overviewPath, []byte(overview+"\n"), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %s\n\n--- business overview ---\n%s\n", overviewPath, overview)

	report := res.Report.String() + "\n" + res2.Report.String()
	reportPath := filepath.Join(*out, "run-report.txt")
	if err := os.WriteFile(reportPath, []byte(report), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n--- run report ---\n%s\ntotal spend: $%.4f\n", report, res.Spent.CostUSD+res2.Spent.CostUSD)

	if v != nil {
		fmt.Printf("\nrun finished — still serving %s (Ctrl-C to exit)\n", vizURL)
		<-ctx.Done()
		_ = v.Close()
	}
}
