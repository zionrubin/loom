// Command longterm is the proving example for Loom's long-term memory: a
// support desk that is measurably better on Tuesday because of what it worked
// out on Monday.
//
// It is the workload the framework's other sharing mechanisms cannot express.
// A broadcast is fixed before the run and dies with it, so nothing a run
// concludes can inform the next one. A fleet's blackboard reaches across
// agents but not across processes. What a support desk needs is neither: a
// knowledge base that outlives every run, is too large to put in a prompt, and
// is therefore read by similarity rather than by name.
//
// The pipeline is four stages and two of them are the new ones:
//
//	tickets    the day's incoming tickets
//	similar    Recall: the k nearest past resolutions, as of the pinned epoch
//	draft      Infer: answer the ticket using what was recalled
//	learn      Remember: stage today's resolution for tomorrow's epoch
//
// It runs three days in a row against one store, and four things in the output
// are claims the design makes that this run either shows or does not:
//
//   - Day 1 recalls nothing. The knowledge base is empty, and the answers say
//     so. By day 3 every ticket recalls something, because the runs before it
//     wrote what they concluded.
//   - Nothing a run writes is visible to the run that wrote it. Every day's
//     recall happens at the epoch pinned before its first task, and the day's
//     own writes land in the next one. Watch the epoch move between days and
//     never within one.
//   - Every remembered item names the run, stage, and task that produced it.
//     The provenance table is the difference between a fact and something a
//     model said once.
//   - The last three sections are the design's central claim, executed. Day 3's
//     tickets are answered three times without writing anything back: once to
//     warm the cache, once against an unchanged knowledge base (free — 0 model
//     calls), and once after committing a single new billing fact, which the
//     stage's product filter puts in front of exactly one of the three tickets.
//     That third pass moves the epoch, so all three recalls recompute; but the
//     recalled item IDs live in the record, so only the one ticket whose
//     retrieved set actually changed pays for a model call. That is what
//     separating recall from inference buys: a knowledge base that can grow
//     without invalidating everything ever computed from it.
//
// The model and the embedder are both deterministic and offline, so this runs
// with no key and no network. The embedder measures lexical overlap rather
// than meaning — see memory.HashEmbedder — which is enough to show the
// mechanism and not enough to judge recall quality.
//
//	go run ./examples/longterm
//	go run ./examples/longterm -backend chromem      # the embedded vector DB
//	go run ./examples/longterm -state /tmp/loom-kb   # keep the knowledge base,
//	                                                 # then run again to watch
//	                                                 # day 1 arrive informed
//
// The tickets and resolutions are fixtures invented for this example.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/memory"
	chromemstore "github.com/zionrubin/loom/memory/chromem"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

// space is the memory partition this application reads and writes. Everything
// else in the store — another team's, another tenant's — is unreachable from
// this pipeline, because the envelope grants exactly this name.
const space = "resolutions"

// days is three days of support tickets. Later days deliberately echo earlier
// ones: that is what gives the knowledge base something to be useful about.
var days = [][]core.Record{
	{
		ticket("d1-1", "checkout", "payment declined but card is valid",
			"customer sees declined at checkout, bank says the charge was never attempted"),
		ticket("d1-2", "search", "search returns no results for valid product names",
			"searching for a product by exact name returns an empty page"),
	},
	{
		ticket("d2-1", "checkout", "card declined at checkout again",
			"another customer reports a decline the bank has no record of"),
		ticket("d2-2", "billing", "invoice pdf is missing line items",
			"the downloaded invoice shows a total but no breakdown"),
	},
	{
		ticket("d3-1", "checkout", "payment declined, bank has no record of the attempt",
			"same decline pattern, customer card is valid and has funds"),
		ticket("d3-2", "search", "searching by exact product name gives no results",
			"empty results for a product that exists in the catalogue"),
		ticket("d3-3", "billing", "invoice missing the line item breakdown",
			"invoice pdf downloads with a total and nothing itemized"),
	},
}

func ticket(id, product, subject, body string) core.Record {
	return core.NewRecord(id, map[string]any{
		"product": product, "subject": subject, "body": body,
	})
}

func main() {
	state := flag.String("state", "", "directory for the persistent cache and knowledge base")
	backend := flag.String("backend", "inmemory", "memory backend: inmemory | chromem")
	k := flag.Int("k", 3, "how many past resolutions to recall per ticket")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// The cache lives in the state directory, and the replay sections below
	// are about the cache, so the demo provisions one when the caller did not.
	dir := *state
	if dir == "" {
		tmp, err := os.MkdirTemp("", "loom-longterm-")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(tmp)
		dir = tmp
	}

	store, err := openStore(*backend, dir)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	fmt.Printf("long-term memory demo · %s backend · k=%d\n", *backend, *k)
	if *state == "" {
		fmt.Println("(no -state: the knowledge base is discarded when this exits)")
	}

	var totalCalls int
	for day, tickets := range days {
		res, calls, err := runDay(ctx, store, dir, *k, day+1, tickets, true)
		if err != nil {
			log.Fatal(err)
		}
		totalCalls += calls
		report(fmt.Sprintf("day %d", day+1), tickets, res, calls)
	}
	fmt.Printf("\n%d model call(s) across three days\n", totalCalls)
	provenance(ctx, store)

	// The sections the design is really about. All three answer day 3's
	// tickets without writing anything back, so the only thing that varies
	// between them is the knowledge base.
	last := days[len(days)-1]
	answer := func() (*loom.RunResult, int) {
		res, calls, err := runDay(ctx, store, dir, *k, 3, last, false)
		if err != nil {
			log.Fatal(err)
		}
		return res, calls
	}

	fmt.Println("\n── answer day 3 again, this time without learning ──────")
	res, calls := answer()
	report("replay", last, res, calls)
	fmt.Println("a different computation from the run above — it read a knowledge base\n" +
		"three epochs old rather than two — so it paid in full.")

	fmt.Println("\n── and again, nothing changed ─────────────────────────")
	res, calls = answer()
	report("replay", last, res, calls)
	fmt.Println("free: same epoch, same recalls, same records, same cache keys.")

	// One new fact, in the billing product. The recall stage filters by
	// product, so it lands in front of exactly one of day 3's three tickets —
	// no reliance on how the embedder happens to rank anything.
	if err := commitFact(ctx, store, "billing",
		"invoice missing line items → regenerate the pdf after the nightly rollup"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("\n── and again, after committing one billing fact ───────")
	res, calls = answer()
	report("replay", last, res, calls)
	fmt.Println("the epoch moved, so all three recalls recomputed — but the recalled IDs\n" +
		"live in the record, so only the ticket whose retrieved set actually changed\n" +
		"paid for a model call. That is what separating recall from inference buys.")
}

// commitFact adds one item to the knowledge base and publishes it, the way a
// human curating the store or another pipeline would.
func commitFact(ctx context.Context, store memory.Store, product, text string) error {
	e := memory.NewHashEmbedder(0)
	vecs, _, err := e.Embed(ctx, memory.Call{}, []string{text})
	if err != nil {
		return err
	}
	it := memory.NewItem(space, text, map[string]any{"product": product})
	it.Vector = vecs[0]
	it.Source = memory.Source{Stage: "curated"}
	if _, err := store.Upsert(ctx, []memory.Item{it}); err != nil {
		return err
	}
	_, err = store.Commit(ctx, space)
	return err
}

func openStore(backend, dir string) (memory.Store, error) {
	if dir != "" {
		dir = filepath.Join(dir, "kb")
	}
	switch backend {
	case "chromem":
		return chromemstore.Open(dir, false)
	case "inmemory":
		return memory.NewInMemory(dir)
	default:
		return nil, fmt.Errorf("unknown backend %q (want inmemory or chromem)", backend)
	}
}

// build assembles the day's pipeline. It is the same pipeline every day: what
// changes between days is the knowledge base behind it.
//
// learn adds the Remember stage. The replay sections below leave it off, so
// they read the knowledge base without touching it — a replay that staged
// items would leave them for the next commit to publish, and the point of
// those sections is that nothing changed.
func build(k int, tickets []core.Record, learn bool) *pipeline.Pipeline {
	p := pipeline.New("support-desk")

	drafted := p.FromRecords("tickets", tickets).
		// Retrieval is a stage, not a lookup inside the inference. That is
		// what puts the recalled item IDs into the record — and therefore into
		// the cache key of everything below — so growing the knowledge base
		// recomputes this cheap stage and leaves the expensive one alone for
		// every ticket whose neighbours did not move.
		Recall("similar", pipeline.RecallSpec{
			Space:    space,
			Query:    "{{.subject}}\n{{.body}}",
			K:        k,
			Filter:   map[string]string{"product": "{{.product}}"},
			MinScore: 0.15,
		}).
		Infer("draft", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			System:  "You answer support tickets.",
			Prompt: "past resolutions:\n{{.memory}}\n\n" +
				"ticket [{{.product}}]: {{.subject}}\n{{.body}}",
		})

	// What today concludes, tomorrow recalls. The write is staged: it becomes
	// visible at the next epoch, never to the run that made it.
	if learn {
		drafted.Remember("learn", pipeline.RememberSpec{
			Space: space,
			Text:  "{{.subject}} → {{.output}}",
			Meta:  map[string]string{"product": "{{.product}}"},
		})
	}

	return p
}

func runDay(ctx context.Context, store memory.Store, state string, k, day int,
	tickets []core.Record, commit bool) (*loom.RunResult, int, error) {

	// atomic: the scheduler runs a stage's tasks concurrently, so the mock is
	// called from several goroutines at once.
	var calls atomic.Int64
	reg, err := registry(&calls)
	if err != nil {
		return nil, 0, err
	}
	opts := []loom.Option{
		loom.WithRegistry(reg),
		// The embedder is nil, so the run uses memory.HashEmbedder: offline,
		// deterministic, and lexical rather than semantic.
		loom.WithMemory(store, nil),
		loom.WithStateDir(state),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 5}),
	}
	if !commit {
		// A replay should leave the knowledge base exactly as it found it.
		opts = append(opts, loom.WithoutMemoryCommit())
	}
	res, err := loom.Run(ctx, build(k, tickets, commit), opts...)
	if err != nil {
		return nil, int(calls.Load()), fmt.Errorf("day %d: %w", day, err)
	}
	return res, int(calls.Load()), nil
}

func report(label string, tickets []core.Record, res *loom.RunResult, calls int) {
	if strings.HasPrefix(label, "day") {
		fmt.Printf("\n── %s ──────────────────────────────────────────────\n", label)
	}
	fmt.Printf("read the knowledge base at epoch %d, committed epoch %d\n",
		res.Memory[space], res.Committed[space])

	recalled := map[string]int{}
	for _, r := range res.StageOutputs["similar"] {
		// A record served from the cache has been through JSON, so the ID list
		// arrives as []any rather than the []string the runner wrote.
		switch ids := r.Data["memory_ids"].(type) {
		case []string:
			recalled[r.ID] = len(ids)
		case []any:
			recalled[r.ID] = len(ids)
		}
	}
	for _, t := range tickets {
		n := recalled[t.ID]
		what := fmt.Sprintf("recalled %d", n)
		if n == 0 {
			what = "recalled nothing"
		}
		fmt.Printf("  %-6s [%-8s] %-52s %s\n", t.ID, t.String("product"),
			clip(t.String("subject"), 52), what)
	}
	fmt.Printf("%d model call(s), $%.4f, %d cache hit(s)\n",
		calls, res.Spent.CostUSD, cacheHits(res))
}

func cacheHits(res *loom.RunResult) int {
	var n int
	for _, s := range res.Report.Stages {
		n += s.CacheHits
	}
	return n
}

// provenance prints where the knowledge base came from. A store of model
// output is only usable later if each entry says which run wrote it.
func provenance(ctx context.Context, store memory.Store) {
	e := memory.NewHashEmbedder(0)
	vecs, _, err := e.Embed(ctx, memory.Call{}, []string{""})
	if err != nil {
		return
	}
	hits, err := store.Search(ctx, memory.Query{
		Space: space, Vector: vecs[0], K: 100, AsOf: memory.Latest,
	})
	if err != nil {
		return
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Item.Epoch != hits[j].Item.Epoch {
			return hits[i].Item.Epoch < hits[j].Item.Epoch
		}
		return hits[i].Item.ID < hits[j].Item.ID
	})

	fmt.Printf("\nknowledge base: %d item(s)\n", len(hits))
	fmt.Printf("  %-6s %-20s %-8s %s\n", "epoch", "run", "stage", "text")
	for _, h := range hits {
		// Run and stage are the join into the lineage log, which is where the
		// model that produced the text is recorded. A Remember stage issues no
		// model call of its own, so it cannot name one directly.
		fmt.Printf("  %-6d %-20s %-8s %s\n",
			h.Item.Epoch, h.Item.Source.RunID, h.Item.Source.Stage,
			clip(h.Item.Text, 60))
	}
}

// registry is a deterministic mock that answers from whatever the recall stage
// put in front of it, so the output visibly depends on the knowledge base.
func registry(calls *atomic.Int64) (*model.Registry, error) {
	reg := model.NewRegistry()
	_, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			if calls != nil {
				calls.Add(1)
			}
			context, _, _ := strings.Cut(req.Prompt, "\n\nticket [")
			context = strings.TrimPrefix(context, "past resolutions:\n")
			if strings.TrimSpace(context) == "" {
				return "no prior resolutions on file; escalating to a human", nil
			}
			return "applying the known fix: " + firstItem(context), nil
		}))
	if err != nil {
		return nil, err
	}
	return reg, nil
}

// firstItem pulls the text out of the best-scoring recalled item, which
// memory.Render formats as "[1] (mem_…) text".
func firstItem(rendered string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(rendered), "\n")
	if _, after, ok := strings.Cut(line, ") "); ok {
		line = after
	}
	if before, _, ok := strings.Cut(line, " → "); ok {
		line = before
	}
	return line
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
