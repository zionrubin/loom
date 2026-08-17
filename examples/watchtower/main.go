// Command watchtower is Loom's stream-mode example: an incident feed that never
// ends, graded per event and digested per minute.
//
// The pipeline is four stages, and only one of them is new:
//
//	incidents   a stream source — a directory of JSONL, read as it grows
//	grade       Infer, per record, the moment it arrives
//	per-minute  Window: the stage that turns an endless stream into finite sets
//	digest      ReduceAI over one window's incidents, once per pane
//
// The window is the whole idea. ReduceAI folds a set, and on a stream there is
// no end of input to tell it the set is closed; a window supplies one, and the
// watermark is the evidence. Everything upstream of the window runs per record
// with full pipelining — a grading call starts the moment an incident lands —
// and everything downstream runs once per pane.
//
// Four things are worth watching, because each is a claim the design makes:
//
//   - Panes fire on event time, not on wall clock. The feed is written with
//     timestamps spanning six minutes and delivered out of order by a few
//     seconds; the panes still come out one per minute per service, each
//     holding exactly the incidents that happened in it. Run it twice and the
//     panes are identical.
//
//   - A pane is the unit that costs money. The report prints panes next to
//     model calls: grading is one call per incident, digesting is one call per
//     pane. Halving the window doubles the second number and changes nothing
//     about the first, which is the trade -window lets you feel.
//
//   - An interrupted job resumes, and the replay is free. With -crash the job
//     is stopped mid-stream, then started again under the same job ID: it picks
//     up at the checkpointed offsets with its half-filled windows intact, and
//     the records it has to re-read cost zero model calls because the result
//     cache is keyed on content rather than on time. That is stream mode's
//     whole answer to at-least-once delivery — deliver twice, pay once.
//
//   - A job cancelled mid-window does not publish a partial answer. The open
//     window goes into the checkpoint instead of being fired half full; the
//     job that runs out of input does drain it, because there really is nothing
//     more coming. The stop reason in the report says which happened.
//
// The models are deterministic mocks, so this runs offline with no key, no
// network, and no cost:
//
//	go run ./examples/watchtower                      # backfill six minutes of feed
//	go run ./examples/watchtower -live                # tail it as a writer appends
//	go run ./examples/watchtower -crash               # stop mid-stream, resume, price the interruption
//	go run ./examples/watchtower -window 30s          # twice the panes, same gradings
//	go run ./examples/watchtower -key ""              # one digest per minute instead of per service
//
// # Watching it
//
// The constellation view draws a stream job as what it is rather than as a run
// that never finishes. The header carries the watermark instead of a progress
// bar — event time is the only clock a stream job obeys — with the split
// holding it back named beside it. A window stage draws one orbit per pane,
// newest outermost, so the stars downstream of it are grouped by the window
// that paid for them; the stage inspector lists the recent panes with what each
// one cost. And because a job that never ends would otherwise fill a browser
// tab, the sky holds only the recent past: the oldest settled tasks are
// forgotten while the counters that say what the job has done are kept.
//
//	go run ./examples/watchtower -live -view localhost:8077
//	# then open http://localhost:8077
//
// -live is the mode worth watching: -slow spaces the incidents out so the panes
// fire while you are looking at them.
//
// The incidents are synthetic: services, messages and timestamps are fixtures
// invented for this example.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/stream"
	"github.com/zionrubin/loom/stream/file"
	"github.com/zionrubin/loom/viz"
)

func main() {
	var (
		minutes  = flag.Int("minutes", 6, "minutes of incident feed to generate")
		rate     = flag.Int("rate", 8, "incidents per minute")
		window   = flag.Duration("window", time.Minute, "window size")
		lateness = flag.Duration("lateness", 20*time.Second, "how late an incident may arrive")
		keyField = flag.String("key", "service", "window key field (empty: one window per interval)")
		live     = flag.Bool("live", false, "tail the feed as a writer appends to it")
		crash    = flag.Bool("crash", false, "stop mid-stream, then resume and price the interruption")
		stateDir = flag.String("state", "", "state directory (default: a temporary one)")
		seed     = flag.Int64("seed", 7, "feed generator seed")
		addr     = flag.String("view", "", "serve the constellation view on this address (e.g. localhost:8077)")
		slow     = flag.Duration("slow", 0, "delay per model call, to watch panes fire in the view")
	)
	flag.Parse()

	// The view is an event handler like any other, so a job that is not being
	// watched runs exactly as it did before.
	handle := viewer(*addr)

	work, cleanup, err := workspace(*stateDir)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	feed := filepath.Join(work, "feed")
	out := filepath.Join(work, "digests")
	state := filepath.Join(work, "state")
	for _, dir := range []string{feed, out, state} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatal(err)
		}
	}

	incidents := generate(*seed, *minutes, *rate)
	fmt.Printf("watchtower: %d incidents over %d minutes, %d services\n",
		len(incidents), *minutes, len(services))

	cfg := desk_{
		size: *window, lateness: *lateness, key: *keyField,
		slow: *slow, view: handle,
	}
	// A stream job's sky is worth reading after it has stopped — the panes, what
	// each cost, where the watermark got to — so the view outlives the job.
	if handle != nil {
		defer holdOpen()
	}
	switch {
	case *live:
		runLive(feed, out, state, incidents, cfg)
	case *crash:
		runCrashAndResume(feed, out, state, incidents, cfg)
	default:
		if err := writeFeed(feed, incidents, len(incidents)); err != nil {
			log.Fatal(err)
		}
		runBackfill(feed, out, state, cfg)
	}
}

// desk_ bundles what every mode needs, so adding the view did not add a
// parameter to four signatures.
type desk_ struct {
	size     time.Duration
	lateness time.Duration
	key      string
	slow     time.Duration
	view     func(observe.Event)
}

// options assembles the loom options every mode shares.
func (c desk_) options(reg *model.Registry, extra ...loom.Option) []loom.Option {
	opts := []loom.Option{
		loom.WithRegistry(reg),
		// The source's out-of-orderness, which is a fact about the feed rather
		// than a preference: this generator delivers at most maxDisorder behind
		// event time, so a watermark that allows for it never declares a window
		// complete too early. Lower it and the report starts counting late
		// records — which is the point of counting them.
		loom.WithLateness(maxDisorder),
		loom.WithWorkers(8),
	}
	if c.view != nil {
		// Two observers on one bus: the terminal narration and the view.
		opts = append(opts, loom.WithEventHandler(func(e observe.Event) {
			panePrinterOnce(e)
			c.view(e)
		}))
	} else {
		opts = append(opts, loom.WithEventHandler(panePrinterOnce))
	}
	return append(opts, extra...)
}

// holdOpen keeps the process alive so the constellation view stays served after
// the job has stopped. Ctrl-C ends it.
func holdOpen() {
	fmt.Println("\nthe view is still serving — the job has stopped, its sky has not.")
	fmt.Println("press Ctrl-C to exit")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
}

// viewer starts the constellation view when an address was given, and returns
// the handler to feed it.
func viewer(addr string) func(observe.Event) {
	if addr == "" {
		return nil
	}
	v := viz.New()
	url, err := v.Start(addr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("constellation view: %s\n", url)
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if v.AwaitViewer(waitCtx) {
		time.Sleep(800 * time.Millisecond) // a beat, so the empty sky is visible first
	} else {
		fmt.Println("no viewer yet — running anyway (the page replays state on connect)")
	}
	return v.Handle
}

// --- The pipeline --------------------------------------------------------

// desk builds the incident desk. It is an ordinary Loom pipeline with one
// stream-specific line in it.
func desk(size, lateness time.Duration, keyField string) *pipeline.Pipeline {
	p := pipeline.New("watchtower")

	spec := stream.WindowSpec{
		Assigner: stream.Tumbling(size),
		// Event time comes from the payload rather than from when the file was
		// read, which is what makes a replay land in the same windows as the
		// original.
		Time:     stream.EventTime("at"),
		Lateness: lateness,
	}
	if keyField != "" {
		spec.Key = func(r core.Record) string { return r.String(keyField) }
	}

	p.FromStream("incidents").
		Infer("grade", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			System:  "You triage infrastructure incidents.",
			// The prefix is the rubric every call repeats, rendered once per
			// task and served from the provider's prompt cache thereafter.
			Prefix: "Severity rubric: data loss or auth failure is critical; " +
				"elevated error rate or saturation is high; everything else is low.",
			Prompt:    "Incident on {{.service}}: {{.message}}",
			ParseJSON: true,
		}).
		Window("per-minute", spec).
		ReduceAI("digest", pipeline.ReduceAISpec{
			Binding:   model.Binding{Tier: model.TierDeep},
			System:    "You write one-line operational digests.",
			Prompt:    "Digest these {{.Count}} incidents:\n{{range .Items}}- {{.}}\n{{end}}",
			FanIn:     16,
			ItemField: "severity",
		})
	return p
}

// registry wires two deterministic mock tiers: a cheap one for grading and an
// expensive one for the per-pane digest, so the report shows where a stream
// job's money actually goes.
func registry(slow time.Duration) (*model.Registry, *model.Mock, *model.Mock) {
	reg := model.NewRegistry()
	fast, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(grade), model.WithLatency(slow))
	if err != nil {
		log.Fatal(err)
	}
	// The deep tier is slower, as it would be: the per-pane digest is the
	// expensive call, and in the view it is the one you watch hold a slot.
	deep, err := model.RegisterMock(reg, "mock-deep", model.TierDeep,
		model.WithHandler(digest), model.WithLatency(slow*3))
	if err != nil {
		log.Fatal(err)
	}
	return reg, fast, deep
}

// grade answers the per-incident inference.
func grade(req model.Request) (string, error) {
	severity := "low"
	switch {
	case strings.Contains(req.Prompt, "data loss"), strings.Contains(req.Prompt, "auth"):
		severity = "critical"
	case strings.Contains(req.Prompt, "error rate"), strings.Contains(req.Prompt, "saturated"):
		severity = "high"
	}
	return fmt.Sprintf(`{"severity": %q}`, severity), nil
}

// digest answers the per-pane aggregation.
func digest(req model.Request) (string, error) {
	counts := map[string]int{}
	for _, line := range strings.Split(req.Prompt, "\n") {
		if s, ok := strings.CutPrefix(line, "- "); ok {
			counts[strings.TrimSpace(s)]++
		}
	}
	var parts []string
	for _, level := range []string{"critical", "high", "low"} {
		if n := counts[level]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, level))
		}
	}
	if len(parts) == 0 {
		return "quiet", nil
	}
	return strings.Join(parts, ", "), nil
}

// --- The three ways to run it -------------------------------------------

func runBackfill(feed, out, state string, c desk_) {
	reg, fast, deep := registry(c.slow)

	fmt.Printf("\nbackfill: reading %s to its end\n\n", feed)
	res, err := loom.Stream(context.Background(), desk(c.size, c.lateness, c.key),
		c.options(reg,
			loom.WithSource("incidents", mustSource(feed, false)),
			loom.WithSink("digest", mustSink(out)),
			loom.WithStateDir(state),
			loom.WithJobID("watchtower"),
		)...)
	if err != nil {
		log.Fatal(err)
	}
	report(res, fast, deep)
	fmt.Printf("digests written to %s\n", out)
}

func runLive(feed, out, state string, incidents []incident, c desk_) {
	// Half the feed is already on disk; the rest is appended while the job runs,
	// which is what "following" means and what a live source looks like.
	half := len(incidents) / 2
	if err := writeFeed(feed, incidents, half); err != nil {
		log.Fatal(err)
	}

	reg, fast, deep := registry(c.slow)

	// A run long enough to watch: with -slow the feed is written at a pace the
	// panes can be seen firing against.
	gap := 15 * time.Millisecond
	limit := 8 * time.Second
	if c.slow > 0 {
		gap, limit = c.slow*2, 45*time.Second
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := half; i < len(incidents); i++ {
			time.Sleep(gap)
			if err := appendFeed(feed, incidents[i]); err != nil {
				log.Print(err)
				return
			}
		}
	}()

	fmt.Printf("\nlive: following %s while a writer appends to it\n\n", feed)
	res, err := loom.Stream(context.Background(), desk(c.size, c.lateness, c.key),
		c.options(reg,
			loom.WithSource("incidents", mustSource(feed, true)),
			loom.WithSink("digest", mustSink(out)),
			loom.WithStateDir(state),
			loom.WithJobID("watchtower-live"),
			loom.WithCheckpointEvery(500*time.Millisecond),
			loom.WithPolling(32, 25*time.Millisecond),
			// A live source never ends, so something has to say when this demo
			// does. In production this is the absent line.
			loom.WithStreamLimit(stream.Limit{Duration: limit}),
		)...)
	wg.Wait()
	if err != nil {
		log.Fatal(err)
	}
	report(res, fast, deep)
	fmt.Println("the windows still open were checkpointed rather than fired:")
	fmt.Println("  a job cancelled mid-window has a half-full window, and publishing")
	fmt.Println("  it would be publishing a partial answer as if it were the real one.")
}

func runCrashAndResume(feed, out, state string, incidents []incident, c desk_) {
	if err := writeFeed(feed, incidents, len(incidents)); err != nil {
		log.Fatal(err)
	}
	reg, fast, deep := registry(c.slow)

	fmt.Printf("\nfirst start: stopped after %d incidents\n\n", len(incidents)/3)
	first, err := loom.Stream(context.Background(), desk(c.size, c.lateness, c.key),
		c.options(reg,
			loom.WithSource("incidents", mustSource(feed, false)),
			loom.WithSink("digest", mustSink(out)),
			loom.WithStateDir(state), loom.WithJobID("watchtower"),
			loom.WithCheckpointEvery(20*time.Millisecond),
			loom.WithPolling(4, 5*time.Millisecond),
			loom.WithStreamLimit(stream.Limit{Records: int64(len(incidents) / 3)}),
		)...)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print(first.Stream)
	callsBefore := fast.Calls() + deep.Calls()

	fmt.Printf("\nsecond start: same job ID, same state directory\n\n")
	second, err := loom.Stream(context.Background(), desk(c.size, c.lateness, c.key),
		c.options(reg,
			loom.WithSource("incidents", mustSource(feed, false)),
			loom.WithSink("digest", mustSink(out)),
			loom.WithStateDir(state), loom.WithJobID("watchtower"),
		)...)
	if err != nil {
		log.Fatal(err)
	}
	report(second, fast, deep)

	replayed := second.Stream.Records - first.Stream.Records
	fmt.Printf("what the interruption cost\n")
	fmt.Printf("  resumed from epoch      %d\n", second.Stream.ResumedFrom)
	fmt.Printf("  incidents after resume  %d\n", replayed)
	fmt.Printf("  model calls, first run  %d\n", callsBefore)
	fmt.Printf("  model calls, both runs  %d\n", fast.Calls()+deep.Calls())
	fmt.Printf("\n  The second start re-read nothing it had checkpointed, and every\n")
	fmt.Printf("  task it did re-execute was served from the result cache. An\n")
	fmt.Printf("  interrupted stream job costs wall clock, not tokens.\n")
}

// --- Plumbing ------------------------------------------------------------

func mustSource(dir string, follow bool) *file.Source {
	src, err := file.Open(file.SourceOptions{
		Glob:         filepath.Join(dir, "*.jsonl"),
		Time:         stream.EventTime("at"),
		Follow:       follow,
		PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	return src
}

func mustSink(dir string) *file.Sink {
	sink, err := file.NewSink(file.SinkOptions{Dir: dir, Meta: true})
	if err != nil {
		log.Fatal(err)
	}
	return sink
}

// printerMu serializes the terminal narration, which several stage goroutines
// reach at once.
var printerMu sync.Mutex

// panePrinterOnce prints each pane as it fires, which is the thing to watch in
// a stream job: it is the moment a set was declared complete and the moment the
// expensive stage ran.
func panePrinterOnce(e observe.Event) {
	switch e.Type {
	case observe.PaneFired:
		printerMu.Lock()
		defer printerMu.Unlock()
		fmt.Printf("  pane  %-46s %3d incidents  (%s)\n", e.Detail, e.Records, e.Note)
	case observe.CheckpointCommitted:
		printerMu.Lock()
		defer printerMu.Unlock()
		fmt.Printf("  ckpt  epoch %-3d held still for %-8s watermark %s\n",
			e.Epoch, e.Latency.Round(time.Millisecond),
			e.Watermark.UTC().Format("15:04:05"))
	}
}

func report(res *loom.StreamResult, fast, deep *model.Mock) {
	fmt.Println()
	fmt.Print(res.Stream)
	fmt.Printf("\n%s\n", res.Report)
	fmt.Printf("model calls: %d grading (one per incident), %d digesting (one per pane)\n",
		fast.Calls(), deep.Calls())
	if res.Stream.Panes > 0 {
		fmt.Printf("a pane is the unit that costs money: %d panes at the deep tier\n",
			res.Stream.Panes)
	}
	fmt.Println()
}

func workspace(dir string) (string, func(), error) {
	if dir != "" {
		return dir, func() {}, os.MkdirAll(dir, 0o755)
	}
	tmp, err := os.MkdirTemp("", "loom-watchtower-")
	if err != nil {
		return "", func() {}, err
	}
	return tmp, func() { os.RemoveAll(tmp) }, nil
}

// --- The synthetic feed --------------------------------------------------

type incident struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Message string `json:"message"`
	At      string `json:"at"`
}

var services = []string{"api", "checkout", "ledger", "search"}

var messages = []string{
	"elevated error rate on /v1/orders",
	"replica lag above threshold",
	"auth token rejected by upstream",
	"connection pool saturated",
	"cache hit rate degraded",
	"possible data loss on shard 3",
	"latency p99 above budget",
	"retry storm from mobile clients",
}

// maxDisorder bounds how far behind event-time order this feed delivers. It is
// the number a source's Lateness has to cover, and having it be a number rather
// than a vibe is the point: watermarks are only useful when the disorder they
// tolerate is bounded, and the whole of a source's contract with a stream job
// is what that bound is.
const maxDisorder = 8 * time.Second

// generate builds a feed whose event times span the requested minutes and whose
// delivery order is out of event-time order by at most maxDisorder — the only
// interesting property a synthetic feed can have, because it is exactly what
// watermarks exist for.
func generate(seed int64, minutes, perMinute int) []incident {
	rng := rand.New(rand.NewSource(seed))
	start := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

	type pending struct {
		inc       incident
		delivered time.Time
	}
	var all []pending
	for m := 0; m < minutes; m++ {
		for i := 0; i < perMinute; i++ {
			offset := time.Duration(rng.Intn(60)) * time.Second
			at := start.Add(time.Duration(m)*time.Minute + offset)
			all = append(all, pending{
				inc: incident{
					ID:      fmt.Sprintf("inc-%03d", len(all)),
					Service: services[rng.Intn(len(services))],
					Message: messages[rng.Intn(len(messages))],
					At:      at.UTC().Format(time.RFC3339),
				},
				// Delivery is delayed by up to maxDisorder, so sorting on it
				// produces an order that lags event time by a bounded amount.
				delivered: at.Add(time.Duration(rng.Int63n(int64(maxDisorder)))),
			})
		}
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].delivered.Before(all[j].delivered) })

	out := make([]incident, len(all))
	for i, p := range all {
		out[i] = p.inc
	}
	return out
}

// writeFeed lays the first n incidents down as two partitions, so the job has
// more than one split to compute a watermark across.
func writeFeed(dir string, incidents []incident, n int) error {
	parts := [2][]incident{}
	for i := 0; i < n && i < len(incidents); i++ {
		parts[i%2] = append(parts[i%2], incidents[i])
	}
	for i, part := range parts {
		path := filepath.Join(dir, fmt.Sprintf("part-%d.jsonl", i))
		if err := os.WriteFile(path, encode(part), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// appendFeed adds one incident to a partition, as a live writer would.
func appendFeed(dir string, inc incident) error {
	path := filepath.Join(dir, fmt.Sprintf("part-%d.jsonl", rand.Intn(2)))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(encode([]incident{inc}))
	return err
}

func encode(incidents []incident) []byte {
	var b strings.Builder
	for _, inc := range incidents {
		blob, err := json.Marshal(inc)
		if err != nil {
			continue
		}
		b.Write(blob)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}
