// Command switchboard is Loom's serving-layer example: a support desk whose
// conversations never stop arriving, and which can be asked about them at any
// moment.
//
// It is the counterpart to examples/watchtower. That one asks what a stream job
// costs when the input never ends; this one asks what you do with the result —
// how an endless feed becomes something you can hold a conversation *about*.
//
//	go run ./examples/switchboard                  # ingest a shift of support traffic, then ask it questions
//	go run ./examples/switchboard -live            # deliver it at a conversational pace
//	go run ./examples/switchboard -serve :8099     # keep it running, with the console at that address
//	go run ./examples/switchboard -budget 1500     # a tighter context, so it compacts sooner
//	go run ./examples/switchboard -ask "what is Northwind waiting on?"
//
// Everything runs offline against a deterministic mock provider: no key, no
// network, no cost.
//
// # What to look for
//
//   - **the cost is on the ingest side**. The report prints model calls per
//     stage. Reading messages is the bill; answering a question is one call
//     against a context of a couple of kilobytes, and it is the same one call
//     whether the desk has ingested ten messages or ten thousand.
//
//   - **the context is maintained, not rebuilt**. Every conversation slice
//     appends one revision to a chain. The line the example prints for each
//     one shows the revision hash changing and the context growing by a line —
//     no re-reading of anything already in it.
//
//   - **a repeated question is free**. Ask the same thing twice and the second
//     answer is a cache hit: the context's revision is part of the query
//     stage's fingerprint, so an unchanged context means an unchanged
//     computation. Then a new conversation lands, the revision moves, and the
//     next answer is computed against it.
//
//   - **compaction is a fold, not a truncation**. With -budget small enough,
//     the oldest entries are folded into a standing brief while the newest stay
//     verbatim. The example prints the context before and after so you can see
//     what survived.
//
//   - **the two sides do not wait for each other**. With -live, questions are
//     answered while messages are still arriving, and each answer says which
//     revision it used and how many messages it does not yet cover. That number
//     is the honest price of decoupling, and printing it is the point.
//
// The conversations are synthetic: the companies, the people, and their
// problems are fixtures invented for this example.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/recall"
)

func main() {
	var (
		serve  = flag.String("serve", "", "serve the console on this address (e.g. :8099) and keep running")
		live   = flag.Bool("live", false, "deliver the conversations at a conversational pace")
		budget = flag.Int("budget", 0, "the living context's byte budget (0: the package default)")
		every  = flag.Duration("every", 3*time.Minute, "how long a slice of one conversation covers")
		ask    = flag.String("ask", "", "ask one question and exit")
		state  = flag.String("state", "", "state directory (default: a temporary one)")
	)
	flag.Parse()

	// A window closes when event time passes its end, so live delivery — where
	// messages are stamped as they are posted — waits up to one window for its
	// last slices. Three minutes is the right shape for reading a shift and the
	// wrong one for watching a feed, so live gets a short window unless the
	// caller asked for a particular one.
	if *live && !flagSet("every") {
		*every = 15 * time.Second
		fmt.Printf("live: slicing every %s, so the last slices land while you are watching\n", *every)
	}

	work, cleanup, err := workspace(*state)
	if err != nil {
		log.Fatal(err)
	}
	defer cleanup()

	desk, err := recall.Open(recall.Config{
		Name:     "switchboard",
		Every:    *every,
		Slice:    6,
		MaxBytes: *budget,
		Keep:     4,
		// Short, because the transcript below is delivered in bursts and the
		// point of the example is to watch slices become queryable.
		Quiet: 750 * time.Millisecond,
	},
		loom.WithRegistry(registry()),
		loom.WithStateDir(work),
		loom.WithWorkers(6),
		// One ceiling over ingestion, compaction and every answer — a desk is a
		// fleet, and a fleet has one governor.
		loom.WithRunBudget(core.Budget{MaxCostUSD: 5}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer desk.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := desk.Start(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("switchboard: %d messages across %d conversations, in %s slices\n",
		len(transcript), conversations(transcript), *every)
	fmt.Println("ingestion and querying are separate from here on: the desk reads")
	fmt.Println("continuously, and a question reads whatever it has published.")
	fmt.Println()

	watch(ctx, desk)

	if *live {
		liveIngest(ctx, desk, *every)
	} else {
		bulkIngest(ctx, desk, *every)
	}

	switch {
	case *ask != "":
		answer(ctx, desk, "you", *ask)
	default:
		interview(ctx, desk)
	}

	fmt.Println()
	fmt.Print(desk.Report())
	fmt.Println(summary(desk))

	if *serve != "" {
		hold(ctx, desk, *serve)
	}
}

// --- ingestion -----------------------------------------------------------

// bulkIngest hands the whole shift over at once and waits for it to be
// digested, which is the shape of a backfill.
//
// The shift is stamped as having ended a window ago, so every window it belongs
// to has already closed and the whole thing becomes queryable as soon as the
// feed goes quiet. A shift stamped as ending now would leave its last slice per
// conversation open until event time caught up — correct, and a slow thing to
// watch.
func bulkIngest(ctx context.Context, desk *recall.Desk, every time.Duration) {
	posted, err := desk.Post(ctx,
		shift(time.Now().UTC().Add(-every-2*time.Second), transcript)...)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("posted %d messages; waiting for the desk to catch up…\n\n", posted)

	wait, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if _, err := desk.Await(wait, posted); err != nil {
		log.Fatalf("the desk did not catch up: %v", err)
	}
}

// liveIngest delivers the conversations a burst at a time, asking a question in
// the middle of it — which is the whole point of the arrangement, and the only
// mode where the answer's "behind" figure is ever non-zero.
func liveIngest(ctx context.Context, desk *recall.Desk, every time.Duration) {
	// Compressed onto a few seconds and stamped as it is posted, which is what
	// a live feed looks like — and which means the last slice of each
	// conversation waits for event time to pass its window, as it must.
	bursts := burst(compress(transcript, 6*time.Second), 4)
	for i, b := range bursts {
		if ctx.Err() != nil {
			return
		}
		if _, err := desk.Post(ctx, shift(time.Now().UTC(), b)...); err != nil {
			log.Fatal(err)
		}
		if i == len(bursts)/2 {
			fmt.Println("\n— asking while messages are still arriving —")
			// MinVersion is how a caller buys back the coupling the design gave
			// away: answer against something the desk has actually published,
			// rather than against an empty context that happens to be current.
			// What it cannot buy is completeness — the answer still reports how
			// many messages it does not cover, because that is the truth.
			ask(ctx, desk, recall.Question{
				Session: "live", Text: "what is going wrong right now?",
				MinVersion: 1, Wait: 30 * time.Second,
			})
		}
		select {
		case <-time.After(400 * time.Millisecond):
		case <-ctx.Done():
			return
		}
	}
	fmt.Printf("\nfeed quiet; waiting up to one %s window for the last slices…\n", every)
	wait, cancel := context.WithTimeout(ctx, every+30*time.Second)
	defer cancel()
	if _, err := desk.Await(wait, desk.Stats().Posted); err != nil {
		fmt.Printf("(the desk was still catching up: %v)\n", err)
	}
}

// burst splits the transcript into n roughly equal deliveries.
func burst(msgs []recall.Message, n int) [][]recall.Message {
	if n < 1 {
		n = 1
	}
	size := (len(msgs) + n - 1) / n
	var out [][]recall.Message
	for i := 0; i < len(msgs); i += size {
		out = append(out, msgs[i:min(i+size, len(msgs))])
	}
	return out
}

// watch narrates the context as it is maintained: one line per revision, which
// is one conversation slice folded in.
func watch(ctx context.Context, desk *recall.Desk) {
	go func() {
		seen := int64(0)
		for ctx.Err() == nil {
			updates := desk.Updates()
			select {
			case <-updates:
			case <-ctx.Done():
				return
			}
			v := desk.Context()
			if v.Version <= seen {
				continue
			}
			seen = v.Version
			brief := ""
			if v.Brief {
				brief = " + brief"
			}
			fmt.Printf("  context v%-3d %s  %2d entries%s  %5d bytes  %d conversations\n",
				v.Version, short(v.Ref.Hash), v.Entries, brief, v.Bytes, v.Conversations)
		}
	}()
}

// --- querying ------------------------------------------------------------

// interview asks the desk a few things, then asks one of them again — which is
// the cheapest question a serving layer can be asked, and the report says so.
func interview(ctx context.Context, desk *recall.Desk) {
	questions := []string{
		"which customers are blocked, and on what?",
		"what has been promised, and by when?",
		"what should the on-call engineer look at first?",
	}
	fmt.Println("\n— asking the desk —")
	at := desk.Context().Version
	for _, q := range questions {
		answer(ctx, desk, "review", q)
	}

	// Asked in a session of its own, so that what reaches the model is
	// byte-identical to the first asking: the same question against the same
	// context revision, which is the thing that costs nothing. Repeating it
	// inside the same session would carry the intervening turns and would
	// rightly be a different question.
	if now := desk.Context().Version; now != at {
		fmt.Printf("\n— the context moved to v%d while those were being asked "+
			"(a compaction, or a slice landing), so the repeat below is "+
			"recomputed rather than free —\n", now)
	} else {
		fmt.Println("\n— the first question again: same context, same history —")
	}
	answer(ctx, desk, "", questions[0])
}

// answer puts one plain question to the desk.
func answer(ctx context.Context, desk *recall.Desk, session, q string) {
	ask(ctx, desk, recall.Question{Session: session, Text: q})
}

// ask puts one question and prints it with the provenance that makes it
// readable: which revision answered it, and what that revision did not cover.
func ask(ctx context.Context, desk *recall.Desk, q recall.Question) {
	timeout := q.Wait + time.Minute
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ans, err := desk.Ask(qctx, q)
	if err != nil {
		fmt.Printf("\nQ %s\n! %v\n", q.Text, err)
		return
	}
	fmt.Printf("\nQ %s\n", q.Text)
	for _, line := range strings.Split(strings.TrimSpace(ans.Text), "\n") {
		fmt.Printf("A %s\n", line)
	}
	notes := []string{
		fmt.Sprintf("v%d", ans.Version),
		fmt.Sprintf("%d entries", ans.Entries),
		fmt.Sprintf("%d bytes", ans.Bytes),
		ans.Latency.Round(time.Millisecond).String(),
	}
	if ans.Cached {
		notes = append(notes, "cached: no model call")
	}
	if ans.Waited > 0 {
		notes = append(notes, "waited "+ans.Waited.Round(time.Millisecond).String()+" for a revision")
	}
	if ans.Behind > 0 {
		notes = append(notes, fmt.Sprintf("%d messages not yet folded in", ans.Behind))
	}
	fmt.Printf("  · %s\n", strings.Join(notes, " · "))
}

// --- serving -------------------------------------------------------------

// hold keeps the desk running with its console served, because a desk that
// stops when its backfill finishes is not a serving layer.
func hold(ctx context.Context, desk *recall.Desk, addr string) {
	url, closeSrv, err := desk.Serve(addr)
	if err != nil {
		log.Fatal(err)
	}
	defer closeSrv()

	fmt.Printf("\nconsole: %s\n", url)
	fmt.Printf("  POST %s/ingest   {\"conversation\":\"acme\",\"messages\":[{\"role\":\"user\",\"text\":\"…\"}]}\n", url)
	fmt.Printf("  GET  %s/context  the living prompt, exactly as the model receives it\n", url)
	fmt.Println("press Ctrl-C to exit")
	<-ctx.Done()
}

// --- reporting -----------------------------------------------------------

// summary is the paragraph the example exists to print: what the desk holds,
// what it cost, and what a question costs against it.
func summary(desk *recall.Desk) string {
	s := desk.Stats()
	var b strings.Builder
	fmt.Fprintf(&b, "\ncontext   v%d · %d entries", s.View.Version, s.View.Entries)
	if s.View.Brief {
		b.WriteString(" + a standing brief")
	}
	fmt.Fprintf(&b, " · %d bytes · %d conversations\n", s.View.Bytes, s.View.Conversations)
	fmt.Fprintf(&b, "ingested  %d messages, %d folded in, %d dropped as not worth remembering\n",
		s.Posted, s.View.Messages, s.Dropped)
	fmt.Fprintf(&b, "queries   %d asked, %d served from cache without a model call\n",
		s.Queries, s.Cached)
	if s.View.Compactions > 0 {
		fmt.Fprintf(&b, "compacted %d times, keeping the newest entries verbatim\n",
			s.View.Compactions)
	}
	fmt.Fprintf(&b, "spent     $%.4f over %d requests, all of it on one budget\n",
		s.Spent.CostUSD, s.Spent.Requests)
	if errs := desk.Errors(); len(errs) > 0 {
		fmt.Fprintf(&b, "absorbed  %d errors: %v\n", len(errs), errs[0])
	}
	return b.String()
}

func short(hash string) string {
	if len(hash) > 8 {
		return hash[:8]
	}
	return hash
}

func conversations(msgs []recall.Message) int {
	seen := map[string]bool{}
	for _, m := range msgs {
		seen[m.Conversation] = true
	}
	return len(seen)
}

// workspace returns a directory to keep state in, and how to clean it up.
func workspace(dir string) (string, func(), error) {
	if dir != "" {
		return dir, func() {}, os.MkdirAll(dir, 0o755)
	}
	tmp, err := os.MkdirTemp("", "switchboard-*")
	if err != nil {
		return "", nil, err
	}
	return tmp, func() { _ = os.RemoveAll(tmp) }, nil
}

// --- the models ----------------------------------------------------------

// registry wires the desk's two tiers onto one deterministic mock brain.
//
// The tiers are the point of the split rather than decoration: reading is one
// call per message and belongs on the cheap one, and everything whose output
// the context carries — a digest, a brief, an answer — belongs on the one whose
// mistakes would be read by every question afterwards.
func registry() *model.Registry {
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(brain)); err != nil {
		log.Fatal(err)
	}
	if _, err := model.RegisterMock(reg, "mock-deep", model.TierDeep,
		model.WithHandler(brain)); err != nil {
		log.Fatal(err)
	}
	return reg
}

// flagSet reports whether a flag was given on the command line, as opposed to
// left at its default.
func flagSet(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
