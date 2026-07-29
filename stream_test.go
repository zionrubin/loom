package loom_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
)

// twoStagePipeline chains two inference stages so the second can only run on
// records the first has finished.
func twoStagePipeline() *pipeline.Pipeline {
	p := pipeline.New("chain")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			Prompt:    "Classify this ticket: {{.subject}}",
			ParseJSON: true,
		}).
		Infer("respond", pipeline.InferSpec{
			Binding:     model.Binding{Tier: model.TierFast},
			Prompt:      "Draft a reply for a {{.category}} ticket",
			OutputField: "reply",
		})
	return p
}

// straggler is the latency of the slow records in the overlap test. It only
// has to dominate the scheduling noise between "a fast record finished" and
// "the whole stage finished".
const straggler = 300 * time.Millisecond

// streamRegistry registers a model whose classify calls are slow for every
// ticket but the first. That asymmetry is what makes overlap observable: one
// record is ready almost immediately while its stage keeps running, so a
// driver that pipelines can be told apart from one that waits for the
// barrier — without either outcome depending on goroutine scheduling luck.
func streamRegistry(t *testing.T, stagger bool) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	handler := func(req model.Request) (string, error) {
		if strings.Contains(req.Prompt, "Draft a reply") {
			return "REPLY", nil
		}
		if stagger && !strings.Contains(req.Prompt, "refund not processed") {
			time.Sleep(straggler)
		}
		return classifyMock(req)
	}
	_, err := model.RegisterMock(reg, "fast", model.TierFast,
		model.WithHandler(handler))
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// eventLog collects events concurrently for assertions about ordering.
type eventLog struct {
	mu     sync.Mutex
	events []observe.Event
}

func (l *eventLog) handle(e observe.Event) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

// count returns how many matching events were seen.
func (l *eventLog) count(typ observe.EventType, stage string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	var n int
	for _, e := range l.events {
		if e.Type == typ && (stage == "" || e.Stage == stage) {
			n++
		}
	}
	return n
}

// firstAt returns the time of the first matching event.
func (l *eventLog) firstAt(typ observe.EventType, stage string) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.events {
		if e.Type == typ && (stage == "" || e.Stage == stage) {
			return e.Time, true
		}
	}
	return time.Time{}, false
}

// TestStreamingOverlapsStages is the continuous-batching claim: a downstream
// stage starts working on finished records while the upstream stage is still
// producing them, instead of waiting for the barrier.
func TestStreamingOverlapsStages(t *testing.T) {
	var streamed, barriered eventLog

	// Enough slots that a freed one is always available downstream; the point
	// under test is the driver, not the concurrency ceiling.
	if _, err := loom.Run(context.Background(), twoStagePipeline(),
		loom.WithRegistry(streamRegistry(t, true)),
		loom.WithRetry(quickRetry()), loom.WithWorkers(8),
		loom.WithStreaming(),
		loom.WithEventHandler(streamed.handle)); err != nil {
		t.Fatalf("streaming run: %v", err)
	}
	if _, err := loom.Run(context.Background(), twoStagePipeline(),
		loom.WithRegistry(streamRegistry(t, true)),
		loom.WithRetry(quickRetry()), loom.WithWorkers(8),
		loom.WithEventHandler(barriered.handle)); err != nil {
		t.Fatalf("barrier run: %v", err)
	}

	upstreamDone, ok := streamed.firstAt(observe.StageFinished, "classify")
	if !ok {
		t.Fatal("streaming run never finished the upstream stage")
	}
	downstreamStart, ok := streamed.firstAt(observe.TaskStarted, "respond")
	if !ok {
		t.Fatal("streaming run never started the downstream stage")
	}
	if !downstreamStart.Before(upstreamDone) {
		t.Fatalf("downstream started %v after upstream finished: "+
			"streaming did not overlap the stages",
			downstreamStart.Sub(upstreamDone))
	}
	// The fast record was ready long before the stragglers; a pipelined
	// driver should have acted on it rather than sitting on it.
	if lead := upstreamDone.Sub(downstreamStart); lead < straggler/2 {
		t.Errorf("downstream led the barrier by only %v, want at least %v",
			lead, straggler/2)
	}

	// The barrier driver is the control: there, the wait is the whole point.
	bUpstreamDone, _ := barriered.firstAt(observe.StageFinished, "classify")
	bDownstreamStart, ok := barriered.firstAt(observe.TaskStarted, "respond")
	if ok && bDownstreamStart.Before(bUpstreamDone) {
		t.Error("barrier driver overlapped stages; the control case is not controlling")
	}
}

// TestStreamingMatchesBarrierResults checks that swapping drivers changes
// scheduling and nothing else.
func TestStreamingMatchesBarrierResults(t *testing.T) {
	barrier, err := loom.Run(context.Background(), twoStagePipeline(),
		loom.WithRegistry(streamRegistry(t, false)), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("barrier run: %v", err)
	}
	stream, err := loom.Run(context.Background(), twoStagePipeline(),
		loom.WithRegistry(streamRegistry(t, false)), loom.WithRetry(quickRetry()),
		loom.WithStreaming())
	if err != nil {
		t.Fatalf("streaming run: %v", err)
	}

	if got, want := summarize(stream.Output), summarize(barrier.Output); got != want {
		t.Errorf("streaming output = %v, want %v", got, want)
	}
	if got, want := stream.Report.Totals().Requests, barrier.Report.Totals().Requests; got != want {
		t.Errorf("streaming made %d model calls, barrier made %d", got, want)
	}
	if len(stream.Failures) != 0 {
		t.Errorf("streaming run had failures: %v", stream.Failures)
	}
}

// summarize renders records order-insensitively: streaming deliberately
// trades input order for occupancy, so the comparison must not depend on it.
func summarize(recs []core.Record) string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, fmt.Sprintf("%s/%s/%s", r.ID, r.String("category"), r.String("reply")))
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// TestStreamingAggregatesAfterDrain covers the stages that cannot stream:
// ReduceAI must still see the whole set, and must still produce one record.
func TestStreamingAggregatesAfterDrain(t *testing.T) {
	reg := model.NewRegistry()
	handler := func(req model.Request) (string, error) {
		if strings.Contains(req.Prompt, "Summarize") {
			return summarizeMock(req)
		}
		return classifyMock(req)
	}
	if _, err := model.RegisterMock(reg, "fast", model.TierFast,
		model.WithHandler(handler)); err != nil {
		t.Fatal(err)
	}

	res, err := loom.Run(context.Background(), triagePipeline(t),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()), loom.WithStreaming())
	if err != nil {
		t.Fatalf("streaming run: %v", err)
	}
	base, err := loom.Run(context.Background(), triagePipeline(t),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("barrier run: %v", err)
	}

	brief := res.StageOutputs["brief"]
	if len(brief) != 1 {
		t.Fatalf("ReduceAI produced %d records, want 1", len(brief))
	}
	// Three of the four tickets are urgent and survive the filter; with
	// FanIn 2 the tree collapses 3 → 2 → 1, so the top aggregate summarizes
	// two intermediates. The value that matters is that streaming agrees with
	// the barrier driver: the reduce saw the same set either way.
	if got, want := brief[0].String("output"), base.StageOutputs["brief"][0].String("output"); got != want {
		t.Errorf("streaming aggregate = %q, barrier = %q — the reduce saw a different set",
			got, want)
	}
	if got := len(res.StageOutputs["urgent-only"]); got != 3 {
		t.Errorf("filter passed %d records into the reduce, want 3", got)
	}
}

// TestStreamingBatchesContinuously checks that a batched stage groups the
// records already queued into one task rather than one task per record, and
// that a partial group still ships instead of waiting for records that will
// never arrive.
func TestStreamingBatchesContinuously(t *testing.T) {
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "fast", model.TierFast,
		model.WithHandler(func(model.Request) (string, error) { return "x", nil })); err != nil {
		t.Fatal(err)
	}

	// Five records at two per task: two full groups and a remainder that only
	// ships because the batch wait expires.
	recs := append(tickets(), core.NewRecord("t5", map[string]any{"subject": "extra"}))
	p := pipeline.New("batched")
	p.FromRecords("tickets", recs).
		Infer("classify", pipeline.InferSpec{
			Binding:     model.Binding{Tier: model.TierFast},
			Prompt:      "Classify {{.subject}}",
			OutputField: "out",
		}, pipeline.WithBatchSize(2))

	var log eventLog
	type outcome struct {
		res *loom.RunResult
		err error
	}
	ch := make(chan outcome, 1)
	go func() {
		res, err := loom.Run(context.Background(), p,
			loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
			loom.WithStreaming(), loom.WithBatchWait(10*time.Millisecond),
			loom.WithEventHandler(log.handle))
		ch <- outcome{res, err}
	}()

	var got outcome
	select {
	case got = <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("streaming run hung: a partial batch never shipped")
	}
	if got.err != nil {
		t.Fatalf("run: %v", got.err)
	}
	if n := len(got.res.Output); n != len(recs) {
		t.Errorf("output records = %d, want %d — the remainder never shipped", n, len(recs))
	}

	// BatchSize groups records into tasks, not into a single model call:
	// Infer still calls the model per record. Three tasks for five records
	// is the batching working.
	if n := log.count(observe.TaskScheduled, "classify"); n != 3 {
		t.Errorf("tasks = %d, want 3 (five records grouped two at a time)", n)
	}
}

// TestStreamingAbortsOnFailure checks that a dead-lettered task tears the run
// down rather than leaving stage goroutines waiting on pipes forever.
func TestStreamingAbortsOnFailure(t *testing.T) {
	reg := model.NewRegistry()
	boom := core.Permanent(errors.New("model exploded"))
	if _, err := model.RegisterMock(reg, "fast", model.TierFast,
		model.WithHandler(func(model.Request) (string, error) { return "", boom })); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		res *loom.RunResult
		err error
	}
	ch := make(chan outcome, 1)
	go func() {
		res, err := loom.Run(context.Background(), twoStagePipeline(),
			loom.WithRegistry(reg), loom.WithRetry(quickRetry()), loom.WithStreaming())
		ch <- outcome{res, err}
	}()

	select {
	case got := <-ch:
		if got.err == nil {
			t.Fatal("run succeeded despite a permanent model failure")
		}
		if len(got.res.Failures) == 0 {
			t.Error("no dead letters recorded for the failing tasks")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("streaming run hung after a task failure")
	}
}

// TestStreamingContinueOnError checks the run completes and dead-letters when
// asked to, instead of aborting on the first failure.
func TestStreamingContinueOnError(t *testing.T) {
	reg := model.NewRegistry()
	var n int
	var mu sync.Mutex
	handler := func(req model.Request) (string, error) {
		mu.Lock()
		n++
		first := n == 1
		mu.Unlock()
		if first {
			return "", core.Permanent(errors.New("first call fails"))
		}
		return classifyMock(req)
	}
	if _, err := model.RegisterMock(reg, "fast", model.TierFast,
		model.WithHandler(handler)); err != nil {
		t.Fatal(err)
	}

	p := pipeline.New("resilient")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			Prompt:    "Classify this ticket: {{.subject}}",
			ParseJSON: true,
		})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithStreaming(), loom.WithContinueOnError())
	if err != nil {
		t.Fatalf("run aborted despite WithContinueOnError: %v", err)
	}
	if len(res.Failures) != 1 {
		t.Errorf("dead letters = %d, want 1", len(res.Failures))
	}
	if got := len(res.Output); got != len(tickets())-1 {
		t.Errorf("output records = %d, want %d (all but the dead-lettered one)",
			got, len(tickets())-1)
	}
}

// TestStreamingRespectsStageParallelism checks a per-stage cap still means
// something under the streaming driver, where concurrency is otherwise
// bounded globally by the engine.
func TestStreamingRespectsStageParallelism(t *testing.T) {
	var (
		mu       sync.Mutex
		inFlight int
		peak     int
	)
	reg := model.NewRegistry()
	handler := func(model.Request) (string, error) {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return "x", nil
	}
	if _, err := model.RegisterMock(reg, "fast", model.TierFast,
		model.WithHandler(handler)); err != nil {
		t.Fatal(err)
	}

	p := pipeline.New("capped")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding:     model.Binding{Tier: model.TierFast},
			Prompt:      "Classify {{.subject}}",
			OutputField: "out",
		}, pipeline.WithParallelism(2))

	if _, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithWorkers(8), loom.WithStreaming()); err != nil {
		t.Fatalf("run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if peak > 2 {
		t.Errorf("peak concurrency = %d, want at most 2 (WithParallelism ignored)", peak)
	}
	if peak < 2 {
		t.Errorf("peak concurrency = %d, want 2 — the cap throttled below its limit", peak)
	}
}

// TestStreamingSharesPrefixCache pins the interaction that streaming broke
// once already: the planner's break-even test counts a stage's tasks, and a
// streaming stage builds them a few records at a time. Counting per batch
// instead of per stage made every streaming stage look like a one-call stage,
// silently disabling prefix caching for the whole run.
func TestStreamingSharesPrefixCache(t *testing.T) {
	run := func(opts ...loom.Option) core.Usage {
		t.Helper()
		reg, _ := prefixRegistry(t)
		base := []loom.Option{loom.WithRegistry(reg), loom.WithRetry(quickRetry())}
		res, err := loom.Run(context.Background(), prefixPipeline(rubric),
			append(base, opts...)...)
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return res.Report.Totals()
	}

	streamed := run(loom.WithStreaming())
	if streamed.CacheReadTokens == 0 {
		t.Fatal("streaming run never read the shared prefix from cache")
	}

	// Both drivers write exactly one entry per stage and read it thereafter.
	// They differ by one call's worth of warm-up, and necessarily so: the
	// barrier driver is handed a stage's whole input and can tell on task one
	// that the prefix will be shared, while a streaming stage only learns it
	// is being fed when its second task arrives. The rule that never writes
	// an entry nothing will read is what costs that first call, and it is
	// bounded to one call however long the stage runs.
	barriered := run()
	if streamed.CacheWriteTokens != barriered.CacheWriteTokens {
		t.Errorf("prefix writes: streaming %d, barrier %d — each driver should "+
			"establish the entry exactly once",
			streamed.CacheWriteTokens, barriered.CacheWriteTokens)
	}
	perCall := barriered.CacheWriteTokens // one call's worth of prefix
	if gap := barriered.CacheReadTokens - streamed.CacheReadTokens; gap != perCall {
		t.Errorf("prefix reads: streaming %d, barrier %d (gap %d) — want exactly "+
			"one call of warm-up (%d)",
			streamed.CacheReadTokens, barriered.CacheReadTokens, gap, perCall)
	}
}
