package loom_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/stream"
	"github.com/zionrubin/loom/stream/file"
)

// The stream tests read a fixed set of files, which makes them a backfill: the
// same job, the same windows and the same code as a live feed, over an input
// that ends. Everything asserted here — which records land in which window,
// how many panes fire, what a restart resumes — is therefore exact rather than
// timing-dependent.

var streamBase = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func atSec(sec int) string {
	return streamBase.Add(time.Duration(sec) * time.Second).UTC().Format(time.RFC3339)
}

// writeEvents lays out a directory of JSONL incidents.
func writeEvents(t *testing.T, dir, name string, events ...map[string]any) {
	t.Helper()
	var b strings.Builder
	for _, e := range events {
		blob, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(blob)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func incident(id, service, at string) map[string]any {
	return map[string]any{"id": id, "service": service, "at": at, "text": "alert from " + service}
}

// eventsDir writes six incidents across two minutes and two services.
//
//	12:00 window: a1(api) a2(api) a3(db)
//	12:01 window: b1(api) b2(db) b3(db)
func eventsDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeEvents(t, dir, "part-0.jsonl",
		incident("a1", "api", atSec(5)),
		incident("a2", "api", atSec(20)),
		incident("a3", "db", atSec(50)),
	)
	writeEvents(t, dir, "part-1.jsonl",
		incident("b1", "api", atSec(65)),
		incident("b2", "db", atSec(80)),
		incident("b3", "db", atSec(110)),
	)
	return dir
}

// gradeMock answers the per-record inference deterministically.
func gradeMock(req model.Request) (string, error) {
	if strings.Contains(req.Prompt, "Summarize") {
		return fmt.Sprintf("DIGEST[%d]", strings.Count(req.Prompt, "- ")), nil
	}
	severity := "low"
	if strings.Contains(req.Prompt, "db") {
		severity = "high"
	}
	return fmt.Sprintf(`{"severity": %q}`, severity), nil
}

// incidentDesk is the canonical stream shape: per-record inference as events
// arrive, a window to make the set finite, and an aggregate over the pane.
func incidentDesk(key func(core.Record) string) *pipeline.Pipeline {
	p := pipeline.New("incident-desk")
	events := p.FromStream("incidents")

	events.
		Infer("grade", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			System:    "You grade incidents.",
			Prompt:    "Grade this incident on {{.service}}: {{.text}}",
			ParseJSON: true,
		}).
		Window("per-minute", stream.WindowSpec{
			Assigner: stream.Tumbling(time.Minute),
			Key:      key,
			Time:     stream.EventTime("at"),
		}).
		ReduceAI("digest", pipeline.ReduceAISpec{
			Binding:   model.Binding{Tier: model.TierFast},
			Prompt:    "Summarize {{.Count}} incidents:\n{{range .Items}}- {{.}}\n{{end}}",
			FanIn:     8,
			ItemField: "severity",
		})
	return p
}

func fastRegistry(t *testing.T) (*model.Registry, *model.Mock) {
	t.Helper()
	reg := model.NewRegistry()
	mock, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(gradeMock))
	if err != nil {
		t.Fatal(err)
	}
	return reg, mock
}

func fileSource(t *testing.T, dir string) *file.Source {
	t.Helper()
	src, err := file.Open(file.SourceOptions{
		Glob: filepath.Join(dir, "*.jsonl"),
		Time: stream.EventTime("at"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return src
}

func TestStreamWindowsAnUnboundedInputIntoPanes(t *testing.T) {
	dir := eventsDir(t)
	reg, mock := fastRegistry(t)
	src := fileSource(t, dir)
	out := t.TempDir()
	sink, err := file.NewSink(file.SinkOptions{Dir: out, Meta: true})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := loom.Stream(ctx, incidentDesk(nil),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithSource("incidents", src), loom.WithSink("digest", sink),
		loom.WithWorkers(4))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	if res.Stream.StopReason != "sources exhausted" {
		t.Fatalf("stop reason = %q", res.Stream.StopReason)
	}
	if res.Stream.Records != 6 {
		t.Fatalf("ingested %d records, want 6", res.Stream.Records)
	}
	// Two minutes of events, unkeyed: two windows, two panes, two digests.
	if res.Stream.Panes != 2 {
		t.Fatalf("panes = %d, want 2\n%s", res.Stream.Panes, res.Stream)
	}
	if res.Stream.Batches != 2 {
		t.Fatalf("sink batches = %d, want 2", res.Stream.Batches)
	}
	if res.Stream.Late != 0 {
		t.Fatalf("late records = %d, want 0", res.Stream.Late)
	}
	if len(res.Failures) != 0 {
		t.Fatalf("failures: %v", res.Failures)
	}

	// Six per-record inferences plus one aggregation per pane.
	if got := mock.Calls(); got != 6+2 {
		t.Fatalf("model calls = %d, want 8", got)
	}

	// Each window stage reports what it saw.
	w := res.Stream.Windows["per-minute"]
	if w.Records != 6 || w.Panes != 2 || w.LiveWindows != 0 {
		t.Fatalf("window stats = %+v", w)
	}

	// The sink wrote one file per pane, each holding one digest, each naming
	// the window it summarized.
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("sink wrote %d files, want one per pane", len(entries))
	}
	counts := map[string]bool{}
	for _, e := range entries {
		blob, err := os.ReadFile(filepath.Join(out, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var line struct {
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal(blob, &line); err != nil {
			t.Fatalf("sink line %q: %v", blob, err)
		}
		output, _ := line.Data["output"].(string)
		if !strings.HasPrefix(output, "DIGEST[3]") {
			t.Fatalf("digest = %q, want a summary of the window's three incidents", output)
		}
		pane, _ := line.Data["_pane"].(map[string]any)
		counts[fmt.Sprint(pane["window"])] = true
	}
	if len(counts) != 2 {
		t.Fatalf("the two panes name the same window: %v", counts)
	}
}

func TestStreamKeyedWindowsFirePerKey(t *testing.T) {
	dir := eventsDir(t)
	reg, _ := fastRegistry(t)
	src := fileSource(t, dir)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	byService := func(r core.Record) string { return r.String("service") }
	res, err := loom.Stream(ctx, incidentDesk(byService),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithSource("incidents", src), loom.WithWorkers(4))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}

	// 12:00 has api(2) and db(1); 12:01 has api(1) and db(2). Four panes.
	if res.Stream.Panes != 4 {
		t.Fatalf("panes = %d, want one per (minute, service)\n%s", res.Stream.Panes, res.Stream)
	}
	if w := res.Stream.Windows["per-minute"]; w.Records != 6 || w.Assignments != 6 {
		t.Fatalf("window stats = %+v", w)
	}
}

func TestStreamResumesFromItsCheckpointAndReplaysForFree(t *testing.T) {
	dir := eventsDir(t)
	state := t.TempDir()
	reg, mock := fastRegistry(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run := func() *loom.StreamResult {
		t.Helper()
		res, err := loom.Stream(ctx, incidentDesk(nil),
			loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
			loom.WithSource("incidents", fileSource(t, dir)),
			loom.WithStateDir(state), loom.WithJobID("desk"),
			loom.WithWorkers(4))
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		return res
	}

	first := run()
	if first.Stream.Checkpoints == 0 {
		t.Fatal("a job that ran to the end of its sources should have checkpointed")
	}
	if first.Stream.ResumedFrom != 0 {
		t.Fatalf("a cold start reported resuming from epoch %d", first.Stream.ResumedFrom)
	}
	callsAfterFirst := mock.Calls()
	if callsAfterFirst == 0 {
		t.Fatal("the first run made no model calls")
	}

	// The checkpoint is on disk, under the job's own name.
	if _, err := os.Stat(filepath.Join(state, "stream", "desk")); err != nil {
		t.Fatalf("checkpoint directory: %v", err)
	}

	// Starting the same job again resumes at the stored positions, so the same
	// files produce nothing new to ingest.
	second := run()
	if second.Stream.ResumedFrom == 0 {
		t.Fatal("the second start did not resume from a checkpoint")
	}
	if second.Stream.Records != first.Stream.Records {
		t.Fatalf("resumed job ingested %d records (cumulative), want the first run's %d",
			second.Stream.Records, first.Stream.Records)
	}
	if got := mock.Calls(); got != callsAfterFirst {
		t.Fatalf("model calls after resume = %d, want no new work (%d)", got, callsAfterFirst)
	}
}

func TestStreamReplayWithoutAJobIDRepeatsTheWorkButNotTheSpend(t *testing.T) {
	dir := eventsDir(t)
	state := t.TempDir()
	reg, mock := fastRegistry(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run := func() *loom.StreamResult {
		t.Helper()
		res, err := loom.Stream(ctx, incidentDesk(nil),
			loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
			loom.WithSource("incidents", fileSource(t, dir)),
			loom.WithStateDir(state), loom.WithWorkers(4))
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		return res
	}

	first := run()
	calls := mock.Calls()

	// A fresh job ID starts cold: every record is read and every task is
	// re-executed — and every one of them is served by the result cache, which
	// is the whole of Loom's answer to at-least-once delivery.
	second := run()
	if second.Stream.Records != first.Stream.Records {
		t.Fatalf("replay ingested %d records, want %d", second.Stream.Records, first.Stream.Records)
	}
	if second.Stream.Panes != first.Stream.Panes {
		t.Fatalf("replay fired %d panes, want %d", second.Stream.Panes, first.Stream.Panes)
	}
	if got := mock.Calls(); got != calls {
		t.Fatalf("replaying cost %d new model calls, want 0", got-calls)
	}
}

func TestStreamStopsAtARecordLimit(t *testing.T) {
	dir := t.TempDir()
	var events []map[string]any
	for i := 0; i < 200; i++ {
		events = append(events, incident(fmt.Sprintf("e%d", i), "api", atSec(i)))
	}
	writeEvents(t, dir, "big.jsonl", events...)

	reg, _ := fastRegistry(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := loom.Stream(ctx, incidentDesk(nil),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithSource("incidents", fileSource(t, dir)),
		loom.WithStreamLimit(stream.Limit{Records: 10}),
		loom.WithPolling(5, 10*time.Millisecond),
		loom.WithWorkers(2))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if res.Stream.StopReason != "record limit reached" {
		t.Fatalf("stop reason = %q", res.Stream.StopReason)
	}
	if res.Stream.Records < 10 || res.Stream.Records > 30 {
		t.Fatalf("ingested %d records, want the job to stop shortly after 10", res.Stream.Records)
	}
	// The job was cancelled rather than exhausted, so its open windows are held
	// in the checkpoint instead of being fired half-full.
	if res.Stream.Panes != 0 {
		t.Fatalf("panes = %d, want the open window left unfired on a limit stop", res.Stream.Panes)
	}
	if w := res.Stream.Windows["per-minute"]; w.LiveWindows == 0 {
		t.Fatalf("window stats = %+v, want records still held", w)
	}
}

func TestStreamDrainOnStopCanBeForced(t *testing.T) {
	dir := t.TempDir()
	var events []map[string]any
	for i := 0; i < 50; i++ {
		events = append(events, incident(fmt.Sprintf("e%d", i), "api", atSec(i)))
	}
	writeEvents(t, dir, "big.jsonl", events...)

	reg, _ := fastRegistry(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := loom.Stream(ctx, incidentDesk(nil),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithSource("incidents", fileSource(t, dir)),
		loom.WithStreamLimit(stream.Limit{Records: 5}),
		loom.WithDrainOnStop(true),
		loom.WithPolling(5, 10*time.Millisecond),
		loom.WithWorkers(2))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if res.Stream.Panes == 0 {
		t.Fatalf("DrainOnStop did not fire the open windows\n%s", res.Stream)
	}
}

func TestRunRefusesAStreamPipeline(t *testing.T) {
	reg, _ := fastRegistry(t)
	_, err := loom.Run(context.Background(), incidentDesk(nil), loom.WithRegistry(reg))
	if err == nil {
		t.Fatal("loom.Run should refuse a pipeline whose source never ends")
	}
	if !strings.Contains(err.Error(), "loom.Stream") {
		t.Fatalf("error should point at the right entry point, got: %v", err)
	}
}

func TestStreamRefusesAnAggregateWithNoWindow(t *testing.T) {
	p := pipeline.New("unwindowed")
	p.FromStream("incidents").
		Infer("grade", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prompt:  "Grade {{.text}}",
		}).
		ReduceAI("digest", pipeline.ReduceAISpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prompt:  "Summarize {{.Count}}:\n{{range .Items}}- {{.}}\n{{end}}",
		})

	reg, _ := fastRegistry(t)
	_, err := loom.Stream(context.Background(), p,
		loom.WithRegistry(reg),
		loom.WithSource("incidents", fileSource(t, t.TempDir())))
	if err == nil {
		t.Fatal("an aggregate over an unbounded input should not start")
	}
	if !strings.Contains(err.Error(), "Window") {
		t.Fatalf("the error should name the fix, got: %v", err)
	}
}

func TestStreamRefusesAnUnboundSource(t *testing.T) {
	reg, _ := fastRegistry(t)
	_, err := loom.Stream(context.Background(), incidentDesk(nil), loom.WithRegistry(reg))
	if err == nil {
		t.Fatal("a stream source with nothing bound to it should fail to start")
	}
	if !strings.Contains(err.Error(), "WithSource") {
		t.Fatalf("the error should name the fix, got: %v", err)
	}
}

func TestStreamRefusesASinkOnAStageThatDoesNotExist(t *testing.T) {
	reg, _ := fastRegistry(t)
	_, err := loom.Stream(context.Background(), incidentDesk(nil),
		loom.WithRegistry(reg),
		loom.WithSource("incidents", fileSource(t, t.TempDir())),
		loom.WithSink("no-such-stage", stream.SinkFunc(
			func(context.Context, stream.Batch) error { return nil })))
	if err == nil {
		t.Fatal("a sink bound to a stage that does not exist should fail to start")
	}
}

func TestStreamCheckpointsOnAnIntervalWhileRunning(t *testing.T) {
	dir := t.TempDir()
	var events []map[string]any
	for i := 0; i < 400; i++ {
		events = append(events, incident(fmt.Sprintf("e%d", i), "api", atSec(i)))
	}
	writeEvents(t, dir, "big.jsonl", events...)

	// A model call that takes a moment is what makes this test about
	// checkpointing rather than about how fast a mock returns: the job has to
	// still be running when the interval fires.
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(gradeMock), model.WithLatency(3*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Following the directory is what makes this a live job rather than a
	// backfill: the splits never end, so the job runs until its duration limit
	// and the checkpointer gets a chance to do its job more than once.
	src, err := file.Open(file.SourceOptions{
		Glob: filepath.Join(dir, "*.jsonl"), Time: stream.EventTime("at"),
		Follow: true, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := loom.Stream(ctx, incidentDesk(nil),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithSource("incidents", src),
		loom.WithStateDir(state), loom.WithJobID("ticker"),
		loom.WithCheckpointEvery(20*time.Millisecond),
		loom.WithPolling(4, 5*time.Millisecond),
		loom.WithStreamLimit(stream.Limit{Duration: 400 * time.Millisecond}),
		loom.WithWorkers(4))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if res.Stream.StopReason != "duration limit reached" {
		t.Fatalf("stop reason = %q", res.Stream.StopReason)
	}
	if res.Stream.Checkpoints < 2 {
		t.Fatalf("checkpoints = %d, want the interval to have fired more than once\n%s",
			res.Stream.Checkpoints, res.Stream)
	}
	if res.Stream.Skipped > 0 {
		t.Fatalf("%d checkpoints were skipped: the job would not come to rest", res.Stream.Skipped)
	}
	if res.Stream.Epoch != res.Stream.Checkpoints {
		t.Fatalf("epoch %d does not match %d checkpoints", res.Stream.Epoch, res.Stream.Checkpoints)
	}

	// Every checkpoint holds the window state that its source positions agree
	// with, which is what makes the pair recoverable.
	store, err := stream.NewFileStore(filepath.Join(state, "stream"), 3)
	if err != nil {
		t.Fatal(err)
	}
	ck, ok, err := store.Load(context.Background(), "ticker")
	if err != nil || !ok {
		t.Fatalf("load checkpoint: ok=%v err=%v", ok, err)
	}
	if len(ck.Positions) == 0 {
		t.Fatal("checkpoint recorded no source positions")
	}
	if _, ok := ck.Windows["per-minute"]; !ok {
		t.Fatalf("checkpoint recorded no window state: %+v", ck.Windows)
	}
}

func TestStreamSinkOnEveryTerminalByDefault(t *testing.T) {
	dir := eventsDir(t)
	reg, _ := fastRegistry(t)

	var mu = make(chan stream.Batch, 16)
	sink := stream.SinkFunc(func(_ context.Context, b stream.Batch) error {
		mu <- b
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := loom.Stream(ctx, incidentDesk(nil),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithSource("incidents", fileSource(t, dir)),
		loom.WithSink("", sink), loom.WithWorkers(4))
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	close(mu)

	var got []stream.Batch
	for b := range mu {
		got = append(got, b)
	}
	if len(got) != 2 {
		t.Fatalf("sink received %d batches, want one per pane", len(got))
	}
	if res.Stream.Batches != 2 {
		t.Fatalf("report says %d batches", res.Stream.Batches)
	}
	// Batch keys are stable and distinct, which is what makes replay safe.
	if got[0].Key() == got[1].Key() {
		t.Fatalf("two panes share a batch key: %q", got[0].Key())
	}
	for _, b := range got {
		if b.Stage != "digest" {
			t.Fatalf("batch came from stage %q", b.Stage)
		}
		if !strings.HasPrefix(b.Key(), "digest/") {
			t.Fatalf("batch key = %q", b.Key())
		}
	}
}
