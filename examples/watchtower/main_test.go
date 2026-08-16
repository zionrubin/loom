package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/stream"
	"github.com/zionrubin/loom/stream/file"
)

// run drives the example's own pipeline over a generated feed, as a backfill.
func run(t *testing.T, work string, keyField string, size time.Duration, opts ...loom.Option) *loom.StreamResult {
	t.Helper()
	feed := filepath.Join(work, "feed")
	if err := mkdirs(feed, filepath.Join(work, "digests")); err != nil {
		t.Fatal(err)
	}
	src, err := file.Open(file.SourceOptions{
		Glob: filepath.Join(feed, "*.jsonl"), Time: stream.EventTime("at"),
	})
	if err != nil {
		t.Fatal(err)
	}
	sink, err := file.NewSink(file.SinkOptions{Dir: filepath.Join(work, "digests")})
	if err != nil {
		t.Fatal(err)
	}
	reg, _, _ := registry()

	base := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithSource("incidents", src),
		loom.WithSink("digest", sink),
		loom.WithLateness(maxDisorder),
		loom.WithWorkers(8),
	}
	res, err := loom.Stream(context.Background(), desk(size, 20*time.Second, keyField),
		append(base, opts...)...)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	return res
}

func mkdirs(dirs ...string) error {
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func TestBackfillProducesOnePanePerMinutePerService(t *testing.T) {
	work := t.TempDir()
	incidents := generate(7, 6, 8)
	if err := mkdirs(filepath.Join(work, "feed")); err != nil {
		t.Fatal(err)
	}
	if err := writeFeed(filepath.Join(work, "feed"), incidents, len(incidents)); err != nil {
		t.Fatal(err)
	}

	res := run(t, work, "service", time.Minute)

	if res.Stream.StopReason != "sources exhausted" {
		t.Fatalf("stop reason = %q", res.Stream.StopReason)
	}
	if res.Stream.Records != int64(len(incidents)) {
		t.Fatalf("ingested %d of %d incidents", res.Stream.Records, len(incidents))
	}
	// A generated feed spans six minutes over four services, and every
	// (minute, service) pair that saw an incident becomes one pane.
	if res.Stream.Panes < 6 || res.Stream.Panes > int64(6*len(services)) {
		t.Fatalf("panes = %d, outside the range six minutes of four services can produce",
			res.Stream.Panes)
	}
	if res.Stream.Panes != res.Stream.Batches {
		t.Fatalf("%d panes fired but %d reached the sink", res.Stream.Panes, res.Stream.Batches)
	}
	if res.Stream.Late != 0 {
		t.Fatalf("%d late records: the job's lateness does not cover the feed's disorder",
			res.Stream.Late)
	}
	if w := res.Stream.Windows["per-minute"]; w.LiveWindows != 0 {
		t.Fatalf("a drained backfill left %d windows open", w.LiveWindows)
	}
}

func TestTheSameFeedProducesTheSamePanes(t *testing.T) {
	incidents := generate(7, 4, 6)

	panes := func() []string {
		work := t.TempDir()
		if err := mkdirs(filepath.Join(work, "feed")); err != nil {
			t.Fatal(err)
		}
		if err := writeFeed(filepath.Join(work, "feed"), incidents, len(incidents)); err != nil {
			t.Fatal(err)
		}
		res := run(t, work, "service", time.Minute)
		var out []string
		for _, rec := range res.StageOutputs["digest"] {
			out = append(out, rec.String("output"))
		}
		return out
	}

	// Determinism is not a nicety here: it is what makes a replayed pane hit
	// the result cache rather than being paid for again.
	a, b := panes(), panes()
	if len(a) != len(b) {
		t.Fatalf("two runs produced %d and %d final digests", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("digest %d differs between runs: %q vs %q", i, a[i], b[i])
		}
	}
}

func TestASmallerWindowCostsMorePanesAndTheSameGradings(t *testing.T) {
	incidents := generate(7, 4, 8)

	measure := func(size time.Duration) (panes int64, gradings int) {
		work := t.TempDir()
		if err := mkdirs(filepath.Join(work, "feed")); err != nil {
			t.Fatal(err)
		}
		if err := writeFeed(filepath.Join(work, "feed"), incidents, len(incidents)); err != nil {
			t.Fatal(err)
		}
		res := run(t, work, "", size)
		for _, s := range res.Report.Stages {
			if s.Stage == "grade" {
				gradings = s.ModelCalls
			}
		}
		return res.Stream.Panes, gradings
	}

	widePanes, wideGradings := measure(time.Minute)
	narrowPanes, narrowGradings := measure(30 * time.Second)

	if narrowPanes <= widePanes {
		t.Fatalf("a 30s window fired %d panes, a 1m window %d: halving the window "+
			"should roughly double the aggregations", narrowPanes, widePanes)
	}
	if narrowGradings != wideGradings {
		t.Fatalf("gradings changed with the window size (%d vs %d): the per-record "+
			"stage runs upstream of the window and should not care",
			narrowGradings, wideGradings)
	}
}

func TestResumingAfterAnInterruptionCostsNoExtraCalls(t *testing.T) {
	work := t.TempDir()
	state := filepath.Join(work, "state")
	feed := filepath.Join(work, "feed")
	if err := mkdirs(feed, state, filepath.Join(work, "digests")); err != nil {
		t.Fatal(err)
	}
	incidents := generate(7, 4, 8)
	if err := writeFeed(feed, incidents, len(incidents)); err != nil {
		t.Fatal(err)
	}

	reg, fast, deep := registry()
	stream1 := func(opts ...loom.Option) *loom.StreamResult {
		t.Helper()
		src, err := file.Open(file.SourceOptions{
			Glob: filepath.Join(feed, "*.jsonl"), Time: stream.EventTime("at"),
		})
		if err != nil {
			t.Fatal(err)
		}
		res, err := loom.Stream(context.Background(), desk(time.Minute, 20*time.Second, "service"),
			append([]loom.Option{
				loom.WithRegistry(reg),
				loom.WithSource("incidents", src),
				loom.WithStateDir(state), loom.WithJobID("watchtower-test"),
				loom.WithLateness(maxDisorder),
				loom.WithWorkers(8),
			}, opts...)...)
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		return res
	}

	interrupted := stream1(
		loom.WithStreamLimit(stream.Limit{Records: int64(len(incidents) / 3)}),
		loom.WithCheckpointEvery(5*time.Millisecond),
		loom.WithPolling(4, 2*time.Millisecond),
	)
	if interrupted.Stream.Checkpoints == 0 {
		t.Fatal("the interrupted run recorded no checkpoint to resume from")
	}

	resumed := stream1()
	if resumed.Stream.ResumedFrom == 0 {
		t.Fatal("the second start did not resume")
	}
	if resumed.Stream.Records != int64(len(incidents)) {
		t.Fatalf("across both runs %d of %d incidents were ingested",
			resumed.Stream.Records, len(incidents))
	}
	// One grading per incident and one digest per pane, across both runs
	// together: the replay after the interruption was served from the cache.
	if got, want := fast.Calls(), len(incidents); got != want {
		t.Fatalf("gradings across both runs = %d, want %d (the interruption was re-billed)",
			got, want)
	}
	if got := int64(deep.Calls()); got != resumed.Stream.Panes {
		t.Fatalf("digests = %d for %d panes", got, resumed.Stream.Panes)
	}
}
