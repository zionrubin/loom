package main

import (
	"context"
	"strings"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
)

// testRegistry is buildRegistry without the latency, so the test exercises the
// same scripted models at full speed.
func testRegistry(t *testing.T) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	for id, tier := range map[string]model.Tier{
		"mock-fast":     model.TierFast,
		"mock-balanced": model.TierBalanced,
		"mock-deep":     model.TierDeep,
	} {
		if err := reg.Register(model.Info{
			ID: id, Tier: tier,
			Pricing:  model.Pricing{InputPerMTok: 1, OutputPerMTok: 5},
			Provider: model.NewMock(id, model.WithHandler(respond(id))),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

func newsroomFleet(t *testing.T) *loom.Fleet {
	t.Helper()
	f, err := loom.NewFleet(
		loom.WithRegistry(testRegistry(t)),
		loom.WithWorkers(6),
		loom.WithFleetBudget(core.Budget{MaxCostUSD: 5}),
		loom.WithTopic("findings"),
		loom.WithBroadcast("style-guide", styleGuide),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// The whole newsroom, end to end: the beats report, post to the board, and the
// editor writes a page out of what they posted.
func TestNewsroomFanInThroughTheBlackboard(t *testing.T) {
	f := newsroomFleet(t)
	ctx := context.Background()

	desk := f.Go(ctx, wireDesk("wire-desk"))

	agents := map[string]*loom.Agent{}
	for _, beat := range beats {
		agents[beat] = f.Go(ctx, beatPipeline(beat))
	}
	for _, beat := range beats {
		res, err := agents[beat].Wait()
		if err != nil {
			t.Fatalf("beat %s: %v", beat, err)
		}
		if len(res.Output) != 1 {
			t.Fatalf("beat %s produced %d findings, want 1", beat, len(res.Output))
		}
		if _, err := f.PostFrom("beat-"+beat, "findings", map[string]any{
			"beat": beat, "finding": res.Output[0].String("output"),
		}); err != nil {
			t.Fatalf("post %s: %v", beat, err)
		}
	}

	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	posts, err := f.Await(awaitCtx, "findings", len(beats))
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if len(posts) != len(beats) {
		t.Fatalf("board holds %d findings, want %d", len(posts), len(beats))
	}

	page, err := f.Run(ctx, frontPage())
	if err != nil {
		t.Fatalf("front-page: %v", err)
	}
	if len(page.Output) != 1 {
		t.Fatalf("front-page produced %d records, want 1", len(page.Output))
	}
	text := page.Output[0].String("output")
	if !strings.HasPrefix(text, "EVENING EDITION") {
		t.Errorf("unexpected front page:\n%s", text)
	}
	// Every beat's finding must have reached the editor through the board.
	for _, beat := range beats {
		if !strings.Contains(text, "["+beat+"]") {
			t.Errorf("front page is missing the %s beat:\n%s", beat, text)
		}
	}

	deskRes, err := desk.Wait()
	if err != nil {
		t.Fatalf("wire-desk: %v", err)
	}
	if got := len(deskRes.StageOutputs["classify"]); got != 60 {
		t.Errorf("wire-desk classified %d reports, want 60", got)
	}
}

// The second desk over the same wire must cost nothing: the cache belongs to
// the fleet, not to the run that filled it.
func TestNewsroomRecheckIsFree(t *testing.T) {
	reg := testRegistry(t)
	info, err := reg.Get("mock-fast")
	if err != nil {
		t.Fatal(err)
	}
	fast := info.Provider.(*model.Mock)

	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithWorkers(6),
		loom.WithBroadcast("style-guide", styleGuide))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()
	if _, err := f.Run(ctx, wireDesk("wire-desk")); err != nil {
		t.Fatalf("wire-desk: %v", err)
	}
	after := fast.Calls()
	if after != 60 {
		t.Fatalf("wire-desk made %d calls, want 60", after)
	}

	if _, err := f.Run(ctx, wireDesk("wire-recheck")); err != nil {
		t.Fatalf("wire-recheck: %v", err)
	}
	if extra := fast.Calls() - after; extra != 0 {
		t.Errorf("wire-recheck made %d model calls, want 0", extra)
	}
}

// The short agents must overtake the long one even though they are launched
// after it. This is the property the fleet's admission policy exists for, and
// the reason the example is worth shipping.
func TestNewsroomShortAgentsOvertakeTheLongOne(t *testing.T) {
	reg := model.NewRegistry()
	for id, tier := range map[string]model.Tier{
		"mock-fast":     model.TierFast,
		"mock-balanced": model.TierBalanced,
		"mock-deep":     model.TierDeep,
	} {
		// Enough latency per call that a 60-task agent cannot finish before a
		// 7-task one unless admission order says otherwise.
		if err := reg.Register(model.Info{
			ID: id, Tier: tier,
			Provider: model.NewMock(id, model.WithLatency(5*time.Millisecond),
				model.WithHandler(respond(id))),
		}); err != nil {
			t.Fatal(err)
		}
	}

	f, err := loom.NewFleet(loom.WithRegistry(reg), loom.WithWorkers(4),
		loom.WithBroadcast("style-guide", styleGuide))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	ctx := context.Background()
	f.Go(ctx, wireDesk("wire-desk"))
	// Let the desk fill every slot and queue the rest of its work first.
	time.Sleep(20 * time.Millisecond)
	for _, beat := range beats {
		f.Go(ctx, beatPipeline(beat))
	}
	if err := f.Wait(); err != nil {
		t.Fatalf("fleet: %v", err)
	}

	rep := f.Report()
	jct := map[string]time.Duration{}
	for _, a := range rep.Agents {
		jct[a.Name] = a.JCT
	}
	for _, beat := range beats {
		name := "beat-" + beat
		if jct[name] >= jct["wire-desk"] {
			t.Errorf("%s finished in %s and wire-desk in %s: the short agent queued "+
				"behind the long one instead of overtaking it",
				name, jct[name], jct["wire-desk"])
		}
	}
	if rep.Pool.Admitted != 60+3*7 {
		t.Errorf("pool admitted %d tasks, want %d", rep.Pool.Admitted, 60+3*7)
	}
}
