package main

import (
	"context"
	"strings"
	"sync"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
)

// TestPipelineOffline runs the exact pipeline main() builds, on mock models —
// no key, no network. It guards the parts a compile can't: the broadcast
// template functions resolve against declared grants, MapTools decodes the
// catalog through the capability-checked session, validation passes, and
// every stage produces output.
func TestPipelineOffline(t *testing.T) {
	handler := func(req model.Request) (string, error) {
		if strings.Contains(req.Prompt, `"refund_due"`) { // classify
			return `{"category":"hardware","sentiment":"neutral","priority":"P2","refund_due":true,"issue":"mock issue"}`, nil
		}
		if strings.Contains(req.Prompt, "operations digest") { // ops-digest
			return "Mock digest.", nil
		}
		return "Mock reply.", nil // draft-reply
	}

	reg := model.NewRegistry()
	for id, tier := range map[string]model.Tier{
		"gpt-5.4-nano": model.TierFast,
		"gpt-5.4-mini": model.TierBalanced,
		"gpt-5.4":      model.TierDeep,
	} {
		if _, err := model.RegisterMock(reg, id, tier, model.WithHandler(handler)); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	reads := map[string]int{}
	res, err := loom.Run(context.Background(), buildPipeline(),
		loom.WithRegistry(reg),
		loom.WithBroadcast("catalog", catalog),
		loom.WithBroadcast("policy", policies["v1"]),
		loom.WithBroadcast("voice", voiceRubric),
		loom.WithEventHandler(func(e observe.Event) {
			// Handlers run on worker goroutines; guard the counter.
			if e.Type == observe.BroadcastRead {
				mu.Lock()
				reads[e.Broadcast]++
				mu.Unlock()
			}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if got := len(res.StageOutputs["route"]); got != len(tickets) {
		t.Errorf("route produced %d records, want %d", got, len(tickets))
	}
	if got := len(res.StageOutputs["draft-reply"]); got == 0 {
		t.Error("draft-reply produced no records")
	}
	if got := len(res.StageOutputs["ops-digest"]); got != 1 {
		t.Errorf("ops-digest produced %d records, want 1", got)
	}
	for _, r := range res.StageOutputs["route"] {
		if r.String("queue") == "" {
			t.Errorf("record %s has no queue", r.ID)
		}
		if _, ok := catalog[r.String("sku")]; ok && r.String("product") == "unknown product" {
			t.Errorf("record %s not enriched from catalog", r.ID)
		}
	}

	// Every ticket reads the catalog (enrich) and the policy (classify);
	// only reply tasks read the voice rubric.
	if reads["catalog"] != len(tickets) {
		t.Errorf("catalog read by %d tasks, want %d", reads["catalog"], len(tickets))
	}
	if reads["policy"] < len(tickets) {
		t.Errorf("policy read by %d tasks, want at least %d", reads["policy"], len(tickets))
	}
	if reads["voice"] == 0 {
		t.Error("voice rubric never read")
	}
	if len(res.Broadcasts) != 3 {
		t.Errorf("run recorded %d broadcasts, want 3", len(res.Broadcasts))
	}
}

// TestPolicyEditRecomputesOnlyReaders proves the demo's selective-invalidation
// claim: with a state dir, a rerun replays everything from cache, and swapping
// the policy broadcast to v2 recomputes exactly the stages that declared it
// (classify, draft-reply) while enrich — which reads only the catalog —
// replays from cache untouched.
func TestPolicyEditRecomputesOnlyReaders(t *testing.T) {
	reg := model.NewRegistry()
	for id, tier := range map[string]model.Tier{
		"gpt-5.4-nano": model.TierFast,
		"gpt-5.4-mini": model.TierBalanced,
		"gpt-5.4":      model.TierDeep,
	} {
		handler := func(req model.Request) (string, error) {
			if strings.Contains(req.Prompt, `"refund_due"`) {
				return `{"category":"hardware","sentiment":"neutral","priority":"P2","refund_due":true,"issue":"mock issue"}`, nil
			}
			return "Mock output.", nil
		}
		if _, err := model.RegisterMock(reg, id, tier, model.WithHandler(handler)); err != nil {
			t.Fatal(err)
		}
	}

	dir := t.TempDir()
	run := func(policy string) map[string]int {
		var mu sync.Mutex
		hits := map[string]int{}
		_, err := loom.Run(context.Background(), buildPipeline(),
			loom.WithRegistry(reg),
			loom.WithStateDir(dir),
			loom.WithBroadcast("catalog", catalog),
			loom.WithBroadcast("policy", policy),
			loom.WithBroadcast("voice", voiceRubric),
			loom.WithEventHandler(func(e observe.Event) {
				// Handlers run on worker goroutines; guard the counter.
				if e.Type == observe.CacheHit {
					mu.Lock()
					hits[e.Stage]++
					mu.Unlock()
				}
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		return hits
	}

	first := run(policies["v1"])
	if len(first) != 0 {
		t.Errorf("first run had cache hits: %v", first)
	}

	second := run(policies["v1"])
	if second["classify"] != len(tickets) || second["enrich"] != len(tickets) {
		t.Errorf("identical rerun should replay everything: %v", second)
	}

	edited := run(policies["v2"])
	if edited["enrich"] != len(tickets) {
		t.Errorf("enrich reads only the catalog and should stay cached, got %d hits of %d", edited["enrich"], len(tickets))
	}
	if edited["classify"] != 0 {
		t.Errorf("classify reads the policy and should recompute, got %d cache hits", edited["classify"])
	}
	if edited["draft-reply"] != 0 {
		t.Errorf("draft-reply reads the policy and should recompute, got %d cache hits", edited["draft-reply"])
	}
}
