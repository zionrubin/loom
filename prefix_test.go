package loom_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
)

const rubric = `Classify each ticket against this rubric.
billing: refunds, charges, invoices, payment failures.
bug: crashes, errors, unexpected behavior, data loss.
general: everything else, including questions about pricing.
Answer with a JSON object holding "category" and "urgent".`

// prefixRegistry registers one fast mock model with pricing that makes cache
// arithmetic legible.
func prefixRegistry(t *testing.T) (*model.Registry, *model.Mock) {
	t.Helper()
	reg := model.NewRegistry()
	mock := model.NewMock("fast", model.WithHandler(classifyMock))
	err := reg.Register(model.Info{
		ID: "fast", Provider: mock, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 10, OutputPerMTok: 40},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return reg, mock
}

func prefixPipeline(prefix string, opts ...pipeline.Option) *pipeline.Pipeline {
	p := pipeline.New("prefix")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			System:    "You are a support ticket classifier.",
			Prefix:    prefix,
			Prompt:    "Classify this ticket: {{.subject}}",
			ParseJSON: true,
		}, opts...)
	return p
}

// TestPrefixCacheSharedAcrossStage is the core shared-memory claim: a stage's
// stable prompt head is paid for once and served from the provider's cache
// for every task after the first.
func TestPrefixCacheSharedAcrossStage(t *testing.T) {
	reg, _ := prefixRegistry(t)
	res, err := loom.Run(context.Background(), prefixPipeline(rubric),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	u := res.Report.Totals()
	if u.CacheWriteTokens == 0 {
		t.Fatal("no prefix cache entry was written")
	}
	if u.CacheReadTokens == 0 {
		t.Fatal("prefix cache was written but never read")
	}
	// Four records, one shared prefix: one write and three reads.
	if got, want := u.CacheReadTokens, 3*u.CacheWriteTokens; got != want {
		t.Errorf("cache reads = %d tokens, want %d (3 reads of a 1-write prefix)", got, want)
	}
	if rate := u.CacheHitRate(); rate < 0.5 {
		t.Errorf("cache hit rate = %.2f, want the shared rubric to dominate the prompt", rate)
	}
	if saved := res.Report.PrefixSavedUSD(); saved <= 0 {
		t.Errorf("prefix savings = %f, want positive once the write is amortized", saved)
	}
}

// TestPrefixCacheSkippedForSingleCall guards the break-even rule: writing an
// entry that nothing will read is strictly worse than not caching.
func TestPrefixCacheSkippedForSingleCall(t *testing.T) {
	reg, _ := prefixRegistry(t)
	p := pipeline.New("single")
	p.FromRecords("one", tickets()[:1]).
		Infer("classify", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			Prefix:    rubric,
			Prompt:    "Classify this ticket: {{.subject}}",
			ParseJSON: true,
		})

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	u := res.Report.Totals()
	if u.CacheWriteTokens != 0 || u.CacheReadTokens != 0 {
		t.Errorf("single-call stage cached its prefix (%d written, %d read); "+
			"the write premium can never be earned back",
			u.CacheWriteTokens, u.CacheReadTokens)
	}
	if u.InputTokens == 0 {
		t.Error("prompt tokens vanished: they should be billed at the full input rate")
	}
}

// TestPrefixCacheOptOut checks WithoutPrefixCache is honored.
func TestPrefixCacheOptOut(t *testing.T) {
	reg, _ := prefixRegistry(t)
	res, err := loom.Run(context.Background(),
		prefixPipeline(rubric, pipeline.WithoutPrefixCache()),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if u := res.Report.Totals(); u.CacheReadTokens != 0 || u.CacheWriteTokens != 0 {
		t.Errorf("opted-out stage still used the prefix cache: %+v", u)
	}
}

// TestPrefixRenderedOncePerTask verifies the prefix reaches the model ahead
// of the per-record prompt, and is not re-rendered per record.
func TestPrefixRenderedOncePerTask(t *testing.T) {
	reg, _ := prefixRegistry(t)
	// Event handlers run on the worker goroutines that publish them, so the
	// collector has to be safe for concurrent use.
	var mu sync.Mutex
	var prompts []string
	res, err := loom.Run(context.Background(), prefixPipeline(rubric),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithEventHandler(func(e observe.Event) {
			if e.Type == observe.ModelCalled {
				mu.Lock()
				prompts = append(prompts, e.Prompt)
				mu.Unlock()
			}
		}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(prompts) != len(tickets()) {
		t.Fatalf("got %d model calls, want %d", len(prompts), len(tickets()))
	}
	for i, p := range prompts {
		if !strings.Contains(p, rubric) {
			t.Fatalf("call %d did not carry the shared rubric", i)
		}
		if strings.Index(p, rubric) > strings.Index(p, "Classify this ticket") {
			t.Fatalf("call %d put the rubric after the varying content; "+
				"a prefix cache needs the stable bytes first", i)
		}
	}
	_ = res
}

// TestPrefixBroadcastComposition covers the intended pairing: a broadcast
// supplies the shared value, and the prefix is where it belongs in the prompt
// so the provider caches it once for the whole stage.
func TestPrefixBroadcastComposition(t *testing.T) {
	reg, _ := prefixRegistry(t)
	p := pipeline.New("broadcast-prefix")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			Prefix:    `Rubric:\n{{broadcast "rubric"}}`,
			Prompt:    "Classify this ticket: {{.subject}}",
			ParseJSON: true,
		}, pipeline.WithBroadcast("rubric"))

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()),
		loom.WithBroadcast("rubric", rubric))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if u := res.Report.Totals(); u.CacheReadTokens == 0 {
		t.Error("a broadcast rendered into the prefix should be cache-read by later tasks")
	}
}

func TestPricingCacheRates(t *testing.T) {
	p := model.Pricing{InputPerMTok: 10, OutputPerMTok: 40}
	if got, want := p.CacheReadRate(), 1.0; got != want {
		t.Errorf("CacheReadRate = %v, want %v", got, want)
	}
	if got, want := p.CacheWriteRate(), 12.5; got != want {
		t.Errorf("CacheWriteRate = %v, want %v", got, want)
	}

	// An explicit rate overrides the derived default.
	explicit := model.Pricing{InputPerMTok: 10, CacheReadPerMTok: 3}
	if got := explicit.CacheReadRate(); got != 3 {
		t.Errorf("explicit CacheReadRate = %v, want 3", got)
	}

	// A fresh write is a loss; reads turn it into a saving.
	write := core.Usage{CacheWriteTokens: 1_000_000}
	if s := p.Saved(write); s >= 0 {
		t.Errorf("Saved(write only) = %v, want negative while unamortized", s)
	}
	amortized := core.Usage{CacheWriteTokens: 1_000_000, CacheReadTokens: 3_000_000}
	if s := p.Saved(amortized); s <= 0 {
		t.Errorf("Saved(1 write, 3 reads) = %v, want positive", s)
	}

	// Cost bills each class of prompt token at its own rate.
	u := core.Usage{InputTokens: 1_000_000, CacheReadTokens: 1_000_000,
		CacheWriteTokens: 1_000_000, OutputTokens: 1_000_000}
	if got, want := p.Cost(u), 10.0+1.0+12.5+40.0; got != want {
		t.Errorf("Cost = %v, want %v", got, want)
	}
}

func TestUsageAccounting(t *testing.T) {
	u := core.Usage{InputTokens: 10, CacheReadTokens: 60, CacheWriteTokens: 30, OutputTokens: 5}
	if got, want := u.PromptTokens(), 100; got != want {
		t.Errorf("PromptTokens = %d, want %d", got, want)
	}
	if got, want := u.TotalTokens(), 105; got != want {
		t.Errorf("TotalTokens = %d, want %d", got, want)
	}
	if got, want := u.CacheHitRate(), 0.6; got != want {
		t.Errorf("CacheHitRate = %v, want %v", got, want)
	}
	var zero core.Usage
	if zero.CacheHitRate() != 0 {
		t.Error("empty usage should report a zero hit rate, not divide by zero")
	}
}
