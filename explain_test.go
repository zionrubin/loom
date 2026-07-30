package loom_test

import (
	"context"
	"math"
	"strings"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

// The projection's expected-output assumption is pinned in these tests so the
// mock can return a response of exactly that length, which makes the
// prompt-side arithmetic comparable to the real run's down to the token.
const (
	explainRatio     = 0.5
	explainMaxTokens = 100
	explainOutTokens = int(explainRatio * explainMaxTokens)
)

const digestRubric = `Aggregate the summaries below into one digest.
Preserve every distinct claim, drop repetition, and keep the ordering stable.
Write plainly: no preamble, no restatement of these instructions.`

// fixedLengthMock returns a response whose length matches what the projection
// assumes a response will be, so any surviving difference between projection
// and run is a real disagreement rather than the output-length assumption.
func fixedLengthMock(model.Request) (string, error) {
	return strings.Repeat("x", 4*explainOutTokens), nil
}

func explainRegistry(t *testing.T) (*model.Registry, *model.Mock) {
	t.Helper()
	reg := model.NewRegistry()
	mock := model.NewMock("fast", model.WithHandler(fixedLengthMock))
	err := reg.Register(model.Info{
		ID: "fast", Provider: mock, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 10, OutputPerMTok: 40},
		Limits:  model.Limits{RequestsPerMinute: 600, TokensPerMinute: 100_000},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return reg, mock
}

// explainPipeline exercises every shape a projection has to reason about: a
// materialized source, a pure stage that changes the record count, a
// per-record inference with a shared prefix, and a reduce tree deep enough to
// have more than one level.
func explainPipeline() *pipeline.Pipeline {
	recs := make([]core.Record, 0, 9)
	for i := range 9 {
		recs = append(recs, core.NewRecord(
			string(rune('a'+i)),
			map[string]any{"subject": strings.Repeat("word ", 10+i), "keep": i%3 != 0},
		))
	}

	p := pipeline.New("explain")
	p.FromRecords("docs", recs).
		Filter("keepers", func(r core.Record) (bool, error) {
			keep, _ := r.Data["keep"].(bool)
			return keep, nil
		}).
		Infer("summarize", pipeline.InferSpec{
			Binding:   model.Binding{Tier: model.TierFast},
			System:    "You summarize documents.",
			Prefix:    rubric,
			Prompt:    "Summarize: {{.subject}}",
			MaxTokens: explainMaxTokens,
		}).
		ReduceAI("digest", pipeline.ReduceAISpec{
			Binding:   model.Binding{Tier: model.TierFast},
			System:    "You aggregate summaries.",
			Prefix:    digestRubric,
			Prompt:    "Combine {{.Count}} summaries:\n{{range .Items}}- {{.}}\n{{end}}",
			FanIn:     4,
			MaxTokens: explainMaxTokens,
		})
	return p
}

// TestExplainMatchesRun is the claim the whole projection rests on: because a
// pipeline's cheap stages are executable Go and its expensive stages are
// declarative data, the prompt side of a run can be computed exactly before
// the run happens — call for call, token for token, including which tokens the
// provider's prefix cache serves.
func TestExplainMatchesRun(t *testing.T) {
	p := explainPipeline()
	reg, mock := explainRegistry(t)

	proj, err := loom.Explain(p, loom.WithRegistry(reg),
		loom.WithExpectedOutput(explainRatio))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if len(proj.Warnings) > 0 {
		t.Errorf("projection is fully computable but warned: %v", proj.Warnings)
	}
	if mock.Calls() != 0 {
		t.Fatalf("Explain issued %d model calls; a projection must not spend", mock.Calls())
	}

	res, err := loom.Run(context.Background(), p,
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	want := res.Report.Totals()
	got := proj.Expected()

	if got.Requests != want.Requests {
		t.Errorf("projected %d model calls, run made %d", got.Requests, want.Requests)
	}
	if got.InputTokens != want.InputTokens {
		t.Errorf("projected %d full-price prompt tokens, run billed %d",
			got.InputTokens, want.InputTokens)
	}
	if got.CacheWriteTokens != want.CacheWriteTokens {
		t.Errorf("projected %d cache-write tokens, run billed %d",
			got.CacheWriteTokens, want.CacheWriteTokens)
	}
	if got.CacheReadTokens != want.CacheReadTokens {
		t.Errorf("projected %d cache-read tokens, run billed %d",
			got.CacheReadTokens, want.CacheReadTokens)
	}
	if want.CacheReadTokens == 0 {
		t.Fatal("the run never read a shared prefix, so this test is not checking one")
	}

	// The ceiling rests on MaxTokens, which the provider enforces, so it is a
	// bound and not an estimate.
	if ceiling := proj.Ceiling(); ceiling.CostUSD < want.CostUSD {
		t.Errorf("run cost $%.6f, above the projected ceiling $%.6f",
			want.CostUSD, ceiling.CostUSD)
	}
	// The expected column carries the one assumption in the projection, so it
	// is checked as an approximation.
	if rel := math.Abs(got.CostUSD-want.CostUSD) / want.CostUSD; rel > 0.05 {
		t.Errorf("expected cost $%.6f is %.1f%% off the run's $%.6f",
			got.CostUSD, 100*rel, want.CostUSD)
	}
}

// TestExplainRecordCountsAreExact checks the projection walks the real record
// counts rather than a selectivity guess: the filter drops a third of the
// input, and the reduce tree's shape follows from what survives.
func TestExplainRecordCountsAreExact(t *testing.T) {
	reg, _ := explainRegistry(t)
	proj, err := loom.Explain(explainPipeline(), loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}

	byStage := map[string]loom.StageProjection{}
	for _, s := range proj.Stages {
		byStage[s.Stage] = s
	}

	// Nine records in, every third one dropped by the filter.
	if got := byStage["docs"].Records; got != 9 {
		t.Errorf("source records = %d, want 9", got)
	}
	if got := byStage["summarize"].Records; got != 6 {
		t.Errorf("records reaching summarize = %d, want 6 (the filter drops 3)", got)
	}
	if got := byStage["summarize"].Calls; got != 6 {
		t.Errorf("summarize calls = %d, want one per record", got)
	}
	// Six records at fan-in 4: two groups, then one final group over their
	// outputs.
	if got := byStage["digest"].Calls; got != 3 {
		t.Errorf("digest calls = %d, want 3 (2 groups + 1 final aggregation)", got)
	}
}

// TestExplainBatchedStageForfeitsPrefixCache documents a discrepancy the
// projection is meant to surface: the planner keys prefix caching on how many
// *tasks* a stage builds, so a stage whose whole input fits one batch sends
// its shared prefix uncached on every one of its calls. The run is the
// authority here, and the projection agrees with the run.
func TestExplainBatchedStageForfeitsPrefixCache(t *testing.T) {
	build := func() *pipeline.Pipeline {
		p := pipeline.New("batched")
		p.FromRecords("tickets", tickets()).
			Infer("classify", pipeline.InferSpec{
				Binding:   model.Binding{Tier: model.TierFast},
				System:    "You classify support tickets.",
				Prefix:    rubric,
				Prompt:    "Classify this ticket: {{.subject}}",
				MaxTokens: explainMaxTokens,
			}, pipeline.WithBatchSize(len(tickets())))
		return p
	}
	reg, _ := explainRegistry(t)

	proj, err := loom.Explain(build(), loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	stage := proj.Stages[len(proj.Stages)-1]
	if stage.Tasks != 1 || stage.Calls != len(tickets()) {
		t.Fatalf("stage = %d task(s)/%d calls, want 1 task and %d calls",
			stage.Tasks, stage.Calls, len(tickets()))
	}
	if stage.CachePrefix {
		t.Error("projection expects prefix caching on a single-task stage")
	}
	if !hasWarning(proj.Warnings, "prefix caching on task count") {
		t.Errorf("projection did not flag the forfeited prefix cache: %v", proj.Warnings)
	}

	res, err := loom.Run(context.Background(), build(),
		loom.WithRegistry(reg), loom.WithRetry(quickRetry()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	u := res.Report.Totals()
	if u.CacheReadTokens != 0 || u.CacheWriteTokens != 0 {
		t.Errorf("run cached the prefix after all (%d written, %d read); the "+
			"projection and its warning are now wrong", u.CacheWriteTokens, u.CacheReadTokens)
	}
	if got := proj.Expected().InputTokens; got != u.InputTokens {
		t.Errorf("projected %d full-price prompt tokens, run billed %d", got, u.InputTokens)
	}
}

// TestExplainWarnsOnFunctionSource covers the deliberate refusal: a source
// function may read the outside world, so a projection declines to call it and
// says what that costs the projection.
func TestExplainWarnsOnFunctionSource(t *testing.T) {
	reg, _ := explainRegistry(t)
	build := func() *pipeline.Pipeline {
		p := pipeline.New("fn-source")
		p.FromFunc("load", func(context.Context) ([]core.Record, error) {
			t.Error("Explain invoked the source function")
			return tickets(), nil
		}).
			Infer("classify", pipeline.InferSpec{
				Binding: model.Binding{Tier: model.TierFast},
				Prompt:  "Classify: {{.subject}}",
			})
		return p
	}

	proj, err := loom.Explain(build(), loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !hasWarning(proj.Warnings, "does not invoke") {
		t.Errorf("no warning about the unprojectable source: %v", proj.Warnings)
	}
	if got := proj.Expected().Requests; got != 0 {
		t.Errorf("projected %d calls downstream of an unknown source, want 0", got)
	}

	// Supplying the records the source would produce makes the rest of the
	// pipeline projectable again.
	sampled, err := loom.Explain(build(), loom.WithRegistry(reg),
		loom.WithSourceSample("load", tickets()))
	if err != nil {
		t.Fatalf("explain with sample: %v", err)
	}
	if len(sampled.Warnings) > 0 {
		t.Errorf("sampled projection still warned: %v", sampled.Warnings)
	}
	if got := sampled.Expected().Requests; got != len(tickets()) {
		t.Errorf("projected %d calls from the sample, want %d", got, len(tickets()))
	}
}

// TestExplainFlagsUndeclaredBroadcast is the cheapest possible place to find
// out that a prefix reads a shared value its stage never asked for.
func TestExplainFlagsUndeclaredBroadcast(t *testing.T) {
	reg, _ := explainRegistry(t)
	p := pipeline.New("undeclared")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prefix:  `Rubric:\n{{broadcast "taxonomy"}}`,
			Prompt:  "Classify: {{.subject}}",
		}, pipeline.WithBroadcast("rubric"))

	proj, err := loom.Explain(p, loom.WithRegistry(reg),
		loom.WithBroadcast("rubric", rubric),
		loom.WithBroadcast("taxonomy", "billing, bug, general"))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !hasWarning(proj.Warnings, "not declared by this stage") {
		t.Errorf("undeclared broadcast read was not flagged: %v", proj.Warnings)
	}
}

// TestExplainBudgetVerdict checks the projection answers the question a budget
// is really asking: will this run finish, or stop partway with partial results?
func TestExplainBudgetVerdict(t *testing.T) {
	reg, _ := explainRegistry(t)
	generous, err := loom.Explain(explainPipeline(), loom.WithRegistry(reg),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 100}))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !generous.FitsBudget() {
		t.Errorf("a $100 budget should cover a ceiling of $%.6f", generous.Ceiling().CostUSD)
	}

	tight, err := loom.Explain(explainPipeline(), loom.WithRegistry(reg),
		loom.WithRunBudget(core.Budget{MaxCostUSD: 1e-9}))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if tight.FitsBudget() {
		t.Error("a sub-nanodollar budget was reported as covering the run")
	}
	if !strings.Contains(tight.String(), "partial results") {
		t.Errorf("the report does not say what an exhausted budget does:\n%s", tight)
	}
}

// TestExplainAdmissionFloor checks the projection reports the wall-clock floor
// provider rate limits impose, which no amount of concurrency can beat.
func TestExplainAdmissionFloor(t *testing.T) {
	reg := model.NewRegistry()
	// One request per minute: nine calls cannot finish in under eight minutes.
	err := reg.Register(model.Info{
		ID: "slow", Provider: model.NewMock("slow", model.WithHandler(fixedLengthMock)),
		Tier: model.TierFast, Pricing: model.Pricing{InputPerMTok: 10, OutputPerMTok: 40},
		Limits: model.Limits{RequestsPerMinute: 1},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	proj, err := loom.Explain(explainPipeline(), loom.WithRegistry(reg))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	calls := proj.Expected().Requests
	if floor := proj.AdmissionFloor().Minutes(); floor < float64(calls)-1 {
		t.Errorf("admission floor = %.1f min for %d calls at 1 req/min, want ≈%d",
			floor, calls, calls)
	}
}

// TestExplainReportsPlanErrors checks Explain fails on the same authoring
// mistakes Run does — it compiles the pipeline rather than approximating it —
// so it doubles as a validation pass that costs nothing.
func TestExplainReportsPlanErrors(t *testing.T) {
	reg, _ := explainRegistry(t)
	p := pipeline.New("broken")
	p.FromRecords("tickets", tickets()).
		Infer("classify", pipeline.InferSpec{
			Binding: model.Binding{Model: "no-such-model"},
			Prompt:  "Classify: {{.subject}}",
		})
	if _, err := loom.Explain(p, loom.WithRegistry(reg)); err == nil {
		t.Error("Explain accepted a pipeline bound to an unregistered model")
	}
}

func hasWarning(warnings []string, substr string) bool {
	for _, w := range warnings {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}
