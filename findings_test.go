package loom_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/findings"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/security"
)

// searchTool is a stand-in for a public source: a network tool that counts its
// calls, so a test can say exactly how much research a fleet actually did.
type searchTool struct {
	calls  int32
	served map[string]int32
	slow   time.Duration
}

func (s *searchTool) Name() string     { return "search" }
func (s *searchTool) Endpoint() string { return "api.search.example" }

func (s *searchTool) Invoke(ctx context.Context, args map[string]any) (any, error) {
	atomic.AddInt32(&s.calls, 1)
	if s.slow > 0 {
		select {
		case <-time.After(s.slow):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	q, _ := args["query"].(string)
	company, _ := args["company"].(string)
	return map[string]any{
		"text":       fmt.Sprintf("Findings for %s: revenue $4.2bn, 12,000 staff.", company),
		"structured": map[string]any{"revenue": "4.2bn", "headcount": 12000, "asked": q},
	}, nil
}

func (s *searchTool) Count() int { return int(atomic.LoadInt32(&s.calls)) }

// researchPipeline asks the search tool one question per company, phrased by
// the agent's own house style — which is exactly how two agents doing the same
// research end up with two different cache keys.
func researchPipeline(name, phrasing string, companies []string) *pipeline.Pipeline {
	recs := make([]core.Record, len(companies))
	for i, c := range companies {
		recs[i] = core.NewRecord(fmt.Sprintf("%s-%s", name, c), map[string]any{"company": c})
	}
	p := pipeline.New(name)
	p.FromRecords("src", recs).
		MapTools("research", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
			out, err := s.Invoke(ctx, "search", map[string]any{
				"query":   fmt.Sprintf(phrasing, r.String("company")),
				"company": r.String("company"),
			})
			if err != nil {
				return core.Record{}, err
			}
			nr := r.Clone()
			if m, ok := out.(map[string]any); ok {
				nr.Data["brief"] = m["text"]
				if prov, ok := m["findings"].(map[string]any); ok {
					nr.Data["origin"] = prov["origin"]
				}
			}
			return nr, nil
		}, pipeline.WithGrants(security.ToolCap("search")), pipeline.WithNoCache()).
		Infer("summarize", pipeline.InferSpec{
			Binding: model.Binding{Tier: model.TierFast},
			Prompt:  "Summarize: {{.brief}}",
		})
	return p
}

func searchRegistry(t *testing.T) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	if _, err := model.RegisterMock(reg, "mock-fast", model.TierFast,
		model.WithHandler(func(req model.Request) (string, error) {
			return "summary", nil
		})); err != nil {
		t.Fatal(err)
	}
	return reg
}

// The headline property, end to end through the public API: several agents
// researching the same subjects in their own words do the research once.
func TestFleetAgentsShareResearchThroughTheGate(t *testing.T) {
	companies := []string{"northwind", "contoso", "fabrikam"}
	phrasings := []string{
		"what is %s's annual revenue and headcount",
		"revenue and staff numbers for %s",
		"tell me about %s: earnings, employees",
		"%s financial profile",
	}
	src := &searchTool{}

	fleet, err := loom.NewFleet(
		loom.WithRegistry(searchRegistry(t)),
		loom.WithWorkers(6),
		loom.WithTools(src),
		loom.WithEgress("api.search.example"),
		loom.WithFindings(findings.Config{
			Gate: []string{"search"},
			Policy: findings.Policy{
				Topics: map[string]findings.TopicPolicy{
					"search": {Volatility: findings.Slow},
				},
			},
		}),
	)
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	defer fleet.Close()

	ctx := context.Background()
	for i, phrasing := range phrasings {
		fleet.Go(ctx, researchPipeline(fmt.Sprintf("desk-%d", i), phrasing, companies))
	}
	if err := fleet.Wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}

	rep := fleet.Report()
	askers := len(phrasings) * len(companies)
	if rep.Findings.Asked != askers {
		t.Fatalf("questions asked = %d, want %d", rep.Findings.Asked, askers)
	}
	// Four agents, three companies, four phrasings each: twelve questions about
	// three subjects. The facets say which subject, so the class tier answers
	// the nine that are re-askings — without an embedder, without a model, and
	// without the exact key ever matching.
	if src.Count() != len(companies) {
		t.Fatalf("external calls = %d, want %d (one per subject)", src.Count(), len(companies))
	}
	if reused := rep.Findings.Reused(); reused != askers-len(companies) {
		t.Fatalf("reused = %d, want %d", reused, askers-len(companies))
	}
	if rep.Findings.AvoidedTime <= 0 {
		t.Fatalf("a reused tool call should credit the wall clock it avoided")
	}
	if len(rep.Commons) != 1 || rep.Commons[0].Live != len(companies) {
		t.Fatalf("commons should hold one live finding per subject, got %+v", rep.Commons)
	}
}

// A gated tool must still be a gated tool: the executor's capability and egress
// checks run before the guard, so the commons cannot become a way around them.
func TestGatedToolStillRequiresItsGrant(t *testing.T) {
	src := &searchTool{}
	fleet, err := loom.NewFleet(
		loom.WithRegistry(searchRegistry(t)),
		loom.WithTools(src),
		loom.WithEgress("api.search.example"),
		loom.WithFindings(findings.Config{Gate: []string{"search"}}),
	)
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	defer fleet.Close()

	p := pipeline.New("ungranted")
	p.FromRecords("src", []core.Record{core.NewRecord("a", map[string]any{"company": "northwind"})}).
		MapTools("research", func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
			_, err := s.Invoke(ctx, "search", map[string]any{"query": "x", "company": "northwind"})
			return r, err
		}, pipeline.WithNoCache()) // ← no WithGrants

	if _, err := fleet.Run(context.Background(), p); err == nil {
		t.Fatalf("a stage that never declared the tool must not reach it through the gate")
	}
	if src.Count() != 0 {
		t.Fatalf("the source was called %d times by an ungranted stage", src.Count())
	}
}

// Findings events are the audit trail of the layer: what was learned, what was
// served, and what it saved.
func TestFindingsEventsReportLearnedAndServed(t *testing.T) {
	src := &searchTool{}
	var learned, served int32
	fleet, err := loom.NewFleet(
		loom.WithRegistry(searchRegistry(t)),
		loom.WithTools(src),
		loom.WithEgress("api.search.example"),
		loom.WithFindings(findings.Config{Gate: []string{"search"}}),
		loom.WithEventHandler(func(e observe.Event) {
			switch e.Type {
			case observe.FindingLearned:
				atomic.AddInt32(&learned, 1)
			case observe.FindingServed:
				atomic.AddInt32(&served, 1)
			}
		}),
	)
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	defer fleet.Close()

	ctx := context.Background()
	if _, err := fleet.Run(ctx, researchPipeline("a", "profile of %s", []string{"northwind"})); err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, err := fleet.Run(ctx, researchPipeline("b", "what about %s", []string{"northwind"})); err != nil {
		t.Fatalf("run: %v", err)
	}
	if learned != 1 {
		t.Fatalf("finding.learned = %d, want 1", learned)
	}
	if served != 1 {
		t.Fatalf("finding.served = %d, want 1", served)
	}
}

// The stage sees where its answer came from, which is what lets a prompt tell a
// model it is reading someone else's research rather than fresh sources.
func TestServedAnswersCarryTheirProvenance(t *testing.T) {
	src := &searchTool{}
	fleet, err := loom.NewFleet(
		loom.WithRegistry(searchRegistry(t)),
		loom.WithTools(src),
		loom.WithEgress("api.search.example"),
		loom.WithFindings(findings.Config{Gate: []string{"search"}}),
	)
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	defer fleet.Close()

	ctx := context.Background()
	first, err := fleet.Run(ctx, researchPipeline("a", "profile of %s", []string{"northwind"}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	second, err := fleet.Run(ctx, researchPipeline("b", "anything on %s", []string{"northwind"}))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := first.StageOutputs["research"][0].String("origin"); got != string(findings.OriginFresh) {
		t.Fatalf("first agent origin = %q, want fresh", got)
	}
	if got := second.StageOutputs["research"][0].String("origin"); got != string(findings.OriginClass) {
		t.Fatalf("second agent origin = %q, want class", got)
	}
}

// Registering a gate for a tool nobody registered is a configuration error, and
// it should stop the fleet at provisioning rather than at the first record that
// reaches it — the same rule MCP connection failures follow.
func TestGatingAnUnknownToolFailsAtProvisioning(t *testing.T) {
	_, err := loom.NewFleet(
		loom.WithRegistry(searchRegistry(t)),
		loom.WithFindings(findings.Config{Gate: []string{"nope"}}),
	)
	if err == nil {
		t.Fatalf("gating an unregistered tool must fail the fleet")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("error should say what is wrong: %v", err)
	}
}

// The commons is fleet-wide machinery, like the governor and the cache, so an
// agent cannot bring its own.
func TestFindingsIsFleetWide(t *testing.T) {
	fleet, err := loom.NewFleet(loom.WithRegistry(searchRegistry(t)))
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	defer fleet.Close()

	a := fleet.Go(context.Background(), researchPipeline("a", "p %s", []string{"x"}),
		loom.WithFindings(findings.Config{Recall: true}))
	if _, err := a.Wait(); err == nil || !strings.Contains(err.Error(), "fleet-wide") {
		t.Fatalf("per-agent WithFindings should be rejected, got %v", err)
	}
}

// Retraction reaches the agents that were served the claim, which is the point
// of recording who was.
func TestRetractionThroughTheFleetFindsItsDependents(t *testing.T) {
	src := &searchTool{}
	fleet, err := loom.NewFleet(
		loom.WithRegistry(searchRegistry(t)),
		loom.WithTools(src),
		loom.WithEgress("api.search.example"),
		loom.WithFindings(findings.Config{Gate: []string{"search"}}),
	)
	if err != nil {
		t.Fatalf("fleet: %v", err)
	}
	defer fleet.Close()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := fleet.Run(ctx, researchPipeline(fmt.Sprintf("a%d", i),
			"profile %s", []string{"northwind"})); err != nil {
			t.Fatalf("run: %v", err)
		}
	}
	gate := fleet.Findings()
	if gate == nil {
		t.Fatalf("fleet should expose its gate")
	}
	entries := gate.Ledger.Entries()
	if len(entries) != 1 {
		t.Fatalf("ledger entries = %d, want 1", len(entries))
	}
	deps, err := gate.Ledger.Retract(entries[0].Finding.ID, "source withdrew it", time.Now())
	if err != nil {
		t.Fatalf("retract: %v", err)
	}
	// Two agents were served the finding; the first learned it.
	if len(deps) != 2 {
		t.Fatalf("dependents = %d, want 2", len(deps))
	}
	// And the next agent researches again rather than reading a withdrawn claim.
	before := src.Count()
	if _, err := fleet.Run(ctx, researchPipeline("after", "profile %s", []string{"northwind"})); err != nil {
		t.Fatalf("run: %v", err)
	}
	if src.Count() != before+1 {
		t.Fatalf("a retracted claim must send the next asker back to the source")
	}
}
