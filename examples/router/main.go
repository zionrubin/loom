// Command router demonstrates cost-based model routing: the escalation ladder
// used as a policy rather than only as a recovery path.
//
//	go run ./examples/router
//
// It runs the same pipeline twice against a deterministic mock model — once
// with the flat ladder Loom has always had, once with a router — and prints
// what each one paid.
//
// The stage extracts structured fields from contracts. Two thirds of them are
// "dense", and the fast model cannot do those: its output fails validation and
// the task escalates. A flat ladder pays for that doomed fast call on every
// dense contract, every time, forever — nothing caches a call that produced no
// result. The router reads the verdicts the validator is already producing and,
// once it has enough of them, stops paying to rediscover the same thing.
//
// Nothing about the answers changes. The router picks a *starting* rung;
// validation still runs and escalation still climbs, so a wrong guess costs a
// call and never an answer.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/route"
)

// The corpus: contracts of two kinds, interleaved so neither run gets an
// easier prefix than the other.
func contracts(n int) []core.Record {
	out := make([]core.Record, n)
	for i := range out {
		kind := "standard"
		if i%3 != 0 {
			kind = "dense"
		}
		out[i] = core.NewRecord(fmt.Sprintf("c%03d", i), map[string]any{
			"kind": kind,
			"body": fmt.Sprintf("Contract %03d, %s terms.", i, kind),
		})
	}
	return out
}

// registry holds a two-rung ladder priced the way a real fast/balanced pair
// is — about four times apart, the Haiku-to-Sonnet shape.
//
// The ratio is the single number that decides what routing is worth in
// dollars. Skipping a doomed call on the bottom rung saves that rung's price,
// so on a ladder whose rungs are four times apart it saves about a fifth of
// what an escalating record costs, and on one whose rungs are fifty times
// apart, almost nothing. What it saves in *calls* is the same either way, and
// on a rate-limited stage that is often the binding constraint.
func registry() (*model.Registry, error) {
	reg := model.NewRegistry()

	// The fast model handles standard contracts and returns nothing usable for
	// dense ones — which is what a validator is for.
	fast := model.NewMock("mock-fast", model.WithHandler(func(req model.Request) (string, error) {
		if strings.Contains(req.Prompt, "kind: dense") {
			return `{"party": "", "term_months": 0}`, nil
		}
		return `{"party": "Northwind", "term_months": 12}`, nil
	}))
	if err := reg.Register(model.Info{
		ID: "mock-fast", Provider: fast, Tier: model.TierFast,
		Pricing: model.Pricing{InputPerMTok: 0.80, OutputPerMTok: 4},
		Limits:  model.Limits{RequestsPerMinute: 100000},
	}); err != nil {
		return nil, err
	}

	deep := model.NewMock("mock-deep", model.WithHandler(func(model.Request) (string, error) {
		return `{"party": "Northwind Traders GmbH", "term_months": 36}`, nil
	}))
	if err := reg.Register(model.Info{
		ID: "mock-deep", Provider: deep, Tier: model.TierDeep,
		Pricing: model.Pricing{InputPerMTok: 3, OutputPerMTok: 15},
		Limits:  model.Limits{RequestsPerMinute: 100000},
	}); err != nil {
		return nil, err
	}
	return reg, nil
}

func extract(n int) *pipeline.Pipeline {
	p := pipeline.New("contract-extract")
	p.FromRecords("contracts", contracts(n)).
		Infer("extract", pipeline.InferSpec{
			// The ladder. Without a router this says "always start cheap, climb
			// when the output is bad". With one it also says "and learn which
			// records that was never going to work for".
			Binding: model.Binding{
				Tier:       model.TierFast,
				Escalation: []string{"mock-deep"},
			},
			System:    "You extract structured fields from contracts.",
			Prompt:    "Extract party and term. kind: {{.kind}}\n{{.body}}",
			ParseJSON: true,
			// The oracle. It already runs on every record and already says
			// whether the model that ran was strong enough — a router is what
			// keeps that answer instead of throwing it away.
			Validate: func(r core.Record) error {
				if r.String("party") == "" {
					return fmt.Errorf("no party extracted")
				}
				return nil
			},
		})
	return p
}

const corpus = 300

func main() {
	reg, err := registry()
	if err != nil {
		log.Fatal(err)
	}

	base := []loom.Option{loom.WithRegistry(reg), loom.WithWorkers(4)}

	flat, err := loom.Run(context.Background(), extract(corpus), base...)
	if err != nil {
		log.Fatal(err)
	}

	// A state directory, so the calibration this run pays for outlives it.
	// Only the routed run gets one — sharing it would let the second run
	// replay the first from the result cache and pay nothing, which would make
	// for a flattering comparison and a meaningless one.
	dir, err := os.MkdirTemp("", "loom-routing-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// The router. ByField("kind") is the whole configuration: a pipeline that
	// knows what makes its records differ should say so, because the default
	// featurizer can only guess from size.
	routed, err := loom.Run(context.Background(), extract(corpus),
		append(base, loom.WithStateDir(dir), loom.WithRouting(route.Config{
			Features:   route.ByField("kind"),
			MinSamples: 20,
			ProbeRate:  0.05,
		}))...)
	if err != nil {
		log.Fatal(err)
	}

	report(flat, routed)

	// Cost before you spend it, calibrated by what was actually spent. The
	// projection reads the profile the run just wrote and prices the
	// escalations — which the columns above it, one call per record at the
	// base model, leave out entirely.
	proj, err := loom.Explain(extract(10*corpus),
		loom.WithRegistry(reg), loom.WithStateDir(dir),
		loom.WithRouting(route.Config{Features: route.ByField("kind"), MinSamples: 20}))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n--- and what the next run, ten times the size, would cost ---\n\n%s", proj)
}

func report(flat, routed *loom.RunResult) {
	ft, rt := flat.Report.Totals(), routed.Report.Totals()

	fmt.Printf("%d contracts, two thirds of them beyond the fast model.\n\n", corpus)
	_ = observe.PayloadCap
	fmt.Printf("%-28s %8s %12s\n", "", "calls", "cost($)")
	fmt.Printf("%-28s %8d %12.4f\n", "flat ladder", ft.Requests, ft.CostUSD)
	fmt.Printf("%-28s %8d %12.4f\n", "routed", rt.Requests, rt.CostUSD)
	fmt.Printf("%-28s %8d %12.4f\n", "difference",
		ft.Requests-rt.Requests, ft.CostUSD-rt.CostUSD)

	fmt.Printf("\n%s", routed.Report)

	// The answers, which is the part that must not have moved.
	same := 0
	for i := range flat.Output {
		if flat.Output[i].String("party") == routed.Output[i].String("party") {
			same++
		}
	}
	fmt.Printf("\n%d/%d records carry identical output: routing changes where a\n"+
		"record starts, never what it says.\n", same, len(flat.Output))

	if s := routed.Routing; s.Decisions > 0 {
		fmt.Printf("\nrouter: %d decisions, %d declined for want of evidence, %d moved,\n"+
			"        %d held back as probes of which %d were answered at the bottom rung.\n",
			s.Decisions, s.Cold, s.Moved, s.Probes, s.ProbeHits)
	}
}
