// Command clearance runs one pipeline three times under three deployment
// policies, and prints what each of them permitted.
//
//	go run ./examples/clearance                # the three rounds
//	go run ./examples/clearance -policy p.json # a policy read from a file
//	go run ./examples/clearance -show-audit    # the trail each round leaves
//
// The pipeline never changes. It reads a queue of support tickets, classifies
// each one, and writes a briefing — the ordinary shape of the work. What
// changes between rounds is the document the *deployment* holds, which the
// author of the pipeline neither writes nor can edit:
//
//	ungoverned   no policy at all. Every stage gets what it asked for, which
//	             is what Loom always did.
//	self-hosted  customer data is classified "pii", and only models the
//	             deployment runs itself are cleared to see it. The pipeline is
//	             bound to a vendor model, so the run is refused before a single
//	             call — and the refusal names the stage, the model, and the
//	             rule.
//	cleared      the same policy against a pipeline bound to the cleared
//	             model. It runs, and every call that touched classified data
//	             leaves an affirmative line in the audit log.
//
// What to look for:
//
//   - **the refusal costs nothing.** The middle round makes zero model calls.
//     Admission happens against the compiled plan, before a scheduler exists,
//     so a pipeline that a deployment will not permit is refused rather than
//     discovered halfway through a bill.
//   - **the class is declared once.** Only the source says "pii". The stage
//     that calls a model is two hops downstream and never mentions it: the
//     planner propagates the label along the graph, because that is where the
//     data goes. Forgetting to repeat it is the mistake the scheme exists to
//     prevent, so there is nothing to forget.
//   - **the ladder is judged whole.** Uncomment the escalation in the third
//     round and it is refused too: a stage that starts on a cleared model and
//     escalates to an uncleared one leaks on the second call, and finding that
//     out then is finding it out too late.
//   - **the projection answers first.** Explain refuses the same plan a run
//     would, for the same reasons, without making a call — so "may this run"
//     is answered next to "what will it cost".
//   - **the trail keeps both halves.** -show-audit prints the admission itself,
//     not only the denials: a deployment asked to prove a run was authorized
//     needs the line that says so.
//
// It runs entirely offline against mock models, with no keys.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/policy"
	"github.com/zionrubin/loom/security"
)

func main() {
	policyFile := flag.String("policy", "", "read the governing policy from a JSON file")
	showAudit := flag.Bool("show-audit", false, "print the audit trail each round leaves")
	flag.Parse()

	reg, calls := registry()

	// Two models: one the deployment runs on its own hardware, one a vendor's.
	// Nothing about the pipeline distinguishes them — that is the deployment's
	// business, and the whole point of holding it somewhere else.
	const (
		selfHosted = "local/llama-3.1-70b"
		vendor     = "vendor/frontier-1"
	)

	// The governing document. It is data, not code: -policy reads the same
	// thing from a file, which is how it reaches production — reviewed and
	// versioned beside the infrastructure it governs.
	governed := policy.Policy{
		Name:    "self-hosted-pii",
		Egress:  policy.Rule{Allow: []string{"*.internal"}},
		Sandbox: policy.Rule{Allow: []string{"inline"}},
		Data: map[string]policy.Rule{
			"pii": {Allow: []string{"local/*"}},
		},
		MaxCostUSD: 5,
	}
	if *policyFile != "" {
		b, err := os.ReadFile(*policyFile)
		if err != nil {
			log.Fatal(err)
		}
		if governed, err = policy.Parse(b); err != nil {
			log.Fatal(err)
		}
	}

	rounds := []struct {
		name    string
		policy  policy.Policy
		binding model.Binding
		note    string
	}{
		{"unregulated", policy.Policy{}, model.Binding{Model: vendor},
			"no policy: the pipeline's author decides everything"},
		{"governed, uncleared model", governed, model.Binding{Model: vendor},
			"pii may only reach a self-hosted model"},
		{"governed, cleared model", governed, model.Binding{Model: selfHosted},
			"the same policy, a pipeline that conforms"},
	}

	for _, r := range rounds {
		before := calls()
		fmt.Printf("\n── %s ─────────────────────────────────\n", r.name)
		fmt.Printf("   %s\n\n", r.note)

		// The projection first: a plan the deployment will not permit is
		// refused here too, without a call and without a scheduler.
		proj, err := loom.Explain(supportDesk(r.binding),
			loom.WithRegistry(reg), loom.WithPolicy(r.policy))
		switch {
		case denied(err):
			fmt.Printf("%s\n", refusal(err))
		case err != nil:
			log.Fatal(err)
		default:
			fmt.Printf("   projection: %d calls, $%.4f expected, ceiling $%.4f\n",
				proj.Expected().Requests, proj.Expected().CostUSD, proj.Ceiling().CostUSD)
			if proj.Policy.Checked > 0 {
				fmt.Printf("   %s\n", proj.Policy)
			}
		}

		res, err := loom.Run(context.Background(), supportDesk(r.binding),
			loom.WithRegistry(reg), loom.WithPolicy(r.policy))
		if denied(err) {
			fmt.Printf("   run refused. model calls this round: %d\n", calls()-before)
			continue
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("   ran: %d tickets classified, %d model calls, $%.4f\n",
			len(res.StageOutputs["classify"]), calls()-before, res.Spent.CostUSD)
		if len(res.Output) == 1 {
			fmt.Printf("   briefing: %s\n", res.Output[0].String("output"))
		}
		if *showAudit {
			printAudit(res.Audit)
		}
	}

	fmt.Printf("\nThe pipeline was identical in all three rounds. What changed is a\n" +
		"document the deployment holds and its author cannot edit.\n")
}

// supportDesk is the work: tickets in, a classification each, a briefing out.
// The only governance in it is one label on the source — everything else the
// planner derives.
func supportDesk(binding model.Binding) *pipeline.Pipeline {
	p := pipeline.New("support-desk")

	// The class is declared exactly here, where the data enters. The stage
	// that calls a model is two hops down and never mentions it.
	src := p.FromRecords("queue", tickets(), pipeline.WithDataClass("pii"))

	src.
		Map("normalize", func(r core.Record) (core.Record, error) {
			r.Data["subject"] = strings.TrimSpace(r.String("subject"))
			return r, nil
		}, pipeline.WithVersion("v1")).
		Infer("classify", pipeline.InferSpec{
			Binding: binding,
			// Uncomment to watch the ladder judged whole: a cleared starting
			// rung does not make an uncleared one above it acceptable.
			// Binding: model.Binding{Model: binding.Model,
			//     Escalation: []string{"vendor/frontier-1"}},
			System:    "You classify support tickets.",
			Prompt:    "Classify this ticket from {{.customer}}: {{.subject}}",
			ParseJSON: true,
		}).
		ReduceAI("briefing", pipeline.ReduceAISpec{
			Binding:   binding,
			Prompt:    "Summarize {{.Count}} tickets:\n{{range .Items}}- {{.}}\n{{end}}",
			FanIn:     4,
			ItemField: "category",
		})
	return p
}

func tickets() []core.Record {
	raw := []struct{ id, customer, subject string }{
		{"t1", "A. Okafor", "URGENT refund not processed"},
		{"t2", "L. Zhang", "app crashes on login"},
		{"t3", "M. Silva", "question about annual pricing"},
		{"t4", "K. Novak", "URGENT charged twice this month"},
		{"t5", "R. Haddad", "cannot reset my password"},
	}
	out := make([]core.Record, 0, len(raw))
	for _, r := range raw {
		out = append(out, core.NewRecord(r.id, map[string]any{
			"customer": r.customer, "subject": r.subject,
		}))
	}
	return out
}

// registry offers the two models and counts what the rounds actually spend, so
// "the refusal cost nothing" is a measurement rather than a claim.
func registry() (*model.Registry, func() int) {
	reg := model.NewRegistry()
	handler := model.WithHandler(func(req model.Request) (string, error) {
		if strings.Contains(req.Prompt, "Summarize") {
			return fmt.Sprintf("%d tickets: billing pressure and one login defect",
				strings.Count(req.Prompt, "- ")), nil
		}
		category := "general"
		switch {
		case strings.Contains(req.Prompt, "refund"),
			strings.Contains(req.Prompt, "charged"):
			category = "billing"
		case strings.Contains(req.Prompt, "crash"),
			strings.Contains(req.Prompt, "password"):
			category = "bug"
		}
		return fmt.Sprintf(`{"category": %q, "urgent": %v}`,
			category, strings.Contains(req.Prompt, "URGENT")), nil
	})

	local := model.NewMock("local/llama-3.1-70b", handler,
		model.WithEndpoint("inference.internal"))
	remote := model.NewMock("vendor/frontier-1", handler,
		model.WithEndpoint("api.frontier.example"))
	price := model.Pricing{InputPerMTok: 3, OutputPerMTok: 15}
	for _, m := range []struct {
		id string
		p  *model.Mock
		t  model.Tier
	}{
		{"local/llama-3.1-70b", local, model.TierFast},
		{"vendor/frontier-1", remote, model.TierDeep},
	} {
		if err := reg.Register(model.Info{
			ID: m.id, Provider: m.p, Tier: m.t, Pricing: price,
		}); err != nil {
			log.Fatal(err)
		}
	}
	return reg, func() int { return local.Calls() + remote.Calls() }
}

// denied reports whether err is a policy refusal rather than a run failure.
func denied(err error) bool {
	var d *policy.Denied
	return errors.As(err, &d)
}

func refusal(err error) string {
	var d *policy.Denied
	errors.As(err, &d)
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(d.Report.String(), "\n"), "\n") {
		fmt.Fprintf(&b, "   %s\n", line)
	}
	return strings.TrimRight(b.String(), "\n")
}

// printAudit shows the two halves a governance trail needs: the decision that
// authorized the run, and every call that carried classified data.
func printAudit(entries []security.AuditEntry) {
	fmt.Printf("   audit:\n")
	shown := map[string]int{}
	for _, e := range entries {
		switch e.Action {
		case "policy.admit":
			fmt.Printf("     %-13s %-22s allowed=%v  %s\n",
				e.Action, e.Subject, e.Allowed, e.Reason)
		case "data.access":
			shown[e.Subject+" "+e.Reason]++
		}
	}
	for k, n := range shown {
		fmt.Printf("     %-13s %s  ×%d\n", "data.access", k, n)
	}
}
