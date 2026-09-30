package main

import (
	"context"
	"fmt"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/studio"
	"github.com/zionrubin/loom/viz"
)

// studioDoc is the reflex pipeline as a studio document: the same guard and
// triage, as "typed questions" cards you can edit on the canvas — add a
// question, move the confidence gate, change the ladder — and watch the price
// move before anything is asked.
func studioDoc(s *setup) *studio.Doc {
	return &studio.Doc{
		Name:      "reflex",
		Note:      "A support inbox on a decision model, with an LLM above it for what it is unsure of.",
		CapUSD:    1,
		KeepGoing: true,
		Steps: []studio.Step{
			{ID: "inbox", Kind: studio.KindSource, Title: "Inbox",
				Source: &studio.SourceSpec{From: "records", Records: inbox()}},
			{ID: "guard", Kind: studio.KindDecide, Title: "Guard", From: "inbox",
				Note: "Screens every message before an agent acts on it",
				Decide: &studio.DecideSpec{
					Model:     typesafeJev(),
					State:     "From: {{.from}}\nSubject: {{.subject}}\n\n{{.body}}",
					Questions: studioQuestions(guardQuestions),
					Output:    "guard",
				}},
			{ID: "no-injection", Kind: studio.KindFilter, Title: "No injection", From: "guard",
				Keep: &studio.Cond{Field: "injection", Op: "lt", Value: "0.5"}},
			{ID: "no-data-grab", Kind: studio.KindFilter, Title: "No data grab", From: "no-injection",
				Keep: &studio.Cond{Field: "data_request", Op: "lt", Value: "0.5"}},
			{ID: "triage", Kind: studio.KindDecide, Title: "Triage", From: "no-data-grab",
				Note: "Five questions, one call; below the gate the LLM answers",
				Decide: &studio.DecideSpec{
					Model:     typesafeJev(),
					Escalate:  []string{s.fallback},
					Gate:      gate,
					System:    "You are a senior support lead triaging the inbox.",
					State:     policy + "\nCustomer plan: {{.plan}}\nSubject: {{.subject}}\n\n{{.body}}",
					Questions: studioQuestions(triageQuestions),
				}},
			{ID: "urgent-only", Kind: studio.KindFilter, Title: "Urgent only", From: "triage",
				Keep: &studio.Cond{Field: "urgent", Op: "gte", Value: "0.7"}},
			{ID: "line", Kind: studio.KindDerive, Title: "One line each", From: "urgent-only",
				Field: &studio.FieldSpec{Name: "line", Template: "[{{.queue}}, {{.plan}} plan] {{.subject}} | {{.body}}"}},
			{ID: "shift-brief", Kind: studio.KindReduce, Title: "Shift brief", From: "line",
				Reduce: &studio.ReduceSpec{
					Model:     s.brief,
					System:    "You brief the on-call support lead. Be concrete and short.",
					Prompt:    "{{.Count}} messages need a human within the hour:\n{{range .Items}}- {{.}}\n{{end}}\nWrite a three-sentence brief: what is happening, who is affected, what to do first.",
					ItemField: "line",
					MaxTokens: 300,
				}},
		},
	}
}

func typesafeJev() string { return studio.DefaultDecisionModel }

// studioQuestions converts declared questions to the document's form.
func studioQuestions(qs model.Questions) []studio.Question {
	out := make([]studio.Question, 0, len(qs))
	for _, id := range qs.IDs() {
		q := qs[id]
		sq := studio.Question{ID: id, Type: string(q.Type), Ask: q.Instructions, Levels: q.Levels}
		for _, name := range q.OptionNames() {
			sq.Options = append(sq.Options, studio.Choice{Name: name, Note: q.Options[name]})
		}
		out = append(out, sq)
	}
	return out
}

// openStudio serves the studio on addr and the constellation view beside it,
// with a Run button that runs the canvas against this example's models.
func openStudio(ctx context.Context, s *setup, addr string) error {
	v := viz.New()
	vizURL, err := v.Start("127.0.0.1:0")
	if err != nil {
		return err
	}
	defer v.Close()

	srv := studio.New(studioDoc(s),
		studio.Models(s.reg),
		studio.Constellation(vizURL),
		studio.Runner(func(ctx context.Context, r studio.RunRequest) error {
			res, err := loom.Run(ctx, r.Pipeline, append(s.opts(),
				loom.WithRunBudget(core.Budget{MaxCostUSD: r.Doc.CapUSD}),
				loom.WithContinueOnError(),
				loom.WithEventHandler(func(e observe.Event) { v.Handle(e) }))...)
			if err != nil && res == nil {
				return err
			}
			fmt.Print(res.Report)
			return err
		}))
	url, err := srv.Start(addr)
	if err != nil {
		return err
	}
	defer srv.Close()
	fmt.Printf("loom studio:        %s\nconstellation view: %s\n\n%s\n", url, vizURL, srv.Estimate())
	fmt.Println("\nthe two TYPED QUESTIONS cards are decisions: open one to edit its questions or move its gate. Ctrl-C to exit.")
	<-ctx.Done()
	return nil
}
