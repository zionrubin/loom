package studio

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

// DefaultDecisionModel is the model a decide step binds to when it names
// none: TypeSafe's Jev, registered by providers/typesafe.
const DefaultDecisionModel = "jev-latest"

// DecideSpec declares typed questions about each record (pipeline.Decide).
//
// It is the studio's cheapest model step and its most declarative one. An
// infer step asks for text and declares the fields it hopes the text will
// hold; a decide step declares the questions themselves — yes/no, pick one,
// rate on a rubric — and every answer arrives as a value of that type, with a
// probability and a confidence, from a decision model that bills input only.
// There is nothing to parse, so there is no answer shape to keep in agreement
// with a prompt.
type DecideSpec struct {
	// Model is the decision model (default DefaultDecisionModel).
	Model string `json:"model,omitempty"`

	// Escalate is the ladder climbed when the weakest answer's confidence is
	// below Gate: typically one LLM, which answers the same questions under
	// the same schema.
	Escalate []string `json:"escalate,omitempty"`

	// Gate is the least confidence accepted from the decision model (0 = accept
	// everything).
	Gate float64 `json:"gate,omitempty"`

	// System instructs an escalated LLM. The decision model has no system
	// prompt.
	System string `json:"system,omitempty"`

	// State is a template over the record's fields: what the questions are
	// asked about. Empty sends the whole record.
	State string `json:"state,omitempty"`

	// Questions are asked together, in one call per record.
	Questions []Question `json:"questions"`

	// Output is the field the full decision lands in (default "decision").
	// Each question's answer also lands in a field named for its ID.
	Output string `json:"output,omitempty"`

	// Workers bounds this step's concurrency (pipeline.WithParallelism).
	Workers int `json:"workers,omitempty"`
}

// Question is one typed question on a decide step.
type Question struct {
	// ID is the record field the answer lands in.
	ID string `json:"id"`
	// Type is "noul" (yes/no, answered as a probability), "choice" (one of
	// Options) or "score" (a point on Levels).
	Type string `json:"type"`
	// Ask is the question.
	Ask string `json:"ask"`
	// Options are a choice's answers, each with the note it is judged by.
	Options []Choice `json:"options,omitempty"`
	// Levels are a score's rubric, lowest first.
	Levels []string `json:"levels,omitempty"`
}

// Choice is one option of a choice question.
type Choice struct {
	Name string `json:"name"`
	Note string `json:"note,omitempty"`
}

func (d *DecideSpec) model() string { return field(d.Model, DefaultDecisionModel) }

func (d *DecideSpec) questions() model.Questions {
	qs := make(model.Questions, len(d.Questions))
	for _, q := range d.Questions {
		mq := model.Question{Type: model.QuestionType(q.Type), Instructions: q.Ask}
		switch mq.Type {
		case model.QuestionChoice:
			mq.Options = make(map[string]string, len(q.Options))
			for _, o := range q.Options {
				mq.Options[o.Name] = o.Note
			}
		case model.QuestionScore:
			mq.Levels = append([]string(nil), q.Levels...)
		}
		qs[q.ID] = mq
	}
	return qs
}

func (d *DecideSpec) validate(id string) error {
	if len(d.Questions) == 0 {
		return fmt.Errorf("studio: step %q: a decide step needs at least one question", id)
	}
	seen := map[string]bool{}
	for _, q := range d.Questions {
		if seen[q.ID] {
			return fmt.Errorf("studio: step %q: two questions answer into %q", id, q.ID)
		}
		seen[q.ID] = true
	}
	if d.Gate < 0 || d.Gate >= 1 {
		return fmt.Errorf("studio: step %q: the confidence gate must be in [0, 1)", id)
	}
	if err := d.questions().Validate(); err != nil {
		return fmt.Errorf("studio: step %q: %w", id, err)
	}
	return nil
}

func (d *DecideSpec) compile(id string) (pipeline.DecideSpec, error) {
	if err := d.validate(id); err != nil {
		return pipeline.DecideSpec{}, err
	}
	return pipeline.DecideSpec{
		Binding:       model.Binding{Model: d.model(), Escalation: d.Escalate},
		System:        d.System,
		State:         d.State,
		Questions:     d.questions(),
		MinConfidence: d.Gate,
		OutputField:   d.Output,
	}, nil
}

// sample is what the pricer assumes each record is answered. The fields are
// declared, so only the values are assumed, and the studio can do better than
// a blanket guess because its filters are declarations too: for each question
// it picks the answer that the most filters below this step keep — so a guard
// is priced as letting everything through and an urgency filter as keeping
// everything, the answers that send the most records downstream. A question no
// filter reads gets its first candidate. The assumptions are stated in a
// warning beside the price.
func (d *DecideSpec) sample(doc *Doc, id string) map[string]any {
	var conds []Cond
	seen := map[string]bool{}
	var walk func(string)
	walk = func(from string) {
		for _, c := range doc.Children(from) {
			if seen[c.ID] {
				continue
			}
			seen[c.ID] = true
			if c.Kind == KindFilter && c.Keep != nil {
				conds = append(conds, *c.Keep)
			}
			walk(c.ID)
		}
	}
	walk(id)

	out := make(map[string]any, len(d.Questions))
	for _, q := range d.Questions {
		var cands []any
		switch model.QuestionType(q.Type) {
		case model.QuestionNoul:
			cands = []any{1.0, 0.0}
		case model.QuestionChoice:
			for _, o := range q.Options {
				cands = append(cands, o.Name)
			}
		case model.QuestionScore:
			for i := len(q.Levels) - 1; i >= 0; i-- {
				cands = append(cands, float64(i))
			}
		}
		best, bestKept := any(nil), -1
		for _, v := range cands {
			rec := core.NewRecord("sample", map[string]any{q.ID: v})
			kept := 0
			for _, c := range conds {
				if c.Field != q.ID {
					continue
				}
				if ok, _ := c.Match(rec); ok {
					kept++
				}
			}
			if kept > bestKept {
				best, bestKept = v, kept
			}
		}
		if best != nil {
			out[q.ID] = best
		}
	}
	return out
}

func (d *DecideSpec) sampleText(doc *Doc, id string) string {
	s := d.sample(doc, id)
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%v", k, s[k])
	}
	return strings.Join(parts, ", ")
}

// decide emits a decide step as Go.
func (g *gen) decide(s *Step, up string, assigned bool) error {
	d := s.Decide
	if _, err := d.compile(s.ID); err != nil {
		return err
	}
	g.use("github.com/zionrubin/loom/model")
	g.p("%s%s.Decide(%s, pipeline.DecideSpec{", g.lhs(s, assigned), up, lit(s.ID))
	g.p("\t\tBinding: %s,", bindingLit("", d.model(), d.Escalate))
	if d.Gate > 0 {
		g.p("\t\tMinConfidence: %s,", num(d.Gate))
	}
	if d.System != "" {
		g.p("\t\tSystem: %s,", lit(d.System))
	}
	if d.State != "" {
		g.p("\t\tState: %s,", lit(d.State))
	}
	g.p("\t\tQuestions: model.Questions{")
	for _, q := range d.Questions {
		switch model.QuestionType(q.Type) {
		case model.QuestionNoul:
			g.p("\t\t\t%s: model.Noul(%s),", lit(q.ID), lit(q.Ask))
		case model.QuestionChoice:
			g.p("\t\t\t%s: model.Choice(%s, map[string]string{", lit(q.ID), lit(q.Ask))
			for _, o := range q.Options {
				g.p("\t\t\t\t%s: %s,", lit(o.Name), lit(o.Note))
			}
			g.p("\t\t\t}),")
		case model.QuestionScore:
			g.p("\t\t\t%s: model.Score(%s, %s),", lit(q.ID), lit(q.Ask),
				strings.TrimSuffix(strings.TrimPrefix(strSlice(q.Levels), "[]string{"), "}"))
		}
	}
	g.p("\t\t},")
	if d.Output != "" {
		g.p("\t\tOutputField: %s,", lit(d.Output))
	}
	if d.Workers > 0 {
		g.p("\t}, pipeline.WithParallelism(%d))", d.Workers)
		return nil
	}
	g.p("\t})")
	return nil
}
