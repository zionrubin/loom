package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// A decision is a model call whose answer type is declared before it is made.
//
// Most of what a pipeline asks a model is not "write something". It is "which
// queue", "how angry", "is this an injection attempt" — closed questions whose
// answers are consumed by code, not read by a person. A generative model
// answers them by writing text that is then parsed back into the type the
// caller wanted all along, and every failure mode of that round trip is a
// failure the caller has to handle: the JSON that does not parse, the category
// that is not in the list, the confidence that was typed rather than measured.
//
// A decision model (a "System One" model — TypeSafe's Jev is the first) skips
// the round trip. It is handed the state and the typed questions, and it
// returns one answer per question drawn from the declared type, each with a
// probability distribution and a calibrated confidence. There is no text to
// parse and no way to answer outside the schema.
//
// Loom treats that as a capability of a provider rather than as a new kind of
// pipeline: a Request that carries Questions is a decision request, a provider
// that declares Capabilities.Decisions answers it natively, and a provider
// that does not — any LLM — answers the same questions through a rendered
// prompt whose reply is held to the same schema (see Generative and
// ParseDecision). That is what lets one escalation ladder run from a 100 ms
// decision model up to a frontier LLM without the stage noticing which one
// answered.

// QuestionType is the answer type of one typed question.
type QuestionType string

const (
	// QuestionNoul is a yes/no question answered with the probability that
	// the answer is yes.
	QuestionNoul QuestionType = "noul"
	// QuestionChoice picks exactly one of a declared set of options, with a
	// probability for every option.
	QuestionChoice QuestionType = "choice"
	// QuestionScore rates against an ordered rubric of levels, answered with a
	// probability-weighted position on that scale.
	QuestionScore QuestionType = "score"
)

// MaxScoreLevels is the most levels one Score rubric may declare. It is the
// decision-model API's own limit; enforcing it at compile time turns what
// would be an HTTP 400 on the first record into an error on the declaration.
const MaxScoreLevels = 10

// Question is one typed question. Build one with Noul, Choice or Score.
type Question struct {
	Type QuestionType `json:"type"`
	// Instructions is the question itself, in plain language.
	Instructions string `json:"instructions"`
	// Options are a Choice's answers, each with the description the model
	// judges it by. The description is part of the question: "billing" means
	// less to a model than "billing — charges, refunds, invoices".
	Options map[string]string `json:"options,omitempty"`
	// Levels are a Score's rubric, lowest first. A score of 0 is Levels[0].
	Levels []string `json:"levels,omitempty"`
}

// Noul declares a yes/no question.
func Noul(instructions string) Question {
	return Question{Type: QuestionNoul, Instructions: instructions}
}

// Choice declares a pick-one question over options (name → description).
func Choice(instructions string, options map[string]string) Question {
	return Question{Type: QuestionChoice, Instructions: instructions, Options: options}
}

// Score declares a rubric question over ordered levels, lowest first.
func Score(instructions string, levels ...string) Question {
	return Question{Type: QuestionScore, Instructions: instructions, Levels: levels}
}

// OptionNames returns a Choice's option names, sorted, so everything that
// renders them — a prompt, a fingerprint, a UI — renders them identically.
func (q Question) OptionNames() []string {
	names := make([]string, 0, len(q.Options))
	for n := range q.Options {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Questions is a set of typed questions keyed by ID. An ID becomes a record
// field when the stage writes its answers, so it must be an identifier.
type Questions map[string]Question

// IDs returns the question IDs, sorted.
func (qs Questions) IDs() []string {
	ids := make([]string, 0, len(qs))
	for id := range qs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

var questionID = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Validate checks every question is well formed: a usable ID, instructions,
// at least two options for a Choice, and two to MaxScoreLevels levels for a
// Score.
func (qs Questions) Validate() error {
	if len(qs) == 0 {
		return errors.New("no questions declared")
	}
	for _, id := range qs.IDs() {
		q := qs[id]
		if !questionID.MatchString(id) {
			return fmt.Errorf("question %q: ID must be an identifier (it becomes a record field)", id)
		}
		if strings.TrimSpace(q.Instructions) == "" {
			return fmt.Errorf("question %q: empty instructions", id)
		}
		switch q.Type {
		case QuestionNoul:
		case QuestionChoice:
			if len(q.Options) < 2 {
				return fmt.Errorf("question %q: a choice needs at least two options", id)
			}
			for name := range q.Options {
				if strings.TrimSpace(name) == "" {
					return fmt.Errorf("question %q: empty option name", id)
				}
			}
		case QuestionScore:
			if len(q.Levels) < 2 || len(q.Levels) > MaxScoreLevels {
				return fmt.Errorf("question %q: a score needs 2 to %d levels, has %d",
					id, MaxScoreLevels, len(q.Levels))
			}
		default:
			return fmt.Errorf("question %q: unknown type %q (want noul, choice or score)", id, q.Type)
		}
	}
	return nil
}

// Answer is one typed answer. Which fields are set follows Type:
//
//	noul    Noul — the probability of yes. Its certainty is how far that sits
//	        from a coin flip; there is no separate confidence.
//	choice  Choice, Probabilities over every option, Confidence.
//	score   Score (a probability-weighted position, 0 = Levels[0]), Legend,
//	        Probabilities keyed by level index, Confidence.
type Answer struct {
	Type          QuestionType       `json:"type"`
	Noul          float64            `json:"noul,omitempty"`
	Choice        string             `json:"choice,omitempty"`
	Score         float64            `json:"score,omitempty"`
	Legend        map[string]string  `json:"legend,omitempty"`
	Probabilities map[string]float64 `json:"probabilities,omitempty"`
	Confidence    float64            `json:"confidence,omitempty"`
}

// Value is the answer as a record field holds it: the chosen option for a
// Choice, the probability of yes for a Noul, the weighted score for a Score.
func (a Answer) Value() any {
	switch a.Type {
	case QuestionChoice:
		return a.Choice
	case QuestionScore:
		return a.Score
	default:
		return a.Noul
	}
}

// Certainty is how sure the answer is, from 0 (no idea) to 1 (certain), on
// one scale for all three types, so a set of answers can be gated on its
// weakest member. For a Choice or a Score it is the model's confidence. A
// Noul carries none, because its value already is a probability; its
// certainty is its distance from 0.5, stretched to fill 0..1 — so 0.5 is 0,
// and both 0.05 and 0.95 are 0.9.
func (a Answer) Certainty() float64 {
	if a.Type == QuestionNoul {
		return math.Abs(2*a.Noul - 1)
	}
	return a.Confidence
}

// Level is a Score answer's most probable level index, or the rounded score
// when no distribution was returned.
func (a Answer) Level() int {
	best, bestP := -1, -1.0
	for k, p := range a.Probabilities {
		i, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		if p > bestP || (p == bestP && i < best) {
			best, bestP = i, p
		}
	}
	if best >= 0 {
		return best
	}
	return int(math.Round(a.Score))
}

// Decision is a full set of answers to one request.
type Decision struct {
	// Model is the model that answered.
	Model string `json:"model"`
	// Calibrated reports whether the confidences were measured by a decision
	// model or stated by a generative one. Only a calibrated confidence is a
	// probability; a stated one is the model's opinion of itself.
	Calibrated bool              `json:"calibrated"`
	Answers    map[string]Answer `json:"answers"`
}

// Confidence is the certainty of the weakest answer — the honest confidence
// of a set of judgments, since acting on all of them is only as safe as the
// least sure one.
func (d Decision) Confidence() float64 {
	c, _ := d.weakest()
	return c
}

// Weakest returns the ID of the least certain answer.
func (d Decision) Weakest() string {
	_, id := d.weakest()
	return id
}

func (d Decision) weakest() (float64, string) {
	if len(d.Answers) == 0 {
		return 0, ""
	}
	ids := make([]string, 0, len(d.Answers))
	for id := range d.Answers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	conf, weak := 2.0, ""
	for _, id := range ids {
		if c := d.Answers[id].Certainty(); c < conf {
			conf, weak = c, id
		}
	}
	return conf, weak
}

// Check holds a set of answers to the schema that asked for them: one answer
// per question, of the declared type, drawn from the declared options or
// levels, with probabilities in range. A decision model guarantees this by
// construction; checking anyway costs nothing and means a provider bug
// surfaces as a semantic failure — which escalates — rather than as a
// record carrying a category nobody declared.
func (qs Questions) Check(answers map[string]Answer) error {
	for _, id := range qs.IDs() {
		q := qs[id]
		a, ok := answers[id]
		if !ok {
			return fmt.Errorf("question %q: no answer", id)
		}
		if a.Type != q.Type {
			return fmt.Errorf("question %q: answered as %q, declared %q", id, a.Type, q.Type)
		}
		switch q.Type {
		case QuestionNoul:
			if a.Noul < 0 || a.Noul > 1 {
				return fmt.Errorf("question %q: probability %v outside [0,1]", id, a.Noul)
			}
		case QuestionChoice:
			if _, ok := q.Options[a.Choice]; !ok {
				return fmt.Errorf("question %q: %q is not one of %s", id, a.Choice,
					strings.Join(q.OptionNames(), ", "))
			}
		case QuestionScore:
			if a.Score < 0 || a.Score > float64(len(q.Levels)-1) {
				return fmt.Errorf("question %q: score %v outside [0,%d]", id, a.Score, len(q.Levels)-1)
			}
		}
		if q.Type != QuestionNoul && (a.Confidence < 0 || a.Confidence > 1) {
			return fmt.Errorf("question %q: confidence %v outside [0,1]", id, a.Confidence)
		}
	}
	return nil
}

// Capabilities are what a provider can be asked to do.
type Capabilities struct {
	// Text: completes free-form prompts. Every LLM does.
	Text bool
	// Decisions: answers Request.Questions natively, with calibrated
	// probabilities, and returns them in Response.Answers.
	Decisions bool
}

// Capable is implemented by providers whose capabilities differ from the
// default of text only.
type Capable interface {
	Capabilities() Capabilities
}

// CapabilitiesOf returns what a provider can do. A provider that does not say
// is a text model, which is what every provider was before decisions existed.
func CapabilitiesOf(p Provider) Capabilities {
	if c, ok := p.(Capable); ok {
		return c.Capabilities()
	}
	return Capabilities{Text: true}
}

// Generative rewrites a decision request into one a text model can answer:
// the questions are rendered into the prompt, with an instruction to reply in
// a JSON shape ParseDecision holds to the same schema. The state stays exactly
// where it was, so the rendered request still shares its prefix with every
// other record's.
func Generative(req Request) Request {
	qs := req.Questions
	req.Questions = nil
	sys := "You answer typed questions about the material you are given. " +
		"Answer every question, choose only from the options or levels offered, " +
		"and reply with one JSON object and nothing else."
	if req.System != "" {
		sys = req.System + "\n\n" + sys
	}
	req.System = sys
	req.Prompt = "<state>\n" + req.Prompt + "\n</state>\n\n" + RenderQuestions(qs)
	return req
}

// RenderQuestions renders a question set as prompt text, ending in the JSON
// shape the reply must take.
func RenderQuestions(qs Questions) string {
	var b strings.Builder
	b.WriteString("Questions:\n")
	shape := make([]string, 0, len(qs))
	for _, id := range qs.IDs() {
		q := qs[id]
		fmt.Fprintf(&b, "\n%s (%s): %s\n", id, q.Type, q.Instructions)
		switch q.Type {
		case QuestionNoul:
			b.WriteString("  answer with the probability, 0 to 1, that the answer is yes\n")
			shape = append(shape, fmt.Sprintf(`%q: {"noul": 0.0}`, id))
		case QuestionChoice:
			for _, name := range q.OptionNames() {
				if d := q.Options[name]; d != "" {
					fmt.Fprintf(&b, "  - %s: %s\n", name, d)
				} else {
					fmt.Fprintf(&b, "  - %s\n", name)
				}
			}
			shape = append(shape, fmt.Sprintf(`%q: {"choice": "<option>", "confidence": 0.0}`, id))
		case QuestionScore:
			for i, lvl := range q.Levels {
				fmt.Fprintf(&b, "  %d = %s\n", i, lvl)
			}
			shape = append(shape, fmt.Sprintf(`%q: {"score": 0, "confidence": 0.0}`, id))
		}
	}
	b.WriteString("\nReply with exactly this JSON shape (confidence is 0 to 1):\n{")
	b.WriteString(strings.Join(shape, ", "))
	b.WriteString("}\n")
	return b.String()
}

// ParseDecision reads a text model's reply to a Generative request and holds
// it to the schema. Anything the schema does not allow — a missing answer, an
// option nobody declared, a score off the rubric — is an error, which a stage
// reports as a semantic failure so the record escalates rather than carrying
// the invention downstream.
func ParseDecision(text string, qs Questions) (map[string]Answer, error) {
	obj, err := extractObject(text)
	if err != nil {
		return nil, err
	}
	var raw map[string]struct {
		Noul       *float64 `json:"noul"`
		Choice     string   `json:"choice"`
		Score      *float64 `json:"score"`
		Confidence *float64 `json:"confidence"`
	}
	if err := json.Unmarshal([]byte(obj), &raw); err != nil {
		return nil, fmt.Errorf("decision reply: %w", err)
	}
	out := make(map[string]Answer, len(qs))
	for _, id := range qs.IDs() {
		q := qs[id]
		r, ok := raw[id]
		if !ok {
			return nil, fmt.Errorf("question %q: no answer", id)
		}
		a := Answer{Type: q.Type}
		if r.Confidence != nil {
			a.Confidence = *r.Confidence
		}
		switch q.Type {
		case QuestionNoul:
			if r.Noul == nil {
				return nil, fmt.Errorf("question %q: no probability", id)
			}
			a.Noul = *r.Noul
			a.Confidence = 0
		case QuestionChoice:
			a.Choice = strings.TrimSpace(r.Choice)
		case QuestionScore:
			if r.Score == nil {
				return nil, fmt.Errorf("question %q: no score", id)
			}
			a.Score = *r.Score
			a.Legend = make(map[string]string, len(q.Levels))
			for i, lvl := range q.Levels {
				a.Legend[strconv.Itoa(i)] = lvl
			}
		}
		out[id] = a
	}
	if err := qs.Check(out); err != nil {
		return nil, err
	}
	return out, nil
}

// extractObject returns the outermost JSON object in text, tolerating prose
// and code fences around it.
func extractObject(text string) (string, error) {
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start < 0 || end <= start {
		return "", errors.New("decision reply: no JSON object found")
	}
	return text[start : end+1], nil
}

// ReadDecision recovers a Decision from a record field written by a decision
// stage. The field is a plain map by the time anything reads it — records are
// JSON on the way through the cache and across executors — so this is the
// one place that turns it back into a type.
func ReadDecision(v any) (Decision, bool) {
	if v == nil {
		return Decision{}, false
	}
	b, err := json.Marshal(v)
	if err != nil {
		return Decision{}, false
	}
	var d Decision
	if err := json.Unmarshal(b, &d); err != nil || len(d.Answers) == 0 {
		return Decision{}, false
	}
	return d, true
}
