package studio

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Assistant answers a question about a document, and may propose changes to
// it. It is what ⌘K talks to.
//
// The contract is deliberately narrow: an assistant returns a [Proposal],
// never a modified document. Nothing on the canvas moves until a human
// accepts the edits, and the canvas draws them as a diff first. An assistant
// that could edit directly would be a second author with no review step, and
// this one costs money when it is wrong.
type Assistant interface {
	// Intents are the rows the palette offers before anything is typed.
	Intents(doc *Doc, est *Estimate) []Intent
	// Ask answers one question.
	Ask(req Request) (*Proposal, error)
}

// Request is everything an assistant is given.
type Request struct {
	Doc      *Doc
	Estimate *Estimate
	// Query is what the human typed, or the ID of an intent they clicked.
	Query string
	// Selected is the step selected on the canvas, if any.
	Selected string
	// Price reprices a candidate document. It issues no model calls and runs
	// nothing — it is the same projection the header shows — so an assistant
	// can and should quote the price of its own proposal rather than guess at
	// it.
	Price func(*Doc) *Estimate
}

// Intent is one row of the ⌘K palette.
type Intent struct {
	ID string `json:"id"`
	// Kind is the label on the left: BUILD, OPTIMIZE, EXPLAIN, RUN, EXPORT.
	Kind  string `json:"kind"`
	Label string `json:"label"`
	Note  string `json:"note"`
}

// Proposal is an answer, and sometimes a change.
type Proposal struct {
	Kind  string `json:"kind"`
	Title string `json:"title"`
	Body  string `json:"body"`

	// Ghost and GhostNote are the dashed card the canvas draws while the
	// proposal is pending; After is the step it hangs off.
	Ghost     string `json:"ghost,omitempty"`
	GhostNote string `json:"ghost_note,omitempty"`
	After     string `json:"after,omitempty"`

	// Edits are what Accept would apply. Empty means the proposal is an
	// answer and nothing more.
	Edits []Edit `json:"edits,omitempty"`

	// Estimate is the document repriced with the edits applied, so the
	// proposal states what it would cost rather than asserting that it is
	// cheaper.
	Estimate *Estimate `json:"estimate,omitempty"`

	// Action asks the UI to do something instead of editing: "export" opens
	// the Go export, "run" starts the run.
	Action string `json:"action,omitempty"`
}

// Insight is the assistant that ships with the studio: it answers from the
// projection.
//
// It is not a language model, and it does not pretend to be one. Everything it
// says is computed — which step dominates the cost and by how much, what a
// change would reprice to, whether a cap still fits, how long the rate limits
// alone will take — and everything it proposes is an edit whose new price it
// has already computed by pricing the edited document. That covers the
// questions people actually ask a cost-shaped builder, and it covers them
// exactly rather than plausibly.
//
// For the questions it cannot answer that way — "are the Hebrew days being
// handled correctly?" — it says so rather than improvising, and a model-backed
// assistant can take its place through [WithAssistant].
type Insight struct{}

// Intents implements Assistant.
func (Insight) Intents(doc *Doc, est *Estimate) []Intent {
	out := []Intent{
		{ID: "cheaper", Kind: "OPTIMIZE", Label: "Make this cheaper without losing the fields", Note: "changes models"},
		{ID: "why", Kind: "EXPLAIN", Label: "Where is the money going?", Note: "answers"},
		{ID: "slow", Kind: "EXPLAIN", Label: "How long can this take, at best?", Note: "rate limits"},
		{ID: "flag", Kind: "BUILD", Label: "Add a step that reads what the last one produced", Note: "adds a step"},
		{ID: "loop", Kind: "BUILD", Label: "Keep improving each record until it is good enough", Note: "adds a loop"},
		{ID: "rerun", Kind: "RUN", Label: "What does a second run cost?", Note: "cache"},
		{ID: "export", Kind: "EXPORT", Label: "Show me this as Go", Note: "opens the code"},
	}
	if est != nil && est.CapUSD > 0 {
		out = append(out, Intent{ID: "cap", Kind: "RUN",
			Label: fmt.Sprintf("Cap this run at $%s and tell me what stops", trimNum(est.CapUSD/2)),
			Note:  "changes a guardrail"})
	}
	return out
}

var moneyRe = regexp.MustCompile(`\$?\s*([0-9]+(?:\.[0-9]+)?)`)

// Ask implements Assistant.
func (in Insight) Ask(req Request) (*Proposal, error) {
	q := strings.ToLower(strings.TrimSpace(req.Query))
	doc, est := req.Doc, req.Estimate
	if doc == nil {
		return nil, fmt.Errorf("studio: no document to answer about")
	}
	switch {
	case q == "export" || has(q, "as go", "export", "source code", "the code"):
		return &Proposal{Kind: "EXPORT", Action: "export",
			Title: "The pipeline as Go",
			Body: "Generated from the canvas, and it compiles: the same operators, the same " +
				"prompts, the same versions. Editing the canvas regenerates it — it is an " +
				"export, not a second source of truth."}, nil

	case q == "cap" || has(q, "cap", "budget", "ceiling", "stop the run at"):
		return in.cap(req, q)

	case q == "cheaper" || has(q, "cheap", "cost less", "save money", "reduce the cost", "lower the price"):
		return in.cheaper(req)

	case q == "why" || has(q, "why", "where is the money", "expensive", "dominat", "share"):
		return in.why(req)

	case q == "slow" || has(q, "how long", "slow", "faster", "rate limit", "throughput"):
		return in.slow(req)

	case q == "rerun" || has(q, "rerun", "again", "cache", "resume", "changed since"):
		return in.rerun(req)

	case q == "loop" || has(q, "loop", "until", "repeat", "refine", "keep going until"):
		return in.loop(req)

	case q == "flag" || has(q, "add a step", "flag", "classify", "extract", "score", "another step"):
		return in.addStep(req)
	}
	return in.fallback(doc, est, req.Query)
}

func has(q string, keys ...string) bool {
	for _, k := range keys {
		if strings.Contains(q, k) {
			return true
		}
	}
	return false
}

// why answers "where is the money going" out of the projection.
func (Insight) why(req Request) (*Proposal, error) {
	est := req.Estimate
	if est == nil || est.Error != "" {
		return &Proposal{Kind: "EXPLAIN", Title: "Nothing is priced yet",
			Body: "The document does not price: " + errText(est)}, nil
	}
	top := est.Priciest()
	if top == nil {
		return &Proposal{Kind: "EXPLAIN", Title: "This pipeline calls no models",
			Body: "Every step is a filter, a derived field or a write, so the run costs nothing " +
				"but the time it takes to read the records."}, nil
	}
	step := req.Doc.Find(top.ID)
	var b strings.Builder
	fmt.Fprintf(&b, "%s is %s of the expected spend: %s calls at %s, averaging %s input tokens each. ",
		name(step, top.ID), pct(top.Share), thousands(top.Calls), money(top.ExpectedUSD),
		thousands(perCall(top.InputTokens, top.Calls)))
	switch top.Kind {
	case KindInfer:
		fmt.Fprintf(&b, "It runs once per record, against %s records, which is what makes it the "+
			"expensive one — a fold reads a summary per record instead of a record. ", thousands(top.Records))
	case KindReduce:
		fmt.Fprintf(&b, "It folds %s records through a tree, so the calls are fewer but each one "+
			"carries a batch. ", thousands(top.Records))
	case KindLoop:
		b.WriteString("It is a loop, so this is its cap rather than its cost: rounds stop early " +
			"as records go quiet. ")
	}
	rest := est.Paid()
	if len(rest) > 1 {
		var others []string
		for _, s := range rest[1:] {
			others = append(others, fmt.Sprintf("%s %s", s.ID, money(s.ExpectedUSD)))
		}
		fmt.Fprintf(&b, "The rest: %s.", strings.Join(others, ", "))
	}
	if top.CachePrefix {
		b.WriteString(" Its shared prompt head is served from the provider's prefix cache, which is " +
			"already in the number above.")
	}
	return &Proposal{Kind: "EXPLAIN", Title: fmt.Sprintf("%s is %s of the run",
		name(step, top.ID), pct(top.Share)), Body: b.String()}, nil
}

// cheaper proposes the three changes that reliably move the number, and
// prices the result before offering it.
func (in Insight) cheaper(req Request) (*Proposal, error) {
	doc, est := req.Doc, req.Estimate
	if est == nil || est.Error != "" {
		return &Proposal{Kind: "OPTIMIZE", Title: "Nothing to make cheaper yet",
			Body: "The document does not price: " + errText(est)}, nil
	}
	var edits []Edit
	var lines []string
	for _, row := range est.Paid() {
		s := doc.Find(row.ID)
		if s == nil {
			continue
		}
		switch s.Kind {
		case KindInfer:
			next, notes := cheaperInfer(s.Infer, row.ID)
			if notes == nil {
				continue
			}
			lines = append(lines, notes...)
			edits = append(edits, Edit{Op: UpdateStep, ID: s.ID, Step: &Step{Infer: next},
				Note: s.ID + ": " + strings.Join(notes, ", ")})
		case KindReduce:
			fanIn := orInt(s.Reduce.FanIn, 8)
			next := *s.Reduce
			next.FanIn = fanIn + fanIn/2 + 2
			edits = append(edits, Edit{Op: UpdateStep, ID: s.ID, Step: &Step{Reduce: &next},
				Note: fmt.Sprintf("%s: folds %d at a time instead of %d", s.ID, next.FanIn, fanIn)})
			lines = append(lines, fmt.Sprintf("%s folds %d records per call instead of %d",
				row.ID, next.FanIn, fanIn))
		}
	}
	if len(edits) == 0 {
		return &Proposal{Kind: "OPTIMIZE", Title: "Already at the cheap end",
			Body: "Every paid step is on its cheapest model with a tight answer cap, and the folds " +
				"are already batching. The next lever is the input: fewer records, or less of each one."}, nil
	}
	after, err := doc.Apply(edits)
	if err != nil {
		return nil, err
	}
	next := req.Price(after)
	title := fmt.Sprintf("%d changes, %s becomes %s", len(edits), money(est.ExpectedUSD), money(next.ExpectedUSD))
	body := "Larger batches cost less and blur more; a shorter answer is terser, not emptier — " +
		"every field and every validation gate is unchanged. The changes are listed below, and " +
		"the price beside them is this document repriced with them applied, not an estimate of a saving."
	if len(lines) <= 2 {
		body = strings.Join(lines, "; ") + ". " + body
	}
	if next.ExpectedUSD >= est.ExpectedUSD {
		title = "These changes do not save anything here"
		body = "Repriced: " + money(next.ExpectedUSD) + " against " + money(est.ExpectedUSD) +
			". The cost is in the number of records, not in the shape of the calls."
	}
	return &Proposal{Kind: "OPTIMIZE", Title: title, Body: body, Edits: edits, Estimate: next,
		Ghost: "Cheaper shape", GhostNote: fmt.Sprintf("%d steps repriced", len(edits))}, nil
}

// cheaperInfer returns the cheaper form of one model step, and the lines
// describing what changed. A nil note list means there was nothing to change,
// which is a real answer and not a failure.
func cheaperInfer(in *InferSpec, id string) (*InferSpec, []string) {
	next := *in
	var notes []string
	if in.Tier != "cheapest" && in.Model == "" {
		next.Tier = "cheapest"
		notes = append(notes, fmt.Sprintf("%s moves to the cheapest tier, escalating only when "+
			"the answer fails validation", id))
	}
	if in.MaxTokens > 200 {
		next.MaxTokens = in.MaxTokens * 3 / 4
		notes = append(notes, fmt.Sprintf("%s answers in %d tokens instead of %d",
			id, next.MaxTokens, in.MaxTokens))
	}
	if len(notes) == 0 {
		return nil, nil
	}
	return &next, notes
}

// cap moves the guardrail and says what that means for this document.
func (in Insight) cap(req Request, q string) (*Proposal, error) {
	doc, est := req.Doc, req.Estimate
	want := doc.CapUSD
	if m := moneyRe.FindStringSubmatch(q); m != nil {
		if v, err := strconv.ParseFloat(m[1], 64); err == nil {
			want = v
		}
	} else if est != nil {
		want = round2(est.CeilingUSD * 1.1)
	}
	if want <= 0 {
		return &Proposal{Kind: "RUN", Title: "Name a cap",
			Body: "Say a number — \"cap this run at $8\" — and the studio will tell you what the " +
				"run does when it reaches it."}, nil
	}
	edits := []Edit{{Op: SetCap, Value: want, Note: fmt.Sprintf("cap $%s", trimNum(want))}}
	after, err := doc.Apply(edits)
	if err != nil {
		return nil, err
	}
	next := req.Price(after)
	body := fmt.Sprintf("The ceiling is %s, so a %s cap covers the whole run with room to spare.",
		money(next.CeilingUSD), money(want))
	if !next.FitsCap {
		body = fmt.Sprintf("At %s the projection no longer fits: the ceiling is %s, so the run would "+
			"stop partway. Loom halts on the budget rather than spending past it, and everything "+
			"finished stays cached — a later run with a higher cap resumes instead of restarting.",
			money(want), money(next.CeilingUSD))
	}
	return &Proposal{Kind: "RUN", Title: fmt.Sprintf("Cap moves to %s", money(want)),
		Body: body, Edits: edits, Estimate: next}, nil
}

// slow answers with the admission floor, which is the one duration a
// projection can state without running anything.
func (Insight) slow(req Request) (*Proposal, error) {
	est := req.Estimate
	if est == nil || est.Error != "" {
		return &Proposal{Kind: "EXPLAIN", Title: "Nothing is priced yet",
			Body: "The document does not price: " + errText(est)}, nil
	}
	floor := est.Floor()
	if floor <= 0 {
		return &Proposal{Kind: "EXPLAIN", Title: "No rate limit binds this run",
			Body: "None of the models this pipeline uses declare requests-per-minute or " +
				"tokens-per-minute limits, so nothing here sets a floor on the wall clock. " +
				"Workers and provider latency decide it."}, nil
	}
	return &Proposal{Kind: "EXPLAIN",
		Title: "At best " + floor.Round(1e9).String(),
		Body: fmt.Sprintf("%s model calls have to be admitted under the models' rate limits, and "+
			"the scheduler will not admit them faster however many workers are free. That is a "+
			"floor, not an estimate: provider latency, retries and escalation all add to it.",
			thousands(est.Calls))}, nil
}

// rerun explains what the content-addressed cache does to a second run.
func (Insight) rerun(req Request) (*Proposal, error) {
	est := req.Estimate
	return &Proposal{Kind: "RUN", Title: "A second run pays only for what changed",
		Body: fmt.Sprintf("Results are keyed by what went into them — the record, the prompt, the "+
			"model, the version of every Go step above it — so a rerun with unchanged inputs "+
			"replays from the state directory at no cost. What it costs is decided by what you "+
			"edit: change a prompt and that step's %s calls are paid again, change a derived "+
			"field's template and the steps below it are too. Nothing else moves.",
			thousands(paidCalls(est)))}, nil
}

// loop proposes a refine loop over the last model step's output.
func (in Insight) loop(req Request) (*Proposal, error) {
	doc := req.Doc
	host := lastOfKind(doc, KindInfer)
	if host == nil {
		return &Proposal{Kind: "BUILD", Title: "There is nothing to refine yet",
			Body: "A loop improves what a model already produced. Add a model step first, then " +
				"this proposal has something to work on."}, nil
	}
	fld := "draft"
	if len(host.Infer.Answer) > 0 {
		fld = host.Infer.Answer[0].Name
	}
	id := doc.UniqueID("refine-" + fld)
	step := Step{
		ID: id, Kind: KindLoop, From: host.ID,
		Title: "Refine until good", Note: "Repeats until the critique passes",
		Loop: &LoopSpec{
			Step: InferSpec{
				Tier: "balanced",
				System: "You improve a draft and judge it. Return the improved draft, a verdict, " +
					"and what still needs work.",
				Prompt: fmt.Sprintf("Here is the current %s:\n\n{{.%s}}\n"+
					"{{if .Inbox}}\nWhat the last round said to fix:\n{{range .Inbox}}- {{.}}\n{{end}}{{end}}",
					fld, fld),
				MaxTokens: 800,
				Answer: []Answer{
					{Name: fld, Note: "the improved version", Required: true},
					{Name: "verdict", Note: "good, or needs-work"},
					{Name: "critique", Note: "what still needs work; empty when good"},
				},
			},
			Until:  Cond{Field: "verdict", Op: "is", Value: "good"},
			Note:   "critique",
			Rounds: 4,
			CapUSD: round2(maxFloat(1, spend(req.Estimate)*0.25)),
		},
	}
	edits := []Edit{{Op: AddStep, Step: &step, Note: "add loop " + id}}
	after, err := doc.Apply(edits)
	if err != nil {
		return nil, err
	}
	next := req.Price(after)
	return &Proposal{Kind: "BUILD",
		Title: "Add a loop: refine until the critique passes",
		Body: fmt.Sprintf("Each round rewrites the %s and says whether it is good; a record that "+
			"says good goes quiet and stops costing anything. Capped at 4 rounds and %s, because "+
			"a loop cannot be priced exactly — only its cap can, and the projection above prices "+
			"every round as if none of them converged.", fld, money(step.Loop.CapUSD)),
		Ghost: step.Title, GhostNote: "Repeats until nothing new", After: host.ID,
		Edits: edits, Estimate: next}, nil
}

// addStep proposes one more model call reading what the last one produced.
func (in Insight) addStep(req Request) (*Proposal, error) {
	doc := req.Doc
	host := doc.Find(req.Selected)
	if host == nil || host.Kind == KindSource {
		host = lastOfKind(doc, KindInfer)
	}
	if host == nil {
		return &Proposal{Kind: "BUILD", Title: "Nothing to read yet",
			Body: "Add a source and a model step, and this proposal will have a record shape to " +
				"write a prompt against."}, nil
	}
	fields := doc.Fields(host.ID)
	read := "output"
	if len(fields) > 0 {
		read = fields[len(fields)-1]
	}
	id := doc.UniqueID("flag-risk")
	step := Step{
		ID: id, Kind: KindInfer, From: host.ID,
		Title: "Flag what deserves attention", Note: "One per record, after " + host.ID,
		Infer: &InferSpec{
			Tier:      "cheapest",
			System:    "You mark records that need a human to look at them.",
			Prompt:    fmt.Sprintf("Read this and decide whether it needs attention:\n\n{{.%s}}", read),
			MaxTokens: 200,
			Answer: []Answer{
				{Name: "attention", Kind: "text", Note: "yes or no", Required: true},
				{Name: "reason", Note: "one sentence; empty when no"},
			},
		},
	}
	edits := []Edit{{Op: AddStep, Step: &step, Note: "add step " + id}}
	after, err := doc.Apply(edits)
	if err != nil {
		return nil, err
	}
	next := req.Price(after)
	delta := next.ExpectedUSD - spend(req.Estimate)
	body := fmt.Sprintf("Reads the %s field of every record %s produces and marks the ones worth "+
		"a look. Priced at %s on the cheapest model, which takes the run to %s.",
		read, host.ID, money(delta), money(next.ExpectedUSD))
	if next.CapUSD > 0 && !next.FitsCap {
		body += fmt.Sprintf(" That is over the %s cap: the run would stop partway unless the cap moves.",
			money(next.CapUSD))
	}
	return &Proposal{Kind: "BUILD", Title: "Add a step: flag what deserves attention", Body: body,
		Ghost: step.Title, GhostNote: "One per record", After: host.ID,
		Edits: edits, Estimate: next}, nil
}

// fallback is what an assistant that computes rather than converses owes the
// person who asked it something else.
func (Insight) fallback(doc *Doc, est *Estimate, q string) (*Proposal, error) {
	var b strings.Builder
	b.WriteString("The assistant that ships with the studio answers from the projection rather " +
		"than from a model, so it can tell you what a step costs, what a change would reprice " +
		"to, how long the rate limits alone will take, and what a second run pays for — and it " +
		"cannot tell you whether the output is any good. ")
	if est != nil && est.Error == "" {
		fmt.Fprintf(&b, "What it knows about %q right now: %s.", doc.Name, est.String())
	} else {
		fmt.Fprintf(&b, "Right now %q does not price: %s.", doc.Name, errText(est))
	}
	b.WriteString(" Attach a model-backed assistant with studio.WithAssistant to ask the rest.")
	return &Proposal{Kind: "EXPLAIN", Title: "Not something this assistant can compute", Body: b.String()}, nil
}

// --- small helpers ---------------------------------------------------------

func lastOfKind(d *Doc, k Kind) *Step {
	order, err := d.Order()
	if err != nil {
		return nil
	}
	var out *Step
	for _, s := range order {
		if s.Kind == k {
			out = s
		}
	}
	return out
}

func name(s *Step, id string) string {
	if s != nil && s.Title != "" {
		return s.Title
	}
	return id
}

func errText(est *Estimate) string {
	if est == nil {
		return "it has not been priced"
	}
	return est.Error
}

// spend is an estimate's expected cost, or zero when there is no estimate.
func spend(est *Estimate) float64 {
	if est == nil {
		return 0
	}
	return est.ExpectedUSD
}

func paidCalls(est *Estimate) int {
	if est == nil {
		return 0
	}
	return est.Calls
}

func perCall(tokens, calls int) int {
	if calls <= 0 {
		return 0
	}
	return tokens / calls
}

func money(v float64) string {
	if v > 0 && v < 0.01 {
		return fmt.Sprintf("$%.4f", v)
	}
	return fmt.Sprintf("$%.2f", v)
}

func pct(share float64) string { return fmt.Sprintf("%.0f%%", share*100) }

func round2(v float64) float64 { return float64(int(v*100+0.5)) / 100 }

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func trimNum(v float64) string {
	s := strconv.FormatFloat(round2(v), 'f', 2, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "" {
		return "0"
	}
	return s
}

// thousands renders a count the way the panels do: 1,204 rather than 1204.
func thousands(n int) string {
	s := strconv.Itoa(n)
	if n < 0 {
		s = s[1:]
	}
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	parts = append([]string{s}, parts...)
	out := strings.Join(parts, ",")
	if n < 0 {
		return "-" + out
	}
	return out
}
