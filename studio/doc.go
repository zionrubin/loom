// Package studio is Loom's authoring surface: a browser-based editor where a
// pipeline is built by placing steps on a canvas, priced from your own data
// before a single model call is made, and exported as the Go program it
// already was.
//
// The studio is not a second way to run a pipeline. It edits a *document* — a
// flat, serializable list of steps — and compiles that document into an
// ordinary [pipeline.Pipeline]. Everything downstream is the framework doing
// what it already does: [loom.Explain] prices it, [loom.Run] runs it, the
// constellation view watches it. Nothing about a pipeline built here is
// weaker than one written by hand, because by the time it matters it *is* one.
//
//	doc, _ := studio.Load("vertical-digest.json")
//	s := studio.New(doc, studio.Models(reg), studio.File("vertical-digest.json"))
//	url, _ := s.Start("localhost:8078")
//
// # Why the document is declarative
//
// A pipeline written in Go holds closures: a filter is a func, a source is a
// func, a validator is a func. A document that must survive a round trip
// through a browser, a JSON file, and an undo stack cannot hold any of them.
// So every step here is data — a condition is a field, an operator and a
// value; a derived field is a template; an answer shape is a list of field
// declarations — and [Doc.Build] is what turns data back into the closures the
// pipeline API wants.
//
// That constraint buys three things the closure form cannot offer. The
// document can be priced, diffed, and proposed against by something other than
// a human: the ⌘K assistant returns *edits*, and nothing changes until they are
// accepted. A derived field's cache version is the hash of its template, so
// editing the template invalidates exactly the results that saw the old one —
// the bookkeeping [pipeline.WithVersion] otherwise leaves to the author. And
// the document exports to Go ([Doc.Go]), which is the direction that matters:
// the studio is where a pipeline starts, not where it is trapped.
//
// # What a step is
//
// Seven kinds, matching the palette in the UI. [KindSource] reads records off
// the local disk; [KindInfer] calls a model once per record; [KindReduce]
// folds many records into one through a tree of calls; [KindLoop] repeats a
// model call over a record until it is good enough; [KindFilter] keeps the
// records matching a condition; [KindDerive] adds a field computed from the
// record; [KindWrite] writes each record to a file. Sources have no upstream,
// everything else names one in [Step.From], and two steps naming the same
// upstream are a branch — the fan-out that turns a chain into a DAG.
package studio

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/zionrubin/loom/core"
)

// Kind enumerates the step types the palette offers. Each maps onto exactly
// one pipeline operator, which is why a document compiles rather than
// interprets.
type Kind string

const (
	// KindSource reads records from the local disk: a folder of files, a
	// table, or records carried in the document itself.
	KindSource Kind = "source"
	// KindInfer is one model call per record (pipeline.Infer).
	KindInfer Kind = "infer"
	// KindReduce folds every record into one through a tree of model calls
	// (pipeline.ReduceAI).
	KindReduce Kind = "reduce"
	// KindLoop repeats a model call over each record until a condition on the
	// record holds, the round cap is reached, or the step's own budget is
	// spent (pipeline.Iterate with algo.Refine).
	KindLoop Kind = "loop"
	// KindFilter keeps the records matching a condition (pipeline.Filter).
	KindFilter Kind = "filter"
	// KindDerive adds one field computed from the record by a template
	// (pipeline.Map). No model, no cost.
	KindDerive Kind = "derive"
	// KindWrite writes each record to a file (pipeline.Map, uncached because
	// writing is a side effect and a replayed cache entry does not repeat it).
	KindWrite Kind = "write"
)

// Kinds lists every step kind in palette order.
func Kinds() []Kind {
	return []Kind{KindSource, KindInfer, KindReduce, KindLoop, KindFilter, KindDerive, KindWrite}
}

// Doc is a pipeline as the studio holds it: a name, the guardrails that bound
// a run, and a flat list of steps whose From fields make the graph.
//
// The order of Steps is presentation order, not execution order. Execution
// order comes from the From edges ([Doc.Order]), so a step can be added
// anywhere in the list without disturbing what runs when.
type Doc struct {
	Name string `json:"name"`
	Note string `json:"note,omitempty"`

	// CapUSD is the hard ceiling for the run, handed to loom.WithRunBudget.
	// It is a different number from what the pipeline is priced at: the price
	// is what this document is expected to cost, the cap is the point at which
	// the run stops rather than spending past it.
	CapUSD float64 `json:"cap_usd,omitempty"`

	// Workers bounds concurrent tasks across the run (loom.WithWorkers).
	Workers int `json:"workers,omitempty"`

	// KeepGoing dead-letters a failed record and carries on instead of ending
	// the run (loom.WithContinueOnError) — the right default for a run over
	// thousands of records, where one bad day should not cost the other 411.
	KeepGoing bool `json:"keep_going,omitempty"`

	Steps []Step `json:"steps"`
}

// Step is one node on the canvas: an identity, a position, the step it reads
// from, and exactly one spec matching its Kind.
type Step struct {
	// ID is the stage ID in the compiled pipeline, so it must be a slug and
	// unique within the document. It is also the join key between a step, its
	// price, and its events in the constellation view.
	ID   string `json:"id"`
	Kind Kind   `json:"kind"`

	// Title and Note are what the card shows. Neither reaches the pipeline.
	Title string `json:"title,omitempty"`
	Note  string `json:"note,omitempty"`

	// From is the ID of the upstream step. Empty for sources; two steps with
	// the same From branch the graph.
	From string `json:"from,omitempty"`

	// Merge names the folds whose finished output this fold reads, and it is
	// the one edge that is not an edge of the pipeline.
	//
	// Loom's graphs fan out but never fan back in: a branch is a second
	// consumer of a dataset, and there is no operator that takes two. A step
	// that has to read three branches is therefore a *second run* over their
	// results — which is exactly what it is here. The first pass builds and
	// runs; the second pass starts from the records the first one produced.
	// The canvas draws it inside a dashed group for the same reason: it is
	// a different run, and pretending otherwise would hide the one thing that
	// changes about when it starts and what it costs.
	//
	// Only a fold may merge, and it may only merge folds — each one
	// contributes exactly one record, which is what makes the second pass
	// priceable before the first has run.
	Merge []string `json:"merge,omitempty"`

	// X and Y are canvas coordinates. Doc.Layout fills them in for steps that
	// have none, so a document built in Go opens laid out.
	X int `json:"x,omitempty"`
	Y int `json:"y,omitempty"`

	Source *SourceSpec `json:"source,omitempty"`
	Infer  *InferSpec  `json:"infer,omitempty"`
	Reduce *ReduceSpec `json:"reduce,omitempty"`
	Loop   *LoopSpec   `json:"loop,omitempty"`
	Keep   *Cond       `json:"keep,omitempty"`
	Field  *FieldSpec  `json:"field,omitempty"`
	Write  *WriteSpec  `json:"write,omitempty"`
}

// SourceSpec declares where records come from. Sources are the one step that
// touches the world before the run starts: the studio reads them itself, on
// the machine the studio runs on, so that the price on the header is computed
// from the real records rather than assumed from a sample size.
type SourceSpec struct {
	// From selects the reader: "folder", "table", or "records".
	From string `json:"from"`

	// Root is the folder to walk (From "folder"). Its immediate
	// subdirectories become the "group" field and each file inside becomes one
	// record, which is the layout of most exported archives: one directory per
	// stream, one file per day.
	Root string `json:"root,omitempty"`

	// Match is a filename glob within each group (default "*").
	Match string `json:"match,omitempty"`

	// Since and Until clamp the record name (the filename without its
	// extension) lexically, which is a date range when the names are dates.
	Since string `json:"since,omitempty"`
	Until string `json:"until,omitempty"`

	// Line renders one line of a .jsonl file into one line of the record's
	// text (default "{{.text}}"). Beyond the record's own fields it may call
	// {{clock .createTime}}, which reduces a timestamp to HH:MM.
	Line string `json:"line,omitempty"`

	// Path is the file to read (From "table"): .csv, .tsv, or .jsonl, one
	// record per row.
	Path string `json:"path,omitempty"`

	// Records are carried in the document itself (From "records"). Useful for
	// a fixed input set, and for a document that must run anywhere.
	Records []core.Record `json:"records,omitempty"`

	// Limit caps how many records the source yields (0 = all).
	Limit int `json:"limit,omitempty"`

	// Fields renames the canonical source fields — group, name, text, count,
	// path — so every prompt downstream reads in the document's own
	// vocabulary: {{.vertical}} and {{.messages}} rather than {{.group}} and
	// {{.text}}. Naming a field the thing it actually holds is most of what
	// makes a prompt template legible six months later.
	Fields map[string]string `json:"fields,omitempty"`

	// Scrub names the redactions applied to each record's text before it can
	// reach a model: "emails" replaces addresses with <email>, "speakers"
	// replaces the speaker label at the head of each line with a stable
	// per-record S1, S2, … Both run on the studio's machine, at load time,
	// which is the only place a redaction is worth anything.
	Scrub []string `json:"scrub,omitempty"`
}

// InferSpec declares one model call per record.
type InferSpec struct {
	// Tier is "cheapest", "balanced", or "best" — the three the UI offers,
	// mapping to model.TierFast, TierBalanced and TierDeep. Model names an
	// exact model instead; setting both is an error.
	Tier  string `json:"tier,omitempty"`
	Model string `json:"model,omitempty"`

	// Escalate is the ladder walked when output fails to parse or fails
	// validation: the same record, retried on a stronger model.
	Escalate []string `json:"escalate,omitempty"`

	// System is the standing instruction ("what to do" in the inspector).
	System string `json:"system,omitempty"`

	// Prompt is a template over the record's fields ("what the model sees").
	Prompt string `json:"prompt"`

	// MaxTokens caps each response. It is also the number the ceiling price is
	// computed from, so it is a budget control as much as a length one.
	MaxTokens int `json:"max_tokens,omitempty"`

	// Answer declares the JSON object the model must return. It is one
	// declaration with three consequences: the prompt gains a generated
	// instruction naming the fields, the response is parsed into the record,
	// and every field marked Required becomes a validation gate whose failure
	// escalates rather than passing an empty record downstream.
	Answer []Answer `json:"answer,omitempty"`

	// Output is the field the raw response lands in when Answer is empty
	// (default "output").
	Output string `json:"output,omitempty"`

	// Workers bounds this step's concurrency (pipeline.WithParallelism).
	Workers int `json:"workers,omitempty"`
}

// Answer is one field of the JSON object an inference step must return.
type Answer struct {
	Name string `json:"name"`
	// Kind is "text" or "list".
	Kind string `json:"kind,omitempty"`
	// Note is the instruction for this field, e.g. "3-5 sentences: what
	// happened, decisions made, problems raised".
	Note string `json:"note,omitempty"`
	// Required rejects a response whose value for this field is empty,
	// escalating the record instead of accepting it.
	Required bool `json:"required,omitempty"`
}

// ReduceSpec declares a hierarchical fold: records are grouped FanIn at a
// time, each group summarized by one model call, and the levels repeat until
// one record is left.
type ReduceSpec struct {
	Tier  string `json:"tier,omitempty"`
	Model string `json:"model,omitempty"`

	System string `json:"system,omitempty"`

	// Prompt is a template over {{.Items}} and {{.Count}}. Leave it empty and
	// one is generated from Cover and Words, which is what the inspector's
	// "the report should cover" list edits.
	Prompt string `json:"prompt,omitempty"`

	// Cover lists the points the report must address, in order.
	Cover []string `json:"cover,omitempty"`

	// Words is the target length of the generated prompt's report (~n words).
	Words int `json:"words,omitempty"`

	// FanIn is how many records one call folds (default 8). It is the knob
	// that trades money for resolution: larger batches cost less and blur
	// more.
	FanIn int `json:"fan_in,omitempty"`

	// ItemField selects the field fed to the fold (default "output"). Pointing
	// it at a one-line summary rather than a whole document is usually what
	// makes a reduce affordable.
	ItemField string `json:"item_field,omitempty"`

	MaxTokens int    `json:"max_tokens,omitempty"`
	Output    string `json:"output,omitempty"`
}

// LoopSpec declares a model call repeated over each record until the record is
// good enough — draft, critique, revise, with the stopping decision made in Go
// rather than by another paid call.
type LoopSpec struct {
	// Step is the call each round makes. Its prompt additionally sees
	// {{.Inbox}}: what the previous round said to fix.
	Step InferSpec `json:"step"`

	// Until is the predicate that stops a record's loop.
	Until Cond `json:"until"`

	// Note is the field carrying the message a rejected record sends its next
	// round (default "critique").
	Note string `json:"note,omitempty"`

	// Rounds is the hard cap on supersteps. Required: a loop over paid calls
	// with no bound is not a pipeline.
	Rounds int `json:"rounds,omitempty"`

	// CapUSD bounds what this step may spend across all its rounds. A loop
	// cannot be priced exactly — only its cap can — which is why it has one of
	// its own rather than a share of the run's.
	CapUSD float64 `json:"cap_usd,omitempty"`

	// Carry keeps every previous note in the inbox instead of only the newest.
	Carry bool `json:"carry,omitempty"`
}

// FieldSpec adds one computed field to each record.
type FieldSpec struct {
	// Name is the field to add.
	Name string `json:"name"`
	// Template is rendered against the record's fields. Besides the standard
	// template functions it may call {{join .topics}}, which renders a list as
	// "a, b, c" (or "none" when empty).
	Template string `json:"template"`
}

// WriteSpec writes each record to a file and adds the path to the record.
type WriteSpec struct {
	// Dir is the output directory, created if missing.
	Dir string `json:"dir"`
	// Name is the filename template (e.g. "{{.group}}.md").
	Name string `json:"name"`
	// Body is the file body template (default "{{.output}}").
	Body string `json:"body,omitempty"`
}

// Cond is a condition over one field of a record: the declarative form of the
// predicate a Filter would otherwise hold as a closure.
type Cond struct {
	Field string `json:"field"`
	// Op is one of: is, is-not, contains, not-contains, exists, missing,
	// non-empty, empty, gt, gte, lt, lte.
	Op    string `json:"op"`
	Value string `json:"value,omitempty"`
}

// Ops lists every condition operator, in the order the UI offers them.
func Ops() []string {
	return []string{"is", "is-not", "contains", "not-contains", "non-empty",
		"empty", "exists", "missing", "gt", "gte", "lt", "lte"}
}

var opNeedsValue = map[string]bool{
	"is": true, "is-not": true, "contains": true, "not-contains": true,
	"gt": true, "gte": true, "lt": true, "lte": true,
}

var (
	slugRe  = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
	nonSlug = regexp.MustCompile(`[^a-z0-9]+`)
)

// Slug reduces s to a legal step ID: lowercase, digits, and single dashes.
func Slug(s string) string {
	out := nonSlug.ReplaceAllString(strings.ToLower(strings.TrimSpace(s)), "-")
	out = strings.Trim(out, "-")
	if out == "" {
		return "step"
	}
	return out
}

// Validate reports every structural problem with the document: a duplicate or
// malformed ID, an upstream that does not exist, a cycle, a missing spec, an
// operator that needs a value and has none.
//
// It is deliberately structural. Whether a prompt template parses, whether a
// model is registered, whether a folder exists — those are answered by
// [Doc.Build] and [Price], which compile and price the real pipeline rather
// than a description of one.
func (d *Doc) Validate() error {
	if strings.TrimSpace(d.Name) == "" {
		return fmt.Errorf("studio: pipeline needs a name")
	}
	if len(d.Steps) == 0 {
		return fmt.Errorf("studio: pipeline %q has no steps", d.Name)
	}
	seen := map[string]bool{}
	for i := range d.Steps {
		s := &d.Steps[i]
		if !slugRe.MatchString(s.ID) {
			return fmt.Errorf("studio: step %d has an invalid id %q "+
				"(lowercase letters, digits, - and _)", i, s.ID)
		}
		if seen[s.ID] {
			return fmt.Errorf("studio: duplicate step id %q", s.ID)
		}
		seen[s.ID] = true
	}
	for i := range d.Steps {
		s := &d.Steps[i]
		switch {
		case s.Kind == KindSource:
			if s.From != "" || len(s.Merge) > 0 {
				return fmt.Errorf("studio: source %q reads from another step; sources have no upstream", s.ID)
			}
		case len(s.Merge) > 0:
			if s.From != "" {
				return fmt.Errorf("studio: step %q both reads from %q and merges %d steps; "+
					"a merge is a second pass over finished output, so it has no upstream of its own",
					s.ID, s.From, len(s.Merge))
			}
			if s.Kind != KindReduce {
				return fmt.Errorf("studio: step %q merges %d steps but is a %s; "+
					"only a fold can merge branches", s.ID, len(s.Merge), s.Kind)
			}
			for _, m := range s.Merge {
				up := d.Find(m)
				if up == nil {
					return fmt.Errorf("studio: step %q merges %q, which is not a step", s.ID, m)
				}
				if up.Kind != KindReduce {
					return fmt.Errorf("studio: step %q merges %q, which is a %s; "+
						"a merge reads folds, because each one contributes exactly one record",
						s.ID, m, up.Kind)
				}
				if len(up.Merge) > 0 {
					return fmt.Errorf("studio: step %q merges %q, which is itself a merge; "+
						"the studio runs two passes, not a chain of them", s.ID, m)
				}
				// The second pass reads records the first pass wrote, so the
				// field one writes has to be the field the other reads. It is
				// the one place in a document where two steps agree by name
				// rather than by edge, which is the one place it can silently
				// stop being true.
				if from, to := field(up.Reduce.Output, "output"), field(s.Reduce.ItemField, "output"); from != to {
					return fmt.Errorf("studio: step %q folds field %q, but %q writes its result to %q",
						s.ID, to, m, from)
				}
			}
		default:
			if s.From == "" {
				return fmt.Errorf("studio: step %q has no upstream (drag an edge from the step it reads)", s.ID)
			}
			if !seen[s.From] {
				return fmt.Errorf("studio: step %q reads from %q, which is not a step", s.ID, s.From)
			}
			if s.From == s.ID {
				return fmt.Errorf("studio: step %q reads from itself", s.ID)
			}
		}
		if err := s.validate(); err != nil {
			return err
		}
	}
	if _, err := d.Order(); err != nil {
		return err
	}
	return nil
}

func (s *Step) validate() error {
	switch s.Kind {
	case KindSource:
		if s.Source == nil {
			return fmt.Errorf("studio: step %q: no source declared", s.ID)
		}
		switch s.Source.From {
		case "folder":
			if s.Source.Root == "" {
				return fmt.Errorf("studio: step %q: folder source needs a root directory", s.ID)
			}
		case "table":
			if s.Source.Path == "" {
				return fmt.Errorf("studio: step %q: table source needs a file path", s.ID)
			}
		case "records":
			// An empty record list is legal: a document under construction.
		default:
			return fmt.Errorf("studio: step %q: unknown source %q (folder, table, records)",
				s.ID, s.Source.From)
		}
		for _, sc := range s.Source.Scrub {
			if sc != "emails" && sc != "speakers" {
				return fmt.Errorf("studio: step %q: unknown redaction %q (emails, speakers)", s.ID, sc)
			}
		}
	case KindInfer:
		if s.Infer == nil {
			return fmt.Errorf("studio: step %q: no model call declared", s.ID)
		}
		return s.Infer.validate(s.ID)
	case KindReduce:
		if s.Reduce == nil {
			return fmt.Errorf("studio: step %q: no fold declared", s.ID)
		}
		if s.Reduce.Tier != "" && s.Reduce.Model != "" {
			return fmt.Errorf("studio: step %q: set a tier or a model, not both", s.ID)
		}
		if s.Reduce.Tier != "" && tierOf(s.Reduce.Tier) == "" {
			return fmt.Errorf("studio: step %q: unknown tier %q (cheapest, balanced, best)", s.ID, s.Reduce.Tier)
		}
		if s.Reduce.Prompt == "" && len(s.Reduce.Cover) == 0 {
			return fmt.Errorf("studio: step %q: the fold needs a prompt, or a list of points to cover", s.ID)
		}
	case KindLoop:
		if s.Loop == nil {
			return fmt.Errorf("studio: step %q: no loop declared", s.ID)
		}
		if err := s.Loop.Step.validate(s.ID); err != nil {
			return err
		}
		if s.Loop.Rounds <= 0 {
			return fmt.Errorf("studio: step %q: a loop needs a round cap", s.ID)
		}
		if err := s.Loop.Until.validate(s.ID, "until"); err != nil {
			return err
		}
	case KindFilter:
		if s.Keep == nil {
			return fmt.Errorf("studio: step %q: no condition declared", s.ID)
		}
		return s.Keep.validate(s.ID, "keep only")
	case KindDerive:
		if s.Field == nil || s.Field.Name == "" {
			return fmt.Errorf("studio: step %q: no field name declared", s.ID)
		}
		if s.Field.Template == "" {
			return fmt.Errorf("studio: step %q: field %q has no template", s.ID, s.Field.Name)
		}
	case KindWrite:
		if s.Write == nil || s.Write.Dir == "" || s.Write.Name == "" {
			return fmt.Errorf("studio: step %q: a write needs a directory and a filename", s.ID)
		}
	default:
		return fmt.Errorf("studio: step %q: unknown kind %q", s.ID, s.Kind)
	}
	return nil
}

func (in *InferSpec) validate(id string) error {
	if in.Tier != "" && in.Model != "" {
		return fmt.Errorf("studio: step %q: set a tier or a model, not both", id)
	}
	if in.Tier != "" && tierOf(in.Tier) == "" {
		return fmt.Errorf("studio: step %q: unknown tier %q (cheapest, balanced, best)", id, in.Tier)
	}
	if strings.TrimSpace(in.Prompt) == "" {
		return fmt.Errorf("studio: step %q: the model call has no prompt", id)
	}
	for _, a := range in.Answer {
		if a.Name == "" {
			return fmt.Errorf("studio: step %q: an answer field has no name", id)
		}
		if a.Kind != "" && a.Kind != "text" && a.Kind != "list" {
			return fmt.Errorf("studio: step %q: answer field %q: unknown kind %q (text, list)",
				id, a.Name, a.Kind)
		}
	}
	return nil
}

func (c *Cond) validate(id, what string) error {
	if c.Field == "" {
		return fmt.Errorf("studio: step %q: %s needs a field", id, what)
	}
	ok := false
	for _, op := range Ops() {
		if op == c.Op {
			ok = true
			break
		}
	}
	if !ok {
		return fmt.Errorf("studio: step %q: unknown operator %q", id, c.Op)
	}
	if opNeedsValue[c.Op] && c.Value == "" {
		return fmt.Errorf("studio: step %q: %q needs a value", id, c.Op)
	}
	return nil
}

// field returns v, or def when v is empty — the studio's half of every
// "default …" in the pipeline API's spec documentation.
func field(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// Find returns the step with the given ID.
func (d *Doc) Find(id string) *Step {
	for i := range d.Steps {
		if d.Steps[i].ID == id {
			return &d.Steps[i]
		}
	}
	return nil
}

// Children returns the steps reading from id, in document order. More than one
// is a branch.
func (d *Doc) Children(id string) []*Step {
	var out []*Step
	for i := range d.Steps {
		if d.Steps[i].From == id {
			out = append(out, &d.Steps[i])
		}
	}
	return out
}

// Mergers returns the second-pass folds that read id's finished output.
func (d *Doc) Mergers(id string) []*Step {
	var out []*Step
	for i := range d.Steps {
		for _, m := range d.Steps[i].Merge {
			if m == id {
				out = append(out, &d.Steps[i])
				break
			}
		}
	}
	return out
}

// upstreams returns every step s reads from: its From edge, or its Merge
// edges when it is a second-pass fold.
func (s *Step) upstreams() []string {
	if len(s.Merge) > 0 {
		return s.Merge
	}
	if s.From == "" {
		return nil
	}
	return []string{s.From}
}

// Merging reports whether the step is the fold that joins branches — the seam
// between the two passes.
func (s *Step) Merging() bool { return len(s.Merge) > 0 }

// SecondPass names every step that runs after the seam: each merging fold, and
// everything downstream of one. A step that reads a merged fold's output is in
// the second run for the same reason the fold is — the records it reads do not
// exist until the first run has finished.
func (d *Doc) SecondPass() map[string]bool {
	out := map[string]bool{}
	order, err := d.Order()
	if err != nil {
		return out
	}
	for _, s := range order {
		if s.Merging() || (s.From != "" && out[s.From]) {
			out[s.ID] = true
		}
	}
	return out
}

// Leaves returns the steps nothing reads from — the outputs of the pipeline.
func (d *Doc) Leaves() []*Step {
	var out []*Step
	for i := range d.Steps {
		id := d.Steps[i].ID
		if len(d.Children(id)) == 0 && len(d.Mergers(id)) == 0 {
			out = append(out, &d.Steps[i])
		}
	}
	return out
}

// Order returns the steps in an execution order: every step after the ones it
// reads from, sources first, document order preserved among independent
// steps. A cycle is an error rather than a silent truncation.
func (d *Doc) Order() ([]*Step, error) {
	indeg := map[string]int{}
	for i := range d.Steps {
		s := &d.Steps[i]
		indeg[s.ID] = len(s.upstreams())
	}
	var ready []*Step
	for i := range d.Steps {
		if indeg[d.Steps[i].ID] == 0 {
			ready = append(ready, &d.Steps[i])
		}
	}
	var out []*Step
	for len(ready) > 0 {
		s := ready[0]
		ready = ready[1:]
		out = append(out, s)
		for _, c := range append(d.Children(s.ID), d.Mergers(s.ID)...) {
			indeg[c.ID]--
			if indeg[c.ID] == 0 {
				ready = append(ready, c)
			}
		}
	}
	if len(out) != len(d.Steps) {
		var stuck []string
		for i := range d.Steps {
			if indeg[d.Steps[i].ID] > 0 {
				stuck = append(stuck, d.Steps[i].ID)
			}
		}
		sort.Strings(stuck)
		return nil, fmt.Errorf("studio: steps form a cycle: %s", strings.Join(stuck, ", "))
	}
	return out, nil
}

// Depth returns each step's distance from its source, which is the column the
// canvas places it in.
func (d *Doc) Depth() map[string]int {
	depth := map[string]int{}
	order, err := d.Order()
	if err != nil {
		return depth
	}
	for _, s := range order {
		d := -1
		for _, up := range s.upstreams() {
			d = max(d, depth[up])
		}
		depth[s.ID] = d + 1
	}
	return depth
}

// Canvas geometry. The numbers are the card pitch the UI draws at; they live
// here because Layout is what a document built in Go gets instead of a mouse.
const (
	colWidth = 260
	rowPitch = 160
	originX  = 40
	originY  = 90
)

// Layout assigns canvas positions to every step that has none: one column per
// depth, rows spread within a column. It never moves a step that already has a
// position, so hand-placed cards survive a round trip through a Go program
// that appends to the document.
func (d *Doc) Layout() {
	depth := d.Depth()
	rows := map[int]int{}
	order, err := d.Order()
	if err != nil {
		return
	}
	for _, s := range order {
		if s.X != 0 || s.Y != 0 {
			// Remember the row this column reached so later auto-placed cards
			// do not land on top of a hand-placed one.
			rows[depth[s.ID]] = max(rows[depth[s.ID]], (s.Y-originY)/rowPitch+1)
			continue
		}
		col := depth[s.ID]
		s.X = originX + col*colWidth
		s.Y = originY + rows[col]*rowPitch
		rows[col]++
	}
}

// Clone returns a deep copy. Every mutation in the studio goes through a copy:
// a proposal is applied to one, priced, and shown as a diff, and the document
// on screen only changes when the diff is accepted.
func (d *Doc) Clone() *Doc {
	b, err := json.Marshal(d)
	if err != nil {
		return &Doc{Name: d.Name}
	}
	var out Doc
	if err := json.Unmarshal(b, &out); err != nil {
		return &Doc{Name: d.Name}
	}
	return &out
}

// EditOp names what an edit does to a document.
type EditOp string

const (
	// AddStep appends a step (and connects it to Step.From).
	AddStep EditOp = "add-step"
	// UpdateStep replaces the step with the same ID, field by field: only the
	// specs the edit carries are replaced, so a proposal that changes a model
	// does not silently rewrite a prompt.
	UpdateStep EditOp = "update-step"
	// RemoveStep deletes a step and re-parents its children onto its upstream.
	RemoveStep EditOp = "remove-step"
	// SetCap changes the run's cost ceiling.
	SetCap EditOp = "set-cap"
	// SetWorkers changes the run's concurrency.
	SetWorkers EditOp = "set-workers"
)

// Edit is one change to a document. Edits are the only thing the assistant
// returns: it proposes, the canvas draws the difference, and the document
// changes when a human accepts it.
type Edit struct {
	Op EditOp `json:"op"`
	// ID names the step for update and remove.
	ID string `json:"id,omitempty"`
	// Step carries the new or updated step.
	Step *Step `json:"step,omitempty"`
	// Value carries the number for set-cap and set-workers.
	Value float64 `json:"value,omitempty"`
	// Note is the human-readable line shown in the diff.
	Note string `json:"note,omitempty"`
}

// Apply returns a copy of the document with the edits applied. The receiver is
// never modified — that is the whole contract of a proposal.
func (d *Doc) Apply(edits []Edit) (*Doc, error) {
	out := d.Clone()
	for _, e := range edits {
		switch e.Op {
		case AddStep:
			if e.Step == nil {
				return nil, fmt.Errorf("studio: add-step without a step")
			}
			if out.Find(e.Step.ID) != nil {
				return nil, fmt.Errorf("studio: add-step %q: a step with that id already exists", e.Step.ID)
			}
			out.Steps = append(out.Steps, *e.Step)
		case UpdateStep:
			id := e.ID
			if id == "" && e.Step != nil {
				id = e.Step.ID
			}
			cur := out.Find(id)
			if cur == nil {
				return nil, fmt.Errorf("studio: update-step %q: no such step", id)
			}
			if e.Step == nil {
				return nil, fmt.Errorf("studio: update-step %q without a step", id)
			}
			cur.merge(e.Step)
		case RemoveStep:
			if err := out.remove(e.ID); err != nil {
				return nil, err
			}
		case SetCap:
			out.CapUSD = e.Value
		case SetWorkers:
			out.Workers = int(e.Value)
		default:
			return nil, fmt.Errorf("studio: unknown edit %q", e.Op)
		}
	}
	out.Layout()
	return out, out.Validate()
}

// merge overlays the non-zero parts of n onto s. An update carries only what
// it means to change.
func (s *Step) merge(n *Step) {
	if n.Title != "" {
		s.Title = n.Title
	}
	if n.Note != "" {
		s.Note = n.Note
	}
	if n.From != "" {
		s.From = n.From
	}
	if len(n.Merge) > 0 {
		s.Merge = n.Merge
	}
	if n.X != 0 || n.Y != 0 {
		s.X, s.Y = n.X, n.Y
	}
	if n.Kind != "" {
		s.Kind = n.Kind
	}
	if n.Source != nil {
		s.Source = n.Source
	}
	if n.Infer != nil {
		s.Infer = n.Infer
	}
	if n.Reduce != nil {
		s.Reduce = n.Reduce
	}
	if n.Loop != nil {
		s.Loop = n.Loop
	}
	if n.Keep != nil {
		s.Keep = n.Keep
	}
	if n.Field != nil {
		s.Field = n.Field
	}
	if n.Write != nil {
		s.Write = n.Write
	}
}

// remove deletes a step, re-parenting its children onto its upstream so the
// graph stays connected. Removing a source with children is refused: there is
// nothing to re-parent them onto.
func (d *Doc) remove(id string) error {
	s := d.Find(id)
	if s == nil {
		return fmt.Errorf("studio: remove-step %q: no such step", id)
	}
	kids := d.Children(id)
	if s.From == "" && len(kids) > 0 {
		return fmt.Errorf("studio: cannot remove source %q: %d step(s) read from it", id, len(kids))
	}
	up := s.From
	for _, k := range kids {
		k.From = up
	}
	steps := make([]Step, 0, len(d.Steps)-1)
	for _, st := range d.Steps {
		if st.ID == id {
			continue
		}
		if len(st.Merge) > 0 {
			kept := make([]string, 0, len(st.Merge))
			for _, m := range st.Merge {
				if m != id {
					kept = append(kept, m)
				}
			}
			st.Merge = kept
		}
		steps = append(steps, st)
	}
	d.Steps = steps
	return nil
}

// UniqueID returns want, or want-2, want-3, … if it is taken.
func (d *Doc) UniqueID(want string) string {
	want = Slug(want)
	if d.Find(want) == nil {
		return want
	}
	for n := 2; ; n++ {
		try := fmt.Sprintf("%s-%d", want, n)
		if d.Find(try) == nil {
			return try
		}
	}
}

// Load reads a document from a JSON file, lays it out, and validates it.
func Load(path string) (*Doc, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var d Doc
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("studio: %s: %w", path, err)
	}
	d.Layout()
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return &d, nil
}

// Save writes the document to a JSON file, atomically: a studio that
// autosaves on every edit must not leave a half-written document behind when
// the process is killed mid-write.
func (d *Doc) Save(path string) error {
	b, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".studio-*.json")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}
