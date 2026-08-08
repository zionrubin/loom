package studio

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"github.com/zionrubin/loom/algo"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
)

// Build compiles the document's first pass into a pipeline.
//
// This is the whole point of the studio: what comes out is an ordinary
// pipeline, indistinguishable from one written by hand, so it is planned,
// fused, fingerprinted, priced, scheduled, cached and audited by the same code
// as everything else. The compilation is where declarative data becomes the
// closures the authoring API takes — a condition becomes a filter function, a
// template becomes a map function, an answer shape becomes a validator — and
// it is the only place in the studio that knows how to do that.
//
// Sources compile to pipeline.FromFunc, so the files are read when the run
// starts rather than when the document was edited. Pricing needs those records
// earlier and reads them itself; see [Price].
//
// Steps that merge branches are not in this pipeline. They are a second pass
// over what this one produces — see [Doc.BuildSecond].
func (d *Doc) Build() (*pipeline.Pipeline, error) { return d.build(false) }

// build compiles the first pass. In dry form — the form [Price] compiles —
// every step that touches the world outside the pipeline is compiled to the
// same computation without the touch.
//
// It exists because of one asymmetry in Explain: it models the paid calls but
// *executes* the cheap Go stages, which is exactly what makes its record counts
// exact. A write step is a cheap Go stage that writes files, so pricing a
// document that ends in one would leave a directory of reports behind. Asking
// what a run would cost must not perform any of it.
func (d *Doc) build(dry bool) (*pipeline.Pipeline, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	order, err := d.Order()
	if err != nil {
		return nil, err
	}
	p := pipeline.New(d.Name)
	second := d.SecondPass()
	sets := map[string]pipeline.Dataset{}
	for _, s := range order {
		if second[s.ID] {
			continue
		}
		if s.Kind == KindSource {
			spec := *s.Source
			sets[s.ID] = p.FromFunc(s.ID, func(ctx context.Context) ([]core.Record, error) {
				return LoadRecords(ctx, &spec)
			})
			continue
		}
		up, ok := sets[s.From]
		if !ok {
			return nil, fmt.Errorf("studio: step %q reads from %q, which is not built", s.ID, s.From)
		}
		ds, err := buildStep(up, s, dry)
		if err != nil {
			return nil, err
		}
		sets[s.ID] = ds
	}
	return p, nil
}

// Merges returns the folds that join branches, in document order.
func (d *Doc) Merges() []*Step {
	var out []*Step
	for i := range d.Steps {
		if d.Steps[i].Merging() {
			out = append(out, &d.Steps[i])
		}
	}
	return out
}

// SecondName is the pipeline name of the second pass: the merging fold's own
// name when there is one, which is what makes it legible in a report and in
// the constellation view's universe.
func (d *Doc) SecondName() string {
	m := d.Merges()
	if len(m) == 1 {
		return m[0].ID
	}
	return d.Name + "-2"
}

// BuildSecond compiles the second pass from the first pass's stage outputs
// (loom.RunResult.StageOutputs), returning nil when the document has no
// merging fold.
//
// The second pass exists because Loom's graphs fan out but do not fan back in.
// A fold over three branches is therefore not a stage — it is another run,
// seeded from records the first run produced, with its own budget, its own
// cache and its own sky in the constellation view. The studio draws it as a
// dashed group on the canvas rather than hiding the seam, because the seam is
// real: it starts when the branches finish, and it costs what it costs on top
// of the run before it.
func (d *Doc) BuildSecond(out map[string][]core.Record) (*pipeline.Pipeline, error) {
	return d.buildSecond(out, false)
}

func (d *Doc) buildSecond(out map[string][]core.Record, dry bool) (*pipeline.Pipeline, error) {
	merges := d.Merges()
	if len(merges) == 0 {
		return nil, nil
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	order, err := d.Order()
	if err != nil {
		return nil, err
	}
	p := pipeline.New(d.SecondName())
	second := d.SecondPass()
	sets := map[string]pipeline.Dataset{}
	for _, s := range order {
		if !second[s.ID] {
			continue
		}
		up, ok := sets[s.From]
		if s.Merging() {
			// The seam: the records the first run produced become this run's
			// source, in the order the fold names them.
			var recs []core.Record
			for _, from := range s.Merge {
				got := out[from]
				if len(got) == 0 {
					return nil, fmt.Errorf("studio: step %q merges %q, which produced no records",
						s.ID, from)
				}
				recs = append(recs, got...)
			}
			up, ok = p.FromRecords(s.ID+"-in", recs), true
		}
		if !ok {
			return nil, fmt.Errorf("studio: step %q reads from %q, which is not in the second pass",
				s.ID, s.From)
		}
		ds, err := buildStep(up, s, dry)
		if err != nil {
			return nil, err
		}
		sets[s.ID] = ds
	}
	return p, nil
}

func buildStep(up pipeline.Dataset, s *Step, dry bool) (pipeline.Dataset, error) {
	switch s.Kind {
	case KindInfer:
		spec, err := s.Infer.compile(s.ID)
		if err != nil {
			return pipeline.Dataset{}, err
		}
		var opts []pipeline.Option
		if s.Infer.Workers > 0 {
			opts = append(opts, pipeline.WithParallelism(s.Infer.Workers))
		}
		return up.Infer(s.ID, spec, opts...), nil

	case KindReduce:
		r := s.Reduce
		spec := pipeline.ReduceAISpec{
			Binding:   binding(r.Tier, r.Model, nil),
			System:    r.System,
			Prompt:    r.prompt(),
			FanIn:     r.FanIn,
			MaxTokens: r.MaxTokens,
			ItemField: r.ItemField,
		}
		if r.Output != "" {
			spec.OutputField = r.Output
		}
		return up.ReduceAI(s.ID, spec), nil

	case KindLoop:
		step, err := s.Loop.Step.compile(s.ID)
		if err != nil {
			return pipeline.Dataset{}, err
		}
		until := s.Loop.Until
		noteField := s.Loop.Note
		if noteField == "" {
			noteField = "critique"
		}
		alg, err := algo.NewRefine(algo.RefineConfig{
			Accept: func(r core.Record) bool { ok, _ := until.Match(r); return ok },
			Note:   func(r core.Record) string { return r.String(noteField) },
			Carry:  s.Loop.Carry,
		})
		if err != nil {
			return pipeline.Dataset{}, fmt.Errorf("studio: step %q: %w", s.ID, err)
		}
		return up.Iterate(s.ID, pipeline.IterateSpec{
			Step:      step,
			Algorithm: alg,
			Halt: pipeline.HaltWhen{
				MaxRounds: s.Loop.Rounds,
				Budget:    core.Budget{MaxCostUSD: s.Loop.CapUSD},
			},
		}), nil

	case KindFilter:
		keep := *s.Keep
		return up.Filter(s.ID, func(r core.Record) (bool, error) {
			return keep.Match(r)
		}, pipeline.WithVersion(versionOf("keep", keep.Field, keep.Op, keep.Value))), nil

	case KindDerive:
		f := *s.Field
		tpl, err := parseTemplate(s.ID, f.Template)
		if err != nil {
			return pipeline.Dataset{}, err
		}
		// The version is the hash of the template, so editing the template
		// invalidates exactly the cached results that saw the old one. In the
		// hand-written form this is the author's job and the first thing they
		// forget; here there is no way to change the behavior without changing
		// the version.
		return up.Map(s.ID, func(r core.Record) (core.Record, error) {
			var sb strings.Builder
			if err := tpl.Execute(&sb, r.Data); err != nil {
				return core.Record{}, core.Permanent(fmt.Errorf("step %q: %w", s.ID, err))
			}
			out := r.Clone()
			out.Data[f.Name] = sb.String()
			return out, nil
		}, pipeline.WithVersion(versionOf("field", f.Name, f.Template))), nil

	case KindWrite:
		w := *s.Write
		nameTpl, err := parseTemplate(s.ID+".name", w.Name)
		if err != nil {
			return pipeline.Dataset{}, err
		}
		body := w.Body
		if body == "" {
			body = "{{.output}}"
		}
		bodyTpl, err := parseTemplate(s.ID+".body", body)
		if err != nil {
			return pipeline.Dataset{}, err
		}
		// Writing is a side effect, and a cached stage does not repeat its
		// side effects — so this one is never cached. A replayed write is a
		// file that silently did not get written.
		return up.Map(s.ID, func(r core.Record) (core.Record, error) {
			var name, sb strings.Builder
			if err := nameTpl.Execute(&name, r.Data); err != nil {
				return core.Record{}, core.Permanent(fmt.Errorf("step %q: %w", s.ID, err))
			}
			if err := bodyTpl.Execute(&sb, r.Data); err != nil {
				return core.Record{}, core.Permanent(fmt.Errorf("step %q: %w", s.ID, err))
			}
			path := filepath.Join(w.Dir, filepath.Base(name.String()))
			if !dry {
				if err := os.MkdirAll(w.Dir, 0o755); err != nil {
					return core.Record{}, err
				}
				if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
					return core.Record{}, err
				}
			}
			out := r.Clone()
			out.Data["path"] = path
			return out, nil
		}, pipeline.WithNoCache()), nil
	}
	return pipeline.Dataset{}, fmt.Errorf("studio: step %q: cannot build kind %q", s.ID, s.Kind)
}

// compile turns a declared model call into the pipeline's InferSpec: the
// answer shape becomes the prompt's JSON instruction, the parser, and the
// validator, which is why those three can never drift apart.
func (in *InferSpec) compile(id string) (pipeline.InferSpec, error) {
	if err := in.validate(id); err != nil {
		return pipeline.InferSpec{}, err
	}
	spec := pipeline.InferSpec{
		Binding:   binding(in.Tier, in.Model, in.Escalate),
		System:    in.System,
		Prompt:    in.Prompt + in.answerInstruction(),
		MaxTokens: in.MaxTokens,
	}
	if len(in.Answer) > 0 {
		spec.ParseJSON = true
		required := in.required()
		if len(required) > 0 {
			spec.Validate = func(r core.Record) error {
				for _, a := range required {
					if err := a.check(r); err != nil {
						return err
					}
				}
				return nil
			}
		}
	} else if in.Output != "" {
		spec.OutputField = in.Output
	}
	return spec, nil
}

func (in *InferSpec) required() []Answer {
	var out []Answer
	for _, a := range in.Answer {
		if a.Required {
			out = append(out, a)
		}
	}
	return out
}

// check is the validation gate for one required answer field. A failure here
// is semantic, so the record escalates to the next model on the ladder rather
// than being retried identically or passed on empty.
//
// It is deliberately two lines long, because [Doc.Go] emits those two lines
// into the exported program. A validator whose exported form only approximated
// this one would make the export a description of the pipeline rather than the
// pipeline.
func (a Answer) check(r core.Record) error {
	if a.Kind == "list" {
		if l, _ := r.Data[a.Name].([]any); len(l) == 0 {
			return fmt.Errorf("empty %s", a.Name)
		}
		return nil
	}
	if strings.TrimSpace(r.String(a.Name)) == "" {
		return fmt.Errorf("empty %s", a.Name)
	}
	return nil
}

// answerInstruction renders the answer shape as the JSON instruction appended
// to the prompt. It is generated rather than typed because it has to agree
// with the parser and the validator exactly, and a hand-written one only
// agrees until someone edits it.
func (in *InferSpec) answerInstruction() string {
	if len(in.Answer) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("\n\nRespond with a single JSON object and nothing else:\n{")
	for i, a := range in.Answer {
		if i > 0 {
			b.WriteString(",\n ")
		}
		note := a.Note
		if note == "" {
			note = a.Name
		}
		if a.Kind == "list" {
			fmt.Fprintf(&b, "%q: [\"<%s>\"]", a.Name, note)
		} else {
			fmt.Fprintf(&b, "%q: \"<%s>\"", a.Name, note)
		}
	}
	b.WriteString("}")
	return b.String()
}

// prompt returns the fold's prompt, generating one from the points to cover
// when none was written. The generated form is what the inspector's list
// edits; typing a prompt replaces it wholesale.
func (r *ReduceSpec) prompt() string {
	if strings.TrimSpace(r.Prompt) != "" {
		return r.Prompt
	}
	var b strings.Builder
	b.WriteString("Below are {{.Count}} items, in order:\n{{range .Items}}- {{.}}\n{{end}}\n")
	b.WriteString("Write a concise markdown report covering:\n")
	for i, c := range r.Cover {
		fmt.Fprintf(&b, "%d. %s\n", i+1, c)
	}
	if r.Words > 0 {
		fmt.Fprintf(&b, "Maximum ~%d words.", r.Words)
	}
	return b.String()
}

// binding maps the studio's three-way model choice onto a model.Binding. With
// neither a tier nor a model named, the stage takes the registry's balanced
// tier — the default a registry declares for itself.
func binding(tier, id string, escalate []string) model.Binding {
	b := model.Binding{Escalation: escalate}
	switch {
	case id != "":
		b.Model = id
	case tier != "":
		b.Tier = tierOf(tier)
	default:
		b.Tier = model.TierBalanced
	}
	return b
}

func tierOf(s string) model.Tier {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "cheapest", "fast":
		return model.TierFast
	case "balanced":
		return model.TierBalanced
	case "best", "deep":
		return model.TierDeep
	}
	return ""
}

// tierName is the inverse of tierOf, for the inspector and the exporter.
func tierName(t model.Tier) string {
	switch t {
	case model.TierFast:
		return "cheapest"
	case model.TierBalanced:
		return "balanced"
	case model.TierDeep:
		return "best"
	}
	return ""
}

// versionOf is the content version of a Go-function stage: the hash of the
// declaration that produced it.
func versionOf(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "v" + hex.EncodeToString(h[:4])
}

// Match evaluates the condition against a record. Comparisons are string
// comparisons unless both sides parse as numbers, which is what a condition
// typed into a text box has to mean.
func (c Cond) Match(r core.Record) (bool, error) {
	v, present := r.Data[c.Field]
	switch c.Op {
	case "exists":
		return present, nil
	case "missing":
		return !present, nil
	}
	s := ""
	if present && v != nil {
		s = fmt.Sprintf("%v", v)
	}
	switch c.Op {
	case "is":
		return s == c.Value, nil
	case "is-not":
		return s != c.Value, nil
	case "contains":
		return strings.Contains(s, c.Value), nil
	case "not-contains":
		return !strings.Contains(s, c.Value), nil
	case "non-empty":
		if l, ok := v.([]any); ok {
			return len(l) > 0, nil
		}
		return strings.TrimSpace(s) != "", nil
	case "empty":
		if l, ok := v.([]any); ok {
			return len(l) == 0, nil
		}
		return strings.TrimSpace(s) == "", nil
	case "gt", "gte", "lt", "lte":
		a, err1 := strconv.ParseFloat(strings.TrimSpace(s), 64)
		b, err2 := strconv.ParseFloat(strings.TrimSpace(c.Value), 64)
		if err1 != nil || err2 != nil {
			// Not numbers: fall back to lexical order rather than failing the
			// record, which is what comparing two dates has to do.
			switch c.Op {
			case "gt":
				return s > c.Value, nil
			case "gte":
				return s >= c.Value, nil
			case "lt":
				return s < c.Value, nil
			default:
				return s <= c.Value, nil
			}
		}
		switch c.Op {
		case "gt":
			return a > b, nil
		case "gte":
			return a >= b, nil
		case "lt":
			return a < b, nil
		default:
			return a <= b, nil
		}
	}
	return false, core.Permanent(fmt.Errorf("studio: unknown operator %q", c.Op))
}

// String renders the condition the way the canvas labels it.
func (c Cond) String() string {
	switch c.Op {
	case "exists", "missing", "non-empty", "empty":
		return c.Field + " " + c.Op
	}
	return c.Field + " " + c.Op + " " + c.Value
}

// TemplateFuncs are the helpers available in derive, write and source-line
// templates. They are deliberately few: a template that needs more than this
// is a Go stage wearing a disguise.
//
// It is exported because the Go a document exports uses it: a derived field
// compiles to an ordinary text/template, and the exported program parses the
// same template with the same functions rather than carrying a copy of them.
func TemplateFuncs() template.FuncMap {
	return template.FuncMap{
		// join renders a list field as "a, b, c" — or "none", because a
		// prompt that says "signals: " with nothing after it reads as a
		// truncated prompt to a model.
		"join": func(v any) string {
			items, ok := v.([]any)
			if !ok {
				if s, ok := v.(string); ok && s != "" {
					return s
				}
				return "none"
			}
			parts := make([]string, 0, len(items))
			for _, it := range items {
				if s := strings.TrimSpace(fmt.Sprintf("%v", it)); s != "" {
					parts = append(parts, s)
				}
			}
			if len(parts) == 0 {
				return "none"
			}
			return strings.Join(parts, ", ")
		},
		// clock reduces an RFC3339-ish timestamp to HH:MM.
		"clock": func(v any) string {
			s := fmt.Sprintf("%v", v)
			if len(s) >= 16 {
				return s[11:16]
			}
			return s
		},
	}
}

func parseTemplate(name, text string) (*template.Template, error) {
	t, err := template.New(name).Funcs(TemplateFuncs()).Option("missingkey=zero").Parse(text)
	if err != nil {
		return nil, fmt.Errorf("studio: step %q: template: %w", name, err)
	}
	return t, nil
}

// --- sources ---------------------------------------------------------------

// canonical source field names, before any renaming through SourceSpec.Fields.
const (
	fGroup = "group"
	fName  = "name"
	fText  = "text"
	fCount = "count"
	fPath  = "path"
)

var emailRe = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)

// speakerRe matches a transcript line's speaker label: an optional HH:MM
// clock, then a name up to the first colon.
var speakerRe = regexp.MustCompile(`^((?:\d{1,2}:\d{2}\s+)?)([^:\n]{1,60}):(\s)`)

// LoadRecords reads a source's records. It runs on the machine the studio runs
// on, reads only the local filesystem, and issues no model calls — which is
// what makes it safe for the studio to call on its own, before a run, to price
// the pipeline against the real data rather than against an assumption about
// how much of it there is.
func LoadRecords(ctx context.Context, spec *SourceSpec) ([]core.Record, error) {
	var (
		recs []core.Record
		err  error
	)
	switch spec.From {
	case "records":
		recs = make([]core.Record, len(spec.Records))
		copy(recs, spec.Records)
	case "folder":
		recs, err = loadFolder(ctx, spec)
	case "table":
		recs, err = loadTable(spec)
	default:
		return nil, fmt.Errorf("studio: unknown source %q", spec.From)
	}
	if err != nil {
		return nil, err
	}
	if spec.Limit > 0 && len(recs) > spec.Limit {
		recs = recs[:spec.Limit]
	}
	for i := range recs {
		rename(&recs[i], spec)
		scrub(&recs[i], spec)
	}
	return recs, nil
}

// field resolves a canonical source field name through the source's renames.
func (spec *SourceSpec) field(canonical string) string {
	if n, ok := spec.Fields[canonical]; ok && n != "" {
		return n
	}
	return canonical
}

// rename applies SourceSpec.Fields to one record's data.
func rename(r *core.Record, spec *SourceSpec) {
	if len(spec.Fields) == 0 || spec.From == "records" {
		return
	}
	for _, canonical := range []string{fGroup, fName, fText, fCount, fPath} {
		to := spec.field(canonical)
		if to == canonical {
			continue
		}
		if v, ok := r.Data[canonical]; ok {
			delete(r.Data, canonical)
			r.Data[to] = v
		}
	}
}

func loadFolder(ctx context.Context, spec *SourceSpec) ([]core.Record, error) {
	groups, err := os.ReadDir(spec.Root)
	if err != nil {
		return nil, fmt.Errorf("studio: source %s: %w", spec.Root, err)
	}
	match := spec.Match
	if match == "" {
		match = "*"
	}
	var lineTpl *template.Template
	if line := spec.Line; line != "" {
		lineTpl, err = parseTemplate("source.line", line)
		if err != nil {
			return nil, err
		}
	}
	var recs []core.Record
	for _, g := range groups {
		if !g.IsDir() {
			continue
		}
		dir := filepath.Join(spec.Root, g.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("studio: source %s: %w", dir, err)
		}
		for _, f := range files {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if f.IsDir() {
				continue
			}
			ok, err := filepath.Match(match, f.Name())
			if err != nil {
				return nil, fmt.Errorf("studio: source: bad match pattern %q: %w", match, err)
			}
			if !ok {
				continue
			}
			name := strings.TrimSuffix(f.Name(), filepath.Ext(f.Name()))
			if spec.Since != "" && name < spec.Since {
				continue
			}
			if spec.Until != "" && name > spec.Until {
				continue
			}
			path := filepath.Join(dir, f.Name())
			text, n, err := readFile(path, lineTpl)
			if err != nil {
				return nil, err
			}
			if n == 0 {
				continue
			}
			recs = append(recs, core.NewRecord(g.Name()+"/"+name, map[string]any{
				fGroup: g.Name(), fName: name, fPath: path, fText: text, fCount: n,
			}))
		}
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].ID < recs[j].ID })
	return recs, nil
}

// readFile renders one file into one record's text. A .jsonl file is a
// transcript: each line is an object rendered through the source's line
// template. Anything else is read as-is.
func readFile(path string, lineTpl *template.Template) (string, int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", 0, fmt.Errorf("studio: source %s: %w", path, err)
	}
	if !strings.HasSuffix(path, ".jsonl") && !strings.HasSuffix(path, ".ndjson") {
		text := strings.TrimRight(string(raw), "\n")
		if strings.TrimSpace(text) == "" {
			return "", 0, nil
		}
		return text, strings.Count(text, "\n") + 1, nil
	}
	if lineTpl == nil {
		lineTpl, err = parseTemplate("source.line", "{{.text}}")
		if err != nil {
			return "", 0, err
		}
	}
	var out []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue // tolerate the odd malformed line
		}
		var sb strings.Builder
		if err := lineTpl.Execute(&sb, obj); err != nil {
			return "", 0, fmt.Errorf("studio: source %s: %w", path, err)
		}
		if s := strings.TrimSpace(sb.String()); s != "" {
			out = append(out, s)
		}
	}
	return strings.Join(out, "\n"), len(out), nil
}

func loadTable(spec *SourceSpec) ([]core.Record, error) {
	f, err := os.Open(spec.Path)
	if err != nil {
		return nil, fmt.Errorf("studio: source %s: %w", spec.Path, err)
	}
	defer f.Close()

	ext := strings.ToLower(filepath.Ext(spec.Path))
	if ext == ".jsonl" || ext == ".ndjson" {
		var recs []core.Record
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		for i := 0; sc.Scan(); i++ {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var obj map[string]any
			if err := json.Unmarshal([]byte(line), &obj); err != nil {
				return nil, fmt.Errorf("studio: source %s line %d: %w", spec.Path, i+1, err)
			}
			recs = append(recs, core.NewRecord(rowID(obj, i), obj))
		}
		return recs, sc.Err()
	}

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	if ext == ".tsv" {
		r.Comma = '\t'
	}
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("studio: source %s: %w", spec.Path, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	head := rows[0]
	recs := make([]core.Record, 0, len(rows)-1)
	for i, row := range rows[1:] {
		data := make(map[string]any, len(head))
		for j, h := range head {
			if j < len(row) {
				data[strings.TrimSpace(h)] = row[j]
			}
		}
		recs = append(recs, core.NewRecord(rowID(data, i), data))
	}
	return recs, nil
}

func rowID(data map[string]any, i int) string {
	for _, k := range []string{"id", "ID", "key", "name"} {
		if v, ok := data[k]; ok {
			if s := strings.TrimSpace(fmt.Sprintf("%v", v)); s != "" {
				return s
			}
		}
	}
	return "row-" + strconv.Itoa(i+1)
}

// scrub applies the source's redactions to a record's text.
//
// Both redactions run here, at load time, on the studio's machine — before the
// record is in a pipeline, before it is in a prompt, before it is in a cache
// entry. A redaction applied any later is a redaction that has already
// travelled.
func scrub(r *core.Record, spec *SourceSpec) {
	if len(spec.Scrub) == 0 {
		return
	}
	key := spec.field(fText)
	text, ok := r.Data[key].(string)
	if !ok {
		return
	}
	for _, s := range spec.Scrub {
		switch s {
		case "emails":
			text = emailRe.ReplaceAllString(text, "<email>")
		case "speakers":
			text = pseudonymize(text)
		}
	}
	r.Data[key] = text
}

// pseudonymize replaces each line's speaker label with a stable per-record
// label: the first speaker seen becomes S1, the second S2, and so on. Stable
// within a record is the property that matters — a digest has to be able to
// say "S1 raised it and S2 agreed" — and stable across records is the property
// worth *not* having.
func pseudonymize(text string) string {
	labels := map[string]string{}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		m := speakerRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		who := strings.TrimSpace(m[2])
		lbl, ok := labels[who]
		if !ok {
			lbl = "S" + strconv.Itoa(len(labels)+1)
			labels[who] = lbl
		}
		lines[i] = m[1] + lbl + ":" + m[3] + line[len(m[0]):]
	}
	return strings.Join(lines, "\n")
}

// Fields returns the field names a step's records carry, given its upstream.
// It is what fills the blue chips in the inspector: the fields a prompt can
// reference, computed from the document rather than typed in beside it.
func (d *Doc) Fields(id string) []string {
	s := d.Find(id)
	if s == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	add := func(names ...string) {
		for _, n := range names {
			if n != "" && !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	// A malformed document can name a cycle, and this walk runs before
	// validation has necessarily rejected one.
	walked := map[string]bool{}
	var walk func(*Step)
	walk = func(cur *Step) {
		if cur == nil || walked[cur.ID] {
			return
		}
		walked[cur.ID] = true
		if ups := cur.upstreams(); len(ups) > 0 {
			walk(d.Find(ups[0]))
		}
		switch cur.Kind {
		case KindSource:
			switch cur.Source.From {
			case "records":
				var keys []string
				for _, r := range cur.Source.Records {
					for k := range r.Data {
						keys = append(keys, k)
					}
				}
				sort.Strings(keys)
				add(keys...)
			default:
				sp := cur.Source
				add(sp.field(fGroup), sp.field(fName), sp.field(fText),
					sp.field(fCount), sp.field(fPath))
			}
		case KindInfer:
			for _, a := range cur.Infer.Answer {
				add(a.Name)
			}
			if len(cur.Infer.Answer) == 0 {
				add(field(cur.Infer.Output, "output"))
			}
		case KindLoop:
			for _, a := range cur.Loop.Step.Answer {
				add(a.Name)
			}
		case KindDerive:
			add(cur.Field.Name)
		case KindReduce:
			// A fold replaces the record rather than adding to it.
			out, seen = nil, map[string]bool{}
			add(field(cur.Reduce.Output, "output"))
		case KindWrite:
			add(fPath)
		}
	}
	walk(s)
	return out
}
