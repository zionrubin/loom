package ops

import (
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/memory"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/task"
)

// Default field names a recall stage writes into each record.
const (
	defaultRecallField = "memory"
	defaultIDsField    = "memory_ids"
	defaultItemIDField = "memory_id"
)

type recallRunner struct {
	spec  *pipeline.RecallSpec
	query *template.Template
	// filter holds one compiled template per filter key, so a filter can
	// narrow the search per record — a tenant, a language, a document class.
	filter map[string]*template.Template
}

func (r *recallRunner) Run(ctx context.Context, rt *executor.Runtime, t task.Task) ([]core.Record, core.Usage, string, error) {
	if rt.Memory == nil {
		return nil, core.Usage{}, "", core.Permanent(
			fmt.Errorf("stage %q: no memory store configured for this run (loom.WithMemory)", t.Stage))
	}
	outField := or(r.spec.OutputField, defaultRecallField)
	idField := or(r.spec.IDField, defaultIDsField)
	k := r.spec.K
	if k <= 0 {
		k = 5
	}

	var usage core.Usage
	out := make([]core.Record, 0, len(t.Input))
	for _, rec := range t.Input {
		query, err := render(r.query, rec.Data)
		if err != nil {
			return nil, usage, "", core.Permanent(fmt.Errorf("render recall query: %w", err))
		}
		filter, err := r.renderFilter(rec)
		if err != nil {
			return nil, usage, "", err
		}

		hits, u, err := rt.Memory.Recall(ctx, rt.Env, rt.TaskID, executor.RecallRequest{
			Space: r.spec.Space, Query: query, K: k,
			Filter: filter, MinScore: r.spec.MinScore,
		})
		usage.Add(u)
		if err != nil {
			return nil, usage, "", err
		}
		if len(hits) == 0 && r.spec.Require {
			// A prompt that is meaningless without context should fail loudly
			// rather than quietly ask the model to reason from nothing.
			// Semantic, not permanent: escalation is pointless but the record
			// is a legitimate dead letter under ContinueOnError.
			return nil, usage, "", core.Semantic(
				fmt.Errorf("record %s: nothing recalled from memory %q", rec.ID, r.spec.Space))
		}

		nr := rec.Clone()
		nr.Data[outField] = memory.Render(hits)
		if idField != "" {
			// The retrieved IDs are what carry this lookup into every
			// downstream cache key. Two runs against different epochs of the
			// knowledge base produce identical records here whenever the
			// neighbourhood did not move, and the inference below them replays
			// for free.
			nr.Data[idField] = memory.IDs(hits)
		}
		if f := r.spec.ScoreField; f != "" {
			scores := make([]float64, 0, len(hits))
			for _, h := range hits {
				scores = append(scores, float64(h.Score))
			}
			nr.Data[f] = scores
		}
		out = append(out, nr)
	}
	return out, usage, "", nil
}

func (r *recallRunner) renderFilter(rec core.Record) (map[string]string, error) {
	if len(r.filter) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(r.filter))
	for k, tmpl := range r.filter {
		v, err := render(tmpl, rec.Data)
		if err != nil {
			return nil, core.Permanent(fmt.Errorf("render recall filter %q: %w", k, err))
		}
		out[k] = v
	}
	return out, nil
}

type rememberRunner struct {
	spec *pipeline.RememberSpec
	text *template.Template
	meta map[string]*template.Template
}

func (r *rememberRunner) Run(ctx context.Context, rt *executor.Runtime, t task.Task) ([]core.Record, core.Usage, string, error) {
	if rt.Memory == nil {
		return nil, core.Usage{}, "", core.Permanent(
			fmt.Errorf("stage %q: no memory store configured for this run (loom.WithMemory)", t.Stage))
	}
	idField := or(r.spec.IDField, defaultItemIDField)

	// One embedding request per task rather than per record: the items are
	// built first, embedded together, and staged together. A stage with
	// WithBatchSize therefore pays one round trip for the whole batch.
	items := make([]memory.Item, 0, len(t.Input))
	at := make([]int, 0, len(t.Input))
	out := make([]core.Record, 0, len(t.Input))
	for i, rec := range t.Input {
		text, err := render(r.text, rec.Data)
		if err != nil {
			return nil, core.Usage{}, "", core.Permanent(fmt.Errorf("render remember text: %w", err))
		}
		out = append(out, rec.Clone())
		if strings.TrimSpace(text) == "" && !r.spec.WriteEmpty {
			continue
		}
		meta, err := r.renderMeta(rec)
		if err != nil {
			return nil, core.Usage{}, "", err
		}
		items = append(items, memory.Item{
			Space: r.spec.Space, Text: text, Meta: meta,
			Source: memory.Source{Model: t.ResolvedModel, Op: t.Fingerprint},
		})
		at = append(at, i)
	}
	if len(items) == 0 {
		return out, core.Usage{}, "", nil
	}

	ids, usage, err := rt.Memory.Remember(ctx, rt.Env, rt.TaskID, items)
	if err != nil {
		return nil, usage, "", err
	}
	if idField != "" {
		for j, id := range ids {
			out[at[j]].Data[idField] = id
		}
	}
	return out, usage, "", nil
}

func (r *rememberRunner) renderMeta(rec core.Record) (map[string]any, error) {
	if len(r.meta) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(r.meta))
	for k, tmpl := range r.meta {
		v, err := render(tmpl, rec.Data)
		if err != nil {
			return nil, core.Permanent(fmt.Errorf("render remember meta %q: %w", k, err))
		}
		out[k] = v
	}
	return out, nil
}

// render executes a template against a record's data.
func render(tmpl *template.Template, data map[string]any) (string, error) {
	var sb strings.Builder
	if err := tmpl.Execute(&sb, data); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// parseFields compiles a map of templates keyed by field name.
func parseFields(prefix string, fields map[string]string) (map[string]*template.Template, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	out := make(map[string]*template.Template, len(fields))
	for k, v := range fields {
		tmpl, err := template.New(prefix + "." + k).Option("missingkey=error").Parse(v)
		if err != nil {
			return nil, err
		}
		out[k] = tmpl
	}
	return out, nil
}

func or(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
