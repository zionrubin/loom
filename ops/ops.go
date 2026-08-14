// Package ops implements the operation runners the executor dispatches to:
// fused pure transforms, per-record inference, and hierarchical AI
// aggregation. Runners talk to models only through the capability-checked
// executor.Runtime.
package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/task"
)

// BuildRunners constructs a runner per executable stage in the plan.
func BuildRunners(pl *plan.Plan) (map[string]executor.OpRunner, error) {
	runners := map[string]executor.OpRunner{}
	for _, sp := range pl.Order {
		s := sp.Stage
		switch s.Kind {
		case pipeline.KindFused:
			runners[s.ID] = &fusedRunner{stages: s.Fused}
		case pipeline.KindInfer:
			tmpl, err := template.New(s.ID).Funcs(pipeline.TemplateFuncs()).
				Option("missingkey=error").Parse(s.Infer.Prompt)
			if err != nil {
				return nil, err
			}
			prefix, err := parsePrefix(s.ID, s.Infer.Prefix)
			if err != nil {
				return nil, err
			}
			runners[s.ID] = &inferRunner{spec: s.Infer, tmpl: tmpl, prefix: prefix}
		case pipeline.KindIterate:
			// A round's task is a per-record inference, so it runs on the
			// inference runner unchanged. What makes it a round is the
			// driver's loop around it and the inbox the driver wrote into the
			// record — neither of which the op needs to know about, which is
			// why iteration required no new execution machinery.
			step := &s.Iterate.Step
			tmpl, err := template.New(s.ID).Funcs(pipeline.TemplateFuncs()).
				Option("missingkey=error").Parse(step.Prompt)
			if err != nil {
				return nil, err
			}
			prefix, err := parsePrefix(s.ID, step.Prefix)
			if err != nil {
				return nil, err
			}
			runners[s.ID] = &inferRunner{spec: step, tmpl: tmpl, prefix: prefix}
		case pipeline.KindReduceAI:
			tmpl, err := template.New(s.ID).Funcs(pipeline.TemplateFuncs()).
				Parse(s.Reduce.Prompt)
			if err != nil {
				return nil, err
			}
			prefix, err := parsePrefix(s.ID, s.Reduce.Prefix)
			if err != nil {
				return nil, err
			}
			runners[s.ID] = &reduceRunner{spec: s.Reduce, tmpl: tmpl, prefix: prefix}
		case pipeline.KindSource, pipeline.KindCombine:
			// Executed by the driver, not the scheduler.
		}
	}
	return runners, nil
}

// resolveModel picks the model for this attempt: the scheduler normally
// pre-resolves it; fall back to the registry for direct executor use.
func resolveModel(rt *executor.Runtime, t task.Task) (string, error) {
	if t.ResolvedModel != "" {
		return t.ResolvedModel, nil
	}
	info, err := rt.Models.Registry.Resolve(rt.Env.Binding, t.Escalation)
	if err != nil {
		return "", core.Permanent(err)
	}
	return info.ID, nil
}

// bindTemplate returns a template whose broadcast functions resolve against
// the values this task's envelope grants.
//
// Each task gets its own clone. The runner's template is shared by every
// concurrently executing task of the stage, so rebinding functions on it in
// place would race; cloning is cheap next to a model call. Clone starts the
// copy with default options, so the caller's options are re-applied here.
func bindTemplate(ctx context.Context, tmpl *template.Template, rt *executor.Runtime, opts ...string) (*template.Template, error) {
	if len(rt.Env.Broadcasts) == 0 {
		return tmpl, nil
	}
	bound, err := tmpl.Clone()
	if err != nil {
		return nil, core.Permanent(fmt.Errorf("bind prompt template: %w", err))
	}
	for _, o := range opts {
		bound.Option(o)
	}
	bound.Funcs(template.FuncMap{
		"broadcast": func(name string) (any, error) {
			return rt.Session.Broadcast(ctx, name)
		},
		"broadcastJSON": func(name string) (string, error) {
			v, err := rt.Session.Broadcast(ctx, name)
			if err != nil {
				return "", err
			}
			b, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				return "", core.Permanent(fmt.Errorf("broadcast %q: %w", name, err))
			}
			return string(b), nil
		},
	})
	return bound, nil
}

// contextPrefix renders envelope context fragments as a prompt preamble.
func contextPrefix(env task.Envelope) string {
	if len(env.Context.Fragments) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("<context>\n")
	for _, f := range env.Context.Fragments {
		fmt.Fprintf(&b, "<%s>\n%s\n</%s>\n", f.Name, f.Content, f.Name)
	}
	b.WriteString("</context>\n\n")
	return b.String()
}

// parsePrefix compiles a stage's shared prompt-prefix template, or returns
// nil when the stage declares none.
func parsePrefix(stageID, prefix string) (*template.Template, error) {
	if prefix == "" {
		return nil, nil
	}
	return template.New(stageID + ".prefix").Funcs(pipeline.TemplateFuncs()).Parse(prefix)
}

// sharedPrefix assembles the part of the prompt every call in this stage
// sends identically: the stage's materialized continuation, then the
// envelope's context fragments, then the stage's rendered prefix template.
//
// It is rendered once per task, not once per record — both because the
// result cannot vary by record (no record data is in scope) and because that
// is what makes it a prefix the provider can cache across the whole stage.
//
// The continuation goes first because it is the largest and the most stable:
// it is the part that grew by two kilobytes since the last round while the
// fragments and the template did not change at all, so putting it at the front
// keeps the longest possible run of unchanged leading bytes — which is what a
// provider's prefix cache measures, and what delta.Hint.Stable reports.
func sharedPrefix(ctx context.Context, tmpl *template.Template, rt *executor.Runtime) (string, error) {
	prefix := rt.Continuation.Text + contextPrefix(rt.Env)
	if tmpl == nil {
		return prefix, nil
	}
	bound, err := bindTemplate(ctx, tmpl, rt)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	if err := bound.Execute(&sb, nil); err != nil {
		return "", core.Permanent(fmt.Errorf("render prompt prefix: %w", err))
	}
	return prefix + sb.String(), nil
}

type inferRunner struct {
	spec   *pipeline.InferSpec
	tmpl   *template.Template
	prefix *template.Template
}

func (r *inferRunner) Run(ctx context.Context, rt *executor.Runtime, t task.Task) ([]core.Record, core.Usage, string, error) {
	modelID, err := resolveModel(rt, t)
	if err != nil {
		return nil, core.Usage{}, "", err
	}
	maxTokens := r.spec.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	prefix, err := sharedPrefix(ctx, r.prefix, rt)
	if err != nil {
		return nil, core.Usage{}, modelID, err
	}
	tmpl, err := bindTemplate(ctx, r.tmpl, rt, "missingkey=error")
	if err != nil {
		return nil, core.Usage{}, modelID, err
	}

	var usage core.Usage
	out := make([]core.Record, 0, len(t.Input))
	for _, rec := range t.Input {
		var sb strings.Builder
		if err := tmpl.Execute(&sb, rec.Data); err != nil {
			return nil, usage, modelID, core.Permanent(fmt.Errorf("render prompt: %w", err))
		}
		resp, err := rt.Models.Call(ctx, rt.Env, rt.TaskID, modelID, model.Request{
			System:       rt.Env.Context.System,
			Prefix:       prefix,
			Prompt:       sb.String(),
			MaxTokens:    maxTokens,
			CachePrefix:  rt.Env.CachePrefix,
			Continuation: rt.Continuation.Hint(),
		})
		if err != nil {
			return nil, usage, modelID, err
		}
		usage.Add(resp.Usage)

		nr := rec.Clone()
		if r.spec.ParseJSON {
			fields, err := ParseJSONObject(resp.Text)
			if err != nil {
				return nil, usage, modelID, core.Semantic(
					fmt.Errorf("record %s: model output is not a JSON object: %w", rec.ID, err))
			}
			for k, v := range fields {
				nr.Data[k] = v
			}
		} else {
			field := r.spec.OutputField
			if field == "" {
				field = "output"
			}
			nr.Data[field] = strings.TrimSpace(resp.Text)
		}
		if r.spec.Validate != nil {
			if err := r.spec.Validate(nr); err != nil {
				return nil, usage, modelID, core.Semantic(
					fmt.Errorf("record %s: validation: %w", rec.ID, err))
			}
		}
		out = append(out, nr)
	}
	return out, usage, modelID, nil
}

type reduceRunner struct {
	spec   *pipeline.ReduceAISpec
	tmpl   *template.Template
	prefix *template.Template
}

func (r *reduceRunner) Run(ctx context.Context, rt *executor.Runtime, t task.Task) ([]core.Record, core.Usage, string, error) {
	modelID, err := resolveModel(rt, t)
	if err != nil {
		return nil, core.Usage{}, "", err
	}
	maxTokens := r.spec.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 1024
	}
	itemField := r.spec.ItemField
	if itemField == "" {
		itemField = "output"
	}
	outField := r.spec.OutputField
	if outField == "" {
		outField = "output"
	}

	items := make([]string, 0, len(t.Input))
	for _, rec := range t.Input {
		item := rec.String(itemField)
		if item == "" && itemField != outField {
			// Only the bottom level of the tree sees the records the stage was
			// pointed at; every level above it sees this stage's own aggregates,
			// which carry OutputField. Without this fallback a reduce with a
			// custom ItemField silently aggregates blanks above level one.
			item = rec.String(outField)
		}
		items = append(items, item)
	}
	prefix, err := sharedPrefix(ctx, r.prefix, rt)
	if err != nil {
		return nil, core.Usage{}, modelID, err
	}
	tmpl, err := bindTemplate(ctx, r.tmpl, rt)
	if err != nil {
		return nil, core.Usage{}, modelID, err
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, map[string]any{"Items": items, "Count": len(items)}); err != nil {
		return nil, core.Usage{}, modelID, core.Permanent(fmt.Errorf("render prompt: %w", err))
	}
	resp, err := rt.Models.Call(ctx, rt.Env, rt.TaskID, modelID, model.Request{
		System:       rt.Env.Context.System,
		Prefix:       prefix,
		Prompt:       sb.String(),
		MaxTokens:    maxTokens,
		CachePrefix:  rt.Env.CachePrefix,
		Continuation: rt.Continuation.Hint(),
	})
	if err != nil {
		return nil, core.Usage{}, modelID, err
	}
	rec := core.NewRecord(core.NewID("agg"), map[string]any{
		outField: strings.TrimSpace(resp.Text),
	})
	rec.Meta = map[string]any{"aggregated": len(items)}
	return []core.Record{rec}, resp.Usage, modelID, nil
}

type fusedRunner struct {
	stages []*pipeline.Stage
}

func (r *fusedRunner) Run(ctx context.Context, rt *executor.Runtime, t task.Task) ([]core.Record, core.Usage, string, error) {
	recs := t.Input
	for _, s := range r.stages {
		var next []core.Record
		for _, rec := range recs {
			switch s.Kind {
			case pipeline.KindMap:
				var out core.Record
				var err error
				if s.MapCtxFn != nil {
					out, err = s.MapCtxFn(ctx, rt.Session, rec.Clone())
				} else {
					out, err = s.MapFn(rec.Clone())
				}
				if err != nil {
					return nil, core.Usage{}, "", fmt.Errorf("stage %s, record %s: %w", s.ID, rec.ID, err)
				}
				next = append(next, out)
			case pipeline.KindFilter:
				keep, err := s.FilterFn(rec)
				if err != nil {
					return nil, core.Usage{}, "", fmt.Errorf("stage %s, record %s: %w", s.ID, rec.ID, err)
				}
				if keep {
					next = append(next, rec)
				}
			case pipeline.KindFlatMap:
				outs, err := s.FlatMapFn(rec.Clone())
				if err != nil {
					return nil, core.Usage{}, "", fmt.Errorf("stage %s, record %s: %w", s.ID, rec.ID, err)
				}
				next = append(next, outs...)
			default:
				return nil, core.Usage{}, "", core.Permanent(fmt.Errorf("fused stage %s: unexpected kind %q", s.ID, s.Kind))
			}
		}
		recs = next
	}
	return recs, core.Usage{}, "", nil
}

// ParseJSONObject extracts a JSON object from model output, tolerating
// surrounding prose and Markdown code fences.
func ParseJSONObject(text string) (map[string]any, error) {
	s := strings.TrimSpace(text)
	if i := strings.Index(s, "```"); i >= 0 {
		s = s[i+3:]
		s = strings.TrimPrefix(s, "json")
		if j := strings.Index(s, "```"); j >= 0 {
			s = s[:j]
		}
		s = strings.TrimSpace(s)
	}
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start < 0 || end <= start {
		return nil, fmt.Errorf("no JSON object found")
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(s[start:end+1]), &out); err != nil {
		return nil, err
	}
	return out, nil
}
