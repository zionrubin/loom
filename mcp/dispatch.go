package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/zionrubin/loom/core"
)

// DispatchSpec configures Dispatch.
type DispatchSpec struct {
	// Server is the MCP server the requested tool must live on.
	Server string
	// Allow narrows what may be dispatched to these tool names. Empty means
	// every tool the stage was granted, which is already narrower than the
	// server offers — but a model choosing from a list it was shown is a good
	// reason to state the list twice.
	Allow []string
	// ToolField holds the tool name the model chose (default "tool"), and
	// ArgsField the arguments object it produced (default "args"). Both are
	// read from the record's Data, which is where a ParseJSON inference stage
	// puts them.
	ToolField string
	ArgsField string
	// OutputField receives the tool's text result (default "tool_result"), and
	// StructuredField its structured content when the server returned any
	// (default OutputField + "_structured").
	OutputField     string
	StructuredField string
	// Optional passes records with no tool name through unchanged instead of
	// failing them. It is what makes "call a tool if you need one" expressible.
	Optional bool
}

// Dispatch returns a MapTools function that executes the tool call a model
// asked for: it reads the tool name and arguments a previous inference stage
// parsed into the record, invokes that tool through the task's
// capability-checked session, and writes the result back into the record.
//
// It is how model-directed tool use works in Loom, and it is deliberately two
// stages rather than a loop hidden inside one. A task that called tools until
// the model was satisfied would be a task whose cost is unbounded, whose cache
// key describes only its input, and whose failure the scheduler cannot
// classify. Splitting it means the model's choice is data — visible in the
// record, in the lineage, and in the projection — and the loop, when a loop is
// wanted, is pipeline.Iterate, which is bounded by rounds, by a budget, and by
// convergence, and whose every round is an ordinary scheduled stage:
//
//	step := src.
//	    Infer("choose", pipeline.InferSpec{
//	        Binding:   model.Binding{Tier: model.TierFast},
//	        Prompt:    `Question: {{.question}}
//	Reply with JSON {"tool": "...", "args": {...}}.`,
//	        ParseJSON: true,
//	    }).
//	    MapTools("call", mcp.Dispatch(mcp.DispatchSpec{Server: "catalog"}),
//	        pipeline.WithMCP("catalog"), pipeline.WithNoCache())
//
// The dispatcher enforces the same rule the envelope does — a tool outside
// Allow, or one the stage was never granted, is a permanent failure — because
// a model naming a tool is a request, not an authorization.
func Dispatch(spec DispatchSpec) func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
	toolField := or(spec.ToolField, "tool")
	argsField := or(spec.ArgsField, "args")
	outField := or(spec.OutputField, "tool_result")
	structField := or(spec.StructuredField, outField+"_structured")

	return func(ctx context.Context, s core.Session, r core.Record) (core.Record, error) {
		tool := strings.TrimSpace(r.String(toolField))
		if tool == "" {
			if spec.Optional {
				return r, nil
			}
			return core.Record{}, core.Semantic(fmt.Errorf(
				"record %s: field %q names no tool to call", r.ID, toolField))
		}
		// A model that answered with the qualified name is answering correctly
		// too; accept either form and check the server it named.
		if server, bare, ok := SplitToolName(tool); ok {
			if server != spec.Server {
				return core.Record{}, core.Semantic(fmt.Errorf(
					"record %s: tool %q is on server %q, not %q", r.ID, tool, server, spec.Server))
			}
			tool = bare
		}
		if len(spec.Allow) > 0 && !slices.Contains(spec.Allow, tool) {
			return core.Record{}, core.Semantic(fmt.Errorf(
				"record %s: tool %q is not one of %s", r.ID, tool, strings.Join(spec.Allow, ", ")))
		}
		args, err := toArgs(r.Data[argsField])
		if err != nil {
			return core.Record{}, core.Semantic(fmt.Errorf("record %s: field %q: %w", r.ID, argsField, err))
		}

		out, err := s.Invoke(ctx, ToolName(spec.Server, tool), args)
		if err != nil {
			return core.Record{}, err
		}
		nr := r.Clone()
		nr.Data[outField] = Text(out)
		if m, ok := out.(map[string]any); ok {
			if v, ok := m["structured"]; ok {
				nr.Data[structField] = v
			}
		}
		return nr, nil
	}
}

// toArgs coerces the arguments field into the object a tool call takes. A
// model asked for JSON sometimes answers with a JSON string containing the
// object, which is a formatting slip rather than a wrong answer.
func toArgs(v any) (map[string]any, error) {
	switch t := v.(type) {
	case nil:
		return map[string]any{}, nil
	case map[string]any:
		return t, nil
	case string:
		if strings.TrimSpace(t) == "" {
			return map[string]any{}, nil
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(t), &out); err != nil {
			return nil, fmt.Errorf("not a JSON object: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected an object, got %T", v)
	}
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// Describe renders a set of tools as the catalogue to show a model when asking
// it to choose one: the name, what it does, and the arguments it takes.
//
// Put it in an InferSpec.Prefix rather than the per-record Prompt. The list is
// identical for every record in the stage, so as a prefix it renders once per
// task and the provider's prompt cache serves it for the rest of the stage
// instead of reprocessing it per record.
func Describe(tools []ToolDesc) string {
	var b strings.Builder
	for _, t := range tools {
		fmt.Fprintf(&b, "- %s", t.Name)
		if t.Description != "" {
			fmt.Fprintf(&b, ": %s", strings.TrimSpace(t.Description))
		}
		b.WriteByte('\n')
		if len(t.InputSchema) > 0 {
			var pretty any
			if err := json.Unmarshal(t.InputSchema, &pretty); err == nil {
				if blob, err := json.Marshal(pretty); err == nil {
					fmt.Fprintf(&b, "  arguments: %s\n", blob)
				}
			}
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

// Describe renders the server's allowlisted tools for a prompt.
func (m ServerManifest) Describe() string { return Describe(m.Tools) }
