package findings

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/task"
)

// GuardSpec configures how one tool's calls are turned into questions and its
// results into findings. The zero value works: the tool's name becomes the
// topic, its most question-shaped argument becomes the text, and the rest
// become facets.
type GuardSpec struct {
	// Topic overrides the topic these calls are filed under. Default: the
	// tool's name. Point two tools that answer the same kind of question at one
	// topic and they share a commons; leave them apart and they do not.
	Topic string

	// Ask turns a tool call into a question, returning false to pass the call
	// through ungated. Default: TextArg over the usual argument names.
	//
	// Returning false is the right answer for a call that is not a question at
	// all — a write, an action, anything whose value is its effect. Gating
	// those would not save research; it would skip work that had to happen.
	Ask func(tool string, args map[string]any) (Question, bool)

	// Rewrite narrows a tool's arguments to a narrowed question, so a partial
	// hit shrinks the external call instead of repeating it. Default: replace
	// the argument Ask read the text from.
	Rewrite func(args map[string]any, q Question) map[string]any

	// Harvest turns a tool result into a contribution. Default: the result's
	// text and structured content, with the calling tool and host as its
	// source.
	Harvest func(q Question, out any) Result

	// Needs, when set, is attached to every question this tool asks — the
	// fields a caller of this tool always requires. It is what turns coverage
	// from a formality into a real test, and what makes topping up possible.
	Needs []string

	// CostUSD is what one call to this source costs, when it costs money.
	//
	// A tool call spends no tokens, so without this the layer's dollar
	// accounting reads zero for a metered search API that bills per query — and
	// a saving reported as zero is a saving nobody will believe. Latency is
	// measured either way; this is the part only the operator knows. It also
	// feeds the gate's break-even rule, which needs to know what the research
	// it is deciding about is worth.
	CostUSD float64
}

// Guard wraps a tool so every call passes the shared research layer first.
//
// This is what makes the layer a *gate* rather than a library an executor may
// remember to consult. The wrapped tool is the one already registered, already
// named, already granted; the guard sits between the executor's capability and
// egress checks and the call itself, so nothing about planning, prompting, or
// authoring changes:
//
//	web := mcp.Stdio("web", "npx", "-y", "@example/search-mcp")
//	loom.NewFleet(
//	    loom.WithMCPServer(web),
//	    loom.WithFindings(findings.Config{
//	        Gate: []string{"mcp/web/search"},          // ← the public source
//	        Policy: findings.Policy{ ... },
//	    }),
//	)
//
// # The one contract a gated tool takes on
//
// A guarded tool returns `{"text": ..., "structured": ...}` — the shape
// mcp.Dispatch already reads — whether the answer came from the commons or
// from the source. It has to: a served answer is reconstructed from a finding,
// and a finding holds prose and fields. So gating a tool normalizes its result
// shape, and that is a real constraint rather than an implementation detail.
// Gate the tools whose value is the information they return; leave the ones
// whose value is their effect alone, which Ask returning false also expresses.
func Guard(g *Gate, t executor.Tool, spec GuardSpec) executor.Tool {
	w := &guarded{gate: g, tool: t, spec: spec}
	// A tool that reaches the network keeps saying so through the wrapper, or
	// the executor's deny-by-default egress check silently stops applying to
	// it — the guard would have made the tool *less* contained, which is the
	// opposite of the point.
	if nt, ok := t.(executor.NetworkTool); ok {
		return &guardedNet{guarded: w, endpoint: nt.Endpoint()}
	}
	return w
}

type guarded struct {
	gate *Gate
	tool executor.Tool
	spec GuardSpec
}

type guardedNet struct {
	*guarded
	endpoint string
}

func (w *guardedNet) Endpoint() string { return w.endpoint }

func (w *guarded) Name() string { return w.tool.Name() }

// Invoke serves a call made without an envelope. With no grants to check
// against, nothing from the commons is reachable — Reachable fails closed — so
// this path researches directly. It exists so a guarded tool is still a Tool.
func (w *guarded) Invoke(ctx context.Context, args map[string]any) (any, error) {
	return w.research(ctx, task.Envelope{}, "", args)
}

// InvokeIn is the path that matters: the executor has already checked the
// capability and the egress allowlist, and hands over the envelope those checks
// were made against — which is exactly what the ledger needs to decide whether
// this reader may be served someone else's research.
func (w *guarded) InvokeIn(ctx context.Context, env task.Envelope, taskID string, args map[string]any) (any, error) {
	return w.research(ctx, env, taskID, args)
}

func (w *guarded) research(ctx context.Context, env task.Envelope, taskID string, args map[string]any) (any, error) {
	q, ok := w.ask(args)
	if !ok {
		return w.call(ctx, env, taskID, args)
	}
	req := Request{
		Question: q, Grants: env.Grants, Egress: env.Egress,
		RunID: env.RunID, Stage: env.Stage, TaskID: taskID,
	}
	ans, err := w.gate.Research(ctx, req, func(ctx context.Context, asked Question) (Result, error) {
		callArgs := args
		if asked.Key() != q.Key() {
			callArgs = w.rewrite(args, asked)
		}
		start := time.Now()
		out, err := w.call(ctx, env, taskID, callArgs)
		if err != nil {
			return Result{}, err
		}
		res := w.harvest(asked, out)
		res.Latency = time.Since(start)
		return res, nil
	})
	if err != nil {
		return nil, err
	}
	return render(ans), nil
}

func (w *guarded) call(ctx context.Context, env task.Envelope, taskID string, args map[string]any) (any, error) {
	if st, ok := w.tool.(executor.ScopedTool); ok {
		return st.InvokeIn(ctx, env, taskID, args)
	}
	return w.tool.Invoke(ctx, args)
}

func (w *guarded) topic() string {
	if w.spec.Topic != "" {
		return w.spec.Topic
	}
	return w.tool.Name()
}

func (w *guarded) ask(args map[string]any) (Question, bool) {
	if w.spec.Ask != nil {
		q, ok := w.spec.Ask(w.tool.Name(), args)
		if ok && q.Topic == "" {
			q.Topic = w.topic()
		}
		if ok && len(q.Needs) == 0 {
			q.Needs = w.spec.Needs
		}
		return q, ok
	}
	q, ok := TextArg(w.topic(), args)
	if ok {
		q.Needs = w.spec.Needs
	}
	return q, ok
}

func (w *guarded) rewrite(args map[string]any, q Question) map[string]any {
	if w.spec.Rewrite != nil {
		return w.spec.Rewrite(args, q)
	}
	out := make(map[string]any, len(args))
	for k, v := range args {
		out[k] = v
	}
	if field, ok := textField(args); ok {
		out[field] = q.Text
	}
	return out
}

func (w *guarded) harvest(q Question, out any) Result {
	if w.spec.Harvest != nil {
		res := w.spec.Harvest(q, out)
		w.attribute(&res)
		return res
	}
	res := Result{Text: textOf(out)}
	if m, ok := out.(map[string]any); ok {
		if v, ok := m["structured"]; ok {
			if fields, ok := v.(map[string]any); ok {
				res.Fields = fields
			}
		}
	}
	// Nothing came back. Recording that is the point of a negative finding:
	// the next agent asking this question is told the search was already run
	// and found nothing, instead of running it again.
	if strings.TrimSpace(res.Text) == "" && len(res.Fields) == 0 {
		res.NoEvidence = true
	}
	w.attribute(&res)
	return res
}

// attribute stamps the calling tool and its host onto a result's provenance, so
// containment is derived from what actually happened rather than from what a
// harvester remembered to declare, and prices the call when the spec says what
// one costs.
func (w *guarded) attribute(res *Result) {
	if w.spec.CostUSD > 0 && res.Cost.CostUSD == 0 {
		res.Cost.CostUSD = w.spec.CostUSD
		res.Cost.Requests++
	}
	host := ""
	if nt, ok := w.tool.(executor.NetworkTool); ok {
		host = nt.Endpoint()
	}
	if len(res.Sources) == 0 {
		res.Sources = []Source{{Tool: w.tool.Name(), Host: host}}
		return
	}
	for i := range res.Sources {
		if res.Sources[i].Tool == "" {
			res.Sources[i].Tool = w.tool.Name()
		}
		if res.Sources[i].Host == "" {
			res.Sources[i].Host = host
		}
	}
}

// render turns an answer back into the shape a tool call returns.
func render(ans Answer) map[string]any {
	out := map[string]any{"text": ans.Text}
	if len(ans.Fields) > 0 {
		out["structured"] = ans.Fields
	}
	// The provenance rides along so a stage — or a prompt — can tell a fresh
	// answer from a reused one, and how old the reused one is. An agent given
	// someone else's research should be able to see that that is what it is.
	prov := map[string]any{"origin": string(ans.Origin)}
	if ans.Hash != "" {
		prov["finding"] = ans.Hash
	}
	if ans.Reused() {
		prov["age"] = ans.Age.Round(time.Second).String()
	}
	if len(ans.Sources) > 0 {
		uris := make([]string, 0, len(ans.Sources))
		for _, s := range ans.Sources {
			if s.URI != "" {
				uris = append(uris, s.URI)
			}
		}
		if len(uris) > 0 {
			prov["sources"] = uris
		}
	}
	out["findings"] = prov
	return out
}

// --- Question extraction ------------------------------------------------

// textArgs are the argument names a search-shaped tool calls its question,
// in the order a tool that uses more than one most likely means them.
var textArgs = []string{"query", "question", "q", "search", "prompt", "text", "input", "url", "uri"}

// TextArg is the default question extractor: the first recognizably
// question-shaped argument becomes the text, and every other scalar argument
// becomes a facet.
//
// Promoting the remaining arguments to facets rather than folding them into the
// text is what gives the class tier something to work with. Two calls with the
// same filters and differently worded queries are then one subject with two
// phrasings — which is the case worth catching, and the one a single opaque key
// over the whole argument map cannot see.
func TextArg(topic string, args map[string]any) (Question, bool) {
	field, ok := textField(args)
	if !ok {
		return Question{}, false
	}
	text := fmt.Sprintf("%v", args[field])
	if strings.TrimSpace(text) == "" {
		return Question{}, false
	}
	q := Question{Topic: topic, Text: text, Facets: map[string]string{}}
	for k, v := range args {
		if k == field {
			continue
		}
		switch v.(type) {
		case string, bool, float64, float32, int, int64:
			q.Facets[k] = fmt.Sprintf("%v", v)
		}
	}
	if len(q.Facets) == 0 {
		q.Facets = nil
	}
	return q, true
}

func textField(args map[string]any) (string, bool) {
	for _, name := range textArgs {
		if v, ok := args[name]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return name, true
			}
		}
	}
	// A single string argument under any name is unambiguous enough.
	var only string
	count := 0
	for k, v := range args {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			only, count = k, count+1
		}
	}
	if count == 1 {
		return only, true
	}
	return "", false
}

// textOf renders a tool result as prose, handling the shapes tools return: a
// string, a content object with a text field, or anything else as JSON.
func textOf(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case map[string]any:
		if s, ok := t["text"].(string); ok {
			return s
		}
	}
	blob, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(blob)
}

// --- The recall tool ----------------------------------------------------

// RecallTool is the name Recall registers under.
const RecallTool = "findings/recall"

// Recall exposes the commons to a model as an ordinary tool: ask what the fleet
// already knows, get an answer or nothing, spend no external calls either way.
//
// The guard covers the case where an agent was going to call a source anyway.
// This covers the other one — an agent deciding whether it needs to research at
// all — and it is deliberately read-only: a lookup that could trigger research
// would be a tool whose cost a plan cannot bound.
func Recall(g *Gate) executor.Tool { return &recall{gate: g} }

type recall struct{ gate *Gate }

func (r *recall) Name() string { return RecallTool }

func (r *recall) Invoke(ctx context.Context, args map[string]any) (any, error) {
	return r.lookup(ctx, task.Envelope{}, "", args)
}

func (r *recall) InvokeIn(ctx context.Context, env task.Envelope, taskID string, args map[string]any) (any, error) {
	return r.lookup(ctx, env, taskID, args)
}

func (r *recall) lookup(ctx context.Context, env task.Envelope, taskID string, args map[string]any) (any, error) {
	q := Question{
		Topic:  str(args["topic"]),
		Text:   str(args["question"]),
		Facets: strMap(args["facets"]),
		Needs:  strSlice(args["needs"]),
	}
	if q.Topic == "" || q.Text == "" {
		return nil, core.Permanent(fmt.Errorf("%s: needs a topic and a question", RecallTool))
	}
	ans, ok := r.gate.Lookup(ctx, Request{
		Question: q, Grants: env.Grants, Egress: env.Egress,
		RunID: env.RunID, Stage: env.Stage, TaskID: taskID,
	})
	if !ok {
		return map[string]any{"found": false, "text": ""}, nil
	}
	out := render(ans)
	out["found"] = true
	return out, nil
}

func str(v any) string {
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func strMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		out[k] = fmt.Sprintf("%v", val)
	}
	return out
}

func strSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s := fmt.Sprintf("%v", item); s != "" {
				out = append(out, s)
			}
		}
		sort.Strings(out)
		return out
	}
	return nil
}
