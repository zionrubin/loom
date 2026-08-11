package findings

// Config declares a host's shared research layer: the policy the gate applies,
// which tools it stands in front of, and whether the commons is also exposed to
// models as something they can consult directly.
//
// It is a host-level declaration for the same reason the rate limiter, the
// budget governor and the result cache are: what has already been learned is a
// property of the work, not of the pipeline that happened to learn it. A fleet
// gets one commons and every agent on it reads and writes that one.
type Config struct {
	// Policy configures the gate: per-topic volatility and corroboration, the
	// optional embedder and judge, and the bounds on the gate's own spending.
	Policy Policy

	// Gate names the registered tools whose calls pass the commons — the ones
	// that reach a public source. Names are the ones the tool set knows them
	// by, so an MCP tool is "mcp/<server>/<tool>".
	//
	// A tool not named here is untouched. That is the intended granularity:
	// gating is worthwhile for tools whose value is the information they
	// return, and wrong for tools whose value is their effect.
	Gate []string

	// Specs refines how a gated tool's calls become questions, by tool name.
	// The zero spec is usually right; supply one when a tool's arguments do
	// not name their question in any of the usual ways, or when the topic
	// should be shared with another tool that answers the same kind of
	// question.
	Specs map[string]GuardSpec

	// Recall registers the read-only findings/recall tool, so a stage can ask
	// what the fleet already knows before deciding to research at all. Stages
	// reach it like any tool, by being granted it.
	Recall bool
}

// Enabled reports whether the config asks for anything.
func (c Config) Enabled() bool { return len(c.Gate) > 0 || c.Recall }
