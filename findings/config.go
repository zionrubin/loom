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

	// Shared connects this host's commons to the other executors through a
	// distributed backend, so what one process learns every process can be
	// served.
	//
	// Leaving it nil is the original, single-process layer, unchanged and with
	// no network on any path. Setting it adds a rung to the ladder rather than
	// replacing one: the in-process ledger is still consulted first and still
	// answers without I/O, and the backend is reached only when it had nothing.
	//
	//	backend, err := pgstore.Open(ctx, dsn, pgstore.Options{Dimensions: 1536})
	//	cfg := findings.Config{
	//	    Gate:   []string{"mcp/web/search"},
	//	    Shared: findings.NewShared(findings.SharedConfig{Backend: backend}),
	//	}
	//
	// The host closes it with everything else it opened.
	Shared *Shared
}

// Enabled reports whether the config asks for anything.
func (c Config) Enabled() bool { return len(c.Gate) > 0 || c.Recall }

// Distributed reports whether this host shares its commons with other
// executors.
func (c Config) Distributed() bool { return c.Shared.ok() }
