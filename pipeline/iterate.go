package pipeline

import (
	"github.com/zionrubin/loom/algo"
	"github.com/zionrubin/loom/core"
)

// IterateSpec declares an iterative computation: a model call applied to a
// set of records, repeatedly, with an algorithm deciding what runs next.
//
// It is the operator for the four workloads one forward pass cannot express —
// research that discovers its own next document, entity resolution whose
// merges change what matches, knowledge graphs whose extraction improves with
// its neighbours, and refine-until-good. All four are fixpoints, and three of
// them are fixpoints over a graph.
//
// The stage is one node of the pipeline from the outside: records in, records
// out, fed by an upstream stage and feeding downstream ones. Inside, it is a
// sequence of rounds. Every round is an ordinary batch of tasks through the
// ordinary scheduler, so admission control, class-aware retry, the escalation
// ladder, the budget governor, the content-addressed cache, lineage, and the
// event stream apply to a round exactly as they apply to a stage — not because
// they were extended to, but because a round is not a new kind of thing.
//
// # What makes the loop affordable
//
// A vertex's cache key is its state plus its inbox — not the round it is in.
// That is the whole economic argument. Convergence *means* vertices stop
// changing, so a converged vertex's key stops changing too, and the cost of a
// round falls as the computation settles instead of climbing as context
// accumulates. The engine goes one better and does not build the task at all:
// a vertex that has already run on this exact (state, inbox) has reached a
// local fixpoint, so it votes to halt rather than paying a cache lookup to
// rediscover its own answer.
//
// That rests on the vertex program being a function of (state, inbox) — the
// contract this operator declares. A step that consults the round number, a
// clock, or a random source is not one, and it turns a priceable loop back
// into an unbounded one.
type IterateSpec struct {
	// Step is the vertex program: one model call per active record per round.
	// It is an ordinary InferSpec, and everything an InferSpec can do — a
	// binding with an escalation ladder, a cached shared prefix, JSON output,
	// semantic validation — works here unchanged.
	//
	// Its prompt template additionally sees the record's inbox:
	//
	//	{{if .Inbox}}What your neighbours told you:
	//	{{range .Inbox}}- {{.}}
	//	{{end}}{{end}}
	//
	// alongside {{.Senders}}, the sending vertex IDs by index. Those two field
	// names are reserved for the duration of the stage (algo.Reserved), and a
	// record already carrying one is rejected before the stage runs rather
	// than silently overwritten.
	Step InferSpec

	// Algorithm is the control flow: what each vertex sends after it runs, and
	// where. Required.
	//
	// Use algo.NewBSP for message passing over a graph, algo.NewRefine for a
	// record that critiques itself, algo.NewBeam for search that keeps the
	// best k candidates — or implement algo.Algorithm, which is two methods
	// over plain data and needs neither a model nor a scheduler to test.
	Algorithm algo.Algorithm

	// Halt bounds the loop. Required, and validated at compile time: a loop
	// over paid, non-deterministic calls with no bound on it is not a
	// pipeline, and the compiler is the last place it can be stopped for free.
	Halt HaltWhen

	// Grow materializes a vertex that a message addressed but the graph does
	// not contain — the open-world case, where the computation discovers its
	// own inputs. Nil closes the world: such messages are dropped, counted,
	// and reported, rather than silently vanishing.
	//
	// This is the boundary worth thinking about twice. A vertex program that
	// follows a reference it invented is a program choosing its own next
	// input, and Grow is where that stops being the model's decision and
	// becomes the pipeline author's. Whatever it returns still runs under the
	// stage's envelope: the same model grants, the same deny-by-default egress
	// allowlist, the same budget. Discovery widens what the computation reads,
	// never what it is allowed to reach.
	Grow func(id string, msgs []algo.Message) (core.Record, error)

	// MaxInbox caps how many messages one vertex reads in a round, keeping the
	// highest-ranked (0 = uncapped).
	//
	// A high-degree vertex is where an iterative model workload dies: its
	// prompt grows with its degree until it exceeds the context window, and
	// the failure arrives as a provider error in round four rather than as a
	// planning problem in round zero. Capping is the blunt fix and it is
	// honest — truncation is reported per round. The sharp fix is to
	// tree-reduce the inbox before the vertex sees it, which is a ReduceAI
	// stage's job and makes a round two stages instead of one.
	MaxInbox int

	// MaxFrontier caps how many vertices run in one round, keeping those with
	// the highest-ranked inboxes (0 = uncapped).
	//
	// This is the per-round half of explosion control, and it is the half that
	// bounds the bill. A per-vertex message cap does not: a thousand vertices
	// each legally emitting two messages is a two-thousand-call round. This
	// caps the round itself, so the stage's worst case is
	// MaxFrontier × MaxRounds calls — a number that can be multiplied by a
	// price before anything runs, which is what loom.Explain reports.
	MaxFrontier int
}

// HaltWhen bounds an iterative stage. All three conditions apply at once, and
// that is the design rather than an abundance of options.
//
// Quiet alone does not terminate: models do not converge monotonically, and
// two vertices can trade messages indefinitely without either being wrong.
// A round cap alone does not bound cost: rounds are not the same size, and the
// expensive one is usually the last. A budget alone does not bound time.
// Each condition covers the others' blind spot, so the loop stops on whichever
// arrives first and the stage reports which one it was.
type HaltWhen struct {
	// MaxRounds is the hard cap on supersteps. Required and positive.
	MaxRounds int

	// Budget caps what this stage may spend across all its rounds. Checked
	// between rounds, so the overrun is bounded by one round's cost.
	//
	// It is separate from the run budget rather than a share of it: the run
	// governor stops the whole run and returns partial results, which is the
	// right response to a run that overspends and the wrong one to a loop that
	// simply did not converge. This stops the loop and lets the pipeline
	// continue with what the loop reached.
	Budget core.Budget
}

// Iterate applies a model operation repeatedly under an algorithm's control,
// emitting the final state of every record when the loop halts.
//
// Records are addressed by ID, which becomes load-bearing here in a way it is
// not elsewhere in a pipeline: a message names its destination, so duplicate
// IDs in the input are rejected rather than merged.
func (d Dataset) Iterate(name string, spec IterateSpec, opts ...Option) Dataset {
	return d.p.add(&Stage{
		ID: name, Kind: KindIterate, Upstream: d.stage,
		Iterate: &spec, Opts: applyOpts(opts),
	})
}
