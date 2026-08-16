package loom

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/zionrubin/loom/algo"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/store"
	"github.com/zionrubin/loom/task"
)

// HaltReason says which of an iterative stage's bounds stopped it. Every loop
// reports one, because "it finished" and "it ran out of money" are the same
// observation from the outside and must not be.
type HaltReason string

const (
	// HaltQuiet is convergence: a round produced no messages, so there is
	// nothing left to do. The only halt an algorithm controls.
	HaltQuiet HaltReason = "quiet"
	// HaltFixpoint is convergence detected one round earlier than quiet: every
	// vertex the round would have run had already run on exactly that state
	// and that inbox, so the round could only have reproduced itself.
	HaltFixpoint HaltReason = "fixpoint"
	// HaltRounds is the superstep cap. The loop did not converge.
	HaltRounds HaltReason = "rounds"
	// HaltBudget is the stage budget or the run governor. The loop did not
	// converge, and the records returned are the state it reached.
	HaltBudget HaltReason = "budget"
	// HaltFailed is a round that could not complete: a task failure the run
	// was not configured to continue through, or a cancelled context.
	HaltFailed HaltReason = "failed"
)

// Converged reports whether the loop stopped because it was finished, rather
// than because it hit a bound.
func (h HaltReason) Converged() bool { return h == HaltQuiet || h == HaltFixpoint }

// IterationReport is what an iterative stage reports about how it ran: the
// shape of the computation, not just its output.
//
// A loop's output alone cannot be read. The same hundred records come back
// whether the computation settled in three rounds or was cut off mid-argument
// by the round cap, and the difference is the entire question. These counters
// are the difference, and they are per round because the useful signal in an
// iterative workload is a slope: a frontier that shrinks is converging, one
// that holds steady is oscillating, one that grows is a message explosion that
// the caps are the only thing standing between you and.
type IterationReport struct {
	Stage     string
	Algorithm string
	// Rounds is how many supersteps actually ran.
	Rounds int
	Halt   HaltReason
	// Active is the number of vertices that ran in each round.
	Active []int
	// Delivered is the number of messages delivered into each round.
	Delivered []int
	// Quiesced counts vertices that were scheduled to run but had already run
	// on exactly that state and inbox — local fixpoints, skipped unpaid.
	Quiesced int
	// Grown counts vertices the computation discovered and created (Grow).
	Grown int
	// Dropped counts messages addressed to vertices that did not exist in a
	// stage with no Grow. Non-zero means the algorithm is reaching for an open
	// world inside a closed one.
	Dropped int
	// Truncated counts messages discarded by MaxInbox or MaxFrontier.
	Truncated int
	// Vertices is the size of the graph when the loop stopped.
	Vertices int
	// Usage is what the whole loop cost.
	Usage core.Usage
}

// String renders the per-round shape of the computation.
func (r IterationReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s  (%s, %d rounds, halted: %s)\n",
		r.Stage, r.Algorithm, r.Rounds, r.Halt)
	fmt.Fprintf(&b, "%-8s %8s %10s\n", "round", "active", "messages")
	for i, n := range r.Active {
		var delivered int
		if i < len(r.Delivered) {
			delivered = r.Delivered[i]
		}
		fmt.Fprintf(&b, "%-8d %8d %10d\n", i, n, delivered)
	}
	fmt.Fprintf(&b, "%d vertices, %d quiesced, %d grown, %d dropped, %d truncated\n",
		r.Vertices, r.Quiesced, r.Grown, r.Dropped, r.Truncated)
	fmt.Fprintf(&b, "%d tokens, $%.4f\n", r.Usage.TotalTokens(), r.Usage.CostUSD)
	return b.String()
}

// roundRunner executes one round's tasks. It is the seam the two drivers meet
// at: the barrier driver supplies its per-stage scheduler, the streaming
// driver supplies the shared engine, and the loop between them is one
// implementation. Rounds therefore behave identically under both drivers for
// the same reason ReduceAI's levels do — there is only one of them.
type roundRunner func(ctx context.Context, tasks []task.Task) ([]task.Result, error)

// vertexTable is the state an iterative stage owns: every vertex by ID, in a
// deterministic order. It implements algo.Graph as the read-only view an
// algorithm sees, which is the whole of the separation between them — the
// algorithm may read any vertex's current state and may write none of it.
type vertexTable struct {
	byID  map[string]core.Record
	order []string // input order first, then discovery order
}

func newVertexTable(input []core.Record) (*vertexTable, error) {
	t := &vertexTable{byID: make(map[string]core.Record, len(input))}
	for _, r := range input {
		if r.ID == "" {
			return nil, core.Permanent(fmt.Errorf(
				"iterate: a record has no ID, and messages are addressed by ID"))
		}
		if _, dup := t.byID[r.ID]; dup {
			return nil, core.Permanent(fmt.Errorf(
				"iterate: duplicate record ID %q; vertices are addressed by ID "+
					"and cannot be merged", r.ID))
		}
		for _, f := range algo.Reserved() {
			if _, taken := r.Data[f]; taken {
				return nil, core.Permanent(fmt.Errorf(
					"iterate: record %s already has field %q, which this stage "+
						"writes the inbox into (algo.Reserved)", r.ID, f))
			}
		}
		t.byID[r.ID] = r.Clone()
		t.order = append(t.order, r.ID)
	}
	return t, nil
}

func (t *vertexTable) Len() int { return len(t.order) }

// IDs implements algo.Graph, sorted so an algorithm that walks the graph is
// deterministic whatever order the vertices were created in.
func (t *vertexTable) IDs() []string {
	out := slices.Clone(t.order)
	sort.Strings(out)
	return out
}

// Vertex implements algo.Graph, returning a copy: an algorithm reads state, it
// does not hold a handle on it.
func (t *vertexTable) Vertex(id string) (core.Record, bool) {
	r, ok := t.byID[id]
	if !ok {
		return core.Record{}, false
	}
	return r.Clone(), true
}

func (t *vertexTable) put(r core.Record) {
	if _, exists := t.byID[r.ID]; !exists {
		t.order = append(t.order, r.ID)
	}
	t.byID[r.ID] = r
}

// records returns every vertex in creation order — input order first, so a
// stage that converges immediately returns exactly what it was given.
func (t *vertexTable) records() []core.Record {
	out := make([]core.Record, 0, len(t.order))
	for _, id := range t.order {
		out = append(out, t.byID[id])
	}
	return out
}

// active is one vertex scheduled to run in a round.
type active struct {
	id    string
	inbox []algo.Message
	rank  float64 // the best rank in the inbox, for frontier pruning
}

// iterate runs an iterative stage to a halt and returns the final state of
// every vertex.
//
// The loop itself holds no execution machinery. It decides who runs and what
// they read; running them is sp.BuildTasksBatch and the runner, which is the
// same path a plain Infer stage takes. That is the reason a round inherits
// admission control, class-aware retry, the escalation ladder, the governor,
// the cache, lineage and the event stream without any of them being taught
// what a round is.
func (d *driver) iterate(ctx context.Context, sp *plan.StagePlan,
	input []core.Record, run roundRunner) ([]core.Record, error) {

	s := sp.Stage
	spec := s.Iterate
	rep := IterationReport{
		Stage: s.ID, Algorithm: spec.Algorithm.Name(), Halt: HaltQuiet,
	}
	var tbl *vertexTable

	// finish reports the stage on every exit path — converged, exhausted, or
	// failed. All three return records, and the records do not say which
	// happened, so the reason has to be reported rather than inferred.
	finish := func(err error) ([]core.Record, error) {
		var out []core.Record
		if err != nil {
			rep.Halt = HaltFailed
		}
		if tbl != nil {
			rep.Vertices = tbl.Len()
			out = tbl.records()
		}
		d.iteration(rep)
		d.bus.Publish(observe.Event{
			Type: observe.StageConverged, RunID: d.runID, Stage: s.ID,
			Round: rep.Rounds, Records: rep.Vertices, Note: string(rep.Halt),
			Usage: rep.Usage,
		})
		return out, err
	}

	tbl, err := newVertexTable(input)
	if err != nil {
		tbl = nil // nothing to return: the vertex set itself was rejected
		return finish(err)
	}
	if tbl.Len() == 0 {
		return finish(nil)
	}

	msgs, err := spec.Algorithm.Seed(tbl)
	if err != nil {
		return finish(core.Permanent(fmt.Errorf("stage %q: seed: %w", s.ID, err)))
	}

	// ran records, per vertex, the (state, inbox) pairs it has already been
	// run on. A repeat means this vertex is at a local fixpoint: its program
	// is a function of exactly those two things, so running it again can only
	// reproduce the output it already produced and re-send the messages it
	// already sent. Skipping it is vote-to-halt with a witness, and it catches
	// oscillation of any period — a two-cycle repeats an input that comparing
	// only against the previous round would miss.
	ran := map[string]map[string]bool{}
	started := time.Now()

	for round := 0; ; round++ {
		// The three bounds are checked in the order that makes the reported
		// reason the true one: a loop that converged in its last permitted
		// round converged, it did not run out of rounds.
		if reason, stop := d.halted(spec, msgs, round, rep.Usage, started); stop {
			rep.Halt = reason
			break
		}

		frontier, delivered := d.deliver(spec, tbl, msgs, &rep)
		frontier = d.quiesce(frontier, tbl, ran, &rep)
		if len(frontier) == 0 {
			// Everything the round would have run had already run on exactly
			// this input. The next round could only repeat this one.
			rep.Halt = HaltFixpoint
			break
		}
		rep.Delivered = append(rep.Delivered, delivered)
		rep.Active = append(rep.Active, len(frontier))

		d.bus.Publish(observe.Event{
			Type: observe.RoundStarted, RunID: d.runID, Stage: s.ID,
			Round: round + 1, Records: len(frontier), Messages: delivered,
		})

		steps, usage, err := d.runRound(ctx, sp, frontier, tbl, run)
		rep.Usage.Add(usage)
		rep.Rounds = round + 1

		d.bus.Publish(observe.Event{
			Type: observe.RoundFinished, RunID: d.runID, Stage: s.ID,
			Round: round + 1, Records: len(steps), Usage: usage,
		})
		if err != nil {
			// The vertices that did complete keep their new state: a round that
			// failed part way is still work that was paid for.
			return finish(err)
		}

		msgs, err = spec.Algorithm.Route(algo.Round{N: round, Steps: steps, Graph: tbl})
		if err != nil {
			return finish(core.Permanent(
				fmt.Errorf("stage %q: route after round %d: %w", s.ID, round, err)))
		}
	}

	return finish(nil)
}

// halted applies the three bounds every iterative stage carries, and reports
// which one stopped the loop.
func (d *driver) halted(spec *pipeline.IterateSpec, msgs []algo.Message,
	round int, spent core.Usage, since time.Time) (HaltReason, bool) {

	switch {
	case len(msgs) == 0:
		return HaltQuiet, true
	case round >= spec.Halt.MaxRounds:
		return HaltRounds, true
	case budgetSpent(spec.Halt.Budget, spent, since):
		return HaltBudget, true
	case d.sched.Governor != nil && d.sched.Governor.Exhausted():
		// The run is over budget, not just this stage. Stopping the loop here
		// rather than letting the round fail its way out is what turns a
		// governor trip into partial results with a reason attached.
		return HaltBudget, true
	}
	return "", false
}

// deliver turns a round's messages into a frontier: group by destination,
// create or drop vertices nobody declared, cap each inbox, cap the round.
func (d *driver) deliver(spec *pipeline.IterateSpec, tbl *vertexTable,
	msgs []algo.Message, rep *IterationReport) ([]active, int) {

	grouped := algo.GroupByTo(msgs)
	dests := make([]string, 0, len(grouped))
	for id := range grouped {
		dests = append(dests, id)
	}
	sort.Strings(dests) // growth order must not depend on map iteration order

	var frontier []active
	var delivered int
	for _, id := range dests {
		inbox := grouped[id]
		if _, known := tbl.byID[id]; !known {
			if spec.Grow == nil {
				// A closed world reached for: counted and reported rather
				// than silently dropped, because a search that finds nothing
				// and a search that was not allowed to look are indis-
				// tinguishable from the output alone.
				rep.Dropped += len(inbox)
				continue
			}
			v, err := spec.Grow(id, inbox)
			if err != nil || v.ID == "" {
				rep.Dropped += len(inbox)
				continue
			}
			v.ID = id
			tbl.put(v)
			rep.Grown++
		}
		kept, dropped := algo.Cap(inbox, spec.MaxInbox)
		rep.Truncated += dropped
		algo.Sort(kept) // capping ranks; delivery order must be canonical

		// A message with no body is a wake-up, not information — it is how an
		// algorithm's Seed makes a vertex active without telling it anything.
		// It decides membership in the frontier and then stops existing, so a
		// seeded vertex reads an empty inbox rather than one blank line, and
		// {{if .Inbox}} means "somebody told me something".
		best := 0.0
		informative := kept[:0]
		for i, m := range kept {
			if i == 0 || m.Rank > best {
				best = m.Rank
			}
			if m.Body != "" {
				informative = append(informative, m)
			}
		}
		delivered += len(informative)
		frontier = append(frontier, active{id: id, inbox: informative, rank: best})
	}

	if n := spec.MaxFrontier; n > 0 && len(frontier) > n {
		sort.SliceStable(frontier, func(i, j int) bool {
			if frontier[i].rank != frontier[j].rank {
				return frontier[i].rank > frontier[j].rank
			}
			return frontier[i].id < frontier[j].id
		})
		for _, cut := range frontier[n:] {
			rep.Truncated += len(cut.inbox)
			delivered -= len(cut.inbox)
		}
		frontier = frontier[:n]
		sort.Slice(frontier, func(i, j int) bool { return frontier[i].id < frontier[j].id })
	}
	return frontier, delivered
}

// quiesce removes vertices that have already run on exactly this state and
// inbox, and records the pairs the survivors are about to run on.
func (d *driver) quiesce(frontier []active, tbl *vertexTable,
	ran map[string]map[string]bool, rep *IterationReport) []active {

	out := frontier[:0]
	for _, a := range frontier {
		v, ok := tbl.byID[a.id]
		if !ok {
			continue
		}
		key, err := store.Key(v.Data, a.inbox)
		if err != nil {
			// A vertex whose state will not serialize cannot be fingerprinted
			// or cached either; run it rather than silently halting it, and
			// let the executor produce the real error.
			out = append(out, a)
			continue
		}
		seen := ran[a.id]
		if seen == nil {
			seen = map[string]bool{}
			ran[a.id] = seen
		}
		if seen[key] {
			rep.Quiesced++
			continue
		}
		seen[key] = true
		out = append(out, a)
	}
	return out
}

// runRound builds and executes one superstep's tasks, then folds the results
// back into the vertex table.
func (d *driver) runRound(ctx context.Context, sp *plan.StagePlan, frontier []active,
	tbl *vertexTable, run roundRunner) ([]algo.Step, core.Usage, error) {

	before := make(map[string]core.Record, len(frontier))
	inboxes := make(map[string][]algo.Message, len(frontier))
	input := make([]core.Record, 0, len(frontier))
	for _, a := range frontier {
		v, ok := tbl.byID[a.id]
		if !ok {
			continue
		}
		before[a.id] = v.Clone()
		inboxes[a.id] = a.inbox

		r := v.Clone()
		r.Data[algo.FieldInbox] = algo.Bodies(a.inbox)
		r.Data[algo.FieldSenders] = algo.Senders(a.inbox)
		input = append(input, r)
	}

	// One vertex per task, whatever the stage's batch size. Batching would
	// coarsen the cache key to cover a group, and per-vertex keys are the
	// mechanism that makes a converging loop get cheaper — a batch of eight
	// re-pays for all eight when any one of them moves.
	tasks, err := sp.BuildTasksBatch(d.runID, input, 1, d.cfg.EgressAllow)
	if err != nil {
		return nil, core.Usage{}, err
	}
	results, err := run(ctx, tasks)

	var usage core.Usage
	steps := make([]algo.Step, 0, len(results))
	for _, res := range results {
		usage.Add(res.Usage)
		for _, out := range res.Output {
			v := out.Clone()
			// The inbox was this round's input, not part of the vertex's
			// state. Leaving it in would make the next round's cache key
			// carry the previous round's messages, and a vertex could never
			// reach a fixpoint it could recognize.
			for _, f := range algo.Reserved() {
				delete(v.Data, f)
			}
			tbl.put(v)
			steps = append(steps, algo.Step{
				Vertex: v, Before: before[v.ID], Inbox: inboxes[v.ID],
			})
		}
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i].ID() < steps[j].ID() })
	return steps, usage, err
}

// budgetSpent reports whether an iterative stage's own budget is used up.
//
// It is checked between rounds and it is separate from the run governor on
// purpose: the governor stopping means the run overspent and should return
// what it has, while this means one loop did not converge inside what it was
// given and the rest of the pipeline should carry on with what it reached.
func budgetSpent(b core.Budget, u core.Usage, since time.Time) bool {
	switch {
	case b.MaxCostUSD > 0 && u.CostUSD >= b.MaxCostUSD:
		return true
	case b.MaxTokens > 0 && u.TotalTokens() >= b.MaxTokens:
		return true
	case b.MaxDuration > 0 && time.Since(since) >= b.MaxDuration:
		return true
	}
	return false
}

// maxIterationReports bounds what a driver keeps. A run produces one report per
// iterative stage and never reaches it; a stream job produces one per pane, for
// as long as it runs, so the list is capped and the newest kept — a report from
// four hours ago is not what anyone is looking at.
const maxIterationReports = 64

// iteration records one stage's iteration report on the run.
func (d *driver) iteration(rep IterationReport) {
	d.mu.Lock()
	d.iterations = append(d.iterations, rep)
	if n := len(d.iterations) - maxIterationReports; n > 0 {
		d.iterations = append(d.iterations[:0], d.iterations[n:]...)
	}
	d.mu.Unlock()
}

// barrierRunner adapts the barrier driver's scheduler to a round: it executes
// a batch to completion and records dead letters on the run.
func (d *driver) barrierRunner(sched runtime.Scheduler) roundRunner {
	return func(ctx context.Context, tasks []task.Task) ([]task.Result, error) {
		results, fails, err := sched.ExecuteAll(ctx, tasks)
		d.fail(fails...)
		if err == nil && len(fails) > 0 && !d.cfg.ContinueOnError {
			err = fails[0].Err
		}
		return results, err
	}
}
