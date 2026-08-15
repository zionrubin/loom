package loom

import (
	"context"
	"errors"
	"fmt"

	"github.com/zionrubin/loom/delta"
	"github.com/zionrubin/loom/executor"
	"github.com/zionrubin/loom/ops"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/plan"
	"github.com/zionrubin/loom/runtime"
	"github.com/zionrubin/loom/worker"
)

// Serve runs this process as a worker for p, claiming tasks from the queue
// until ctx ends.
//
// It is the other half of WithWorkerService, and it takes the same options for
// a reason worth stating plainly: an op is code. A model call can be described
// in an envelope and shipped anywhere, but the Go function inside a Map, the
// validator inside an Infer and the reducer inside a ReduceAI cannot be — so a
// worker does not receive a pipeline, it *is* one. Handing both sides the same
// pipeline and the same options is what makes "the same pipeline runs unchanged
// across several worker processes" true rather than aspirational: the two
// processes compile the same plan, register the same tools, connect to the same
// servers, and agree on every stage fingerprint without exchanging any of it.
//
//	func main() {
//	    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
//	    defer stop()
//	    q, _ := filequeue.Open(*dir, filequeue.Options{})
//	    log.Fatal(loom.Serve(ctx, buildPipeline(), opts(q)...))
//	}
//
// A stopped worker finishes what it is holding before returning; a killed one
// loses its leases, which the queue redelivers. Both are safe. The difference
// is the tasks in flight.
func Serve(ctx context.Context, p *pipeline.Pipeline, opts ...Option) error {
	w, err := NewWorker(p, opts...)
	if err != nil {
		return err
	}
	defer w.Close()
	return w.Run(ctx)
}

// Worker is one process serving a pipeline's tasks to a fleet. Build it with
// NewWorker when the process needs to do something between provisioning and
// serving — announce itself, report what it advertises, wait on a signal —
// and use Serve when it does not.
//
// It is not to be confused with Config.Workers, which is a count of concurrent
// tasks. This is the process; that is how many things it does at once.
type Worker struct {
	host *host
	w    *worker.Worker
	caps worker.Capabilities
}

// NewWorker provisions a worker for p without starting it.
func NewWorker(p *pipeline.Pipeline, opts ...Option) (*Worker, error) {
	cfg := Config{Workers: 8, Retry: runtime.DefaultRetry}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.Queue == nil {
		return nil, errors.New("loom: a worker needs a queue (loom.WithWorkerService)")
	}
	h, err := newHost(cfg)
	if err != nil {
		return nil, err
	}
	// From here on the host's config is the one that counts: newHost fills in
	// what was left unset — a registry, above all — and compiling against the
	// caller's copy would compile against the holes.
	cfg = h.cfg

	// The plan is compiled here for exactly one reason: its runners. A worker
	// never schedules, never fans out and never reads the DAG — it needs the
	// map from stage ID to the code that runs it, and compiling the same
	// pipeline the client compiled is how it gets one that agrees.
	pl, err := plan.Compile(p, cfg.Registry,
		plan.WithBroadcasts(h.shared.Hashes()), plan.WithContinuations(declared(p, cfg)),
		plan.WithMCP(h.manifest))
	if err != nil {
		_ = h.close()
		return nil, err
	}
	runners, err := ops.BuildRunners(pl)
	if err != nil {
		_ = h.close()
		return nil, err
	}

	local := &executor.Local{
		Runners: runners, Client: h.client, Tools: h.tools,
		Broadcasts: h.shared, State: h.state,
		Audit: h.audit, Cache: h.cache, Lineage: h.lineage, Bus: h.bus,
	}
	caps := worker.CapabilitiesFor(local, cfg.Registry, cfg.Workers)
	if cfg.WorkerName != "" {
		caps.Worker = cfg.WorkerName
	}
	// MCP servers are advertised by name, because a task naming one is a task
	// that will call it: a worker without the connection would claim the work
	// and then fail it.
	for name := range h.manifest {
		caps.MCP = append(caps.MCP, name)
	}

	wk, err := worker.New(worker.Config{
		Queue: cfg.Queue, Blobs: h.cas, Exec: local, Caps: caps, Bus: h.bus,
		Name: cfg.WorkerName, LeaseTTL: cfg.WorkerLease,
		// What this worker holds, asked fresh on every claim. It is the only
		// thing the queue is told that is not a capability, and the only thing
		// it may act on without being obliged to.
		Locality: h.state.Resident,
	})
	if err != nil {
		_ = h.close()
		return nil, err
	}
	return &Worker{host: h, w: wk, caps: wk.Capabilities()}, nil
}

// declared is the continuation set a worker compiles against: whatever the run
// bound, plus the keys the pipeline itself names.
//
// A worker is sent revisions; it never picks one. Requiring it to name one at
// startup would be requiring it to hold a fact that is stale by the first
// round, and reading the keys off the pipeline it *is* costs nothing and cannot
// disagree with what it will be asked to run. The client keeps its typo check,
// because the client is the side that binds.
func declared(p *pipeline.Pipeline, cfg Config) map[string]delta.Ref {
	out := make(map[string]delta.Ref, len(cfg.Continuations))
	for k, v := range cfg.Continuations {
		out[k] = v
	}
	for _, s := range p.Stages() {
		if k := s.Opts.Continuation; k != "" {
			if _, ok := out[k]; !ok {
				out[k] = delta.Ref{Key: k}
			}
		}
	}
	return out
}

// Run claims and executes tasks until ctx ends.
func (w *Worker) Run(ctx context.Context) error { return w.w.Run(ctx) }

// Name returns this worker's identity in the fleet — the lease owner recorded
// against every task it claims.
func (w *Worker) Name() string { return w.w.Name() }

// Advertises reports what this worker told the queue it can do.
func (w *Worker) Advertises() worker.Capabilities { return w.caps }

// Stats reports what this worker has done: tasks claimed and completed,
// redeliveries it picked up, leases it lost, and duplicates it discovered it
// had executed.
func (w *Worker) Stats() worker.WorkerStats { return w.w.Stats() }

// Holding lists the leases this worker holds right now — what the fleet would
// redeliver if the process died at this instant.
func (w *Worker) Holding() []worker.Lease { return w.w.Holding() }

// Close releases the host: MCP connections, the cache index, the commons.
func (w *Worker) Close() error {
	if err := w.host.close(); err != nil {
		return fmt.Errorf("loom: closing worker %s: %w", w.Name(), err)
	}
	return nil
}
