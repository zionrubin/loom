package quota

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
)

// This file is the half of a shared quota that costs something: admission.
//
// A shared bucket is a bucket behind a round trip, and the naive integration —
// one round trip per request — is wrong in exactly the way a chat client's MCP
// integration is wrong for a pipeline. A stage with a hundred concurrent tasks
// against one model would make a hundred calls to ask one question, and when
// the bucket is empty it would make them again on every retry, so the moment
// the fleet is actually contended is the moment coordination costs the most.
//
// So a process does not ask per request. One goroutine per model takes the
// round trip on behalf of every task waiting in this process, hands out what
// came back in arrival order, and waits once for the rest:
//
//	The lock is taken by one goroutine on behalf of every task waiting here,
//	so the cost of coordination per call falls as the fleet gets busier.
//
// What it does not do is hold quota. A leader draws exactly what its followers
// are waiting for and hands all of it out; nothing is reserved for later and
// nothing is lost when this process dies mid-round. That is what keeps the
// shared bucket exact rather than approximately fair, and it is why the only
// way to lose a draw is to be granted one and then not use it — which is a
// refund, and refunds are the one thing this file is allowed to be lazy about.

// pool is the queue of tasks waiting on one model in this process.
type pool struct {
	mu      sync.Mutex
	waiters []*waiter
	leading bool
	// lim is the model's limits as the last caller stated them. It is kept so
	// a refund can find the same bucket its draw came from even when nobody is
	// asking for admission any more.
	lim model.Limits
	// refunds are draws given back but not yet returned to the store. A leader
	// carries them on its next round, so a run that mostly replays from cache
	// pays no round trips of its own to say so.
	refunds []int
}

type waiter struct {
	tokens int
	done   chan error    // closed-with-value when admitted or failed
	lead   chan struct{} // closed when this waiter is asked to lead
	gone   bool          // abandoned: its ctx ended before it was served
	served bool
}

func (s *Shared) pool(modelID string, lim model.Limits) *pool {
	s.mu.Lock()
	p, ok := s.pools[modelID]
	if !ok {
		p = &pool{}
		s.pools[modelID] = p
	}
	s.mu.Unlock()

	p.mu.Lock()
	p.lim = lim
	p.mu.Unlock()
	return p
}

// Acquire blocks until one request of ~estTokens has been drawn from modelID's
// shared buckets, or ctx ends.
//
// It satisfies runtime.Buckets, and it is deliberately the same contract the
// in-process limiter offers: block until admission is possible. What changed is
// only who else is drawing on the bucket.
//
// A store that cannot be reached fails the acquisition as a *transient* error,
// which is the scheduler's cue to back off and try again rather than to
// dead-letter the task — and, if the outage lasts, to run out of attempts and
// stop the run. Admitting anyway would be the one direction this package is not
// allowed to be wrong in.
func (s *Shared) Acquire(ctx context.Context, modelID string, lim model.Limits, estTokens int) error {
	d := Draw{Model: modelID, Limits: lim}
	if !d.Metered() {
		return nil // unlimited is unlimited in every process
	}
	if estTokens <= 0 {
		estTokens = 1
	}
	p := s.pool(modelID, lim)
	w := &waiter{tokens: estTokens, done: make(chan error, 1), lead: make(chan struct{})}

	p.mu.Lock()
	p.waiters = append(p.waiters, w)
	lead := !p.leading
	if lead {
		p.leading = true
	}
	p.mu.Unlock()

	if lead {
		return s.drive(ctx, p, modelID, lim, w)
	}
	for {
		select {
		case err := <-w.done:
			return err
		case <-w.lead:
			return s.drive(ctx, p, modelID, lim, w)
		case <-ctx.Done():
			s.abandon(p, w)
			return core.Transient(ctx.Err())
		}
	}
}

// abandon takes a waiter out of the queue. A waiter the leader has already
// served has spent a draw that now belongs to nobody, so it goes back on the
// refund pile rather than being silently lost — the bucket errs low either way,
// but a fleet that leaked a draw on every cancelled task would err low
// permanently.
func (s *Shared) abandon(p *pool, w *waiter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w.gone = true
	for i, x := range p.waiters {
		if x == w {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			break
		}
	}
	if w.served {
		p.refunds = append(p.refunds, w.tokens)
	}
}

// drive runs the leader loop until this goroutine's own waiter is served, its
// context ends, or the store fails. Leadership then passes to the waiter at the
// front of the queue, so the process never has more than one round trip per
// model in flight and never has zero while anyone is waiting.
func (s *Shared) drive(ctx context.Context, p *pool, modelID string, lim model.Limits, self *waiter) error {
	defer s.handOff(p)

	started := time.Now()
	waited := false
	for {
		if err := ctx.Err(); err != nil {
			s.abandon(p, self)
			s.countWait(started, waited)
			return core.Transient(err)
		}
		batch, refunds := p.snapshot()
		if len(refunds) > 0 {
			s.flush(modelID, lim, refunds)
		}
		if len(batch) == 0 {
			// Everyone left while we were away. Nothing to ask about.
			return s.selfResult(ctx, self, started, waited)
		}

		tokens := make([]int, len(batch))
		for i, w := range batch {
			tokens[i] = w.tokens
		}
		// The round trip is made on everyone's behalf, so it does not inherit
		// the leader's deadline: one task giving up must not fail the
		// admission of the ninety-nine queued behind it. The leader's own
		// context is checked around the call instead.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.Timeout)
		g, err := s.store.Draw(rctx, Draw{Model: modelID, Limits: lim, Tokens: tokens})
		cancel()
		s.count(func(st *Stats) {
			st.Rounds++
			st.DrawRounds++
			if n := len(batch) - 1; n > 0 {
				st.Coalesced += n
			}
		})
		if err != nil {
			s.observe(Ledger{}, err)
			p.failAll(fmt.Errorf("quota: draw on %s: %w", modelID, err))
			return core.Transient(fmt.Errorf("quota: draw on %s: %w", modelID, err))
		}
		p.serve(batch, g.Admitted)
		s.count(func(st *Stats) { st.Admitted += g.Admitted })

		if self.served {
			return s.selfResult(ctx, self, started, waited)
		}
		// Somebody is still waiting, and this goroutine is one of them.
		waited = true
		wait := g.Wait
		if wait <= 0 {
			wait = 10 * time.Millisecond
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			s.abandon(p, self)
			s.countWait(started, waited)
			return core.Transient(ctx.Err())
		}
	}
}

// selfResult reports the leader's own admission and folds in what waiting for
// it cost.
func (s *Shared) selfResult(ctx context.Context, self *waiter, started time.Time, waited bool) error {
	s.countWait(started, waited)
	if self.served {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return core.Transient(err)
	}
	// The queue emptied without serving us, which can only happen if we were
	// abandoned — a case the caller already has an answer for.
	select {
	case err := <-self.done:
		return err
	default:
		return core.Transient(fmt.Errorf("quota: admission for %d token(s) was not served", self.tokens))
	}
}

func (s *Shared) countWait(started time.Time, waited bool) {
	if !waited {
		return
	}
	d := time.Since(started)
	s.count(func(st *Stats) { st.Waits++; st.Waited += d })
}

// handOff drops leadership and wakes the next waiter to take it.
func (s *Shared) handOff(p *pool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.leading = false
	for _, w := range p.waiters {
		if w.gone || w.served {
			continue
		}
		p.leading = true
		close(w.lead)
		return
	}
}

// snapshot takes the queue as it stands, plus any refunds waiting for a ride.
func (p *pool) snapshot() ([]*waiter, []int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	batch := make([]*waiter, 0, len(p.waiters))
	for _, w := range p.waiters {
		if !w.gone && !w.served {
			batch = append(batch, w)
		}
	}
	refunds := p.refunds
	p.refunds = nil
	return batch, refunds
}

// serve hands the first n admissions of a batch to the waiters that asked for
// them, in order, and takes them out of the queue.
func (p *pool) serve(batch []*waiter, n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 0; i < n && i < len(batch); i++ {
		w := batch[i]
		if w.gone {
			// Cancelled between the snapshot and the grant: the draw was made
			// and belongs to nobody, so it goes back.
			p.refunds = append(p.refunds, w.tokens)
			continue
		}
		w.served = true
		w.done <- nil
		p.remove(w)
	}
}

// failAll ends every waiting acquisition with the same error. A store nobody
// can reach is unreachable for all of them, and leaving the rest to discover it
// one round trip at a time would turn one outage into a queue of them.
func (p *pool) failAll(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.waiters {
		if w.gone || w.served {
			continue
		}
		select {
		case w.done <- core.Transient(err):
		default:
		}
	}
	p.waiters = p.waiters[:0]
}

// remove takes w out of the queue. Callers hold p.mu.
func (p *pool) remove(w *waiter) {
	for i, x := range p.waiters {
		if x == w {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			return
		}
	}
}

// Refund gives back a draw whose request was never issued: a task settled from
// the result cache, or one coalesced onto an identical task already in flight.
//
// It satisfies runtime.Buckets. Refunds are batched onto the next leader's
// round when there is a leader, because a run that mostly replays from cache
// would otherwise pay a round trip per task to say it spent nothing. When there
// is nobody to carry them, the refunding goroutine takes them itself — off the
// hot path, since the task it belongs to has already settled.
func (s *Shared) Refund(modelID string, lim model.Limits, estTokens int) {
	d := Draw{Model: modelID, Limits: lim}
	if !d.Metered() {
		return
	}
	if estTokens <= 0 {
		estTokens = 1
	}
	p := s.pool(modelID, lim)
	p.mu.Lock()
	p.refunds = append(p.refunds, estTokens)
	carried := p.leading
	var mine []int
	if !carried {
		mine, p.refunds = p.refunds, nil
	}
	p.mu.Unlock()
	if len(mine) > 0 {
		s.flush(modelID, lim, mine)
	}
}

// flush returns draws to the store. A failed return is dropped rather than
// retried: the bucket is then lower than it should be, which costs the fleet
// throughput until it refills and can never let it exceed the limit. Retrying
// into an unreachable store from the path that settles a task would trade that
// for a stall, which is the worse of the two.
func (s *Shared) flush(modelID string, lim model.Limits, tokens []int) {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	defer cancel()
	err := s.store.Return(ctx, Draw{Model: modelID, Limits: lim, Tokens: tokens})
	s.count(func(st *Stats) {
		st.Rounds++
		if err == nil {
			st.Refunded += len(tokens)
		}
	})
	if err != nil {
		s.observe(Ledger{}, err)
	}
}

// Flush returns any refunds still waiting for a ride. A process shutting down
// calls it so the last replayed tasks give their admission back to the fleet
// rather than to the refill rate.
func (s *Shared) Flush() {
	s.mu.Lock()
	pools := make(map[string]*pool, len(s.pools))
	for k, v := range s.pools {
		pools[k] = v
	}
	s.mu.Unlock()
	for id, p := range pools {
		p.mu.Lock()
		pending, lim := p.refunds, p.lim
		p.refunds = nil
		p.mu.Unlock()
		if len(pending) > 0 {
			s.flush(id, lim, pending)
		}
	}
}
