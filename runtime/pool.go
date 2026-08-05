package runtime

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/zionrubin/loom/core"
)

// DefaultAging is how fast a queued task earns priority credit for waiting:
// a task that has waited d earns d of credit against its program's attained
// service. It is the guarantee that a program which has already been served a
// great deal still makes progress rather than starving behind lighter ones.
const DefaultAging = 1.0

// Pool is the fixed set of execution slots concurrent programs share, and the
// policy that decides whose task gets the next free one.
//
// A single pipeline needs no such policy: its tasks are interchangeable, so
// first-come-first-served is both fair and optimal. A *fleet* is different.
// Several pipelines run at once against one provider quota, and they are not
// interchangeable — one may be a 10,000-record sweep and the next a three-call
// summary that a person is waiting on. Served first-come-first-served, the
// summary queues behind however much of the sweep arrived first, and its
// completion time is set by the sweep's size rather than its own. That is
// head-of-line blocking at the level of the program, and it is the failure the
// agentic-serving literature identifies as the one that matters: what a caller
// experiences is not the latency of a single call but the completion time of
// the whole program.
//
// So a contended slot goes to the waiting task whose *program* has been served
// least. Service is measured in slot-time, including the work a program has in
// flight right now — without that, a program holding every slot would still
// look idle to the next admission decision, which is precisely the case the
// policy exists to handle. A short program therefore overtakes a long one
// almost immediately, and a long one is never held back by more than the
// slots its rivals are entitled to.
//
// Least-attained-service is deliberately not the same as an equal share of
// what remains. A program arriving with nothing attained outranks everything
// until it catches up, which is what lets a three-call agent overtake a
// 10,000-record one rather than merely draw level with it. When service times
// vary as widely as they do here, least-served-first is the best available
// guess at shortest-remaining-first, and shortest-remaining-first is what
// minimises mean completion time.
//
// The cost of that policy is the incumbent: served heavily, it would yield to
// every newcomer forever. So a queued task earns priority credit at Aging
// times the time it has waited, which bounds the wait — a program is held back
// by at most its own attained service divided by the aging rate, whatever
// arrives while it waits. Aging is the knob; a fleet whose agents keep
// arriving indefinitely wants it higher than one whose agents are launched
// together and drain.
//
// With a single program both rules are inert and every waiter ties, so
// admission falls back to arrival order: a pool serving one pipeline behaves
// exactly as the FIFO slot pool it replaced.
type Pool struct {
	mu      sync.Mutex
	total   int
	free    []string
	waiting []*waiter
	active  map[*lease]struct{}

	// service is per-program attained service: slot-time already returned to
	// the pool. Work still in flight is added on demand by serviced.
	service map[string]time.Duration
	aging   float64
	seq     uint64

	admitted int
	stats    map[string]*ProgramStats
	order    []string
}

// lease is one program's claim on one slot for the duration of one task.
type lease struct {
	slot    string
	program string
	start   time.Time
}

// waiter is a task queued for admission.
type waiter struct {
	program string
	seq     uint64
	queued  time.Time
	grant   chan *lease
	granted bool
}

// Lease is an admitted claim on an execution slot. Release returns the slot to
// the pool and charges the elapsed time to the lease's program; it is
// idempotent, so releasing twice is harmless.
type Lease struct {
	pool  *Pool
	lease *lease
}

// Slot returns the name of the granted execution slot, the identity that
// travels with the task so occupancy is observable.
func (l Lease) Slot() string {
	if l.lease == nil {
		return ""
	}
	return l.lease.slot
}

// Program returns the program the lease was granted to.
func (l Lease) Program() string {
	if l.lease == nil {
		return ""
	}
	return l.lease.program
}

// Release hands the slot back, charging its occupancy to the program.
func (l Lease) Release() {
	if l.pool != nil && l.lease != nil {
		l.pool.release(l.lease)
	}
}

// NewPool returns a pool of n execution slots with the default aging rate.
func NewPool(n int) *Pool {
	if n <= 0 {
		n = 8
	}
	p := &Pool{
		total:   n,
		active:  map[*lease]struct{}{},
		service: map[string]time.Duration{},
		aging:   DefaultAging,
		stats:   map[string]*ProgramStats{},
	}
	for i := 1; i <= n; i++ {
		p.free = append(p.free, fmt.Sprintf("e%d", i))
	}
	return p
}

// Aging sets how fast queued tasks earn priority credit for waiting and
// returns the pool. Zero disables aging, leaving pure attained-service order.
func (p *Pool) Aging(rate float64) *Pool {
	p.mu.Lock()
	p.aging = rate
	p.mu.Unlock()
	return p
}

// Slots returns the pool's concurrency ceiling.
func (p *Pool) Slots() int { return p.total }

// Acquire blocks until a slot is assigned to program and returns the lease
// governing it. The wait is the backpressure that keeps in-flight work
// bounded; the ordering of waits is the fairness policy above.
func (p *Pool) Acquire(ctx context.Context, program string) (Lease, error) {
	if program == "" {
		program = "-"
	}
	p.mu.Lock()
	p.enterLocked(program)

	// Uncontended: nothing is queued, so there is no fairness question to
	// answer. Take a slot and go.
	if len(p.waiting) == 0 && len(p.free) > 0 {
		slot := p.takeFreeLocked()
		l := p.startLocked(slot, program)
		p.mu.Unlock()
		return Lease{pool: p, lease: l}, nil
	}

	p.seq++
	w := &waiter{program: program, seq: p.seq, queued: time.Now(), grant: make(chan *lease, 1)}
	p.waiting = append(p.waiting, w)
	p.mu.Unlock()

	select {
	case l := <-w.grant:
		return Lease{pool: p, lease: l}, nil
	case <-ctx.Done():
		p.mu.Lock()
		granted := w.granted
		if !granted {
			p.removeLocked(w)
		}
		p.mu.Unlock()
		if granted {
			// A slot was handed over as we gave up on it. Releasing the lease
			// passes it straight to the next waiter instead of losing a slot
			// for the rest of the pool's life.
			Lease{pool: p, lease: <-w.grant}.Release()
		}
		return Lease{}, core.Transient(ctx.Err())
	}
}

// enterLocked registers a program's first appearance. It starts with nothing
// attained, which is the whole overtaking mechanism: a program that has not
// been served yet outranks one that has.
func (p *Pool) enterLocked(program string) {
	if _, ok := p.service[program]; ok {
		return
	}
	p.service[program] = 0
	p.stats[program] = &ProgramStats{Program: program}
	p.order = append(p.order, program)
}

// servicedLocked is a program's attained service: slot-time already returned
// plus the elapsed time of everything it currently holds. Counting in-flight
// work is what makes the policy work at all — a program occupying every slot
// has attained a great deal of service and returned none of it.
func (p *Pool) servicedLocked(program string, now time.Time) time.Duration {
	d := p.service[program]
	for l := range p.active {
		if l.program == program {
			d += now.Sub(l.start)
		}
	}
	return d
}

// bestLocked picks the waiter to admit next: least attained service, credited
// for time spent waiting, with arrival order breaking ties.
func (p *Pool) bestLocked(now time.Time) *waiter {
	var best *waiter
	var bestPriority time.Duration
	serviced := make(map[string]time.Duration, len(p.waiting))
	for _, w := range p.waiting {
		s, ok := serviced[w.program]
		if !ok {
			s = p.servicedLocked(w.program, now)
			serviced[w.program] = s
		}
		priority := s
		if p.aging > 0 {
			priority -= time.Duration(float64(now.Sub(w.queued)) * p.aging)
		}
		if best == nil || priority < bestPriority ||
			(priority == bestPriority && w.seq < best.seq) {
			best, bestPriority = w, priority
		}
	}
	return best
}

// dispatchLocked hands slot to the best waiter, or parks it as free.
func (p *Pool) dispatchLocked(slot string) {
	now := time.Now()
	w := p.bestLocked(now)
	if w == nil {
		p.free = append(p.free, slot)
		return
	}
	p.removeLocked(w)
	w.granted = true
	l := p.startLocked(slot, w.program)
	st := p.stats[w.program]
	waited := now.Sub(w.queued)
	st.Wait += waited
	if waited > st.MaxWait {
		st.MaxWait = waited
	}
	w.grant <- l // buffered: never blocks the releasing goroutine
}

func (p *Pool) startLocked(slot, program string) *lease {
	l := &lease{slot: slot, program: program, start: time.Now()}
	p.active[l] = struct{}{}
	p.admitted++
	p.stats[program].Admitted++
	return l
}

// release charges the lease's occupancy to its program and passes the slot on.
func (p *Pool) release(l *lease) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.active[l]; !ok {
		return // already released
	}
	delete(p.active, l)
	d := time.Since(l.start)
	p.service[l.program] += d
	p.stats[l.program].Service += d
	p.dispatchLocked(l.slot)
}

func (p *Pool) takeFreeLocked() string {
	slot := p.free[0]
	p.free = p.free[1:]
	return slot
}

func (p *Pool) removeLocked(w *waiter) {
	for i, q := range p.waiting {
		if q == w {
			p.waiting = append(p.waiting[:i], p.waiting[i+1:]...)
			return
		}
	}
}

// ProgramStats is one program's history with the pool: how many tasks it had
// admitted, how much slot-time they occupied, and how long they queued.
type ProgramStats struct {
	Program  string
	Admitted int
	Service  time.Duration
	Wait     time.Duration
	MaxWait  time.Duration
}

// PoolStats is a snapshot of a pool's occupancy and per-program fairness.
type PoolStats struct {
	Slots    int
	Admitted int
	Waiting  int
	InFlight int
	Service  time.Duration // slot-time across every program
	Programs []ProgramStats
}

// Stats snapshots the pool. Service counts only completed occupancy, so it is
// a lower bound while tasks are running.
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := PoolStats{
		Slots: p.total, Admitted: p.admitted,
		Waiting: len(p.waiting), InFlight: len(p.active),
	}
	for _, name := range p.order {
		st := *p.stats[name]
		s.Service += st.Service
		s.Programs = append(s.Programs, st)
	}
	sort.SliceStable(s.Programs, func(i, j int) bool {
		return s.Programs[i].Service > s.Programs[j].Service
	})
	return s
}
