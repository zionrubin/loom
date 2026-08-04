package runtime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/runtime"
)

// waitForQueue spins until the pool reports n queued waiters. Acquire blocks,
// so a test that wants a known queue shape has to observe it rather than sleep
// for it.
func waitForQueue(t *testing.T, p *runtime.Pool, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Stats().Waiting == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue never reached %d waiters (stuck at %d)", n, p.Stats().Waiting)
}

// queue enqueues one Acquire for program and reports admissions on order.
func queue(ctx context.Context, p *runtime.Pool, program string,
	order chan<- string, wg *sync.WaitGroup) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		lease, err := p.Acquire(ctx, program)
		if err != nil {
			return
		}
		order <- program
		lease.Release()
	}()
}

// A short program queued behind a long one must not wait for the long one to
// drain. This is the whole point of admitting by attained service: the second
// program's completion time should be set by its own size, not by whatever
// arrived first.
func TestPoolAdmitsTheLeastServedProgramFirst(t *testing.T) {
	ctx := context.Background()
	p := runtime.NewPool(1) // one slot makes admission order fully determined

	// "sweep" takes the slot and holds it long enough to attain real service.
	held, err := p.Acquire(ctx, "sweep")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	order := make(chan string, 8)

	// Four more of the sweep's tasks queue up first...
	for i := 0; i < 4; i++ {
		queue(ctx, p, "sweep", order, &wg)
	}
	waitForQueue(t, p, 4)

	// ...and only then does a short program arrive.
	queue(ctx, p, "summary", order, &wg)
	waitForQueue(t, p, 5)

	held.Release()
	wg.Wait()
	close(order)

	var admitted []string
	for program := range order {
		admitted = append(admitted, program)
	}
	if len(admitted) != 5 {
		t.Fatalf("expected 5 admissions, got %d: %v", len(admitted), admitted)
	}
	if admitted[0] != "summary" {
		t.Errorf("expected the newly arrived short program to be admitted first, "+
			"got %v — arrival order won, so the policy is not applying", admitted)
	}
}

// With one program there is no fairness question, and the pool must behave
// exactly as the FIFO channel of slots it replaced.
func TestPoolIsFIFOWithinAProgram(t *testing.T) {
	ctx := context.Background()
	p := runtime.NewPool(1)

	held, err := p.Acquire(ctx, "solo")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	const n = 6
	order := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := p.Acquire(ctx, "solo")
			if err != nil {
				return
			}
			order <- i
			lease.Release()
		}()
		// Enqueue one at a time so arrival order is the order of the loop.
		waitForQueue(t, p, i+1)
	}

	held.Release()
	wg.Wait()
	close(order)

	want := 0
	for got := range order {
		if got != want {
			t.Fatalf("admission %d went to waiter %d: a single program must be served FIFO", want, got)
		}
		want++
	}
	if want != n {
		t.Fatalf("expected %d admissions, got %d", n, want)
	}
}

// Aging is what bounds how long a heavily served program can be held back by
// programs that keep arriving with nothing attained. The two cases below are
// the same scenario with aging on and off, which is what shows it is the aging
// rule doing the work and not something else.
//
// Note that the incumbent has to have *released* its work to benefit: a
// program still occupying a slot keeps attaining service at the same rate its
// own waiter ages, so aging cannot help a program jump a queue it is already
// being served ahead of. That is the intended reading of the rule.
func TestPoolAgingRescuesAServedProgram(t *testing.T) {
	for _, tc := range []struct {
		name  string
		aging float64
		want  string
	}{
		{"aging on", runtime.DefaultAging, "incumbent"},
		{"aging off", 0, "newcomer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			p := runtime.NewPool(1).Aging(tc.aging)

			// The incumbent attains 40ms of service and gives the slot back.
			served, err := p.Acquire(ctx, "incumbent")
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}
			time.Sleep(40 * time.Millisecond)
			served.Release()

			// A third program holds the only slot, so both contenders queue.
			blocker, err := p.Acquire(ctx, "blocker")
			if err != nil {
				t.Fatalf("acquire: %v", err)
			}

			var wg sync.WaitGroup
			order := make(chan string, 4)
			queue(ctx, p, "incumbent", order, &wg)
			waitForQueue(t, p, 1)

			// Let the incumbent's waiter age past the 40ms it attained, then
			// bring in a newcomer that has attained nothing.
			time.Sleep(70 * time.Millisecond)
			queue(ctx, p, "newcomer", order, &wg)
			waitForQueue(t, p, 2)

			blocker.Release()
			wg.Wait()
			close(order)

			if first := <-order; first != tc.want {
				t.Errorf("admitted %q first, want %q", first, tc.want)
			}
		})
	}
}

// Cancelling an Acquire that is racing a hand-off must never cost the pool a
// slot. Either the cancel wins and the waiter leaves the queue, or the
// hand-off wins and the waiter has to give the slot straight back — and the
// only way to exercise the second path is to run the race repeatedly.
func TestPoolCancelledAcquireNeverLeaksASlot(t *testing.T) {
	for i := 0; i < 300; i++ {
		p := runtime.NewPool(1)
		held, err := p.Acquire(context.Background(), "a")
		if err != nil {
			t.Fatalf("iteration %d: acquire: %v", i, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := p.Acquire(ctx, "b")
			switch {
			case err == nil:
				lease.Release() // the hand-off won the race
			case !errors.Is(err, context.Canceled):
				t.Errorf("iteration %d: acquire returned %v, want context.Canceled", i, err)
			}
		}()
		waitForQueue(t, p, 1)

		go cancel()
		held.Release()
		wg.Wait()

		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		lease, err := p.Acquire(ctx2, "c")
		if err != nil {
			cancel2()
			t.Fatalf("iteration %d: the pool lost a slot to a cancelled acquire: %v", i, err)
		}
		lease.Release()
		cancel2()
	}
}

// Stats must attribute occupancy to the program that caused it.
func TestPoolStatsAttributeServicePerProgram(t *testing.T) {
	ctx := context.Background()
	p := runtime.NewPool(2)

	long, _ := p.Acquire(ctx, "long")
	short, _ := p.Acquire(ctx, "short")
	time.Sleep(20 * time.Millisecond)
	short.Release()
	time.Sleep(30 * time.Millisecond)
	long.Release()

	stats := p.Stats()
	if stats.Slots != 2 {
		t.Errorf("Slots = %d, want 2", stats.Slots)
	}
	if stats.Admitted != 2 {
		t.Errorf("Admitted = %d, want 2", stats.Admitted)
	}
	if len(stats.Programs) != 2 {
		t.Fatalf("expected 2 programs, got %d", len(stats.Programs))
	}
	// Sorted by service, descending.
	if stats.Programs[0].Program != "long" {
		t.Errorf("expected %q to have attained the most service, got %q",
			"long", stats.Programs[0].Program)
	}
	if stats.Programs[0].Service <= stats.Programs[1].Service {
		t.Errorf("service not attributed: long=%s short=%s",
			stats.Programs[0].Service, stats.Programs[1].Service)
	}
	if stats.InFlight != 0 {
		t.Errorf("InFlight = %d after every release, want 0", stats.InFlight)
	}
}

// Releasing twice must not conjure an extra slot.
func TestPoolReleaseIsIdempotent(t *testing.T) {
	p := runtime.NewPool(1)
	lease, err := p.Acquire(context.Background(), "a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	lease.Release()
	lease.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	first, err := p.Acquire(ctx, "b")
	if err != nil {
		t.Fatalf("acquire after double release: %v", err)
	}
	defer first.Release()
	if _, err := p.Acquire(ctx, "c"); err == nil {
		t.Fatal("a double release handed out a second slot: the ceiling is not bounded")
	}
}
