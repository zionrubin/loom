package worker_test

import (
	"testing"
	"time"

	"github.com/zionrubin/loom/worker"
	"github.com/zionrubin/loom/worker/queuetest"
)

// The in-memory queue owes its callers exactly what a durable one does. Running
// the same suite against both is the only way that claim survives contact with
// a second implementation.
func TestConformance(t *testing.T) {
	queuetest.Run(t, func(t *testing.T, ttl time.Duration) worker.Queue {
		q := worker.NewMemQueue(worker.MemOptions{LeaseTTL: ttl})
		t.Cleanup(func() { _ = q.Close() })
		return q
	})
}
