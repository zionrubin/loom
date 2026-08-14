package worker

import (
	"encoding/json"
	"fmt"

	"github.com/zionrubin/loom/core"
)

// Blobs is the shared content-addressed storage both sides of the queue reach:
// where task inputs are detached to, where broadcast values already live, and
// where outputs are written before a receipt names them.
//
// It is the two methods store.CAS already has, named as an interface so this
// package does not depend on where the bytes physically are. That matters
// sooner here than anywhere else in Loom: a fleet on one host shares a
// directory, and a fleet on many shares object storage, and the difference
// should be a constructor argument rather than a change to the worker. The CAS
// maps onto S3 or GCS with the same hash keys, which is the whole of the
// migration.
//
// Two properties make content addressing the right shape for a queue rather
// than merely a convenient one:
//
//   - a write is idempotent by construction. Two workers executing one task
//     serialize the same records and store them at the same address, so
//     duplicate execution costs the model call twice and the storage once,
//     with no conflict to resolve and no last-writer-wins to reason about;
//   - a hash is a name any process can resolve. A task references its input,
//     its broadcasts and its output by hash, so the queue carries names while
//     the payloads are fetched once by whoever needs them.
type Blobs interface {
	// Put stores data and returns its content hash.
	Put(data []byte) (string, error)
	// Get returns the blob stored under hash.
	Get(hash string) ([]byte, bool)
}

// putRecords stores records in shared storage and returns their content hash.
//
// The bytes are canonical JSON of the record slice — the same encoding
// store.Cache uses for a cached result — so a task whose output is also cached
// lands on one blob rather than two copies of the same records at different
// addresses.
func putRecords(b Blobs, recs []core.Record) (string, error) {
	blob, err := json.Marshal(recs)
	if err != nil {
		return "", fmt.Errorf("worker: records must be JSON-serializable: %w", err)
	}
	hash, err := b.Put(blob)
	if err != nil {
		return "", fmt.Errorf("worker: shared storage: %w", err)
	}
	return hash, nil
}

// getRecords resolves a content hash back into records.
//
// A miss is the one failure in this package that means the deployment is
// wrong rather than that something died: it says the two sides of the queue
// are not looking at the same storage, which no amount of retrying fixes. The
// error says so, because the alternative — a worker silently running a task
// with no input — would be a correctness bug wearing an empty result.
func getRecords(b Blobs, hash string) ([]core.Record, error) {
	blob, ok := b.Get(hash)
	if !ok {
		return nil, fmt.Errorf("worker: blob %s not found in shared storage "+
			"(client and worker must share a content-addressed store)", hash)
	}
	var recs []core.Record
	if err := json.Unmarshal(blob, &recs); err != nil {
		return nil, fmt.Errorf("worker: blob %s: %w", hash, err)
	}
	return recs, nil
}
