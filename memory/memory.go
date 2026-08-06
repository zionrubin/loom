// Package memory is Loom's long-term, cross-run knowledge base: a durable
// store of facts that outlives the run that wrote them, is retrieved by
// meaning rather than by name, and is shared between the pipelines of a
// long-running application.
//
// Loom already had two ways to share state, and neither is long-term. A
// broadcast (store.Broadcasts) is registered before a run and read whole by
// the tasks that declare it; it dies with the run. A blackboard topic
// (loom.Fleet) is appended to by one agent and read whole by the next; it dies
// with the process. Both are addressed by name, which means the reader has to
// know what it wants before it asks, and both are small enough to fit in a
// prompt, which a knowledge base accumulated over months is not.
//
// # The tension with content-addressed replay
//
// Loom's cache is also its checkpoint: a task's result is keyed by
// (op fingerprint, input content), so replaying it must be indistinguishable
// from re-running it. Broadcasts are safe to combine with that because they
// are immutable for the run's lifetime and their content hash joins the
// reading stage's fingerprint. A knowledge base breaks both properties — it is
// mutable by construction, and it is far too large to hash into a fingerprint
// on every read.
//
// Two mechanisms restore them.
//
// # Epochs
//
// A Space carries a monotonic epoch. A run pins an epoch before its first task
// and every read is served as of that epoch, so a commit landing mid-run is
// invisible to it however long it takes. Writes never land in the epoch that
// made them: Upsert stages an item and Commit publishes the staged set as a new
// epoch. This is the blackboard's rule — publish between units of work, not
// inside one — generalized from a process to a durable store, and it is what
// keeps a cached result from depending on when during the run it happened to
// execute.
//
// # Recall-keyed caching
//
// Pinning alone would be correct and useless: the pinned epoch joins the
// fingerprint of every stage that reads the space, so a single commit would
// cold-start every reading stage in the application. A knowledge base that
// grows daily would never see a cache hit again.
//
// So retrieval is its own operation. A Recall stage turns a query into the
// top-k items and writes their IDs into the record; the downstream stage is
// then keyed by its own fingerprint plus record content, and that content now
// carries the recalled IDs. The epoch invalidates only the recall — which is
// one embedding and one index lookup — and the expensive inference below it
// recomputes only for the records whose recalled set actually changed. Commit
// ten thousand items and the queries whose neighbourhoods did not move replay
// for free.
//
// That is record-granular invalidation, and it is not new machinery: it is the
// existing content-addressed key doing its job on a record that now says what
// it read.
//
// # Provenance
//
// Every item records the run, stage, task, and model that produced it. A
// knowledge base whose entries are model outputs is otherwise a laundering
// channel for hallucination: the second run cannot tell a fact it retrieved
// from a fact it invented. Recall propagates item IDs into the record, so
// lineage already links a produced artifact to the memory it saw, and each item
// back to the run that wrote it.
package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/security"
)

// Latest asks for the newest committed epoch. Runs never use it — they pin a
// concrete epoch before their first task — but a caller inspecting the store
// directly, outside any run, has nothing to pin to.
const Latest = ^uint64(0)

// Source records how a memory item came to exist: the run, stage, and task
// that wrote it, and the op fingerprint of the writing stage.
//
// It is the same question store.LineageEntry answers about an artifact, asked
// about a fact. An item retrieved a month later is evidence for whatever a
// model then concludes, and evidence with no provenance is indistinguishable
// from something the previous run made up.
//
// RunID and Stage are the join into that lineage log, and they are what
// answers "which model produced this text" in the general case: a Remember
// stage issues no model call of its own — the text it stores was produced
// upstream — so Model is populated only when the writing path had a model
// resolved, and is empty for the declarative Remember stage.
type Source struct {
	RunID string `json:"run_id,omitempty"`
	Stage string `json:"stage,omitempty"`
	Task  string `json:"task_id,omitempty"`
	Model string `json:"model,omitempty"`
	Op    string `json:"op,omitempty"` // writing stage's fingerprint
}

// Item is one unit of long-term memory.
//
// ID is the content hash of (Space, Text, Meta) and nothing else. Writing the
// same fact twice is therefore free and idempotent, which matters more here
// than anywhere else in Loom: a knowledge base is fed by every run of every
// pipeline that touches it, and the same conclusion will be reached again and
// again. Created and Source sit outside the hash deliberately — they say when
// and by whom a fact was first learned, and folding them in would make one
// fact into a new item per run.
type Item struct {
	ID    string         `json:"id"`
	Space string         `json:"space"`
	Text  string         `json:"text"`
	Meta  map[string]any `json:"meta,omitempty"`
	// Vector is the embedding Text was indexed under. Stores persist it so a
	// reopened knowledge base does not re-embed (and re-pay for) its own
	// contents.
	Vector []float32 `json:"vector,omitempty"`
	// Epoch is the epoch at which this item became visible. Zero means staged
	// but not yet committed.
	Epoch   uint64    `json:"epoch,omitempty"`
	Source  Source    `json:"source,omitzero"`
	Created time.Time `json:"created,omitzero"`
}

// NewItem returns an item with its content-addressed ID computed.
func NewItem(space, text string, meta map[string]any) Item {
	it := Item{Space: space, Text: text, Meta: meta}
	it.ID = it.contentID()
	return it
}

// contentID hashes exactly the fields that make two items the same fact.
func (i Item) contentID() string {
	// encoding/json sorts map keys, so a meta map built in a different order
	// hashes identically — the same canonicalization store.Key relies on.
	b, err := json.Marshal([]any{i.Space, i.Text, i.Meta})
	if err != nil {
		// Meta is required to be JSON-serializable; fall back to a total
		// function rather than panicking inside a task.
		b = []byte(i.Space + "\x00" + i.Text + "\x00" + fmt.Sprint(i.Meta))
	}
	h := sha256.Sum256(b)
	return "mem_" + hex.EncodeToString(h[:16])
}

// Hit is one search result: the item and its similarity to the query.
type Hit struct {
	Item  Item    `json:"item"`
	Score float32 `json:"score"`
}

// Query is a similarity search against one space, as of one epoch.
type Query struct {
	Space  string
	Vector []float32
	K      int
	// AsOf bounds visibility to items committed at or before this epoch. A run
	// always supplies the epoch it pinned; Latest reads the newest committed
	// state. Zero is not a shorthand for "newest" — it is the empty store,
	// which is what a space nobody has committed to should return.
	AsOf uint64
	// Filter constrains results to items whose Meta matches every entry, by
	// string equality on the rendered value. Structured recall (a tenant, a
	// document class, a validity window) belongs here rather than in the query
	// text, because a filter is exact and a nearest-neighbour search is not.
	Filter map[string]string
	// MinScore drops hits below a similarity floor. Retrieval always returns
	// its k nearest neighbours, even when the nearest is unrelated; a floor is
	// what turns "the closest thing in the store" into "nothing relevant".
	MinScore float32
}

// Store is a durable, epoch-versioned vector store. Implementations range from
// the in-process InMemory below to an embedded index (memory/chromem) to a
// hosted service; the interface is the seam, exactly as model.Provider is for
// completions.
//
// Implementations must be safe for concurrent use: a fleet's agents share one
// store, and the tasks of a single run reach it in parallel.
type Store interface {
	// Epoch returns the newest committed epoch of a space. An unknown space is
	// not an error — it is epoch 0, the empty knowledge base every application
	// starts from.
	Epoch(ctx context.Context, space string) (uint64, error)
	// Search returns the q.K nearest items visible at q.AsOf, best first.
	Search(ctx context.Context, q Query) ([]Hit, error)
	// Upsert stages items for the next epoch, returning their IDs. Staged
	// items are invisible to Search — including to the run that wrote them —
	// until Commit. Re-staging an existing ID is a no-op.
	Upsert(ctx context.Context, items []Item) ([]string, error)
	// Commit publishes each named space's staged items as a new epoch and
	// returns the epochs reached. Committing a space with nothing staged
	// leaves its epoch alone, so an idle run does not churn every reader's
	// cache.
	Commit(ctx context.Context, spaces ...string) (map[string]uint64, error)
	// Get returns one item by ID.
	Get(ctx context.Context, space, id string) (Item, bool, error)
	// Spaces lists the space names the store knows about.
	Spaces(ctx context.Context) ([]string, error)
	// Close releases the store's resources.
	Close() error
}

// Call carries per-task security context into an embedder, mirroring
// model.CallContext: ResolveSecret is pre-scoped to the calling task's grants,
// so an embedder obtains exactly the credential that task was granted and
// every resolution is audited. Embedders hold no ambient credentials, for the
// same reason providers don't.
type Call struct {
	TaskID        string
	ResolveSecret func(ref security.SecretRef) (string, error)
}

// Embedder maps text to vectors. Its Name joins the fingerprint of every
// recall stage: a different embedder produces different neighbours for the
// same query, so results computed under one must not be replayed for another.
type Embedder interface {
	Name() string
	// Dims is the vector width, so stores can reject a mismatched index rather
	// than return silent nonsense.
	Dims() int
	// Endpoint is the network host the embedder contacts, or "" when it runs
	// in-process. The planner puts it on the egress allowlist of exactly the
	// stages that recall, and the executor enforces it.
	Endpoint() string
	// Secret is the credential reference the embedder resolves, or "" when it
	// needs none. The planner grants it to exactly the stages that recall.
	Secret() security.SecretRef
	// Embed returns one vector per input text, plus what the call cost. The
	// usage flows into the run's governor and report like a completion's does:
	// embedding a large corpus is real money, and a budget that ignored it
	// would not be a budget.
	Embed(ctx context.Context, call Call, texts []string) ([][]float32, core.Usage, error)
}

// Normalize scales v to unit length in place and returns it, so cosine
// similarity is a dot product. A zero vector is left alone.
func Normalize(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return v
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return v
}

// Similarity is the cosine similarity of two unit vectors — their dot product.
// Mismatched widths score 0 rather than panicking: a store that outlived an
// embedder change should return nothing, not crash a task.
func Similarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return dot
}

// Matches reports whether an item satisfies a metadata filter.
func Matches(it Item, filter map[string]string) bool {
	for k, want := range filter {
		v, ok := it.Meta[k]
		if !ok || fmt.Sprint(v) != want {
			return false
		}
	}
	return true
}

// TopK sorts hits best-first and truncates to k, breaking score ties by ID so
// two stores holding the same items return the same ordering — the recalled
// IDs are a cache key, and a key that depends on map iteration order would
// make every rerun a miss.
func TopK(hits []Hit, k int) []Hit {
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].Item.ID < hits[j].Item.ID
	})
	if k > 0 && len(hits) > k {
		hits = hits[:k]
	}
	return hits
}

// IDs returns the hit item IDs in order. This is the value a recall stage
// writes into the record, and therefore the part of the record that carries
// what was retrieved into every downstream cache key.
func IDs(hits []Hit) []string {
	out := make([]string, 0, len(hits))
	for _, h := range hits {
		out = append(out, h.Item.ID)
	}
	return out
}

// Render joins hits into the block a prompt template interpolates, numbering
// them and labelling each with its ID so a model can cite what it used and a
// reader can trace the citation back to the item and its Source.
func Render(hits []Hit) string {
	if len(hits) == 0 {
		return ""
	}
	var b strings.Builder
	for i, h := range hits {
		fmt.Fprintf(&b, "[%d] (%s) %s\n", i+1, h.Item.ID, h.Item.Text)
	}
	return strings.TrimRight(b.String(), "\n")
}
