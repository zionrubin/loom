package telemetry

import (
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/observe"
)

// collector is the other end of the wire: a real HTTP server that decodes
// what the exporter sent, so these tests exercise the encoding rather than a
// mock of it.
type collector struct {
	*httptest.Server
	mu       chan struct{}
	last     atomic.Value // map[string]any
	requests atomic.Int64
	status   atomic.Int64
}

func newCollector(t *testing.T) *collector {
	t.Helper()
	c := &collector{mu: make(chan struct{}, 1)}
	c.status.Store(http.StatusOK)
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.requests.Add(1)
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Errorf("gzip: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer zr.Close()
			body = zr
		}
		var decoded map[string]any
		if err := json.NewDecoder(body).Decode(&decoded); err != nil {
			t.Errorf("decode: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		c.last.Store(decoded)
		if code := int(c.status.Load()); code != http.StatusOK {
			w.WriteHeader(code)
			_, _ = w.Write([]byte("nope"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(c.Close)
	return c
}

func (c *collector) payload(t *testing.T) map[string]any {
	t.Helper()
	v, _ := c.last.Load().(map[string]any)
	if v == nil {
		t.Fatal("the collector received nothing")
	}
	return v
}

func onlySpan(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()
	rs := payload["resourceSpans"].([]any)
	if len(rs) != 1 {
		t.Fatalf("want one resourceSpans entry, got %d", len(rs))
	}
	ss := rs[0].(map[string]any)["scopeSpans"].([]any)
	spans := ss[0].(map[string]any)["spans"].([]any)
	if len(spans) == 0 {
		t.Fatal("no spans in the payload")
	}
	return spans[0].(map[string]any)
}

// TestTheWireFormatIsWhatTheSpecificationPins. The whole reason this package
// encodes OTLP by hand instead of taking the SDK is that the format is a
// specification rather than an implementation detail — so the encoding is
// what gets asserted, field by field, against the protobuf-to-JSON rules:
// IDs in hex, 64-bit values as strings, enums as numbers.
func TestTheWireFormatIsWhatTheSpecificationPins(t *testing.T) {
	c := newCollector(t)
	opts := Options{Service: "triage", Instance: "pod-3",
		Attrs:    map[string]string{"deployment.environment": "prod"},
		Endpoint: c.URL, Sample: keepAll()}
	tel := New(opts)
	feed(tel)
	flush(t, tel)

	payload := c.payload(t)
	res := payload["resourceSpans"].([]any)[0].(map[string]any)["resource"].(map[string]any)
	found := map[string]string{}
	for _, a := range res["attributes"].([]any) {
		attr := a.(map[string]any)
		found[attr["key"].(string)] = attr["value"].(map[string]any)["stringValue"].(string)
	}
	if found["service.name"] != "triage" || found["service.instance.id"] != "pod-3" {
		t.Errorf("the resource must identify the process: %v", found)
	}
	if found["deployment.environment"] != "prod" {
		t.Errorf("Options.Attrs should reach the resource: %v", found)
	}

	span := onlySpan(t, payload)
	trace, _ := span["traceId"].(string)
	if len(trace) != 32 {
		t.Errorf("traceId is 32 hex characters, got %q", trace)
	}
	if _, err := hex.DecodeString(trace); err != nil {
		t.Errorf("traceId must be hex, not base64: %q", trace)
	}
	if id, _ := span["spanId"].(string); len(id) != 16 {
		t.Errorf("spanId is 16 hex characters, got %q", id)
	}
	start, ok := span["startTimeUnixNano"].(string)
	if !ok {
		t.Fatalf("a 64-bit timestamp must be a string, got %T", span["startTimeUnixNano"])
	}
	if _, err := strconv.ParseInt(start, 10, 64); err != nil {
		t.Errorf("startTimeUnixNano should parse as an int64: %q", start)
	}
	if _, ok := span["kind"].(float64); !ok {
		t.Errorf("kind is an enum number, got %T", span["kind"])
	}
}

// TestAttributeValuesCarryTheirType: a dollar figure that arrives as a string
// cannot be summed by the backend it was sent to.
func TestAttributeValuesCarryTheirType(t *testing.T) {
	c := newCollector(t)
	tel := New(Options{Endpoint: c.URL, Sample: keepAll()})
	tel.Handle(observe.Event{Type: observe.RunStarted, RunID: "r", Pipeline: "p",
		Budget: core.Budget{MaxCostUSD: 5}, Time: at(0)})
	tel.Handle(observe.Event{Type: observe.RunFinished, RunID: "r", Pipeline: "p", Time: at(1)})
	flush(t, tel)

	kinds := map[string]string{}
	for _, a := range onlySpan(t, c.payload(t))["attributes"].([]any) {
		attr := a.(map[string]any)
		for k := range attr["value"].(map[string]any) {
			kinds[attr["key"].(string)] = k
		}
	}
	if kinds["loom.budget_usd"] != "doubleValue" {
		t.Errorf("a dollar figure is a double, got %q", kinds["loom.budget_usd"])
	}
	if kinds["loom.pipeline"] != "stringValue" {
		t.Errorf("a name is a string, got %q", kinds["loom.pipeline"])
	}
}

// TestGzipTravelsAndDecodes.
func TestGzipTravelsAndDecodes(t *testing.T) {
	c := newCollector(t)
	tel := New(Options{Endpoint: c.URL, OTLP: OTLPOptions{Gzip: true}, Sample: keepAll()})
	feed(tel)
	flush(t, tel)
	if c.requests.Load() == 0 {
		t.Fatal("nothing reached the collector")
	}
	onlySpan(t, c.payload(t)) // decoded through the gzip reader above
}

// TestARejectedPayloadIsNotOfferedThreeTimes: retrying a 400 is a way to fail
// three times. A 503 is the collector restarting, and that is worth waiting
// out.
func TestARejectedPayloadIsNotOfferedThreeTimes(t *testing.T) {
	c := newCollector(t)
	exp := OTLP(c.URL, nil, OTLPOptions{Attempts: 3, Timeout: 2 * time.Second})
	span := []Span{{TraceID: TraceIDFor("r"), SpanID: spanIDFor("r", "run"), Name: "x"}}

	c.status.Store(http.StatusBadRequest)
	if err := exp.Export(context.Background(), span); err == nil {
		t.Fatal("a rejected payload should surface as an error")
	}
	if n := c.requests.Load(); n != 1 {
		t.Errorf("a 400 is final: want 1 attempt, got %d", n)
	}

	c.requests.Store(0)
	c.status.Store(http.StatusServiceUnavailable)
	if err := exp.Export(context.Background(), span); err == nil {
		t.Fatal("want the last failure surfaced")
	}
	if n := c.requests.Load(); n != 3 {
		t.Errorf("a 503 is worth retrying: want 3 attempts, got %d", n)
	}
}

// blocked is an exporter that never returns, standing in for a collector that
// has gone away mid-request.
type blocked struct{ release chan struct{} }

func (b *blocked) Export(context.Context, []Span) error { <-b.release; return nil }
func (b *blocked) Shutdown(context.Context) error       { close(b.release); return nil }

// TestACollectorThatHangsCostsACounterAndNotTheRun is the property that makes
// this safe to switch on in production. Telemetry attached to a run must
// never be able to stop it: the queue is bounded, a full queue drops, and the
// drop is itself a metric.
func TestACollectorThatHangsCostsACounterAndNotTheRun(t *testing.T) {
	b := &blocked{release: make(chan struct{})}
	tel := New(Options{Traces: b, Sample: keepAll(), SpanQueue: 1, SpanBatch: 1})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tel.Close(ctx)
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 500 {
			tel.Handle(observe.Event{Type: observe.RunStarted, RunID: "r" + strconv.Itoa(i),
				Pipeline: "p", Time: at(i)})
			tel.Handle(observe.Event{Type: observe.RunFinished, RunID: "r" + strconv.Itoa(i),
				Pipeline: "p", Time: at(i + 1)})
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a hung collector blocked the event bus")
	}
	if got := value(t, tel.Scrape(), "loom_telemetry_spans_dropped_total"); got == 0 {
		t.Error("spans dropped for a hung collector should be visible in the exposition")
	}
}
