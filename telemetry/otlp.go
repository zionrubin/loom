package telemetry

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// This file speaks OTLP over HTTP with the JSON encoding, by hand.
//
// Going through the official OpenTelemetry SDK would be the obvious choice
// and it is the wrong one here. Loom's dependency list is provider SDKs and
// nothing else, on purpose: it is a framework other programs import, and
// every module it drags in is a module its callers must reconcile with their
// own. The OTLP wire format is a stable, versioned specification with a
// documented JSON encoding, and what this package needs of it — one request
// shape, spans with attributes and events — is a hundred lines. That trade
// looks different for an application, which should use the SDK; it looks like
// this for a library.
//
// The encoding follows the protobuf-to-JSON rules the specification pins:
// trace and span IDs are lowercase hex rather than base64, 64-bit values are
// strings, and enum values are numbers. Any OTLP-compatible collector accepts
// it — the Collector's own otlp receiver, Jaeger, Tempo, a vendor endpoint —
// and the endpoint is the same one every OTLP HTTP exporter is pointed at.

// OTLPOptions configures the exporter. The zero value posts uncompressed JSON
// to the endpoint with a ten-second timeout.
type OTLPOptions struct {
	// Headers are sent on every request — an API key, a tenant ID.
	Headers map[string]string
	// Timeout bounds one export attempt. Default 10s.
	Timeout time.Duration
	// Attempts is how many times one batch is offered before it is given up
	// on, with exponential backoff between them. Default 3. A batch is
	// retried only when the collector's answer says retrying could help:
	// a 429, a 503, or no answer at all. A 400 means the collector rejected
	// the payload, and offering it again is a way to fail three times.
	Attempts int
	// Gzip compresses request bodies. Spans of a large run compress well,
	// and every collector accepts it.
	Gzip bool
	// Client overrides the HTTP client, for a deployment that needs its own
	// transport — a proxy, mutual TLS, a tighter dialer.
	Client *http.Client
}

type otlpExporter struct {
	url     string
	opts    OTLPOptions
	client  *http.Client
	res     []Attr
	scope   string
	closing sync.Once
}

// OTLP returns an Exporter that posts spans to an OTLP/HTTP collector.
// endpoint is the collector's base URL — "http://localhost:4318" — and the
// exporter appends the specification's path. A full path ending in
// /v1/traces is taken as given.
//
// resource are the attributes every span carries: service.name,
// service.instance.id and whatever Options.Attrs added.
func OTLP(endpoint string, resource []Attr, opts OTLPOptions) Exporter {
	if opts.Timeout <= 0 {
		opts.Timeout = 10 * time.Second
	}
	if opts.Attempts <= 0 {
		opts.Attempts = 3
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: opts.Timeout}
	}
	url := strings.TrimRight(endpoint, "/")
	if !strings.HasSuffix(url, "/v1/traces") {
		url += "/v1/traces"
	}
	sorted := append([]Attr(nil), resource...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
	return &otlpExporter{url: url, opts: opts, client: client, res: sorted,
		scope: "github.com/zionrubin/loom/telemetry"}
}

func (e *otlpExporter) Export(ctx context.Context, spans []Span) error {
	if len(spans) == 0 {
		return nil
	}
	body, err := json.Marshal(e.payload(spans))
	if err != nil {
		return err
	}
	encoded, encoding, err := e.encode(body)
	if err != nil {
		return err
	}

	var last error
	for attempt := 1; attempt <= e.opts.Attempts; attempt++ {
		retry, err := e.post(ctx, encoded, encoding)
		if err == nil {
			return nil
		}
		last = err
		if !retry || attempt == e.opts.Attempts {
			break
		}
		select {
		case <-time.After(time.Duration(attempt) * 250 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return last
}

func (e *otlpExporter) encode(body []byte) ([]byte, string, error) {
	if !e.opts.Gzip {
		return body, "", nil
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(body); err != nil {
		return nil, "", err
	}
	if err := zw.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), "gzip", nil
}

// post makes one attempt and says whether another could help.
func (e *otlpExporter) post(ctx context.Context, body []byte, encoding string) (retry bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, e.opts.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	for k, v := range e.opts.Headers {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return true, err // no answer at all: the collector may simply be restarting
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return false, nil
	}
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<10))
	retry = resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode == http.StatusServiceUnavailable ||
		resp.StatusCode == http.StatusBadGateway ||
		resp.StatusCode == http.StatusGatewayTimeout
	return retry, fmt.Errorf("otlp: %s: %s", resp.Status, strings.TrimSpace(string(detail)))
}

func (e *otlpExporter) Shutdown(ctx context.Context) error {
	e.closing.Do(func() { e.client.CloseIdleConnections() })
	return nil
}

// --- Wire shapes --------------------------------------------------------

type otlpRequest struct {
	ResourceSpans []otlpResourceSpans `json:"resourceSpans"`
}

type otlpResourceSpans struct {
	Resource   otlpResource    `json:"resource"`
	ScopeSpans []otlpScopeSpan `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpAttr `json:"attributes,omitempty"`
}

type otlpScopeSpan struct {
	Scope otlpScope  `json:"scope"`
	Spans []otlpSpan `json:"spans"`
}

type otlpScope struct {
	Name string `json:"name"`
}

type otlpSpan struct {
	TraceID      string      `json:"traceId"`
	SpanID       string      `json:"spanId"`
	ParentSpanID string      `json:"parentSpanId,omitempty"`
	Name         string      `json:"name"`
	Kind         int         `json:"kind"`
	Start        string      `json:"startTimeUnixNano"`
	End          string      `json:"endTimeUnixNano"`
	Attributes   []otlpAttr  `json:"attributes,omitempty"`
	Events       []otlpEvent `json:"events,omitempty"`
	Status       *otlpStatus `json:"status,omitempty"`
}

type otlpEvent struct {
	Time       string     `json:"timeUnixNano"`
	Name       string     `json:"name"`
	Attributes []otlpAttr `json:"attributes,omitempty"`
}

type otlpStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

type otlpAttr struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

// otlpValue is protobuf's AnyValue: exactly one field is set.
type otlpValue struct {
	String *string  `json:"stringValue,omitempty"`
	Int    *string  `json:"intValue,omitempty"`
	Double *float64 `json:"doubleValue,omitempty"`
	Bool   *bool    `json:"boolValue,omitempty"`
}

func (e *otlpExporter) payload(spans []Span) otlpRequest {
	out := make([]otlpSpan, 0, len(spans))
	for _, s := range spans {
		out = append(out, otlpSpanOf(s))
	}
	return otlpRequest{ResourceSpans: []otlpResourceSpans{{
		Resource:   otlpResource{Attributes: otlpAttrs(e.res)},
		ScopeSpans: []otlpScopeSpan{{Scope: otlpScope{Name: e.scope}, Spans: out}},
	}}}
}

func otlpSpanOf(s Span) otlpSpan {
	o := otlpSpan{
		TraceID: s.TraceID, SpanID: s.SpanID, ParentSpanID: s.ParentID,
		Name: s.Name, Kind: int(s.Kind),
		Start: nanos(s.Start), End: nanos(s.End),
		Attributes: otlpAttrs(s.Attrs),
	}
	for _, ev := range s.Events {
		o.Events = append(o.Events, otlpEvent{
			Time: nanos(ev.Time), Name: ev.Name, Attributes: otlpAttrs(ev.Attrs)})
	}
	if s.Status != StatusUnset {
		o.Status = &otlpStatus{Code: int(s.Status), Message: s.Message}
	}
	return o
}

func otlpAttrs(attrs []Attr) []otlpAttr {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]otlpAttr, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, otlpAttr{Key: a.Key, Value: otlpValueOf(a.Value)})
	}
	return out
}

func otlpValueOf(v any) otlpValue {
	switch x := v.(type) {
	case string:
		return otlpValue{String: &x}
	case bool:
		return otlpValue{Bool: &x}
	case float64:
		return otlpValue{Double: &x}
	case float32:
		d := float64(x)
		return otlpValue{Double: &d}
	case int:
		s := strconv.FormatInt(int64(x), 10)
		return otlpValue{Int: &s}
	case int64:
		s := strconv.FormatInt(x, 10)
		return otlpValue{Int: &s}
	default:
		s := fmt.Sprint(v)
		return otlpValue{String: &s}
	}
}

// nanos renders a timestamp the way protobuf's JSON mapping wants a 64-bit
// integer: as a decimal string, because a JSON number cannot hold one
// without losing the low bits.
func nanos(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}

// --- Capture ------------------------------------------------------------

// Capture is an Exporter that keeps spans in memory. It is what tests and
// offline examples export to, and it is the reason the Exporter seam exists
// at all: a package whose only implementation talks to a network is a package
// nobody can test without one.
type Capture struct {
	mu    sync.Mutex
	spans []Span
}

func (c *Capture) Export(_ context.Context, spans []Span) error {
	c.mu.Lock()
	c.spans = append(c.spans, spans...)
	c.mu.Unlock()
	return nil
}

func (c *Capture) Shutdown(context.Context) error { return nil }

// Spans returns everything exported so far, in export order.
func (c *Capture) Spans() []Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Span(nil), c.spans...)
}
