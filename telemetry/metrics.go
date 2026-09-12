package telemetry

import (
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// This file is the metric side of the package: a tiny registry that folds
// events into series and writes them in the Prometheus text exposition
// format. It is deliberately not a general-purpose metrics library.
//
// Two properties matter more here than generality, and both come from the
// same place — this code runs inline on the event bus, on the hot path of
// every task in every run.
//
// The first is that the metric surface is *declared*, once, in the table at
// the top of telemetry.go. A registry that creates families on first use
// cannot tell a new metric from a typo, and a deployment's dashboards are
// built against names; declaring them makes the surface reviewable in one
// screen and makes an undeclared name a counted mistake rather than a silent
// new series.
//
// The second is that cardinality is bounded. A label whose values come from
// the workload rather than from the program is how monitoring systems are
// brought down, and Loom has one of those — a stage ID is whatever the
// pipeline's author named it, and a generated pipeline generates names. So
// every family has a ceiling on its series count, and the series past it are
// not dropped: they are folded into one overflow member. The number stays
// true and only the attribution collapses, which is the right way around —
// an operator whose dollar counter silently stopped counting has been lied
// to, and one whose dollars arrive under a label saying "too many stages to
// break down" has been told something useful.

// Label is one dimension of a series. Label sets are held sorted by name so
// that a series has exactly one signature.
type Label struct{ Name, Value string }

// L is shorthand for a label, used at every call site in this package.
func L(name, value string) Label { return Label{Name: name, Value: value} }

// overflowValue is the label value every dimension collapses to once a family
// has reached its ceiling. It is deliberately ugly: it should read, on a
// dashboard, as a configuration problem rather than as a stage nobody
// remembers writing.
const overflowValue = "__overflow__"

type metricKind uint8

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

func (k metricKind) String() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	default:
		return "histogram"
	}
}

// metric is a declared family's name, minus the namespace prefix. Every call
// site in this package uses one of the constants in telemetry.go, so a name
// that is not declared cannot be written by accident.
type metric string

// decl declares one metric family.
type decl struct {
	name    metric
	help    string
	kind    metricKind
	buckets []float64
}

// durationBuckets spans the range a model call actually occupies: tens of
// milliseconds for a cache replay, minutes for a long generation on a
// contended account. Latency histograms in this package share it so that a
// dashboard can put task duration and model-call duration on one axis.
var durationBuckets = []float64{
	0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300,
}

// series is one labelled member of a family.
type series struct {
	labels []Label
	sig    string

	value   float64 // counter total, or gauge value
	count   uint64  // histogram observations
	sum     float64 // histogram sum
	buckets []uint64
}

type family struct {
	decl
	full string // namespaced name, as exposed

	mu     sync.Mutex
	series map[string]*series
	cap    int
}

func (f *family) at(labels []Label) *series {
	sort.Slice(labels, func(i, j int) bool { return labels[i].Name < labels[j].Name })
	sig := signature(labels)

	f.mu.Lock()
	defer f.mu.Unlock()
	if s := f.series[sig]; s != nil {
		return s
	}
	if len(f.series) >= f.cap {
		labels = folded(labels)
		sig = signature(labels)
		if s := f.series[sig]; s != nil {
			return s
		}
	}
	s := &series{labels: labels, sig: sig}
	if f.kind == kindHistogram {
		s.buckets = make([]uint64, len(f.buckets))
	}
	f.series[sig] = s
	return s
}

// folded collapses a label set onto the family's single overflow member. The
// names survive and the values do not, so the exposition still says which
// dimensions existed.
func folded(labels []Label) []Label {
	out := make([]Label, len(labels))
	for i, l := range labels {
		out[i] = Label{Name: l.Name, Value: overflowValue}
	}
	return out
}

func signature(labels []Label) string {
	var b strings.Builder
	for _, l := range labels {
		b.WriteString(l.Name)
		b.WriteByte('\x00')
		b.WriteString(l.Value)
		b.WriteByte('\x00')
	}
	return b.String()
}

// registry holds every declared family. It is safe for concurrent use from
// the event bus and from a scrape at the same time.
type registry struct {
	fams  map[metric]*family
	order []metric

	// unknown counts writes to names that were never declared. It is a bug
	// counter rather than a metric about the workload, and it is exposed for
	// the same reason the overflow label is: a monitoring surface that hides
	// its own failures is worse than one that has them.
	unknown atomic.Int64
}

func newRegistry(namespace string, seriesCap int, decls []decl) *registry {
	r := &registry{fams: make(map[metric]*family, len(decls))}
	for _, d := range decls {
		f := &family{decl: d, full: namespace + "_" + string(d.name),
			series: map[string]*series{}, cap: seriesCap}
		r.fams[d.name] = f
		r.order = append(r.order, d.name)
	}
	sort.Slice(r.order, func(i, j int) bool { return r.order[i] < r.order[j] })
	return r
}

// add increments a counter. Negative deltas are refused rather than applied:
// a counter that can go down is a series every rate() over it will misread,
// and the call site that wanted one wanted a gauge.
func (r *registry) add(name metric, v float64, labels ...Label) {
	if v < 0 {
		return
	}
	f := r.fams[name]
	if f == nil {
		r.unknown.Add(1)
		return
	}
	s := f.at(labels)
	f.mu.Lock()
	s.value += v
	f.mu.Unlock()
}

// set writes a gauge.
func (r *registry) set(name metric, v float64, labels ...Label) {
	f := r.fams[name]
	if f == nil {
		r.unknown.Add(1)
		return
	}
	s := f.at(labels)
	f.mu.Lock()
	s.value = v
	f.mu.Unlock()
}

// addGauge moves a gauge by a delta, for the ones that count things in
// flight.
func (r *registry) addGauge(name metric, v float64, labels ...Label) {
	f := r.fams[name]
	if f == nil {
		r.unknown.Add(1)
		return
	}
	s := f.at(labels)
	f.mu.Lock()
	s.value += v
	f.mu.Unlock()
}

// observe records one histogram sample.
func (r *registry) observe(name metric, v float64, labels ...Label) {
	f := r.fams[name]
	if f == nil {
		r.unknown.Add(1)
		return
	}
	s := f.at(labels)
	f.mu.Lock()
	s.count++
	s.sum += v
	for i, b := range f.buckets {
		if v <= b {
			s.buckets[i]++
		}
	}
	f.mu.Unlock()
}

// value reads one series back. It exists for tests and for the readiness
// checks that want to look at what has been counted; nothing on the hot path
// calls it.
func (r *registry) value(name metric, labels ...Label) float64 {
	f := r.fams[name]
	if f == nil {
		return 0
	}
	sorted := append([]Label(nil), labels...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	sig := signature(sorted)
	f.mu.Lock()
	defer f.mu.Unlock()
	if s := f.series[sig]; s != nil {
		if f.kind == kindHistogram {
			return float64(s.count)
		}
		return s.value
	}
	return 0
}

// writeTo renders the registry in the Prometheus text exposition format
// (version 0.0.4). Families with no series are omitted entirely: a HELP line
// with nothing under it tells a scraper about a dimension of the system that
// this process has not touched, and there are enough of those here — stream
// mode, MCP, the commons — that printing them all would bury the ones that
// matter.
//
// Output is fully ordered — families by name, series by signature, buckets by
// bound — because a scrape that differs between two identical processes is a
// diff nobody can read.
func (r *registry) writeTo(w io.Writer) error {
	var b strings.Builder
	for _, name := range r.order {
		f := r.fams[name]
		f.mu.Lock()
		members := make([]*series, 0, len(f.series))
		for _, s := range f.series {
			members = append(members, s)
		}
		sort.Slice(members, func(i, j int) bool { return members[i].sig < members[j].sig })
		snapshot := make([]series, len(members))
		for i, s := range members {
			snapshot[i] = series{labels: s.labels, value: s.value, count: s.count, sum: s.sum}
			if s.buckets != nil {
				snapshot[i].buckets = append([]uint64(nil), s.buckets...)
			}
		}
		f.mu.Unlock()
		if len(snapshot) == 0 {
			continue
		}

		b.WriteString("# HELP " + f.full + " " + f.help + "\n")
		b.WriteString("# TYPE " + f.full + " " + f.kind.String() + "\n")
		for i := range snapshot {
			s := &snapshot[i]
			if f.kind != kindHistogram {
				b.WriteString(f.full)
				writeLabels(&b, s.labels, Label{})
				b.WriteString(" " + num(s.value) + "\n")
				continue
			}
			// Buckets are stored cumulatively — observe() increments every
			// bound the sample falls under — so they are written straight out.
			for j, bound := range f.buckets {
				b.WriteString(f.full + "_bucket")
				writeLabels(&b, s.labels, L("le", num(bound)))
				b.WriteString(" " + strconv.FormatUint(s.buckets[j], 10) + "\n")
			}
			b.WriteString(f.full + "_bucket")
			writeLabels(&b, s.labels, L("le", "+Inf"))
			b.WriteString(" " + strconv.FormatUint(s.count, 10) + "\n")
			b.WriteString(f.full + "_sum")
			writeLabels(&b, s.labels, Label{})
			b.WriteString(" " + num(s.sum) + "\n")
			b.WriteString(f.full + "_count")
			writeLabels(&b, s.labels, Label{})
			b.WriteString(" " + strconv.FormatUint(s.count, 10) + "\n")
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func writeLabels(b *strings.Builder, labels []Label, extra Label) {
	if len(labels) == 0 && extra.Name == "" {
		return
	}
	b.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteString(`="`)
		b.WriteString(escape(l.Value))
		b.WriteByte('"')
	}
	if extra.Name != "" {
		if len(labels) > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extra.Name)
		b.WriteString(`="`)
		b.WriteString(escape(extra.Value))
		b.WriteByte('"')
	}
	b.WriteByte('}')
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escape(s string) string { return labelEscaper.Replace(s) }

// num formats a value the way the exposition format wants it: shortest
// round-trippable decimal, no exponent games that a scraper would read
// differently from the process that wrote them.
func num(v float64) string {
	if v == float64(int64(v)) && v < 1e15 && v > -1e15 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
