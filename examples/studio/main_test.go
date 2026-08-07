package main

import (
	"context"
	"encoding/json"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/studio"
)

// TestExampleDocumentPricesAndRuns walks the whole path the example offers,
// offline: invent the archive, price the document, run both passes against the
// mock models, and check the report the write step leaves behind.
func TestExampleDocumentPricesAndRuns(t *testing.T) {
	root, err := inventArchive(6)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	out := filepath.Join(t.TempDir(), "reports")
	doc := digestDoc(root, out, 15, false)
	if err := doc.Validate(); err != nil {
		t.Fatalf("the example's document should validate: %v", err)
	}

	reg, secrets, err := registry(false)
	if err != nil {
		t.Fatal(err)
	}
	est := studio.Price(doc, studio.WithRegistry(reg))
	if est.Error != "" {
		t.Fatalf("price: %s", est.Error)
	}
	if est.Records != 18 {
		t.Fatalf("priced against %d records, want the 18 on disk", est.Records)
	}
	if est.Step("daily-digest").Calls != 18 {
		t.Fatalf("the digest projects %d calls, want one per day", est.Step("daily-digest").Calls)
	}
	// Each branch keeps exactly its own vertical: the filters really ran.
	for _, v := range []string{"payments", "logistics", "retail"} {
		if got := est.Step("rollup-" + v).Records; got != 6 {
			t.Fatalf("the %s branch carries %d records, want 6", v, got)
		}
	}
	if est.Partial {
		t.Fatal("a declared answer shape should leave nothing to guess")
	}
	if !est.FitsCap {
		t.Fatalf("$%.4f does not fit a $15 cap", est.CeilingUSD)
	}

	p, err := doc.Build()
	if err != nil {
		t.Fatal(err)
	}
	run := runner(reg, secrets, nil, "")
	if err := run(context.Background(), studio.RunRequest{
		Doc: doc, Estimate: est, Pipeline: p, Second: doc.BuildSecond,
	}); err != nil {
		t.Fatal(err)
	}

	report, err := os.ReadFile(filepath.Join(out, "business-overview.md"))
	if err != nil {
		t.Fatalf("the write step left nothing behind: %v", err)
	}
	if len(report) == 0 {
		t.Fatal("the one-pager is empty")
	}

	// What was actually spent has to sit under what was projected: the mock
	// answers are shorter than the caps they were priced at, never longer.
	res, err := loom.Run(context.Background(), p, loom.WithRegistry(reg), loom.WithWorkers(8))
	if err != nil {
		t.Fatal(err)
	}
	if res.Spent.CostUSD > est.CeilingUSD {
		t.Fatalf("spent $%.4f against a projected ceiling of $%.4f",
			res.Spent.CostUSD, est.CeilingUSD)
	}
}

// TestPricingWritesNothing is the property that makes the header safe to watch
// while editing: asking what a run would cost must not perform any of it.
func TestPricingWritesNothing(t *testing.T) {
	root, err := inventArchive(6)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	out := filepath.Join(t.TempDir(), "reports")
	reg, _, err := registry(false)
	if err != nil {
		t.Fatal(err)
	}
	if est := studio.Price(digestDoc(root, out, 15, false), studio.WithRegistry(reg)); est.Error != "" {
		t.Fatal(est.Error)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("pricing created %s", out)
	}
}

// TestExampleExportsCompilableGo checks the way out of the studio: the document
// the example opens renders as a Go program that parses.
func TestExampleExportsCompilableGo(t *testing.T) {
	src, err := digestDoc("/data/messages", "reports", 15, true).Go()
	if err != nil {
		t.Fatalf("export: %v\n%s", err, src)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "export.go", src, parser.AllErrors); err != nil {
		t.Fatalf("generated Go does not parse: %v\n%s", err, src)
	}
	for _, want := range []string{
		`pipeline.New("vertical-digest")`,
		`studio.LoadRecords(ctx, &studio.SourceSpec{`,
		`func buildSecond(out map[string][]core.Record) (*pipeline.Pipeline, error)`,
		`loom.WithRunBudget(core.Budget{MaxCostUSD: 15})`,
	} {
		if !strings.Contains(src, want) {
			t.Fatalf("the export is missing %q", want)
		}
	}
}

// TestStudioServesTheExample runs the server the example starts, without
// binding a port.
func TestStudioServesTheExample(t *testing.T) {
	root, err := inventArchive(6)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	reg, _, err := registry(false)
	if err != nil {
		t.Fatal(err)
	}
	doc := digestDoc(root, filepath.Join(t.TempDir(), "reports"), 15, false)

	srv := httptest.NewServer(studio.New(doc, studio.Models(reg)))
	defer srv.Close()

	res, err := http.Get(srv.URL + "/api/state")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var st struct {
		Doc      *studio.Doc      `json:"doc"`
		Estimate *studio.Estimate `json:"estimate"`
		Fields   map[string][]string
	}
	if err := json.NewDecoder(res.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.Estimate.Error != "" {
		t.Fatalf("served state carries an error: %s", st.Estimate.Error)
	}
	if len(st.Doc.Steps) != 11 {
		t.Fatalf("the document has %d steps", len(st.Doc.Steps))
	}
	// The chips the prompt editor offers are the fields the record actually
	// carries at that step, renamed the way the source declared.
	want := map[string]bool{"vertical": true, "date": true, "messages": true}
	for _, f := range st.Fields["daily-digest"] {
		delete(want, f)
	}
	if len(want) > 0 {
		t.Fatalf("the digest step is missing field chips: %v", want)
	}
}

// TestInventedArchiveIsRedactedOnTheWayIn checks the promise the source panel
// makes: names and emails are gone before a record exists at all.
func TestInventedArchiveIsRedactedOnTheWayIn(t *testing.T) {
	root, err := inventArchive(6)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	doc := digestDoc(root, "reports", 15, false)
	recs, err := studio.LoadRecords(context.Background(), doc.Find("load-days").Source)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 18 {
		t.Fatalf("read %d records", len(recs))
	}
	var seen int
	for _, r := range recs {
		text := r.String("messages")
		for _, name := range []string{"Dana", "Roi", "Yael", "Noa", "Maya", "Gil"} {
			if strings.Contains(text, name) {
				t.Fatalf("%s still names %s:\n%s", r.ID, name, text)
			}
		}
		if strings.Contains(text, "@example.com") {
			t.Fatalf("%s still carries an email address:\n%s", r.ID, text)
		}
		if strings.Contains(text, "S1:") {
			seen++
		}
		var _ core.Record = r
	}
	if seen == 0 {
		t.Fatal("no record was pseudonymized")
	}
}
