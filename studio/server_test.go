package studio

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/pipeline"
)

func testServer(t *testing.T, opts ...Option) (*Server, *httptest.Server, *Doc) {
	t.Helper()
	doc := digestDoc(chatRoot(t))
	s := New(doc.Clone(), append([]Option{Models(testRegistry(t))}, opts...)...)
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	return s, srv, doc
}

func getJSON[T any](t *testing.T, url string) T {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("GET %s: %s: %s", url, res.Status, b)
	}
	var out T
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func postJSON(t *testing.T, url string, body any) (*http.Response, []byte) {
	t.Helper()
	blob, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.Post(url, "application/json", strings.NewReader(string(blob)))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	got, _ := io.ReadAll(res.Body)
	return res, got
}

func TestServesTheUIAndTheState(t *testing.T) {
	_, srv, _ := testServer(t)

	res, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if !strings.Contains(string(page), "<title>Loom Studio</title>") {
		t.Fatal("the UI is not being served")
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("content type is %q", ct)
	}
	if res.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("missing nosniff")
	}

	st := getJSON[map[string]any](t, srv.URL+"/api/state")
	if st["doc"] == nil || st["estimate"] == nil {
		t.Fatalf("state is missing the document or its price: %v", st)
	}
	est := st["estimate"].(map[string]any)
	if est["error"] != nil && est["error"] != "" {
		t.Fatalf("the reference document should price: %v", est["error"])
	}
	if est["records"].(float64) != 4 {
		t.Fatalf("priced against %v records", est["records"])
	}
	if len(st["intents"].([]any)) == 0 {
		t.Fatal("no intents offered")
	}
	fields := st["fields"].(map[string]any)["daily-digest"].([]any)
	if len(fields) == 0 {
		t.Fatal("no field chips computed for the digest step")
	}
	if st["can_run"].(bool) {
		t.Fatal("a studio with no runner should not claim it can run")
	}
}

func TestPostingADocumentRepricesAndSaves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "doc.json")
	s, srv, doc := testServer(t, File(path))

	before := s.Estimate().CeilingUSD
	doc.Find("daily-digest").Infer.MaxTokens = 100
	res, body := postJSON(t, srv.URL+"/api/doc", doc)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("%s: %s", res.Status, body)
	}
	var st state
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	if st.Estimate.CeilingUSD >= before {
		t.Fatalf("the price did not move: %v then %v", before, st.Estimate.CeilingUSD)
	}
	if st.SavedAt == 0 {
		t.Fatal("the document was not saved")
	}
	saved, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if saved.Find("daily-digest").Infer.MaxTokens != 100 {
		t.Fatal("the file on disk does not carry the edit")
	}
}

func TestAHalfEditedDocumentIsAnswerNotACrash(t *testing.T) {
	_, srv, doc := testServer(t)
	doc.Find("only-retail").Keep.Op = "kinda"
	res, body := postJSON(t, srv.URL+"/api/doc", doc)
	if res.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status is %s, want 422", res.Status)
	}
	var out struct{ Error string }
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.Error, "unknown operator") {
		t.Fatalf("error is %q", out.Error)
	}
	// The last good document is still what the studio holds.
	st := getJSON[state](t, srv.URL+"/api/state")
	if st.Doc.Find("only-retail").Keep.Op != "is" {
		t.Fatal("a rejected edit changed the document anyway")
	}
}

func TestAskProposesAndAcceptApplies(t *testing.T) {
	_, srv, _ := testServer(t)

	res, body := postJSON(t, srv.URL+"/api/ask", map[string]string{"q": "make this cheaper"})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("%s: %s", res.Status, body)
	}
	var p Proposal
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Edits) == 0 {
		t.Fatalf("no edits proposed: %+v", p)
	}
	if p.Estimate == nil {
		t.Fatal("a proposal that changes the document should carry its new price")
	}

	// Nothing has changed yet: a proposal is a proposal.
	st := getJSON[state](t, srv.URL+"/api/state")
	if st.Doc.Find("daily-digest").Infer.MaxTokens != 400 {
		t.Fatal("the proposal edited the document before it was accepted")
	}

	res, body = postJSON(t, srv.URL+"/api/accept", map[string]any{"edits": p.Edits})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("%s: %s", res.Status, body)
	}
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	if st.Doc.Find("daily-digest").Infer.MaxTokens != 300 {
		t.Fatalf("accepting did not apply the edit: %d", st.Doc.Find("daily-digest").Infer.MaxTokens)
	}
	if st.Estimate.ExpectedUSD == 0 {
		t.Fatal("the accepted document was not repriced")
	}
}

func TestAskAnswersWithoutProposingWhenThereIsNothingToChange(t *testing.T) {
	_, srv, _ := testServer(t)
	for _, q := range []string{"why is it expensive", "how long can this take", "what does a rerun cost",
		"are the Hebrew days handled correctly"} {
		res, body := postJSON(t, srv.URL+"/api/ask", map[string]string{"q": q})
		if res.StatusCode != http.StatusOK {
			t.Fatalf("%q: %s: %s", q, res.Status, body)
		}
		var p Proposal
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatal(err)
		}
		if p.Title == "" || p.Body == "" {
			t.Fatalf("%q got an empty answer: %+v", q, p)
		}
		if len(p.Edits) != 0 {
			t.Fatalf("%q proposed edits for a question: %+v", q, p.Edits)
		}
	}
}

func TestExportServesCompilableGo(t *testing.T) {
	_, srv, _ := testServer(t)
	res, err := http.Get(srv.URL + "/api/export")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	src, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("%s: %s", res.Status, src)
	}
	for _, want := range []string{"package main", `pipeline.New("vertical-digest")`, "func buildSecond("} {
		if !strings.Contains(string(src), want) {
			t.Fatalf("export is missing %q", want)
		}
	}
}

func TestSampleShowsWhatTheRedactionsDid(t *testing.T) {
	_, srv, _ := testServer(t)
	got := getJSON[struct {
		Total   int           `json:"total"`
		Records []core.Record `json:"records"`
	}](t, srv.URL+"/api/sample?step=load-days")
	if len(got.Records) != 1 {
		t.Fatalf("got %d records, want the first one", len(got.Records))
	}
	text := got.Records[0].String("messages")
	if strings.Contains(text, "@example.com") || strings.Contains(text, "Dana") {
		t.Fatalf("the preview shows unredacted text, which is the one thing it exists to rule out:\n%s", text)
	}

	res, err := http.Get(srv.URL + "/api/sample?step=daily-digest")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("sampling a step that is not a source returned %s", res.Status)
	}
}

func TestRunNeedsARunnerAndThenUsesIt(t *testing.T) {
	_, srv, _ := testServer(t)
	res, body := postJSON(t, srv.URL+"/api/run", map[string]any{})
	if res.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status is %s, want 501: %s", res.Status, body)
	}
	if !strings.Contains(string(body), "export the Go") {
		t.Fatalf("the refusal should say what to do instead: %s", body)
	}

	var mu sync.Mutex
	var got RunRequest
	done := make(chan struct{})
	_, srv2, _ := testServer(t,
		Constellation("http://localhost:8077"),
		Runner(func(ctx context.Context, r RunRequest) error {
			mu.Lock()
			got = r
			mu.Unlock()
			close(done)
			return nil
		}))
	res, body = postJSON(t, srv2.URL+"/api/run", map[string]any{})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("%s: %s", res.Status, body)
	}
	var st state
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	if !st.CanRun || st.Run == nil || st.Run.URL != "http://localhost:8077" {
		t.Fatalf("run state is %+v", st.Run)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the runner was never called")
	}
	mu.Lock()
	defer mu.Unlock()
	if got.Pipeline == nil || got.Pipeline.Name != "vertical-digest" {
		t.Fatalf("the runner got %+v", got.Pipeline)
	}
	if got.Estimate == nil || got.Estimate.CeilingUSD == 0 {
		t.Fatal("the runner should be told what this is expected to cost")
	}
	second, err := got.Second(map[string][]core.Record{
		"rollup-payments": {core.NewRecord("a", map[string]any{"output": "x"})},
		"rollup-retail":   {core.NewRecord("b", map[string]any{"output": "y"})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || second.Name != "business-overview" {
		t.Fatalf("the second pass is %+v", second)
	}
	var _ *pipeline.Pipeline = second

	// The finished run is reported, not forgotten.
	deadline := time.Now().Add(3 * time.Second)
	for {
		st = getJSON[state](t, srv2.URL+"/api/state")
		if !st.Run.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the run never finished")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st.Run.Finished == 0 || st.Run.Err != "" {
		t.Fatalf("finished run reads %+v", st.Run)
	}
}

func TestCrossOriginWritesAreRefused(t *testing.T) {
	_, srv, doc := testServer(t)
	blob, _ := json.Marshal(doc)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/doc", strings.NewReader(string(blob)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://evil.example")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("a cross-origin write returned %s, want 403", res.Status)
	}
}

func TestMethodsAreChecked(t *testing.T) {
	_, srv, _ := testServer(t)
	res, err := http.Get(srv.URL + "/api/doc")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/doc returned %s", res.Status)
	}
	res, err = http.Get(srv.URL + "/nope")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("an unknown path returned %s", res.Status)
	}
}

func TestSourcesAreReadOncePerSpec(t *testing.T) {
	root := chatRoot(t)
	doc := digestDoc(root)
	s := New(doc.Clone(), Models(testRegistry(t)))

	// Move the folder out from under the studio: a second price that still
	// answers proves the records were cached rather than re-read.
	moved := root + "-moved"
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	next := s.Doc()
	next.Find("daily-digest").Infer.System = "changed"
	if err := s.SetDoc(next); err != nil {
		t.Fatal(err)
	}
	if got := s.Estimate(); got.Error != "" {
		t.Fatalf("the second price re-read the folder: %s", got.Error)
	}

	// Changing the source itself is a different spec, and that one has to read.
	next = s.Doc()
	next.Find("load-days").Source.Match = "*.json"
	if err := s.SetDoc(next); err == nil || !strings.Contains(err.Error(), "no such file") {
		if got := s.Estimate(); got.Error == "" {
			t.Fatalf("editing the source did not re-read it (err %v, estimate %+v)", err, got)
		}
	}
}

func TestNewWithoutADocumentStillServes(t *testing.T) {
	s := New(nil)
	srv := httptest.NewServer(s)
	defer srv.Close()
	st := getJSON[state](t, srv.URL+"/api/state")
	if st.Doc.Name != "untitled" {
		t.Fatalf("empty studio is named %q", st.Doc.Name)
	}
	if st.Estimate.Error == "" {
		t.Fatal("an empty document cannot be priced, and should say so")
	}
}
