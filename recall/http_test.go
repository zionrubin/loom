package recall

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestHTTPIngestAndAsk drives the desk the way a client does: write on one
// endpoint, read on another, and never coordinate the two.
func TestHTTPIngestAndAsk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := openDesk(t, Config{Name: "http"})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	srv := httptest.NewServer(d.Handler())
	defer srv.Close()

	// The console is served at the root, and it is what makes a desk something
	// a person can talk to rather than an API they have to script.
	page := get(t, srv.URL+"/")
	if !strings.Contains(page, "living context") {
		t.Errorf("the console does not show the context it answers from")
	}

	body := `{"conversation":"acme","messages":[
	  {"role":"user","text":"hello"},
	  {"role":"user","text":"we are blocked on SSO"}]}`
	var ingested struct {
		Accepted int   `json:"accepted"`
		Posted   int64 `json:"posted"`
	}
	postJSON(t, srv.URL+"/ingest", body, &ingested)
	if ingested.Accepted != 2 {
		t.Fatalf("accepted %d messages, want 2", ingested.Accepted)
	}
	if _, err := d.Await(ctx, ingested.Posted); err != nil {
		t.Fatalf("await: %v", err)
	}

	// The living prompt is served as the bytes a model gets, with the revision
	// it belongs to in the headers — a desk that would not show you what it is
	// telling the model is one you cannot debug.
	res, err := http.Get(srv.URL + "/context")
	if err != nil {
		t.Fatalf("GET /context: %v", err)
	}
	defer res.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(res.Body); err != nil {
		t.Fatalf("read context: %v", err)
	}
	if !strings.Contains(buf.String(), "blocked on SSO") {
		t.Errorf("served context does not carry the conversation:\n%s", buf)
	}
	if res.Header.Get("X-Recall-Version") == "" || res.Header.Get("X-Recall-Revision") == "" {
		t.Error("served context does not name the revision it is")
	}

	var ans Answer
	postJSON(t, srv.URL+"/ask", `{"session":"s","text":"what is Acme blocked on?"}`, &ans)
	if ans.Empty {
		t.Fatal("the desk answered as if it had ingested nothing")
	}
	if !strings.Contains(ans.Text, "context carried") {
		t.Errorf("the context did not reach the model: %q", ans.Text)
	}
	if ans.Version == 0 {
		t.Error("the answer does not name the context version it used")
	}

	var stats Stats
	getJSON(t, srv.URL+"/stats", &stats)
	if stats.Posted != 2 {
		t.Errorf("stats report %d posted, want 2", stats.Posted)
	}
	if stats.Queries == 0 {
		t.Error("stats did not count the question")
	}
	if stats.Dropped == 0 {
		t.Error("stats did not count the greeting the desk dropped")
	}
	if !stats.Running {
		t.Error("stats say ingestion is not running")
	}
}

// TestHTTPUpdatesFollowsTheContext checks the long poll a live view uses: ask
// for a revision past the one you hold, and the request returns when ingestion
// publishes it.
func TestHTTPUpdatesFollowsTheContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := openDesk(t, Config{Name: "updates"})
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	srv := httptest.NewServer(d.Handler())
	defer srv.Close()

	done := make(chan View, 1)
	go func() {
		var v View
		getJSON(t, srv.URL+"/updates?version=0&wait=20s", &v)
		done <- v
	}()

	if _, err := d.Post(ctx, say("acme", "user", "we are blocked on SSO")); err != nil {
		t.Fatalf("post: %v", err)
	}

	select {
	case v := <-done:
		if v.Version < 1 {
			t.Errorf("the long poll returned v%d without a revision to report", v.Version)
		}
		if !strings.Contains(v.Text, "blocked on SSO") {
			t.Errorf("the published revision does not carry the conversation:\n%s", v.Text)
		}
	case <-ctx.Done():
		t.Fatal("the long poll never returned")
	}
}

func TestHTTPRejectsBadRequests(t *testing.T) {
	d := openDesk(t, Config{Name: "reject"})
	srv := httptest.NewServer(d.Handler())
	defer srv.Close()

	for _, tc := range []struct {
		name, method, path, body string
		want                     int
	}{
		{"ask by GET", http.MethodGet, "/ask", "", http.StatusMethodNotAllowed},
		{"ask without JSON", http.MethodPost, "/ask", "not json", http.StatusBadRequest},
		{"ingest nothing", http.MethodPost, "/ingest", `{"messages":[]}`, http.StatusBadRequest},
		{"unknown path", http.MethodGet, "/nope", "", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, srv.URL+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do: %v", err)
			}
			defer res.Body.Close()
			if res.StatusCode != tc.want {
				t.Errorf("status %d, want %d", res.StatusCode, tc.want)
			}
		})
	}
}

// --- helpers -------------------------------------------------------------

func get(t *testing.T, url string) string {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer res.Body.Close()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(res.Body); err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return buf.String()
}

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(into); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

func postJSON(t *testing.T, url, body string, into any) {
	t.Helper()
	res, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(res.Body)
		t.Fatalf("POST %s: status %d: %s", url, res.StatusCode, buf)
	}
	if err := json.NewDecoder(res.Body).Decode(into); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}
