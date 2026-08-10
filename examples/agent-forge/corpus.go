package main

// Reading the conversation corpus, and scrubbing it before any of it leaves the
// machine.
//
// Layout, the same one examples/vertical-digest reads:
//
//	<root>/<space>/<YYYY-MM-DD>.jsonl     one JSON message per line
//
// A flat folder of <name>.jsonl also works — each file becomes its own space.
//
// Nothing identifying is sent to a model. Senders and @mentions become stable
// pseudonyms (TM-xxxx, derived from a per-run salt so they cannot be reversed
// or joined across runs), and e-mail addresses, phone numbers and long digit
// runs are replaced with placeholders. The scrubbing happens at load, before
// the record exists, so there is no path by which raw text reaches a prompt.

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zionrubin/loom/core"
)

type chatMsg struct {
	Sender struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"sender"`
	CreateTime string `json:"createTime"`
	Text       string `json:"text"`
}

type dayFile struct {
	Space string
	Date  string
	Path  string
}

var dateRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// discover walks the corpus and returns the day files to read, clamped by date
// and optionally trimmed to the last N days of each space.
func discover(root, since, until string, last int) ([]dayFile, []string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, fmt.Errorf("read corpus root: %w", err)
	}

	bySpace := map[string][]dayFile{}
	add := func(space, date, path string) {
		if since != "" && date < since {
			return
		}
		if until != "" && date > until {
			return
		}
		bySpace[space] = append(bySpace[space], dayFile{Space: space, Date: date, Path: path})
	}

	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		if !e.IsDir() {
			if !strings.HasSuffix(name, ".jsonl") {
				continue
			}
			// Flat layout: one file per space, dateless.
			space := strings.TrimSuffix(name, ".jsonl")
			add(space, "", filepath.Join(root, name))
			continue
		}
		days, err := os.ReadDir(filepath.Join(root, name))
		if err != nil {
			return nil, nil, fmt.Errorf("read space %s: %w", name, err)
		}
		for _, d := range days {
			if d.IsDir() || !strings.HasSuffix(d.Name(), ".jsonl") {
				continue
			}
			date := strings.TrimSuffix(d.Name(), ".jsonl")
			if !dateRe.MatchString(date) {
				continue
			}
			add(name, date, filepath.Join(root, name, d.Name()))
		}
	}

	spaces := make([]string, 0, len(bySpace))
	var files []dayFile
	for space, days := range bySpace {
		spaces = append(spaces, space)
		sort.Slice(days, func(i, j int) bool { return days[i].Date < days[j].Date })
		if last > 0 && len(days) > last {
			days = days[len(days)-last:]
		}
		files = append(files, days...)
	}
	sort.Strings(spaces)
	sort.Slice(files, func(i, j int) bool {
		if files[i].Space != files[j].Space {
			return files[i].Space < files[j].Space
		}
		return files[i].Date < files[j].Date
	})
	if len(files) == 0 {
		return nil, nil, fmt.Errorf("no .jsonl day files under %s", root)
	}
	return files, spaces, nil
}

var (
	emailRe   = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
	phoneRe   = regexp.MustCompile(`(?:\+972[-\s]?|0)5\d(?:[-\s]?\d){7}`)
	longNumRe = regexp.MustCompile(`\b\d{9,}\b`)
	urlRe     = regexp.MustCompile(`https?://\S+`)
	mentionRe = regexp.MustCompile(`@[\p{L}][\p{L}\p{M}'.\-]*(?:\s+[\p{L}][\p{L}\p{M}'.\-]*)?`)
)

// scrubber maps names to stable pseudonyms. Stability matters — the same person
// has to be the same TM-xxxx across every day and every space, or a handoff
// looks like two different people — and the salt makes the mapping local to a
// run rather than a lookup table anyone else can reuse.
type scrubber struct {
	salt string
	seen map[string]string
}

func newScrubber(salt string) *scrubber {
	return &scrubber{salt: salt, seen: map[string]string{}}
}

func (s *scrubber) pseudonym(name string) string {
	key := strings.ToLower(strings.TrimSpace(name))
	if key == "" {
		return "TM-0000"
	}
	if p, ok := s.seen[key]; ok {
		return p
	}
	sum := sha256.Sum256([]byte(s.salt + "|" + key))
	p := "TM-" + hex.EncodeToString(sum[:])[:4]
	s.seen[key] = p
	return p
}

// text scrubs one message body: contact details and URLs become placeholders,
// then what is left of an @name is a mention and becomes a pseudonym.
//
// The order is load-bearing. The mention rule matches @ followed by letters, so
// it also matches the domain half of an address — run it first and
// "dana.levi@example.com" becomes "dana.levi TM-1a2b", which redacts the domain
// and leaves the person's name behind. Addresses and URLs have to go first.
func (s *scrubber) text(in string) string {
	out := urlRe.ReplaceAllString(in, "<url>")
	out = emailRe.ReplaceAllString(out, "<email>")
	out = phoneRe.ReplaceAllString(out, "<phone>")
	out = longNumRe.ReplaceAllString(out, "<id>")
	out = mentionRe.ReplaceAllStringFunc(out, func(m string) string {
		return s.pseudonym(strings.TrimPrefix(m, "@"))
	})
	return strings.TrimSpace(out)
}

const maxMsgChars = 1600

// loadDay reads one day file into a record the census stage can read. Empty and
// unreadable days return ok=false and are skipped rather than failing the run:
// a 3,000-file corpus always has a few of both.
func loadDay(f dayFile, s *scrubber) (core.Record, bool, error) {
	fh, err := os.Open(f.Path)
	if err != nil {
		return core.Record{}, false, err
	}
	defer fh.Close()

	var lines []string
	senders := map[string]bool{}
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		raw := strings.TrimSpace(sc.Text())
		if raw == "" {
			continue
		}
		var m chatMsg
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			continue // a malformed line is not worth failing a day over
		}
		body := s.text(m.Text)
		if body == "" {
			continue
		}
		if len(body) > maxMsgChars {
			body = body[:maxMsgChars] + " …"
		}
		who := s.pseudonym(m.Sender.Name)
		senders[who] = true
		lines = append(lines, fmt.Sprintf("%s %s: %s", clockOf(m.CreateTime), who, body))
	}
	if err := sc.Err(); err != nil {
		return core.Record{}, false, err
	}
	if len(lines) == 0 {
		return core.Record{}, false, nil
	}

	date := f.Date
	if date == "" {
		date = "undated"
	}
	rec := core.NewRecord(f.Space+"/"+date, map[string]any{
		"space":    f.Space,
		"date":     date,
		"count":    len(lines),
		"senders":  len(senders),
		"messages": strings.Join(lines, "\n"),
	})
	return rec, true, nil
}

func clockOf(ts string) string {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return "--:--"
	}
	return t.Format("15:04")
}
