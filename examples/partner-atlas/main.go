// Command partner-atlas reads an exported Google Chat corpus — one JSONL file
// per vertical per day, the same layout examples/vertical-digest consumes — and
// produces, for every vertical and every partner discussed in it, the state a
// bizdev owner needs before their next conversation: where the relationship
// stands, who talks to whom, how satisfied the partner appears (and on whose
// word), what is still open, and what to do next.
//
// The channels are not bizdev channels. They carry everything about a vertical
// — media buying, quality, product, finance — so the pipeline's first job is to
// find the partner signal inside traffic that is mostly about something else.
//
// Three runs, because loom DAGs fan out but do not fan back in:
//
//	1. partner-roster   sample-days → scan-names (nano) → name-line ─┬→ only-<v> → roster-<v> (mini)
//	                                                                 └→ ... one branch per vertical
//	2. partner-history  load-days → day-extract (nano ↗ mini) → split-partners ─┬→ only-<v>-<pNN>
//	                                                                            │    → history-<v>-<pNN> (mini)
//	                                                                            │    → brief-<v>-<pNN> (gpt-5.4)
//	                                                                            └→ ... one branch per partner
//	3. partner-portfolio  states ─┬→ only-<v> → portfolio-<v> (mini)
//	                              └→ ... one branch per vertical
//
// Between run 1 and run 2 the roster is frozen to roster.json: hand-editable,
// reloadable with -roster, and the thing that keeps the expensive extraction
// cache stable. Between run 2 and run 3 the per-partner state JSON is rendered
// to markdown in Go — the model produces state, not prose layout.
//
//	OPENAI_API_KEY=sk-... go run ./examples/partner-atlas \
//	    -messages ~/Desktop/google-chat/messages -out atlas -budget 20
//
// All three runs publish to one constellation view, which holds them as three
// skies in one universe. Press `u` for the overview, `,`/`.` to move between.
//
// Privacy: sender IDs and @mentions become stable pseudonyms (TM-xxxx) derived
// from a hash, so the same colleague reads the same across four years of
// history without their name leaving the machine; emails and mobile numbers are
// redacted. Partner *company* names are the subject of the analysis and are
// kept. See README for what that does and does not cover.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
	"github.com/zionrubin/loom/observe"
	"github.com/zionrubin/loom/pipeline"
	"github.com/zionrubin/loom/providers/openai"
	"github.com/zionrubin/loom/security"
	"github.com/zionrubin/loom/viz"
)

var (
	emailRe = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
	// Israeli mobile numbers, narrow on purpose: the corpus is full of bare
	// numbers that are metrics, not phones.
	phoneRe = regexp.MustCompile(`(?:\+972[-  ]?|\b0)5\d[-  ]?\d{3}[-  ]?\d{4}\b`)
	// Google Chat renders a mention as the display name in the message text.
	// Latin display names are matched exactly — a following Hebrew word cannot
	// be swallowed, which is why the offsets in the annotations (UTF-16 code
	// units) are never used.
	latinMentionRe  = regexp.MustCompile(`@([A-Z][A-Za-z'\-]+(?:[ ][A-Z][A-Za-z'\-]+){0,2})`)
	hebrewMentionRe = regexp.MustCompile(`@([\x{0590}-\x{05FF}]+(?:[ ][\x{0590}-\x{05FF}]+){0,1})`)
	nonSlugRe       = regexp.MustCompile(`[^a-z0-9]+`)
)

// chatMsg is the subset of the Google Chat export schema the atlas needs.
// Quoted snapshots and attachment names both carry partner substance —
// "quality of <partner> is dropping" is as often a reply as a fresh message.
type chatMsg struct {
	Sender struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"sender"`
	CreateTime  string `json:"createTime"`
	Text        string `json:"text"`
	Annotations []struct {
		Type        string `json:"type"`
		UserMention struct {
			User struct {
				Name string `json:"name"`
			} `json:"user"`
		} `json:"userMention"`
	} `json:"annotations"`
	Quoted *struct {
		Snapshot struct {
			Text string `json:"text"`
		} `json:"quotedMessageSnapshot"`
	} `json:"quotedMessageMetadata"`
	Attachment []struct {
		ContentName string `json:"contentName"`
	} `json:"attachment"`
}

// dayFile is one vertical+date JSONL file.
type dayFile struct {
	Vertical string
	Date     string // YYYY-MM-DD, from the filename
	Path     string
}

// dayDoc is one day-file rendered into a scrubbed transcript. Lines are kept
// alongside the joined text because the recent-history excerpts quote them
// individually, with their date restored.
type dayDoc struct {
	Vertical string
	Date     string
	Lines    []string
	Text     string
}

func (d dayDoc) record() core.Record {
	return core.NewRecord(d.Vertical+"/"+d.Date, map[string]any{
		"vertical": d.Vertical,
		"date":     d.Date,
		"count":    len(d.Lines),
		"messages": d.Text,
	})
}

// discover walks root and returns every per-day file (optionally clamped to
// [since, until], and to the `last` most recent days per vertical) plus the
// sorted list of verticals that have at least one.
func discover(root, since, until string, last int) ([]dayFile, []string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, err
	}
	var files []dayFile
	seen := map[string]bool{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		vertical := e.Name()
		days, err := os.ReadDir(filepath.Join(root, vertical))
		if err != nil {
			return nil, nil, err
		}
		for _, d := range days {
			if d.IsDir() || !strings.HasSuffix(d.Name(), ".jsonl") {
				continue
			}
			date := strings.TrimSuffix(d.Name(), ".jsonl")
			if since != "" && date < since {
				continue
			}
			if until != "" && date > until {
				continue
			}
			files = append(files, dayFile{Vertical: vertical, Date: date, Path: filepath.Join(root, vertical, d.Name())})
			seen[vertical] = true
		}
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Vertical != files[j].Vertical {
			return files[i].Vertical < files[j].Vertical
		}
		return files[i].Date < files[j].Date
	})
	if last > 0 {
		byVertical := map[string]int{}
		for _, f := range files {
			byVertical[f.Vertical]++
		}
		kept := files[:0]
		soFar := map[string]int{}
		for _, f := range files {
			soFar[f.Vertical]++
			if soFar[f.Vertical] > byVertical[f.Vertical]-last {
				kept = append(kept, f)
			}
		}
		files = kept
	}
	verticals := make([]string, 0, len(seen))
	for v := range seen {
		verticals = append(verticals, v)
	}
	sort.Strings(verticals)
	return files, verticals, nil
}

func readMessages(path string) ([]chatMsg, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var msgs []chatMsg
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var m chatMsg
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			continue // tolerate the odd malformed line
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// ---------------------------------------------------------------- pseudonyms

// scrubber replaces people with stable pseudonyms and strips direct
// identifiers. Labels are hashes, not counters: the label for a colleague is
// the same whether you run over four years or one month, so -since does not
// invalidate a single cached model call.
type scrubber struct {
	nameToID map[string]string // mention display name → sender ID, when resolvable
	knownHeb map[string]bool   // Hebrew mention names seen often enough to trust
	groups   []nameGroup       // extra literal names supplied with -names
}

// nameGroup is one person's aliases from the -names file, all mapping to a
// single label. It is the only way Hebrew names written without an @ get
// pseudonymized — no reliable Hebrew NER happens here.
type nameGroup struct {
	Label   string
	Aliases []string // longest first
}

func label(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "TM-" + fmt.Sprintf("%x", sum[:2])
}

func (s *scrubber) senderLabel(id string) string {
	if id == "" {
		return "TM-anon"
	}
	return label(id)
}

// mentionLabel prefers the sender-ID label, so a colleague reads the same
// whether they wrote a message or were mentioned in one.
func (s *scrubber) mentionLabel(name string) string {
	if id, ok := s.nameToID[strings.ToLower(name)]; ok {
		return s.senderLabel(id)
	}
	return label("name:" + strings.ToLower(name))
}

// scrub rewrites one message body: supplied names first (they may contain
// spaces and overlap mentions), then mentions, then direct identifiers.
func (s *scrubber) scrub(text string) string {
	for _, g := range s.groups {
		for _, a := range g.Aliases {
			text = strings.ReplaceAll(text, a, g.Label)
		}
	}
	text = latinMentionRe.ReplaceAllStringFunc(text, func(m string) string {
		return "@" + s.mentionLabel(strings.TrimPrefix(m, "@"))
	})
	text = hebrewMentionRe.ReplaceAllStringFunc(text, func(m string) string {
		name := strings.TrimPrefix(m, "@")
		if !s.knownHeb[name] {
			return m // a lone @word, not a display name we have evidence for
		}
		return "@" + s.mentionLabel(name)
	})
	text = emailRe.ReplaceAllString(text, "<email>")
	return phoneRe.ReplaceAllString(text, "<phone>")
}

// indexPeople reads the corpus once to learn who is who before any transcript
// is rendered — otherwise the same person would get one label in an early file
// and another in a later one, and the model would see two colleagues where
// there is one.
//
// Latin display names are paired to sender IDs positionally: the export lists
// USER_MENTION annotations in message order, so when a message has as many
// @Name matches as it has mention annotations, the nth of each is the same
// person. That pairing — not the annotation offsets, which are UTF-16 code
// units and cut Hebrew spans in the wrong place — is what makes a mention and a
// message from the same colleague land on one label.
//
// Hebrew display names have no such anchor, so they are kept only once seen
// minHeb times: a threshold that discards the accidental two-word capture
// without discarding a real colleague.
func indexPeople(files []dayFile, minHeb int) (*scrubber, error) {
	s := &scrubber{nameToID: map[string]string{}, knownHeb: map[string]bool{}}
	hebCount := map[string]int{}
	for _, f := range files {
		msgs, err := readMessages(f.Path)
		if err != nil {
			return nil, err
		}
		for _, m := range msgs {
			for _, mt := range hebrewMentionRe.FindAllStringSubmatch(m.Text, -1) {
				hebCount[mt[1]]++
			}
			names := latinMentionRe.FindAllStringSubmatch(m.Text, -1)
			var ids []string
			for _, a := range m.Annotations {
				if a.Type == "USER_MENTION" && a.UserMention.User.Name != "" {
					ids = append(ids, a.UserMention.User.Name)
				}
			}
			if len(names) == 0 || len(names) != len(ids) {
				continue // cannot pair with confidence; the name hash handles it
			}
			for i, n := range names {
				key := strings.ToLower(n[1])
				if _, ok := s.nameToID[key]; !ok {
					s.nameToID[key] = ids[i]
				}
			}
		}
	}
	for name, n := range hebCount {
		if n >= minHeb {
			s.knownHeb[name] = true
		}
	}
	return s, nil
}

func loadNameGroups(path string) ([]nameGroup, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var groups []nameGroup
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var aliases []string
		for _, a := range strings.Split(line, ",") {
			if a = strings.TrimSpace(a); a != "" {
				aliases = append(aliases, a)
			}
		}
		if len(aliases) == 0 {
			continue
		}
		// Longest first: replacing "Ofek" before "Ofek Aviv" would leave a
		// dangling surname behind.
		sort.Slice(aliases, func(i, j int) bool { return len(aliases[i]) > len(aliases[j]) })
		groups = append(groups, nameGroup{Label: label("group:" + strings.ToLower(aliases[len(aliases)-1])), Aliases: aliases})
	}
	return groups, nil
}

// loadDay renders one day-file into a scrubbed chronological transcript.
func loadDay(f dayFile, s *scrubber) (dayDoc, error) {
	msgs, err := readMessages(f.Path)
	if err != nil {
		return dayDoc{}, err
	}
	doc := dayDoc{Vertical: f.Vertical, Date: f.Date}
	for _, m := range msgs {
		var parts []string
		if t := strings.TrimSpace(m.Text); t != "" {
			parts = append(parts, s.scrub(t))
		}
		if m.Quoted != nil {
			if q := strings.TrimSpace(m.Quoted.Snapshot.Text); q != "" {
				parts = append(parts, "↳ quoting: "+s.scrub(q))
			}
		}
		for _, a := range m.Attachment {
			if a.ContentName != "" {
				parts = append(parts, "[attached: "+a.ContentName+"]")
			}
		}
		if len(parts) == 0 {
			continue
		}
		text := strings.Join(parts, " ")
		if len(text) > 3000 {
			text = text[:3000] + " …"
		}
		clock := m.CreateTime
		if len(clock) >= 16 {
			clock = clock[11:16] // HH:MM
		}
		doc.Lines = append(doc.Lines, fmt.Sprintf("%s %s: %s", clock, s.senderLabel(m.Sender.Name), text))
	}
	doc.Text = strings.Join(doc.Lines, "\n")
	return doc, nil
}

func loadDays(files []dayFile, s *scrubber) ([]dayDoc, error) {
	docs := make([]dayDoc, 0, len(files))
	for _, f := range files {
		doc, err := loadDay(f, s)
		if err != nil {
			return nil, err
		}
		if len(doc.Lines) == 0 {
			continue
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// sample takes every stride-th day per vertical, plus each vertical's most
// recent day. Uniform on purpose: a recency-weighted sample would never see a
// partner who churned three years ago, and "history of the partners" is the
// whole point.
func sample(docs []dayDoc, stride int) []dayDoc {
	if stride <= 1 {
		return docs
	}
	byVertical := map[string][]dayDoc{}
	for _, d := range docs {
		byVertical[d.Vertical] = append(byVertical[d.Vertical], d)
	}
	verticals := make([]string, 0, len(byVertical))
	for v := range byVertical {
		verticals = append(verticals, v)
	}
	sort.Strings(verticals)
	var out []dayDoc
	for _, v := range verticals {
		days := byVertical[v]
		for i, d := range days {
			if i%stride == 0 || i == len(days)-1 {
				out = append(out, d)
			}
		}
	}
	return out
}

// ------------------------------------------------------------------- roster

// partner is one canonical partner in one vertical, with every spelling the
// channel uses for it. Hebrew transliterations of the same brand differ by a
// letter all the time, and the same partner shows up in Latin script too.
type partner struct {
	Canonical string   `json:"canonical"`
	Aliases   []string `json:"aliases"`
	Kind      string   `json:"kind,omitempty"`
	Note      string   `json:"note,omitempty"`
}

// rosterFile is what -roster reads and every run writes: the frozen entity
// list, hand-editable by the people who know these partners better than a
// sampled model does.
type rosterFile struct {
	Source    string               `json:"source"`
	Verticals map[string][]partner `json:"verticals"`
}

func normName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.Trim(s, `"'.,:;!?()[]{}`)
	return strings.Join(strings.Fields(s), " ")
}

// parseRoster reads the line format the roster reducer emits:
//
//	canonical | alias; alias | kind | note
//
// A line format rather than JSON because it survives a hierarchical reduce:
// a truncated response loses its last partner instead of every partner.
func parseRoster(text string) []partner {
	var out []partner
	seen := map[string]bool{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		line = strings.TrimPrefix(line, "-")
		line = strings.TrimSpace(strings.Trim(line, "`"))
		if line == "" || !strings.Contains(line, "|") {
			continue
		}
		cols := strings.Split(line, "|")
		name := strings.TrimSpace(cols[0])
		if name == "" || normName(name) == "canonical" {
			continue // header row from a model that could not resist
		}
		p := partner{Canonical: name, Aliases: []string{name}}
		if len(cols) > 1 {
			for _, a := range strings.Split(cols[1], ";") {
				if a = strings.TrimSpace(a); a != "" {
					p.Aliases = append(p.Aliases, a)
				}
			}
		}
		if len(cols) > 2 {
			p.Kind = strings.TrimSpace(cols[2])
		}
		if len(cols) > 3 {
			p.Note = strings.TrimSpace(strings.Join(cols[3:], " "))
		}
		if seen[normName(name)] {
			continue
		}
		seen[normName(name)] = true
		out = append(out, dedupeAliases(p))
	}
	return out
}

func dedupeAliases(p partner) partner {
	seen := map[string]bool{}
	kept := make([]string, 0, len(p.Aliases))
	for _, a := range p.Aliases {
		n := normName(a)
		// Aliases shorter than 3 characters match everything; a 2-letter code
		// like "JD" hits hundreds of files across every vertical, almost all of
		// them somebody's initials.
		if n == "" || len([]rune(n)) < 3 || seen[n] {
			continue
		}
		seen[n] = true
		kept = append(kept, a)
	}
	sort.Slice(kept, func(i, j int) bool { return len(kept[i]) > len(kept[j]) })
	p.Aliases = kept
	return p
}

// mergeNearDuplicates folds partners whose canonical names differ by a
// character into one — the deterministic safety net under a model that
// clustered "אלפאקו" and "אלפקו" as two partners.
func mergeNearDuplicates(ps []partner) []partner {
	var out []partner
	for _, p := range ps {
		if len(p.Aliases) == 0 {
			continue
		}
		merged := false
		for i := range out {
			if editDistance([]rune(normName(out[i].Canonical)), []rune(normName(p.Canonical))) <= 1 {
				out[i].Aliases = append(out[i].Aliases, p.Aliases...)
				out[i] = dedupeAliases(out[i])
				merged = true
				break
			}
		}
		if !merged {
			out = append(out, p)
		}
	}
	return out
}

func editDistance(a, b []rune) int {
	if len(a) < 3 || len(b) < 3 {
		return 99 // never merge on short names
	}
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, min(cur[j-1]+1, prev[j-1]+cost))
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

// ------------------------------------------------------------- alias matching

// matcher finds partner aliases in a transcript. Hebrew matches on plain
// substrings: the prefix particles ל/ב/מ/ה/ו/ש attach directly to the noun, so
// "לאלפאקו" must still count as a mention of "אלפאקו" — a word-boundary regex
// would miss most of the corpus. Latin aliases do get a boundary check, or
// "Ace" matches inside every "Acetrack".
type matcher struct {
	names   []string
	hebrew  [][]string // per partner
	latin   [][]string // per partner, lowercased
	minRune int
}

func newMatcher(ps []partner) *matcher {
	m := &matcher{minRune: 3}
	for _, p := range ps {
		var heb, lat []string
		for _, a := range p.Aliases {
			if isASCII(a) {
				lat = append(lat, strings.ToLower(a))
			} else {
				heb = append(heb, a)
			}
		}
		m.names = append(m.names, p.Canonical)
		m.hebrew = append(m.hebrew, heb)
		m.latin = append(m.latin, lat)
	}
	return m
}

func isASCII(s string) bool {
	for _, r := range s {
		if r > 127 {
			return false
		}
	}
	return true
}

// hits returns, per partner index, how many lines of the transcript mention it.
func (m *matcher) hits(lines []string) map[int]int {
	out := map[int]int{}
	for _, line := range lines {
		lower := strings.ToLower(line)
		for i := range m.names {
			if m.lineHits(line, lower, i) {
				out[i]++
			}
		}
	}
	return out
}

func (m *matcher) lineHits(line, lower string, i int) bool {
	for _, a := range m.hebrew[i] {
		if strings.Contains(line, a) {
			return true
		}
	}
	for _, a := range m.latin[i] {
		if containsWord(lower, a) {
			return true
		}
	}
	return false
}

func containsWord(haystack, needle string) bool {
	for i := 0; ; {
		j := strings.Index(haystack[i:], needle)
		if j < 0 {
			return false
		}
		start := i + j
		end := start + len(needle)
		if !isWordByte(haystack, start-1) && !isWordByte(haystack, end) {
			return true
		}
		i = start + 1
	}
}

func isWordByte(s string, i int) bool {
	if i < 0 || i >= len(s) {
		return false
	}
	c := s[i]
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// ------------------------------------------------------------------- tracking

// tracked is one partner the atlas will brief, with the deterministic facts
// computed in Go. Nothing here is a model's opinion, which is what makes it
// safe to put in the prompt as ground truth.
type tracked struct {
	Vertical string
	Name     string
	Aliases  []string
	Kind     string
	Slug     string // pNN, rank by activity within the vertical
	Key      string // <vertical-slug>/pNN, the branch key
	Days     []string
	Lines    int
	ByYear   map[string]int
	Recent   string // verbatim excerpt of the most recent mentions

	// mentions holds the partner's own lines by date while track() measures,
	// travelling with the struct so ranking cannot shear it off the partner it
	// belongs to.
	mentions map[string][]string
}

func (t tracked) first() string {
	if len(t.Days) == 0 {
		return ""
	}
	return t.Days[0]
}

func (t tracked) last() string {
	if len(t.Days) == 0 {
		return ""
	}
	return t.Days[len(t.Days)-1]
}

func (t tracked) yearLine() string {
	years := make([]string, 0, len(t.ByYear))
	for y := range t.ByYear {
		years = append(years, y)
	}
	sort.Strings(years)
	parts := make([]string, 0, len(years))
	for _, y := range years {
		parts = append(parts, fmt.Sprintf("%s:%d", y, t.ByYear[y]))
	}
	return strings.Join(parts, " ")
}

func slugify(s string) string {
	s = nonSlugRe.ReplaceAllString(strings.ToLower(s), "-")
	return strings.Trim(s, "-")
}

// track measures every rostered partner against the whole corpus and keeps the
// ones with enough history to brief. It also decides which day-files run 2
// sends to a model at all: a day nobody mentions a partner in is a day with
// nothing for bizdev in it.
func track(docs []dayDoc, ros rosterFile, top, minDays, recentDays int) (kept []tracked, dropped []string, hitDocs []dayDoc) {
	byVertical := map[string][]dayDoc{}
	for _, d := range docs {
		byVertical[d.Vertical] = append(byVertical[d.Vertical], d)
	}
	verticals := make([]string, 0, len(byVertical))
	for v := range byVertical {
		verticals = append(verticals, v)
	}
	sort.Strings(verticals)

	for _, v := range verticals {
		ps := ros.Verticals[v]
		if len(ps) == 0 {
			continue
		}
		m := newMatcher(ps)
		cand := make([]tracked, len(ps))
		for i, p := range ps {
			cand[i] = tracked{Vertical: v, Name: p.Canonical, Aliases: p.Aliases, Kind: p.Kind,
				ByYear: map[string]int{}, mentions: map[string][]string{}}
		}
		for _, d := range byVertical[v] {
			hits := m.hits(d.Lines)
			if len(hits) == 0 {
				continue
			}
			hitDocs = append(hitDocs, d)
			for i, n := range hits {
				cand[i].Days = append(cand[i].Days, d.Date)
				cand[i].Lines += n
				if len(d.Date) >= 4 {
					cand[i].ByYear[d.Date[:4]]++
				}
				for k, line := range d.Lines {
					lower := strings.ToLower(line)
					if !m.lineHits(line, lower, i) {
						continue
					}
					if k > 0 && len(cand[i].mentions[d.Date]) == 0 {
						cand[i].mentions[d.Date] = append(cand[i].mentions[d.Date], "  (context) "+d.Lines[k-1])
					}
					cand[i].mentions[d.Date] = append(cand[i].mentions[d.Date], "  "+line)
				}
			}
		}
		sort.SliceStable(cand, func(i, j int) bool {
			if len(cand[i].Days) != len(cand[j].Days) {
				return len(cand[i].Days) > len(cand[j].Days)
			}
			return cand[i].Name < cand[j].Name
		})
		rank := 0
		for _, c := range cand {
			if len(c.Days) < minDays {
				dropped = append(dropped, fmt.Sprintf("%s/%s (%d day-files < %d)", v, c.Name, len(c.Days), minDays))
				continue
			}
			if top > 0 && rank >= top {
				dropped = append(dropped, fmt.Sprintf("%s/%s (%d day-files, below the top %d)", v, c.Name, len(c.Days), top))
				continue
			}
			rank++
			c.Slug = fmt.Sprintf("p%02d", rank)
			c.Key = slugify(v) + "/" + c.Slug
			c.Recent = recentExcerpt(c.mentions, c.Days, recentDays)
			kept = append(kept, c)
		}
	}
	sort.Slice(hitDocs, func(i, j int) bool {
		if hitDocs[i].Vertical != hitDocs[j].Vertical {
			return hitDocs[i].Vertical < hitDocs[j].Vertical
		}
		return hitDocs[i].Date < hitDocs[j].Date
	})
	return kept, dropped, dedupeDocs(hitDocs)
}

func dedupeDocs(docs []dayDoc) []dayDoc {
	seen := map[string]bool{}
	out := docs[:0]
	for _, d := range docs {
		key := d.Vertical + "/" + d.Date
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, d)
	}
	return out
}

// recentExcerpt quotes the partner's most recent mentioning days verbatim.
// Current status and satisfaction are read from these, not from the rolled-up
// history: a hierarchical reduce weighs a 2022 partial summary exactly as
// heavily as last week's, which is right for "what happened" and wrong for
// "where does this stand today".
func recentExcerpt(byDate map[string][]string, days []string, recentDays int) string {
	if len(days) == 0 {
		return "(none)"
	}
	start := len(days) - recentDays
	if start < 0 {
		start = 0
	}
	var b strings.Builder
	for _, date := range days[start:] {
		lines := byDate[date]
		if len(lines) == 0 {
			continue
		}
		fmt.Fprintf(&b, "[%s]\n%s\n", date, strings.Join(lines, "\n"))
		if b.Len() > 7000 {
			b.WriteString("… (older recent days trimmed)\n")
			break
		}
	}
	if b.Len() == 0 {
		return "(none)"
	}
	return b.String()
}

// --------------------------------------------------------------------- state

// partnerState is the artifact: what a bizdev owner should know about one
// partner in one vertical. The model fills the judgement fields; Go owns
// identity and the counted facts, and renders the markdown.
type partnerState struct {
	Vertical string   `json:"vertical"`
	Partner  string   `json:"partner"`
	Aliases  []string `json:"aliases,omitempty"`
	Kind     string   `json:"kind,omitempty"`

	Headline   string `json:"headline"`
	Stage      string `json:"stage"`
	StageBasis string `json:"stage_basis,omitempty"`
	Trajectory string `json:"trajectory,omitempty"`

	Relationship struct {
		OurOwners     []string `json:"our_owners,omitempty"`
		TheirContacts []string `json:"their_contacts,omitempty"`
		Cadence       string   `json:"cadence,omitempty"`
		Tone          string   `json:"tone,omitempty"`
		Note          string   `json:"history_note,omitempty"`
	} `json:"relationship"`

	// Satisfaction carries its own provenance. The partner is not in this
	// channel: almost every read on their mood is our paraphrase of theirs, and
	// a brief that cannot tell the difference will assert things nobody said.
	Satisfaction struct {
		Score      *int   `json:"score"`
		Trend      string `json:"trend,omitempty"`
		Confidence string `json:"confidence,omitempty"`
		Evidence   []struct {
			Date  string `json:"date,omitempty"`
			Claim string `json:"claim"`
			Basis string `json:"basis,omitempty"` // partner_direct | internal_secondhand | inferred
		} `json:"evidence,omitempty"`
	} `json:"satisfaction"`

	Commercials []struct {
		Date  string `json:"date,omitempty"`
		What  string `json:"what"`
		Value string `json:"value,omitempty"`
	} `json:"commercials,omitempty"`

	Timeline []struct {
		Date  string `json:"date,omitempty"`
		Event string `json:"event"`
		Why   string `json:"why_it_matters,omitempty"`
	} `json:"timeline,omitempty"`

	OpenThreads []struct {
		What   string `json:"what"`
		Owner  string `json:"owner,omitempty"`
		Since  string `json:"since,omitempty"`
		Status string `json:"status,omitempty"`
	} `json:"open_threads,omitempty"`

	Risks []struct {
		What     string `json:"what"`
		Severity string `json:"severity,omitempty"`
		Evidence string `json:"evidence,omitempty"`
	} `json:"risks,omitempty"`

	NextActions []struct {
		Action  string `json:"action"`
		Why     string `json:"why,omitempty"`
		Urgency string `json:"urgency,omitempty"`
	} `json:"next_actions,omitempty"`

	// ExternalLookups is where "connect it with more info if needed" lands:
	// the atlas reads one channel, so it names the gap instead of guessing.
	ExternalLookups []struct {
		What  string `json:"what"`
		Where string `json:"where,omitempty"`
		Why   string `json:"why,omitempty"`
	} `json:"external_lookups_needed,omitempty"`

	ConfidenceNote string   `json:"confidence_note,omitempty"`
	Gaps           []string `json:"gaps,omitempty"`

	// Evidence base, counted in Go.
	Activity struct {
		DayFiles  int            `json:"day_files"`
		Mentions  int            `json:"mention_lines"`
		FirstSeen string         `json:"first_seen"`
		LastSeen  string         `json:"last_seen"`
		ByYear    map[string]int `json:"by_year"`
	} `json:"activity"`
}

func (s partnerState) satisfactionLine() string {
	score := "n/a"
	if s.Satisfaction.Score != nil {
		score = strconv.Itoa(*s.Satisfaction.Score) + "/5"
	}
	parts := []string{score}
	if s.Satisfaction.Trend != "" {
		parts = append(parts, s.Satisfaction.Trend)
	}
	if s.Satisfaction.Confidence != "" {
		parts = append(parts, "confidence "+s.Satisfaction.Confidence)
	}
	return strings.Join(parts, ", ")
}

func renderState(s partnerState) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }

	w("# %s — %s\n\n", s.Partner, s.Vertical)
	w("**%s**\n\n", orDash(s.Headline))
	w("| | |\n|---|---|\n")
	w("| Stage | %s |\n", orDash(s.Stage))
	w("| Trajectory | %s |\n", orDash(s.Trajectory))
	w("| Satisfaction | %s |\n", s.satisfactionLine())
	w("| Activity | %d day-files, %d mentions, %s → %s |\n",
		s.Activity.DayFiles, s.Activity.Mentions, orDash(s.Activity.FirstSeen), orDash(s.Activity.LastSeen))
	if s.Kind != "" {
		w("| Kind | %s |\n", s.Kind)
	}
	if len(s.Aliases) > 1 {
		w("| Also called | %s |\n", strings.Join(s.Aliases[1:], ", "))
	}
	w("\n")
	if s.StageBasis != "" {
		w("Why this stage: %s\n\n", s.StageBasis)
	}

	w("## Relationship\n\n")
	w("- Our side: %s\n", orDash(strings.Join(s.Relationship.OurOwners, ", ")))
	w("- Their side: %s\n", orDash(strings.Join(s.Relationship.TheirContacts, ", ")))
	w("- Cadence: %s\n", orDash(s.Relationship.Cadence))
	w("- Tone: %s\n", orDash(s.Relationship.Tone))
	if s.Relationship.Note != "" {
		w("- History: %s\n", s.Relationship.Note)
	}
	w("\n")

	w("## Satisfaction — %s\n\n", s.satisfactionLine())
	if len(s.Satisfaction.Evidence) == 0 {
		w("No direct evidence found in this channel.\n\n")
	} else {
		w("| Date | Signal | Basis |\n|---|---|---|\n")
		for _, e := range s.Satisfaction.Evidence {
			w("| %s | %s | %s |\n", orDash(e.Date), cell(e.Claim), orDash(e.Basis))
		}
		w("\n")
	}

	if len(s.Commercials) > 0 {
		w("## Commercials & scale\n\n")
		for _, c := range s.Commercials {
			w("- %s%s%s\n", datePrefix(c.Date), c.What, suffix(c.Value))
		}
		w("\n")
	}
	if len(s.Timeline) > 0 {
		w("## Timeline\n\n")
		for _, t := range s.Timeline {
			w("- %s%s%s\n", datePrefix(t.Date), t.Event, suffix(t.Why))
		}
		w("\n")
	}
	if len(s.OpenThreads) > 0 {
		w("## Open threads\n\n")
		for _, o := range s.OpenThreads {
			w("- %s — owner %s, since %s, %s\n", o.What, orDash(o.Owner), orDash(o.Since), orDash(o.Status))
		}
		w("\n")
	}
	if len(s.Risks) > 0 {
		w("## Risks\n\n")
		for _, r := range s.Risks {
			w("- **%s** %s%s\n", orDash(r.Severity), r.What, suffix(r.Evidence))
		}
		w("\n")
	}
	if len(s.NextActions) > 0 {
		w("## Next actions\n\n")
		for _, a := range s.NextActions {
			w("- **%s** %s%s\n", orDash(a.Urgency), a.Action, suffix(a.Why))
		}
		w("\n")
	}
	if len(s.ExternalLookups) > 0 {
		w("## Needs information this channel does not have\n\n")
		for _, e := range s.ExternalLookups {
			w("- %s — look in %s%s\n", e.What, orDash(e.Where), suffix(e.Why))
		}
		w("\n")
	}
	if s.ConfidenceNote != "" || len(s.Gaps) > 0 {
		w("## Confidence\n\n")
		if s.ConfidenceNote != "" {
			w("%s\n\n", s.ConfidenceNote)
		}
		for _, g := range s.Gaps {
			w("- gap: %s\n", g)
		}
		w("\n")
	}
	if y := s.Activity.ByYear; len(y) > 0 {
		years := make([]string, 0, len(y))
		for k := range y {
			years = append(years, k)
		}
		sort.Strings(years)
		parts := make([]string, 0, len(years))
		for _, k := range years {
			parts = append(parts, fmt.Sprintf("%s: %d", k, y[k]))
		}
		w("---\n\nDay-files mentioning this partner by year — %s.\n", strings.Join(parts, ", "))
	}
	w("\nPeople appear as stable pseudonyms (TM-xxxx). Generated by loom's partner-atlas from one Google Chat channel; treat it as a starting brief, not a system of record.\n")
	return b.String()
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return s
}

func cell(s string) string { return strings.ReplaceAll(strings.ReplaceAll(s, "|", "\\|"), "\n", " ") }

func datePrefix(d string) string {
	if d == "" {
		return ""
	}
	return "**" + d + "** — "
}

func suffix(s string) string {
	if strings.TrimSpace(s) == "" {
		return ""
	}
	return " (" + s + ")"
}

// ------------------------------------------------------------------ pipeline

// tmplSafe keeps corpus text out of the template parser: an excerpt containing
// "{{" would otherwise be read as an action and fail the whole stage.
func tmplSafe(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "{{", "{ {"), "}}", "} }")
}

const scanPrefix = `You are mapping the business entities a company works with, from its internal chat.

A PARTNER is an external commercial counterparty: an insurance carrier, an agency, an aggregator,
a lead buyer or seller, a network, a comparison site, a vendor whose volume or payout is discussed.
NOT a partner: internal teams and colleagues, ad platforms (Google, Meta, Bing, TikTok), analytics
and internal tools, generic industry words, or a competitor mentioned only as market colour.

Names are often Hebrew transliterations of Latin brands and are spelled inconsistently. Return the
spelling as it appears in the text — a later step clusters variants.`

const rosterPrefix = `You are consolidating candidate partner names from one business vertical's chat channel into a
canonical roster.

Cluster spellings of the same entity together: Hebrew transliterations of one brand differ by a
letter or two, and the same partner also appears in Latin script. Prefer the most frequent spelling
as the canonical name. Drop anything that is not an external commercial counterparty, and drop
names so short or generic that matching them would hit unrelated text.

Output one line per partner and nothing else — no prose, no header, no code fence:

canonical | alias; alias; alias | kind | one-line note

kind is one of: carrier, agency, aggregator, network, comparison_site, lead_buyer, lead_seller, vendor, other.
Include the canonical spelling in the alias list. Every alias must be at least 3 characters.`

const extractPrefix = `You are reading one day of an internal chat channel that runs a whole insurance vertical, and
pulling out only what bears on the company's PARTNER relationships.

Most of what is said each day is not about partners — media buying, creative, tracking bugs,
hiring, product, internal process. Skip all of it. Returning an empty list is the correct answer
for most days.

For each partner discussed, record what actually happened, with the words of the channel behind it.

Provenance matters more than anything else here. The partner is NOT in this channel. When you read
sentiment, mark where it comes from:
  partner_direct       — the partner said it (relayed from a call, email, or their message)
  internal_secondhand  — a colleague reporting the partner's position
  inferred             — nobody said it; you are reading it off behaviour or numbers

Messages are Hebrew, English, or both. Always answer in English. People appear as pseudonyms
(TM-xxxx); use those exact strings. Refer to people at the partner by role and first name only.

Respond with a single JSON object and nothing else.`

const briefPrefix = `You write the partner state a business-development owner reads before their next conversation.

You get three things about one partner in one vertical: counted facts (computed, trustworthy), a
rolled-up history of the whole relationship, and the most recent mentions quoted verbatim.

Rules:
  - The verbatim recent block decides current stage, trajectory and satisfaction. The rolled-up
    history supplies the arc, the pattern, and how the relationship got here.
  - Never invent a date. If the history gives no date for something, omit the date field.
  - Satisfaction is the partner's, not ours. Every evidence item carries its basis:
    partner_direct, internal_secondhand, or inferred. If all you have is inferred, say so and set
    confidence low. A null score is better than a guessed one.
  - "our_owners" are pseudonyms (TM-xxxx) that appear in the material — do not invent them. For
    people at the partner, use role and first name only.
  - next_actions are for the bizdev owner and must follow from the evidence.
  - external_lookups_needed is where you name what this channel cannot tell you: a number to pull
    from the BI system, a contract clause, a CRM field, a person to ask. Be specific about where.

Respond with a single JSON object and nothing else, in this shape:
{"headline": "<one sentence an owner can read in three seconds>",
 "stage": "prospect|onboarding|ramping|steady|scaling|at_risk|paused|dormant|churned|unknown",
 "stage_basis": "<what in the evidence puts them there>",
 "trajectory": "improving|stable|declining|volatile|unknown",
 "relationship": {"our_owners": ["TM-xxxx"], "their_contacts": ["<role, first name>"],
                  "cadence": "<how often and how we talk>", "tone": "<working relationship in a few words>",
                  "history_note": "<how this relationship got to where it is>"},
 "satisfaction": {"score": <1-5 or null>, "trend": "improving|stable|declining|unknown",
                  "confidence": "low|medium|high",
                  "evidence": [{"date": "YYYY-MM-DD", "claim": "<what was said or seen>",
                                "basis": "partner_direct|internal_secondhand|inferred"}]},
 "commercials": [{"date": "", "what": "<terms, volumes, caps, payouts, budgets>", "value": ""}],
 "timeline": [{"date": "", "event": "", "why_it_matters": ""}],
 "open_threads": [{"what": "", "owner": "", "since": "", "status": ""}],
 "risks": [{"what": "", "severity": "low|medium|high", "evidence": ""}],
 "next_actions": [{"action": "", "why": "", "urgency": "now|this_week|this_quarter"}],
 "external_lookups_needed": [{"what": "", "where": "", "why": ""}],
 "confidence_note": "<how much of this is solid>",
 "gaps": ["<what you could not determine>"]}`

// buildRosterPipeline is run 1: scan a uniform sample of days for candidate
// partner names, then consolidate one roster per vertical.
func buildRosterPipeline(docs []dayDoc, verticals []string) *pipeline.Pipeline {
	p := pipeline.New("partner-roster")

	src := p.FromFunc("sample-days", func(ctx context.Context) ([]core.Record, error) {
		recs := make([]core.Record, 0, len(docs))
		for _, d := range docs {
			recs = append(recs, d.record())
		}
		return recs, nil
	})

	scanned := src.Infer("scan-names", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"gpt-5.4-mini"}},
		Prefix:  scanPrefix,
		System:  "You extract external partner names from internal chat. Respond with a single JSON object and nothing else.",
		Prompt: `One day of the "{{.vertical}}" channel, {{.date}} ({{.count}} messages):

{{.messages}}

Respond with JSON:
{"partners": ["<name exactly as written>"]}
Empty list if no external partner is discussed.`,
		MaxTokens: 300,
		ParseJSON: true,
	}, pipeline.WithParallelism(8))

	// Pure Go: one line per day for the reducer, dropping days with nothing.
	lined := scanned.Filter("has-names", func(r core.Record) (bool, error) {
		names, _ := r.Data["partners"].([]any)
		return len(names) > 0, nil
	}, pipeline.WithVersion("v1")).
		Map("name-line", func(r core.Record) (core.Record, error) {
			out := r.Clone()
			out.Data["name_line"] = fmt.Sprintf("[%s] %s", r.String("date"), joinAny(r.Data["partners"]))
			return out, nil
		}, pipeline.WithVersion("v1"))

	for _, v := range verticals {
		v := v
		lined.
			Filter("only-"+slugify(v), func(r core.Record) (bool, error) {
				return r.String("vertical") == v, nil
			}, pipeline.WithVersion("v1")).
			ReduceAI("roster-"+slugify(v), pipeline.ReduceAISpec{
				Binding: model.Binding{Model: "gpt-5.4-mini"},
				Prefix:  rosterPrefix,
				System:  "You consolidate partner names into a canonical roster. Output only the pipe-delimited lines.",
				Prompt: fmt.Sprintf(`Candidate partner names seen in the %q vertical, one line per day ({{.Count}} inputs).
Lines that are already pipe-delimited roster rows are partial rosters — merge them in.

{{range .Items}}{{.}}
{{end}}
Output the consolidated roster for this vertical, one partner per line.`, tmplSafe(v)),
				FanIn:     10,
				MaxTokens: 1200,
				ItemField: "name_line",
			})
	}
	return p
}

func joinAny(v any) string {
	items, _ := v.([]any)
	parts := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok && strings.TrimSpace(s) != "" {
			parts = append(parts, strings.TrimSpace(s))
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

// buildHistoryPipeline is run 2: extract partner events per day, split them per
// partner, roll each partner's history up chronologically, and write the state.
func buildHistoryPipeline(docs []dayDoc, kept []tracked) *pipeline.Pipeline {
	p := pipeline.New("partner-history")

	// alias → canonical, per vertical: the model is asked to use roster names
	// but writes what it reads, so canonicalize here rather than trust it.
	canon := map[string]map[string]tracked{}
	for _, t := range kept {
		if canon[t.Vertical] == nil {
			canon[t.Vertical] = map[string]tracked{}
		}
		for _, a := range t.Aliases {
			canon[t.Vertical][normName(a)] = t
		}
	}

	src := p.FromFunc("load-days", func(ctx context.Context) ([]core.Record, error) {
		recs := make([]core.Record, 0, len(docs))
		for _, d := range docs {
			recs = append(recs, d.record())
		}
		return recs, nil
	})

	extracted := src.Infer("day-extract", pipeline.InferSpec{
		Binding: model.Binding{Tier: model.TierFast, Escalation: []string{"gpt-5.4-mini"}},
		Prefix:  extractPrefix,
		System:  "You extract partner-relationship signal from internal chat. Respond with a single JSON object and nothing else.",
		Prompt: `One day of the "{{.vertical}}" channel, {{.date}} ({{.count}} messages):

{{.messages}}

Respond with JSON:
{"partners": [
  {"partner": "<name as written in the channel>",
   "events": [{"what": "<what happened, concretely>",
               "kind": "commercial|volume|quality|payment|escalation|meeting|contract|launch|pause|churn|other",
               "ours": ["TM-xxxx"], "theirs": ["<role, first name>"]}],
   "sentiment": "positive|neutral|negative|mixed|unknown",
   "satisfaction_notes": [{"claim": "<signal about how the partner feels>",
                           "basis": "partner_direct|internal_secondhand|inferred"}],
   "metrics": ["<numbers with their units and what they measure>"],
   "asks": ["<what they want from us>"],
   "commitments": ["<what either side promised, and who>"]}
]}
Use an empty list when the day holds nothing about any partner.`,
		MaxTokens: 900,
		ParseJSON: true,
	}, pipeline.WithParallelism(8))

	// Pure Go: one record per (partner, day), carrying a single dense line.
	// Fan-out per partner happens on the branch key, so a partner whose name
	// the model spelled a new way is dropped here and reported after the run.
	split := extracted.FlatMap("split-partners", func(r core.Record) ([]core.Record, error) {
		items, _ := r.Data["partners"].([]any)
		vertical, date := r.String("vertical"), r.String("date")
		out := make([]core.Record, 0, len(items))
		for i, it := range items {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			name, _ := m["partner"].(string)
			if strings.TrimSpace(name) == "" {
				continue
			}
			t, known := canon[vertical][normName(name)]
			rec := core.NewRecord(fmt.Sprintf("%s/%s/%d", vertical, date, i), map[string]any{
				"vertical":   vertical,
				"date":       date,
				"raw_name":   name,
				"key":        t.Key,
				"partner":    t.Name,
				"matched":    known,
				"event_line": eventLine(date, m),
			})
			out = append(out, rec)
		}
		return out, nil
	}, pipeline.WithVersion("v1"))

	for _, t := range kept {
		t := t
		split.
			Filter("only-"+t.Key, func(r core.Record) (bool, error) {
				return r.String("key") == t.Key, nil
			}, pipeline.WithVersion("v1")).
			ReduceAI("history-"+t.Key, pipeline.ReduceAISpec{
				Binding: model.Binding{Model: "gpt-5.4-mini"},
				Prefix: `You are building the running history of one partner relationship from dated daily notes.

Inputs arrive in chronological order and may themselves be partial histories — merge them into one.
Carry state forward: when something changed, keep both the change and its date. Never average two
positions into a vague middle; say what held first, when it changed, and what holds now. Keep the
numbers, the named commitments, and who was involved. Drop anything that is not about this partner.`,
				System: "You maintain a partner relationship history. Answer in English, dense and dated.",
				Prompt: fmt.Sprintf(`Chronological notes about the partner %q in the %q vertical ({{.Count}} inputs):

{{range .Items}}{{.}}
{{end}}
Write the merged history: how the relationship developed, the dated turning points, commercial
terms and volumes as they changed, quality and payment episodes, satisfaction signals with whose
word each rests on, commitments made by either side and whether they were kept, and who dealt with
whom. Chronological, dated, no preamble. Up to ~500 words.`, tmplSafe(t.Name), tmplSafe(t.Vertical)),
				FanIn:     10,
				MaxTokens: 1300,
				ItemField: "event_line",
			}).
			Infer("brief-"+t.Key, pipeline.InferSpec{
				Binding: model.Binding{Model: "gpt-5.4"},
				Prefix:  briefPrefix,
				System:  "You write partner state briefs for business development. Respond with a single JSON object and nothing else.",
				// Identity and the counted facts are baked in at build time:
				// a reduce output carries only the aggregate text, and these
				// are not things a model should be asked to remember.
				Prompt: fmt.Sprintf(`PARTNER: %s   (also called: %s)
VERTICAL: %s
COUNTED FACTS: mentioned in %d day-files (%d message lines), %s → %s; day-files per year — %s
OTHER PARTNERS IN THIS VERTICAL: %s

ROLLED-UP HISTORY (whole relationship, from daily notes):
{{.output}}

RECENT MENTIONS, VERBATIM (most recent days, exactly as written — this is what decides current state):
%s

Write this partner's state as JSON.`,
					tmplSafe(t.Name), tmplSafe(strings.Join(aliasTail(t.Aliases), ", ")), tmplSafe(t.Vertical),
					len(t.Days), t.Lines, t.first(), t.last(), t.yearLine(),
					tmplSafe(siblings(kept, t)), tmplSafe(t.Recent)),
				MaxTokens: 3000,
				ParseJSON: true,
				Validate: func(r core.Record) error {
					if strings.TrimSpace(r.String("headline")) == "" {
						return fmt.Errorf("empty headline")
					}
					if strings.TrimSpace(r.String("stage")) == "" {
						return fmt.Errorf("empty stage")
					}
					return nil
				},
			})
	}
	return p
}

// eventLine compresses one day's findings about one partner into a single dense
// line — the unit the history reduce aggregates.
func eventLine(date string, m map[string]any) string {
	var parts []string
	if events, ok := m["events"].([]any); ok {
		for _, e := range events {
			em, ok := e.(map[string]any)
			if !ok {
				continue
			}
			what, _ := em["what"].(string)
			if strings.TrimSpace(what) == "" {
				continue
			}
			seg := what
			if kind, _ := em["kind"].(string); kind != "" {
				seg = "(" + kind + ") " + seg
			}
			if who := joinAny(em["ours"]); who != "none" {
				seg += " [ours: " + who + "]"
			}
			if who := joinAny(em["theirs"]); who != "none" {
				seg += " [theirs: " + who + "]"
			}
			parts = append(parts, seg)
		}
	}
	line := fmt.Sprintf("[%s] events: %s", date, orNone(strings.Join(parts, " · ")))
	if s, _ := m["sentiment"].(string); s != "" {
		line += " | sentiment: " + s
	}
	if notes, ok := m["satisfaction_notes"].([]any); ok && len(notes) > 0 {
		var sat []string
		for _, n := range notes {
			nm, ok := n.(map[string]any)
			if !ok {
				continue
			}
			claim, _ := nm["claim"].(string)
			if strings.TrimSpace(claim) == "" {
				continue
			}
			basis, _ := nm["basis"].(string)
			if basis == "" {
				basis = "unstated"
			}
			sat = append(sat, fmt.Sprintf("%s (%s)", claim, basis))
		}
		if len(sat) > 0 {
			line += " | satisfaction: " + strings.Join(sat, " · ")
		}
	}
	for field, key := range map[string]string{"metrics": "metrics", "asks": "asks", "commitments": "commitments"} {
		if got := joinAny(m[key]); got != "none" {
			line += " | " + field + ": " + got
		}
	}
	return line
}

func orNone(s string) string {
	if strings.TrimSpace(s) == "" {
		return "none recorded"
	}
	return s
}

func aliasTail(aliases []string) []string {
	if len(aliases) <= 1 {
		return []string{"—"}
	}
	return aliases[1:]
}

// siblings lists the other partners in the same vertical, so a brief can place
// one relationship against the portfolio it competes inside.
func siblings(kept []tracked, self tracked) string {
	var out []string
	for _, t := range kept {
		if t.Vertical != self.Vertical || t.Key == self.Key {
			continue
		}
		out = append(out, fmt.Sprintf("%s (%d day-files, last %s)", t.Name, len(t.Days), t.last()))
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, "; ")
}

// buildPortfolioPipeline is run 3: one portfolio read per vertical over the
// partner states that run 2 produced.
func buildPortfolioPipeline(recs []core.Record, verticals []string) *pipeline.Pipeline {
	p := pipeline.New("partner-portfolio")
	src := p.FromRecords("partner-states", recs)
	for _, v := range verticals {
		v := v
		src.
			Filter("only-"+slugify(v), func(r core.Record) (bool, error) {
				return r.String("vertical") == v, nil
			}, pipeline.WithVersion("v1")).
			ReduceAI("portfolio-"+slugify(v), pipeline.ReduceAISpec{
				Binding: model.Binding{Model: "gpt-5.4-mini"},
				System:  "You brief a business-development lead on their whole partner portfolio in one vertical.",
				Prompt: fmt.Sprintf(`Partner states in the %q vertical ({{.Count}} inputs):

{{range .Items}}{{.}}

---

{{end}}
Write a markdown portfolio read:

## Where the portfolio stands
## Who needs attention first
<ranked, with the reason>
## Patterns across partners
<what recurs — pricing, quality, payment, responsiveness>
## Concentration and exposure
<who we depend on, and what happens if they slip>
## What to do this week

Reference partner names and dates. Up to ~450 words.`, tmplSafe(v)),
				FanIn:     8,
				MaxTokens: 1400,
				ItemField: "item",
			})
	}
	return p
}

// ---------------------------------------------------------------------- main

func main() {
	home, _ := os.UserHomeDir()
	messages := flag.String("messages", filepath.Join(home, "Desktop/google-chat/messages"), "root directory: <vertical>/<date>.jsonl")
	out := flag.String("out", "atlas", "output directory for the generated states")
	rosterPath := flag.String("roster", "", "load a frozen roster.json instead of discovering one (skips run 1)")
	names := flag.String("names", "", "optional file of extra people to pseudonymize: one person per line, comma-separated aliases")
	keyfile := flag.Bool("keyfile", false, "also write people-key.csv mapping pseudonyms back to the -names aliases")
	budget := flag.Float64("budget", 20, "hard cost cap in USD for the history run")
	scanBudget := flag.Float64("scan-budget", 3, "hard cost cap in USD for the roster run")
	stride := flag.Int("sample", 10, "roster discovery reads every Nth day per vertical")
	top := flag.Int("top", 8, "brief at most N partners per vertical, ranked by activity (0 = all)")
	minDays := flag.Int("min-days", 5, "skip partners mentioned in fewer than N day-files")
	recent := flag.Int("recent", 6, "quote the N most recent mentioning days verbatim in each brief")
	minHeb := flag.Int("min-mentions", 3, "a Hebrew @name must appear N times to be treated as a person")
	workers := flag.Int("workers", 8, "concurrent workers")
	rpm := flag.Int("rpm", 200, "per-model requests-per-minute admission limit")
	since := flag.String("since", "", "only include dates >= this (YYYY-MM-DD)")
	until := flag.String("until", "", "only include dates <= this (YYYY-MM-DD)")
	last := flag.Int("last", 0, "only include the N most recent days per vertical (0 = all)")
	explain := flag.Bool("explain", false, "project the history run's cost and exit without spending it")
	state := flag.String("state", os.Getenv("LOOM_STATE"), "state dir for cache/resume (recommended for large runs)")
	addr := flag.String("addr", "localhost:8078", "address for the constellation view (empty to disable)")
	flag.Parse()

	key := os.Getenv("OPENAI_API_KEY")
	if key == "" {
		log.Fatal("set OPENAI_API_KEY to run this pipeline")
	}

	files, verticals, err := discover(*messages, *since, *until, *last)
	if err != nil {
		log.Fatal(err)
	}
	if len(files) == 0 {
		log.Fatalf("no <vertical>/<date>.jsonl files under %s", *messages)
	}
	fmt.Printf("corpus: %d day-files across %d verticals: %s\n", len(files), len(verticals), strings.Join(verticals, ", "))

	// One pass to learn who is who, so a colleague reads the same across four
	// years of history; then one pass to render scrubbed transcripts.
	scrub, err := indexPeople(files, *minHeb)
	if err != nil {
		log.Fatal(err)
	}
	if *names != "" {
		groups, err := loadNameGroups(*names)
		if err != nil {
			log.Fatal(err)
		}
		scrub.groups = groups
		fmt.Printf("pseudonymizing %d supplied people plus every @mention\n", len(groups))
	}
	docs, err := loadDays(files, scrub)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("loaded %d non-empty days, %d team pseudonyms in play\n", len(docs), len(scrub.knownHeb)+len(scrub.groups))

	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	if *keyfile && len(scrub.groups) > 0 {
		var b strings.Builder
		b.WriteString("pseudonym,aliases\n")
		for _, g := range scrub.groups {
			b.WriteString(g.Label + "," + strings.Join(g.Aliases, " / ") + "\n")
		}
		if err := os.WriteFile(filepath.Join(*out, "people-key.csv"), []byte(b.String()), 0o600); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("wrote %s (keep it local)\n", filepath.Join(*out, "people-key.csv"))
	}

	reg := model.NewRegistry()
	if err := openai.RegisterDefaults(reg, model.Limits{RequestsPerMinute: *rpm}); err != nil {
		log.Fatal(err)
	}

	var mu sync.Mutex
	var calls int
	var cost float64
	progress := func(e observe.Event) {
		if e.Type != observe.ModelCalled && e.Type != observe.CacheHit {
			return
		}
		mu.Lock()
		calls++
		cost += e.Usage.CostUSD
		if calls%100 == 0 {
			fmt.Printf("  %d model calls, $%.4f so far (stage %s)\n", calls, cost, e.Stage)
		}
		mu.Unlock()
	}

	// Constellation view: all three runs stream to the same page and are held
	// as three skies in one universe, so the roster run stays inspectable while
	// the history run fills in. Press `u` for the universe, `,`/`.` to move.
	handle := progress
	var v *viz.Server
	var vizURL string
	if *addr != "" {
		v = viz.New()
		url, err := v.Start(*addr)
		if err != nil {
			log.Fatal(err)
		}
		vizURL = url
		fmt.Printf("constellation view: %s\n", vizURL)
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
		if v.AwaitViewer(waitCtx) {
			time.Sleep(800 * time.Millisecond) // a beat, so the empty sky is visible first
		}
		cancelWait()
		handle = func(e observe.Event) {
			v.Handle(e)
			progress(e)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	base := []loom.Option{
		loom.WithRegistry(reg),
		loom.WithSecrets(map[security.SecretRef]string{openai.DefaultSecretRef: key}),
		loom.WithWorkers(*workers),
		loom.WithEventHandler(handle),
	}
	if *state != "" {
		base = append(base, loom.WithStateDir(*state))
	}

	// ---- run 1: the roster (or the frozen one from disk) ----
	var ros rosterFile
	var report1 string
	if *rosterPath != "" {
		raw, err := os.ReadFile(*rosterPath)
		if err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(raw, &ros); err != nil {
			log.Fatalf("parse %s: %v", *rosterPath, err)
		}
		fmt.Printf("roster: loaded %d verticals from %s (run 1 skipped)\n", len(ros.Verticals), *rosterPath)
	} else {
		samples := sample(docs, *stride)
		fmt.Printf("roster discovery: scanning %d of %d days (every %dth per vertical)\n", len(samples), len(docs), *stride)
		res1, err := loom.Run(ctx, buildRosterPipeline(samples, verticals),
			append(append([]loom.Option{}, base...),
				loom.WithRunBudget(core.Budget{MaxCostUSD: *scanBudget}),
				loom.WithContinueOnError())...)
		if err != nil && res1 == nil {
			log.Fatal(err)
		}
		if err != nil {
			fmt.Printf("roster run ended with error: %v — using what completed\n", err)
		}
		ros = rosterFile{Source: *messages, Verticals: map[string][]partner{}}
		for _, vert := range verticals {
			recs := res1.StageOutputs["roster-"+slugify(vert)]
			if len(recs) == 0 {
				fmt.Printf("  no roster produced for %s\n", vert)
				continue
			}
			ps := mergeNearDuplicates(parseRoster(recs[0].String("output")))
			if len(ps) == 0 {
				fmt.Printf("  roster for %s parsed to nothing; raw output kept in the run report\n", vert)
				continue
			}
			ros.Verticals[vert] = ps
			fmt.Printf("  %s: %d partners — %s\n", vert, len(ps), strings.Join(partnerNames(ps), ", "))
		}
		report1 = res1.Report.String()
	}
	rosterOut := filepath.Join(*out, "roster.json")
	if blob, err := json.MarshalIndent(ros, "", "  "); err == nil {
		if err := os.WriteFile(rosterOut, blob, 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("wrote %s — edit it and re-run with -roster to fix the entity list\n", rosterOut)
	}
	if len(ros.Verticals) == 0 {
		log.Fatal("no partners discovered; nothing to brief")
	}

	// ---- measure every partner against the whole corpus, in Go ----
	kept, dropped, hitDocs := track(docs, ros, *top, *minDays, *recent)
	if len(kept) == 0 {
		log.Fatalf("no partner cleared -min-days=%d; loosen it or fix roster.json", *minDays)
	}
	fmt.Printf("\nbriefing %d partners across %d verticals; %d of %d day-files mention one\n",
		len(kept), len(ros.Verticals), len(hitDocs), len(docs))
	for _, vert := range verticals {
		var line []string
		for _, t := range kept {
			if t.Vertical == vert {
				line = append(line, fmt.Sprintf("%s=%s(%dd)", t.Slug, t.Name, len(t.Days)))
			}
		}
		if len(line) > 0 {
			fmt.Printf("  %s: %s\n", vert, strings.Join(line, " "))
		}
	}
	if len(dropped) > 0 {
		// Say what was left out: a partial atlas that looks complete is worse
		// than a smaller one that admits its edges.
		fmt.Printf("  not briefed (%d): %s\n", len(dropped), strings.Join(dropped, "; "))
	}

	history := buildHistoryPipeline(hitDocs, kept)

	if *explain {
		proj, err := loom.Explain(history, append(append([]loom.Option{}, base...),
			loom.WithRunBudget(core.Budget{MaxCostUSD: *budget}),
			// day-extract parses its output as JSON, so the fields the stages
			// below it filter and template on come out of the model. Naming one
			// makes the projection a computation instead of a guess.
			loom.WithStageSample("day-extract", map[string]any{
				"partners": []any{map[string]any{
					"partner": kept[0].Name, "sentiment": "neutral",
					"events": []any{map[string]any{"what": "call about volumes", "kind": "commercial"}},
				}},
			}))...)
		if err != nil {
			log.Fatalf("projection failed: %v", err)
		}
		fmt.Print("\n", proj, "\n")
		return
	}

	// ---- run 2: history and state, one branch per partner ----
	res2, err := loom.Run(ctx, history,
		append(append([]loom.Option{}, base...),
			loom.WithRunBudget(core.Budget{MaxCostUSD: *budget}),
			loom.WithContinueOnError())...)
	if err != nil && res2 == nil {
		log.Fatal(err)
	}
	if err != nil {
		fmt.Printf("history run ended with error: %v (spent $%.4f) — writing what completed\n", err, res2.Spent.CostUSD)
	}
	if n := len(res2.Failures); n > 0 {
		fmt.Printf("  %d task(s) dead-lettered; see the run report\n", n)
	}

	// Names the model produced that no roster alias covers: the honest measure
	// of what the roster missed, and the first thing to fix in roster.json.
	unmatched := map[string]int{}
	for _, r := range res2.StageOutputs["split-partners"] {
		if matched, _ := r.Data["matched"].(bool); !matched {
			unmatched[r.String("vertical")+"/"+r.String("raw_name")]++
		}
	}

	var states []partnerState
	var portfolioRecs []core.Record
	for _, t := range kept {
		recs := res2.StageOutputs["brief-"+t.Key]
		if len(recs) == 0 {
			fmt.Printf("  no state produced for %s/%s (skipping)\n", t.Vertical, t.Name)
			continue
		}
		s, err := stateFrom(recs[0], t)
		if err != nil {
			fmt.Printf("  state for %s/%s did not parse: %v\n", t.Vertical, t.Name, err)
			continue
		}
		states = append(states, s)

		dir := filepath.Join(*out, slugify(t.Vertical))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatal(err)
		}
		name := t.Slug
		if sl := slugify(t.Name); sl != "" {
			name += "-" + sl
		}
		path := filepath.Join(dir, name+".md")
		if err := os.WriteFile(path, []byte(renderState(s)), 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("wrote %s\n", path)

		portfolioRecs = append(portfolioRecs, core.NewRecord("state-"+t.Key, map[string]any{
			"vertical": t.Vertical,
			"item":     portfolioItem(s),
		}))
	}
	if len(states) == 0 {
		log.Fatal("no partner states produced")
	}

	if blob, err := json.MarshalIndent(states, "", "  "); err == nil {
		if err := os.WriteFile(filepath.Join(*out, "atlas.json"), blob, 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("wrote %s (%d states, machine-readable)\n", filepath.Join(*out, "atlas.json"), len(states))
	}

	// ---- run 3: one portfolio read per vertical ----
	activeVerticals := activeIn(states)
	res3, err := loom.Run(ctx, buildPortfolioPipeline(portfolioRecs, activeVerticals),
		append(append([]loom.Option{}, base...),
			loom.WithRunBudget(core.Budget{MaxCostUSD: 3}),
			loom.WithContinueOnError())...)
	if err != nil && res3 == nil {
		log.Fatal(err)
	}
	if err != nil {
		fmt.Printf("portfolio run ended with error: %v — writing what completed\n", err)
	}
	portfolios := map[string]string{}
	for _, vert := range activeVerticals {
		recs := res3.StageOutputs["portfolio-"+slugify(vert)]
		if len(recs) == 0 {
			continue
		}
		text := recs[0].String("output")
		portfolios[vert] = text
		path := filepath.Join(*out, slugify(vert), "_portfolio.md")
		body := fmt.Sprintf("# %s — partner portfolio\n\n%s\n\n%s\n", vert, text, verticalIndex(states, vert))
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("wrote %s\n", path)
	}

	indexPath := filepath.Join(*out, "README.md")
	if err := os.WriteFile(indexPath, []byte(atlasIndex(states, activeVerticals, dropped, unmatched)), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %s\n", indexPath)

	report := strings.TrimSpace(report1 + "\n" + res2.Report.String() + "\n" + res3.Report.String())
	if err := os.WriteFile(filepath.Join(*out, "run-report.txt"), []byte(report+"\n"), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n--- run report ---\n%s\ntotal spend: $%.4f\n", report, res2.Spent.CostUSD+res3.Spent.CostUSD)
	if len(unmatched) > 0 {
		fmt.Printf("\n%d partner names the roster does not cover (add them as aliases in roster.json and re-run with -roster):\n", len(unmatched))
		for _, line := range topKeys(unmatched, 15) {
			fmt.Printf("  %s\n", line)
		}
	}

	if v != nil {
		fmt.Printf("\nall runs finished — still serving %s\n"+
			"  press `u` for the universe: roster, history and portfolio runs, each still open to inspect\n"+
			"  (Ctrl-C to exit)\n", vizURL)
		<-ctx.Done()
		_ = v.Close()
	}
}

// stateFrom re-reads the model's JSON into the typed state, then overwrites
// everything Go knows better: identity and the counted evidence base.
func stateFrom(r core.Record, t tracked) (partnerState, error) {
	blob, err := json.Marshal(r.Data)
	if err != nil {
		return partnerState{}, err
	}
	var s partnerState
	if err := json.Unmarshal(blob, &s); err != nil {
		return partnerState{}, err
	}
	s.Vertical, s.Partner, s.Aliases, s.Kind = t.Vertical, t.Name, t.Aliases, t.Kind
	s.Activity.DayFiles = len(t.Days)
	s.Activity.Mentions = t.Lines
	s.Activity.FirstSeen, s.Activity.LastSeen = t.first(), t.last()
	s.Activity.ByYear = t.ByYear
	if s.Satisfaction.Score != nil && (*s.Satisfaction.Score < 1 || *s.Satisfaction.Score > 5) {
		s.Satisfaction.Score = nil
	}
	return s, nil
}

func portfolioItem(s partnerState) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### %s (%s)\nHeadline: %s\nStage: %s (%s) · trajectory %s · satisfaction %s\nActivity: %d day-files, %s → %s\n",
		s.Partner, orDash(s.Kind), s.Headline, orDash(s.Stage), orDash(s.StageBasis), orDash(s.Trajectory),
		s.satisfactionLine(), s.Activity.DayFiles, orDash(s.Activity.FirstSeen), orDash(s.Activity.LastSeen))
	for _, r := range s.Risks {
		fmt.Fprintf(&b, "Risk (%s): %s\n", orDash(r.Severity), r.What)
	}
	for _, a := range s.NextActions {
		fmt.Fprintf(&b, "Next (%s): %s\n", orDash(a.Urgency), a.Action)
	}
	for _, o := range s.OpenThreads {
		fmt.Fprintf(&b, "Open: %s (%s)\n", o.What, orDash(o.Status))
	}
	return b.String()
}

func verticalIndex(states []partnerState, vertical string) string {
	var b strings.Builder
	b.WriteString("## Partners\n\n| Partner | Stage | Trajectory | Satisfaction | Last seen | Headline |\n|---|---|---|---|---|---|\n")
	for _, s := range states {
		if s.Vertical != vertical {
			continue
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n", s.Partner, orDash(s.Stage), orDash(s.Trajectory),
			s.satisfactionLine(), orDash(s.Activity.LastSeen), cell(s.Headline))
	}
	return b.String()
}

func atlasIndex(states []partnerState, verticals []string, dropped []string, unmatched map[string]int) string {
	var b strings.Builder
	b.WriteString("# Partner atlas\n\nPer vertical, per partner: where the relationship stands, who runs it, how satisfied they look and on whose word, what is open, what to do next.\n\n")
	for _, v := range verticals {
		fmt.Fprintf(&b, "## %s\n\n[portfolio read](%s/_portfolio.md)\n\n%s\n", v, slugify(v), verticalIndex(states, v))
	}
	b.WriteString("\n## Coverage\n\n")
	fmt.Fprintf(&b, "%d partner states across %d verticals.\n\n", len(states), len(verticals))
	if len(dropped) > 0 {
		fmt.Fprintf(&b, "Discovered but not briefed (%d): %s\n\n", len(dropped), strings.Join(dropped, "; "))
	}
	if len(unmatched) > 0 {
		fmt.Fprintf(&b, "Names the extraction saw that the roster does not cover — add them as aliases in `roster.json` and re-run with `-roster`:\n\n")
		for _, line := range topKeys(unmatched, 25) {
			fmt.Fprintf(&b, "- %s\n", line)
		}
		b.WriteString("\n")
	}
	b.WriteString("Satisfaction carries a basis per evidence item: `partner_direct` is the partner's own word, `internal_secondhand` is a colleague relaying it, `inferred` is nobody's word. People are stable pseudonyms (TM-xxxx).\n")
	return b.String()
}

func topKeys(m map[string]int, n int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}
		return keys[i] < keys[j]
	})
	if len(keys) > n {
		keys = keys[:n]
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, fmt.Sprintf("%s (%d days)", k, m[k]))
	}
	return out
}

func activeIn(states []partnerState) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range states {
		if !seen[s.Vertical] {
			seen[s.Vertical] = true
			out = append(out, s.Vertical)
		}
	}
	sort.Strings(out)
	return out
}

func partnerNames(ps []partner) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, p.Canonical)
	}
	return out
}
