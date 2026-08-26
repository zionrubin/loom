package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/zionrubin/loom/model"
)

// brain is the deterministic mock behind all four of the desk's operations.
//
// It recognizes each by something structural in the request — the instruction
// the desk puts in the prefix, the shape of the aggregation prompt — rather
// than by guessing, so the same message always produces the same note and the
// same context always produces the same answer. Running the example twice
// produces identical output, which is what makes the cache numbers in the
// report mean something.
func brain(req model.Request) (string, error) {
	switch {
	case strings.Contains(req.Prefix, "Reply with JSON"):
		return note(req.Prompt), nil
	case strings.Contains(req.Prompt, "observations from one stretch"):
		return digest(req.Prompt), nil
	case strings.Contains(req.Prompt, "Compress these"):
		return brief(req.Prompt), nil
	default:
		return respond(req), nil
	}
}

// keywords maps a phrase in a message to how the desk should file it. A real
// model reads; this one matches, which is enough to make the pipeline's shape
// visible without pretending to be intelligent.
var keywords = []struct {
	phrase, kind, subject string
}{
	{"blocked", "problem", "a blocker"},
	{"outage", "problem", "an outage"},
	{"failing", "problem", "failures"},
	{"escalat", "decision", "an escalation"},
	{"we will", "decision", "a commitment"},
	{"ship", "decision", "a commitment"},
	{"renewal", "fact", "the renewal"},
	{"invoice", "request", "billing"},
	{"prefer", "preference", "a preference"},
	{"can you", "request", "a request"},
	{"need", "request", "a request"},
}

// pleasantries are what a desk should not spend a line of its context on.
var pleasantries = []string{"hi ", "hello", "thanks", "thank you", "morning", "no worries", "sure thing"}

// note answers the per-message stage: what, if anything, is worth remembering.
func note(prompt string) string {
	said := strings.TrimSpace(prompt)
	if i := strings.Index(said, ":\n"); i >= 0 {
		said = strings.TrimSpace(said[i+2:])
	}
	who := "someone"
	if i := strings.Index(prompt, ", "); i >= 0 {
		if j := strings.Index(prompt[i+2:], " said"); j >= 0 {
			who = prompt[i+2 : i+2+j]
		}
	}

	lower := strings.ToLower(said)
	for _, p := range pleasantries {
		if strings.HasPrefix(lower, p) && len(said) < 40 {
			return `{"kind": "noise", "note": "", "subject": "small talk"}`
		}
	}
	kind, subject := "fact", "the account"
	for _, k := range keywords {
		if strings.Contains(lower, k.phrase) {
			kind, subject = k.kind, k.subject
			break
		}
	}
	return fmt.Sprintf(`{"kind": %q, "note": %q, "subject": %q}`,
		kind, fmt.Sprintf("The %s reports: %s", who, said), subject)
}

// digest answers the per-slice aggregation: one entry for the context.
//
// It keeps the kind each observation was filed under, because that is what a
// later question is answered by — "what has been promised" is a question about
// decisions, and an entry that lost the word has lost the answer.
func digest(prompt string) string {
	obs := bullets(prompt)
	if len(obs) == 0 {
		return ""
	}
	// Ordered by the timestamp each observation carries, because a pane holds
	// what finished rather than what was said first, and an exchange out of
	// order is a different exchange.
	sort.SliceStable(obs, func(i, j int) bool { return obs[i] < obs[j] })

	var b strings.Builder
	for i, o := range obs {
		if i > 0 {
			b.WriteString(" ")
		}
		if k := kindOf(o); k != "" {
			b.WriteString(strings.ToUpper(k[:1]) + k[1:] + ": ")
		}
		b.WriteString(strings.TrimSuffix(said(o), "."))
		b.WriteString(".")
	}
	return b.String()
}

// kindOf reads back the classification the Keep stage stamped on an
// observation.
func kindOf(observation string) string {
	i, j := strings.Index(observation, "("), strings.Index(observation, ") ")
	if i < 0 || j < i {
		return ""
	}
	return observation[i+1 : j]
}

// brief answers a compaction: everything folded, kept by subject and shortened.
func brief(prompt string) string {
	entries := bullets(prompt)
	subjects := map[string]int{}
	for _, e := range entries {
		for _, k := range keywords {
			if strings.Contains(strings.ToLower(e), k.phrase) {
				subjects[k.subject]++
			}
		}
	}
	names := make([]string, 0, len(subjects))
	for s := range subjects {
		names = append(names, fmt.Sprintf("%s (%d)", s, subjects[s]))
	}
	sort.Strings(names)

	var b strings.Builder
	fmt.Fprintf(&b, "Earlier in this feed, across %d folded entries: ", len(entries))
	if len(names) == 0 {
		b.WriteString("nothing that resolved to a subject.")
		return b.String()
	}
	b.WriteString(strings.Join(names, ", "))
	b.WriteString(".")
	return b.String()
}

// respond answers a question, out of the context it was given and nothing else.
//
// It matches the question's words against the lines of the maintained context
// and quotes what it finds. That is a poor model and an excellent test: an
// answer can only contain what the context contained, so the output of this
// example is evidence that the context reached the model rather than a claim
// that it did.
func respond(req model.Request) string {
	lines := entries(req.Prefix)
	if len(lines) == 0 {
		return "I have nothing on file yet."
	}
	terms := terms(question(req.Prompt))

	type hit struct {
		line  string
		score int
	}
	var hits []hit
	for _, line := range lines {
		score := 0
		lower := strings.ToLower(line)
		for _, t := range terms {
			if strings.Contains(lower, t) {
				score++
			}
		}
		if score > 0 {
			hits = append(hits, hit{line: line, score: score})
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })

	var b strings.Builder
	if len(hits) == 0 {
		fmt.Fprintf(&b, "Nothing in the %d entries I hold speaks to that.", len(lines))
		return b.String()
	}
	fmt.Fprintf(&b, "From %d of the %d entries I hold:", min(len(hits), 3), len(lines))
	for _, h := range hits[:min(len(hits), 3)] {
		b.WriteString("\n  · ")
		b.WriteString(clip(strings.ReplaceAll(h.line, "\n", " "), 200))
	}
	return b.String()
}

// --- reading the prompts back --------------------------------------------

// bullets pulls an aggregation prompt's list back apart.
func bullets(prompt string) []string {
	var out []string
	for _, line := range strings.Split(prompt, "\n") {
		if strings.HasPrefix(line, "- ") {
			out = append(out, strings.TrimSpace(strings.TrimPrefix(line, "- ")))
		}
	}
	return out
}

// said strips the timestamp, speaker and kind the Keep stage stamped on an
// observation, leaving what the reading model actually wrote.
func said(observation string) string {
	if i := strings.Index(observation, ") "); i >= 0 {
		return strings.TrimSpace(observation[i+2:])
	}
	return observation
}

// entries splits the maintained context back into the lines it is made of. The
// context arrives as the continuation at the front of the prompt, rendered as
// tagged segments.
func entries(prefix string) []string {
	var (
		out  []string
		cur  strings.Builder
		open bool
	)
	for _, line := range strings.Split(prefix, "\n") {
		switch {
		case line == "<entry>" || line == "<brief>":
			open, cur = true, strings.Builder{}
		case line == "</entry>" || line == "</brief>":
			if open && strings.TrimSpace(cur.String()) != "" {
				out = append(out, strings.TrimSpace(cur.String()))
			}
			open = false
		case open:
			if cur.Len() > 0 {
				cur.WriteString("\n")
			}
			cur.WriteString(line)
		}
	}
	return out
}

func question(prompt string) string {
	if i := strings.LastIndex(prompt, "Question: "); i >= 0 {
		return prompt[i+len("Question: "):]
	}
	return prompt
}

// stopWords are the words a keyword match should ignore, because matching on
// them would make every line a hit.
var stopWords = map[string]bool{
	"what": true, "which": true, "who": true, "when": true, "where": true,
	"the": true, "and": true, "are": true, "is": true, "on": true, "in": true,
	"of": true, "to": true, "for": true, "at": true, "by": true, "a": true,
	"has": true, "have": true, "been": true, "should": true, "look": true,
	"first": true, "right": true, "now": true, "going": true,
	"customers": true, "that": true, "with": true, "about": true,
}

// synonyms map how a person asks onto how the desk wrote it down.
//
// It stands in for the one thing a real model would do here without being
// told: recognize that "what has been promised" is a question about the
// entries filed as decisions. A keyword matcher has to be given the mapping.
var synonyms = map[string][]string{
	"blocked":  {"problem", "blocked"},
	"wrong":    {"problem", "outage", "failing"},
	"blocker":  {"problem"},
	"broken":   {"problem"},
	"failing":  {"problem", "failing"},
	"promised": {"decision", "will"},
	"promise":  {"decision"},
	"commit":   {"decision"},
	"deadline": {"decision", "friday", "tomorrow"},
	"waiting":  {"problem", "request"},
	"want":     {"request", "preference"},
	"asked":    {"request"},
	"urgent":   {"problem", "outage"},
	"engineer": {"problem", "outage"},
	"oncall":   {"problem", "outage"},
}

func terms(q string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(w string) {
		if w != "" && !seen[w] {
			seen[w] = true
			out = append(out, w)
		}
	}
	for _, w := range strings.FieldsFunc(strings.ToLower(q), func(r rune) bool {
		return !('a' <= r && r <= 'z') && !('0' <= r && r <= '9')
	}) {
		if len(w) <= 2 || stopWords[w] {
			continue
		}
		add(w)
		for _, syn := range synonyms[w] {
			add(syn)
		}
	}
	return out
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
