package main

// Folding raw job labels onto the canonical taxonomy.
//
// The census stage reads each day independently, so the same job comes back as
// "pause losing keywords", "keyword pausing", "pausing bad kws" — three labels,
// one capability. Counting them separately is what makes a real pattern look
// like a long tail, so everything is matched onto the canonical list before any
// metric is computed. Labels nothing matches are kept as their own capability
// rather than dropped: an unmatched label is evidence the taxonomy is short, and
// it shows up in the match rate.

import (
	"sort"
	"strings"
)

type matcher struct {
	byFunction map[string][]capability
	all        []capability
	synth      map[string]capability
}

func newMatcher(caps []capability) *matcher {
	m := &matcher{byFunction: map[string][]capability{}, synth: map[string]capability{}}
	for _, c := range caps {
		c.Function = knownFunction(c.Function)
		if c.ID == "" {
			c.ID = slug(c.Name)
		}
		if c.ID == "" {
			continue
		}
		if !contains(c.Aliases, c.Name) && c.Name != "" {
			c.Aliases = append(c.Aliases, c.Name)
		}
		m.byFunction[c.Function] = append(m.byFunction[c.Function], c)
		m.all = append(m.all, c)
	}
	return m
}

// match folds one raw label onto a canonical capability. Candidates from the
// same job family are preferred: "budget" means something different to finance
// than to PPC, and the census already decided which family this observation is.
func (m *matcher) match(label, fn string) (capability, bool) {
	norm := normalizeLabel(label)
	if norm == "" {
		norm = "unspecified"
	}
	best, bestScore := capability{}, 0.0
	for _, pool := range [][]capability{m.byFunction[fn], m.all} {
		for _, c := range pool {
			s := aliasScore(norm, c)
			if c.Function != fn {
				s *= 0.8 // a cross-family match has to be clearly better to win
			}
			if s > bestScore {
				best, bestScore = c, s
			}
		}
		if bestScore >= 0.99 {
			break
		}
	}
	if bestScore >= 0.6 {
		return best, true
	}
	return m.synthesize(norm, fn), false
}

// synthesize keeps an unmatched label as a capability of its own, memoised so
// the same label lands in the same bucket across days.
func (m *matcher) synthesize(norm, fn string) capability {
	key := fn + "|" + norm
	if c, ok := m.synth[key]; ok {
		return c
	}
	c := capability{
		ID:          fn + ":" + slug(norm),
		Name:        norm,
		Function:    fn,
		Synthesized: true,
		Summary:     "seen in the corpus but not folded into the taxonomy",
	}
	m.synth[key] = c
	return c
}

func aliasScore(norm string, c capability) float64 {
	best := 0.0
	for _, a := range c.Aliases {
		an := normalizeLabel(a)
		if an == "" {
			continue
		}
		switch {
		case an == norm:
			return 1.0
		case containsWord(norm, an), containsWord(an, norm):
			if s := 0.85; s > best {
				best = s
			}
		default:
			if s := jaccard(tokens(norm), tokens(an)); s > best {
				best = s
			}
		}
	}
	return best
}

// containsWord reports whether needle appears in hay on token boundaries, so
// "bid" does not match "forbidden".
func containsWord(hay, needle string) bool {
	if needle == "" || len(needle) < 3 || len(needle) > len(hay) {
		return false
	}
	h, n := " "+hay+" ", " "+needle+" "
	return strings.Contains(h, n)
}

func tokens(s string) map[string]bool {
	out := map[string]bool{}
	for _, t := range strings.Fields(s) {
		if len(t) > 2 && !stopword[t] {
			out[t] = true
		}
	}
	return out
}

var stopword = map[string]bool{
	"the": true, "and": true, "for": true, "with": true, "from": true,
	"our": true, "new": true, "per": true, "into": true, "that": true, "this": true,
}

func jaccard(a, b map[string]bool) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	inter := 0
	for k := range a {
		if b[k] {
			inter++
		}
	}
	union := len(a) + len(b) - inter
	if union == 0 {
		return 0
	}
	return float64(inter) / float64(union)
}

func normalizeLabel(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	prevSpace := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevSpace = false
		case r > 127: // keep non-Latin script intact — the corpus is bilingual
			b.WriteRune(r)
			prevSpace = false
		default:
			if !prevSpace {
				b.WriteRune(' ')
				prevSpace = true
			}
		}
	}
	return strings.TrimSpace(b.String())
}

func slug(s string) string {
	s = normalizeLabel(s)
	s = strings.ReplaceAll(s, " ", "-")
	if len(s) > 48 {
		s = s[:48]
		if i := strings.LastIndex(s, "-"); i > 20 {
			s = s[:i]
		}
	}
	return strings.Trim(s, "-")
}

// dedupeLabels collapses observations to unique labels with counts, which is
// what the taxonomy stage reads. On the real corpus this is the difference
// between a few hundred items and fifteen thousand: the model only needs to see
// each distinct phrasing once, together with how often and where it occurs.
type labelCount struct {
	Label    string
	Function string
	Count    int
	Spaces   []string
}

func dedupeLabels(obs []jobObs) []labelCount {
	type acc struct {
		label  string
		fn     string
		n      int
		spaces map[string]bool
	}
	byKey := map[string]*acc{}
	for _, o := range obs {
		fn := knownFunction(o.Function)
		norm := normalizeLabel(o.Label)
		if norm == "" {
			continue
		}
		key := fn + "|" + norm
		a := byKey[key]
		if a == nil {
			a = &acc{label: norm, fn: fn, spaces: map[string]bool{}}
			byKey[key] = a
		}
		a.n++
		a.spaces[o.Space] = true
	}
	out := make([]labelCount, 0, len(byKey))
	for _, a := range byKey {
		spaces := make([]string, 0, len(a.spaces))
		for s := range a.spaces {
			spaces = append(spaces, s)
		}
		sort.Strings(spaces)
		out = append(out, labelCount{Label: a.label, Function: a.fn, Count: a.n, Spaces: spaces})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].Function != out[j].Function {
			return out[i].Function < out[j].Function
		}
		return out[i].Label < out[j].Label
	})
	return out
}
