package ops

import "testing"

func TestParseJSONObject(t *testing.T) {
	cases := []struct {
		in      string
		wantKey string
		wantErr bool
	}{
		{`{"a": 1}`, "a", false},
		{"Here you go:\n```json\n{\"a\": 1}\n```", "a", false},
		{"```\n{\"a\": 1}\n```\ntrailing prose", "a", false},
		{`prefix text {"a": 1} suffix`, "a", false},
		{`not json at all`, "", true},
		{`[1, 2, 3]`, "", true},
	}
	for _, c := range cases {
		got, err := ParseJSONObject(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseJSONObject(%q): expected error, got %v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseJSONObject(%q): %v", c.in, err)
			continue
		}
		if _, ok := got[c.wantKey]; !ok {
			t.Errorf("ParseJSONObject(%q): missing key %q in %v", c.in, c.wantKey, got)
		}
	}
}
