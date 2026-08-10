package main

// The blueprint UI: one self-contained page with the whole design embedded in
// it. No CDN, no fetch, no server needed beyond something that hands over the
// file — the point is that the artefact survives being e-mailed to someone.

import (
	_ "embed"
	"encoding/json"
	"strings"
)

//go:embed ui.html
var uiTemplate string

// jsonInHTML escapes the characters that could end a <script> element early or
// break a JavaScript string literal. Each replacement is a valid JSON escape,
// so the payload still parses as exactly the same value.
var jsonInHTML = strings.NewReplacer(
	"<", "\\u003c",
	">", "\\u003e",
	"&", "\\u0026",
	"\u2028", "\\u2028",
	"\u2029", "\\u2029",
)

func renderUI(bp blueprint) string {
	blob, err := json.Marshal(bp)
	if err != nil {
		blob = []byte(`{"error":"blueprint could not be encoded"}`)
	}
	return strings.Replace(uiTemplate, "__BLUEPRINT__", jsonInHTML.Replace(string(blob)), 1)
}
