package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	loom "github.com/zionrubin/loom"
	"github.com/zionrubin/loom/core"
	"github.com/zionrubin/loom/model"
)

func writeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	days := map[string][]string{
		"alpha/2026-01-01.jsonl": {
			`{"sender":{"name":"users/111","type":"HUMAN"},"createTime":"2026-01-01T09:15:00.000000Z","text":"CPA is up 12% this week, contact bob@example.com for the sheet"}`,
			`{"sender":{"name":"users/222","type":"HUMAN"},"createTime":"2026-01-01T09:16:00.000000Z","text":"Looking into it, likely the new landing page"}`,
		},
		"alpha/2026-01-02.jsonl": {
			`{"sender":{"name":"users/111","type":"HUMAN"},"createTime":"2026-01-02T10:00:00.000000Z","text":"Landing page rolled back, CPA recovering"}`,
		},
		"beta/2026-01-01.jsonl": {
			`{"sender":{"name":"users/333","type":"HUMAN"},"createTime":"2026-01-01T12:00:00.000000Z","text":"New partner signed, launch next week"}`,
		},
	}
	for rel, lines := range days {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func mockRegistry(t *testing.T) *model.Registry {
	t.Helper()
	reg := model.NewRegistry()
	handler := func(req model.Request) (string, error) {
		if strings.Contains(req.Prompt, `"summary"`) {
			return `{"summary":"busy day around CPA","topics":["cpa"],"signals":["cpa spike"]}`, nil
		}
		return "aggregate report", nil
	}
	for id, tier := range map[string]model.Tier{
		"gpt-5.4-nano": model.TierFast,
		"gpt-5.4-mini": model.TierBalanced,
		"gpt-5.4":      model.TierDeep,
	} {
		if _, err := model.RegisterMock(reg, id, tier, model.WithHandler(handler)); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

func TestLoadDayAnonymizesAndRedacts(t *testing.T) {
	root := writeFixture(t)
	rec, err := loadDay(dayFile{Vertical: "alpha", Date: "2026-01-01", Path: filepath.Join(root, "alpha/2026-01-01.jsonl")})
	if err != nil {
		t.Fatal(err)
	}
	msgs := rec.String("messages")
	if strings.Contains(msgs, "bob@example.com") {
		t.Errorf("email not redacted: %s", msgs)
	}
	if strings.Contains(msgs, "users/111") {
		t.Errorf("sender ID not anonymized: %s", msgs)
	}
	if !strings.Contains(msgs, "09:15 S1:") || !strings.Contains(msgs, "09:16 S2:") {
		t.Errorf("unexpected transcript format: %s", msgs)
	}
	if rec.Data["count"].(int) != 2 {
		t.Errorf("count = %v, want 2", rec.Data["count"])
	}
}

func TestDiscoverClampsToDateRange(t *testing.T) {
	root := writeFixture(t)
	files, verticals, err := discover(root, "2026-01-02", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Vertical != "alpha" || files[0].Date != "2026-01-02" {
		t.Fatalf("files = %+v, want only alpha/2026-01-02", files)
	}
	if len(verticals) != 1 || verticals[0] != "alpha" {
		t.Fatalf("verticals = %v, want [alpha]", verticals)
	}
}

func TestDiscoverKeepsLastNDaysPerVertical(t *testing.T) {
	root := writeFixture(t)
	files, verticals, err := discover(root, "", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	// alpha has 2 days, beta has 1 — last=1 keeps the most recent of each.
	if len(files) != 2 {
		t.Fatalf("files = %+v, want 2 (one per vertical)", files)
	}
	if files[0].Vertical != "alpha" || files[0].Date != "2026-01-02" {
		t.Errorf("alpha kept %s, want 2026-01-02", files[0].Date)
	}
	if files[1].Vertical != "beta" || files[1].Date != "2026-01-01" {
		t.Errorf("beta kept %+v, want 2026-01-01", files[1])
	}
	if len(verticals) != 2 {
		t.Errorf("verticals = %v, want both", verticals)
	}
}

func TestPipelineProducesRollupPerVertical(t *testing.T) {
	root := writeFixture(t)
	files, verticals, err := discover(root, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(verticals) != 2 {
		t.Fatalf("verticals = %v, want 2", verticals)
	}

	res, err := loom.Run(context.Background(), buildPipeline(files, verticals),
		loom.WithRegistry(mockRegistry(t)),
		loom.WithWorkers(4),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range verticals {
		recs := res.StageOutputs["rollup-"+v]
		if len(recs) != 1 {
			t.Fatalf("rollup-%s: got %d records, want 1", v, len(recs))
		}
		if recs[0].String("output") != "aggregate report" {
			t.Errorf("rollup-%s output = %q", v, recs[0].String("output"))
		}
	}
}

func TestOverviewFusesRollups(t *testing.T) {
	rollups := []core.Record{
		core.NewRecord("rollup-alpha", map[string]any{"item": "## Vertical: alpha\n\nall good"}),
		core.NewRecord("rollup-beta", map[string]any{"item": "## Vertical: beta\n\nlaunching"}),
	}
	res, err := loom.Run(context.Background(), buildOverview(rollups),
		loom.WithRegistry(mockRegistry(t)),
		loom.WithWorkers(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Output) != 1 || res.Output[0].String("output") != "aggregate report" {
		t.Fatalf("overview output = %+v", res.Output)
	}
}
