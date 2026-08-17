package stream_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zionrubin/loom/stream"
)

func TestFileStoreRoundTripsACheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := stream.NewFileStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	ck := stream.Checkpoint{
		JobID: "desk", Epoch: 4, Time: time.Now().UTC(),
		Watermark: base,
		Positions: map[string]stream.Position{"events/part-0": {Offset: 4096}},
		Windows:   map[string]json.RawMessage{"per-minute": json.RawMessage(`{"watermark":"x"}`)},
		Progress:  stream.Progress{Records: 900, Panes: 12},
	}
	if err := store.Save(context.Background(), ck); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, ok, err := store.Load(context.Background(), "desk")
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if got.Epoch != 4 || got.Progress.Records != 900 {
		t.Fatalf("checkpoint = %+v", got)
	}
	if got.Positions["events/part-0"].Offset != 4096 {
		t.Fatalf("positions = %+v", got.Positions)
	}
	if string(got.Windows["per-minute"]) != `{"watermark":"x"}` {
		t.Fatalf("window state = %s", got.Windows["per-minute"])
	}
	if !got.Watermark.Equal(base) {
		t.Fatalf("watermark = %s, want %s", got.Watermark, base)
	}
}

func TestFileStoreLoadOfAnUnknownJobIsNotAnError(t *testing.T) {
	store, err := stream.NewFileStore(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := store.Load(context.Background(), "never-ran")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if ok {
		t.Fatal("a job that never checkpointed should report no checkpoint")
	}
}

func TestFileStoreKeepsTheLastFewAndPrunesTheRest(t *testing.T) {
	dir := t.TempDir()
	store, err := stream.NewFileStore(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	for epoch := int64(1); epoch <= 5; epoch++ {
		if err := store.Save(context.Background(), stream.Checkpoint{JobID: "j", Epoch: epoch}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(dir, "j"))
	if err != nil {
		t.Fatal(err)
	}
	checkpoints := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".json" {
			checkpoints++
		}
	}
	if checkpoints != 2 {
		t.Fatalf("kept %d checkpoints, want 2", checkpoints)
	}
	got, _, err := store.Load(context.Background(), "j")
	if err != nil || got.Epoch != 5 {
		t.Fatalf("latest = %d (err %v), want 5", got.Epoch, err)
	}
}

func TestFileStoreFallsBackWhenThePointerIsUnusable(t *testing.T) {
	dir := t.TempDir()
	store, err := stream.NewFileStore(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	for epoch := int64(1); epoch <= 2; epoch++ {
		if err := store.Save(context.Background(), stream.Checkpoint{JobID: "j", Epoch: epoch}); err != nil {
			t.Fatal(err)
		}
	}
	// A crash between writing a checkpoint and writing the pointer leaves the
	// pointer stale or missing; the newest intact file is still the answer.
	if err := os.Remove(filepath.Join(dir, "j", "latest")); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.Load(context.Background(), "j")
	if err != nil || !ok || got.Epoch != 2 {
		t.Fatalf("load = %d ok=%v err=%v, want epoch 2", got.Epoch, ok, err)
	}

	// A truncated newest file falls back to the one before it.
	if err := os.WriteFile(filepath.Join(dir, "j", "ckpt-00000000000000000002.json"),
		[]byte("{trunc"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, ok, err = store.Load(context.Background(), "j")
	if err != nil || !ok || got.Epoch != 1 {
		t.Fatalf("load = %d ok=%v err=%v, want the intact epoch 1", got.Epoch, ok, err)
	}
}

func TestMemStoreIsPerJob(t *testing.T) {
	m := stream.NewMemStore()
	if err := m.Save(context.Background(), stream.Checkpoint{JobID: "a", Epoch: 3}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := m.Load(context.Background(), "b"); ok {
		t.Fatal("job b should not see job a's checkpoint")
	}
	got, ok, _ := m.Load(context.Background(), "a")
	if !ok || got.Epoch != 3 {
		t.Fatalf("load = %+v ok=%v", got, ok)
	}
}
