package route

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ProfileFile is the name a profile takes inside a state directory, beside the
// result cache's index and the findings ledger.
const ProfileFile = "routing.jsonl"

// LoadProfile reads the calibration accumulated in a state directory.
//
// A missing directory or file is not an error: it is a cold pipeline, and a
// cold pipeline routes exactly the way Loom does without a router. Nor is a
// corrupt or version-mismatched line — a profile is a cache of decisions, and
// the worst a lost one can cost is a run that has to learn again. Anything
// that could turn "the calibration is unreadable" into "the run does not
// start" would be trading a large failure for a small saving.
func LoadProfile(dir string) (*Profile, error) {
	p := NewProfile()
	if dir == "" {
		return p, nil
	}
	data, err := os.ReadFile(filepath.Join(dir, ProfileFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return p, nil
		}
		return p, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	for dec.More() {
		var snap Snapshot
		if err := dec.Decode(&snap); err != nil {
			break // a torn trailing write; everything before it still counts
		}
		if snap.Version != SnapshotVersion {
			continue
		}
		p.mergeSnapshot(snap)
	}
	return p, nil
}

// SaveProfile appends a contribution to the state directory's profile.
//
// The file is append-only and additive, for the same reason the cache index
// is: several processes calibrating one pipeline write concurrently, and a
// last-writer-wins total would silently discard whichever fleet member
// finished first. Summing lines makes the merge associative, so the file means
// the same thing whatever order the appends land in.
//
// Nothing compacts the file. Each run appends one line proportional to the
// buckets it saw rather than the records it processed, so it grows with the
// number of runs; a deployment that runs a pipeline often enough to care can
// rewrite it with a single line from LoadProfile.
func SaveProfile(dir string, p *Profile) error {
	if dir == "" || p == nil {
		return nil
	}
	snap := p.Snapshot()
	if len(snap.Stages) == 0 {
		return nil // nothing was learned; do not grow the file to say so
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	line, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, ProfileFile),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("route: append profile: %w", err)
	}
	return nil
}

// mergeSnapshot folds one snapshot's counts in. The caller holds no lock; this
// takes it.
func (p *Profile) mergeSnapshot(snap Snapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, s := range snap.Stages {
		cs := p.stage(s.Stage)
		for _, b := range s.Buckets {
			cb := cs.bucket(b.Bucket)
			for i, r := range b.Rungs {
				t := cb.at(i)
				t.Valid += r.Valid
				t.Invalid += r.Invalid
				t.Starts += r.Starts
			}
		}
	}
}
