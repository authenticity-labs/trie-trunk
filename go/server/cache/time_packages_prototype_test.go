//go:build trunk_prototype

package cache

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestSemanticCollectionRestartAndPolicy(t *testing.T) {
	l := testLayout(t, false)
	loc := time.UTC
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, loc)
	noon := time.Date(2026, 11, 1, 12, 0, 0, 0, loc)
	cfg := Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner", Layout: l, Now: func() time.Time { return at.AddDate(0, 0, 3) }}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	setTestPackagePeriod(t, s, "12h")
	appendAt := func(at time.Time, n uint32) {
		t.Helper()
		commitLegacyFixture(t, s, Write, testFrame(t, l, at, n, false))
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	appendAt(at.Add(time.Hour), 1)
	appendAt(at.Add(2*time.Hour), 4)
	appendAt(noon, 2)
	appendAt(at.AddDate(0, 0, 1), 3)
	request := CollectionRequest{Branch: Write, At: at, IndexPeriod: "12h", DataFrom: at, DataTo: noon}
	var manifest Manifest
	var chunks []NativeChunk
	pull := func() {
		t.Helper()
		chunks = nil
		err = s.Collect(context.Background(), request, func(m Manifest) error { manifest = m; return nil }, func(c NativeChunk) error { chunks = append(chunks, c); return nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	pull()
	if !reflect.DeepEqual(manifest.Index, []Span{{1, 2}, {3, 4}}) || manifest.Provisional || !manifest.To.Equal(noon) {
		t.Fatalf("initial %+v", manifest)
	}
	request.Have = Coverage{Definition: manifest.Definition, Generation: manifest.Generation, Index: manifest.Index, MemberRanges: manifest.MemberRanges, TrieNodes: manifest.TrieNodes}
	pull()
	for _, c := range chunks {
		if c.Kind == "index" || c.Kind == "pac" {
			t.Fatal("duplicate transfer")
		}
	}
	before := manifest
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	pull()
	if !reflect.DeepEqual(before, manifest) {
		t.Fatalf("restart changed boundaries\nbefore=%+v\nafter=%+v", before, manifest)
	}
	// Exact search still excludes noon.
	if got := collect(t, s, Write, at.UTC(), noon.UTC()); len(got) != 2 {
		t.Fatalf("exact search %d", len(got))
	}
	b := s.branches[Write]
	originalRows := append([]byte(nil), b.rows...)
	originalGeneration := b.generation
	setPolicy := func(period string) {
		t.Helper()
		raw := fmt.Sprintf("version=1\ntimezone='America/New_York'\n[defaults.replication]\nindex_period=%q\nprovisional_tail='30m'\n", period)
		if err := os.WriteFile(configurationPath(s.config.Root, []string{s.config.Cache}), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	setPolicy("6h")
	if err = s.ReloadPolicy(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(originalRows, b.rows) || originalGeneration != b.generation {
		t.Fatal("resizing changed storage")
	}
	if err = s.Collect(context.Background(), request, func(Manifest) error { return nil }, func(NativeChunk) error { return nil }); err == nil {
		t.Fatal("stale plan accepted")
	}
	request.IndexPeriod = "6h"
	request.DataTo = time.Date(2026, 11, 1, 6, 0, 0, 0, loc)
	pull()
	if !reflect.DeepEqual(before.Index, manifest.Index) || manifest.IndexPeriod != "6h" {
		t.Fatalf("resized %+v", manifest)
	}
	// Earlier 12-hour source-row coverage is reusable after resizing.
	request.Have = Coverage{Definition: before.Definition, Generation: before.Generation, Index: before.Index, MemberRanges: before.MemberRanges, TrieNodes: before.TrieNodes}
	pull()
	for _, c := range chunks {
		if c.Kind == "index" || c.Kind == "pac" {
			t.Fatal("resize duplicated cached bytes")
		}
	}
	request.At = at.AddDate(0, 0, -1)
	request.DataFrom = time.Time{}
	request.DataTo = time.Time{}
	pull()
	if len(manifest.Index) != 0 || manifest.Checkpoint == 0 {
		t.Fatal("known empty package")
	}
	setPolicy("5h")
	if err = s.ReloadPolicy(); err == nil || s.policy.Replication.hours() != 6 {
		t.Fatal("invalid sizing replaced policy")
	}
	// A larger configured provisional tail is respected.
	request.At = at
	request.IndexPeriod = "6h"
	end := time.Date(2026, 11, 1, 6, 0, 0, 0, loc)
	s.config.Now = func() time.Time { return end.Add(20 * time.Minute) }
	pull()
	if !manifest.Provisional {
		t.Fatal("ignored 30-minute tail")
	}
	s.config.Now = func() time.Time { return end.Add(30 * time.Minute) }
	pull()
	if manifest.Provisional {
		t.Fatal("closed package still provisional")
	}
}
