//go:build trunk_prototype

package cache

import (
	"context"
	"testing"
	"time"
)

func TestHourlyRepeatedHourReplication(t *testing.T) {
	l := testLayout(t, false)
	loc, _ := time.LoadLocation("America/New_York")
	midnight := time.Date(2026, time.November, 1, 0, 0, 0, 0, loc)
	cfg := Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner", Layout: l, Now: func() time.Time { return midnight.AddDate(0, 0, 3) }}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	first := midnight.Add(time.Hour)
	second := first.Add(time.Hour)
	if first.Hour() != 1 || second.Hour() != 1 {
		t.Fatal("expected repeated 01:00")
	}
	// Arrival order may differ while both occurrences remain staged.
	for i, at := range []time.Time{second, first} {
		if _, err = s.Append(Write, testFrame(t, l, at.Add(10*time.Minute), uint32(i+1), false)); err != nil {
			t.Fatal(err)
		}
	}
	check := func() {
		t.Helper()
		for i, at := range []time.Time{first, second} {
			var m Manifest
			err = s.Collect(context.Background(), CollectionRequest{Branch: Write, At: at, IndexPeriod: "1h", DataFrom: at, DataTo: at.Add(time.Hour)}, func(v Manifest) error { m = v; return nil }, func(NativeChunk) error { return nil })
			if err != nil || len(m.Index) != 1 || m.Index[0] != (Span{uint64(i*2 + 1), uint64(i*2 + 2)}) || !m.From.Equal(at) || !m.To.Equal(at.Add(time.Hour)) {
				t.Fatalf("fold %d: %+v %v", i, m, err)
			}
			if got := collect(t, s, Write, at, at.Add(time.Hour)); len(got) != 1 {
				t.Fatalf("fold search %d", len(got))
			}
		}
	}
	check()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	check()
	// The repeated local hour is two ordinary, independently addressed UTC hours.
	a, ok := s.branches[Write].time.leaf(first.UnixMilli())
	z, ok2 := s.branches[Write].time.leaf(second.UnixMilli())
	if !ok || !ok2 || a.Start == z.Start || a.Segments != 1 || z.Segments != 1 {
		t.Fatal("UTC hours collapsed")
	}
}
