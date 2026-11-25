//go:build trunk_prototype

package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMixedAggregateRecoveryAndRollupPublication(t *testing.T) {
	l := testLayout(t, false)
	from := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return from.Add(72 * time.Hour) }}
	s, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Append(Write, testFrame(t, l, from, 1, false)); e != nil {
		t.Fatal(e)
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	b := s.branches[Write]
	// Rewrite the first durable summary and verify committed recovery.
	legacy, e := encodeSummary(b.summaries[0])
	if e != nil {
		t.Fatal(e)
	}
	if e = b.sum.Truncate(headerSize); e != nil {
		t.Fatal(e)
	}
	if e = appendRecord(b.sum, legacy); e != nil {
		t.Fatal(e)
	}
	b.sumEnd = uint64(headerSize + 8 + len(legacy))
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	// Add a separate complete hour after recovery.
	commitLegacyFixture(t, s, Write, testFrame(t, l, from.Add(time.Hour), 2, true))
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	groups, e := s.Summaries(Write, from, from.Add(2*time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	var all Aggregate
	for _, g := range groups {
		for _, a := range g {
			if e = all.merge(a); e != nil {
				t.Fatal(e)
			}
		}
	}
	if all.Count != 2 || all.DurationCount != 1 || all.Sum != 11 {
		t.Fatalf("mixed totals %+v", all)
	}
	if e = s.PublishRollup(Write, Daily, from); e != nil {
		t.Fatal(e)
	}
	path, _, _, e := s.rollupPeriod(Write, Daily, from)
	if e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(path, os.O_RDWR, 0600)
	if e != nil {
		t.Fatal(e)
	}
	var value rollup
	if e = records(f, func(raw []byte) error { return decodeRollup(raw, &value) }); e != nil {
		t.Fatal(e)
	}
	raw, e := encodeRollup(value)
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Truncate(headerSize); e != nil {
		t.Fatal(e)
	}
	if e = appendRecord(f, raw); e != nil {
		t.Fatal(e)
	}
	f.Close()
	// Publishing an equivalent rollup is idempotent, not a conflict.
	if e = s.PublishRollup(Write, Daily, from); e != nil {
		t.Fatal(e)
	}
	if e = s.PublishRollup(Write, Daily, from.AddDate(0, 0, 1)); e != nil {
		t.Fatal(e)
	}
	got, e := s.ReadRollup(Write, Daily, from)
	if e != nil {
		t.Fatal(e)
	}
	if len(got) != len(value.Groups) {
		t.Fatal("mixed rollup read")
	}
	for _, branch := range []Branch{Debug, Trace} {
		if _, e = s.Append(branch, testFrame(t, l, from, 3, false)); e != nil {
			t.Fatal(e)
		}
		if e = s.Flush(); e != nil {
			t.Fatal(e)
		}
		if _, e = os.Stat(filepath.Join(s.dir, branch.Prefix()+".summ")); e != nil {
			t.Fatal("missing Definition-driven summary file", e)
		}
		if len(s.branches[branch].summaries) == 0 || len(s.branches[branch].summaries[0].Fields) == 0 {
			t.Fatal("missing field aggregate PAC")
		}
	}
}
