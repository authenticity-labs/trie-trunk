//go:build trunk_prototype

package cache

import (
	"context"
	"testing"
	"time"
)

func TestAdaptiveIntegersStorage(t *testing.T) {
	for _, mode := range []string{"16", "32"} {
		d := testLayout(t, false).Definition
		d.Resource.Path = []string{"amount"}
		l := adaptiveLayout(t, d, mode)
		at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
		c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, ObjectIndex: true, BatchRecords: 2}
		s, e := Open(c)
		if e != nil {
			t.Fatal(e)
		}
		full, sparse := testFrame(t, l, at, 1, false), testFrame(t, l, at.Add(time.Second), 2, true)
		for _, raw := range [][]byte{full, sparse} {
			if _, e = s.Append(Write, raw); e != nil {
				t.Fatal(e)
			}
		}
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
		s, e = Open(c)
		if e != nil {
			t.Fatal(e)
		}
		rows := collect(t, s, Write, at, at.Add(time.Minute))
		if len(rows) != 2 || !sameSeedData(t, l, rows[0], full) || !sameSeedData(t, l, rows[1], sparse) {
			t.Fatal("native assembly mismatch")
		}
		if dup, e := s.Append(Write, full); e == nil || dup {
			t.Fatal("dedup", e)
		}
		locations, e := s.ObjectLocations(testObject(42))
		if e != nil || len(locations) != 2 {
			t.Fatal("object lookup", locations, e)
		}
		count := 0
		e = s.Search(context.Background(), Query{Branch: Write, From: at, To: at.Add(time.Minute), Equals: map[string]Value{"amount": Uint(999, 8)}}, func(_ uint64, b []byte) error { count++; return nil })
		if e != nil || count != 1 {
			t.Fatal("predicate", e, count)
		}
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
	}
}
