//go:build trunk_prototype

package cache

import (
	"reflect"
	"testing"
	"time"
)

func TestDurationUnitsAgreeInIndexAndScan(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	now := at.Add(2 * time.Hour)
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	off, on := false, true
	s.policy.SearchIndex = &off
	for i, us := range []uint64{0, 9999, 10000, 9999999, 10000000, 10000001} {
		var f Frame
		if err = l.Scan(testFrame(t, l, at.Add(time.Duration(i)*time.Minute), uint32(i+1), false), &f); err != nil {
			t.Fatal(err)
		}
		f.Values[l.Duration] = Uint(us, 8)
		raw, e := l.Encode(nil, f.Values)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.Append(Write, raw); e != nil {
			t.Fatal(e)
		}
	}
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		op, kind, value string
		count           int
	}{
		{"<=", "number", "10000", 5}, {"<=", "duration", "10000ms", 5},
		{"<=", "duration", "10s", 5}, {"<=", "duration", "10000us", 3},
		{"<=", "duration", "9999999999ns", 4},
		{">", "number", "10000", 1}, {">", "duration", "10000ms", 1},
		{">", "duration", "10s", 1}, {">", "duration", "10000us", 3},
	} {
		q := QueryPlan{Filters: []QueryFilter{{"duration", tc.op, QueryLiteral{tc.kind, tc.value}}}, Order: "asc"}
		s.policy.SearchIndex = &off
		scanned := searchRows(t, s, at, now, q)
		s.policy.SearchIndex = &on
		indexed := searchRows(t, s, at, now, q)
		if len(scanned) != tc.count || !reflect.DeepEqual(scanned, indexed) {
			t.Fatalf("%+v: scan %v index %v", tc, scanned, indexed)
		}
	}
}
