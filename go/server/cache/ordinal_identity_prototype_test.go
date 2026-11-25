//go:build trunk_prototype

package cache

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestOrdinalIngressSealRestartAndIndexOnlyLeaf(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	now := at.Add(2 * time.Minute)
	cfg := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }, ObjectIndex: true}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	off := false
	s.policy.SearchIndex = &off
	originals := [][]byte{testFrame(t, l, at.Add(time.Minute), 10, false), testFrame(t, l, at, 20, false)}
	for _, raw := range originals {
		if _, err = s.Append(Write, raw); err != nil {
			t.Fatal(err)
		}
	}
	b := s.branches[Write]
	pending := b.pendingWindow(nil, at, at.Add(time.Hour))
	if len(pending) != 2 {
		t.Fatal("pending count", len(pending))
	}
	var f Frame
	for _, p := range pending {
		if err = l.Scan(p.frame, &f); err != nil {
			t.Fatal(err)
		}
		id := f.Values[l.Event].Bytes
		if id[6] != 0x70 || id[7] != 0 || id[8] != 0x80 || !bytes.Equal(id[9:13], make([]byte, 4)) {
			t.Fatal("ingress entropy retained")
		}
		if f.Values[l.Record].State != Direct || len(f.Values[l.Record].Bytes) != 3 {
			t.Fatal("record is not fixed U24")
		}
		if f.Values[l.Leaf].State != Absent {
			t.Fatal("leaf duplicated in staged PIL")
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.policy.SearchIndex = &off
	// A third event shares a millisecond; recovery must not reuse a provisional ID.
	if _, err = s.Append(Write, testFrame(t, l, at, 30, false)); err != nil {
		t.Fatal(err)
	}
	if s.branches[Write].live.count(0) != 3 {
		t.Fatal("provisional identity collision after restart")
	}
	now = at.Add(2 * time.Hour)
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		b = s.branches[Write]
		if len(b.runs) != 1 {
			t.Fatal("hour count", len(b.runs))
		}
		run := b.runs[0]
		raw, e := readBytes(b.packs, run.PacksStart-b.packsRetired, run.PacksEnd-b.packsRetired)
		if e != nil {
			t.Fatal(e)
		}
		pac, e := decodeStoredPAC(l, run, raw)
		if e != nil {
			t.Fatal(e)
		}
		for n, member := range pac.Members {
			full, e := ResolvePAC(l, pac.Common, member)
			if e != nil {
				t.Fatal(e)
			}
			if e = l.Scan(full, &f); e != nil {
				t.Fatal(e)
			}
			if f.Values[l.Record].State != Direct || len(f.Values[l.Record].Bytes) != 3 || unsigned(f.Values[l.Record].Bytes) != uint64(n) {
				t.Fatalf("ordinal %d: %+v", n, f.Values[l.Record])
			}
			for _, i := range []int{l.Event, l.Time, l.Leaf, l.Path[0]} {
				if f.Values[i].State != Absent {
					t.Fatalf("duplicated field %s", l.Definition.Fields[i].Name)
				}
			}
			if b.rowLeaf(run.Start+uint64(n)) == 0 {
				t.Fatal("missing index leaf")
			}
		}
		count := 0
		e = s.Search(context.Background(), Query{Branch: Write, From: at, To: at.Add(time.Hour)}, func(_ uint64, raw []byte) error {
			if e := l.Scan(raw, &f); e != nil {
				return e
			}
			if unsigned(f.Values[l.Record].Bytes) != uint64(count) {
				t.Fatal("wrong materialized ordinal")
			}
			if f.Values[l.Path[0]].State != Direct {
				t.Fatal("state not resolved")
			}
			count++
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
		if count != 3 {
			t.Fatal("search count", count)
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
}
