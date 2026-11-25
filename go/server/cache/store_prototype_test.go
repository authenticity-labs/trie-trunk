//go:build trunk_prototype

package cache

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStoreLateSparseBranchesAndRecovery(t *testing.T) {
	for _, variable := range []bool{false, true} {
		t.Run(map[bool]string{false: "fixed", true: "variable"}[variable], func(t *testing.T) {
			l := testLayout(t, variable)
			start := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
			now := start.Add(72 * time.Hour)
			c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "authenticity", Layout: l, BatchRecords: 2, Now: func() time.Time { return now }}
			s, e := Open(c)
			if e != nil {
				t.Fatal(e)
			}
			first := testFrame(t, l, start, 1, false)
			late := testFrame(t, l, start.Add(-time.Hour), 2, true)
			next := testFrame(t, l, start.Add(24*time.Hour), 3, false)
			for _, b := range []Branch{Read, Write, Error, Debug, Trace} {
				for _, raw := range [][]byte{first, next, late} {
					if dup, e := s.Append(b, raw); e != nil || dup {
						t.Fatalf("append: %v %v", dup, e)
					}
				}
			}
			if e = s.Flush(); e != nil {
				t.Fatal(e)
			}
			for _, b := range []Branch{Read, Write, Error, Debug, Trace} {
				if dup, e := s.Append(b, first); e == nil || dup {
					t.Fatalf("duplicate: %v %v", dup, e)
				}
			}
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			s, e = Open(c)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			for _, b := range []Branch{Read, Write, Error, Debug, Trace} {
				rows := collect(t, s, b, start.Add(-2*time.Hour), start.Add(25*time.Hour))
				if len(rows) != 3 || !sameSeedData(t, l, rows[0], late) || !sameSeedData(t, l, rows[1], first) || !sameSeedData(t, l, rows[2], next) {
					t.Fatalf("round trip %s", b.Name())
				}
				if dup, e := s.Append(b, late); e == nil || dup {
					t.Fatalf("recovered duplicate: %v %v", dup, e)
				}
				path := filepath.Join(s.dir, b.Prefix()+".summ")
				_, e := os.Stat(path)
				if b.Aggregates() {
					if e != nil {
						t.Fatal(e)
					}
					groups, e := s.Summaries(b, start.Add(-2*time.Hour), now)
					if e != nil {
						t.Fatal(e)
					}
					var a Aggregate
					for _, g := range groups {
						for _, v := range g {
							if e = a.merge(v); e != nil {
								t.Fatal(e)
							}
						}
					}
					if a.Count != 3 || a.DurationCount != 2 || a.Sum != 24 {
						t.Fatalf("aggregate %+v", a)
					}
				} else {
					if e != nil {
						t.Fatalf("missing diagnostic field summaries: %v", e)
					}
					if _, e = s.Summaries(b, start, now); e == nil {
						t.Fatal("diagnostic summary accepted")
					}
				}
			}
			ranges, tail, e := s.PeriodRanges(Write, 2026, 244)
			if e != nil || tail != 6 || len(ranges) != 2 || ranges[0].End == nil || *ranges[0].End != 2 {
				t.Fatalf("late day ranges: %+v tail %d %v", ranges, tail, e)
			}
			count := 0
			e = s.Search(context.Background(), Query{Branch: Write, From: start.Add(-2 * time.Hour), To: now, Equals: map[string]Value{"state": Text("authorized")}}, func(_ uint64, _ []byte) error { count++; return nil })
			if e != nil || count != 1 {
				t.Fatalf("predicate %d %v", count, e)
			}
		})
	}
}

func TestRecoveryDiscardUncommittedTails(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner", Layout: l, Now: func() time.Time { return at.Add(2 * time.Hour) }}
	s, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	frame := testFrame(t, l, at, 1, false)
	if _, e = s.Append(Write, frame); e != nil {
		t.Fatal(e)
	}
	dir := s.dir
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	for _, ext := range []string{"indx", "packs", "summ", "time"} {
		f, e := os.OpenFile(filepath.Join(dir, "write."+ext), os.O_WRONLY|os.O_APPEND, 0)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.Write([]byte{1, 2, 3}); e != nil {
			t.Fatal(e)
		}
		f.Close()
	}
	s, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	if rows := collect(t, s, Write, at.Add(-time.Second), at.Add(time.Second)); len(rows) != 1 || !sameSeedData(t, l, rows[0], frame) {
		t.Fatal("recovery lost data")
	}
	s.Close()
	f, e := os.OpenFile(filepath.Join(dir, "write.packs"), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	f.WriteAt([]byte{0xff}, headerSize)
	f.Close()
	if bad, e := Open(c); e == nil {
		bad.Close()
		t.Fatal("committed corruption accepted")
	}
}

func TestConcurrentWriterAndSnapshotReaders(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner", Layout: l, BatchRecords: 32, Now: func() time.Time { return at.Add(time.Minute) }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	frames := make([][]byte, 96)
	for i := range frames {
		frames[i] = testFrame(t, l, at.Add(time.Duration(i)*time.Millisecond), uint32(i), false)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, frame := range frames {
			if _, e := s.Append(Write, frame); e != nil {
				failures <- e
				return
			}
		}
	}()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 6; round++ {
				var previous []byte
				e := s.SearchLive(context.Background(), Query{Branch: Write, From: at, To: at.Add(time.Hour)}, func(frame []byte) error {
					var f Frame
					if e := l.Scan(frame, &f); e != nil {
						return e
					}
					id := f.Values[l.Event].Bytes
					if previous != nil && bytes.Compare(previous, id) >= 0 {
						return errors.New("snapshot order or dedup violation")
					}
					previous = append(previous[:0], id...)
					return nil
				})
				if e != nil {
					failures <- e
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		t.Fatal(e)
	}
	s.config.Now = func() time.Time { return at.Add(2 * time.Hour) }
	if rows := collect(t, s, Write, at, at.Add(time.Hour)); len(rows) != len(frames) {
		t.Fatal("concurrent event loss")
	}
}
