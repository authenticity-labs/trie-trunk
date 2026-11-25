//go:build trunk_prototype

package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPositionalTimeTrieRestartRecoveryAndUnchangedPayloads(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 12, 31, 22, 0, 0, 0, time.UTC)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(4 * time.Hour) }, BatchRecords: 1}
	s, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		if _, e = s.Append(Write, testFrame(t, l, at.Add(time.Duration(i)*time.Minute), uint32(i+1), false)); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	b := s.branches[Write]
	leaf, ok := b.time.leaf(at.UnixMilli())
	if !ok || leaf.Segments != 1 || leaf.End-leaf.Start != 4 {
		t.Fatal(leaf)
	}
	files := map[string][]byte{}
	for _, ext := range []string{".indx", ".packs"} {
		raw, e := os.ReadFile(filepath.Join(s.dir, "write"+ext))
		if e != nil {
			t.Fatal(e)
		}
		files[ext] = raw
	}
	triPath := filepath.Join(s.dir, "write.time")
	want := append([]byte(nil), b.time.raw...)
	if _, e = os.Stat(filepath.Join(s.dir, "write.journal")); !os.IsNotExist(e) {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	// Crash/torn derived publication: recover from PAC placement and index headers.
	if e = os.WriteFile(triPath, []byte("torn"), 0600); e != nil {
		t.Fatal(e)
	}
	s, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if !bytes.Equal(want, s.branches[Write].time.raw) {
		t.Fatal("time directory recovery changed coordinates")
	}
	for ext, want := range files {
		got, e := os.ReadFile(filepath.Join(s.dir, "write"+ext))
		if e != nil || !bytes.Equal(got, want) {
			t.Fatal("changed payload/index format", ext, e)
		}
	}
	if got := collect(t, s, Write, at, at.Add(time.Hour)); len(got) != 3 {
		t.Fatal("direct trie search lost events")
	}
}

func TestPositionalTimeTrieRetentionRecovery(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(72 * time.Hour) }}
	s, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	for i, h := range []int{0, 27, 50} {
		if _, e = s.Append(Write, testFrame(t, l, at.Add(time.Duration(h)*time.Hour), uint32(i+1), false)); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	before, _ := s.branches[Write].time.leaf(at.Add(27 * time.Hour).UnixMilli())
	s.mu.Lock()
	e = s.prune(Write, at.Add(24*time.Hour))
	s.mu.Unlock()
	if e != nil {
		t.Fatal(e)
	}
	want := append([]byte(nil), s.branches[Write].time.raw...)
	path := filepath.Join(s.dir, s.prefix(Write)+".time")
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, []byte("torn"), 0600); e != nil {
		t.Fatal(e)
	}
	s, e = Open(c)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if !bytes.Equal(want, s.branches[Write].time.raw) {
		t.Fatal("retention recovery changed parent displacements")
	}
	after, ok := s.branches[Write].time.leaf(at.Add(27 * time.Hour).UnixMilli())
	if !ok || before != after {
		t.Fatal("surviving reference changed", before, after)
	}
	if _, ok = s.branches[Write].time.leaf(at.UnixMilli()); ok {
		t.Fatal("retired hour still present")
	}
}
