package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLeafFooterInterruptedAtEveryByte(t *testing.T) {
	f, err := openFile(filepath.Join(t.TempDir(), "leaf.trie"), "NUTSTRI6", [32]byte{}, [16]byte{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	nodes, err := encoder.Marshal([]sharedNode{{Field: 1, Bytes: []byte{3}}})
	if err != nil {
		t.Fatal(err)
	}
	trie := newTrie()
	if err = appendLeafNodes(f, &trie, nodes); err != nil {
		t.Fatal(err)
	}
	prefix, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := encoder.Marshal(TrieStatistics{Nodes: 2, Capacity: 65536})
	if err != nil {
		t.Fatal(err)
	}
	footer, err := leafStatisticsFooter(trie.fileEnd, stats)
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut <= len(footer); cut++ {
		if err = f.Truncate(trie.fileEnd); err != nil {
			t.Fatal(err)
		}
		if _, err = f.WriteAt(footer[:cut], trie.fileEnd); err != nil {
			t.Fatal(err)
		}
		count := 0
		end, got, e := readLeafFile(f, func(raw []byte) error {
			count++
			if !bytes.Equal(raw, nodes) {
				t.Fatal("node bytes changed")
			}
			return nil
		})
		if e != nil || count != 1 || end != trie.fileEnd {
			t.Fatalf("cut %d: end=%d count=%d err=%v", cut, end, count, e)
		}
		if cut == len(footer) {
			if !bytes.Equal(got, stats) {
				t.Fatal("lost complete statistics")
			}
		} else if got != nil {
			t.Fatal("published partial statistics")
		}
		disk, e := os.ReadFile(f.Name())
		if e != nil || !bytes.Equal(disk[:len(prefix)], prefix) {
			t.Fatal("node prefix changed", e)
		}
		if cut < len(footer) && len(disk) != len(prefix) {
			t.Fatalf("retained partial footer at %d", cut)
		}
	}
	// A complete corrupt footer must not be mistaken for an interrupted append.
	footer[8] ^= 1
	if _, err = f.WriteAt(footer, trie.fileEnd); err != nil {
		t.Fatal(err)
	}
	if _, _, err = readLeafFile(f, func([]byte) error { return nil }); err == nil {
		t.Fatal("accepted corrupt footer")
	}
}

func TestLeafStatisticsLifecycle(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	now := at.Add(90 * time.Minute)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(Read, testFrame(t, l, at, 1, false)); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.dir, "leaf.trie")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	end := s.trie.fileEnd
	if _, err = os.Stat(filepath.Join(s.dir, "resource.stats")); !os.IsNotExist(err) {
		t.Fatal("separate statistics file exists", err)
	}
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.trie.footer || s.trie.fileEnd != end {
		t.Fatal("footer not restored")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("open rewrote footer")
	}
	now = at.Add(210 * time.Minute)
	if _, err = s.Append(Read, testFrame(t, l, at.Add(2*time.Hour), 2, false)); err != nil {
		t.Fatal(err)
	}
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	if s.trie.footer || s.trie.fileEnd <= end {
		t.Fatal("new nodes did not reclaim footer")
	}
	after, _ = os.ReadFile(path)
	if !bytes.Equal(before[:end], after[:end]) {
		t.Fatal("prior node addresses changed")
	}
	if len(collect(t, s, Read, at, at.Add(4*time.Hour))) != 2 {
		t.Fatal("lost records across footer replacement")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	_, raw, err := readLeafFile(f, func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	var stats TrieStatistics
	if err = decoder.Unmarshal(raw, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Nodes != len(s.trie.nodes) || stats.Branches["read"].Events != 2 {
		t.Fatal(stats)
	}
	if dir := os.Getenv("TRUNK_LEAF_FIXTURE"); dir != "" {
		if err = os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		after, _ = os.ReadFile(path)
		for name, data := range map[string][]byte{"before.trie": before, "after.trie": after, "definition.rx": l.Bytes} {
			if err = os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestLeafNodeAppendInterruptedAtEveryByte(t *testing.T) {
	f, err := openFile(filepath.Join(t.TempDir(), "leaf.trie"), "NUTSTRI6", [32]byte{}, [16]byte{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	trie := newTrie()
	nodes, _ := encoder.Marshal([]sharedNode{{Field: 1, Bytes: []byte{1}}})
	if err = appendLeafNodes(f, &trie, nodes); err != nil {
		t.Fatal(err)
	}
	boundary := trie.fileEnd
	more, _ := encoder.Marshal([]sharedNode{{Field: 1, Bytes: []byte{2}}})
	if err = appendLeafNodes(f, &trie, more); err != nil {
		t.Fatal(err)
	}
	disk, _ := os.ReadFile(f.Name())
	tail := disk[boundary:]
	for cut := 0; cut <= len(tail); cut++ {
		if err = f.Truncate(boundary); err != nil {
			t.Fatal(err)
		}
		if _, err = f.WriteAt(tail[:cut], boundary); err != nil {
			t.Fatal(err)
		}
		count := 0
		end, _, e := readLeafFile(f, func([]byte) error { count++; return nil })
		want := 1
		if cut == len(tail) {
			want = 2
		}
		if e != nil || count != want {
			t.Fatalf("cut %d: count=%d err=%v", cut, count, e)
		}
		if cut < len(tail) && end != boundary {
			t.Fatal("published incomplete node frame")
		}
	}
}
