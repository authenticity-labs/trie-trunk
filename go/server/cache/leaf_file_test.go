package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
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
