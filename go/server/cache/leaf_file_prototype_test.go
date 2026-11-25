//go:build trunk_prototype

package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
