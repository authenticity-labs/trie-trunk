package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestObsoleteSharedTrieNameCannotSilentlyResetReferences(t *testing.T) {
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner", Layout: testLayout(t, false)}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	dir, root := s.dir, c.Root
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "leaf.trie"), filepath.Join(dir, "resource.tri")); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(c); err == nil {
		s.Close()
		t.Fatal("opened obsolete shared trie as a fresh state trie")
	} else if !strings.Contains(err.Error(), "recreate the cache") {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "leaf.trie")); !os.IsNotExist(err) {
		t.Fatal("created a replacement state trie", err)
	}
	after, err := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed open changed the catalog", err)
	}
}
