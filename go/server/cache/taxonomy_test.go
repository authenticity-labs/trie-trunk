package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"trie-trunk/go/internal/taxonomy"
)

func TestTrunkTaxonomyIdentityAcrossCachesAndRestart(t *testing.T) {
	root := t.TempDir()
	l := testLayout(t, false)
	open := func(cache, kind string) *Store {
		t.Helper()
		s, err := Open(Config{Root: root, Cache: cache, Tenant: "owner", Layout: l, ResourceType: kind})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	first := open("one", "web")
	oldNode := first.taxonomyBranches[Read]
	before, err := taxonomyBytes(root)
	if err != nil {
		t.Fatal(err)
	}
	second := open("two", "jobs")
	if second.taxonomyBranches[Read] == oldNode {
		t.Fatal("branch identities collide across caches")
	}
	after, err := taxonomyBytes(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) <= len(before) {
		t.Fatal("registration moved existing identities")
	}
	previous, err := taxonomy.Load(before)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := taxonomy.Load(after)
	if err != nil {
		t.Fatal(err)
	}
	if !tr.Extends(previous) {
		t.Fatal("registration changed existing identities")
	}
	for _, s := range []*Store{first, second} {
		node, found := tr.Leaf(taxonomy.LeafID(s.taxonomyBranches[Read]))
		if !found {
			t.Fatal("missing branch ID")
		}
		n, ok := tr.Node(node)
		kind, _ := tr.Label(n.Type)
		if !ok || kind != s.config.ResourceType {
			t.Fatal("registration type was not preserved")
		}
		if _, err := os.Stat(filepath.Join(root, s.config.Cache, "trunk.trie")); !os.IsNotExist(err) {
			t.Fatal("created a cache-local taxonomy", err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Drop the published snapshot to exercise reopening from the file, not a
	// retained process-local lookup. Classification comes from the registration section in trunk.trie.
	taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie"))
	first = open("one", "")
	if first.taxonomyBranches[Read] != oldNode || first.config.ResourceType != "web" {
		t.Fatal("restart changed branch identity or classification")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if err != nil || !bytes.Equal(raw, after) {
		t.Fatal("restart appended duplicate taxonomy entries", err)
	}
	if s, err := Open(Config{Root: root, Cache: "one", Tenant: "owner", Layout: l, ResourceType: "jobs"}); err == nil {
		s.Close()
		t.Fatal("reclassified an immutable branch")
	}
}
