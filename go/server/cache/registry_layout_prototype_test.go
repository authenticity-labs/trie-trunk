//go:build trunk_prototype

package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"trie-trunk/go/internal/taxonomy"
)

func TestUnifiedRegistryDefinitionsAndRestart(t *testing.T) {
	root := t.TempDir()
	l := testLayout(t, false)
	for _, name := range []string{"one", "two"} {
		s, e := Open(Config{Root: root, Cache: name, Tenant: "owner", Layout: l})
		if e != nil {
			t.Fatal(e)
		}
		if e = s.Close(); e != nil {
			t.Fatal(e)
		}
		if _, e = os.Stat(filepath.Join(root, name, "registry.map")); !os.IsNotExist(e) {
			t.Fatal("separate registry", e)
		}
		if _, e = os.Stat(filepath.Join(s.dir, "resource.rx")); !os.IsNotExist(e) {
			t.Fatal("local RX duplicate", e)
		}
	}
	entries, e := os.ReadDir(filepath.Join(root, "definitions"))
	if e != nil || len(entries) != 1 {
		t.Fatal(entries, e)
	}
	raw, e := os.ReadFile(definitionPath(root, l.Digest))
	if e != nil || !bytes.Equal(raw, l.Bytes) {
		t.Fatal("Definition changed", e)
	}
	before, e := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if e != nil {
		t.Fatal(e)
	}
	tree, e := taxonomy.Load(before)
	if e != nil {
		t.Fatal(e)
	}
	tree.SetSection(99, []byte("preserve future section"))
	if _, e = taxonomy.Save(filepath.Join(root, "trunk.trie"), tree); e != nil {
		t.Fatal(e)
	}
	taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie"))
	for _, name := range []string{"one", "two"} {
		c, e := readCatalog(root, name)
		if e != nil || len(c.Definitions) != 1 {
			t.Fatal(c, e)
		}
		p, e := OpenEndpoint(Config{Root: root, Cache: name, Tenant: "owner"})
		if e != nil {
			t.Fatal(e)
		}
		if p.api.Store.layout.Digest != l.Digest {
			t.Fatal("wrong RX")
		}
		p.Close()
	}
	after, e := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if e != nil {
		t.Fatal(e)
	}
	loaded, e := taxonomy.Load(after)
	if e != nil || !loaded.Extends(tree) || !bytes.Equal(loaded.Section(99), tree.Section(99)) {
		t.Fatal("footer publication lost identity or unknown section", e)
	}
	if _, e = os.Stat(filepath.Join(root, "trunk.conf")); e != nil {
		t.Fatal(e)
	}
}
