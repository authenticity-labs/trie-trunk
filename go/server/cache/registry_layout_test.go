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
func TestConfigurationScopedPathsAndMergedCache(t *testing.T) {
	root := t.TempDir()
	for _, tc := range []struct {
		scope []string
		path  string
	}{
		{nil, "trunk.conf"}, {[]string{"demo"}, "demo/demo.conf"}, {[]string{"demo", "payment"}, "demo/payment/payment.conf"}, {[]string{"demo", "payment", "transactions.v1"}, "demo/payment/transactions.v1/transactions.v1.conf"}, {[]string{"demo", "payment", "transactions.v1", "debug"}, "demo/payment/transactions.v1/debug/debug.conf"},
	} {
		if got := configurationPath(root, tc.scope); got != filepath.Join(root, tc.path) {
			t.Fatal(got, tc.path)
		}
	}
	if e := os.MkdirAll(filepath.Join(root, "demo"), 0700); e != nil {
		t.Fatal(e)
	}
	raw := []byte("version=1\ntimezone='UTC'\n[defaults.retention]\nread='30d'\n[policy.retention]\nread='14d'\n[resources.'payment/transactions.v1'.retention]\nread='7d'\n")
	if e := os.WriteFile(configurationPath(root, []string{"demo"}), raw, 0600); e != nil {
		t.Fatal(e)
	}
	c, e := ParseConfiguration(raw)
	if e != nil {
		t.Fatal(e)
	}
	cache, e := configuredScope(root, []string{"demo"}, c)
	if e != nil || cache.Retention.Read != "14d" {
		t.Fatal(cache, e)
	}
	resource, e := configuredScope(root, []string{"demo", "payment", "transactions.v1"}, c)
	if e != nil || resource.Retention.Read != "7d" {
		t.Fatal(resource, e)
	}
}
func TestDefinitionToolsLocalCopies(t *testing.T) {
	root, source := t.TempDir(), t.TempDir()
	for _, name := range []string{"doc", "rx"} {
		if e := os.WriteFile(filepath.Join(source, name), []byte(name), 0755); e != nil {
			t.Fatal(e)
		}
	}
	if e := InstallDefinitionTools(root, source); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"doc", "rx"} {
		path := filepath.Join(root, "definitions", name)
		b, e := os.ReadFile(path)
		if e != nil || string(b) != name {
			t.Fatal(e)
		}
		s, e := os.Stat(path)
		if e != nil || s.Mode().Perm()&0111 == 0 {
			t.Fatal("not executable", e)
		}
	}
}
