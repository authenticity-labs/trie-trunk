package cache

import (
	"os"
	"path/filepath"
	"testing"
)

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
