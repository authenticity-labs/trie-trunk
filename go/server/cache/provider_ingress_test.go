package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func providerStore(t testing.TB) (*Store, Config) {
	t.Helper()
	dir, e := filepath.Abs("../../../schemas/logs")
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(dir, "log-http_trietrunk.rx"))
	if e != nil {
		t.Fatal(e)
	}
	raw, e = BindAuthoredDefinition(raw, Binding{Format: Format, Strategy: "log", Service: "test", Resource: "http", Major: 1, Timezone: "UTC", Roles: Roles{Event: "@id", Duration: "@duration"}, Path: []string{"level"}})
	if e != nil {
		t.Fatal(e)
	}
	l, e := ParseDefinition(raw)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(l.Close)
	cfg := Config{Root: t.TempDir(), Cache: "test", Tenant: "local", Layout: l, Now: func() time.Time { return time.UnixMilli(1793742600000) }}
	s, e := Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	path := s.dir
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	config := fmt.Sprintf("target = %q\n[provider]\nbundle = %q\ndefinition = 'log-http_trietrunk'\n", fmtDigestNative(l.Digest), dir)
	if e = os.WriteFile(filepath.Join(path, "transforms.cfg"), []byte(config), 0600); e != nil {
		t.Fatal(e)
	}
	s, e = Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	return s, cfg
}
