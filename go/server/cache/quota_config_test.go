package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/pelletier/go-toml/v2"
)

func TestScopedQuotaOverridesPreservePolicyAndInherit(t *testing.T) {
	root := t.TempDir()
	endpoint, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "test", Layout: testLayout(t, false)})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	cachePath := configurationPath(root, []string{"demo"})
	if _, err = os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatal("created default cache override", err)
	}
	trunk := configurationPath(root, nil)
	if err = os.WriteFile(trunk, []byte("version=1\n[policy.pac]\nlevel=4\n"), 0600); err != nil {
		t.Fatal(err)
	}
	p := quotaPolicy{Pool: quotaNumber(10000), Scopes: []quotaScope{}}
	if err = saveQuotaPolicy(root, p); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatal("root quota created cache override", err)
	}
	leaf := []string{"demo", "payment", "transactions.v1", "write"}
	p.Scopes = []quotaScope{{Path: leaf, Limit: quotaNumber(1000), Recovery: &quotaRecovery{Unit: "percent", Value: 20}}}
	if err = saveQuotaPolicy(root, p); err != nil {
		t.Fatal(err)
	}
	branchPath := configurationPath(root, leaf)
	if _, err = os.Stat(branchPath); err != nil {
		t.Fatal(err)
	}
	for depth := 1; depth < 4; depth++ {
		if _, err = os.Stat(configurationPath(root, leaf[:depth])); !os.IsNotExist(err) {
			t.Fatal("created inherited scope config", depth, err)
		}
	}
	got, revision, err := readQuotaPolicy(root)
	if err != nil || got.Pool == nil || *got.Pool != 10000 || len(got.Scopes) != 1 || *got.Scopes[0].Limit != 1000 || got.Scopes[0].Recovery.Value != 20 {
		t.Fatal(got, err)
	}
	before, err := os.ReadFile(trunk)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(before, []byte("level = 4")) {
		t.Fatal("lost policy", string(before))
	}
	// A repeated save must leave file bytes intact, not republish inherited values.
	if err = saveQuotaPolicy(root, got); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(trunk)
	if !bytes.Equal(before, after) {
		t.Fatal("rewrote unchanged config")
	}
	// Removing the only branch override removes its config, not the branch data.
	sentinel := filepath.Join(filepath.Dir(branchPath), "keep")
	if err = os.WriteFile(sentinel, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	p.Scopes = nil
	if err = saveQuotaPolicy(root, p); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(branchPath); !os.IsNotExist(err) {
		t.Fatal("did not clear override", err)
	}
	if _, err = os.Stat(sentinel); err != nil {
		t.Fatal(err)
	}
	got, next, err := readQuotaPolicy(root)
	if err != nil || len(got.Scopes) != 0 || revision == next {
		t.Fatal(got, next, err)
	}
	// Removing quota from a file with another setting preserves that setting.
	if err = os.WriteFile(branchPath, []byte("version=1\n[policy.pac]\nlevel=3\n[quota]\nlimit_bytes=1000\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = saveQuotaPolicy(root, p); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(branchPath)
	var cfg scopeConfiguration
	if err = toml.Unmarshal(raw, &cfg); err != nil || cfg.Quota != nil || cfg.Policy.PAC.level() != 3 {
		t.Fatal(string(raw), err)
	}
	if _, _, err = resolveScope(raw, branchPath, Policy{}, true); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, "quotas.cfg")); !os.IsNotExist(err) {
		t.Fatal("separate quota file", err)
	}
}
