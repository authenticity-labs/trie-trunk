package cache

import (
	"bytes"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"trie-trunk/go/internal/taxonomy"
)

func TestTaxonomyRegistrationTypeInterface(t *testing.T) {
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner"}
	p, err := OpenEndpoint(c)
	if err != nil {
		t.Fatal(err)
	}
	l := testLayout(t, false)
	request := func(method, kind string, want int) {
		t.Helper()
		r := httptest.NewRequest(method, "/demo/definitions", bytes.NewReader(l.Bytes))
		definitionHeaders(r.Header, l)
		r.Header.Set("Content-Type", "application/pil")
		r.Header.Set("Resource-Type", kind)
		w := httptest.NewRecorder()
		p.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, kind, w.Code, w.Body.String())
		}
		if want == 204 && w.Header().Get("Resource-Type") != "web" {
			t.Fatal("registration did not return classification")
		}
	}
	request("PUT", "bad/type", 400)
	request("PUT", "web", 204)
	request("PUT", "web", 204)
	request("HEAD", "jobs", 409)
	request("PUT", "jobs", 409)
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	taxonomyRegistrations.Delete(filepath.Join(c.Root, "trunk.trie"))
	p, err = OpenEndpoint(c)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	request("HEAD", "", 204)
	request("HEAD", "web", 204)
}

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
