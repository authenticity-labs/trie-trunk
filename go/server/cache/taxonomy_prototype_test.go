//go:build trunk_prototype

package cache

import (
	"bytes"
	"net/http/httptest"
	"path/filepath"
	"testing"
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
