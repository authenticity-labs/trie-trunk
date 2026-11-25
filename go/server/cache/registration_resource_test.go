package cache

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestIndependentRegistrationResourcesWithoutEventStores(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	p := &Endpoint{config: Config{Root: root, Cache: "demo"}}
	p.routing.Store(&routingSnapshot{})
	target := SeedRegistration{Cache: "demo", Service: "payment", Resource: "transactions", ResourceType: "http", File: "event.rx"}
	register := func(annotation string) []byte {
		t.Helper()
		doc, err := seedNativeDocument([]byte(`[{"header":{"rx-name":"event"},"fields":[{"name":"time","type":[1,0],"size":6,"annotations":["` + annotation + `"]}]}]`))
		if err != nil {
			t.Fatal(err)
		}
		defer doc.close()
		if err = RegisterSeedDefinition(root, doc.bytes, target); err != nil {
			t.Fatal(err)
		}
		published, err := os.ReadFile(filepath.Join(root, "definitions", target.File))
		if err != nil {
			t.Fatal(err)
		}
		return published
	}
	get := func(method, path string, headers map[string]string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, path, nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		p.ServeHTTP(w, r)
		return w
	}
	first := register("@time")
	trie := get("GET", "/demo/registries?kind=taxonomy", nil)
	if trie.Code != 200 {
		t.Fatalf("trie: %d %s", trie.Code, trie.Body.String())
	}
	onDisk, err := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, trie.Body.Bytes()) {
		t.Fatal("trie response is not the separate complete trie resource")
	}
	rx := get("GET", "/demo/definitions?file=event.rx", nil)
	if rx.Code != 200 || !bytes.Equal(first, rx.Body.Bytes()) {
		t.Fatalf("Definition: %d %s", rx.Code, rx.Body.String())
	}
	if rx.Header().Get("ETag") == trie.Header().Get("ETag") {
		t.Fatal("resources share an ETag")
	}
	for _, resource := range []struct{ path, tag string }{{"/demo/registries?kind=taxonomy", trie.Header().Get("ETag")}, {"/demo/definitions?file=event.rx", rx.Header().Get("ETag")}} {
		w := get("GET", resource.path, map[string]string{"If-None-Match": resource.tag})
		if w.Code != 304 || w.Body.Len() != 0 {
			t.Fatalf("conditional %s: %d", resource.path, w.Code)
		}
		w = get("HEAD", resource.path, nil)
		if w.Code != 200 || w.Body.Len() != 0 || w.Header().Get("Content-Length") == "" {
			t.Fatalf("HEAD %s: %d", resource.path, w.Code)
		}
	}
	second := register("@time(ms)")
	if bytes.Equal(first, second) {
		t.Fatal("annotation edit did not change fixture")
	}
	changed := get("GET", "/demo/registries?kind=taxonomy", map[string]string{"If-None-Match": trie.Header().Get("ETag")})
	if changed.Code != 200 || changed.Header().Get("ETag") == trie.Header().Get("ETag") {
		t.Fatal("annotation change not discoverable through trie")
	}
	stale := get("GET", "/demo/definitions?file=event.rx", map[string]string{"If-Match": rx.Header().Get("ETag")})
	if stale.Code != http.StatusPreconditionFailed || stale.Body.Len() != 0 {
		t.Fatal("served a different Definition revision to an old trie")
	}
	current := get("GET", "/demo/definitions?file=event.rx", nil)
	if current.Code != 200 || !bytes.Equal(second, current.Body.Bytes()) {
		t.Fatal("new Definition resource unavailable")
	}
	for _, path := range []string{"/demo/definitions?file=../outside.rx", "/demo/definitions?file=missing.rx"} {
		if w := get("GET", path, nil); w.Code != 404 {
			t.Fatalf("unregistered file: %d", w.Code)
		}
	}
	if w := get("GET", "/demo/definitions?file=event.rx&file=event.rx", nil); w.Code != 400 {
		t.Fatal("accepted duplicate selector")
	}
	// Simulate the file publication arriving before its new trie reference.
	if err := os.WriteFile(filepath.Join(root, "definitions", "event.rx"), first, 0600); err != nil {
		t.Fatal(err)
	}
	if w := get("GET", "/demo/definitions?file=event.rx", nil); w.Code != 409 {
		t.Fatal("served bytes inconsistent with trie revision")
	}
}
