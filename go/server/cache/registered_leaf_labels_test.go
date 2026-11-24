package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegisteredLeafLabelsAreSharedAndConditional(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	fixture := func(name string) []byte {
		b, e := os.ReadFile("../../../testdata/registered-trunk/" + name)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	if e := RegisterSeedDefinition(root, fixture("sample.rx"), SeedRegistration{Cache: "demo", Service: "lab", Resource: "events", ResourceType: "http", File: "sample.rx", Selection: &SeedDefinitionSelection{Major: 1, Namespace: "measurements", Name: "sample"}}); e != nil {
		t.Fatal(e)
	}
	if e := RegisterSeedDefinition(root, fixture("sample.rx"), SeedRegistration{Cache: "demo", Service: "lab", Resource: "events", ResourceType: "http", File: "sample.rx", Selection: &SeedDefinitionSelection{Major: 1, Namespace: "measurements", Name: "sample", Minor: 1}}); e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(root, "demo/lab/events")
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	original := fixture("sample.leaf")
	// Fixtures may already have labels from their own roundtrip tests. Start
	// this conditional-update test from an explicitly unlabeled root.
	reader, e := taxonomyReader(root)
	if e != nil {
		t.Fatal(e)
	}
	for _, branch := range []uint16{1, 2} {
		route, e := taxonomyRoute(root, branch)
		if e != nil {
			t.Fatal(e)
		}
		def, e := reader.prescribe(root, route.Definition)
		if e != nil {
			t.Fatal(e)
		}
		def.close()
		unlabeled, e := reader.trie.label(branch, 1, original, nil)
		if e != nil {
			t.Fatal(e)
		}
		original = bytes.Clone(unlabeled.bytes)
		unlabeled.close()
	}

	if e := os.WriteFile(filepath.Join(dir, "http.v1.leaf"), original, 0600); e != nil {
		t.Fatal(e)
	}
	open := func() *Endpoint {
		p, e := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local"})
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	p := open()
	defer func() { p.Close() }()
	request := func(method, q, body, etag string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/demo/lab/events/leaves"+q, strings.NewReader(body))
		r.Header.Set("Trunk-Branch", "1")
		if etag != "" {
			r.Header.Set("If-Match", etag)
		}
		w := httptest.NewRecorder()
		p.ServeHTTP(w, r)
		return w
	}
	before := request("GET", "?labels=1", "", "")
	if before.Code != 200 {
		t.Fatal(before.Code, before.Body.String())
	}
	etag := before.Header().Get("ETag")
	w := request("PATCH", "?leaf=1", "Canceled Order", etag)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	stale := request("PATCH", "?leaf=1", "Other", etag)
	if stale.Code != 412 {
		t.Fatal("stale overwrite", stale.Code)
	}
	// A replica receives the same labeled leaf file through the existing resource.
	raw := request("GET", "", "", "")
	if raw.Code != 200 || bytes.Equal(original, raw.Body.Bytes()) {
		t.Fatal("leaf resource did not publish metadata")
	}
	p.Close()
	p = open()
	next := request("GET", "?labels=1", "", "")
	var labels map[string]string
	if e := json.Unmarshal(next.Body.Bytes(), &labels); e != nil {
		t.Fatal(next.Code, e)
	}
	if labels["1"] != "Canceled Order" {
		t.Fatal(labels)
	}
	// A revalidation for another root must not reuse this root's JSON body.
	r := httptest.NewRequest("GET", "/demo/lab/events/leaves?labels=1", nil)
	r.Header.Set("Trunk-Branch", "2")
	r.Header.Set("If-None-Match", next.Header().Get("ETag"))
	other := httptest.NewRecorder()
	p.ServeHTTP(other, r)
	if other.Code != 200 || other.Body.String() != "{}" || other.Header().Get("ETag") == next.Header().Get("ETag") {
		t.Fatal("labels crossed Definition roots", other.Code, other.Body.String())
	}
	r.Header.Set("If-None-Match", other.Header().Get("ETag"))
	unchanged := httptest.NewRecorder()
	p.ServeHTTP(unchanged, r)
	if unchanged.Code != 304 {
		t.Fatal("unchanged root did not revalidate", unchanged.Code)
	}
	w = request("PATCH", fmt.Sprintf("?leaf=%d", 65535), "Missing", next.Header().Get("ETag"))
	if w.Code != 400 {
		t.Fatal("invalid leaf", w.Code)
	}
	w = request("PATCH", "?leaf=1", "", next.Header().Get("ETag"))
	if w.Code != 204 {
		t.Fatal("remove label", w.Code, w.Body.String())
	}
}
