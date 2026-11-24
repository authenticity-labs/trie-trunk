package cache

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestRXRegistrationHTTPBodyPlacementAndLineage(t *testing.T) {
	root := t.TempDir()
	endpoint, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "test", Storage: "browse"})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	defer taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie"))
	document := func(minor, width int) string {
		return `{"header":{"rx-name":"http","namespace":"log","rx-major":1,"rx-minor":` + strconv.Itoa(minor) + `},"fields":[{"name":"status","type":[1,0],"size":` + strconv.Itoa(width) + `}]}`
	}
	encode := func(text string) []byte {
		t.Helper()
		d, e := seedNativeDocument([]byte(text))
		if e != nil {
			t.Fatal(e)
		}
		defer d.close()
		return bytes.Clone(d.bytes)
	}
	original := encode("[" + document(0, 1) + "]")
	placement := http.Header{"Content-Type": []string{"application/pil"}, "Resource-Service": []string{"billing"}, "Resource-Name": []string{"requests"}, "Resource-Type": []string{"http"}}
	request := func(body []byte, headers http.Header) *httptest.ResponseRecorder {
		t.Helper()
		method := "POST"
		if headers.Get("Trunk-Branch") != "" {
			method = "PUT"
		}
		r := httptest.NewRequest(method, "/definitions", bytes.NewReader(body))
		r.Header = headers.Clone()
		w := httptest.NewRecorder()
		endpoint.ServeHTTP(w, r)
		return w
	}
	w := request(original, placement)
	if w.Code != 204 || w.Header().Get("Trunk-Branch") == "" || w.Header().Get("Location") != "/demo/definitions?file=log.http.rx" {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	branch := w.Header().Get("Trunk-Branch")
	fetch := func(location string, want []byte) {
		t.Helper()
		got := httptest.NewRecorder()
		endpoint.ServeHTTP(got, httptest.NewRequest("GET", location, nil))
		if got.Code != 200 || got.Header().Get("Content-Type") != "application/pil" || !bytes.Equal(got.Body.Bytes(), want) {
			t.Fatal("RX retrieval changed PIL body", got.Code, got.Body.String())
		}
	}
	fetch(w.Header().Get("Location"), original)
	// Namespace/file identity remains separate from placement.
	id, _ := strconv.ParseUint(branch, 10, 16)
	route, e := taxonomyRoute(root, uint16(id))
	if e != nil || route.Service != "billing" || route.Resource != "requests" || route.ResourceType != "http" {
		t.Fatal(route, e)
	}
	retry := request(original, placement)
	if retry.Code != 204 || retry.Header().Get("Trunk-Branch") != branch {
		t.Fatal("retry changed registration", retry.Code, retry.Body.String())
	}
	scoped := httptest.NewRequest("PUT", "/demo/definitions", bytes.NewReader(original))
	scoped.Header = placement.Clone()
	scopedResult := httptest.NewRecorder()
	endpoint.ServeHTTP(scopedResult, scoped)
	if scopedResult.Code != 204 || scopedResult.Header().Get("Trunk-Branch") != branch {
		t.Fatal("cache-scoped registration failed", scopedResult.Code, scopedResult.Body.String())
	}
	// Persist configuration through the existing registry transaction; HTTP updates
	// must preserve it rather than replacing it with an empty binding.
	if e = RegisterSeedDefinition(root, original, SeedRegistration{Cache: "demo", Service: "billing", Resource: "requests", ResourceType: "http", File: "log.http.rx", Binding: map[string]any{"timezone": "America/New_York"}}); e != nil {
		t.Fatal(e)
	}
	next := encode("[" + document(1, 2) + "," + document(0, 1) + "]")
	update := http.Header{"Content-Type": []string{"application/pil"}, "Trunk-Branch": []string{branch}}
	w = request(next, placement)
	if w.Code != 204 || w.Header().Get("Trunk-Branch") == branch {
		t.Fatal("placement-only lineage registration failed", w.Code, w.Body.String())
	}
	fetch(w.Header().Get("Location"), next)
	newID, _ := strconv.ParseUint(w.Header().Get("Trunk-Branch"), 10, 16)
	raw, e := taxonomyBytes(root)
	if e != nil {
		t.Fatal(e)
	}
	binding, e := seedNativeBinding(raw, uint16(newID))
	if e != nil {
		t.Fatal(e)
	}
	defer binding.close()
	var stored struct{ Binding map[string]any }
	if e = json.Unmarshal(binding.bytes, &stored); e != nil || stored.Binding["timezone"] != "America/New_York" {
		t.Fatal(string(binding.bytes), e)
	}
	if old, e := taxonomyRoute(root, uint16(id)); e != nil || old.Minor != 0 {
		t.Fatal("old lineage lost", old, e)
	}
	if retry = request(next, placement); retry.Code != 204 || retry.Header().Get("Trunk-Branch") != w.Header().Get("Trunk-Branch") {
		t.Fatal("lineage retry failed", retry.Code, retry.Body.String())
	}
	before, e := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if e != nil {
		t.Fatal(e)
	}
	// Explicit references remain valid internal selectors, but are not required.
	if explicit := request(next, update); explicit.Code != 204 || explicit.Header().Get("Trunk-Branch") != w.Header().Get("Trunk-Branch") {
		t.Fatal("explicit reference retry failed", explicit.Code, explicit.Body.String())
	}
	mismatch := update.Clone()
	mismatch.Set("Resource-Service", "other")
	for _, test := range []struct {
		body    []byte
		headers http.Header
		status  int
	}{
		{[]byte("{}"), update, 400},
		{next, mismatch, 400},
		{encode(`[ {"header":{"rx-name":"other","namespace":"log"},"fields":[{"name":"status","type":[1,0],"size":1}]} ]`), placement, 400},
		{next, http.Header{"Content-Type": []string{"application/json"}}, 415},
		{next, http.Header{"Content-Type": []string{"application/pil"}}, 400},
		{next, http.Header{"Content-Type": []string{"application/pil"}, "Trunk-Branch": []string{"0"}}, 400},
	} {
		got := request(test.body, test.headers)
		if got.Code != test.status {
			t.Fatal(got.Code, got.Body.String())
		}
	}
	after, e := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected requests changed registry", e)
	}

	// New revisions inherit the most recent configuration, not the first
	// historical registration found in the registry.
	if e = RegisterSeedDefinition(root, next, SeedRegistration{Cache: "demo", Service: "billing", Resource: "requests", ResourceType: "http", File: "log.http.rx", Binding: map[string]any{"timezone": "UTC"}}); e != nil {
		t.Fatal(e)
	}
	third := encode("[" + document(2, 3) + "," + document(1, 2) + "," + document(0, 1) + "]")
	w = request(third, placement)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	thirdID, _ := strconv.ParseUint(w.Header().Get("Trunk-Branch"), 10, 16)
	raw, e = taxonomyBytes(root)
	if e != nil {
		t.Fatal(e)
	}
	latest, e := seedNativeBinding(raw, uint16(thirdID))
	if e != nil {
		t.Fatal(e)
	}
	defer latest.close()
	if e = json.Unmarshal(latest.bytes, &stored); e != nil || stored.Binding["timezone"] != "UTC" {
		t.Fatal(string(latest.bytes), e)
	}

	// Configured splits use their names, not numeric registry handles.
	if e = RegisterSeedDefinition(root, third, SeedRegistration{Cache: "demo", Service: "billing", Resource: "split-requests", ResourceType: "http", Level: "ok", File: "log.http.rx", Binding: map[string]any{"field.split": "status", "split.ok": "1", "split.error": "2"}}); e != nil {
		t.Fatal(e)
	}
	split := placement.Clone()
	split.Set("Resource-Name", "split-requests")
	if ambiguous := request(third, split); ambiguous.Code != 400 || !bytes.Contains(ambiguous.Body.Bytes(), []byte("Resource-Split")) {
		t.Fatal(ambiguous.Code, ambiguous.Body.String())
	}
	split.Set("Resource-Split", "ok")
	fourth := encode("[" + document(3, 4) + "," + document(2, 3) + "," + document(1, 2) + "," + document(0, 1) + "]")
	w = request(fourth, split)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	splitID, _ := strconv.ParseUint(w.Header().Get("Trunk-Branch"), 10, 16)
	if route, err := taxonomyRoute(root, uint16(splitID)); err != nil || route.Level != "ok" || route.Minor != 3 {
		t.Fatal(route, err)
	}
}

func TestDraftPromotionThroughDefinitions(t *testing.T) {
	root := t.TempDir()
	endpoint, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "test", Storage: "browse"})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	rx, err := seedNativeDocument([]byte(`[{"header":{"rx-name":"account"},"fields":[{"name":"id","type":[4,0],"size":16,"annotations":["@object"]}]}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer rx.close()
	call := func(action, generation string, body []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/demo/definitions", bytes.NewReader(body))
		r.Header = http.Header{"Content-Type": []string{"application/pil"}, "Resource-Service": []string{"customer"}, "Resource-Name": []string{"accounts"}, "Resource-Type": []string{"account"}, "Draft-Action": []string{action}}
		if generation != "" {
			r.Header.Set("Draft-Generation", generation)
		}
		w := httptest.NewRecorder()
		endpoint.ServeHTTP(w, r)
		return w
	}
	saved := call("save", "", rx.bytes)
	token := saved.Header().Get("Draft-Generation")
	if saved.Code != 204 || token == "" || saved.Header().Get("Trunk-Branch") != "" {
		t.Fatal(saved.Code, saved.Header(), saved.Body.String())
	}
	raw := call("read", token, nil)
	if raw.Code != 200 || raw.Header().Get("Content-Type") != "application/pil" || !bytes.Equal(raw.Body.Bytes(), rx.bytes) {
		t.Fatal("could not reopen saved RX", raw.Code, raw.Header())
	}
	inspected := call("inspect", token, nil)
	var model struct {
		Members []struct{ Header map[string]any }
	}
	if err = json.Unmarshal(inspected.Body.Bytes(), &model); err != nil || inspected.Code != 200 || len(model.Members) != 1 {
		t.Fatal(inspected.Code, inspected.Body.String(), err)
	}
	if stale := call("promote", "expired", nil); stale.Code != 400 {
		t.Fatal(stale.Code, stale.Body.String())
	}
	w := call("promote", token, nil)
	if w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	id, err := strconv.ParseUint(w.Header().Get("Trunk-Branch"), 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	route, err := taxonomyRoute(root, uint16(id))
	if err != nil || route.Major != 1 {
		t.Fatal(route, err)
	}
	if _, err = os.Stat(filepath.Join(root, ".trunk", "drafts", token)); !os.IsNotExist(err) {
		t.Fatal("draft directory survived promotion", err)
	}
}
