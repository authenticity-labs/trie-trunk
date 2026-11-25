package cache

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func registeredConfigurationEndpoint(t *testing.T, root string) *Endpoint {
	t.Helper()
	doc, err := seedNativeDocument([]byte(`[{"header":{"rx-name":"event","rx-major":1},"fields":[{"name":"time","type":[1,0],"size":6,"annotations":["@time"]}]}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer doc.close()
	target := SeedRegistration{Cache: "demo", Service: "payment", Resource: "transactions", ResourceType: "http", File: "event.rx"}
	if err = RegisterSeedDefinition(root, doc.bytes, target); err != nil {
		t.Fatal(err)
	}
	p, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestConfigurationEditorUsesHierarchyAndPreservesOtherSettings(t *testing.T) {
	root := t.TempDir()
	p := registeredConfigurationEndpoint(t, root)
	var err error
	resource := []string{"demo", "payment", "transactions"}
	branch := append(append([]string{}, resource...), "http")
	trunk := configurationPath(root, nil)
	if err = os.WriteFile(trunk, []byte("version=1\n[policy]\nsummary_utc_offset_hours=-5\n[policy.pac]\nlevel=8\n[policy.staging]\nmax_bytes=100\nmax_records=20\n"), 0600); err != nil {
		t.Fatal(err)
	}
	resourceFile := configurationPath(root, resource)
	if err = os.MkdirAll(filepath.Dir(resourceFile), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(resourceFile, []byte("version=1\n[quota]\nlimit_bytes=1000\n[policy.replication]\nindex_period='4h'\n[policy.staging]\nmax_bytes=0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	call := func(method string, scope []string, revision, body, token string) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(scope)
		r := httptest.NewRequest(method, "/configurations?scope="+url.QueryEscape(string(raw)), bytes.NewBufferString(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("If-Match", `"`+revision+`"`)
		w := httptest.NewRecorder()
		p.Handler("secret").ServeHTTP(w, r)
		return w
	}
	if w := call("GET", resource, "", "", ""); w.Code != 401 {
		t.Fatal(w.Code)
	}
	get := func(scope []string) configurationView {
		t.Helper()
		w := call("GET", scope, "", "", "secret")
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var v configurationView
		if err = json.Unmarshal(w.Body.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	initial := get(resource)
	if initial.Effective["summary_utc_offset_hours"] != float64(-5) {
		t.Fatal(initial.Effective)
	}
	if initial.Effective["pac.level"] != float64(8) || initial.Effective["staging.max_bytes"] != float64(0) || initial.Effective["staging.max_records"] != float64(20) {
		t.Fatal(initial.Effective)
	}
	if len(initial.Transforms) != 0 {
		t.Fatal(initial.Transforms)
	}
	w := call("PUT", resource, initial.Revision, `{"category":"compression","values":{"pac.level":9}}`, "secret")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	rawConfig, err := os.ReadFile(resourceFile)
	if err != nil || !bytes.Contains(rawConfig, []byte("limit_bytes = 1000")) {
		t.Fatal("policy save lost quota", string(rawConfig), err)
	}
	after := get(resource)
	if after.Local["replication.index_period"] != "4h" || after.Local["staging.max_bytes"] != float64(0) || after.Local["pac.level"] != float64(9) {
		t.Fatal(after.Local)
	}
	if w = call("PUT", resource, initial.Revision, `{"category":"compression","values":{"pac.level":4}}`, "secret"); w.Code != 412 {
		t.Fatal(w.Code)
	}
	before, _ := os.ReadFile(resourceFile)
	for _, body := range []string{
		`{"category":"compression","values":{"pac.level":23}}`,
		`{"category":"compression","values":{"object_index":true}}`,
		`{"category":"staging","values":{"staging.max_bytes":-1}}`,
		`{"category":"replication","values":{"replication.provisional_tail":"1m"}}`,
		`{"category":"compression","values":{"pac.level":4.5}}`,
		`{"category":"compression","values":{"pac.level":false}}`,
		`{"category":"replication","values":{"summary_utc_offset_hours":15}}`,
		`{"category":"replication","values":{"summary_utc_offset_hours":-13}}`,
		`{"category":"replication","values":{"summary_utc_offset_hours":5.5}}`,
	} {
		if w = call("PUT", resource, after.Revision, body, "secret"); w.Code != 400 {
			t.Fatal(body, w.Code, w.Body.String())
		}
	}
	unchanged, _ := os.ReadFile(resourceFile)
	if !bytes.Equal(before, unchanged) {
		t.Fatal("invalid update changed file")
	}
	bv := get(branch)
	if w = call("PUT", branch, bv.Revision, `{"category":"staging","values":{"staging.max_records":20}}`, "secret"); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w = call("GET", []string{"demo", "..", ".."}, "", "", "secret"); w.Code != 400 {
		t.Fatal(w.Code)
	}
	// Removing the last staging override must restore inheritance, including count.
	if w = call("PUT", resource, after.Revision, `{"category":"staging","values":{"staging.max_bytes":null}}`, "secret"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	inherited := get(resource)
	if inherited.Effective["staging.max_bytes"] != float64(100) || inherited.Effective["staging.max_records"] != float64(20) {
		t.Fatal(inherited.Effective)
	}
	// Explicit UTC overrides the inherited offset; null restores inheritance.
	for _, value := range []string{"0", "null"} {
		v := get(branch)
		if w = call("PUT", branch, v.Revision, `{"category":"replication","values":{"summary_utc_offset_hours":`+value+`}}`, "secret"); w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		want := float64(-5)
		if value == "0" {
			want = 0
		}
		if get(branch).Effective["summary_utc_offset_hours"] != want {
			t.Fatal("offset inheritance")
		}
	}
	policy, err := configuredScope(root, resource, Configuration{})
	if err != nil || policy.PAC.level() != 9 || policy.Staging.MaxBytes != 100 || policy.Replication.IndexPeriod != "4h" {
		t.Fatal(policy, err)
	}
}
