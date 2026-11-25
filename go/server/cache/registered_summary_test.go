package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRegisteredSummaryReadsPreservePublishedPAC(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	fixture := func(suffix string) []byte {
		t.Helper()
		b, e := os.ReadFile("../../../testdata/generated-summaries/day." + suffix)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	directory := filepath.Join(root, "demo", "lab", "measurements")
	for _, path := range []string{directory, filepath.Join(root, "definitions")} {
		if e := os.MkdirAll(path, 0700); e != nil {
			t.Fatal(e)
		}
	}
	const first = int64(1767225600000)
	run := timeRun(first, 0, 27)
	timeline, e := buildTimeDirectory([]Run{run}, first, first+summaryDayMS, 28, [16]byte{1})
	if e != nil {
		t.Fatal(e)
	}
	for path, raw := range map[string][]byte{
		"trunk.trie": fixture("trunk"), "definitions/summary-hour.rx": fixture("source.rx"),
		"definitions/summary-hour-summaries.rx": fixture("summary.rx"),
		"demo/lab/measurements/sample.0.summ":   fixture("summ"), "demo/lab/measurements/sample.0.time": timeline.raw,
	} {
		if e = os.WriteFile(filepath.Join(root, path), raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	endpoint, e := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", Now: func() time.Time { return time.UnixMilli(first + 2*summaryDayMS) }})
	if e != nil {
		t.Fatal(e)
	}
	defer endpoint.Close()
	request := httptest.NewRequest("GET", fmt.Sprintf("/demo/lab/measurements/sample/summaries?range=%d,%d", first, first+summaryDayMS), nil)
	request.Header.Set("Trunk-Branch", "1")
	reply := httptest.NewRecorder()
	endpoint.ServeHTTP(reply, request)
	if reply.Code != 200 {
		t.Fatalf("summary retrieval: %d %s", reply.Code, reply.Body.String())
	}
	if !bytes.Equal(reply.Body.Bytes(), fixture("pac")) {
		t.Fatal("HTTP changed the sealed summary PAC")
	}
	if reply.Header().Get("Trunk-Pack-Count") != "1" || reply.Header().Get("Trunk-Format") != "summary-packs/0" {
		t.Fatal(reply.Header())
	}
	for _, snapshot := range endpoint.registeredReads {
		if snapshot.refs.Load() != 1 || snapshot.summaryIndex == nil {
			t.Fatal("summary footer or lease was not retained")
		}
	}
	// The summary lineage remains a separately cacheable resource.
	resource := httptest.NewRequest("GET", "/demo/definitions?file=summary-hour-summaries.rx", nil)
	response := httptest.NewRecorder()
	endpoint.ServeHTTP(response, resource)
	if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), fixture("summary.rx")) || response.Header().Get("ETag") == "" {
		t.Fatal("summary Definition resource unavailable", response.Code)
	}
}

func TestAnnotationPublicationReusesRegisteredSummaryDefinition(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	target := SeedRegistration{Cache: "demo", Service: "lab", Resource: "measurements", ResourceType: "sample", File: "sample.rx"}
	definition := func(annotated bool) []byte {
		t.Helper()
		annotations := "[]"
		if annotated {
			annotations = `["#summ(boolean)"]`
		}
		value, e := seedNativeDocument([]byte(fmt.Sprintf(`[{"header":{"rx-name":"sample","rx-major":1},"fields":[{"name":"duration","type":[1,0],"size":4,"annotations":["#summ(numeric)"]},{"name":"cached","type":[6,0],"size":1,"annotations":%s}]}]`, annotations)))
		if e != nil {
			t.Fatal(e)
		}
		defer value.close()
		return bytes.Clone(value.bytes)
	}
	before := definition(false)
	if e := RegisterSeedDefinition(root, before, target); e != nil {
		t.Fatal(e)
	}
	read := func(name string) []byte {
		t.Helper()
		b, e := os.ReadFile(filepath.Join(root, name))
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	summary := read("definitions/summary.rx")
	route, e := taxonomyRoute(root, 1)
	if e != nil {
		t.Fatal(e)
	}
	after := definition(true)
	if e = RegisterSeedDefinition(root, after, target); e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(summary, read("definitions/summary.rx")) {
		t.Fatal("annotation publication changed the generic summary Definition")
	}
	next, e := taxonomyRoute(root, 1)
	if e != nil {
		t.Fatal(e)
	}
	if next.Definition != route.Definition || next.Digest != route.Digest {
		t.Fatal("annotation publication changed the event schema identity")
	}
	if !bytes.Equal(after, read("definitions/sample.rx")) {
		t.Fatal("updated annotations were not published")
	}
	binding, e := seedNativeBinding(read("trunk.trie"), 1)
	if e != nil {
		t.Fatal(e)
	}
	defer binding.close()
	var resource struct{ Summaries []uint32 }
	if e = json.Unmarshal(binding.bytes, &resource); e != nil {
		t.Fatal(e)
	}
	if len(resource.Summaries) != 1 {
		t.Fatal("expected one generic summary Definition", string(binding.bytes))
	}
	if e = RegisterSeedDefinition(root, before, target); e != nil {
		t.Fatal(e)
	}
	rollback, e := seedNativeBinding(read("trunk.trie"), 1)
	if e != nil {
		t.Fatal(e)
	}
	defer rollback.close()
	var restored struct{ Summaries []uint32 }
	if e = json.Unmarshal(rollback.bytes, &restored); e != nil {
		t.Fatal(e)
	}
	if len(restored.Summaries) != 1 || restored.Summaries[0] != resource.Summaries[0] || !bytes.Equal(before, read("definitions/sample.rx")) {
		t.Fatal("rollback did not restore the original annotations and summary binding")
	}
}
