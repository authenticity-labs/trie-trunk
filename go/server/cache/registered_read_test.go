package cache

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRegisteredPACKSReadUsesRXAndExistingTimeAddresses(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	fixture := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join("../../../testdata/registered-trunk", name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	// These fixtures are authored by the Rust registration test, using DOC
	// builders and SeedDefinition.ingest for every event.
	rx := fixture("sample.rx")
	const hour = int64(1_793_520_000_000)
	for version, kind := range []string{"http", "security"} {
		target := SeedRegistration{Cache: "demo", Service: "payment", Resource: "transactions", ResourceType: kind, Level: "", File: "sample.rx",
			Selection: &SeedDefinitionSelection{Major: 1, Namespace: "measurements", Name: "sample", Minor: uint32(version)}}
		if err := RegisterSeedDefinition(root, rx, target); err != nil {
			t.Fatal(err)
		}
		directory := filepath.Join(root, "demo", "payment", "transactions")
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		fixtureName, count := "sample", uint64(3)
		if version == 1 {
			fixtureName, count = "current", 1
		}
		storedName := fixtureName + ".packs"
		if version == 0 {
			storedName = "sample.zstd.packs"
		}
		packs := fixture(storedName)
		run := timeRun(hour, 0, count)
		run.PacksStart = 0
		timeline, err := buildTimeDirectory([]Run{run}, hour-TimeHourMS, hour+2*TimeHourMS, count+1, [16]byte{1})
		if err != nil {
			t.Fatal(err)
		}
		for suffix, raw := range map[string][]byte{".time": timeline.raw, ".packs": packs, ".leaf": fixture(map[int]string{0: "old.leaf", 1: "current.leaf"}[version])} {
			if err = os.WriteFile(filepath.Join(directory, kind+".v1"+suffix), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	endpoint, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", Now: func() time.Time { return time.UnixMilli(hour + 3*TimeHourMS) }})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	// Type files are separate; retrieval retains the existing endpoint and bytes.
	stateBytes := fixture("old.leaf")
	for _, path := range []string{"/demo/payment/transactions/leaves", "/demo/registries?kind=inventory"} {
		request := httptest.NewRequest("GET", path, nil)
		if path != "/demo/registries?kind=inventory" {
			request.Header.Set("Trunk-Branch", "1")
		}
		response := httptest.NewRecorder()
		endpoint.ServeHTTP(response, request)
		if response.Code != 200 {
			t.Fatalf("state publication: %s %d %s", path, response.Code, response.Body.String())
		}
		if path != "/demo/registries?kind=inventory" && !bytes.Equal(response.Body.Bytes(), stateBytes) {
			t.Fatal("state resource was rewritten")
		}
		request.Header.Set("If-None-Match", response.Header().Get("ETag"))
		unchanged := httptest.NewRecorder()
		endpoint.ServeHTTP(unchanged, request)
		if unchanged.Code != 304 {
			t.Fatal("state resource did not revalidate", unchanged.Code)
		}
	}
	if len(endpoint.registeredLeaves) != 2 {
		t.Fatal("type state snapshots were not separated")
	}
	for _, snapshot := range endpoint.registeredLeaves {
		if len(snapshot.definitions) != 1 {
			t.Fatal("missing version root")
		}
	}
	request := httptest.NewRequest("GET", "/demo/payment/transactions/leaves", nil)
	missing := httptest.NewRecorder()
	endpoint.ServeHTTP(missing, request)
	if missing.Code != 400 {
		t.Fatal("ambiguous type must require a selector", missing.Code)
	}
	request.Header.Set("Trunk-Branch", "2")
	state := httptest.NewRecorder()
	endpoint.ServeHTTP(state, request)
	if state.Code != 200 || !bytes.Equal(state.Body.Bytes(), fixture("current.leaf")) {
		t.Fatal("wrong type state snapshot", state.Code)
	}
	get := func(id int, operation, query string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest("GET", "/demo/payment/transactions/"+map[int]string{1: "http", 2: "security"}[id]+"/"+operation+"?"+query, nil)
		r.Header.Set("Trunk-Branch", fmt.Sprint(id))
		w := httptest.NewRecorder()
		endpoint.ServeHTTP(w, r)
		return w
	}
	for _, id := range []int{1, 2} {
		name := "sample.zstd.pac"
		if id == 2 {
			name = "current.pac"
		}
		pack := fixture(name)
		w := get(id, "packs", fmt.Sprintf("beginTime=%d&endTime=%d", hour-TimeHourMS, hour+2*TimeHourMS))
		if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), pack) {
			t.Fatalf("branch %d: %d %s", id, w.Code, w.Body.String())
		}
		if w.Header().Get("Trunk-Branch") != fmt.Sprint(id) || w.Header().Get("Trunk-Format") != "seed-packs/0" {
			t.Fatal(w.Header())
		}
		beds := fixture(map[int]string{1: "sample.beds", 2: "current.beds"}[id])
		if w.Header().Get("Trunk-Beds-Length") != fmt.Sprint(len(beds)) || w.Header().Get("Trunk-Beds-Packs") != "1" {
			t.Fatal("missing published replica scalars", w.Header())
		}
		// Complete-empty is distinct from an unavailable or incomplete hour.
		empty := get(id, "packs", fmt.Sprintf("beginTime=%d&endTime=%d", hour-TimeHourMS, hour))
		if empty.Code != 200 || empty.Body.Len() != 0 || empty.Header().Get("Trunk-Sealed-Through") == "" {
			t.Fatal(empty.Code, empty.Body.String())
		}
		timeline := get(id, "timelines", fmt.Sprintf("beginTime=%d&endTime=%d", hour-TimeHourMS, hour+2*TimeHourMS))
		if timeline.Code != 200 || timeline.Body.Len() != TimeTrieHeader+3*16 {
			t.Fatal(timeline.Code, timeline.Body.Len(), timeline.Body.String())
		}
	}
	if len(endpoint.registeredReads) != 2 {
		t.Fatal("resource types shared their storage snapshot")
	}
	for _, snapshot := range endpoint.registeredReads {
		if snapshot.refs.Load() != 1 {
			t.Fatal("request leaked a read lease")
		}
	}
}

func TestRustTimeTrieMatchesExistingHourAddressing(t *testing.T) {
	for _, year := range []int{2024, 2025, 2026} {
		first := time.Date(year, 12, 31, 23, 0, 0, 0, time.UTC).UnixMilli()
		runs := []Run{timeRun(first+TimeHourMS, 0, 3)}
		runs[0].PacksStart = 70000
		original, err := buildTimeDirectory(runs, first, first+3*TimeHourMS, 4, [16]byte{1})
		if err != nil {
			t.Fatal(err)
		}
		reader, err := seedNativeOpenTime(original.raw)
		if err != nil {
			t.Fatal(err)
		}
		selection, err := reader.selectRange(uint64(first), uint64(first+3*TimeHourMS))
		if err != nil {
			reader.close()
			t.Fatal(err)
		}
		if len(selection.leaves) != 3 {
			t.Fatal("missing year boundary leaves")
		}
		for _, leaf := range selection.leaves {
			prior, ok := original.leaf(int64(leaf.Hour))
			if !ok || leaf.Start != prior.Start || leaf.End != prior.End || leaf.Packs != prior.Packs || leaf.Beds != prior.Beds {
				t.Fatal("time addressing changed", leaf, prior)
			}
		}
		selection.close()
		reader.close()
	}
}

func TestRegisteredIndexRequestsUsePinnedRustTimeSpans(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	files := map[string]string{"trunk.trie": "trunk.trie", "indexed.rx": "definitions/indexed.rx", "indexed.summary.rx": "definitions/indexed.summary.rx", "indexed.summ": "test/lab/indexed/sample.0.summ", "indexed.packs": "test/lab/indexed/sample.0.packs", "indexed.time": "test/lab/indexed/sample.0.time", "indexed.indx": "test/lab/indexed/sample.0.indx", "sample.leaf": "test/lab/indexed/sample.0.leaf"}
	var expected []byte
	for fixture, target := range files {
		raw, err := os.ReadFile(filepath.Join("../../../testdata/registered-indexed", fixture))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, target)
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if fixture == "indexed.indx" {
			expected = raw
		}
	}
	endpoint, err := OpenEndpoint(Config{Root: root, Cache: "test", Tenant: "local"})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	get := func(rows string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/test/lab/indexed/sample/indexes?generation=00000000000000000000000000000000&"+rows, nil)
		r.Header.Set("Trunk-Branch", "2")
		w := httptest.NewRecorder()
		endpoint.ServeHTTP(w, r)
		return w
	}
	for range 2 {
		w := get("rows=0,10001&rows=10001,20002")
		if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), expected) || w.Header().Get("Trunk-Format") != "index-segments/0" {
			t.Fatalf("index response: %d %s", w.Code, w.Body.String())
		}
	}
	if w := get("rows=1,10001"); w.Code != 409 {
		t.Fatalf("partial hour accepted: %d", w.Code)
	}
	if w := get("rows=10001,20002&rows=0,10001"); w.Code != 409 {
		t.Fatalf("unordered hours accepted: %d", w.Code)
	}
	for _, snapshot := range endpoint.registeredReads {
		if snapshot.index == nil || snapshot.index.refs.Load() != 1 {
			t.Fatal("index read lease leaked")
		}
	}
}

func TestRegisteredTimelineKeepsRequestDefaultsAndOrdering(t *testing.T) {
	now := time.Date(2026, 11, 12, 15, 20, 0, 0, time.UTC)
	first := now.Truncate(time.Hour).Add(-2 * time.Hour).UnixMilli()
	request := func(q string) ([]summaryRange, error) {
		return registeredTimelineRanges(httptest.NewRequest("GET", "/demo/service/resource/info/timelines?"+q, nil), now)
	}
	ranges, err := request(fmt.Sprintf("beginTime=%d", first))
	if err != nil || len(ranges) != 1 || ranges[0].to != now.Truncate(time.Hour).Add(time.Hour).UnixMilli() {
		t.Fatal(ranges, err)
	}
	for _, q := range []string{
		fmt.Sprintf("beginTime=%d&endTime=%d", first+1, first+TimeHourMS),
		fmt.Sprintf("range=%d,%d&range=%d,%d", first+TimeHourMS, first+2*TimeHourMS, first, first+TimeHourMS),
		fmt.Sprintf("range=%d,%d&beginTime=%d", first, first+TimeHourMS, first),
	} {
		if _, err := request(q); err == nil {
			t.Fatalf("accepted %s", q)
		}
	}
}
