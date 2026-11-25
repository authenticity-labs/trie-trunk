package cache

import (
	"encoding/json"
	"math"
	"net/http/httptest"

	"slices"
	"testing"
	"time"
)

func TestStorageSnapshotLiveSealedAndReadOnly(t *testing.T) {
	l := testLayout(t, false)
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	now := start.Add(30 * time.Minute)
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Append(Write, testFrame(t, l, start, 1, false)); err != nil {
		t.Fatal(err)
	}
	check := func() storageSnapshot {
		t.Helper()
		h := &HTTP{Store: s, Token: "secret"}
		req := httptest.NewRequest("GET", "/storage", nil)
		req.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var result storageSnapshot
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		var events uint64
		var bytes float64
		for _, sample := range result.Samples {
			events += sample.Events
			bytes += sample.Bytes
			if sample.Hour != start.UnixMilli() {
				t.Fatal(sample)
			}
		}
		if events != 1 || result.Allocated == 0 || math.Abs(bytes+float64(result.Unattributed)-float64(result.Allocated)) > 1 {
			t.Fatalf("bad snapshot %+v", result)
		}
		return result
	}
	check()
	now = start.Add(2 * time.Hour)
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	before := len(s.branches[Write].runs)
	check()
	if before != len(s.branches[Write].runs) {
		t.Fatal("snapshot mutated storage")
	}
	for _, tc := range []struct {
		method, path, token string
		status              int
	}{{"GET", "/storage", "", 401}, {"POST", "/storage", "secret", 405}, {"GET", "/storage?root=/", "secret", 400}} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		(&HTTP{Store: s, Token: "secret"}).ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Fatal(tc, w.Code)
		}
	}
}
func TestStorageEmptyEndpoint(t *testing.T) {
	p, err := OpenEndpoint(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	w := httptest.NewRecorder()
	p.operationHandler("").ServeHTTP(w, httptest.NewRequest("GET", "/storage", nil))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var result storageSnapshot
	if err = json.Unmarshal(w.Body.Bytes(), &result); err != nil || result.Samples == nil || len(result.Samples) != 0 {
		t.Fatal(result, err)
	}
}

// Used by the registered mixed-version lifecycle before ingestion, while live,
// after sealing, and after restart. Each major has its own physical file.
func checkRegisteredStorage(t *testing.T, endpoint *Endpoint, scope []string, hour int64, count uint64) {
	t.Helper()
	result := httptest.NewRecorder()
	endpoint.Handler("").ServeHTTP(result, httptest.NewRequest("GET", "/storage", nil))
	var got storageSnapshot
	if err := json.Unmarshal(result.Body.Bytes(), &got); err != nil || result.Code != 200 {
		t.Fatal(result.Code, result.Body.String(), err)
	}
	var allocated, events uint64
	var weighted float64
	var branches int
	for _, node := range got.Nodes {
		allocated += node.OwnAllocated
		if node.Kind == "branch" {
			branches++
			if !slices.Equal(node.Path, scope) {
				t.Fatal("unexpected storage branch", node.Path)
			}
			if count > 0 && node.OwnLogical == 0 {
				t.Fatal("branch files have no owner", node)
			}
		}
	}
	if count > 0 && len(got.Samples) != 1 {
		t.Fatalf("expected one logical branch-hour across major versions, got %d", len(got.Samples))
	}
	for _, sample := range got.Samples {
		if !slices.Equal(sample.Path, scope) || sample.Hour != hour {
			t.Fatal(sample)
		}
		events += sample.Events
		weighted += sample.Bytes
	}
	if branches != 1 || allocated != got.Allocated || events != count || math.Abs(weighted+float64(got.Unattributed)-float64(allocated)) > 1 {
		t.Fatalf("branches %d, attributed bytes %d of %d, events %d of %d", branches, allocated, got.Allocated, events, count)
	}
}
