package cache

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func withoutBranch(q url.Values) string {
	copy := url.Values{}
	for k, v := range q {
		if k != "branch" {
			copy[k] = v
		}
	}
	return copy.Encode()
}

// Regression: a resource GET must not run the sealing writer.
func TestSeedGETDoesNotSeal(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	now := at.Add(time.Minute)
	calls := 0
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }, sealHook: func(phase string) error {
		if phase == "prepare" {
			calls++
			return fmt.Errorf("diagnostic: GET entered sealing")
		}
		return nil
	}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Append(Read, testFrame(t, l, at, 1, false)); e != nil {
		t.Fatal(e)
	}
	now = at.Add(80 * time.Minute)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/demo/payment/transactions.v1/read/packs?beginTime="+fmt.Sprint(at.UnixMilli()), nil)
	r.Header.Set("Accept", "application/pil")
	(&HTTP{Store: s}).ServeHTTP(w, r)
	if calls != 0 {
		t.Fatalf("resource GET invoked sealing %d time(s), HTTP %d: %s", calls, w.Code, w.Body.String())
	}
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestTaxonomyETag(t *testing.T) {
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: testLayout(t, false)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := &HTTP{Store: s}
	get := func(tag string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/demo/registries?kind=taxonomy", nil)
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("If-None-Match", tag)
		h.ServeHTTP(w, r)
		return w
	}
	first := get("")
	if first.Code != 200 || first.Header().Get("ETag") == "" {
		t.Fatal(first.Code, first.Body.String())
	}
	raw, err := os.ReadFile(filepath.Join(s.config.Root, "trunk.trie"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, first.Body.Bytes()) {
		t.Fatal("taxonomy was filtered or transformed")
	}
	unchanged := get(first.Header().Get("ETag"))
	if unchanged.Code != 304 || unchanged.Body.Len() != 0 {
		t.Fatal(unchanged.Code, unchanged.Body.String())
	}
	if stale := get("\"stale\""); stale.Code != 200 || !bytes.Equal(stale.Body.Bytes(), raw) {
		t.Fatal("stale reader did not receive the whole trie")
	}
}

// Empty hours still need the snapshot boundary even though no child Pack can
// carry it. This exercises the existing handler without a synthetic wire body.
func TestSeedEmptyResponseSnapshotHeaders(t *testing.T) {
	l := testLayout(t, false)
	now := time.Date(2026, 11, 10, 10, 15, 0, 0, time.UTC)
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := &HTTP{Store: s}
	q := url.Values{"tap": {"demo/payment/transactions.v1/read"}, "from": {now.Add(-time.Hour).Format(time.RFC3339Nano)}, "to": {"now"}}
	response := httptest.NewRecorder()
	h.seedPacks(response, httptest.NewRequest("GET", "/packs?"+q.Encode(), nil))
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	if response.Header().Get("Trunk-Window-To") != fmt.Sprint(now.UnixMilli()) {
		t.Fatal("missing final time slot")
	}
	if response.Header().Get("Trunk-Generation") == "" || response.Header().Get("Trunk-Generation-Sequence") == "" {
		t.Fatal("missing snapshot identity")
	}
	sealed, err := time.Parse(time.RFC3339Nano, response.Header().Get("Trunk-Sealed-Through"))
	if err != nil {
		t.Fatal(err)
	}
	retained, err := time.Parse(time.RFC3339Nano, response.Header().Get("Trunk-Retained-From"))
	if err != nil {
		t.Fatal(err)
	}
	if retained.After(sealed) || sealed.After(now) || sealed.UnixMilli()%TimeHourMS != 0 {
		t.Fatal("invalid snapshot bounds")
	}
}
