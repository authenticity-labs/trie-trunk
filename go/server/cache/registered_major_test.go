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

func TestRegisteredMajorGenerationsShareURLAndKeepIndependentStorage(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	read := func(name string) []byte {
		t.Helper()
		b, e := os.ReadFile("../../../testdata/major-generations/" + name)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	if e := os.MkdirAll(filepath.Join(root, "definitions"), 0700); e != nil {
		t.Fatal(e)
	}
	for name, raw := range map[string][]byte{"trunk.trie": read("trunk.trie"), "definitions/query.rx": read("query.rx")} {
		if e := os.WriteFile(filepath.Join(root, name), raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	const hour = int64(3600000)
	now := time.UnixMilli(hour + 100)
	open := func() *Endpoint {
		t.Helper()
		p, e := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", Now: func() time.Time { return now }})
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	p := open()
	defer func() { p.Close() }()
	request := func(branch int, operation string, body []byte) *httptest.ResponseRecorder {
		t.Helper()
		method := "GET"
		query := fmt.Sprintf("?beginTime=%d&endTime=%d", hour, hour+TimeHourMS)
		if operation == "seeds" {
			method = "POST"
			query = ""
		}
		r := httptest.NewRequest(method, "/demo/trunk-client/queries/query/"+operation+query, bytes.NewReader(body))
		r.Header.Set("Trunk-Branch", fmt.Sprint(branch))
		r.Header.Set("Content-Type", "application/pil")
		w := httptest.NewRecorder()
		p.ServeHTTP(w, r)
		expected := 200
		if method == "POST" {
			expected = 204
		}
		if w.Code != expected {
			t.Fatalf("branch %d %s: %d %s", branch, operation, w.Code, w.Body.String())
		}
		return w
	}
	for _, entry := range []struct {
		branch int
		file   string
	}{{1, "v1.pil"}, {2, "v1-minor.pil"}, {3, "v2.pil"}, {1, "v1.pil"}} {
		request(entry.branch, "seeds", read(entry.file))
	}
	old, newer := request(1, "packs", nil).Body.Bytes(), request(3, "packs", nil).Body.Bytes()
	if bytes.Equal(old, newer) {
		t.Fatal("major generations shared a live Pack")
	}
	for _, major := range []int{1, 2} {
		if _, e := os.Stat(filepath.Join(root, fmt.Sprintf("demo/trunk-client/queries/query.v%d.seeds", major))); e != nil {
			t.Fatal("missing independent recovery spool", e)
		}
	}

	p.Close()
	p = open()
	if !bytes.Equal(old, request(2, "packs", nil).Body.Bytes()) || !bytes.Equal(newer, request(3, "packs", nil).Body.Bytes()) {
		t.Fatal("recovery changed generation membership")
	}
	now = time.UnixMilli(hour + TimeHourMS + 900000)
	if e := p.flushRegistered(uint64(now.UnixMilli())); e != nil {
		t.Fatal(e)
	}
	for _, entry := range []struct{ branch, major int }{{2, 1}, {3, 2}} {
		timeline := request(entry.branch, "timelines", nil)
		stem := fmt.Sprintf("demo/trunk-client/queries/query.v%d", entry.major)
		if timeline.Header().Get("Trunk-Path") != stem+".time" {
			t.Fatal("wrong generation", timeline.Header())
		}
		if timeline.Header().Get("Trunk-Source") != "demo/trunk-client/queries/query" {
			t.Fatal("physical version escaped into logical resource")
		}
		raw, e := os.ReadFile(filepath.Join(root, stem+".packs"))
		if e != nil || len(raw) == 0 {
			t.Fatal("missing sealed generation", e)
		}
		request(entry.branch, "packs", nil)
	}
	p.Close()
	p = open()
	request(1, "packs", nil)
	request(3, "packs", nil)
}
