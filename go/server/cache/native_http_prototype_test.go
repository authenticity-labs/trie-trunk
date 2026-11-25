//go:build trunk_prototype

package cache

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeCollectHTTPGapOnlyAndLazyPayload(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	p, e := OpenEndpoint(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(2 * time.Hour) }})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	s := p.api.Store
	for i := 0; i < 6; i++ {
		if _, e = s.Append(Write, testFrame(t, l, at.Add(time.Duration(i)*10*time.Minute), uint32(i+1), false)); e != nil {
			t.Fatal(e)
		}
	}
	from, to := at.Add(12*time.Minute), at.Add(28*time.Minute)
	path := "/collect?format=native&tap=" + url.QueryEscape(p.api.tap(Write)) + "&from=" + url.QueryEscape(from.Format(time.RFC3339Nano)) + "&to=" + url.QueryEscape(to.Format(time.RFC3339Nano))
	request := func(have *Coverage, capability bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("PIL-Version", "2")
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("Accept-Encoding", "gzip")
		if capability {
			r.Header.Set("PIL-Features", "native-ranges,adaptive-integers")
		}
		if have != nil {
			raw, _ := json.Marshal([]Coverage{*have})
			r.Header.Set("Trunk-Coverage", base64.RawURLEncoding.EncodeToString(raw))
			r.Header.Set("Definition-Known", fmtDigest(l))
		}
		w := httptest.NewRecorder()
		p.serveHTTP(w, r)
		return w
	}
	if w := request(nil, false); w.Code != 426 {
		t.Fatal("unnegotiated extension", w.Code)
	}
	cold := request(nil, true)
	if cold.Code != 200 {
		t.Fatal(cold.Code, cold.Body.String())
	}
	ms, chunks := readNativeTest(t, cold.Body.Bytes())
	if len(ms) != 1 || ms[0].Provisional || len(ms[0].Index) != 1 || ms[0].Index[0] != (Span{1, 7}) {
		t.Fatal("not complete hourly index", ms)
	}
	var idx, packs []byte
	for _, c := range chunks {
		if c.Kind == "index" {
			idx = append(idx, c.Data...)
		}
		if c.Kind == "pac" {
			packs = append(packs, c.Data...)
		}
	}
	if !bytes.Equal(idx, s.branches[Write].rows[IndexRowSize:]) {
		t.Fatal("index was reencoded")
	}
	lo, hi, e := s.branches[Write].packsRange(2, s.row)
	if e != nil {
		t.Fatal(e)
	}
	source, e := readBytes(s.branches[Write].packs, lo, hi)
	if e != nil || !bytes.Equal(source, packs) {
		t.Fatal("not exact lazy payload", e)
	}
	m := ms[0]
	have := Coverage{Definition: m.Definition, Generation: m.Generation, Index: m.Index, MemberRanges: m.MemberRanges, TrieNodes: m.TrieNodes}
	warm := request(&have, true)
	_, chunks = readNativeTest(t, warm.Body.Bytes())
	for _, c := range chunks {
		if c.Kind != "state" {
			t.Fatal("redundant transfer", c.Kind)
		}
	}
	have.Index = []Span{{1, 3}, {4, 7}}
	warm = request(&have, true)
	_, chunks = readNativeTest(t, warm.Body.Bytes())
	for _, c := range chunks {
		if c.Kind == "index" && (c.Start != 1 || len(c.Data) != 6*IndexRowSize) {
			t.Fatal("wrong gap", c)
		}
	}
	if dir := os.Getenv("PIL_NATIVE_RANGE_FIXTURE_DIR"); dir != "" {
		if e = os.MkdirAll(dir, 0755); e != nil {
			t.Fatal(e)
		}
		exportViewNative(t, dir, p.api, at, at.Add(time.Hour))
		oldPath := path
		path += "&data=none"
		indexOnly := request(nil, true)
		path = oldPath
		states, e := os.ReadFile(s.shared.Name())
		if e != nil {
			t.Fatal(e)
		}
		for name, raw := range map[string][]byte{"leaf.trie": states, "whole-index.bin": s.branches[Write].rows, "ranges.pil": ungzipTest(t, cold.Body.Bytes()), "indexes.pil": ungzipTest(t, indexOnly.Body.Bytes()), "definition.rx": l.Bytes, "index.bin": idx, "packs.bin": packs} {
			if e = os.WriteFile(filepath.Join(dir, name), raw, 0644); e != nil {
				t.Fatal(e)
			}
		}
	}
	// Completion lives in the response body; an interrupted body is never enough
	// to publish coverage even if all payload chunks happened to arrive.
}
