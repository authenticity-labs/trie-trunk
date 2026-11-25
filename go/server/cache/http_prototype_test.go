//go:build trunk_prototype

package cache

import (
	"bytes"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeHTTPInterop(t *testing.T) {
	l := adaptiveLayout(t, testLayout(t, false).Definition, "16")
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	h := &HTTP{Store: s, Token: "test"}
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	frame := testFrame(t, l, at, 1, false)
	request := func(method, path string, body []byte, known bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer test")
		r.Header.Set("PIL-Version", "2")
		r.Header.Set("PIL-Features", "adaptive-integers,native-ranges")
		r.Header.Set("Accept-Encoding", "gzip")
		r.Header.Set("Accept", "application/pil")
		r.Header.Set("Content-Type", "application/pil")
		r.Header.Set("Definition-Version", fmtDigest(l))
		if known {
			r.Header.Set("Definition-Known", fmtDigest(l))
		}
		w := httptest.NewRecorder()
		h.serveHTTP(w, r)
		return w
	}
	tap := url.QueryEscape(h.tap(Write))
	w := request("POST", "/ingest?tap="+tap, frame, false)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	path := "/collect?tap=" + tap + "&from=2026-09-01T00:00:00Z&to=2026-09-02T00:00:00Z"
	w = request("GET", path, nil, false)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	raw := ungzipTest(t, w.Body.Bytes())
	if !bytes.HasPrefix(raw, []byte{'T', 'P', 'I', 'L', 2, 1}) || !bytes.HasSuffix(raw, []byte{255, 254}) {
		t.Fatal("framing")
	}
	define := &pil.Layout{Widths: []int{0, 0, 0}}
	reader := bytes.NewReader(raw[6:])
	d, e := define.ReadFrame(reader, nil)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(d, l.Bytes) {
		t.Fatal("missing unified map")
	}
	if tag, _ := reader.ReadByte(); tag != 2 {
		t.Fatal("binding")
	}
	bind := &pil.Layout{Widths: []int{4, 0, 0}}
	if _, e = bind.ReadFrame(reader, nil); e != nil {
		t.Fatal(e)
	}
	_, chunks := readNativeTest(t, w.Body.Bytes())
	pacs := 0
	for _, chunk := range chunks {
		if chunk.Kind == "pac" {
			pacs++
		}
	}
	if pacs != 1 {
		t.Fatalf("expected whole PAC, got %d", pacs)
	}
	warm := request("GET", path, nil, true)
	if ungzipTest(t, warm.Body.Bytes())[5] != 2 {
		t.Fatal("definition downloaded again")
	}
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer test")
	r.Header.Set("PIL-Version", "2")
	w = httptest.NewRecorder()
	h.serveHTTP(w, r)
	if w.Code != http.StatusUpgradeRequired {
		t.Fatal("unnegotiated integer profile")
	}
	if dir := os.Getenv("PIL_NATIVE_FIXTURE_DIR"); dir != "" {
		// Standalone codec fixture, not a collection endpoint response.
		var fixture bytes.Buffer
		fixture.Write([]byte{'T', 'P', 'I', 'L', 2})
		if e = writeDefine(&fixture, l); e != nil {
			t.Fatal(e)
		}
		if e = writeBind(&fixture, 0, l.Digest, h.tap(Write)); e != nil {
			t.Fatal(e)
		}
		if e = writeEvent(&fixture, 0, frame); e != nil {
			t.Fatal(e)
		}
		fixture.Write([]byte{255, 254})
		raw = fixture.Bytes()
		if e = os.MkdirAll(dir, 0755); e != nil {
			t.Fatal(e)
		}
		for n, b := range map[string][]byte{"definition.rx": l.Bytes, "collect.pil": raw, "frame.pil": frame} {
			if e = os.WriteFile(filepath.Join(dir, n), b, 0644); e != nil {
				t.Fatal(e)
			}
		}
	}
}
