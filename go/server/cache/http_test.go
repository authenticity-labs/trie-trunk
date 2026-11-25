package cache

import (
	"bytes"
	"context"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func fmtDigest(l *Layout) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 64)
	for i, b := range l.Digest {
		out[i*2] = digits[b>>4]
		out[i*2+1] = digits[b&15]
	}
	return string(out)
}

func TestPILIngressStagingAndReceiverStubs(t *testing.T) {
	l := testLayout(t, false)
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	h := &HTTP{Store: s, Token: "secret"}
	request := func(kind string, body []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/ingest?tap="+url.QueryEscape(h.tap(Write)), bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer secret")
		r.Header.Set("PIL-Version", "2")
		r.Header.Set("Definition-Version", fmtDigest(l))
		r.Header.Set("Content-Type", kind)
		w := httptest.NewRecorder()
		h.serveHTTP(w, r)
		return w
	}
	for _, kind := range []string{"application/jsonl", "application/x-ndjson", "text/plain", "application/syslog"} {
		if w := request(kind, []byte("not parsed")); w.Code != 415 {
			t.Fatal(kind, w.Code, w.Body.String())
		}
	}
	if w := request("application/cbor", []byte("not parsed")); w.Code != 415 {
		t.Fatal("typed destination accepted generic CBOR", w.Code)
	}
	raw := testFrame(t, l, now.Add(-time.Minute), 1, false)
	if w := request("Application/PIL", raw); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if len(s.branches[Write].rows) != 0 || s.branches[Write].live.count(0) != 1 {
		t.Fatal("HTTP bypassed holdback")
	}
	now = now.Add(time.Hour)
	if e = s.Flush(); e != nil {
		t.Fatal(e)
	}
	raw = testFrame(t, l, now.Add(-2*time.Hour), 2, false)
	if w := request("application/pil", raw); w.Code != 409 || w.Header().Get("Trunk-Received") != "0" {
		t.Fatal(w.Code, w.Body.String())
	}
	r := httptest.NewRequest("GET", "/rejections", nil)
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	h.serveHTTP(w, r)
	if w.Code != 200 || !bytes.Contains(w.Body.Bytes(), []byte(`"message":"request completed"`)) || !bytes.Contains(w.Body.Bytes(), []byte(`"pil_base64"`)) {
		t.Fatal(w.Code, w.Body.String())
	}
	r = httptest.NewRequest("GET", "/rejections", nil)
	w = httptest.NewRecorder()
	h.serveHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("unauthenticated rejection log")
	}
}

// Ingress buffering must not limit frame size or change partial-read handling.
func TestPILIngressLargeFrameAndFragmentedBody(t *testing.T) {
	l := testLayout(t, false)
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := &HTTP{Store: s}
	values := []Value{uuid(now.UnixMilli(), 1), testObject(42), Uint(1, 8), {}, {}, Text(string(bytes.Repeat([]byte("large field "), 24000))), {}}
	first, err := l.Encode(nil, values)
	if err != nil {
		t.Fatal(err)
	}
	second := testFrame(t, l, now.Add(time.Millisecond), 2, true)
	body := &fragmentedIngressBody{bytes.NewReader(append(bytes.Clone(first), second...))}
	r := httptest.NewRequest("POST", "/ingest?tap="+url.QueryEscape(h.tap(Write)), body)
	r.Header.Set("PIL-Version", "2")
	r.Header.Set("Definition-Version", fmtDigest(l))
	r.Header.Set("Content-Type", "application/pil")
	w := httptest.NewRecorder()
	h.serveHTTP(w, r)
	if w.Code != 200 || w.Header().Get("Trunk-Received") != "2" || w.Header().Get("Trunk-Acknowledgement") != "durable-staging" {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	var rows [][]byte
	if err = s.SearchLive(context.Background(), Query{Branch: Write, From: now, To: now.Add(time.Second)}, func(raw []byte) error {
		rows = append(rows, bytes.Clone(raw))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || !sameSeedData(t, l, rows[0], first) || !sameSeedData(t, l, rows[1], second) {
		t.Fatal("large or trailing sparse frame changed during ingress")
	}
}

type fragmentedIngressBody struct{ *bytes.Reader }

func (r *fragmentedIngressBody) Read(p []byte) (int, error) {
	return r.Reader.Read(p[:min(len(p), 37)])
}
