//go:build trunk_prototype

package cache

import (
	"bytes"
	"context"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestHTTPGzipIngressMembersRecovery(t *testing.T) {
	l := testLayout(t, false)
	now := time.Now()
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	h := &HTTP{Store: s}
	a, b := testFrame(t, l, now, 1, false), testFrame(t, l, now, 2, false)
	packed := append(gzipMember(t, a), gzipMember(t, b)...)
	r := httptest.NewRequest("POST", "/ingest?tap="+url.QueryEscape(h.tap(Write)), bytes.NewReader(packed))
	r.Header.Set("Content-Type", "application/pil")
	r.Header.Set("Content-Encoding", "gzip")
	r.Header.Set("PIL-Version", "2")
	r.Header.Set("Definition-Version", fmtDigest(l))
	w := httptest.NewRecorder()
	h.serveHTTP(w, r)
	if w.Code != 200 || w.Header().Get("Trunk-Received") != "2" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	for _, p := range s.branches[Write].pendingAll() {
		if !sameSeedData(t, l, p.frame, a) && !sameSeedData(t, l, p.frame, b) {
			t.Fatal("staging did not retain original plain PIL")
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n := 0
	err = s.SearchLive(context.Background(), Query{Branch: Write, From: now.Add(-time.Hour), To: now.Add(time.Hour)}, func(raw []byte) error {
		if !sameSeedData(t, l, raw, a) && !sameSeedData(t, l, raw, b) {
			t.Fatal("recovery changed payload")
		}
		n++
		return nil
	})
	if err != nil || n != 2 {
		t.Fatal(n, err)
	}
}

func TestHTTPGzipRejectsDamagedOrUnsupportedCoding(t *testing.T) {
	for _, kind := range []string{"truncated", "checksum", "trailing", "unsupported"} {
		t.Run(kind, func(t *testing.T) {
			l := testLayout(t, false)
			s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			h := &HTTP{Store: s}
			packed := gzipMember(t, testFrame(t, l, time.Now(), 1, false))
			coding := "gzip"
			switch kind {
			case "truncated":
				packed = packed[:len(packed)-4]
			case "checksum":
				packed[len(packed)-8] ^= 0xff
			case "trailing":
				packed = append(packed, 1, 2, 3)
			case "unsupported":
				coding = "br"
			}
			r := httptest.NewRequest("POST", "/ingest?tap="+url.QueryEscape(h.tap(Write)), bytes.NewReader(packed))
			r.Header.Set("Content-Type", "application/pil")
			r.Header.Set("Content-Encoding", coding)
			r.Header.Set("PIL-Version", "2")
			r.Header.Set("Definition-Version", fmtDigest(l))
			w := httptest.NewRecorder()
			h.serveHTTP(w, r)
			if w.Code < 400 || w.Header().Get("Trunk-Acknowledgement") != "" {
				t.Fatalf("acknowledged invalid coding: %d", w.Code)
			}
		})
	}
}
