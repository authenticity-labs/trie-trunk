//go:build trunk_prototype

package cache

import (
	"bytes"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestStagingCapacityHTTPAcknowledgesDurablePrefix(t *testing.T) {
	l := testLayout(t, false)
	now := time.Now()
	p, e := OpenEndpoint(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	p.api.Store.policy.Staging = &StagingLimits{MaxRecords: 1}
	first := testFrame(t, l, now, 1, false)
	second := testFrame(t, l, now, 2, false)
	r := httptest.NewRequest("POST", "/ingest?tap="+url.QueryEscape(p.api.tap(Write)), bytes.NewReader(append(first, second...)))
	r.Header.Set("Content-Type", "application/pil")
	definitionHeaders(r.Header, l)
	w := httptest.NewRecorder()
	p.serveHTTP(w, r)
	if w.Code != 503 || w.Header().Get("Trunk-Received") != "1" || w.Header().Get("Retry-After") == "" {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	if p.api.Store.failed != nil || p.api.Store.branches[Write].live.count(0) != 1 {
		t.Fatal("prefix not retained")
	}
}
