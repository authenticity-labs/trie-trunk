//go:build trunk_prototype

package cache

import (
	"bytes"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"net/http/httptest"
	"os"
	"testing"
)

func TestProviderEndpointExternalFormats(t *testing.T) {
	s, cfg := providerStore(t)
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	endpoint, e := OpenEndpoint(cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer endpoint.Close()
	sourceRaw, e := os.ReadFile("../../../schemas/logs/log-http_datadog.rx")
	if e != nil {
		t.Fatal(e)
	}
	source, e := pil.Define(sourceRaw)
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	doc := []byte(`{"timestamp":1793740800000,"http.method":"GET","http.status_code":"404"}`)
	cbor, e := ingressEncoder.Marshal(map[string]any{"timestamp": uint64(1793740800000), "http.method": "GET", "http.status_code": "404"})
	if e != nil {
		t.Fatal(e)
	}
	encoded, e := source.EncodeJSON(doc)
	if e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		kind  string
		raw   []byte
		count string
	}{{"application/json", doc, "1"}, {"application/x-ndjson", append(append(append([]byte{}, doc...), '\n'), doc...), "2"}, {"application/cbor", cbor, "1"}, {"application/pil", encoded, "1"}} {
		t.Run(tc.kind, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/ingest?tap=test/test/http.v1/read", bytes.NewReader(tc.raw))
			r.Header.Set("Content-Type", tc.kind)
			r.Header.Set("Definition-Version", fmtDigestNative(source.Digest))
			r.Header.Set("PIL-Version", "2")
			r.Header.Set("PIL-Features", "adaptive-integers")
			w := httptest.NewRecorder()
			endpoint.serveHTTP(w, r)
			if w.Code != 200 || w.Header().Get("Trunk-Received") != tc.count {
				t.Fatal(w.Code, w.Header(), w.Body.String())
			}
		})
	}
	// A provider RX never installs itself as a new generic resource, and cannot
	// select a destination implicitly when its digest is not the storage RX.
	r := httptest.NewRequest("POST", "/ingest", bytes.NewReader(doc))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Definition-Version", fmtDigestNative(source.Digest))
	w := httptest.NewRecorder()
	endpoint.serveHTTP(w, r)
	if w.Code < 400 {
		t.Fatal("accepted ambiguous destination")
	}
	if len(endpoint.apis) != 1 {
		t.Fatal("provider created extra resource", len(endpoint.apis))
	}
}
