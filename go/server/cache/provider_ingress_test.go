package cache

import (
	"bytes"
	"context"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func providerStore(t testing.TB) (*Store, Config) {
	t.Helper()
	dir, e := filepath.Abs("../../../schemas/logs")
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(dir, "log-http_trietrunk.rx"))
	if e != nil {
		t.Fatal(e)
	}
	raw, e = BindAuthoredDefinition(raw, Binding{Format: Format, Strategy: "log", Service: "test", Resource: "http", Major: 1, Timezone: "UTC", Roles: Roles{Event: "@id", Duration: "@duration"}, Path: []string{"level"}})
	if e != nil {
		t.Fatal(e)
	}
	l, e := ParseDefinition(raw)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(l.Close)
	cfg := Config{Root: t.TempDir(), Cache: "test", Tenant: "local", Layout: l, Now: func() time.Time { return time.UnixMilli(1793742600000) }}
	s, e := Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	path := s.dir
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	config := fmt.Sprintf("target = %q\n[provider]\nbundle = %q\ndefinition = 'log-http_trietrunk'\n", fmtDigestNative(l.Digest), dir)
	if e = os.WriteFile(filepath.Join(path, "transforms.cfg"), []byte(config), 0600); e != nil {
		t.Fatal(e)
	}
	s, e = Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	return s, cfg
}
func TestProviderIngressNativeBatchRecovery(t *testing.T) {
	s, cfg := providerStore(t)
	defer func() { s.Close() }()
	var digest string
	for d, p := range s.providers {
		if p.Source.Definition.Identity == "log-http_opentelemetry" {
			digest = d
		}
	}
	if digest == "" {
		t.Fatal("missing provider")
	}
	source := s.providers[digest].Source
	input := func(timestamp uint64) []byte {
		t.Helper()
		b, e := source.EncodeJSON([]byte(fmt.Sprintf(`{"Timestamp":%d,"SeverityNumber":9,"SeverityText":"INFO","Attributes":{"http.request.method":"GET","http.response.status_code":404}}`, timestamp)))
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	good, bad := input(1793740800000000000), input(1793740800000000001)
	h := &HTTP{Store: s}
	request := func(data []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/ingest?tap="+h.tap(Read), bytes.NewReader(data))
		r.Header.Set("Content-Type", "application/pil")
		r.Header.Set("PIL-Version", "2")
		r.Header.Set("PIL-Features", "adaptive-integers")
		r.Header.Set("Definition-Version", digest)
		w := httptest.NewRecorder()
		h.serveHTTP(w, r)
		return w
	}
	w := request(append(append([]byte{}, good...), bad...))
	if w.Code != 422 || w.Header().Get("Trunk-Received") != "1" {
		t.Fatalf("%d %v %s", w.Code, w.Header(), w.Body.String())
	}
	var rejected []Rejection
	if e := s.Rejections(func(r Rejection) error { rejected = append(rejected, r); return nil }); e != nil {
		t.Fatal(e)
	}
	if len(rejected) != 1 || rejected[0].DefinitionVersion != digest || !bytes.Equal(rejected[0].PIL, bad) {
		t.Fatal("provider rejection lost original identity", rejected)
	}
	check := func() {
		t.Helper()
		n := 0
		e := s.SearchLive(context.Background(), Query{Branch: Read, From: time.UnixMilli(1793740800000), To: time.UnixMilli(1793744400000)}, func(raw []byte) error {
			n++
			var f Frame
			if e := s.layout.Scan(raw, &f); e != nil {
				return e
			}
			if len(f.Values[s.layout.Event].Bytes) != 5 {
				return fmt.Errorf("entropy not generated")
			}
			v, e := pil.FieldValue(s.layout.Layout.Definition.Fields[fieldIndex(s.layout, "status")], f.Values[fieldIndex(s.layout, "status")])
			if e != nil || v != uint64(404) {
				return fmt.Errorf("status %v: %v", v, e)
			}
			return nil
		})
		if e != nil || n != 1 {
			t.Fatal(n, e)
		}
	}
	check()
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	var e error
	s, e = Open(cfg)
	if e != nil {
		t.Fatal(e)
	}
	check()
}

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
