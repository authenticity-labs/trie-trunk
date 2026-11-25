//go:build trunk_prototype

package cache

import (
	"bytes"
	"context"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"net/http/httptest"
	"testing"
	"time"
)

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
