package logging

import (
	"bytes"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"trie-trunk/go/server/cache"
)

func TestLogStylesPublishCanonicalPIL(t *testing.T) {
	rx, err := os.ReadFile("../../../schemas/logs/log.http.rx")
	if err != nil {
		t.Fatal(err)
	}
	trunk, err := HTTP()
	if err != nil {
		t.Fatal(err)
	}
	defer trunk.Close()
	otel, err := HTTP.OpenTelemetry()
	if err != nil {
		t.Fatal(err)
	}
	defer otel.Close()
	options := map[string]LogOption{}
	for _, o := range otel.plan.Options() {
		options[o.Name] = o
	}
	if options["http.response.status_code"].Field != "status" || options["Timestamp"].Unit != "ns" || !otel.plan.CustomFields() {
		t.Fatal(options)
	}
	const ms = 1794830400000
	canonical := trunk.Event().Time(ms).Method("GET").Status(503).Level(17).
		TraceID([16]byte{1}).Field("order", map[string]any{"id": "A17", "note": nil}).
		Field("bytes", []byte{0, 255}).SpanID([8]byte{2})
	external := otel.Event().Timestamp(ms*1000000).HTTPRequestMethod("GET").
		HTTPResponseStatusCode(503).SeverityNumber(17).TraceID([16]byte{1}).
		Field("order", map[string]any{"id": "A17", "note": nil}).
		Field("bytes", []byte{0, 255}).SpanID([8]byte{2})
	external.seed.id = canonical.seed.id
	first, err := canonical.Encode()
	if err != nil {
		t.Fatal(err)
	}
	second, err := external.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("styles encoded different PILs")
	}
	endpoint, err := cache.OpenEndpoint(cache.Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Storage: "browse", Now: func() time.Time { return time.UnixMilli(ms + 1) }})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	handler := endpoint.Handler("")
	register := httptest.NewRequest("PUT", "/definitions", bytes.NewReader(rx))
	register.Header.Set("Content-Type", "application/pil")
	register.Header.Set("Resource-Service", "shop")
	register.Header.Set("Resource-Name", "orders")
	register.Header.Set("Resource-Type", "http")
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, register)
	if out.Code != 204 {
		t.Fatalf("register: %d %s", out.Code, out.Body.String())
	}
	req, err := external.Request(t.Context(), "/demo/shop/orders/http/seeds")
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Trunk-Branch", out.Header().Get("Trunk-Branch"))
	out = httptest.NewRecorder()
	handler.ServeHTTP(out, req)
	if out.Code != 204 {
		t.Fatalf("ingest: %d %s", out.Code, out.Body.String())
	}
	read := httptest.NewRequest("GET", "/demo/shop/orders/http/packs?beginTime=1794830400000&endTime=1794830400002", nil)
	read.Header.Set("Trunk-Branch", req.Header.Get("Trunk-Branch"))
	out = httptest.NewRecorder()
	handler.ServeHTTP(out, read)
	if out.Code != 200 || out.Body.Len() == 0 {
		t.Fatalf("read: %d %s", out.Code, out.Body.String())
	}
}

func TestLogOptionsFollowSelectedRX(t *testing.T) {
	rx, err := os.ReadFile("../../../testdata/seed-identity/query.rx")
	if err != nil {
		t.Fatal(err)
	}
	l, err := OpenLog(rx, TrieTrunk)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	options := map[string]bool{}
	for _, o := range l.Options() {
		options[o.Name] = true
	}
	if !options["checkpoints"] || options["method"] || !l.CustomFields() {
		t.Fatal(options)
	}
	preset, err := Log("log.query", OpenTelemetry)
	if err != nil {
		t.Fatal(err)
	}
	defer preset.Close()
	fileEvent := l.Event().Set("time", 123).Field("job.id", "J9")
	presetEvent := preset.Event().Set("Timestamp", 123000000).Field("job.id", "J9")
	presetEvent.id = fileEvent.id
	want, err := fileEvent.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := presetEvent.Encode()
	if err != nil || !bytes.Equal(want, got) {
		t.Fatalf("query preset differs from registered RX: %v", err)
	}
	if _, err := l.Event().Set("method", "GET").Encode(); err == nil {
		t.Fatal("accepted an HTTP option on query")
	}
	if _, err := l.Event().Field("duration", true).Encode(); err == nil {
		t.Fatal("allowed a custom field to shadow duration")
	}
	if _, err := l.Event().Field("job.id", nil).Encode(); err != nil {
		t.Fatal(err)
	}
	l.Close()
	if _, err := l.Event().Encode(); err == nil {
		t.Fatal("encoded after Close")
	}
}

func TestTypedQuerySettersMatchRegisteredPIL(t *testing.T) {
	trunk, err := Query()
	if err != nil {
		t.Fatal(err)
	}
	defer trunk.Close()
	otel, err := Query.OpenTelemetry()
	if err != nil {
		t.Fatal(err)
	}
	defer otel.Close()
	name, zero := "collect", 0.0
	checkpoint := QueryCheckpoint{Name: &name, Duration: &zero}
	a := trunk.Event().Time(123).Operation("execute").Rows(0).Status("complete").Checkpoints(checkpoint)
	b := otel.Event().Timestamp(123000000).Operation("execute").Rows(0).Status("complete").Checkpoints(checkpoint)
	expected := trunk.plan.Event().Set("time", 123).Set("operation", "execute").Set("rows", 0).
		Set("status", "complete").Set("checkpoints", []any{map[string]any{"name": "collect", "duration": zero}})
	b.seed.id, expected.id = a.seed.id, a.seed.id
	want, err := expected.Encode()
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []interface{ Encode() ([]byte, error) }{a, b} {
		got, err := event.Encode()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("checkpoint PIL differs from RX encoder: %v", err)
		}
	}
	_, err = a.Checkpoints().Encode()
	if err != nil {
		t.Fatal(err)
	}
}
