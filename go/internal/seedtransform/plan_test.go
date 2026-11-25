//go:build trunk_prototype

package seedtransform

import (
	"bytes"
	"encoding/json"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	meta "trie-trunk/go/internal/definitionmeta"
)

func fixture(t testing.TB, provider string) *Plan {
	return fixtureShape(t, "log-http", provider)
}
func fixtureShape(t testing.TB, shape, provider string) *Plan {
	t.Helper()
	dir := "../../../schemas/logs"
	source, e := meta.LoadSource(dir)
	if e != nil {
		t.Fatal(e)
	}
	layouts := map[string]*pil.Layout{}
	read := func(name string) *pil.Layout {
		raw, e := os.ReadFile(filepath.Join(dir, name+".rx"))
		if e != nil {
			t.Fatal(e)
		}
		l, e := pil.Define(raw)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(l.Close)
		return l
	}
	for _, d := range source.Definitions {
		layouts[d.Name] = read(d.Name)
	}
	metadata := map[string]*meta.Layout{}
	for name, layout := range layouts {
		m, err := meta.Open(layout.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Close)
		metadata[name] = m
	}
	readMeta := func(name string) *meta.Layout {
		m, err := meta.Open(read(name).Bytes)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Close)
		return m
	}
	ms, ss := readMeta("field-meta"), readMeta("synonym")
	md, sy, reg, e := meta.Build(source, metadata, ms, ss)
	if e != nil {
		t.Fatal(e)
	}
	target := layouts[shape+"_trietrunk"]
	targetMeta := metadata[shape+"_trietrunk"]
	entry := reg[fmt.Sprintf("%x", target.Digest)]
	mp, e := meta.ReadPack(bytes.NewReader(md), entry.Metadata, ms, targetMeta)
	if e != nil {
		t.Fatal(e)
	}
	prepared, e := meta.Prepare(mp, ms, targetMeta)
	if e != nil {
		t.Fatal(e)
	}
	sp, e := meta.ReadPack(bytes.NewReader(sy), entry.Synonyms[shape+"_"+provider], ss, targetMeta)
	if e != nil {
		t.Fatal(e)
	}
	mapping, e := meta.PrepareSynonyms(sp, ss, targetMeta)
	if e != nil {
		t.Fatal(e)
	}
	p, e := Compile(layouts[shape+"_"+provider], target, mapping, prepared)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.Close)
	return p
}
func TestProviderBorrowCastAndExtensions(t *testing.T) {
	p := fixture(t, "opentelemetry")
	raw, e := p.Source.EncodeJSON([]byte(`{"Timestamp":1793740800000000000,"SeverityNumber":9,"SeverityText":"INFO","Body":"hello","Attributes":{"http.request.method":"_OTHER","http.request.method_original":"CUSTOM","http.response.status_code":404,"business":"kept"},"time":"user-time","fields":{"mine":true}}`))
	if e != nil {
		t.Fatal(e)
	}
	values, e := p.Apply(raw, nil)
	if e != nil {
		t.Fatal(e)
	}
	encoded, e := p.Target.Encode(nil, values)
	if e != nil {
		t.Fatal(e)
	}
	view, e := p.Target.DecodeJSON(encoded)
	if e != nil {
		t.Fatal(e)
	}
	t.Log(string(view))
	for _, pair := range []struct {
		name string
		want string
	}{{"method", "CUSTOM"}, {"message", "hello"}} {
		v, e := pil.FieldValue(p.Target.Definition.Fields[indexOf(p.Target, pair.name)], values[indexOf(p.Target, pair.name)])
		if e != nil || v != pair.want {
			t.Fatal(v, e)
		}
	}
	timeValue, _ := pil.FieldValue(p.Target.Definition.Fields[indexOf(p.Target, "@time")], values[indexOf(p.Target, "@time")])
	if timeValue != uint64(1793740800000) {
		t.Fatal(timeValue)
	}
	if bytes.Contains(view, []byte("SeverityText")) {
		t.Fatal("duplicated derived severity", string(view))
	}
	if !bytes.Contains(view, []byte("business")) || !bytes.Contains(view, []byte("_OTHER")) {
		t.Fatal("lost unknown or overridden value", string(view))
	}
	// Compatible scalar bytes remain a slice of the original ingress allocation.
	method := values[indexOf(p.Target, "method")].Bytes
	offset := bytes.Index(raw, []byte("CUSTOM"))
	if offset < 0 || &method[0] != &raw[offset] {
		t.Fatal("copied compatible field")
	}
	// A sparse next record must not inherit nested fields from prior scratch.
	next, e := p.Source.EncodeJSON([]byte(`{"Timestamp":1793740800000000000}`))
	if e != nil {
		t.Fatal(e)
	}
	values, e = p.Apply(next, values)
	if e != nil {
		t.Fatal(e)
	}
	if values[indexOf(p.Target, "method")].State != pil.Absent {
		t.Fatal("stale child")
	}
}
func TestExactCastRejectsTruncation(t *testing.T) {
	p := fixture(t, "opentelemetry")
	raw, e := p.Source.EncodeJSON([]byte(`{"Timestamp":1793740800000000001}`))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.Apply(raw, nil); e == nil {
		t.Fatal("rounded timestamp without explicit cast")
	}
}
func TestDatadogProvider(t *testing.T) {
	p := fixture(t, "datadog")
	raw, e := p.Source.EncodeJSON([]byte(`{"timestamp":1793740800000,"http.method":"GET","http.status_code":"404","duration":10000000}`))
	if e != nil {
		t.Fatal(e)
	}
	values, e := p.Apply(raw, nil)
	if e != nil {
		t.Fatal(e)
	}
	out, e := p.Target.Encode(nil, values)
	if e != nil {
		t.Fatal(e)
	}
	v, e := p.Target.DecodeJSON(out)
	if e != nil {
		t.Fatal(e)
	}
	t.Log(string(v))
}

var benchmarkValues []pil.Value

func BenchmarkProviderTransform(b *testing.B) {
	for _, size := range []int{64, 4096, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			p := fixture(b, "opentelemetry")
			input := fmt.Sprintf(`{"Timestamp":1793740800000000000,"SeverityNumber":9,"Attributes":{"http.request.method":"GET","http.response.status_code":404},"Body":%q}`, string(bytes.Repeat([]byte("x"), size)))
			raw, e := p.Source.EncodeJSON([]byte(input))
			if e != nil {
				b.Fatal(e)
			}
			values, e := p.Apply(raw, nil)
			if e != nil {
				b.Fatal(e)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				values, e = p.Apply(raw, values)
				if e != nil {
					b.Fatal(e)
				}
			}
			benchmarkValues = values
		})
	}
}

func TestAllProviderProfiles(t *testing.T) {
	cases := []struct{ shape, required, otel string }{
		{"log", "", ""}, {"log-http", "method", "http.request.method"},
		{"log-db", "system", "db.system.name"}, {"log-messaging", "system", "messaging.system"},
		{"log-security", "action", "action"}, {"log-job", "job", "job"},
		{"log-task", "task", "task"}, {"log-workflow", "workflow", "workflow"},
		{"log-system", "action", "action"},
	}
	for _, c := range cases {
		for _, provider := range []string{"opentelemetry", "datadog"} {
			t.Run(c.shape+"_"+provider, func(t *testing.T) {
				p := fixtureShape(t, c.shape, provider)
				event := map[string]any{"timestamp": uint64(1793740800000), "message": "hello"}
				if c.required != "" {
					event[c.required] = "sample"
				}
				if c.shape == "log-http" {
					delete(event, "method")
					event["http.method"] = "GET"
				}
				if c.shape == "log-db" {
					delete(event, "system")
					event["db.system"] = "postgresql"
				}
				if provider == "opentelemetry" {
					attrs := map[string]any{}
					if c.otel != "" {
						attrs[c.otel] = "sample"
					}
					event = map[string]any{"Timestamp": uint64(1793740800000000000), "Body": "hello", "Attributes": attrs}
				}
				object, e := pil.MarshalObject(event)
				if e != nil {
					t.Fatal(e)
				}
				raw, e := p.Source.EncodeObject(object)
				if e != nil {
					t.Fatal(e)
				}
				values, e := p.Apply(raw, nil)
				if e != nil {
					t.Fatal(e)
				}
				out, e := p.Target.Encode(nil, values)
				if e != nil {
					t.Fatal(e)
				}
				view, e := p.Target.DecodeJSON(out)
				if e != nil {
					t.Fatal(e)
				}
				var decoded map[string]any
				if e = json.Unmarshal(view, &decoded); e != nil {
					t.Fatal(e)
				}
				if decoded["@time"] != float64(1793740800000) || decoded["message"] != "hello" {
					t.Fatalf("lost base values: %s", view)
				}
				if c.required != "" && decoded[c.required] == nil {
					t.Fatalf("missing required field: %s", view)
				}
			})
		}
	}
}
func TestWorkflowChoiceBorrowing(t *testing.T) {
	for _, provider := range []string{"opentelemetry", "datadog"} {
		t.Run(provider, func(t *testing.T) {
			p := fixtureShape(t, "log-workflow", provider)
			for _, value := range []any{"object-key", uint64(1<<63) + 17, []byte{0, 1, 255}} {
				targetObject := map[string]any{"@time": uint64(1793740800000), "workflow": "sample", "object": value}
				sourceObject := map[string]any{"timestamp": uint64(1793740800000), "workflow": "sample", "object": value}
				if provider == "opentelemetry" {
					sourceObject = map[string]any{"Timestamp": uint64(1793740800000000000), "Attributes": map[string]any{"workflow": "sample", "object": value}}
				}
				marshal := func(v any) []byte {
					raw, e := pil.MarshalObject(v)
					if e != nil {
						t.Fatal(e)
					}
					return raw
				}
				raw, e := p.Source.EncodeObject(marshal(sourceObject))
				if e != nil {
					t.Fatal(e)
				}
				expected, e := p.Target.EncodeObject(marshal(targetObject))
				if e != nil {
					t.Fatal(e)
				}
				values, e := p.Apply(raw, nil)
				if e != nil {
					t.Fatal(e)
				}
				out, e := p.Target.Encode(nil, values)
				if e != nil {
					t.Fatal(e)
				}
				if !bytes.Equal(out, expected) {
					t.Fatalf("choice type %T changed", value)
				}
				// No choice expansion/copy when the two schemas describe identical bytes.
				for _, b := range p.bindings {
					if int(b.mapping.Ordinal) == indexOf(p.Target, "object") && !b.borrow {
						t.Fatal("identical choice was expanded")
					}
				}
				got, e := p.Target.DecodeJSON(out)
				if e != nil {
					t.Fatal(e)
				}
				want, e := p.Target.DecodeJSON(expected)
				if e != nil {
					t.Fatal(e)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("choice render differs")
				}
			}
		})
	}
}
