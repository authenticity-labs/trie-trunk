//go:build trunk_prototype

package logging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
	"trie-trunk/go/codec"
	"trie-trunk/go/logger/pil"
	"trie-trunk/go/server/cache"
)

func TestAuthoredLogBuilderLeavesIdentityToIngress(t *testing.T) {
	raw, err := BuildLogDefinition(pil.Binding{Service: "test", Resource: "requests", Major: 1, Timezone: "UTC", Path: []string{"SeverityNumber"}})
	if err != nil {
		t.Fatal(err)
	}
	l, err := cache.ParseDefinition(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	p, err := compilePILEntry(l, nil)
	if err != nil {
		t.Fatal(err)
	}
	at := time.UnixMilli(1793718000123)
	values, err := p.values(&Entry{Time: at, Level: INFO, Duration: 42 * time.Microsecond, Service: codec.Text("test"), HttpMethod: codec.Text("GET"), Message: codec.Text("Done")})
	if err != nil {
		t.Fatal(err)
	}
	if values[l.Event].State != cache.Absent || values[l.Record].State != cache.Absent || values[l.Leaf].State != cache.Absent {
		t.Fatal("logger generated storage identity")
	}
	record, err := l.Encode(nil, values)
	if err != nil {
		t.Fatal(err)
	}
	view, err := l.DecodeJSON(record)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err = json.Unmarshal(view, &data); err != nil {
		t.Fatal(err)
	}
	if data["@time"] != float64(at.UnixMilli()) || data["duration"] != float64(42) || data["SeverityNumber"] != float64(9) || data["Body"] != "Done" {
		t.Fatalf("bad composites %s", view)
	}
	if _, ok := data["Timestamp"]; ok {
		t.Fatal("duplicate timestamp")
	}
	if dir := os.Getenv("TRUNK_LOG_FIXTURE"); dir != "" {
		if err = os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		for name, b := range map[string][]byte{"definition.rx": raw, "record.pil": record} {
			if err = os.WriteFile(filepath.Join(dir, name), b, 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func BenchmarkLogDefinitionEncoding(b *testing.B) {
	_, dict := builderMap(b)
	entry := Entry{Time: time.UnixMilli(1793718000123), Level: INFO, Duration: 42 * time.Microsecond, Service: codec.Text("payment"), HttpMethod: codec.Text("GET"), Message: codec.Text("Done")}
	entry.Id, _ = codec.NewUUIDv7(entry.Time)
	for _, strategy := range []string{"Flat", "Authored"} {
		b.Run(strategy, func(b *testing.B) {
			binding := pil.Binding{Service: "payment", Resource: "requests", Major: 1, Timezone: "UTC"}
			var raw []byte
			var err error
			if strategy == "Flat" {
				raw, err = BuildPILDefinition(binding, dict, "")
			} else {
				raw, err = BuildLogDefinition(binding)
			}
			if err != nil {
				b.Fatal(err)
			}
			l, err := cache.ParseDefinition(raw)
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			p, err := compilePILEntry(l, dict)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				v, e := p.values(&entry)
				if e != nil {
					b.Fatal(e)
				}
				if _, e = l.Encode(nil, v); e != nil {
					b.Fatal(e)
				}
			}
		})
	}
}

func TestLogSchemasKeepAdditionsInFields(t *testing.T) {
	for _, root := range []string{"log", "http-log", "database-log", "messaging-log"} {
		t.Run(root, func(t *testing.T) {
			source, err := os.ReadFile(filepath.Join("..", "..", "..", "schemas", root+".rx"))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := cache.BindAuthoredDefinition(source, pil.Binding{Format: pil.StorageFormat, Strategy: "log", Service: "test", Resource: "events", Major: 1, Timezone: "UTC", Roles: cache.Roles{Event: "@id", Duration: "duration"}})
			if err != nil {
				t.Fatal(err)
			}
			l, err := cache.ParseDefinition(raw)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			p, err := compilePILEntry(l, nil)
			if err != nil {
				t.Fatal(err)
			}
			e := Entry{Time: time.UnixMilli(1793718000123), Level: INFO, Duration: 42 * time.Microsecond, Service: codec.Text("payment"), Message: codec.Text("Done"), Fields: map[string]codec.Value{
				"invoice_number": codec.String("A-17"), "retry_count": codec.Uint(3),
				"context": codec.Object(map[string]codec.Value{"attempts": codec.List(codec.Uint(1), codec.Bool(true), codec.Value{})}),
			}}
			values, err := p.values(&e)
			if err != nil {
				t.Fatal(err)
			}
			if values[p.slots["@fields"]].State != cache.Offset {
				t.Fatal("additions missing from @fields")
			}
			if values[p.slots["Attributes"]].State != cache.Absent {
				t.Fatal("application additions moved into OTel Attributes")
			}
			encoded, err := l.Encode(nil, values)
			if err != nil {
				t.Fatal(err)
			}
			view, err := l.DecodeJSON(encoded)
			if err != nil {
				t.Fatal(err)
			}
			var object map[string]any
			if err = json.Unmarshal(view, &object); err != nil {
				t.Fatal(err)
			}
			if object["invoice_number"] != "A-17" || object["retry_count"] != float64(3) {
				t.Fatalf("additions did not round trip: %s", view)
			}
			context, ok := object["context"].(map[string]any)
			if !ok {
				t.Fatalf("nested addition missing: %s", view)
			}
			attempts, ok := context["attempts"].([]any)
			if !ok || len(attempts) != 3 || attempts[0] != float64(1) || attempts[1] != true || attempts[2] != nil {
				t.Fatalf("nested additions changed: %s", view)
			}
			if _, ok := object["@id"]; ok {
				t.Fatal("logger supplied identity")
			}
			e.Fields = map[string]codec.Value{"@time": codec.Uint(0)}
			if _, err = p.values(&e); err == nil {
				t.Fatal("additions shadowed a declared field")
			}
		})
	}
}
