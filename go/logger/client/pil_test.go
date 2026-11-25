//go:build trunk_prototype

package logging

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"
	"trie-trunk/go/codec"
	"trie-trunk/go/logger/pil"
	"trie-trunk/go/server/cache"
)

type pilRoundTrip func(*http.Request) (*http.Response, error)

func (f pilRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func builderMap(t testing.TB) ([]byte, *codec.Dictionary) {
	t.Helper()
	dictionary, err := codec.NewDictionary("payment/v1", map[string]map[uint16]string{
		"service": {17: "payment"}, "resource": {8: "transactions"}, "operation": {23: "authorize"}, "checkpoint": {41: "db"},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := BuildPILDefinition(pil.Binding{Service: "payment", Resource: "requests", Major: 1, Timezone: "UTC", Path: []string{"level", "service", "resource", "operation"}}, dictionary, "16")
	if err != nil {
		t.Fatal(err)
	}
	return raw, dictionary
}

func TestPILBuilderRejectsAmbiguousFields(t *testing.T) {
	raw, dict := builderMap(t)
	layout, _ := cache.ParseDefinition(raw)
	plan, err := compilePILEntry(layout, dict)
	if err != nil {
		t.Fatal(err)
	}
	l, _ := newLogger(Config{Dictionary: dict})
	_, b := l.Begin(context.Background())
	b.Service(99)
	e := b.Snapshot()
	if _, err := plan.values(&e); err == nil {
		t.Fatal("unknown reference accepted")
	}
	b.Service(17).Field("event_id", codec.String("overwrite"))
	e = b.Snapshot()
	if _, err := plan.values(&e); err == nil {
		t.Fatal("identity shadow accepted")
	}
	d := layout.Definition
	d.Fields = append(d.Fields, pil.Field{Name: "attempts", Type: "U16"})
	layout, err = cache.Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	plan, err = compilePILEntry(layout, dict)
	if err != nil {
		t.Fatal(err)
	}
	b.Remove("event_id").Field("attempts", codec.Uint(65536))
	e = b.Snapshot()
	if _, err := plan.values(&e); err == nil {
		t.Fatal("overflow accepted")
	}
	b.Field("attempts", codec.Uint(0))
	e = b.Snapshot()
	values, err := plan.values(&e)
	if err != nil {
		t.Fatal(err)
	}
	if values[len(values)-1].State != pil.Direct {
		t.Fatal("zero lost")
	}
}

// Serialization-only controls: same request entry and precompiled dictionaries.
// No HTTP, durable staging, or whole-object map conversion in either path.
var requestEncodingSink []byte

func BenchmarkRequestEncoding(b *testing.B) {
	raw, dict := builderMap(b)
	layout, err := cache.ParseDefinition(raw)
	if err != nil {
		b.Fatal(err)
	}
	plan, err := compilePILEntry(layout, dict)
	if err != nil {
		b.Fatal(err)
	}
	logger, _ := newLogger(Config{Dictionary: dict, Service: codec.Ref(17)})
	_, entry := logger.Begin(context.Background())
	entry.Resource(8).Operation(23).HttpMethod(GET).HttpStatus(200).Message(SuccessfulTransaction)
	event := entry.Snapshot()
	event.Duration = 250 * time.Microsecond
	b.Run("DictionaryPIL", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			raw, e := event.Encode(codec.PIL, dict)
			if e != nil {
				b.Fatal(e)
			}
			requestEncodingSink = raw
			b.SetBytes(int64(len(raw)))
		}
	})
	b.Run("PIL", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			values, e := plan.values(&event)
			if e != nil {
				b.Fatal(e)
			}
			raw, e := layout.Encode(make([]byte, 0, 128), values)
			if e != nil {
				b.Fatal(e)
			}
			requestEncodingSink = raw
			b.SetBytes(int64(len(raw)))
		}
	})
}

func TestPILAccessSurvivesEveryLevel(t *testing.T) {
	raw, dict := builderMap(t)
	layout, err := cache.ParseDefinition(raw)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := compilePILEntry(layout, dict)
	if err != nil {
		t.Fatal(err)
	}
	plan.access = "read"
	logger, _ := newLogger(Config{Dictionary: dict})
	for _, method := range []HttpMethod{GET, POST} {
		for _, level := range []Level{INFO, ERROR, WARN, DEBUG, TRACE} {
			_, b := logger.Begin(context.Background())
			b.HttpMethod(method).Level(level)
			e := b.Snapshot()
			values, err := plan.values(&e)
			if err != nil {
				t.Fatal(err)
			}
			access := "read"
			if method == POST {
				access = "write"
			}
			i := plan.core[slotAccess]
			want := plan.text(i, access)
			if values[i].State != want.State || !bytes.Equal(values[i].Bytes, want.Bytes) {
				t.Fatalf("%v/%v lost access", method, level)
			}
			li := plan.core[slotLevel]
			if !bytes.Equal(values[li].Bytes, plan.refs[li][uint16(level)].Bytes) {
				t.Fatal("access changed level")
			}
		}
	}
	// Non-HTTP operations retain the explicit routing access, including failures.
	plan.access = "write"
	_, b := logger.Begin(context.Background())
	b.Level(ERROR)
	e := b.Snapshot()
	if plan.accessFor(&e) != "write" {
		t.Fatal("failed write lost access")
	}
}
