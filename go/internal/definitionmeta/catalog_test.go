package definitionmeta

import (
	"bytes"
	"encoding/json"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"os"
	"path/filepath"
	"testing"
)

const fixtureRoot = "../../../schemas/logs"

func fixture(t testing.TB) (Source, map[string]*pil.Layout, *pil.Layout, *pil.Layout) {
	t.Helper()
	source, e := LoadSource(fixtureRoot)
	if e != nil {
		t.Fatal(e)
	}
	read := func(name string) *pil.Layout {
		raw, e := os.ReadFile(filepath.Join(fixtureRoot, name+".rx"))
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
	definitions := map[string]*pil.Layout{}
	for _, d := range source.Definitions {
		definitions[d.Name] = read(d.Name)
	}
	return source, definitions, read("field-meta"), read("synonym")
}
func ordinal(rx *pil.Layout, name string) int {
	for i, f := range rx.Definition.Fields {
		if f.Name == name {
			return i
		}
	}
	panic(name)
}
func TestGeneratedCatalogRoundTrip(t *testing.T) {
	source, definitions, schema, synonym := fixture(t)
	metadata, synonyms, registry, e := Build(source, definitions, schema, synonym)
	if e != nil {
		t.Fatal(e)
	}
	for name, expected := range map[string][]byte{"definition.meta": metadata, "synonym.map": synonyms} {
		got, e := os.ReadFile(filepath.Join(fixtureRoot, name))
		if e != nil || !bytes.Equal(got, expected) {
			t.Fatalf("non-reproducible %s: %v", name, e)
		}
	}
	var end uint64
	for _, d := range source.Definitions {
		rx := definitions[d.Name]
		entry := registry[fmt.Sprintf("%x", rx.Digest)]
		if entry.Metadata.Offset != end {
			t.Fatal("metadata gap")
		}
		end += uint64(entry.Metadata.Length)
		pack, e := ReadPack(bytes.NewReader(metadata), entry.Metadata, schema, rx)
		if e != nil {
			t.Fatal(e)
		}
		prepared, e := Prepare(pack, schema, rx)
		if e != nil {
			t.Fatal(e)
		}
		for i, f := range rx.Definition.Fields {
			if prepared.Fields[i].Semantic == "" {
				t.Fatal("missing ordinal", f.Name)
			}
		}
		for vocabulary, location := range entry.Synonyms {
			p, e := ReadPack(bytes.NewReader(synonyms), location, synonym, rx)
			if e != nil {
				t.Fatal(e)
			}
			if string(p.Headers["source_name"]) != vocabulary {
				t.Fatal("vocabulary range mismatch")
			}
			if _, e = PrepareSynonyms(p, synonym, rx); e != nil {
				t.Fatal(e)
			}
		}
	}
	if end != uint64(len(metadata)) {
		t.Fatal("unaddressed bytes")
	}
}
func TestMetadataSemanticsAndTypedMappings(t *testing.T) {
	source, definitions, schema, synonym := fixture(t)
	metadata, synonyms, registry, e := Build(source, definitions, schema, synonym)
	if e != nil {
		t.Fatal(e)
	}
	http := definitions["log-http_trietrunk"]
	entry := registry[fmt.Sprintf("%x", http.Digest)]
	p, e := ReadPack(bytes.NewReader(metadata), entry.Metadata, schema, http)
	if e != nil {
		t.Fatal(e)
	}
	prepared, e := Prepare(p, schema, http)
	if e != nil {
		t.Fatal(e)
	}
	status := ordinal(http, "status")
	v, ok := prepared.Lookup(status, "404", nil)
	if !ok || v.Label != "Not Found" {
		t.Fatal(v)
	}
	if _, ok = prepared.Lookup(status, "599", nil); ok {
		t.Fatal("unknown value falsely labeled")
	}
	if _, e = ReadPack(bytes.NewReader(metadata), entry.Metadata, schema, definitions["log-db_trietrunk"]); e == nil {
		t.Fatal("accepted wrong Definition")
	}
	p, e = ReadPack(bytes.NewReader(synonyms), entry.Synonyms["log-http_datadog"], synonym, http)
	if e != nil {
		t.Fatal(e)
	}
	mapping, e := PrepareSynonyms(p, synonym, http)
	if e != nil {
		t.Fatal(e)
	}
	s := findMapping(mapping, "http.status_code")
	if s.Ordinal != uint32(status) || s.Cast != "decimal_to_integer" {
		t.Fatal(s)
	}
	p, e = ReadPack(bytes.NewReader(synonyms), entry.Synonyms["log-http_opentelemetry"], synonym, http)
	if e != nil {
		t.Fatal(e)
	}
	mapping, e = PrepareSynonyms(p, synonym, http)
	if e != nil {
		t.Fatal(e)
	}
	s = findMapping(mapping, "Timestamp")
	if s.Cast != "scale_exact" || s.Numerator != 1 || s.Denominator != 1000000 {
		t.Fatal(s)
	}
	if findMapping(mapping, "Attributes", "http.request.method_original").Priority <= findMapping(mapping, "Attributes", "http.request.method").Priority {
		t.Fatal("original method precedence lost")
	}
	db := definitions["log-db_trietrunk"]
	entry = registry[fmt.Sprintf("%x", db.Digest)]
	p, e = ReadPack(bytes.NewReader(metadata), entry.Metadata, schema, db)
	if e != nil {
		t.Fatal(e)
	}
	prepared, e = Prepare(p, schema, db)
	if e != nil {
		t.Fatal(e)
	}
	selector := func(i uint32) string {
		if i != uint32(ordinal(db, "system")) {
			t.Fatal("wrong selector")
		}
		return "postgresql"
	}
	if v, ok = prepared.Lookup(ordinal(db, "status"), "23505", selector); !ok || v.Label != "Unique Violation" {
		t.Fatal(v)
	}
	if _, ok = prepared.Lookup(ordinal(db, "status"), "23505", func(uint32) string { return "mysql" }); ok {
		t.Fatal("cross-engine label")
	}
}
func TestUserNamesAndSparseRecords(t *testing.T) {
	_, definitions, _, _ := fixture(t)
	rx := definitions["log-http_trietrunk"]
	for _, input := range []string{`{"@time":1793740800000,"method":"GET","id":"business-id","time":"business-time","fields":{"custom":true}}`, `{"@time":1793740800000,"@id":null,"method":"CUSTOM","status":599}`, `{"@time":1793740800000,"method":"POST","status":404,"@duration":0}`, `{"@time":1,"method":"GET","@id":[1,2,3]}`} {
		record, e := rx.EncodeJSON([]byte(input))
		if e != nil {
			t.Fatal(e)
		}
		view, e := rx.DecodeJSON(record)
		if e != nil {
			t.Fatal(e)
		}
		var a, b any
		_ = json.Unmarshal([]byte(input), &a)
		_ = json.Unmarshal(view, &b)
		av, _ := json.Marshal(a)
		bv, _ := json.Marshal(b)
		if !bytes.Equal(av, bv) {
			t.Fatalf("%s != %s", av, bv)
		}
	}
	for _, input := range []string{`{"method":"GET"}`, `{"@time":1,"method":"GET","status":700}`, `{"@time":1,"method":"GET","@id":[]}`, `{"@time":1,"method":"GET","@id":[1,2,3,4,5,6,7,8,9,10,11]}`} {
		if _, e := rx.EncodeJSON([]byte(input)); e == nil {
			t.Fatal("accepted invalid record", input)
		}
	}
}
func TestInvalidRanges(t *testing.T) {
	_, definitions, schema, _ := fixture(t)
	rx := definitions["log-http_trietrunk"]
	for _, loc := range []Location{{0, 0}, {^uint64(0), 100}, {0, 100}} {
		if _, e := ReadPack(bytes.NewReader(nil), loc, schema, rx); e == nil {
			t.Fatal("accepted invalid location", loc)
		}
	}
}

func TestMetadataBackedValuesAvoidDuplicateStorage(t *testing.T) {
	source, definitions, schema, synonym := fixture(t)
	metadata, _, registry, e := Build(source, definitions, schema, synonym)
	if e != nil {
		t.Fatal(e)
	}
	prepare := func(name string) *Prepared {
		rx := definitions[name]
		p, err := ReadPack(bytes.NewReader(metadata), registry[fmt.Sprintf("%x", rx.Digest)].Metadata, schema, rx)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := Prepare(p, schema, rx)
		if err != nil {
			t.Fatal(err)
		}
		return prepared
	}
	http := prepare("log-http_trietrunk")
	status := ordinal(definitions["log-http_trietrunk"], "status")
	if code, ok := http.ResolveLabel(status, "Not Found", nil); !ok || code != "404" {
		t.Fatal(code, ok)
	}
	if !http.IsDerivedLabel(status, "404", "Not Found", nil) {
		t.Fatal("redundant label not recognized")
	}
	for _, label := range []string{"Custom Missing", "OK", "not found"} {
		if http.IsDerivedLabel(status, "404", label, nil) {
			t.Fatal("would discard non-redundant label", label)
		}
	}
	if _, ok := http.ResolveLabel(status, "Custom Missing", nil); ok {
		t.Fatal("invented code")
	}
	if http.IsDerivedLabel(status, "599", "Custom", nil) {
		t.Fatal("invented metadata")
	}
	ambiguous := &Prepared{Fields: []PreparedField{{labels: labelMap([]Value{{Value: "9", Label: "INFO"}, {Value: "10", Label: "INFO"}})}}}
	if _, ok := ambiguous.ResolveLabel(0, "INFO", nil); ok {
		t.Fatal("ambiguous severity guessed")
	}
	db := prepare("log-db_trietrunk")
	i := ordinal(definitions["log-db_trietrunk"], "status")
	if code, ok := db.ResolveLabel(i, "Unique Violation", func(uint32) string { return "postgresql" }); !ok || code != "23505" {
		t.Fatal(code, ok)
	}
	if _, ok := db.ResolveLabel(i, "Unique Violation", func(uint32) string { return "mysql" }); ok {
		t.Fatal("wrong engine")
	}
	// No HTTP-specific implementation: empty/zero codes remain valid, and
	// ambiguity is independent of input order or duplicate descriptions.
	labels := labelMap([]Value{{Value: "", Label: "Empty"}, {Value: "0", Label: "Zero"}, {Value: "1", Label: "Shared"}, {Value: "2", Label: "Shared"}, {Value: "1", Label: "Shared"}})
	if !labels["Empty"].unique || !labels["Zero"].unique || labels["Shared"].unique {
		t.Fatal(labels)
	}
}

var lookupValue Value

func BenchmarkPreparedFieldLookup(b *testing.B) {
	source, definitions, schema, synonym := fixture(b)
	metadata, _, registry, e := Build(source, definitions, schema, synonym)
	if e != nil {
		b.Fatal(e)
	}
	rx := definitions["log-http_trietrunk"]
	p, e := ReadPack(bytes.NewReader(metadata), registry[fmt.Sprintf("%x", rx.Digest)].Metadata, schema, rx)
	if e != nil {
		b.Fatal(e)
	}
	prepared, e := Prepare(p, schema, rx)
	if e != nil {
		b.Fatal(e)
	}
	i := ordinal(rx, "status")
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		lookupValue, _ = prepared.Lookup(i, "404", nil)
	}
}

func findMapping(p *PreparedSynonyms, path ...string) Synonym {
	for _, s := range p.Fields {
		if fmt.Sprint(s.From) == fmt.Sprint(path) {
			return s
		}
	}
	panic(path)
}
