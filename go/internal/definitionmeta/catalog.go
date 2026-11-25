// Package definitionmeta builds and reads application metadata using PIL/PAC.
// It does not add application semantics to pill-packer or alter event storage.
package definitionmeta

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Location addresses one complete PAC in a homogeneous datastore.
type Location struct {
	Offset uint64
	Length uint32
}

// Resources is attached to the existing registry, keyed by exact RX digest.
// Field ordinals must never be reused across different Definitions.
type Resources struct {
	Name     string
	Metadata Location
	Synonyms map[string]Location
}
type Value struct {
	Value       string `json:"value"`
	Label       string `json:"label"`
	Description string `json:"description"`
}
type Domain struct {
	Ordinal uint32  `json:"ordinal"`
	Value   string  `json:"value"`
	Values  []Value `json:"values"`
}
type Field struct {
	Semantic    string   `json:"semantic"`
	Label       string   `json:"label"`
	Description string   `json:"description"`
	Unit        string   `json:"unit,omitempty"`
	Format      string   `json:"format,omitempty"`
	Role        string   `json:"role,omitempty"`
	Source      string   `json:"source,omitempty"`
	Values      []Value  `json:"values,omitempty"`
	Domains     []Domain `json:"domains,omitempty"`
}

// Synonym carries bindings and cast parameters only. Both RX identities live
// in the containing PAC headers; datatypes/constraints stay in those RX files.
type Synonym struct {
	From        []string `json:"from"`
	Ordinal     uint32   `json:"ordinal"`
	Cast        string   `json:"cast"`
	Priority    uint32   `json:"priority"`
	Numerator   uint64   `json:"numerator,omitempty"`
	Denominator uint64   `json:"denominator,omitempty"`
}
type Mapping struct {
	From        []string `json:"from"`
	To          string   `json:"to"`
	Cast        string   `json:"cast"`
	Priority    uint32   `json:"priority,omitempty"`
	Numerator   uint64   `json:"numerator,omitempty"`
	Denominator uint64   `json:"denominator,omitempty"`
}
type MappingSet struct {
	Source string    `json:"source"`
	Target string    `json:"target"`
	Fields []Mapping `json:"fields"`
}
type AuthoredField struct {
	Name string `json:"name"`
	Field
	Domains []struct {
		Field  string  `json:"field"`
		Value  string  `json:"value"`
		Values []Value `json:"values"`
	} `json:"domains"`
}
type Definition struct {
	Name        string `json:"name"`
	File        string `json:"file"`
	Provider    string `json:"provider"`
	Target      string `json:"target,omitempty"`
	Source      string `json:"source,omitempty"`
	Description string `json:"description"`
}
type Presentation struct {
	Base        []AuthoredField            `json:"base"`
	Definitions map[string][]AuthoredField `json:"definitions"`
}
type Source struct {
	Sources      map[string]string `json:"sources"`
	Definitions  []Definition      `json:"definitions"`
	Notes        []string          `json:"notes"`
	Presentation Presentation      `json:"-"`
	Mappings     []MappingSet      `json:"-"`
}

func LoadSource(dir string) (Source, error) {
	var s Source
	for name, target := range map[string]any{"catalog.json": &s, "presentation.json": &s.Presentation, "synonyms.json": &s.Mappings} {
		raw, e := os.ReadFile(filepath.Join(dir, name))
		if e != nil {
			return s, e
		}
		if e = json.Unmarshal(raw, target); e != nil {
			return s, e
		}
	}
	return s, nil
}

func encodePack(layout *Layout, rows []any, headers map[string][]byte) ([]byte, error) {
	records := make([][]byte, len(rows))
	for i, row := range rows {
		raw, e := json.Marshal(row)
		if e != nil {
			return nil, e
		}
		records[i], e = layout.EncodeJSON(raw)
		if e != nil {
			return nil, fmt.Errorf("record %d: %w", i, e)
		}
	}
	return layout.Pack(records, headers)
}

// Build appends one metadata PAC per RX and one mapping PAC per source/destination RX pair.
// It does not wrap these datastores in a second directory or transport envelope.
func Build(source Source, definitions map[string]*Layout, meta, synonym *Layout) ([]byte, []byte, map[string]Resources, error) {
	var metadata, synonyms []byte
	registry := make(map[string]Resources, len(source.Definitions))
	for _, def := range source.Definitions {
		rx := definitions[def.Name]
		if rx == nil {
			return nil, nil, nil, fmt.Errorf("missing RX %s", def.Name)
		}
		byName := map[string]AuthoredField{}
		ordinals := map[string]int{}
		if def.Provider == "trietrunk" {
			for _, f := range append(append([]AuthoredField{}, source.Presentation.Base...), source.Presentation.Definitions[def.Name]...) {
				if _, ok := byName[f.Name]; ok {
					return nil, nil, nil, fmt.Errorf("duplicate field %s", f.Name)
				}
				byName[f.Name] = f
			}
		}
		for i, f := range rx.Fields {
			ordinals[f.Name] = i
		}
		rows := make([]any, len(rx.Fields))
		for i, f := range rx.Fields {
			a, ok := byName[f.Name]
			if !ok && def.Provider == "trietrunk" && f.Name != "@fields" {
				return nil, nil, nil, fmt.Errorf("unmapped field %s", f.Name)
			}
			field := a.Field
			if !ok {
				field = Field{Semantic: strings.TrimPrefix(f.Name, "@"), Label: strings.TrimPrefix(f.Name, "@"), Description: "Provider field. Constraints are defined by the paired RX."}
			}
			if f.Name == "@fields" {
				field = Field{Semantic: "fields", Label: "Fields", Description: "Additional source fields, excluding successfully consumed mappings."}
			}
			if field.Source != "" {
				url, ok := source.Sources[field.Source]
				if !ok {
					return nil, nil, nil, fmt.Errorf("unknown source %s", field.Source)
				}
				field.Source = url
			}
			for _, d := range a.Domains {
				o, ok := ordinals[d.Field]
				if !ok {
					return nil, nil, nil, fmt.Errorf("unknown domain %s", d.Field)
				}
				field.Domains = append(field.Domains, Domain{uint32(o), d.Value, d.Values})
			}
			rows[i] = field
		}
		raw, e := encodePack(meta, rows, map[string][]byte{"definition": rx.Digest[:], "schema": meta.Digest[:], "name": []byte(def.Name)})
		if e != nil {
			return nil, nil, nil, e
		}
		entry := Resources{Name: def.Name, Metadata: Location{uint64(len(metadata)), uint32(len(raw))}, Synonyms: map[string]Location{}}
		metadata = append(metadata, raw...)
		for _, set := range source.Mappings {
			if set.Target != def.Name {
				continue
			}
			src := definitions[set.Source]
			if src == nil {
				return nil, nil, nil, fmt.Errorf("missing source RX %s", set.Source)
			}
			mapped := make([]any, 0, len(set.Fields))
			seen := map[string]bool{}
			for _, m := range set.Fields {
				ordinal, ok := ordinals[m.To]
				if !ok || m.To == "@fields" {
					return nil, nil, nil, fmt.Errorf("invalid target %s", m.To)
				}
				key := strings.Join(m.From, "\x00")
				if len(m.From) == 0 || seen[key] {
					return nil, nil, nil, fmt.Errorf("duplicate/empty source binding")
				}
				seen[key] = true
				priority := m.Priority
				if priority == 0 {
					priority = 1
				}
				mapped = append(mapped, Synonym{m.From, uint32(ordinal), m.Cast, priority, m.Numerator, m.Denominator})
			}
			raw, e = encodePack(synonym, mapped, map[string][]byte{"source": src.Digest[:], "source_name": []byte(set.Source), "definition": rx.Digest[:], "schema": synonym.Digest[:]})
			if e != nil {
				return nil, nil, nil, e
			}
			entry.Synonyms[set.Source] = Location{uint64(len(synonyms)), uint32(len(raw))}
			synonyms = append(synonyms, raw...)
		}
		registry[hex.EncodeToString(rx.Digest[:])] = entry
	}
	return metadata, synonyms, registry, nil
}

type Pack struct {
	Headers map[string][]byte
	Members [][]byte
}

// ReadPack reads only the registered byte range. PAC parsing/validation remains
// owned by pill-packer; callers retain the returned backing bytes while in use.
func ReadPack(reader io.ReaderAt, location Location, schema, definition *Layout) (*Pack, error) {
	if location.Length < 1 || location.Offset > uint64(^uint64(0)>>1)-uint64(location.Length) {
		return nil, fmt.Errorf("invalid metadata range")
	}
	raw := make([]byte, int(location.Length))
	if _, e := reader.ReadAt(raw, int64(location.Offset)); e != nil {
		return nil, e
	}
	rows, headers, e := schema.Unpack(raw)
	if e != nil {
		return nil, e
	}
	if !bytes.Equal(headers["definition"], definition.Digest[:]) || !bytes.Equal(headers["schema"], schema.Digest[:]) {
		return nil, fmt.Errorf("metadata Definition mismatch")
	}
	return &Pack{Headers: headers, Members: rows}, nil
}

type PreparedField struct {
	Field
	values  map[string]Value
	labels  map[string]labelTarget
	domains []preparedDomain
}
type preparedDomain struct {
	ordinal uint32
	value   string
	values  map[string]Value
	labels  map[string]labelTarget
}
type Prepared struct{ Fields []PreparedField }

func valueMap(values []Value) (map[string]Value, error) {
	out := make(map[string]Value, len(values))
	for _, v := range values {
		if _, ok := out[v.Value]; ok {
			return nil, fmt.Errorf("duplicate metadata value %s", v.Value)
		}
		out[v.Value] = v
	}
	return out, nil
}

// Only unique, exact labels can replace a stored value. Repeated labels (for
// example several severity numbers named INFO) must not invent an exact code.
type labelTarget struct {
	value  string
	unique bool
}

func labelMap(values []Value) map[string]labelTarget {
	out := make(map[string]labelTarget, len(values))
	for _, v := range values {
		if v.Label == "" {
			continue
		}
		if prior, exists := out[v.Label]; exists && prior.value != v.Value {
			out[v.Label] = labelTarget{}
		} else if !exists {
			out[v.Label] = labelTarget{v.Value, true}
		}
	}
	return out
}

// Prepare performs schema decoding once, not once per rendered event or cell.
func Prepare(p *Pack, schema, definition *Layout) (*Prepared, error) {
	if !bytes.Equal(p.Headers["definition"], definition.Digest[:]) || !bytes.Equal(p.Headers["schema"], schema.Digest[:]) || len(p.Members) != len(definition.Fields) {
		return nil, fmt.Errorf("metadata ordinal binding mismatch")
	}
	result := &Prepared{Fields: make([]PreparedField, len(p.Members))}
	for i, raw := range p.Members {
		view, e := schema.DecodeJSON(raw)
		if e != nil {
			return nil, e
		}
		f := &result.Fields[i]
		if e = json.Unmarshal(view, &f.Field); e != nil {
			return nil, e
		}
		f.values, e = valueMap(f.Values)
		if e != nil {
			return nil, e
		}
		f.labels = labelMap(f.Values)
		for _, d := range f.Domains {
			if int(d.Ordinal) >= len(result.Fields) {
				return nil, fmt.Errorf("invalid domain ordinal")
			}
			values, e := valueMap(d.Values)
			if e != nil {
				return nil, e
			}
			f.domains = append(f.domains, preparedDomain{d.Ordinal, d.Value, values, labelMap(d.Values)})
		}
	}
	return result, nil
}

// ResolveLabel advises a metadata-backed cast. Its returned text is still
// validated/cast using the destination RX. Unknown/ambiguous labels remain
// source data, not metadata, and must not be silently discarded.
func (p *Prepared) ResolveLabel(ordinal int, label string, context func(uint32) string) (string, bool) {
	if ordinal < 0 || ordinal >= len(p.Fields) {
		return "", false
	}
	f := &p.Fields[ordinal]
	if context != nil {
		for _, d := range f.domains {
			if context(d.ordinal) == d.value {
				if value, exists := d.labels[label]; exists {
					return value.value, value.unique
				}
			}
		}
	}
	value, exists := f.labels[label]
	return value.value, exists && value.unique
}

// IsDerivedLabel permits omitting an input label only when the retained code
// reconstructs it exactly. A conflicting/custom label carries extra information.
func (p *Prepared) IsDerivedLabel(ordinal int, code, label string, context func(uint32) string) bool {
	v, ok := p.Lookup(ordinal, code, context)
	return ok && v.Label == label
}

// Lookup receives already-formatted keys and optional context lookup. It does
// not decode PIL, concatenate qualified names, or allocate on the hot path.
func (p *Prepared) Lookup(ordinal int, value string, context func(uint32) string) (Value, bool) {
	if ordinal < 0 || ordinal >= len(p.Fields) {
		return Value{}, false
	}
	f := &p.Fields[ordinal]
	if context != nil {
		for _, d := range f.domains {
			if context(d.ordinal) == d.value {
				if v, ok := d.values[value]; ok {
					return v, true
				}
			}
		}
	}
	v, ok := f.values[value]
	return v, ok
}

type PreparedSynonyms struct {
	Fields []Synonym
	Source [32]byte
}

func PrepareSynonyms(p *Pack, schema, definition *Layout) (*PreparedSynonyms, error) {
	if !bytes.Equal(p.Headers["definition"], definition.Digest[:]) || !bytes.Equal(p.Headers["schema"], schema.Digest[:]) || len(p.Headers["source"]) != 32 {
		return nil, fmt.Errorf("synonym Definition mismatch")
	}
	result := &PreparedSynonyms{}
	copy(result.Source[:], p.Headers["source"])
	seen := map[string]bool{}
	for _, raw := range p.Members {
		view, e := schema.DecodeJSON(raw)
		if e != nil {
			return nil, e
		}
		var s Synonym
		if e = json.Unmarshal(view, &s); e != nil {
			return nil, e
		}
		if int(s.Ordinal) >= len(definition.Fields) || definition.Fields[s.Ordinal].Name == "@fields" || len(s.From) == 0 {
			return nil, fmt.Errorf("invalid synonym binding")
		}
		switch s.Cast {
		case "identity", "decimal_to_integer", "scale_exact", "metadata_label":
		default:
			return nil, fmt.Errorf("unknown cast %s", s.Cast)
		}
		if s.Cast == "scale_exact" && (s.Numerator == 0 || s.Denominator == 0) {
			return nil, fmt.Errorf("invalid scale ratio")
		}
		for _, part := range s.From {
			if part == "" {
				return nil, fmt.Errorf("empty source path")
			}
		}
		key := strings.Join(s.From, "\x00")
		if seen[key] {
			return nil, fmt.Errorf("duplicate source binding")
		}
		seen[key] = true
		result.Fields = append(result.Fields, s)
	}
	sort.SliceStable(result.Fields, func(i, j int) bool {
		a, b := result.Fields[i], result.Fields[j]
		if a.Cast == "metadata_label" && b.Cast != "metadata_label" {
			return false
		}
		if a.Cast != "metadata_label" && b.Cast == "metadata_label" {
			return true
		}
		return a.Priority > b.Priority
	})
	return result, nil
}
