// Package cache implements the collection-native storage contract. Legacy store
// formats remain in package store and are never reinterpreted in place.
package cache

import (
	"crypto/sha256"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"strings"
	"time"
)

const Format uint32 = 6
const MaxFrame = pil.MaxFrame

type Branch uint8

const (
	Read Branch = iota
	Write
	Error
	Debug
	Trace
)

func (b Branch) Valid() bool { return b <= Trace }
func (b Branch) Prefix() string {
	if !b.Valid() {
		return ""
	}
	return [...]string{"read", "write", "error", "debug", "trace"}[b]
}
func (b Branch) Name() string {
	if !b.Valid() {
		return ""
	}
	return [...]string{"read", "write", "error", "debug", "trace"}[b]
}

// Operational rollups retain their existing branch scope. Field accumulators
// and five-minute summary PACs are maintained for every valid branch.
func (b Branch) Aggregates() bool { return b <= Error }
func ParseBranch(s string) (Branch, error) {
	for b := Read; b <= Trace; b++ {
		if s == b.Name() {
			return b, nil
		}
	}
	return 0, fmt.Errorf("unknown branch %q", s)
}

type Field = pil.Field
type Roles struct {
	Shared   string `json:"shared,omitempty" pil:"shared,omitempty"`
	Event    string `json:"event_id" pil:"event_id"`
	Object   string `json:"id,omitempty" pil:"id,omitempty"`
	Revision string `json:"revision_id,omitempty" pil:"revision_id,omitempty"`
	Duration string `json:"duration,omitempty" pil:"duration,omitempty"`
}
type Binding struct {
	Strategy     string                 `json:"strategy,omitempty" pil:"strategy,omitempty"`
	Format       uint32                 `json:"format" pil:"format"`
	Service      string                 `json:"service" pil:"service"`
	Resource     string                 `json:"resource" pil:"resource"`
	Major        uint32                 `json:"major" pil:"major"`
	Minor        uint32                 `json:"minor" pil:"minor"`
	Timezone     string                 `json:"timezone" pil:"timezone"`
	Roles        Roles                  `json:"roles" pil:"roles"`
	Path         []string               `json:"path,omitempty" pil:"path,omitempty"`
	Aggregations []string               `json:"aggregations,omitempty" pil:"aggregations,omitempty"`
	References   []ReferenceTranslation `json:"references,omitempty" pil:"references,omitempty"`
}

// The standard PIL definition is also the storage map. Its resource section supplies
// storage roles and projections; PIL clients can read the same definition bytes.
type Definition struct {
	CDDL             pil.RawObject `json:"cddl,omitempty" pil:"cddl,omitempty"`
	PIL              uint32        `json:"pil" pil:"pil"`
	AdaptiveIntegers string        `json:"adaptive_integers,omitempty" pil:"adaptive_integers,omitempty"`
	Identity         string        `json:"definition" pil:"definition"`
	Version          uint64        `json:"version" pil:"version"`
	Fields           []Field       `json:"fields" pil:"fields"`
	Resource         Binding       `json:"resource" pil:"resource"`
}
type Layout struct {
	statistics *statisticsLayout
	aggregates *aggregatePlan
	*pil.Layout
	Definition                                Definition
	Event, Object, Revision, Duration, Shared int
	Record, Time, Leaf                        int
	Path                                      []int
}

// Cache metadata, controls, and open objects all use the PIL bootstrap layout.
type objectCodec struct{}

func (objectCodec) Marshal(v any) ([]byte, error)   { return pil.MarshalObject(v) }
func (objectCodec) Unmarshal(b []byte, v any) error { return pil.UnmarshalObject(b, v) }
func (objectCodec) Valid(b []byte) error            { var v any; return pil.UnmarshalObject(b, &v) }

var encoder objectCodec
var decoder objectCodec

func name(s string) bool {
	if len(s) == 0 || len(s) > 128 || s == "." || s == ".." {
		return false
	}
	for i, c := range []byte(s) {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || i > 0 && (c == '-' || c == '_' || c == '.') {
			continue
		}
		return false
	}
	return true
}
func Compile(d Definition) (*Layout, error) {
	// Detach the compiled map from a caller's mutable definition slices.
	d.Fields = append([]Field(nil), d.Fields...)
	for i := range d.Fields {
		d.Fields[i].Values = append([]string(nil), d.Fields[i].Values...)
	}
	d.Resource.Path = append([]string(nil), d.Resource.Path...)
	d.Resource.Aggregations = append([]string(nil), d.Resource.Aggregations...)
	d.Resource.References = cloneReferences(d.Resource.References)
	intrinsics := []Field{{Name: "@event_id", Type: "U24"}, {Name: "@time", Type: "TIMESTAMP_MS"}, {Name: "@leaf", Type: "REF16"}}
	if d.Resource.Strategy == "log" {
		intrinsics = []Field{{Name: "@record", Type: "U24"}, {Name: "@time", Type: "U64"}, {Name: "@leaf", Type: "U16"}}
	}
	for _, intrinsic := range intrinsics {
		found := false
		for _, f := range d.Fields {
			if f.Name == intrinsic.Name {
				if f.Type != intrinsic.Type {
					return nil, fmt.Errorf("invalid Seed intrinsic field %s", f.Name)
				}
				found = true
			}
		}
		if !found {
			d.Fields = append(d.Fields, intrinsic)
		}
	}
	n := d.Resource
	raw, err := pil.MarshalObject(d)
	if err != nil {
		return nil, err
	}
	portable, err := pil.Parse(raw, pil.IdentityProfile{Primary: -1})
	if err != nil {
		return nil, err
	}
	if n.Format != Format || !name(n.Service) || !name(n.Resource) || n.Major == 0 || n.Major >= 1<<12 || n.Minor >= 1<<20 || n.Timezone == "" {
		return nil, fmt.Errorf("invalid cache definition binding")
	}
	if _, err := time.LoadLocation(n.Timezone); err != nil {
		return nil, fmt.Errorf("invalid timezone: %w", err)
	}
	if d.Identity != n.Service+"/"+n.Resource || d.Version != uint64(n.Major)<<20|uint64(n.Minor) {
		return nil, fmt.Errorf("definition identity/version disagrees with binding")
	}
	l := &Layout{Layout: portable, Definition: d, Object: -1, Revision: -1, Duration: -1, Shared: -1}
	byName := map[string]int{}
	for i, f := range d.Fields {
		byName[f.Name] = i
	}
	if err := validateReferences(d, byName); err != nil {
		return nil, err
	}
	role := func(s string, required bool) (int, error) {
		if s == "" && !required {
			return -1, nil
		}
		i, ok := byName[s]
		if !ok {
			return -1, fmt.Errorf("unknown role field %q", s)
		}
		return i, nil
	}
	l.Record = byName[intrinsics[0].Name]
	l.Leaf = byName["@leaf"]
	l.Time = byName["@time"]
	if l.Event, err = role(n.Roles.Event, true); err != nil {
		return nil, err
	}
	if d.Resource.Strategy == "log" {
		if n.Roles.Event != "@id" || d.Fields[l.Event].Type != "BYTES" {
			return nil, fmt.Errorf("log strategy requires @id bytes")
		}
	} else if d.Fields[l.Event].Type != "UUID" {
		return nil, fmt.Errorf("event_id role must be UUID")
	}
	if l.Object, err = role(n.Roles.Object, false); err != nil {
		return nil, err
	}
	if l.Revision, err = role(n.Roles.Revision, false); err != nil {
		return nil, err
	}
	if l.Duration, err = role(n.Roles.Duration, false); err != nil {
		return nil, err
	}
	if l.Shared, err = role(n.Roles.Shared, false); err != nil {
		return nil, err
	}
	if l.Shared >= 0 && d.Fields[l.Shared].Type != "REF16" {
		return nil, fmt.Errorf("shared role must be REF16")
	}
	roles := map[int]bool{}
	for _, i := range []int{l.Event, l.Object, l.Revision, l.Duration, l.Shared} {
		if i < 0 {
			continue
		}
		if roles[i] {
			return nil, fmt.Errorf("role fields must be distinct")
		}
		roles[i] = true
	}
	for _, i := range []int{l.Object, l.Revision} {
		if i >= 0 {
			switch d.Fields[i].Type {
			case "UUID", "U8", "U16", "U24", "U32", "U64", "STRING", "BYTES":
			default:
				return nil, fmt.Errorf("unsupported identity type")
			}
			if len(d.Fields[i].Values) > 0 {
				return nil, fmt.Errorf("identity fields cannot be mapped")
			}
		}
	}
	if l.Duration >= 0 {
		switch d.Fields[l.Duration].Type {
		case "U8", "U16", "U24", "U32", "U64":
		default:
			return nil, fmt.Errorf("duration role must be an unsigned integer in microseconds")
		}
		if len(d.Fields[l.Duration].Values) > 0 {
			return nil, fmt.Errorf("duration role cannot be mapped")
		}
	}
	// Aggregatable operations are derived from all Definition fields. The
	// resource's declared projections remain validated, without a duration-only
	// vocabulary. Blank declarations do not disable type-driven accumulators.
	for _, a := range n.Aggregations {
		if a == "count" {
			continue
		}
		at := strings.LastIndexByte(a, '.')
		if at < 1 {
			return nil, fmt.Errorf("invalid aggregation %q", a)
		}
		ordinal, ok := byName[a[:at]]
		if !ok {
			return nil, fmt.Errorf("unknown aggregate field %q", a[:at])
		}
		kind := d.Fields[ordinal].Type
		switch a[at+1:] {
		case "count", "nulls":
		case "sum", "min", "max":
			if len(d.Fields[ordinal].Values) > 0 || !(strings.HasPrefix(kind, "U") && kind != "UUID" || strings.HasPrefix(kind, "I") || kind == "F32" || kind == "F64" || kind == "BOOL" || strings.HasPrefix(kind, "TIMESTAMP_") && a[at+1:] != "sum") {
				return nil, fmt.Errorf("unsupported aggregation %q", a)
			}
		default:
			return nil, fmt.Errorf("unsupported aggregation %q", a)
		}
	}
	seen := map[int]bool{}
	for _, s := range n.Path {
		i, ok := byName[s]
		if !ok || roles[i] || i == l.Record || i == l.Time || i == l.Leaf || seen[i] || l.Widths[i] == 0 {
			return nil, fmt.Errorf("invalid shared path field %q", s)
		}
		seen[i] = true
		l.Path = append(l.Path, i)
	}
	l.Layout.Identity = pil.IdentityProfile{Primary: l.Event, Fields: map[int]bool{l.Object: true, l.Revision: true}, Validate: l.validateIdentity}
	l.statistics = compileStatistics(l)
	l.aggregates, err = compileAggregates(l)
	if err != nil {
		return nil, err
	}
	return l, nil
}
func ParseDefinition(raw []byte) (*Layout, error) {
	if len(raw) > MaxFrame {
		return nil, fmt.Errorf("map too large")
	}
	var d Definition
	if err := pil.UnmarshalObject(raw, &d); err != nil {
		return nil, err
	}
	l, err := Compile(d)
	if err != nil {
		return nil, err
	}
	if len(d.Fields) != len(l.Definition.Fields) {
		return nil, fmt.Errorf("Definition is missing Seed intrinsic fields")
	}
	portable, err := pil.Parse(raw, l.Layout.Identity)
	if err != nil {
		return nil, err
	}
	l.Layout = portable
	l.Bytes = append([]byte(nil), raw...)
	l.Digest = sha256.Sum256(raw)
	return l, nil
}

// Existing positional producers supply user columns; Seed-owned columns are
// appended by the compiled binding and populated from the original UUID.
func (l *Layout) ProducerValues(values []Value) []Value {
	if len(values) == len(l.Definition.Fields)-3 && l.Record == len(values) && l.Time == len(values)+1 && l.Leaf == len(values)+2 {
		return append(append([]Value(nil), values...), Value{}, Value{}, Value{})
	}
	return values
}

func (l *Layout) Encode(dst []byte, values []Value) ([]byte, error) {
	values = l.ProducerValues(values)
	if !l.logStrategy() && len(values) == len(l.Definition.Fields) && values[l.Event].State != Absent && values[l.Event].State != Null {
		id, err := eventID(values[l.Event])
		if err != nil {
			return nil, err
		}
		values = append([]Value(nil), values...)
		values[l.Time] = Uint(uint64(eventTime(id[:])), 8)
	}
	return l.Layout.Encode(dst, values)
}
