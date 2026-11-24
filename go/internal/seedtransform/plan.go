// Package seedtransform binds provider fields to an application's Definition.
// PIL framing, field decoding and output encoding remain owned by pill-packer.
package seedtransform

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	native "github.com/authenticity-labs/pill-packer/go"
	api "github.com/authenticity-labs/pill-packer/go/api"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"math/big"
	"math/bits"
	"reflect"
	"strconv"
	"strings"
	meta "trie-trunk/go/internal/definitionmeta"
)

type node struct {
	layout      *pil.Layout
	frame       pil.Frame
	children    map[int]*node
	used        []bool
	owner       *api.Prescription
	projections map[int]*api.Prescription
	schemas     map[int]map[string]any
}
type binding struct {
	mapping meta.Synonym
	node    *node
	ordinal int
	borrow  bool
	scalar  [8]byte
}

// Plan owns reusable scan slots. The resource's existing ingress writer owns it
// exclusively; readers never observe scratch or partially transformed records.
type Plan struct {
	Source, Target *pil.Layout
	root           *node
	bindings       []binding
	metadata       *meta.Prepared
	priorities     []uint32
	extension      int
	validator      *native.Definition
	targetSchemas  map[int]map[string]any
}

func Compile(source, target *pil.Layout, mappings *meta.PreparedSynonyms, metadata *meta.Prepared) (*Plan, error) {
	if source.Digest != mappings.Source {
		return nil, fmt.Errorf("source Definition mismatch")
	}
	p := &Plan{Source: source, Target: target, metadata: metadata, extension: -1, priorities: make([]uint32, len(target.Definition.Fields))}
	var document struct {
		CDDL map[string]any `json:"cddl"`
	}
	if err := pil.UnmarshalObject(source.Bytes, &document); err != nil {
		return nil, err
	}
	schema := normalize(document.CDDL).(map[string]any)
	types, _ := schema["types"].(map[string]any)
	root, _ := schema["root"].(string)
	var build func(*pil.Layout, map[string]any, int) (*node, error)
	build = func(l *pil.Layout, value map[string]any, depth int) (*node, error) {
		if depth > 32 {
			return nil, fmt.Errorf("nested Definition exceeds depth limit")
		}
		n := &node{layout: l, children: map[int]*node{}, used: make([]bool, len(l.Definition.Fields)), projections: map[int]*api.Prescription{}, schemas: map[int]map[string]any{}}
		success := false
		defer func() {
			if !success {
				closeNode(n)
			}
		}()
		fields, _ := value["fields"].([]any)
		for _, f := range fields {
			field := f.(map[string]any)
			name := field["name"].(string)
			v, _ := field["value"].(map[string]any)
			for steps := 0; v["type"] == "reference"; steps++ {
				if steps > 32 {
					return nil, fmt.Errorf("recursive provider record")
				}
				v, _ = types[v["name"].(string)].(map[string]any)
			}
			n.schemas[indexOf(l, name)] = v
			if v["type"] != "record" && v["type"] != "open_record" {
				i := indexOf(l, name)
				if i >= 0 && l.Definition.Fields[i].Type == "BYTES" && v["type"] == "choice" {
					childTypes := map[string]any{}
					for k, v := range types {
						childTypes[k] = v
					}
					childTypes["provider-field"] = map[string]any{"type": "record", "fields": []any{field}}
					raw, e := json.Marshal(map[string]any{"identity": "provider-field", "root": "provider-field", "types": childTypes})
					if e != nil {
						return nil, e
					}
					rx, e := (api.Doc{}).Write(raw, api.JSON)
					if e != nil {
						return nil, e
					}
					n.projections[i] = rx
				}
				continue
			}
			index := indexOf(l, name)
			if index < 0 {
				return nil, fmt.Errorf("missing child %s", name)
			}
			childTypes := map[string]any{}
			for k, v := range types {
				childTypes[k] = v
			}
			childTypes["provider-child"] = v
			raw, err := json.Marshal(map[string]any{"identity": "provider-child", "root": "provider-child", "types": childTypes})
			if err != nil {
				return nil, err
			}
			// Use the same Definition compiler as DOC, including nested schema rules.
			rx, err := (api.Doc{}).Write(raw, api.JSON)
			if err != nil {
				return nil, fmt.Errorf("child %s: %w", name, err)
			}
			child, err := build(rx.Layout, v, depth+1)
			if err != nil {
				rx.Close()
				return nil, err
			}
			child.owner = rx
			n.children[index] = child
		}
		success = true
		return n, nil
	}
	var err error
	p.root, err = build(source, asMap(types[root]), 0)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Plan, error) { p.Close(); return nil, err }
	var targetDocument struct {
		CDDL map[string]any `json:"cddl"`
	}
	if err := pil.UnmarshalObject(target.Bytes, &targetDocument); err != nil {
		return fail(err)
	}
	targetSchema := normalize(targetDocument.CDDL).(map[string]any)
	targetTypes := asMap(targetSchema["types"])
	targetRoot, _ := targetSchema["root"].(string)
	p.targetSchemas = map[int]map[string]any{}
	targetFields, _ := asMap(targetTypes[targetRoot])["fields"].([]any)
	for _, f := range targetFields {
		field := asMap(f)
		name, _ := field["name"].(string)
		value := asMap(field["value"])
		for steps := 0; value["type"] == "reference"; steps++ {
			if steps > 32 {
				return fail(fmt.Errorf("recursive destination field"))
			}
			name, _ := value["name"].(string)
			value = asMap(targetTypes[name])
		}
		p.targetSchemas[indexOf(target, name)] = value
	}
	for i, f := range target.Definition.Fields {
		if f.Name == "@fields" {
			p.extension = i
		}
	}
	for _, m := range mappings.Fields {
		if len(m.From) == 0 || int(m.Ordinal) >= len(target.Definition.Fields) {
			return fail(fmt.Errorf("invalid binding"))
		}
		n := p.root
		for _, part := range m.From[:len(m.From)-1] {
			i := indexOf(n.layout, part)
			n = n.children[i]
			if n == nil {
				return fail(fmt.Errorf("undefined record path %v", m.From))
			}
		}
		i := indexOf(n.layout, m.From[len(m.From)-1])
		if i < 0 {
			return fail(fmt.Errorf("undefined source %v", m.From))
		}
		if m.Cast == "scale_exact" && (m.Numerator == 0 || m.Denominator == 0) {
			return fail(fmt.Errorf("invalid scale"))
		}
		a, b := n.layout.Definition.Fields[i], target.Definition.Fields[m.Ordinal]
		// A physical BYTES type alone cannot establish choice compatibility.
		// Identical compiled choices can retain their original encoded span.
		choiceCompatible := (n.schemas[i]["type"] != "choice" && p.targetSchemas[int(m.Ordinal)]["type"] != "choice") || reflect.DeepEqual(n.schemas[i], p.targetSchemas[int(m.Ordinal)])
		borrow := m.Cast == "identity" && a.Type == b.Type && reflect.DeepEqual(a.Values, b.Values) && n.children[i] == nil && choiceCompatible
		p.bindings = append(p.bindings, binding{mapping: m, node: n, ordinal: i, borrow: borrow})
	}
	p.validator, err = native.FromPIL(source.Bytes)
	if err != nil {
		return fail(err)
	}
	return p, nil
}
func normalize(v any) any {
	switch x := v.(type) {
	case map[any]any:
		m := map[string]any{}
		for k, v := range x {
			m[fmt.Sprint(k)] = normalize(v)
		}
		return m
	case map[string]any:
		for k, v := range x {
			x[k] = normalize(v)
		}
		return x
	case []any:
		for i, v := range x {
			x[i] = normalize(v)
		}
		return x
	}
	return v
}
func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }
func indexOf(l *pil.Layout, name string) int {
	for i, f := range l.Definition.Fields {
		if f.Name == name {
			return i
		}
	}
	return -1
}
func closeNode(n *node) {
	for _, child := range n.children {
		closeNode(child)
	}
	for _, rx := range n.projections {
		rx.Close()
	}
	if n.owner != nil {
		n.owner.Close()
	}
}
func (p *Plan) Close() {
	if p.validator != nil {
		p.validator.Close()
		p.validator = nil
	}
	if p.root != nil {
		closeNode(p.root)
		p.root = nil
	}
}
func scan(n *node, raw []byte, root bool) error {
	clear(n.used)
	err := n.layout.ScanStructure(raw, &n.frame)
	if err != nil {
		return err
	}
	for i, child := range n.children {
		v := n.frame.Values[i]
		if v.State == pil.Absent || v.State == pil.Null {
			clearNode(child)
			continue
		}
		if err = scan(child, v.Bytes, false); err != nil {
			return err
		}
	}
	return nil
}
func clearNode(n *node) {
	clear(n.frame.Values)
	clear(n.used)
	for _, c := range n.children {
		clearNode(c)
	}
}

// Apply borrows compatible spans. Returned spans live until the next Apply;
// ingress encodes them directly into its retained Rust-owned output allocation.
func (p *Plan) Apply(raw []byte, dst []pil.Value) ([]pil.Value, error) {
	if n, err := p.validator.Scan(raw, pil.MaxFrame); err != nil {
		return nil, err
	} else if n != len(raw) {
		return nil, fmt.Errorf("trailing provider bytes")
	}
	if err := scan(p.root, raw, true); err != nil {
		return nil, err
	}
	if cap(dst) < len(p.Target.Definition.Fields) {
		dst = make([]pil.Value, len(p.Target.Definition.Fields))
	} else {
		dst = dst[:len(p.Target.Definition.Fields)]
		clear(dst)
	}
	clear(p.priorities)
	selector := func(i uint32) string {
		if int(i) >= len(dst) || dst[i].State == pil.Absent {
			return ""
		}
		v, err := pil.FieldValue(p.Target.Definition.Fields[i], dst[i])
		if err != nil {
			return ""
		}
		return fmt.Sprint(v)
	}
	for bi := range p.bindings {
		b := &p.bindings[bi]
		if len(b.node.frame.Values) == 0 {
			continue
		}
		v := b.node.frame.Values[b.ordinal]
		if v.State == pil.Absent {
			continue
		}
		m := b.mapping
		target := int(m.Ordinal)
		var out pil.Value
		var err error
		if b.borrow || v.State == pil.Null {
			out = v
		} else {
			var value any
			value, err = fieldValue(b.node, b.ordinal, v)
			if err != nil {
				return nil, err
			}
			if m.Cast == "metadata_label" {
				label, ok := value.(string)
				if !ok {
					return nil, fmt.Errorf("metadata_label requires text")
				}
				if dst[target].State != pil.Absent {
					if p.metadata.IsDerivedLabel(target, selector(m.Ordinal), label, selector) {
						b.node.used[b.ordinal] = true
					}
					continue
				}
				code, ok := p.metadata.ResolveLabel(target, label, selector)
				if !ok {
					continue
				}
				value = code
				if strings.HasPrefix(p.Target.Definition.Fields[target].Type, "U") || strings.HasPrefix(p.Target.Definition.Fields[target].Type, "I") {
					value, err = integer(code)
				}
			} else {
				value, err = cast(value, m)
			}
			if err != nil {
				return nil, fmt.Errorf("%v: %w", m.From, err)
			}
			if u, ok := value.(uint64); ok && strings.HasPrefix(p.Target.Definition.Fields[target].Type, "U") && p.Target.Widths[target] > 0 {
				width := p.Target.Widths[target]
				if width < 8 && u>>(8*width) != 0 {
					return nil, fmt.Errorf("integer overflow for %s", p.Target.Definition.Fields[target].Name)
				}
				binary.LittleEndian.PutUint64(b.scalar[:], u)
				out = pil.Value{State: pil.Direct, Bytes: b.scalar[:width]}
			} else {
				var obj []byte
				obj, err = pil.MarshalObject(value)
				if err == nil {
					switch p.targetSchemas[target]["type"] {
					case "choice", "record", "open_record":
						out, err = p.Target.EncodeFieldObject(target, obj)
					default:
						out, err = p.Target.ValueFromObject(target, obj)
					}
				}
			}
			if err != nil {
				return nil, fmt.Errorf("%v: %w", m.From, err)
			}
		}
		if dst[target].State != pil.Absent {
			same := dst[target].State == out.State && bytes.Equal(dst[target].Bytes, out.Bytes)
			if !same && p.priorities[target] <= m.Priority {
				return nil, fmt.Errorf("conflicting mapping to %s", p.Target.Definition.Fields[target].Name)
			}
			// Lower-priority alternatives remain in @fields if they differ.
			if same {
				b.node.used[b.ordinal] = true
			}
			continue
		}
		dst[target] = out
		p.priorities[target] = m.Priority
		b.node.used[b.ordinal] = true
	}
	extra, err := remaining(p.root)
	if err != nil {
		return nil, err
	}
	if len(extra) > 0 {
		if p.extension < 0 {
			return nil, fmt.Errorf("unmapped fields require extension container")
		}
		object, err := pil.MarshalObject(extra)
		if err != nil {
			return nil, err
		}
		dst[p.extension], err = p.Target.ValueFromObject(p.extension, object)
		if err != nil {
			return nil, err
		}
	}
	return dst, nil
}
func remaining(n *node) (map[string]any, error) {
	var out map[string]any
	put := func(k string, v any) error {
		if out == nil {
			out = map[string]any{}
		}
		if _, ok := out[k]; ok {
			return fmt.Errorf("extension collision %s", k)
		}
		out[k] = v
		return nil
	}
	for i, v := range n.frame.Values {
		if v.State == pil.Absent || n.used[i] {
			continue
		}
		f := n.layout.Definition.Fields[i]
		if child := n.children[i]; child != nil && v.State != pil.Null {
			rest, err := remaining(child)
			if err != nil {
				return nil, err
			}
			if len(rest) > 0 {
				if err = put(f.Name, rest); err != nil {
					return nil, err
				}
			}
			continue
		}
		value, err := fieldValue(n, i, v)
		if err != nil {
			return nil, err
		}
		if f.Name == "@fields" {
			m, ok := normalize(value).(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid provider extensions")
			}
			for k, v := range m {
				if err = put(k, v); err != nil {
					return nil, err
				}
			}
		} else {
			if err = put(f.Name, value); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}
func integer(s string) (any, error) {
	if s == "" || strings.TrimSpace(s) != s {
		return nil, fmt.Errorf("expected exact integer")
	}
	if strings.HasPrefix(s, "-") {
		return strconv.ParseInt(s, 10, 64)
	}
	return strconv.ParseUint(s, 10, 64)
}
func cast(value any, m meta.Synonym) (any, error) {
	switch m.Cast {
	case "identity":
		return value, nil
	case "decimal_to_integer":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected decimal text")
		}
		return integer(s)
	case "scale_exact":
		if u, ok := value.(uint64); ok {
			high, low := bits.Mul64(u, m.Numerator)
			if high >= m.Denominator {
				return nil, fmt.Errorf("integer overflow")
			}
			q, r := bits.Div64(high, low, m.Denominator)
			if r != 0 {
				return nil, fmt.Errorf("inexact scale")
			}
			return q, nil
		}
		r := new(big.Rat)
		switch v := value.(type) {
		case uint64:
			r.SetInt(new(big.Int).SetUint64(v))
		case int64:
			r.SetInt64(v)
		case float64:
			if r.SetFloat64(v) == nil {
				return nil, fmt.Errorf("nonfinite number")
			}
		case json.Number:
			if _, ok := r.SetString(string(v)); !ok {
				return nil, fmt.Errorf("invalid number")
			}
		default:
			return nil, fmt.Errorf("expected numeric source, got %T", value)
		}
		r.Mul(r, new(big.Rat).SetFrac(new(big.Int).SetUint64(m.Numerator), new(big.Int).SetUint64(m.Denominator)))
		if !r.IsInt() {
			return nil, fmt.Errorf("inexact scale")
		}
		n := r.Num()
		if n.IsUint64() {
			return n.Uint64(), nil
		}
		if n.IsInt64() {
			return n.Int64(), nil
		}
		return nil, fmt.Errorf("integer overflow")
	default:
		return nil, fmt.Errorf("unknown cast %s", m.Cast)
	}
}

// Choice payloads are interpreted by their compiled codec, never by assumptions
// about the choice tag or nested PIL layout. Only this field is materialized.
func fieldValue(n *node, i int, v pil.Value) (any, error) {
	if rx := n.projections[i]; rx != nil && v.State != pil.Absent && v.State != pil.Null {
		raw, e := rx.Layout.Encode(nil, []pil.Value{v})
		if e != nil {
			return nil, e
		}
		raw, e = rx.Layout.DecodeJSON(raw)
		if e != nil {
			return nil, e
		}
		var object map[string]any
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		if e = d.Decode(&object); e != nil {
			return nil, e
		}
		return object[n.layout.Definition.Fields[i].Name], nil
	}
	return pil.FieldValue(n.layout.Definition.Fields[i], v)
}
