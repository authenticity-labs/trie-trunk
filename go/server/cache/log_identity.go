package cache

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	layout "github.com/authenticity-labs/pill-packer/go/codec"

	pil "github.com/authenticity-labs/pill-packer/go"
)

func (l *Layout) logStrategy() bool { return l.Definition.Resource.Strategy == "log" }

func (l *Layout) validateIdentity(v Value) error {
	if !l.logStrategy() {
		_, err := eventID(v)
		return err
	}
	if (v.State != Direct && v.State != Offset) || len(v.Bytes) < 1 || len(v.Bytes) > 10 || len(v.Bytes) == 10 && v.Bytes[0] > 3 {
		return fmt.Errorf("@id must contain at most 74 entropy bits")
	}
	return nil
}

// Entropy is an application identity, never a row address. It remains unchanged
// while sealing replaces the provisional @record with its final hour ordinal.
func (l *Layout) generateLogIdentity(values []Value, entropy []byte) error {
	if v := values[l.Time]; v.State != Direct || len(v.Bytes) != 8 || unsigned(v.Bytes) >= 1<<48 {
		return fmt.Errorf("log ingress requires @time in epoch milliseconds")
	}
	if v := values[l.Event]; v.State == Absent || v.State == Null {
		if len(entropy) < 5 || len(entropy) > 10 {
			return fmt.Errorf("generated entropy requires 5 to 10 bytes")
		}
		if _, err := rand.Read(entropy); err != nil {
			return err
		}
		if len(entropy) == 10 {
			entropy[0] &= 3
		}
		values[l.Event] = Value{State: Offset, Bytes: entropy}
	}
	return l.validateIdentity(values[l.Event])
}

// Recovery rebuilds the same internal ordering key from persisted coordinates.
func (l *Layout) orderingKey(values []Value) ([16]byte, error) {
	if !l.logStrategy() {
		return eventID(values[l.Event])
	}
	if err := l.validateIdentity(values[l.Event]); err != nil {
		return [16]byte{}, err
	}
	if values[l.Time].State != Direct || values[l.Record].State != Direct {
		return [16]byte{}, fmt.Errorf("missing log storage coordinates")
	}
	return seedKey(int64(unsigned(values[l.Time].Bytes)), uint32(unsigned(values[l.Record].Bytes)))
}

// BindAuthoredDefinition preserves the compiled CDDL layout and constraints.
// Only the document identity/version and Seed application annotation are bound.
func BindAuthoredDefinition(raw []byte, binding Binding) ([]byte, error) {
	var d Definition
	if err := pil.UnmarshalObject(raw, &d); err != nil {
		return nil, err
	}
	if len(d.CDDL) == 0 {
		return nil, fmt.Errorf("authored Definition requires CDDL")
	}
	d.Identity = binding.Service + "/" + binding.Resource
	d.Version = uint64(binding.Major)<<20 | uint64(binding.Minor)
	d.Resource = binding
	var schema map[string]pil.RawObject
	if err := pil.UnmarshalObject(d.CDDL, &schema); err != nil {
		return nil, err
	}
	var err error
	schema["identity"], err = pil.MarshalObject(d.Identity)
	if err != nil {
		return nil, err
	}
	schema["version"], err = pil.MarshalObject(d.Version)
	if err != nil {
		return nil, err
	}
	d.CDDL, err = pil.MarshalObject(schema)
	if err != nil {
		return nil, err
	}

	// An authored RX has no storage-only coordinates. Add those to the bound
	// CDDL through the standard compiler, so the layout and schema stay identical.
	if binding.Strategy == "log" {
		missing := []Field{}
		for _, f := range []Field{{Name: "@record", Type: "U24"}, {Name: "@leaf", Type: "U16"}} {
			found := false
			for _, old := range d.Fields {
				if old.Name == f.Name {
					found = true
					break
				}
			}
			if !found {
				missing = append(missing, f)
			}
		}
		if len(missing) > 0 {
			var root string
			var types map[string]pil.RawObject
			if err = pil.UnmarshalObject(schema["root"], &root); err != nil {
				return nil, err
			}
			if err = pil.UnmarshalObject(schema["types"], &types); err != nil {
				return nil, err
			}
			var node map[string]pil.RawObject
			if err = pil.UnmarshalObject(types[root], &node); err != nil {
				return nil, err
			}
			var fields []pil.RawObject
			if err = pil.UnmarshalObject(node["fields"], &fields); err != nil {
				return nil, err
			}
			for _, f := range missing {
				max := uint64(65535)
				if f.Type == "U24" {
					max = 16777215
				}
				raw, e := pil.MarshalObject(map[string]any{"name": f.Name, "optional": true, "value": map[string]any{"type": "integer", "min": uint64(0), "max": max}})
				if e != nil {
					return nil, e
				}
				fields = append(fields, raw)
			}
			node["fields"], err = pil.MarshalObject(fields)
			if err != nil {
				return nil, err
			}
			types[root], err = pil.MarshalObject(node)
			if err != nil {
				return nil, err
			}
			schema["types"], err = pil.MarshalObject(types)
			if err != nil {
				return nil, err
			}
			// DOC accepts the CDDL JSON document. Decode its values without converting
			// integer bounds to floating point.
			var document any
			encoded, e := pil.MarshalObject(schema)
			if e != nil {
				return nil, e
			}
			if e = pil.UnmarshalObject(encoded, &document); e != nil {
				return nil, e
			}
			text, e := json.Marshal(cddlJSONValue(document))
			if e != nil {
				return nil, e
			}
			rx, e := layout.FromJSON(text)
			if e != nil {
				return nil, e
			}
			d.Fields = append([]Field(nil), rx.Definition.Fields...)
			var compiled map[string]pil.RawObject
			e = pil.UnmarshalObject(rx.Bytes, &compiled)
			rx.Close()
			if e != nil {
				return nil, e
			}
			d.CDDL = compiled["cddl"]
		}
	}
	l, err := Compile(d)
	if err != nil {
		return nil, err
	}
	defer l.Close()
	return append([]byte(nil), l.Bytes...), nil
}

func cddlJSONValue(v any) any {
	switch x := v.(type) {
	case map[any]any:
		m := map[string]any{}
		for k, v := range x {
			m[fmt.Sprint(k)] = cddlJSONValue(v)
		}
		return m
	case []any:
		for i, v := range x {
			x[i] = cddlJSONValue(v)
		}
		return x
	}
	return v
}
