package cache

import (
	"fmt"
	"trie-trunk/go/codec"
)

// These adapters intentionally use the existing compact CBOR implementation.
// Field IDs and dictionary ordinals remain numeric. No expanded JSON is built.
// This is a benchmark payload adapter, not a second production file format.
func fixtureCBOR(l *Layout, values []Value) (codec.Record, error) {
	r := codec.Record{}
	for i, v := range values {
		if v.State == Absent {
			continue
		}
		var value any
		switch v.State {
		case Null:
			value = nil
		case Direct:
			if l.Definition.Fields[i].Type == "UUID" {
				value = v.Bytes
			} else {
				value = unsigned(v.Bytes)
			}
		case Offset:
			if l.Definition.Fields[i].Type == "STRING" {
				value = string(v.Bytes)
			} else {
				return nil, fmt.Errorf("unsupported benchmark variable type")
			}
		}
		r[codec.Key(i)] = value
	}
	return r, nil
}

func scanFixtureCBOR(l *Layout, raw []byte, f *Frame) error {
	r, e := codec.Decode(raw, codec.CBOR)
	if e != nil {
		return e
	}
	f.Values = make([]Value, len(l.Widths))
	for key, v := range r {
		i := int(key)
		if i >= len(f.Values) {
			return fmt.Errorf("unknown benchmark field")
		}
		switch v := v.(type) {
		case nil:
			f.Values[i] = Value{State: Null}
		case uint64:
			w := l.Widths[i]
			if w <= 0 || w > 8 {
				return fmt.Errorf("invalid integer width")
			}
			if w < 8 && v >= uint64(1)<<uint(w*8) {
				return fmt.Errorf("CBOR integer overflow")
			}
			if len(l.Definition.Fields[i].Values) > 0 && v >= uint64(len(l.Definition.Fields[i].Values)) {
				return fmt.Errorf("invalid CBOR dictionary reference")
			}
			f.Values[i] = Uint(v, w)
		case string:
			f.Values[i] = Text(v)
		case []byte:
			f.Values[i] = Value{Direct, v}
		default:
			return fmt.Errorf("unsupported benchmark scalar %T", v)
		}
	}
	return nil
}
