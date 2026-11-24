package cache

import (
	"math"
	"math/bits"
	"strings"

	pil "github.com/authenticity-labs/pill-packer/go/codec"
)

// Aggregate behavior is bound once from field types, never from field names.
// Dictionaries are categorical even when their physical codes are integers.
const (
	aggregatePresence byte = iota
	aggregateUnsigned
	aggregateSigned
	aggregateFloat
	aggregateBoolean
	aggregateTime
)

type aggregateField struct {
	ordinal int
	kind    byte
}
type aggregatePlan struct {
	fields     []aggregateField
	layout     *pil.Layout
	cell       *pil.Layout
	descriptor []byte
}
type fieldAggregate struct {
	Count, Nulls uint64
	Min, Max     uint64
	SumLo, SumHi uint64 // exact 128-bit two's complement for integral sums
	compensation float64
	unsafe       bool // a non-finite input disables bounds and sum, never ingress
}
type fieldBucket struct {
	Count  uint64
	Fields []fieldAggregate
}
type fieldHour struct {
	Buckets [12]fieldBucket
	Total   fieldBucket
}

func compileAggregates(l *Layout) (*aggregatePlan, error) {
	p := &aggregatePlan{}
	d := pil.Definition{Identity: "trie-trunk/field-aggregates", Fields: []pil.Field{{Name: "count", Type: "U64"}, {Name: "fields", Type: "ARRAY<BYTES>"}}}
	for i, f := range l.Definition.Fields {
		// These values are assigned by sealing/indexing, not by ingress.
		// Omitting their cells leaves summary pruning conservative.
		if i == l.Record || i == l.Leaf {
			continue
		}
		kind := aggregatePresence
		if len(f.Values) == 0 {
			switch f.Type {
			case "U8", "U16", "U24", "U32", "U64":
				kind = aggregateUnsigned
			case "I8", "I16", "I32", "I64":
				kind = aggregateSigned
			case "F32", "F64":
				kind = aggregateFloat
			case "BOOL":
				kind = aggregateBoolean
			default:
				if strings.HasPrefix(f.Type, "TIMESTAMP_") {
					kind = aggregateTime
				}
			}
		}
		p.fields = append(p.fields, aggregateField{i, kind})
		p.descriptor = append(p.descriptor, byte(i), byte(i>>8), kind)
	}
	var err error
	p.layout, err = pil.Compile(d)
	if err != nil {
		return nil, err
	}
	cell := pil.Definition{Identity: "trie-trunk/field-aggregate"}
	for _, name := range []string{"count", "nulls", "min", "max", "sum_lo", "sum_hi"} {
		cell.Fields = append(cell.Fields, pil.Field{Name: name, Type: "U64"})
	}
	p.cell, err = pil.Compile(cell)
	return p, err
}
func signedField(raw []byte) int64 {
	n := unsigned(raw)
	shift := uint(64 - len(raw)*8)
	return int64(n<<shift) >> shift
}
func (a *fieldAggregate) add(kind byte, v Value) {
	if v.State == pil.Absent {
		return
	}
	if v.State == pil.Null {
		a.Nulls++
		return
	}
	a.Count++
	if kind == aggregatePresence {
		return
	}
	n := unsigned(v.Bytes)
	if kind == aggregateSigned {
		n = uint64(signedField(v.Bytes))
	}
	if kind == aggregateFloat {
		x := math.Float64frombits(n)
		if len(v.Bytes) == 4 {
			x = float64(math.Float32frombits(uint32(n)))
			n = math.Float64bits(x)
		}
		if math.IsNaN(x) || math.IsInf(x, 0) {
			a.unsafe = true
			return
		}
		if a.Count == 1 || x < math.Float64frombits(a.Min) {
			a.Min = n
		}
		if a.Count == 1 || x > math.Float64frombits(a.Max) {
			a.Max = n
		}
		y := x - a.compensation
		sum := math.Float64frombits(a.SumLo)
		next := sum + y
		a.compensation = (next - sum) - y
		a.SumLo = math.Float64bits(next)
		// Overflow invalidates the sum, but finite min/max still remain useful.
		return
	}
	less := func(x, y uint64) bool {
		if kind == aggregateSigned {
			return int64(x) < int64(y)
		}
		return x < y
	}
	if a.Count == 1 || less(n, a.Min) {
		a.Min = n
	}
	if a.Count == 1 || less(a.Max, n) {
		a.Max = n
	}
	if kind == aggregateTime {
		return
	}
	hi := uint64(0)
	if kind == aggregateSigned && int64(n) < 0 {
		hi = math.MaxUint64
	}
	var carry uint64
	a.SumLo, carry = bits.Add64(a.SumLo, n, 0)
	a.SumHi, _ = bits.Add64(a.SumHi, hi, carry)
}
func (b *fieldBucket) add(p *aggregatePlan, values []Value) {
	if b.Fields == nil {
		b.Fields = make([]fieldAggregate, len(p.fields))
	}
	b.Count++
	for i, f := range p.fields {
		if f.ordinal < len(values) {
			b.Fields[i].add(f.kind, values[f.ordinal])
		}
	}
}
func (h *fieldHour) add(p *aggregatePlan, record int, values []Value) {
	h.Total.add(p, values)
	h.Buckets[record].add(p, values)
}
func (h fieldHour) clone() fieldHour {
	h.Total.Fields = append([]fieldAggregate(nil), h.Total.Fields...)
	for i := range h.Buckets {
		h.Buckets[i].Fields = append([]fieldAggregate(nil), h.Buckets[i].Fields...)
	}
	return h
}
func (p *aggregatePlan) record(b fieldBucket) ([]byte, error) {
	cells := make([][]byte, len(p.fields))
	for i, f := range p.fields {
		a := fieldAggregate{}
		if i < len(b.Fields) {
			a = b.Fields[i]
		}
		values := make([]pil.Value, 6)
		values[0] = pil.Uint(a.Count, 8)
		if a.Nulls > 0 {
			values[1] = pil.Uint(a.Nulls, 8)
		}
		if a.Count > 0 && f.kind != aggregatePresence && !a.unsafe {
			values[2], values[3] = pil.Uint(a.Min, 8), pil.Uint(a.Max, 8)
			if f.kind != aggregateTime && !(f.kind == aggregateFloat && (math.IsNaN(math.Float64frombits(a.SumLo)) || math.IsInf(math.Float64frombits(a.SumLo), 0))) {
				values[4], values[5] = pil.Uint(a.SumLo, 8), pil.Uint(a.SumHi, 8)
			}
		}
		var err error
		cells[i], err = p.cell.Encode(nil, values)
		if err != nil {
			return nil, err
		}
	}
	array, err := pil.EncodeArray(cells, 0)
	if err != nil {
		return nil, err
	}
	return p.layout.Encode(nil, []pil.Value{pil.Uint(b.Count, 8), {State: pil.Offset, Bytes: array}})
}
func (p *aggregatePlan) pack(h fieldHour, first, end int) ([]byte, error) {
	records := make([][]byte, 0, end-first)
	for _, b := range h.Buckets[first:end] {
		raw, err := p.record(b)
		if err != nil {
			return nil, err
		}
		records = append(records, raw)
	}
	pack, err := pil.FactorPAC(p.layout, records, nil, false)
	if err != nil {
		return nil, err
	}
	total, err := p.record(h.Total)
	if err != nil {
		return nil, err
	}
	pack.Headers = map[string][]byte{"fields": p.descriptor, "rollup": total}
	pack.AddressBytes = 2
	return pack.Marshal()
}
