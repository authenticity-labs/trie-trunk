package cache

import (
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFieldAccumulatorsBindTypesNotNames(t *testing.T) {
	l, err := Compile(Definition{Identity: "test/values", Version: 1 << 20, Fields: []Field{{Name: "event", Type: "UUID"}, {Name: "temperature", Type: "I64"}, {Name: "balance", Type: "U64"}, {Name: "ratio", Type: "F64"}, {Name: "ready", Type: "BOOL"}, {Name: "text", Type: "STRING"}, {Name: "category", Type: "STRING", Values: []string{"a", "b"}}}, Resource: Binding{Format: Format, Service: "test", Resource: "values", Major: 1, Timezone: "UTC", Roles: Roles{Event: "event"}}})
	if err != nil {
		t.Fatal(err)
	}
	var h fieldHour
	value := func(s int64, u uint64, f float64) []Value {
		v := make([]Value, len(l.Definition.Fields))
		v[1] = pil.Uint(uint64(s), 8)
		v[2] = pil.Uint(u, 8)
		v[3] = pil.Uint(math.Float64bits(f), 8)
		v[4] = pil.Uint(1, 1)
		v[5] = pil.Value{State: pil.Null}
		v[6] = pil.Uint(1, 1)
		return v
	}
	h.add(l.aggregates, 0, value(-5, math.MaxUint64, 1.5))
	h.add(l.aggregates, 11, value(3, math.MaxUint64, -0.5))
	a := h.Total.Fields
	if a[1].Count != 2 || int64(a[1].Min) != -5 || int64(a[1].Max) != 3 || a[1].SumLo != math.MaxUint64-1 || a[1].SumHi != math.MaxUint64 {
		t.Fatal("signed sum/bounds", a[1])
	}
	if a[2].SumHi != 1 || a[2].SumLo != math.MaxUint64-1 {
		t.Fatal("lost exact U64 sum", a[2])
	}
	if math.Float64frombits(a[3].SumLo) != 1 || a[4].SumLo != 2 || a[5].Nulls != 2 || a[5].Count != 0 || l.aggregates.fields[6].kind != aggregatePresence {
		t.Fatal("wrong type aggregation", a)
	}
	saved := h.clone()
	h.add(l.aggregates, 0, value(-8, 1, math.NaN()))
	if saved.Total.Count != 2 || saved.Total.Fields[1].Count != 2 || saved.Buckets[0].Fields[1].Count != 1 {
		t.Fatal("published snapshot mutated")
	}
	raw, err := l.aggregates.pack(h, 0, 12)
	if err != nil {
		t.Fatal(err)
	}
	if dir := os.Getenv("TRUNK_LIVE_SUMMARY_FIXTURE"); dir != "" {
		if err = os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "typed.pac"), raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	p, err := pil.ParsePAC(l.aggregates.layout, raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	var total pil.Frame
	if err = l.aggregates.layout.ScanPlain(p.Headers["rollup"], &total); err != nil {
		t.Fatal(err)
	}
	array, err := pil.OpenArray(total.Values[1].Bytes, 0)
	if err != nil {
		t.Fatal(err)
	}
	cell, err := array.Get(3)
	if err != nil {
		t.Fatal(err)
	}
	var values pil.Frame
	if err = l.aggregates.cell.ScanPlain(cell, &values); err != nil {
		t.Fatal(err)
	}
	if values.Values[2].State != pil.Absent {
		t.Fatal("NaN must not produce pruning bounds")
	}
}
func TestAccumulatorRingRecyclesOnlyOnSuccessfulSeal(t *testing.T) {
	var ring summaryRing
	hour := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC).UnixMilli()
	original := ring.acquire(hour)
	original.live.Total.Count = 7
	for i := 1; i < 26; i++ {
		ring.acquire(hour + int64(i)*TimeHourMS)
	}
	if ring.len() != 26 || ring.get(hour) != original || ring.get(hour).live.Total.Count != 7 {
		t.Fatal("wall clock or backlog overwrote unsealed hour")
	}
	frozen := ring.frozen(hour)
	ring.acquire(hour + 26*TimeHourMS)
	if frozen.get(hour) != original || frozen.len() != 1 {
		t.Fatal("seal snapshot did not retain exact hour")
	}
	ring.release(hour)
	if ring.get(hour) != nil || ring.len() != 26 || ring.get(hour+2*TimeHourMS) == nil {
		t.Fatal("seal rotation lost another hour")
	}
	if frozen.get(hour).live.Total.Count != 7 {
		t.Fatal("retired snapshot mutated")
	}
}

func TestFieldAccumulatorsFullDefinition(t *testing.T) {
	d := Definition{Identity: "test/wide", Version: 1 << 20, Resource: Binding{Format: Format, Service: "test", Resource: "wide", Major: 1, Timezone: "UTC", Roles: Roles{Event: "event"}}, Fields: []Field{{Name: "event", Type: "UUID"}}}
	for i := 1; i < 253; i++ {
		d.Fields = append(d.Fields, Field{Name: fmt.Sprintf("metric%d", i), Type: "U64"})
	}
	l, err := Compile(d)
	if err != nil {
		t.Fatal(err)
	}
	values := make([]Value, 256)
	values[252] = pil.Uint(42, 8)
	var h fieldHour
	h.add(l.aggregates, 0, values)
	raw, err := l.aggregates.pack(h, 0, 12)
	if err != nil {
		t.Fatal(err)
	}
	p, err := pil.ParsePAC(l.aggregates.layout, raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	var record pil.Frame
	if err = l.aggregates.layout.ScanPlain(p.Headers["rollup"], &record); err != nil {
		t.Fatal(err)
	}
	array, err := pil.OpenArray(record.Values[1].Bytes, 0)
	if err != nil || array.Len() != 254 {
		t.Fatal(err, array.Len())
	}
}
