package cache

import (
	"encoding/json"
	"testing"
)

func TestDurationQueryUnitsAndBoundaries(t *testing.T) {
	l := testLayout(t, false)
	f := Frame{Values: make([]Value, len(l.Widths))}
	f.Values[l.Duration] = Uint(10000000, 8)
	for _, tc := range []struct {
		op, kind, value string
		want            bool
	}{
		{"<=", "number", "10000", true}, {"<", "number", "10000", false},
		{">=", "number", "10000", true}, {">", "number", "10000", false},
		{"==", "number", "10000", true}, {"!=", "number", "10000", false},
		{"<=", "duration", "10000us", false}, {"==", "duration", "10s", true},
		{"==", "duration", "1e4ms", true}, {"==", "duration", "10000000µs", true},
		{"==", "duration", "10000000μs", true}, {"==", "duration", "10000000000ns", true},
		{"<", "duration", "10000.0001ms", true}, {">", "duration", "9999999999ns", true},
	} {
		q := QueryPlan{Filters: []QueryFilter{{"duration", tc.op, QueryLiteral{tc.kind, tc.value}}}, Order: "asc"}
		raw, _ := json.Marshal(q)
		parsed, _, err := parseQueryPlan(string(raw))
		if err != nil {
			t.Fatal(err)
		}
		if got := matchQuery(l, &f, parsed); got != tc.want {
			t.Errorf("%+v: %v", tc, got)
		}
	}
	// Units follow the role even if the field has a different name.
	l.Definition.Fields[l.Duration].Name = "elapsed"
	if !matchQuery(l, &f, &QueryPlan{Filters: []QueryFilter{{"elapsed", "==", QueryLiteral{"number", "10000"}}}}) {
		t.Fatal("custom duration role")
	}
	f.Values[2] = Uint(10000, 2)
	if !matchQuery(l, &f, &QueryPlan{Filters: []QueryFilter{{"revision_id", "==", QueryLiteral{"number", "10000"}}}}) {
		t.Fatal("unrelated numeric field was scaled")
	}
	for _, value := range []string{"1e999ms", "NaNms", "10minutes", "0x10us"} {
		raw, _ := json.Marshal(QueryPlan{Filters: []QueryFilter{{"elapsed", "==", QueryLiteral{"duration", value}}}, Order: "asc"})
		if _, _, err := parseQueryPlan(string(raw)); err == nil {
			t.Fatal("accepted", value)
		}
	}
	sources := []*viewSource{{api: &HTTP{Store: &Store{layout: l}}}}
	if err := validateQueryPlan(&QueryPlan{Filters: []QueryFilter{{"revision_id", "==", QueryLiteral{"duration", "10ms"}}}}, sources); err == nil {
		t.Fatal("units accepted on non-duration field")
	}
}
