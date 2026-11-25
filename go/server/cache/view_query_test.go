package cache

import (
	"encoding/json"
	"testing"
)

func TestQueryScalarPrecisionAndStates(t *testing.T) {
	if !compareQuery(QueryLiteral{"number", "18446744073709551615"}, QueryLiteral{"number", "18446744073709551614"}, ">") {
		t.Fatal("U64 precision")
	}
	if compareQuery(QueryLiteral{"absent", ""}, QueryLiteral{"null", ""}, "==") || !compareQuery(QueryLiteral{"null", ""}, QueryLiteral{"null", ""}, "==") {
		t.Fatal("absent/null")
	}
	if !compareQuery(QueryLiteral{"number", "-32768"}, QueryLiteral{"number", "-1"}, "<") || !compareQuery(QueryLiteral{"boolean", "false"}, QueryLiteral{"boolean", "false"}, "==") {
		t.Fatal("typed comparisons")
	}
	for _, value := range []string{"1e999999999", "0x12", "1/2", "NaN"} {
		q := QueryPlan{Filters: []QueryFilter{{"duration", "==", QueryLiteral{"number", value}}}, Order: "asc"}
		raw, _ := json.Marshal(q)
		if _, _, e := parseQueryPlan(string(raw)); e == nil {
			t.Fatalf("accepted %s", value)
		}
	}
	if _, _, e := parseQueryPlan(`{"filters":[],"order":"asc","execute":"bad"}`); e == nil {
		t.Fatal("unknown query members")
	}
	l := testLayout(t, false)
	f := Frame{Values: make([]Value, len(l.Widths))}
	f.Values[3] = Uint(^uint64(0), 8)
	if !matchQuery(l, &f, &QueryPlan{Filters: []QueryFilter{{"duration", ">", QueryLiteral{"duration", "18446744073709551614us"}}}}) {
		t.Fatal("full width integer")
	}
	f.Values[5] = Value{Null, nil}
	if !matchQuery(l, &f, &QueryPlan{Filters: []QueryFilter{{"message", "==", QueryLiteral{"null", ""}}}}) {
		t.Fatal("null")
	}
}
