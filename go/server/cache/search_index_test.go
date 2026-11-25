package cache

import (
	"context"
	"testing"
	"time"
)

func requireSearch(t testing.TB) { t.Helper() }

func searchRows(t testing.TB, s *Store, from, to time.Time, q QueryPlan) []uint64 {
	t.Helper()
	v, e := s.viewSnapshot(Write, from, to, nil, nil, true)
	if e != nil {
		t.Fatal(e)
	}
	defer v.close()
	v.api = &HTTP{Store: s}
	if e = v.filterQuery(context.Background(), &q); e != nil {
		t.Fatal(e)
	}
	var rows []uint64
	for _, span := range v.ranges {
		for row := span.at; row < span.end; row++ {
			rows = append(rows, row)
		}
	}
	return rows
}
