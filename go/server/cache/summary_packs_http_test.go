package cache

import (
	"net/url"
	"testing"
)

func TestSummarySparseRanges(t *testing.T) {
	for _, raw := range []string{"range=0,86400001", "range=-1,86400000", "range=86400000,0", "range=0,86400000&beginTime=0", "range=0,88560000000"} {
		q, _ := url.ParseQuery(raw)
		if _, e := summaryRanges(q); e == nil {
			t.Fatal(raw)
		}
	}
	q := url.Values{"range": {"172800000,259200000", "0,86400000", "0,172800000"}}
	r, e := summaryRanges(q)
	if e != nil || len(r) != 1 || r[0] != (summaryRange{0, 259200000}) {
		t.Fatal(r, e)
	}
}
