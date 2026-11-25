package cache

import (
	"testing"
)

func TestRecordStartsAcrossHeadersPagesAndRetention(t *testing.T) {
	const base = int64(1792584000000)
	b := branchStore{rows: make([]byte, 261*IndexRowSize)}
	b.runs = []Run{
		{Start: 1, End: 258, PAC: &StoredPAC{Placement: PACPlacement{From: base, To: base + TimeHourMS, MemberStart: 4, MemberBytes: 257 * 37}}},
		{Start: 259, End: 261, PAC: &StoredPAC{Placement: PACPlacement{From: base + TimeHourMS, To: base + 2*TimeHourMS, MemberStart: 4 + 257*37 + 4, MemberBytes: 2 * 37}}},
	}
	for _, r := range b.runs {
		copy(b.row(r.Start-1), indexHeader(r.PAC.Placement.From, r.End-r.Start))
		for n := r.Start; n < r.End; n++ {
			order.PutUint32(b.row(n), uint32(r.PAC.Placement.From+int64(n-r.Start)))
		}
	}
	check := func() {
		for _, r := range b.runs {
			if e := validateIndexHeader(b.row(r.Start-1), r); e != nil {
				t.Fatal(e)
			}
			for n := r.Start; n < r.End; n++ {
				if b.rowTime(n) != r.PAC.Placement.From+int64(n-r.Start) {
					t.Fatal("start boundary", n)
				}
			}
		}
	}
	check()
	b.rows = b.rows[258*IndexRowSize:]
	b.baseRows = 258
	b.runs = b.runs[1:]
	check()
}
