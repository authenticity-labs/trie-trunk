package cache

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func TestHourDirectoryIndependentWidening(t *testing.T) {
	origin := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	first := origin + 65536*TimeHourMS
	r := timeRun(first, 1<<32, 65536)
	r.PacksStart = 1<<40 + 72
	r.PAC.Placement.MemberStart = 1<<40 + 4
	d, err := buildHourDirectory([]Run{r}, first, first+TimeHourMS, r.End, [16]byte{1}, origin, [6]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(d.raw[40:46], []byte{5, 6, 6, 3, 3, 3}) {
		t.Fatal("widths", d.raw[40:46])
	}
	leaf, ok := d.leaf(first)
	if !ok || leaf.Start != r.Start-1 || leaf.Packs != r.PacksStart || leaf.Beds != r.PAC.Placement.MemberStart-4-4*(r.End-r.Start) || leaf.End != r.End {
		t.Fatal(leaf)
	}
	address := collectionAddress{65536, 65536}
	if got, e := d.resolveReference(address); e != nil || got != r.End-1 {
		t.Fatal(got, e)
	}
	if _, e := d.resolveReference(collectionAddress{65536, 65537}); e == nil {
		t.Fatal("out-of-hour record accepted")
	}
	h := indexHeader(first, 65536)
	if len(h) != 6 || validateIndexHeader(h, r) != nil {
		t.Fatal("literal count did not widen")
	}
}

func TestPACCompressionWrapperBoundaries(t *testing.T) {
	read := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile("../../../testdata/http-ingress/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	def, err := seedNativeRegister(read("query.rx"))
	if err != nil {
		t.Fatal(err)
	}
	defer def.close()
	members := [][]byte{read("0.pil"), read("1.pil"), read("2.pil")}
	pack, err := def.pack(members, 1794830400000, true, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pack.close()
	for _, algorithm := range []uint32{1, 2, 3} {
		compressed, err := seedCompressPAC(pack.bytes, algorithm, 1)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := seedNativeLength(compressed); err != nil || n != uint64(len(compressed)) {
			t.Fatal(n, err)
		}
		decoded, err := seedExpandPAC(compressed)
		if err != nil || !bytes.Equal(decoded, pack.bytes) {
			t.Fatalf("algorithm %d round trip: %v", algorithm, err)
		}
		for _, bad := range [][]byte{compressed[:len(compressed)-1], append(bytes.Clone(compressed), 0)} {
			if _, err = seedExpandPAC(bad); err == nil {
				t.Fatal("accepted wrong stored length", algorithm)
			}
		}
	}
}
