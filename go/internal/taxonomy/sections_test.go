package taxonomy

import (
	"bytes"
	"errors"
	"testing"
)

func TestFooterSectionsPreserveAddressesAndRecover(t *testing.T) {
	old, service, branch := fixture(t)
	old.SetSection(1, []byte("first registry"))
	before := old.Bytes()
	for _, growNodes := range []bool{false, true} {
		next := old.Clone()
		next.SetSection(1, bytes.Repeat([]byte("registry"), 30))
		next.SetSection(500, []byte("future footer section"))
		if growNodes {
			add(t, next, service, Resource, "new resource", "", "")
		}
		for cut := 0; cut < 2*(len(before)+next.Size())+128; cut++ {
			f := &crashFile{raw: bytes.Clone(before), durable: bytes.Clone(before), remaining: cut}
			_, err := retail(f, before, next)
			if err != nil && !errors.Is(err, interrupted) {
				t.Fatal(cut, err)
			}
			for _, disk := range [][]byte{f.raw, f.durable} {
				got, e := Load(disk)
				if e != nil {
					t.Fatal(growNodes, cut, e)
				}
				if !got.Extends(old) {
					t.Fatal("identities moved", cut)
				}
				id, _ := got.BranchID(branch)
				prior, _ := old.BranchID(branch)
				if id != prior {
					t.Fatal("branch ID changed")
				}
				if !bytes.Equal(got.Section(1), old.Section(1)) && !bytes.Equal(got.Section(1), next.Section(1)) {
					t.Fatal("partial registry exposed")
				}
				retry := &crashFile{raw: bytes.Clone(disk), remaining: -1}
				raw, e := retail(retry, disk, next)
				if e != nil {
					t.Fatal(cut, e)
				}
				restored, e := Load(raw)
				if e != nil || !bytes.Equal(restored.Bytes(), next.Bytes()) {
					t.Fatal("retry", cut, e)
				}
				if !bytes.Equal(raw[headerBytes:headerBytes+(len(old.nodes)-1)*nodeBytes], before[headerBytes:headerBytes+(len(old.nodes)-1)*nodeBytes]) {
					t.Fatal("node bytes rewritten")
				}
			}
		}
	}
}
