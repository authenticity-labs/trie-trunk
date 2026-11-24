package taxonomy

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func add(t testing.TB, tr *Trie, p NodeID, k Kind, n, ty, l string) NodeID {
	t.Helper()
	a, e := tr.Add(p, k, n, ty, l)
	if e != nil {
		t.Fatal(e)
	}
	return a
}
func fixture(t testing.TB) (*Trie, NodeID, NodeID) {
	t.Helper()
	tr, e := New("Main")
	if e != nil {
		t.Fatal(e)
	}
	c := add(t, tr, tr.Root(), Cache, "Business", "", "")
	s := add(t, tr, c, Service, "Payment", "", "")
	r := add(t, tr, s, Resource, "Transactions", "", "")
	v := add(t, tr, r, Version, "1", "", "")
	b := add(t, tr, v, Branch, "", "Web", "Info")
	return tr, s, b
}
func TestFixedNodesStableEOFReferencesAndLeafIDs(t *testing.T) {
	tr, service, branch := fixture(t)
	before := tr.Bytes()
	oldCount := tr.NodeCount()
	oldLeaf, _ := tr.BranchID(branch)
	if oldLeaf != 1 {
		t.Fatal("intermediate nodes consumed leaf IDs")
	}
	old, err := Load(before)
	if err != nil {
		t.Fatal(err)
	}
	r := add(t, tr, service, Resource, "Refunds", "", "")
	v := add(t, tr, r, Version, "1", "", "")
	other := add(t, tr, v, Branch, "", "Collections", "")
	after := tr.Bytes()
	if !bytes.Equal(before[headerBytes:headerBytes+oldCount*nodeBytes], after[headerBytes:headerBytes+oldCount*nodeBytes]) {
		t.Fatal("existing trie bytes moved")
	}
	if !tr.Extends(old) {
		t.Fatal("old directory references changed")
	}
	for ref, name := range old.labels {
		at := len(after) - int(ref)
		if string(after[at:at+len(name)]) != name {
			t.Fatal("EOF-relative label moved", name)
		}
	}
	loaded, err := Load(after)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := loaded.Leaf(oldLeaf); !ok || got != branch {
		t.Fatal("branch identity changed")
	}
	id, _ := loaded.BranchID(other)
	if id != 2 {
		t.Fatal("leaf ID is not compact")
	}
	path, err := loaded.Path(other)
	if err != nil || len(path) != 6 || path[2] != service {
		t.Fatal(path, err)
	}
	n, _ := loaded.Node(other)
	if n.Level != 0 {
		t.Fatal("empty Level must preserve zero slot")
	}
	size := loaded.Size()
	if add(t, loaded, service, Resource, "Refunds", "", "") != r || loaded.Size() != size {
		t.Fatal("duplicate registration appended")
	}
	snapshot := loaded.Bytes()
	snapshot[0] = 0
	if loaded.Bytes()[0] == 0 {
		t.Fatal("mutable snapshot")
	}
}
func TestBranchCapacityDoesNotCountIntermediateNodes(t *testing.T) {
	tr, service, _ := fixture(t)
	for i := 0; i < 65536; i++ {
		add(t, tr, service, Resource, fmt.Sprintf("resource-%d", i), "", "")
	}
	if tr.NodeCount() <= 65535 {
		t.Fatal("fixture did not cross node limit")
	}
	r := add(t, tr, service, Resource, "Leaves", "", "")
	v := add(t, tr, r, Version, "1", "", "")
	for i := 1; i < math.MaxUint16; i++ {
		add(t, tr, v, Branch, "", "Web", fmt.Sprintf("level-%d", i))
	}
	last := tr.Bytes()
	if _, err := tr.Add(v, Branch, "", "Web", "overflow"); err == nil {
		t.Fatal("wrapped branch ID")
	}
	if !bytes.Equal(last, tr.Bytes()) {
		t.Fatal("capacity failure changed trie")
	}
	loaded, err := Load(last)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := loaded.Leaf(math.MaxUint16); !ok {
		t.Fatal("last leaf unavailable")
	}
}
func TestRejectCorruptDirectoryAndNode(t *testing.T) {
	tr, _, _ := fixture(t)
	raw := tr.Bytes()
	for _, mutate := range []func([]byte){
		func(b []byte) { b[0] = 0 },
		func(b []byte) { b[len(b)-1] = 0 },
		func(b []byte) { order.PutUint64(b[len(b)-trailerBytes:], math.MaxUint64) },
		func(b []byte) { order.PutUint32(b[headerBytes+nodeBytes+4:], 3) },
		func(b []byte) { order.PutUint32(b[headerBytes+8:], 1) },
	} {
		b := append([]byte(nil), raw...)
		mutate(b)
		if _, err := Load(b); err == nil {
			t.Fatal("accepted corrupt resource")
		}
	}
	for cut := 0; cut < len(raw); cut++ {
		if _, err := Load(raw[:cut]); err == nil {
			t.Fatalf("accepted incomplete initial file at %d", cut)
		}
	}
}
func TestRegistrationRetailsAndRestartDoesNotWrite(t *testing.T) {
	tr, s, b := fixture(t)
	path := filepath.Join(t.TempDir(), "trunk.trie")
	before, err := Save(path, tr)
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := Save(path, tr)
	if err != nil || !bytes.Equal(before, unchanged) {
		t.Fatal("unchanged registration wrote", err)
	}
	r := add(t, tr, s, Resource, "Future", "", "")
	v := add(t, tr, r, Version, "1", "", "")
	add(t, tr, v, Branch, "", "Web", "Error")
	after, err := Save(path, tr)
	if err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, disk) {
		t.Fatal("published image differs from file", err)
	}
	loaded, err := Load(disk)
	if err != nil {
		t.Fatal(err)
	}
	if id, ok := loaded.BranchID(b); !ok || id != 1 {
		t.Fatal("restart changed leaf")
	}
	c, err := selectCommit(disk)
	if err != nil || int(c.end) != len(disk) {
		t.Fatal("retained staging tail", err)
	}
}
func BenchmarkTaxonomy(b *testing.B) {
	tr, s, branch := fixture(b)
	for i := 0; i < 10000; i++ {
		add(b, tr, s, Resource, fmt.Sprintf("resource-%d", i), "", "")
	}
	raw := tr.Bytes()
	b.Run("Node", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := tr.Node(branch); !ok {
				b.Fatal("missing")
			}
		}
	})
	b.Run("Leaf", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := tr.Leaf(1); !ok {
				b.Fatal("missing")
			}
		}
	})
	b.Run("Child", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := tr.Find(s, Resource, "resource-9999", "", ""); !ok {
				b.Fatal("missing")
			}
		}
	})
	b.Run("Path", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := tr.Path(branch); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Load", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		for i := 0; i < b.N; i++ {
			if _, err := Load(raw); err != nil {
				b.Fatal(err)
			}
		}
	})
}
