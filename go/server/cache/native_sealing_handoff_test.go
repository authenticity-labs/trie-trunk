package cache

import (
	"bytes"
	"testing"
	"time"
)

func TestNativeSealedPACOwnershipAndCodec(t *testing.T) {
	at := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	document, err := seedNativeDocument([]byte(`[{"header":{"rx-name":"storage-handoff"},"fields":[
		{"name":"time","type":[1,0],"size":6,"annotations":["@time"]},
		{"name":"message","type":[5,0],"size":0}
	]}]`))
	if err != nil {
		t.Fatal(err)
	}
	defer document.close()
	definition, err := seedNativeRegister(document.bytes)
	if err != nil {
		t.Fatal(err)
	}
	defer definition.close()
	var frames [][]byte
	for i := range 2 {
		pill, err := definition.ingest([]seedNativeValue{
			{ordinal: 0, kind: seedUnsigned, low: uint64(at.UnixMilli()) + uint64(i)*1000},
			{ordinal: 1, kind: seedText, bytes: []byte("received")},
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		defer pill.close()
		frames = append(frames, pill.bytes)
	}
	for _, policy := range []PACPolicy{{Algorithm: "gzip"}, {Algorithm: "zstd"}, {Algorithm: "brotli"}} {
		m := newNativeMemory(16 << 20)
		plain, err := definition.pack(frames, uint64(at.UnixMilli()), true, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		raw := plain.bytes
		expected := bytes.Clone(raw)
		borrowed, err := expandPAC(raw)
		if err != nil || &borrowed[0] != &raw[0] {
			t.Fatal("plain Pack was copied", err)
		}
		if allocations := testing.AllocsPerRun(100, func() {
			view, err := expandPAC(raw)
			if err != nil || &view[0] != &raw[0] {
				panic("plain Pack borrow failed")
			}
		}); allocations != 0 {
			t.Fatalf("plain Pack adapter allocated %g times per open", allocations)
		}
		sealed, err := m.sealPAC(raw, policy.algorithmCode(), 1)
		if err != nil {
			t.Fatal(err)
		}
		compressed, err := seedCompressPAC(raw, policy.algorithmCode(), 1)
		if err != nil || !bytes.Equal(compressed, sealed.bytes) {
			t.Fatal("storage policy differs between native and host-owned adapters", err)
		}
		plain.close()
		b := &branchStore{}
		s := &Store{memory: m, config: Config{Now: func() time.Time { return at.Add(2 * time.Hour) }}}
		run := Run{MaxMS: at.UnixMilli(), PacksStart: 24, PacksEnd: 24 + uint64(len(sealed.bytes)), PAC: &StoredPAC{Placement: PACPlacement{From: at.UnixMilli()}}}
		address := &sealed.bytes[0]
		if err = s.rememberPackOwned(b, run, sealed); err != nil {
			t.Fatal(err)
		}
		retained := b.residentPack(run, time.Time{})
		if len(retained) != 1 || &retained[0][0] != address {
			t.Fatal("resident publication copied output")
		}
		pin := b.pinMemory()
		sealed.close()
		for _, arena := range b.sealedMemory {
			arena.close()
		}
		decoded, err := expandPAC(retained[0])
		if err != nil || !bytes.Equal(decoded, expected) {
			t.Fatal("native payload invalid after producer release", err)
		}
		pin()
		if used := m.stats().Used; used != 0 {
			t.Fatal("native allocations leaked", used)
		}
		m.close()
	}
}
func TestNativeLivePACRetainsAllViews(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(time.Minute) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	events := []pending{}
	for i := 0; i < 32; i++ {
		raw := testFrame(t, l, at.Add(time.Duration(i)*time.Millisecond), uint32(i+1), false)
		var f Frame
		if err = l.Scan(raw, &f); err != nil {
			t.Fatal(err)
		}
		id, _ := eventID(f.Values[l.Event])
		events = append(events, pending{id, raw})
	}
	before := s.memory.stats().Used
	p, owner, err := s.livePack(Read, at.UnixMilli(), events)
	if err != nil {
		t.Fatal(err)
	}
	if !owner.matches(p.Bytes) || !owner.matches(p.Common) {
		t.Fatal("PAC views escape native output")
	}
	for _, member := range p.Members {
		if !owner.matches(member) {
			t.Fatal("member borrows expired scratch")
		}
	}
	owner.close()
	if s.memory.stats().Used != before {
		t.Fatal("live response leaked native buffers")
	}
}
