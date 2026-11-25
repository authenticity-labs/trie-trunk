package cache

import (
	"os"
	"reflect"
	"testing"
	"time"
)

func TestCoverageAndPackedGaps(t *testing.T) {
	got, e := Missing([]Span{{10, 30}, {40, 60}}, []Span{{0, 12}, {15, 25}, {45, 55}})
	want := []Span{{12, 15}, {25, 30}, {40, 45}, {55, 60}}
	if e != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("%v %v", got, e)
	}
	extents := []Extent{{Span{100, 120}, 0}, {Span{140, 160}, 20}, {Span{120, 140}, 40}}
	gotExt, e := Resolve(extents, Span{110, 150})
	if e != nil || !reflect.DeepEqual(gotExt, []Extent{{Span{110, 120}, 10}, {Span{120, 140}, 40}, {Span{140, 150}, 20}}) {
		t.Fatalf("%v %v", gotExt, e)
	}
	if _, e = Resolve(extents[:2], Span{110, 150}); e == nil {
		t.Fatal("unfilled gap accepted")
	}
}

func TestConfigOverridesAndInvalidReload(t *testing.T) {
	c, e := ParseConfiguration([]byte("version=1\ntimezone='America/New_York'\n[defaults.retention]\nread='7d'\nwrite='30d'\n[resources.\"payment/transactions.v1\"]\nobject_index=true\n[resources.\"payment/transactions.v1\".retention]\nread='3d'\n"))
	if e != nil {
		t.Fatal(e)
	}
	p := c.Resolve("payment/transactions.v1")
	dur, e := p.Retention.Durations()
	if e != nil || !(*p.ObjectIndex) || dur[Read] != 72*time.Hour || dur[Write] != 720*time.Hour {
		t.Fatalf("policy %+v %v", p, e)
	}
	s, e := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner", Layout: testLayout(t, false)})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	old := s.policy
	e = os.WriteFile(configurationPath(s.config.Root, []string{s.config.Cache}), []byte("version=999\n"), 0600)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.ReloadPolicy(); e == nil || !reflect.DeepEqual(old, s.policy) {
		t.Fatal("invalid policy replaced active policy")
	}
}

func TestReferenceCapacityKeepsLiteral(t *testing.T) {
	l := testLayout(t, false)
	d := l.Definition
	d.Fields[4] = Field{Name: "state", Type: "U16"}
	var e error
	l, e = Compile(d)
	if e != nil {
		t.Fatal(e)
	}
	tr := newTrie()
	v := make([]Value, len(l.Widths))
	for i := 0; i < 65535; i++ {
		v[4] = Uint(uint64(i), 2)
		ref := tr.intern(l, v)
		if ref != uint16(i+1) {
			t.Fatalf("reference %d %d", i, ref)
		}
	}
	v[4] = Uint(65535, 2)
	if ref := tr.intern(l, v); ref != 0 || len(tr.nodes) != 65536 {
		t.Fatalf("capacity overflow %d %d", ref, len(tr.nodes))
	}
	if v[4].State != Direct || unsigned(v[4].Bytes) != 65535 {
		t.Fatal("literal lost")
	}
	v[4] = Value{}
	if ref := tr.intern(l, v); ref != 0 {
		t.Fatal("absent root referenced")
	}
}
