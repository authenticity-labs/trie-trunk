package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testLayout(t testing.TB, variable bool) *Layout {
	t.Helper()
	identity := "UUID"
	l, e := Compile(Definition{PIL: 0, Identity: "payment/transactions", Version: 1 << 20, Fields: []Field{
		{Name: "event_id", Type: "UUID"}, {Name: "id", Type: identity}, {Name: "revision_id", Type: "U64"}, {Name: "duration", Type: "U64"}, {Name: "state", Type: "STRING", Values: []string{"pending", "authorized", "canceled"}}, {Name: "message", Type: "STRING"}, {Name: "amount", Type: "U64"},
	}, Resource: Binding{Format: Format, Service: "payment", Resource: "transactions", Major: 1, Timezone: "America/New_York", Roles: Roles{Event: "event_id", Object: "id", Revision: "revision_id", Duration: "duration"}, Path: []string{"state"}, Aggregations: []string{"count", "duration.count", "duration.sum", "duration.min", "duration.max"}}})
	if e != nil {
		t.Fatal(e)
	}
	return l
}

func testObject(n uint32) Value { return uuid(0, n) }

func uuid(ms int64, n uint32) Value {
	var id [16]byte
	binary.BigEndian.PutUint64(id[:8], uint64(ms)<<16)
	id[6] = 0x70
	id[8] = 0x80
	binary.BigEndian.PutUint32(id[12:], n)
	return Value{Direct, id[:]}
}

func testFrame(t testing.TB, l *Layout, at time.Time, n uint32, sparse bool) []byte {
	t.Helper()
	id := testObject(42)
	v := []Value{uuid(at.UnixMilli(), n), id, Uint(uint64(n), 8), Uint(uint64(n+10), 8), Uint(uint64(n%3), 1), Text("request completed"), Uint(999, 8)}
	if sparse {
		v[3] = Value{}
		v[4] = Value{}
		v[5] = Value{Null, nil}
		v[6] = Uint(uint64(n), 8)
	}
	raw, e := l.Encode(nil, v)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}

func collect(t *testing.T, s *Store, b Branch, from, to time.Time) [][]byte {
	t.Helper()
	var rows [][]byte
	e := s.Search(context.Background(), Query{Branch: b, From: from, To: to}, func(_ uint64, b []byte) error { rows = append(rows, append([]byte(nil), b...)); return nil })
	if e != nil {
		t.Fatal(e)
	}
	return rows
}

func TestOwnershipAndDefinitionConflict(t *testing.T) {
	l := testLayout(t, false)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner", Layout: l}
	s, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	if second, e := Open(c); e == nil {
		second.Close()
		t.Fatal("second writer accepted")
	}
	s.Close()
	c.Tenant = "another"
	if second, e := Open(c); e == nil {
		second.Close()
		t.Fatal("cross-tenant cache accepted")
	}
	c.Tenant = "owner"
	d := l.Definition
	d.Fields[6].Name = "other"
	c.Layout, e = Compile(d)
	if e != nil {
		t.Fatal(e)
	}
	if second, e := Open(c); e == nil {
		second.Close()
		t.Fatal("definition replacement accepted")
	}
}

func TestMaturityAndLateAggregateRejection(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	now := at.Add(10 * time.Minute)
	c := Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner", Layout: l, Now: func() time.Time { return now }}
	s, e := Open(c)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.Append(Write, testFrame(t, l, at, 1, false))
	g, e := s.Summaries(Write, at, at.Add(time.Hour))
	if e != nil || len(g) != 0 {
		t.Fatalf("premature %v %v", g, e)
	}
	now = at.Add(90 * time.Minute)
	g, e = s.Summaries(Write, at, now)
	if e != nil || len(g) != 1 {
		t.Fatalf("mature %v %v", g, e)
	}
	if _, e = s.Append(Write, testFrame(t, l, at.Add(time.Minute), 4, false)); e == nil {
		t.Fatal("late correction accepted")
	}
	g, e = s.Summaries(Write, at, now)
	if e != nil {
		t.Fatal(e)
	}
	var a Aggregate
	for _, v := range g[at.UnixMilli()] {
		a.merge(v)
	}
	if a.Count != 1 || a.Sum != 11 {
		t.Fatalf("late correction %+v", a)
	}
}

func TestFrameValidationAndReuse(t *testing.T) {
	l := testLayout(t, false)
	at := time.Now()
	raw := testFrame(t, l, at, 1, false)
	var f Frame
	if e := l.Scan(raw, &f); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < len(raw); i++ {
		if e := l.Scan(raw[:i], &f); e == nil {
			t.Fatalf("truncation %d accepted", i)
		}
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if e := l.Scan(raw, &f); e != nil {
			panic(e)
		}
	}); allocs != 0 {
		t.Fatalf("scan allocations %f", allocs)
	}
	// The agreed PIL mask/order, little-endian scalars and cumulative offsets.
	d := l.Definition
	d.Fields = []Field{{Name: "event_id", Type: "UUID"}, {Name: "n", Type: "U16"}, {Name: "text", Type: "STRING"}, {Name: "other", Type: "STRING"}}
	d.Resource.Roles = Roles{Event: "event_id"}
	d.Resource.Path = nil
	d.Resource.Aggregations = nil
	l, e := Compile(d)
	if e != nil {
		t.Fatal(e)
	}
	id := uuid(at.UnixMilli(), 1)
	raw, e = l.Encode(nil, []Value{id, Uint(0x1234, 2), Text("hi"), Value{Null, nil}})
	if e != nil {
		t.Fatal(e)
	}
	want := append([]byte{0x2b, 0x48}, id.Bytes...)
	want = append(want, 0x34, 0x12)
	want = append(want, Uint(uint64(at.UnixMilli()), 8).Bytes...)
	want = append(want, 2, 0, 'h', 'i')
	if !bytes.Equal(raw, want) {
		t.Fatalf("wire %x != %x", raw, want)
	}
}

func TestExportRustInterop(t *testing.T) {
	dir := os.Getenv("TRUNK_INTEROP_FIXTURE")
	if dir == "" {
		t.Skip("set TRUNK_INTEROP_FIXTURE to export a cross-language fixture")
	}
	l := testLayout(t, true)
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "resource.rx"), l.Bytes, 0600); e != nil {
		t.Fatal(e)
	}
	raw := append(testFrame(t, l, at, 1, false), testFrame(t, l, at, 2, true)...)
	if e := os.WriteFile(filepath.Join(dir, "frames.pil"), raw, 0600); e != nil {
		t.Fatal(e)
	}
}

// commitLegacyFixture models bytes already produced by the old immediate-run
// writer. It is intentionally not an ingress path: read compatibility with
// interleaved files must survive enforcement of ordered new writes.
func commitLegacyFixture(t *testing.T, s *Store, branch Branch, raw []byte) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	b, e := s.openBranch(branch)
	if e != nil {
		t.Fatal(e)
	}
	var f Frame
	if e = s.layout.Scan(raw, &f); e != nil {
		t.Fatal(e)
	}
	id, e := eventID(f.Values[s.layout.Event])
	if e != nil {
		t.Fatal(e)
	}
	if e = s.accumulate(b, id, f.Values); e != nil {
		t.Fatal(e)
	}
	if e = s.commitBatch(b, []pending{{id, raw}}); e != nil {
		t.Fatal(e)
	}
}
