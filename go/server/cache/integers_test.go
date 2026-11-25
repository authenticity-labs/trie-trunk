package cache

import (
	"bytes"
	"io"
	"testing"
)

func adaptiveLayout(t *testing.T, d Definition, mode string) *Layout {
	t.Helper()
	d.PIL = 1
	d.AdaptiveIntegers = mode
	l, e := Compile(d)
	if e != nil {
		t.Fatal(e)
	}
	return l
}

func TestAdaptiveIntegersMatchOptimizedOracle(t *testing.T) {
	d := compactTestDefinition("U16", "I16", "U32", "I32", "U64", "I64", "STRING")
	for _, mode := range []string{"16", "32"} {
		l := adaptiveLayout(t, d, mode)
		oracle, e := compileSeparateWidthExperiment(l.Definition, mode == "16")
		if e != nil {
			t.Fatal(e)
		}
		var raw, again []byte
		var f Frame
		for u := 0; u < 65536; u++ {
			v := []Value{{}, Uint(uint64(u), 2), Uint(uint64(u), 2), Uint(uint64(u)*65537, 4), Uint(uint64(int64(int16(u))), 4), Uint(uint64(u)<<40, 8), Uint(uint64(int64(int16(u))), 8), Text("λ")}
			if u%3 == 0 {
				v[1] = Value{}
				v[4] = Value{Null, nil}
			}
			v = append(v, Value{}, Value{}, Value{})
			want, e := oracle.encodeReuse(nil, v)
			if e != nil {
				t.Fatal(e)
			}
			raw, e = l.Encode(raw, v)
			if e != nil {
				t.Fatalf("%s %d: %x != %x: %v", mode, u, raw, want, e)
			}
			if e = l.Scan(raw, &f); e != nil {
				t.Fatal(e)
			}
			var expected Frame
			if err := oracle.scan(want, &expected); err != nil {
				t.Fatal(err)
			}
			compactExperimentEqual(t, l, expected.Values, f.Values)
			for i := range v {
				if !l.equal(i, v[i], f.Values[i]) {
					t.Fatalf("value %d mismatch", i)
				}
			}
			again, e = l.Encode(again, f.Values)
			if e != nil || !bytes.Equal(raw, again) {
				t.Fatalf("borrowed reencode mode %s u %d fields %v: %v", mode, u, f.Values, e)
			}
			stream := bytes.NewReader(append(append([]byte{}, raw...), raw...))
			for j := 0; j < 2; j++ {
				got, e := l.ReadFrame(stream, nil)
				if e != nil || !bytes.Equal(got, raw) {
					t.Fatal("stream", e)
				}
			}
			if _, e = l.ReadFrame(stream, nil); e != io.EOF {
				t.Fatal("stream end", e)
			}
		}
	}
}

func TestAdaptiveMalformedAndVersion(t *testing.T) {
	d := compactTestDefinition("U16")
	d.AdaptiveIntegers = "16"
	if _, e := Compile(d); e == nil {
		t.Fatal("old version accepts changed wire layout")
	}
	l := adaptiveLayout(t, compactTestDefinition("U16"), "16")
	for _, raw := range [][]byte{{0x20}, {0x20, 0}, {0x20, 1, 7}, {0x20, 0x80, 7, 0}, {0x30, 0, 7}, {0x21, 0, 7}} {
		var f Frame
		if l.Scan(raw, &f) == nil {
			t.Fatalf("accepted %x", raw)
		}
	}
	raw, e := l.Encode(nil, []Value{{}, Uint(7, 2)})
	if e != nil || !bytes.Equal(raw, []byte{0x48, 0, 0, 7}) {
		t.Fatalf("golden %x %v", raw, e)
	}
	for i := 0; i < len(raw); i++ {
		var f Frame
		if l.Scan(raw[:i], &f) == nil {
			t.Fatal("truncation")
		}
	}
}
