package cache

import (
	"bytes"
	"encoding/json"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"io"
	"testing"
	"time"
)

type viewDecoded struct {
	result      ViewResult
	definitions int
	frames      [][]byte
	bindings    map[uint32]string
}

func decodeView(t *testing.T, l *Layout, raw []byte) (d viewDecoded) {
	t.Helper()
	d.bindings = map[uint32]string{}
	if !bytes.HasPrefix(raw, []byte{'T', 'P', 'I', 'L', 2}) {
		t.Fatalf("invalid response: %q", raw)
	}
	r := bytes.NewReader(raw[5:])
	control := &pil.Layout{Widths: []int{0}, Definition: pil.Definition{Fields: []Field{{Name: "metadata", Type: "BYTES"}}}}
	for {
		tag, e := r.ReadByte()
		if e != nil {
			t.Fatal(e)
		}
		switch tag {
		case 1:
			d.definitions++
			_, e = (&pil.Layout{Widths: []int{0, 0, 0}}).ReadFrame(r, nil)
		case 2:
			cl := nativeControl("U32", "BYTES", "STRING")
			var raw []byte
			raw, e = cl.ReadFrame(r, nil)
			if e == nil {
				var f Frame
				e = cl.Scan(raw, &f)
				if e == nil {
					d.bindings[uint32(unsigned(f.Values[0].Bytes))] = string(f.Values[2].Bytes)
				}
			}
		case 8:
			var f []byte
			f, e = control.ReadFrame(r, nil)
			if e == nil {
				var v Frame
				e = control.Scan(f, &v)
				if e == nil {
					e = json.Unmarshal(v.Values[0].Bytes, &d.result)
				}
			}
		case 7:
			var header [PACHeaderSize]byte
			if _, err := io.ReadFull(r, header[:]); err != nil {
				t.Fatal(err)
			}
			size := uint64(order.Uint32(header[:4]))
			if size < PACHeaderSize || size > PACMaximum {
				t.Fatal("PAC size")
			}
			raw := make([]byte, size)
			copy(raw, header[:])
			if _, err := io.ReadFull(r, raw[PACHeaderSize:]); err != nil {
				t.Fatal(err)
			}
			pac, err := ParsePAC(l, raw, map[uint32]bool{PACSource: true})
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range pac.Members {
				full, err := ResolvePAC(l, pac.Common, member)
				if err != nil {
					t.Fatal(err)
				}
				d.frames = append(d.frames, full)
			}

		case 255:
			done, _ := r.ReadByte()
			if done != 254 || r.Len() != 0 {
				t.Fatal("incomplete response")
			}
			return
		default:
			t.Fatalf("unknown tag %d", tag)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
}

func readViewEUI(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, e := io.ReadFull(r, b[:2]); e != nil {
		return 0, e
	}
	n := uint64(order.Uint16(b[:2]))
	if n < 65535 {
		return n, nil
	}
	if _, e := io.ReadFull(r, b[:4]); e != nil {
		return 0, e
	}
	n = uint64(order.Uint32(b[:4]))
	if n < 0xffffffff {
		return n, nil
	}
	_, e := io.ReadFull(r, b[:])
	return order.Uint64(b[:]), e
}

func TestPagedViewCursorIsolationAndExpiry(t *testing.T) {
	c := viewCursor{Expires: time.Now().Add(time.Minute).Unix()}
	raw, e := viewSign(c, "a")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = viewParse(raw, "b"); e == nil {
		t.Fatal("cross credential cursor")
	}
	c.Expires = time.Now().Add(-time.Minute).Unix()
	raw, _ = viewSign(c, "a")
	if _, e = viewParse(raw, "a"); e == nil {
		t.Fatal("expired cursor")
	}
}

func TestPACArrayExactFraming(t *testing.T) {
	l := testLayout(t, false)
	frames := [][]byte{testFrame(t, l, time.UnixMilli(1), 1, false), testFrame(t, l, time.UnixMilli(2), 2, false)}
	var b bytes.Buffer
	if err := writePAC(&b, l, "demo/payment/transactions.v1/write", frames); err != nil {
		t.Fatal(err)
	}
	if b.Bytes()[0] != 7 {
		t.Fatal("PAC stream tag")
	}
	p, err := ParsePAC(l, b.Bytes()[1:], map[uint32]bool{PACSource: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Common) == 0 || len(p.Members) != 2 {
		t.Fatal("prefix packing missing")
	}
	for i, m := range p.Members {
		full, err := ResolvePAC(l, p.Common, m)
		if err != nil || !bytes.Equal(full, frames[i]) {
			t.Fatal("round trip", err)
		}
	}
}
