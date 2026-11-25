package cache

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"io"
	"testing"
)

func nativeControl(types ...string) *Layout {
	l := &Layout{Layout: &pil.Layout{Identity: pil.IdentityProfile{Primary: -1}}}
	for i, kind := range types {
		w, _ := width(kind)
		l.Widths = append(l.Widths, w)
		l.Definition.Fields = append(l.Definition.Fields, Field{Name: fmt.Sprint(i), Type: kind})
	}
	l.Layout.Definition.Fields = l.Definition.Fields
	return l
}

func readNativeTest(t *testing.T, raw []byte) ([]Manifest, []NativeChunk) {
	t.Helper()
	if bytes.HasPrefix(raw, []byte{31, 139}) {
		z, e := gzip.NewReader(bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		raw, e = io.ReadAll(z)
		z.Close()
		if e != nil {
			t.Fatal(e)
		}
	}
	if !bytes.HasPrefix(raw, []byte{'T', 'P', 'I', 'L', 2}) {
		t.Fatal("preamble")
	}
	r := bytes.NewReader(raw[5:])
	var manifests []Manifest
	var chunks []NativeChunk
	open := false
	var rangeLeft uint64
	var e error
	for {
		tag := byte(7)
		if rangeLeft == 0 {
			var e error
			tag, e = r.ReadByte()
			if e != nil {
				t.Fatal(e)
			}
		}
		if tag == 9 {
			var h [12]byte
			if _, e = io.ReadFull(r, h[:]); e != nil {
				t.Fatal(e)
			}
			rangeLeft = order.Uint64(h[4:])
			if !open || rangeLeft == 0 || rangeLeft > uint64(r.Len()) {
				t.Fatal("invalid PAC range")
			}
			continue
		}
		if tag == 255 {
			last, _ := r.ReadByte()
			if last != 254 || r.Len() != 0 || open {
				t.Fatal("incomplete native stream")
			}
			break
		}
		if tag == 7 {
			h := make([]byte, 4)
			if _, e = io.ReadFull(r, h); e != nil {
				t.Fatal(e)
			}
			n := uint64(order.Uint32(h[:4]))
			if n < PACCompressedHeaderSize || n > PACMaximum {
				t.Fatal("PAC length")
			}
			data := make([]byte, n)
			copy(data, h)
			if _, e = io.ReadFull(r, data[4:]); e != nil {
				t.Fatal(e)
			}
			if rangeLeft > 0 {
				if n > rangeLeft {
					t.Fatal("PAC exceeds range")
				}
				rangeLeft -= n
			}
			chunks = append(chunks, NativeChunk{Kind: "pac", Data: data})
			continue
		}
		var l *Layout
		switch tag {
		case 1:
			l = nativeControl("BYTES", "BYTES", "BYTES")
		case 2:
			l = nativeControl("U32", "BYTES", "STRING")
		case 4:
			l = nativeControl("U32", "OBJECT")
		case 5:
			l = nativeControl("U32", "U8", "U64", "BYTES", "BYTES")
		case 6:
			l = nativeControl("U32")
		default:
			t.Fatal("unknown tag", tag)
		}
		b, e := l.ReadFrame(r, nil)
		if e != nil {
			t.Fatal(e)
		}
		var f Frame
		if e = l.Scan(b, &f); e != nil {
			t.Fatal(tag, e)
		}
		switch tag {
		case 4:
			if open {
				t.Fatal("overlapping packages")
			}
			open = true
			var m Manifest
			if e = decoder.Unmarshal(f.Values[1].Bytes, &m); e != nil {
				t.Fatal(e)
			}
			manifests = append(manifests, m)
		case 5:
			if !open {
				t.Fatal("chunk outside package")
			}
			kinds := []string{"map", "trie", "state", "index"}
			kind := int(f.Values[1].Bytes[0])
			if kind >= len(kinds) {
				t.Fatal("kind")
			}
			var digest [32]byte
			copy(digest[:], f.Values[3].Bytes)
			data := f.Values[4].Bytes
			if sha256.Sum256(data) != digest {
				t.Fatal("chunk checksum")
			}
			chunks = append(chunks, NativeChunk{Kind: kinds[kind], Start: unsigned(f.Values[2].Bytes), Data: append([]byte(nil), data...), Digest: digest})
		case 6:
			if !open {
				t.Fatal("unexpected completion")
			}
			open = false
		}
	}
	return manifests, chunks
}

func ungzipTest(t testing.TB, raw []byte) []byte {
	t.Helper()
	if bytes.HasPrefix(raw, []byte("TPIL")) {
		return raw
	}
	z, e := gzip.NewReader(bytes.NewReader(raw))
	if e != nil {
		t.Fatal(e)
	}
	out, e := io.ReadAll(z)
	z.Close()
	if e != nil {
		t.Fatal(e)
	}
	return out
}
