package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	pil "github.com/authenticity-labs/pill-packer/go"
)

func TestSealedViewControlParsing(t *testing.T) {
	raw := append([]byte{'T', 'P', 'I', 'L', 2}, 1, 0xfc)
	for _, n := range []uint16{32, 34, 36} {
		raw = binary.LittleEndian.AppendUint16(raw, n)
	}
	raw = append(raw, make([]byte, 36)...)
	raw = append(raw, 2, 0xbc)
	raw = binary.LittleEndian.AppendUint32(raw, 7)
	raw = binary.LittleEndian.AppendUint16(raw, 32)
	raw = binary.LittleEndian.AppendUint16(raw, 36)
	raw = append(raw, bytes.Repeat([]byte{1}, 32)...)
	raw = append(raw, "demo"...)
	meta := []byte(`{"snapshot":"stable","page":[{"stream":7,"key":"event","native":{"from":1,"to":2}}]}`)
	raw = append(raw, 8, 0xc0)
	raw = binary.LittleEndian.AppendUint16(raw, uint16(len(meta)))
	raw = append(raw, meta...)
	raw = append(raw, 255, 254)
	v, b, e := parse(raw)
	if e != nil {
		t.Fatal(e)
	}
	if len(v.Page) != 1 || v.Page[0].Stream != 7 || b[7].Source != "demo" || len(b[7].Definition) != 64 {
		t.Fatalf("wrong view/binding: %+v %+v", v, b)
	}
	for n := 0; n < len(raw); n++ {
		if _, _, e := parse(raw[:n]); e == nil {
			t.Fatalf("accepted truncated stream at %d", n)
		}
	}
	if _, _, e := parse(append(bytes.Clone(raw), 0)); e == nil {
		t.Fatal("accepted trailing bytes")
	}
}

func TestLargeViewControlAndUnknownPayload(t *testing.T) {
	meta, _ := json.Marshal(map[string]any{"snapshot": string(bytes.Repeat([]byte{'a'}, 70000)), "page": []any{}})
	raw := []byte{'T', 'P', 'I', 'L', 2, 8, 0xc0, 255, 255}
	raw = binary.LittleEndian.AppendUint32(raw, uint32(len(meta)))
	raw = append(raw, meta...)
	raw = append(raw, 255, 254)
	if _, _, e := parse(raw); e != nil {
		t.Fatal(e)
	}
	if _, _, e := parse([]byte{'T', 'P', 'I', 'L', 2, 7, 0, 255, 254}); e == nil {
		t.Fatal("accepted unmeasured live payload")
	}
}

func TestCollectedPacksUseRustExtents(t *testing.T) {
	d, err := pil.FromJSON([]byte(`{"pil":0,"definition":"size","version":0,"fields":[{"name":"n","type":"U16"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	pack, err := d.Pack([][]byte{{0xc0, 42, 0}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	flags, err := pil.CompressionFlags("zstd", 1)
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := pil.SealOwned(pack, flags, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	body := append(bytes.Clone(pack), compressed.Bytes()...)
	packs, err := splitPacks(body)
	if err != nil || len(packs) != 2 || !bytes.Equal(packs[0], pack) || !bytes.Equal(packs[1], compressed.Bytes()) {
		t.Fatal("incorrect PAC boundaries", err)
	}
	for end := 1; end < len(body); end++ {
		if end == len(pack) {
			continue
		}
		if _, err := splitPacks(body[:end]); err == nil {
			t.Fatalf("accepted truncation at %d", end)
		}
	}
}
