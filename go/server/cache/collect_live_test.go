package cache

import (
	"bytes"
	"testing"
)

func collectionLivePacks(t testing.TB, raw []byte, l *Layout) []*PAC {
	t.Helper()
	if len(raw) < 7 || !bytes.Equal(raw[:5], []byte{'T', 'P', 'I', 'L', 2}) {
		t.Fatal("invalid collection stream")
	}
	var packs []*PAC
	at := 5
	for at < len(raw) {
		tag := raw[at]
		at++
		if tag == 255 {
			if at != len(raw)-1 || raw[at] != 254 {
				t.Fatal("incomplete collection")
			}
			return packs
		}
		if tag == 7 {
			size := int(order.Uint32(raw[at:]))
			p, e := ParsePAC(l, raw[at:at+size], map[uint32]bool{PACSource: true})
			if e != nil {
				t.Fatal(e)
			}
			packs = append(packs, p)
			at += size
			continue
		}
		if tag == 9 {
			size := int(order.Uint64(raw[at+4:]))
			at += 12 + size
			continue
		}
		var width []int
		switch tag {
		case 1:
			width = defineControl.definition().Widths
		case 2:
			width = bindControl.definition().Widths
		case 4:
			width = manifestControl.definition().Widths
		case 5:
			width = chunkControl.definition().Widths
		case 6:
			width = endControl.definition().Widths
		case 11:
			width = liveWindowControl.definition().Widths
		default:
			t.Fatalf("unexpected tag %d", tag)
		}
		scanner := *defineControl.definition()
		scanner.Widths = width
		frame, e := scanner.ReadFrame(bytes.NewReader(raw[at:]), nil)
		if e != nil {
			t.Fatal(e)
		}
		at += len(frame)
	}
	t.Fatal("missing completion")
	return nil
}
