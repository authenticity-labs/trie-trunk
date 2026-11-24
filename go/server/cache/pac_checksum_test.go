package cache

import (
	"bytes"
	"strings"
	"testing"
	"time"

	pil "github.com/authenticity-labs/pill-packer/go/codec"
)

func TestPACChecksumFollowsStorageEncoding(t *testing.T) {
	l := testLayout(t, false)
	frame := testFrame(t, l, time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC), 1, false)
	for _, compressed := range []bool{false, true} {
		p, err := factorPAC(l, [][]byte{frame}, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		p.Headers = map[string][]byte{"application": []byte("preserved")}
		allocations := 0
		var allocation []byte
		raw, err := marshalStoredPAC(l, p, compressed, func(n int) ([]byte, error) {
			allocations++
			allocation = make([]byte, n)
			return allocation, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if allocations != 1 || &raw[0] != &allocation[0] {
			t.Fatal("PAC payload was copied or allocated more than once")
		}
		bound, err := ParsePAC(l, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, hasChecksum := bound.Headers[pacChecksumHeader]
		if hasChecksum == compressed || string(bound.Headers["application"]) != "preserved" {
			t.Fatal("storage encoding did not determine checksum policy or headers lost")
		}
	}
}

func TestStoredPACApplicationChecksum(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	cfg := Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return at.Add(3 * time.Hour) }}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, branch := range []Branch{Write, Read} {
		if _, err := s.Append(branch, testFrame(t, l, at, 1, false)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, branch := range []Branch{Write, Read} {
		b := s.branches[branch]
		run := b.runs[0]
		raw, err := readBytes(b.packs, run.PacksStart, run.PacksEnd)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := expandPAC(raw)
		if err != nil {
			t.Fatal(err)
		}
		p, err := ParsePAC(l, plain, map[uint32]bool{PACNative: true})
		if err != nil {
			t.Fatal(err)
		}
		if branch == Read {
			if _, ok := p.Headers[pacChecksumHeader]; ok {
				t.Fatal("compressed PAC gained an application checksum")
			}
			continue
		}
		if len(p.Headers[pacChecksumHeader]) != 4 {
			t.Fatal("uncompressed PAC missing application checksum")
		}
		before := bytes.Clone(plain)
		if err := verifyPACChecksum(p); err != nil || !bytes.Equal(before, plain) {
			t.Fatal("verification mutated shared bytes", err)
		}
		for _, part := range []string{"payload", "placement", "checksum"} {
			t.Run(part, func(t *testing.T) {
				// Extra capacity also exercises borrowed views into a larger allocation.
				bad := make([]byte, len(plain), len(plain)+32)
				copy(bad, plain)
				view, err := pil.ParsePAC(l.Layout, bad, map[uint32]bool{PACNative: true})
				if err != nil {
					t.Fatal(err)
				}
				switch part {
				case "payload":
					bad[len(bad)-1] ^= 1
				case "placement":
					for _, e := range view.Extensions {
						if e.Type == PACNative {
							e.Data[16] ^= 1
						}
					}
				case "checksum":
					view.Headers[pacChecksumHeader][0] ^= 1
				}
				if _, err := pil.ParsePAC(l.Layout, bad, map[uint32]bool{PACNative: true}); err != nil {
					t.Fatal("test must retain a structurally valid PAC", err)
				}
				if _, err := ParsePAC(l, bad, map[uint32]bool{PACNative: true}); err == nil || !strings.Contains(err.Error(), "crc32c mismatch") {
					t.Fatal("corruption escaped application checksum", err)
				}
			})
		}
		delete(p.Headers, pacChecksumHeader)
		without, err := p.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		run.PAC.Decoded = uint64(len(without))
		if _, err := decodeStoredPAC(l, run, without); err == nil || !strings.Contains(err.Error(), "requires crc32c") {
			t.Fatal("stored uncompressed PAC accepted a missing checksum", err)
		}
	}
}
