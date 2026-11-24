package cache

import (
	"fmt"
	"hash/crc32"

	pil "github.com/authenticity-labs/pill-packer/go/codec"
)

// crc32c is a Trie-Trunk application header, not part of the PIL/PAC format.
// It covers the entire plain PAC with its four value bytes treated as zero.
const pacChecksumHeader = "crc32c"

// Storage encoding determines integrity policy, independently of branch names
// or resource strategies. The caller retains ownership of the single allocation.
func marshalStoredPAC(l *Layout, p *PAC, compressed bool, allocate func(int) ([]byte, error)) ([]byte, error) {
	if compressed {
		return p.MarshalAllocated(allocate)
	}
	if p.Headers == nil {
		p.Headers = make(map[string][]byte)
	}
	p.Headers[pacChecksumHeader] = make([]byte, 4)
	raw, err := p.MarshalAllocated(allocate)
	if err != nil {
		return nil, err
	}
	// Bind the newly marshaled bytes without copying their payload into Go's heap.
	bound, err := pil.ParsePAC(l.Layout, raw, map[uint32]bool{PACNative: true})
	if err != nil {
		return nil, err
	}
	crc, err := pacChecksum(bound)
	if err != nil {
		return nil, err
	}
	order.PutUint32(bound.Headers[pacChecksumHeader], crc)
	return raw, nil
}

func pacChecksum(p *PAC) (uint32, error) {
	value := p.Headers[pacChecksumHeader]
	if len(value) != 4 {
		return 0, fmt.Errorf("invalid PAC crc32c header")
	}
	// ParsePAC borrows header values directly from Bytes without limiting their
	// capacity. Locate that slice without copying or modifying shared PAC bytes.
	at := cap(p.Bytes) - cap(value)
	if at < PACHeaderSize || at > len(p.Bytes)-4 || &p.Bytes[at] != &value[0] {
		return 0, fmt.Errorf("PAC crc32c header is not a borrowed byte range")
	}
	var zero [4]byte
	crc := crc32.Update(0, checksum, p.Bytes[:at])
	crc = crc32.Update(crc, checksum, zero[:])
	return crc32.Update(crc, checksum, p.Bytes[at+4:]), nil
}

func verifyPACChecksum(p *PAC) error {
	want, err := pacChecksum(p)
	if err != nil {
		return err
	}
	if order.Uint32(p.Headers[pacChecksumHeader]) != want {
		return fmt.Errorf("PAC crc32c mismatch")
	}
	return nil
}
