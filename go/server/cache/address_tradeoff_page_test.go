package cache

import (
	"bytes"
	"hash/crc32"
)

const collectionUnits = 123
const collectionEmpty = 128

type collectionPage [4096]byte

func newComparisonPage() collectionPage {
	var p collectionPage
	copy(p[:4], "OBJ0")
	p[29] = 1
	p[30], p[31] = 2, 3
	for n := 37; n < 160; n++ {
		p[n] = collectionEmpty
	}
	return p
}
func (p *collectionPage) used() int        { return int(p[28]) }
func (p *collectionPage) start(n int) bool { return p[37+n]&128 == 0 }
func (p *collectionPage) at(n int) int     { return int(p[37+n]&127) * 32 }
func (p *collectionPage) key(n int) []byte { a := p.at(n); return p[a : a+16] }

// This benchmark appends hypothetical four-, seven- and eight-byte tuples in key order.
func (p *collectionPage) find(key []byte) int {
	for n := 0; n < p.used(); n++ {
		if p.start(n) && bytes.Equal(key, p.key(n)) {
			return n
		}
	}
	return -1
}
func (p *collectionPage) add(extension bool) {
	n := p.used()
	p[37+n] = byte(n + 5)
	if extension {
		p[37+n] |= 128
	}
	p[28]++
}

func (p *collectionPage) seal() {
	clear(p[24:28])
	order.PutUint32(p[24:28], crc32.Checksum(p[:], checksum))
}
