package cache

import (
	"bytes"
	"fmt"
	"time"
)

// A query owns one bounded decoded PAC. Native replication bypasses this reader.
type packsReader struct {
	context    recordContext
	branch     *branchStore
	pac        *PAC
	block      uint32
	start, end uint64
	data       []byte
	reads      uint64
	point      bool // Object lookups read exact extents; sequential scans keep read-ahead.
}

func (r *packsReader) read(b *branchStore, number uint64, row RowLayout) ([]byte, error) {
	if number < b.baseRows || number >= b.tail() {
		return nil, fmt.Errorf("payload row outside index")
	}
	run := r.context.segment(b, number)
	if number < run.Start {
		return nil, fmt.Errorf("reference points to segment header")
	}
	block := run.PAC.Placement.Block
	if r.branch != b || r.pac == nil || r.block != block {
		var raw []byte
		var err error
		if parts := b.residentPack(*run, time.Time{}); len(parts) > 0 {
			if len(parts) == 1 {
				raw = parts[0]
			} else {
				raw = bytes.Join(parts, nil)
			}
		} else {
			start, end, e := b.packsRange(number, row)
			if e != nil {
				return nil, e
			}
			raw, err = readBytes(b.packs, start-b.packsRetired, end-b.packsRetired)
		}
		if err != nil {
			return nil, err
		}
		p, err := decodeStoredPAC(b.layout, b.runs[block-b.blockBase], raw)
		if err != nil {
			return nil, err
		}
		r.branch, r.block, r.pac = b, block, p
		r.reads++
	}
	member := r.pac.Members[number-b.runs[block-b.blockBase].Start]
	if len(r.pac.Common) == 0 {
		return member, nil
	}
	return ResolvePAC(b.layout, r.pac.Common, member)
}
