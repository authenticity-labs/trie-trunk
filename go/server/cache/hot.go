package cache

import (
	"time"
)

const HotWindow = 24 * time.Hour

// ProvisionalRow marks a live UUID without a permanent disk reference.
const ProvisionalRow = ^uint64(0)

// Rebuild the resident window from durable files on restart. Ordinary time
// scans still use the sequential reader outside this window.
func (s *Store) warmHot(b *branchStore) error {
	now := s.config.Now()
	var reader packsReader
	a := frameAssembler{layout: s.layout, trie: &s.trie}
	for _, run := range b.runs {
		if run.MaxMS < now.Add(-HotWindow).UnixMilli() {
			continue
		}
		owner := s.memory.buffer()
		raw, e := owner.resizePAC(int(run.PacksEnd - run.PacksStart))
		if e == nil {
			_, e = b.packs.ReadAt(raw, int64(run.PacksStart-b.packsRetired))
		}
		if e == nil {
			e = s.rememberPackOwned(b, run, owner)
		}
		owner.close()
		if e != nil {
			return e
		}
		span := cursor{b.lower(run.Start, run.End, now.Add(-HotWindow).UnixMilli()), run.End}
		for row := span.at; row < span.end; row++ {
			var full []byte
			if b.branch == Write && s.config.ObjectIndex && s.layout.Object >= 0 {
				raw, e := reader.read(b, row, s.row)
				if e != nil {
					return e
				}
				full, e = a.assemble(b.assemblyRow(row), raw, nil, b.rowLeaf(row))
				if e != nil {
					return e
				}
			}
			var id [16]byte
			identity, e := b.rowID(row)
			if e != nil {
				return e
			}
			copy(id[:], identity)
			if e = s.rememberSealed(b, id, full, row); e != nil {
				return e
			}
		}
	}
	return nil
}
