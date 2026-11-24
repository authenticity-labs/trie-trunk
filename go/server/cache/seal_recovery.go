package cache

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// Recover the directory from the existing PAC placement headers and index rows.
// A surviving .pack owns its hour: reuse a complete PAC/index pair, or discard
// an incomplete tail and replay ingress. The pack remains until recovery finishes.
func (s *Store) recoverSealed(b *branchStore) error {
	base, err := retiredBytes(b.idx)
	if err != nil {
		return err
	}
	if base%IndexRowSize != 0 {
		return fmt.Errorf("invalid retired index boundary")
	}
	b.baseRows = base / IndexRowSize
	if b.baseRows != s.catalog.RetiredRows[s.binding][b.branch] {
		return fmt.Errorf("index retirement boundary mismatch")
	}
	if raw, err := os.ReadFile(filepath.Join(s.dir, s.prefix(b.branch)+".time")); err == nil {
		if prior, e := parseTimeDirectory(raw, b.generation); e == nil {
			b.timeAnchor = &TimeAnchor{First: prior.first(), Origin: prior.origin()}
			b.finalizedBefore = prior.sealed()
		}
	}
	if b.baseRows > 0 && b.timeAnchor == nil {
		return fmt.Errorf("retired timeline anchor cannot be recovered from surviving PACs")
	}
	rollback := int64(1<<63 - 1)
	for hour := range b.stagingPacks {
		rollback = min(rollback, hour)
	}
	b.finalizedBefore = min(b.finalizedBefore, rollback)
	if b.timeAnchor != nil && rollback != int64(1<<63-1) {
		b.timeAnchor.First = min(b.timeAnchor.First, rollback)
		b.timeAnchor.Origin = min(b.timeAnchor.Origin, rollback)
	}
	stat, err := b.packs.Stat()
	if err != nil {
		return err
	}
	if b.baseRows > 0 && stat.Size() == headerSize {
		return fmt.Errorf("fully retired branch has no surviving PAC placement coordinates")
	}
	at := uint64(headerSize)
	tail := b.baseRows
	for at < uint64(stat.Size()) {
		// Less than one length word is an interrupted append, not a PAC.
		if uint64(stat.Size())-at < 4 {
			break
		}
		run, err := readStoredRun(s.layout, b.idx, b.packs, b.generation, b.baseRows, b.packsRetired, at)
		if err != nil {
			if rollback != int64(1<<63-1) && pendingIndexTail(b, tail, rollback) {
				break
			}
			return fmt.Errorf("sealed PAC at %d: %w", at, err)
		}
		if run.Start != tail+IndexHeaderSlots {
			return fmt.Errorf("noncontiguous sealed index")
		}
		if len(b.runs) == 0 {
			b.blockBase = run.PAC.Placement.Block
			b.memberEnd = run.PAC.Placement.MemberStart - 4 - 4*(run.End-run.Start)
		} else {
			last := b.runs[len(b.runs)-1]
			if run.PAC.Placement.From < last.PAC.Placement.To || run.PAC.Placement.Block != last.PAC.Placement.Block+1 || run.PAC.Placement.MemberStart != last.PAC.Placement.MemberStart+last.PAC.Placement.MemberBytes+4+4*(run.End-run.Start) {
				return fmt.Errorf("noncontiguous PAC placement")
			}
		}
		run.TrieSize = s.trieSizes[run.PAC.Placement.Trie]
		if run.TrieSize == 0 {
			return fmt.Errorf("sealed PAC references an unknown state trie prefix")
		}
		for ref := range run.References {
			if int(ref) >= run.TrieSize {
				return fmt.Errorf("index leaf outside PAC state trie prefix")
			}
		}
		b.runs = append(b.runs, run)
		b.finalizedBefore = max(b.finalizedBefore, min(rollback, run.PAC.Placement.To))
		tail = run.End
		at = run.PacksEnd - b.packsRetired
	}
	b.packsEnd = at + b.packsRetired
	if err = b.idx.Truncate(headerSize + int64(tail-b.baseRows)*IndexRowSize); err != nil {
		return err
	}
	if err = b.packs.Truncate(int64(at)); err != nil {
		return err
	}
	if err = b.idx.Sync(); err != nil {
		return err
	}
	return b.packs.Sync()
}

func readStoredRun(l *Layout, idx, packs *os.File, source [16]byte, baseRows, retired, at uint64) (Run, error) {
	var run Run
	start, end, err := storedSegmentRange(packs, retired, at+retired)
	if err != nil {
		return run, err
	}
	raw, err := readBytes(packs, start-retired, end-retired)
	if err != nil {
		return run, err
	}
	decoded, err := expandPAC(raw)
	if err != nil {
		return run, err
	}
	p, err := ParsePAC(l, decoded, map[uint32]bool{PACNative: true})
	if err != nil {
		return run, err
	}
	if order.Uint32(raw[4:8])&PACGzip == 0 && len(p.Headers[pacChecksumHeader]) != 4 {
		return run, fmt.Errorf("stored uncompressed PAC requires crc32c")
	}
	var place PACPlacement
	found := false
	for _, ext := range p.Extensions {
		if ext.Type == PACNative {
			if found {
				return run, fmt.Errorf("duplicate PAC placement")
			}
			place, err = ParsePACPlacement(ext)
			if err != nil {
				return run, err
			}
			found = true
		}
	}
	if !found || place.Rows == 0 || place.Rows != uint64(len(p.Members)) || place.MemberStart < 4+4*place.Rows || place.FirstRow < baseRows+IndexHeaderSlots || place.FirstRow+place.Rows > IndexCapacity {
		return run, fmt.Errorf("invalid stored PAC placement")
	}
	rows, err := readBytes(idx, headerSize+(place.FirstRow-baseRows)*IndexRowSize, headerSize+(place.FirstRow+place.Rows-baseRows)*IndexRowSize)
	if err != nil {
		return run, err
	}
	refs := map[uint16]uint64{}
	for n := 0; n < len(rows); n += IndexRowSize {
		refs[order.Uint16(rows[n+4:n+6])]++
	}
	run = Run{Start: place.FirstRow, End: place.FirstRow + place.Rows, PacksStart: start, PacksEnd: end, MinMS: indexTime(rows, place.From), MaxMS: indexTime(rows[len(rows)-IndexRowSize:], place.From), References: refs, IndexCRC: crc32.Checksum(rows, checksum), PacksCRC: crc32.Checksum(raw, checksum), PAC: &StoredPAC{Placement: place, Decoded: uint64(len(decoded)), Compressed: order.Uint32(raw[4:8])&PACGzip != 0}}
	head, err := readBytes(idx, headerSize+(run.Start-IndexHeaderSlots-baseRows)*IndexRowSize, headerSize+(run.Start-baseRows)*IndexRowSize)
	if err != nil {
		return run, err
	}
	if err = validateIndexHeader(head, run); err != nil {
		return run, err
	}
	if err = validateBlock(run, rows, l.RowLayout(), source); err != nil {
		return run, err
	}
	if _, err = PACLocalRows(p, place, rows); err != nil {
		return run, err
	}
	return run, nil
}

// Never use a later recovery pack to discard damage in an older sealed hour.
func pendingIndexTail(b *branchStore, tail uint64, hour int64) bool {
	at := int64(headerSize + (tail-b.baseRows)*IndexRowSize)
	info, err := b.idx.Stat()
	if err != nil {
		return false
	}
	if info.Size() < at+2*IndexRowSize {
		return true
	}
	var raw [2 * IndexRowSize]byte
	if _, err = b.idx.ReadAt(raw[:], at); err != nil {
		return false
	}
	ms := indexTime(raw[IndexRowSize:], hour)
	return order.Uint32(raw[:4]) == uint32(uint64(hour)>>32) && ms >= hour && ms < hour+TimeHourMS
}

func (s *Store) recoverHourSummary(b *branchStore, run Run, f *os.File) (Summary, error) {
	scratch := &branchStore{branch: b.branch, sum: b.sum}
	first := true
	count := uint64(0)
	err := visitRecords(f, MaxFrame, s.memory, func(raw []byte, _ *nativeBuffer) error {
		if first {
			first = false
			return nil
		}
		if len(raw) == 8 {
			return nil
		}
		var frame Frame
		if err := s.layout.Scan(raw, &frame); err != nil {
			return err
		}
		id, err := s.layout.orderingKey(frame.Values)
		if err != nil {
			return err
		}
		if eventTime(id[:]) < run.PAC.Placement.From || eventTime(id[:]) >= run.PAC.Placement.To {
			return fmt.Errorf("summary ingress outside sealed hour")
		}
		count++
		return s.accumulate(scratch, id, frame.Values)
	})
	if err != nil {
		return Summary{}, err
	}
	if count != run.End-run.Start {
		return Summary{}, fmt.Errorf("summary ingress count differs from PAC")
	}
	return scratch.accumulators.get(run.PAC.Placement.From).snapshot(run, s.layout, &s.trie)
}
