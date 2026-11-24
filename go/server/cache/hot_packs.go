package cache

import (
	"fmt"
	"time"
)

// Keep the serialized PAC in its own immutable hour arena, separate from ingress
// PILs. Replication borrows these bytes; it never rebuilds a sealed pack.
func (s *Store) rememberPack(b *branchStore, run Run, raw []byte) error {
	if run.MaxMS < s.config.Now().Add(-HotWindow).UnixMilli() {
		return nil
	}
	if uint64(len(raw)) != run.PacksEnd-run.PacksStart {
		return fmt.Errorf("resident PAC extent mismatch")
	}
	if b.sealedMemory == nil {
		b.sealedMemory = map[int64]*nativeArena{}
	}
	hour := run.PAC.Placement.From / TimeHourMS
	arena := &nativeArena{ptr: newArena(), packs: map[uint64][][]byte{}}
	var parts [][]byte
	for len(raw) > 0 {
		n := min(len(raw), MaxFrame)
		buffer := s.memory.buffer()
		owned, e := buffer.resize(n)
		if e != nil {
			buffer.close()
			arena.close()
			return e
		}
		copy(owned, raw[:n])
		if e = arena.adopt(buffer); e != nil {
			buffer.close()
			arena.close()
			return e
		}
		parts = append(parts, owned)
		buffer.close()
		raw = raw[n:]
	}
	if prior := b.sealedMemory[hour]; prior != nil {
		prior.close()
	}
	b.sealedMemory[hour] = arena
	arena.packs[run.PacksStart] = parts
	return nil
}

// Caller owns a retained publication or holds the writer lock. A zero clock
// reads the retained snapshot without applying a newer eviction cutoff.
func (b *branchStore) residentPack(run Run, now time.Time) [][]byte {
	if !now.IsZero() && run.MaxMS < now.Add(-HotWindow).UnixMilli() {
		return nil
	}
	if a := b.sealedMemory[run.PAC.Placement.From/TimeHourMS]; a != nil {
		return a.packs[run.PacksStart]
	}
	return nil
}

// Sealing transfers the exact Rust allocation into the immutable resident hour.
// No Go heap body and no second native copy are introduced during publication.
func (s *Store) rememberPackOwned(b *branchStore, run Run, owner *nativeBuffer) error {
	if run.MaxMS < s.config.Now().Add(-HotWindow).UnixMilli() {
		return nil
	}
	if owner == nil || uint64(len(owner.bytes)) != run.PacksEnd-run.PacksStart {
		return fmt.Errorf("resident PAC ownership mismatch")
	}
	if b.sealedMemory == nil {
		b.sealedMemory = map[int64]*nativeArena{}
	}
	hour := run.PAC.Placement.From / TimeHourMS
	arena := &nativeArena{ptr: newArena(), packs: map[uint64][][]byte{run.PacksStart: {owner.bytes}}}
	if err := arena.adopt(owner); err != nil {
		arena.close()
		return err
	}
	if prior := b.sealedMemory[hour]; prior != nil {
		prior.close()
	}
	b.sealedMemory[hour] = arena
	return nil
}
