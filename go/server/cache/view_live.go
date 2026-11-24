package cache

import (
	"fmt"
	"time"
)

// Live views pin immutable frame references, not disk rows. Continuations keep
// the original pending set even if it is finalized or new late events arrive.
// A bounded, expiring registry prevents abandoned cursors from retaining frames
// indefinitely. Eviction is an explicit restart, never a drifting page.
type liveViewSnapshot struct {
	release func()
	events  []pending
	expires time.Time
}

const maxLiveViews = 64

func (b *branchStore) captureLive(from, to time.Time, prior *viewBinding) (string, []pending, error) {
	now := time.Now()
	for key, v := range b.liveViews {
		if !now.Before(v.expires) {
			if v.release != nil {
				v.release()
			}
			delete(b.liveViews, key)
		}
	}
	if prior != nil {
		if prior.Live == "" {
			return "", nil, nil
		}
		v, ok := b.liveViews[prior.Live]
		if !ok {
			return "", nil, fmt.Errorf("live snapshot expired; restart search")
		}
		return prior.Live, v.events, nil
	}
	values := b.pendingWindow(nil, from, to)
	if len(values) == 0 {
		return "", nil, nil
	}
	key := viewDigest(struct {
		Version  uint64
		From, To int64
		Count    int
	}{b.pendingVersion, from.UnixNano(), to.UnixNano(), len(values)})
	if v, ok := b.liveViews[key]; ok {
		v.expires = now.Add(15 * time.Minute)
		b.liveViews[key] = v
		return key, v.events, nil
	}
	if b.liveViews == nil {
		b.liveViews = map[string]liveViewSnapshot{}
	}
	if len(b.liveViews) >= maxLiveViews {
		oldest := ""
		var expiry time.Time
		for k, v := range b.liveViews {
			if oldest == "" || v.expires.Before(expiry) {
				oldest = k
				expiry = v.expires
			}
		}
		if v := b.liveViews[oldest]; v.release != nil {
			v.release()
		}
		delete(b.liveViews, oldest)
	}
	values = append([]pending(nil), values...)
	b.liveViews[key] = liveViewSnapshot{events: values, expires: now.Add(15 * time.Minute), release: b.pinMemory()}
	return key, values, nil
}
func (s *viewSource) orderingKey(row uint64) []byte {
	if row >= s.binding.Tail {
		return timeKey(eventTime(s.pending[row-s.binding.Tail].id[:]))
	}
	return s.snap.assemblyRow(row)
}
func (s *viewSource) fullFrame(row uint64) ([]byte, error) {
	if row >= s.binding.Tail {
		return s.pending[row-s.binding.Tail].frame, nil
	}
	if raw := s.resident(row); raw != nil {
		return raw, nil
	}
	raw, e := s.reader.read(&s.snap, row, s.store.row)
	if e != nil {
		return nil, e
	}
	return s.assembler.assemble(s.snap.assemblyRow(row), raw, nil, s.snap.rowLeaf(row))
}

func (s *viewSource) resident(row uint64) []byte {
	now := s.store.config.Now()
	if s.indexing {
		now = time.UnixMilli(s.snap.rowTime(row))
	}
	run := s.reader.context.segment(&s.snap, row)
	if len(s.snap.residentPack(*run, now)) == 0 {
		return nil
	}
	raw, err := s.reader.read(&s.snap, row, s.store.row)
	if err != nil {
		return nil
	}
	if s.assembler.layout == nil {
		return nil
	}
	full, err := s.assembler.assemble(s.snap.assemblyRow(row), raw, nil, s.snap.rowLeaf(row))
	if err != nil {
		return nil
	}
	return full
}
