package cache

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const FinalizationCycle = 5 * time.Minute
const FinalizationCycles = 3

// Run promotes the mature tail even when there are no new requests. Flush and
// Close use the same gate; neither can force young events into permanent rows.
func (s *Store) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("positive maintenance interval required")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if e := s.Flush(); e != nil {
				return e
			}
		}
	}
}

func (s *Store) swapPath(b *branchStore) string {
	// Independent of retention generations: the unfinalized tail must survive
	// replacement of the finalized prefix.
	return filepath.Join(s.dir, b.branch.Prefix()+".seeds")
}

// The first framed record identifies the ingress hour. The rest are immutable PILs.
func stagingHour(f *os.File) (int64, error) {
	var raw [16]byte
	if _, err := f.ReadAt(raw[:], headerSize); err != nil {
		return 0, err
	}
	if order.Uint32(raw[:4]) != 8 || crc32.Checksum(raw[8:], checksum) != order.Uint32(raw[4:8]) {
		return 0, fmt.Errorf("invalid ingress hour header")
	}
	hour := int64(order.Uint64(raw[8:]))
	if hour < 0 || hour != utcHour(hour) {
		return 0, fmt.Errorf("invalid ingress hour")
	}
	return hour, nil
}

func (s *Store) createStaging(path string, hour int64) (*os.File, error) {
	// No event can have been acknowledged before the initial header/hour sync.
	if info, e := os.Stat(path); e == nil && info.Size() < headerSize+16 {
		size := int64(0)
		if info.Size() >= headerSize {
			size = headerSize
		}
		if e = os.Truncate(path, size); e != nil {
			return nil, e
		}
	}
	f, err := openFile(path, "PILSEED0", s.layout.Digest, [16]byte{}, 0)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*os.File, error) { f.Close(); return nil, err }
	stat, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if stat.Size() == headerSize {
		if err = s.sealProbe("seed-file-header"); err != nil {
			return fail(err)
		}
		var raw [8]byte
		order.PutUint64(raw[:], uint64(hour))
		if err = appendRecord(f, raw[:]); err != nil {
			return fail(err)
		}
		if err = s.sealProbe("seed-hour-written"); err != nil {
			return fail(err)
		}
		if err = f.Sync(); err != nil {
			return fail(err)
		}
		if err = s.sealProbe("seed-file-durable"); err != nil {
			return fail(err)
		}
		if err = syncDir(s.dir); err != nil {
			return fail(err)
		}
	}
	return f, nil
}

func (s *Store) packPath(b *branchStore, hour int64) string {
	// Normally only the previous hour exists. Qualified names retain a backlog
	// across downtime/backfill without overwriting another hour's recovery file.
	primary := filepath.Join(s.dir, b.branch.Prefix()+".pack")
	if _, err := os.Stat(primary); os.IsNotExist(err) {
		return primary
	}
	return filepath.Join(s.dir, fmt.Sprintf("%s.%d.pack", b.branch.Prefix(), hour))
}

func (s *Store) openStaging(b *branchStore) error {
	b.stagingPacks = map[int64]*os.File{}
	b.ingressDirty = map[*os.File]bool{}
	paths, err := filepath.Glob(filepath.Join(s.dir, b.branch.Prefix()+".*pack"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		if filepath.Ext(path) != ".pack" {
			continue
		}
		if info, e := os.Stat(path); e != nil {
			return e
		} else if info.Size() < headerSize+16 {
			if e = os.Remove(path); e != nil {
				return e
			}
			if e = syncDir(s.dir); e != nil {
				return e
			}
			continue
		}
		f, err := openFile(path, "PILSEED0", s.layout.Digest, [16]byte{}, 0)
		if err != nil {
			return err
		}
		hour, err := stagingHour(f)
		if err != nil {
			f.Close()
			return err
		}
		if b.stagingPacks[hour] != nil {
			f.Close()
			return fmt.Errorf("duplicate ingress hour")
		}
		b.stagingPacks[hour] = f
	}
	b.swap, err = s.createStaging(s.swapPath(b), utcHour(s.config.Now().UnixMilli()))
	if err != nil {
		return err
	}
	b.seedHour, err = stagingHour(b.swap)
	if err != nil {
		return err
	}
	return s.rotateSeed(b)
}

func (s *Store) rotateSeed(b *branchStore) error {
	hour := utcHour(s.config.Now().UnixMilli())
	if hour <= b.seedHour {
		return nil
	}
	if b.stagingPacks[b.seedHour] != nil {
		return fmt.Errorf("duplicate rotating ingress hour")
	}
	if err := b.swap.Sync(); err != nil {
		return err
	}
	if err := s.sealProbe("seed-synced"); err != nil {
		return err
	}
	delete(b.ingressDirty, b.swap)
	path := s.packPath(b, b.seedHour)
	if err := os.Rename(s.swapPath(b), path); err != nil {
		return err
	}
	if err := s.sealProbe("seed-renamed"); err != nil {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	// Reopen under its new name so deletion always targets the rotated file.
	if err := b.swap.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	b.stagingPacks[b.seedHour] = f
	b.swap = nil
	if err := s.sealProbe("rotation-durable"); err != nil {
		return err
	}
	b.swap, err = s.createStaging(s.swapPath(b), hour)
	if err != nil {
		return err
	}
	b.seedHour = hour
	if b.finalizedBefore > 0 {
		var raw [8]byte
		order.PutUint64(raw[:], uint64(b.finalizedBefore))
		if err := appendRecord(b.swap, raw[:]); err != nil {
			return err
		}
		if err := b.swap.Sync(); err != nil {
			return err
		}
	}
	return s.sealProbe("fresh-seed")
}

func (s *Store) ingressFile(b *branchStore, hour int64) (*os.File, error) {
	if err := s.rotateSeed(b); err != nil {
		return nil, err
	}
	if hour == b.seedHour {
		return b.swap, nil
	}
	if f := b.stagingPacks[hour]; f != nil {
		return f, nil
	}
	f, err := s.createStaging(s.packPath(b, hour), hour)
	if err != nil {
		return nil, err
	}
	b.stagingPacks[hour] = f
	return f, nil
}

func (b *branchStore) syncIngress() error {
	for f := range b.ingressDirty {
		if err := f.Sync(); err != nil {
			return err
		}
		delete(b.ingressDirty, f)
	}
	return nil
}

func (s *Store) openSwap(b *branchStore) error {
	files := []*os.File{b.swap}
	hours := make([]int64, 0, len(b.stagingPacks))
	for hour := range b.stagingPacks {
		hours = append(hours, hour)
	}
	sort.Slice(hours, func(i, j int) bool { return hours[i] < hours[j] })
	for _, hour := range hours {
		files = append(files, b.stagingPacks[hour])
	}
	for _, f := range files {
		hour, err := stagingHour(f)
		if err != nil {
			return err
		}
		first := true
		err = visitRecords(f, MaxFrame, s.memory, func(raw []byte, input *nativeBuffer) error {
			if first {
				first = false
				return nil
			}
			if len(raw) == 8 {
				cutoff := int64(order.Uint64(raw))
				if cutoff < 0 || cutoff != utcHour(cutoff) {
					return fmt.Errorf("invalid ingress finalization watermark")
				}
				b.finalizedBefore = max(b.finalizedBefore, cutoff)
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
			if utcHour(eventTime(id[:])) != hour {
				return fmt.Errorf("PIL outside ingress hour")
			}
			if eventTime(id[:]) < b.ingressBoundary() {
				return nil
			}
			if prior := b.live.get(id[:], true, s.config.Now()); prior != nil {
				if !s.layout.sameEvent(prior, raw) {
					return fmt.Errorf("conflicting staged event_id")
				}
				return nil
			}
			if b.branch == Write && s.config.ObjectIndex {
				if _, err = s.checkCollectionWrite(b, frame.Values); err != nil {
					return err
				}
			}
			if err = s.invalidateReceived(b.branch, raw); err != nil {
				return err
			}
			raw, err = b.own(s, id, raw, input)
			if err != nil {
				return err
			}
			return s.rememberPendingValues(b, pending{id, raw}, frame.Values)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) retirePack(b *branchStore, hour int64) error {
	f := b.stagingPacks[hour]
	if f == nil {
		return nil
	}
	if err := s.sealProbe("before-pack-remove"); err != nil {
		return err
	}
	if err := os.Remove(f.Name()); err != nil {
		return err
	}
	if err := s.sealProbe("pack-removed"); err != nil {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	delete(b.stagingPacks, hour)
	delete(b.ingressDirty, f)
	if err := f.Close(); err != nil {
		return err
	}
	return s.sealProbe("pack-removal-durable")
}

func (s *Store) flush(b *branchStore) error {
	if s.sealing != nil {
		return nil
	}
	if err := s.rotateSeed(b); err != nil {
		return err
	}
	b.live.prune(s.config.Now())
	defer s.publishMaintained(b)
	cutoff := utcHour(s.config.Now().Add(-15 * time.Minute).UnixMilli())
	cutoff = max(cutoff, b.finalizedBefore)
	for {
		first := b.live.firstPending()
		if first >= cutoff {
			break
		}
		start, end := semanticBounds(time.UnixMilli(first), 1, s.location)
		batch := b.pendingWindow(nil, start, end)
		if len(batch) == 0 {
			return fmt.Errorf("native pending hour is empty")
		}
		if err := s.commitBatch(b, batch); err != nil {
			return err
		}
		if err := s.retirePack(b, start.UnixMilli()); err != nil {
			return err
		}
	}
	// Empty rotated hours have no payload to publish.
	for hour := range b.stagingPacks {
		if hour < cutoff {
			if err := s.retirePack(b, hour); err != nil {
				return err
			}
		}
	}
	if cutoff > b.finalizedBefore {
		var raw [8]byte
		order.PutUint64(raw[:], uint64(cutoff))
		if err := appendRecord(b.swap, raw[:]); err != nil {
			return err
		}
		if err := b.swap.Sync(); err != nil {
			return err
		}
	}
	b.finalizedBefore = cutoff
	if err := s.publishTime(b); err != nil {
		return err
	}
	return s.packSummaryDays(b)
}

// An error or revision-less collection write invalidates replay caches at receipt.
// Replaying this after a kill may conservatively invalidate twice, never clear
// uncertainty or leave a cache valid after an acknowledged error.
func (s *Store) invalidateReceived(branch Branch, raw []byte) error {
	policy := s.policyFor(branch)
	if (branch != Error && !(branch == Write && s.config.ObjectIndex)) || policy.CacheInvalidation != nil && !*policy.CacheInvalidation {
		return nil
	}
	var f Frame
	if e := s.layout.Scan(raw, &f); e != nil {
		return e
	}
	return s.invalidateReceivedValues(branch, f.Values)
}

func (s *Store) invalidateReceivedValues(branch Branch, values []Value) error {
	policy := s.policyFor(branch)
	if (branch != Error && !(branch == Write && s.config.ObjectIndex)) || policy.CacheInvalidation != nil && !*policy.CacheInvalidation {
		return nil
	}
	if branch == Write && values[s.layout.Revision].State != Absent {
		return nil
	}
	if s.layout.Object >= 0 && values[s.layout.Object].State >= Direct {
		return s.invalidate([][]byte{s.layout.NormalizeIntegerWidth(s.layout.Object, values[s.layout.Object]).Bytes}, false)
	}
	return s.invalidate(nil, true)
}

// A kill before rename may leave an unpublished copy. The original swap is
// still authoritative. This runs only while holding the cache writer lease.
func (s *Store) removeSwapTemps() error {
	entries, e := os.ReadDir(s.dir)
	if e != nil {
		return e
	}
	changed := false
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), ".seed-") {
			if e = os.Remove(filepath.Join(s.dir, entry.Name())); e != nil {
				return e
			}
			changed = true
		}
	}
	if changed {
		return syncDir(s.dir)
	}
	return nil
}
