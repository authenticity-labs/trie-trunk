package cache

// Publish complete hours in payload → index → summary order, protected by .pack.
import (
	"bytes"
	"fmt"
	"hash/crc32"
	"time"
)

func (s *Store) commitBatch(b *branchStore, batch []pending) error {
	if len(batch) == 0 {
		return nil
	}
	_, endHour := semanticBounds(time.UnixMilli(eventTime(batch[0].id[:])), 1, s.location)
	for i, p := range batch {
		if eventTime(p.id[:]) >= endHour.UnixMilli() || i > 0 && bytes.Compare(batch[i-1].id[:], p.id[:]) >= 0 {
			return fmt.Errorf("seal requires one ordered hour")
		}
	}

	// All input and placement metadata are frozen under the writer lock. A
	// separate store value owns the worker's trie; ingestion cannot observe it
	// before the canonical commit and publication complete.
	if s.sealing != nil {
		return fmt.Errorf("overlapping seal job")
	}
	done := make(chan struct{})
	s.sealing = done
	b.sealingBefore = endHour.UnixMilli()
	releaseMemory := b.pinMemory()
	worker := &Store{config: s.config, layout: s.layout, row: s.row, location: s.location,
		memory: s.memory, policy: s.policy, branchPolicies: s.branchPolicies, shared: s.shared, trie: s.trie.clone()}
	frozen := &branchStore{branch: b.branch, generation: b.generation, baseRows: b.baseRows,
		packsRetired: b.packsRetired, memberEnd: b.memberEnd, blockBase: b.blockBase,
		rows: b.rows, runs: append([]Run(nil), b.runs...), packsEnd: b.packsEnd, sumEnd: b.sumEnd,
		idx: b.idx, packs: b.packs, sum: b.sum, accumulators: b.accumulators.frozen(endHour.UnixMilli() - TimeHourMS)}
	s.mu.Unlock()
	releaseSeal := s.config.CommitScheduler.acquire()
	result, err := worker.writeSealedHour(frozen, batch)
	releaseSeal()
	waiting := time.Now()
	s.mu.Lock()
	publishing := time.Now()
	defer func() { s.config.CommitScheduler.observePublication(publishing.Sub(waiting), time.Since(publishing)) }()
	defer func() { releaseMemory(); b.sealingBefore = 0; s.sealing = nil; close(done) }()
	if err != nil {
		return s.fail(err)
	}
	defer result.owner.close()
	if err = s.check(); err != nil {
		return err
	}
	s.trie = worker.trie
	if result.summary != nil {
		b.summaries = append(b.summaries, *result.summary)
	}
	b.sumEnd = frozen.sumEnd
	if err = b.mapIndex(); err != nil {
		return s.fail(err)
	}
	if err = s.rememberPackOwned(b, result.run, result.owner); err != nil {
		return s.fail(err)
	}
	b.runs = append(b.runs, result.run)
	b.timeDirty = true
	b.packsEnd = result.run.PacksEnd
	s.queueSearch(b.branch)
	if err = s.indexSealedObjects(b, result); err != nil {
		return s.fail(err)
	}
	if err = s.sealProbe("collection-published"); err != nil {
		return s.fail(err)
	}
	// Drop provisional identities before publishing the canonical row identities.
	b.finishPending(endHour.UnixMilli())
	delete(b.provisionalRecords, endHour.UnixMilli()-TimeHourMS)
	for n, p := range batch {
		b.pendingBytes -= len(p.frame)
		id, e := seedKey(eventTime(p.id[:]), uint32(n))
		if e != nil {
			return s.fail(e)
		}
		// Only object/revision metadata is retained. Its original immutable PIL
		// already contains those fields; do not reconstruct another full record.
		if err = s.rememberSealed(b, id, p.frame, result.run.Start+uint64(n)); err != nil {
			return s.fail(err)
		}
	}
	b.pendingVersion++
	delete(b.ingressPages, result.run.PAC.Placement.From/TimeHourMS)
	if err = s.publishStates(); err != nil {
		return s.fail(err)
	}
	b.accumulators.release(result.run.PAC.Placement.From)
	s.publishResident(b)
	b.pruneMemory(s.config.Now())
	s.config.CommitScheduler.observeWindow(endHour, s.config.Now())
	return nil
}

type sealedHour struct {
	owner    *nativeBuffer
	packs     []byte
	run      Run
	summary  *Summary
	postings []collectionPosting
}

// Only this worker touches unpublished file tails. Readers keep their prior
// ranges/mappings, and ingestion writes solely to .seeds and the native live index.
func (s *Store) writeSealedHour(b *branchStore, batch []pending) (*sealedHour, error) {
	if e := s.sealProbe("prepare"); e != nil {
		return nil, e
	}
	if len(batch) > maxHourRecords {
		return nil, ErrIndexCapacity
	}
	idx := make([]byte, 0, len(batch)*s.row.Width)
	frames := make([][]byte, 0, len(batch))
	var frame Frame
	var encoded []byte
	scratch := nativeScratch{memory: s.memory}
	defer scratch.close()
	startRow := b.tail() + IndexHeaderSlots
	if startRow+uint64(len(batch)) > IndexCapacity {
		return nil, ErrIndexCapacity
	}
	hourStart, _ := semanticBounds(time.UnixMilli(eventTime(batch[0].id[:])), 1, s.location)
	hourBase := hourStart.UnixMilli()
	refs := map[uint16]uint64{}
	var postings []collectionPosting
	for ordinal, p := range batch {
		if e := s.layout.Scan(p.frame, &frame); e != nil {
			return nil, e
		}
		v := frame.Values
		if b.branch == Write && s.config.ObjectIndex && v[s.layout.Revision].State != Absent {
			key, e := s.collectionRevision(v)
			if e != nil {
				return nil, e
			}
			postings = append(postings, collectionPosting{key.Object, key.Revision, collectionAddress{Record: uint32(ordinal + 1)}})
		}
		ref := s.trie.intern(s.layout, v)
		refs[ref]++
		row := make([]byte, s.row.Width)
		order.PutUint16(row[4:6], ref)
		if e := packIndexID(row, p.id, hourBase); e != nil {
			return nil, e
		}

		if !s.layout.logStrategy() {
			v[s.layout.Event] = Value{}
		}
		v[s.layout.Time] = Value{}
		v[s.layout.Record] = Uint(uint64(ordinal), 3)
		v[s.layout.Leaf] = Value{}
		if ref != 0 {
			s.trie.remove(ref, v)
		}
		if s.layout.Shared >= 0 {
			v[s.layout.Shared] = Value{}
		}
		var e error
		encoded, e = s.layout.Layout.EncodeAllocated(v, scratch.allocate)
		if e != nil {
			return nil, e
		}
		frames = append(frames, encoded)
		idx = append(idx, row...)
	}
	owner, block, err := s.packBlock(b, startRow, batch, frames, idx)
	if err != nil {
		return nil, err
	}

	if e := s.sealProbe("packed"); e != nil {
		owner.close()
		return nil, e
	}
	packs := owner.bytes
	transferred := false
	defer func() {
		if !transferred {
			owner.close()
		}
	}()
	if len(s.trie.nodes) > s.trie.persisted {
		raw, e := encoder.Marshal(s.trie.nodes[s.trie.persisted:])
		if e != nil {
			return nil, e
		}
		if e = appendLeafNodes(s.shared, &s.trie, raw); e != nil {
			return nil, e
		}
		s.trie.persisted = len(s.trie.nodes)
	}
	if _, e := b.packs.WriteAt(packs, int64(b.packsEnd-b.packsRetired)); e != nil {
		return nil, e
	}
	if e := s.sealProbe("payload-written"); e != nil {
		return nil, e
	}
	if e := b.packs.Sync(); e != nil {
		return nil, e
	}
	if e := s.sealProbe("payload"); e != nil {
		return nil, e
	}
	if _, e := b.idx.WriteAt(indexHeader(hourBase, uint64(len(batch))), headerSize+int64(len(b.rows))); e != nil {
		return nil, e
	}
	if e := s.sealProbe("index-header-written"); e != nil {
		return nil, e
	}
	if _, e := b.idx.WriteAt(idx, headerSize+int64(len(b.rows))+IndexRowSize); e != nil {
		return nil, e
	}
	if e := s.sealProbe("index-written"); e != nil {
		return nil, e
	}
	if e := b.idx.Sync(); e != nil {
		return nil, e
	}
	if e := s.sealProbe("index"); e != nil {
		return nil, e
	}
	run := Run{References: refs, Start: startRow, End: startRow + uint64(len(batch)), PacksStart: b.packsEnd, PacksEnd: b.packsEnd + uint64(len(packs)), MinMS: eventTime(batch[0].id[:]), MaxMS: eventTime(batch[len(batch)-1].id[:]), IndexCRC: crc32.Checksum(idx, checksum), PacksCRC: crc32.Checksum(packs, checksum), TrieSize: len(s.trie.nodes), PAC: block}
	var summaryResult *Summary
	if b.sum != nil {
		accumulator := b.accumulators.get(hourBase)
		if accumulator == nil {
			return nil, fmt.Errorf("missing ingress accumulator")
		}
		summary, e := accumulator.snapshot(run, s.layout, &s.trie)
		if e != nil {
			return nil, e
		}
		raw, e := encodeSummary(summary)
		if e != nil {
			return nil, e
		}
		if e = appendRecord(b.sum, raw); e != nil {
			return nil, e
		}
		if e = s.sealProbe("summary-written"); e != nil {
			return nil, e
		}
		if e = b.sum.Sync(); e != nil {
			return nil, e
		}
		stat, e := b.sum.Stat()
		if e != nil {
			return nil, e
		}
		b.sumEnd = uint64(stat.Size())
		summaryResult = &summary
	}
	if e := s.sealProbe("summary"); e != nil {
		return nil, e
	}
	transferred = true
	return &sealedHour{owner: owner, packs: packs, run: run, summary: summaryResult, postings: postings}, nil
}

func (s *Store) sealProbe(stage string) error {
	if s.config.sealHook != nil {
		return s.config.sealHook(stage)
	}
	return nil
}

// An hour is one collection transaction. Preparing identities alongside PIL
// packing avoids rereading the PAC and fsyncing every 1,024 replay references.
// Recovery retains its bounded canonical backfill path in indexObjects.
func (s *Store) indexSealedObjects(b *branchStore, result *sealedHour) error {
	if b.branch != Write || !s.config.ObjectIndex || s.objects == nil {
		return nil
	}
	key := objectBinding{s.layout.Definition.Resource.Minor, b.generation}
	from := result.run.Start - IndexHeaderSlots
	if s.objects.through[key] != from {
		return fmt.Errorf("collection watermark changed during seal")
	}
	hour := uint32((result.run.PAC.Placement.From - b.hourOrigin()) / TimeHourMS)
	for i := range result.postings {
		result.postings[i].Address.Hour = hour
	}
	return s.objects.append(key, byte(s.layout.Widths[s.layout.Revision]), from, result.run.End, result.postings)
}
