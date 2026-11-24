package cache

import (
	"bytes"
	"maps"
	"os"
	"sort"
	"sync/atomic"
)

// Each page is a published ingress batch. Neither its descriptors nor its PIL
// bytes change. New batches link a new tail, never rewrite an older page.
type ingressPage struct {
	prior       *ingressPage
	records     []pending
	first, last int64
}

type residentFile struct {
	source *os.File
	refs   atomic.Int64
	file   *os.File
}

func (f *residentFile) release() {
	if f != nil && f.refs.Add(-1) == 0 {
		_ = f.file.Close()
	}
}

// One publication selects both representations of an hour. Reference acquisition
// races safely with retirement: a reader can retain only a nonzero generation.
// The Go object remains reachable while attempting the CAS, even if its native
// owners have already been released. No byte is accessed until retention succeeds.
type residentSnapshot struct {
	index            *residentMapping
	summaryFile      *residentFile
	refs             atomic.Int64
	branch           branchStore
	ingress          map[int64]*ingressPage
	liveSummaries    map[int64]*liveSummaryHour
	sequence         uint64
	sealed, retained int64
	file             *residentFile
	unpin            func()
}

func (p *residentSnapshot) release() {
	if p.refs.Add(-1) == 0 {
		p.index.release()
		p.summaryFile.release()
		p.unpin()
		p.file.release()
	}
}
func (s *Store) resident(branch Branch) *residentSnapshot {
	for {
		p := s.published[branch].Load()
		if p == nil {
			return nil
		}
		n := p.refs.Load()
		if n > 0 && p.refs.CompareAndSwap(n, n+1) {
			return p
		}
	}
}
func cloneArenas(source map[int64]*nativeArena) map[int64]*nativeArena {
	out := make(map[int64]*nativeArena, len(source))
	for hour, arena := range source {
		out[hour] = arena
	}
	return out
}

// Writer-side publication triggers: durable ingress acknowledgement, completed
// seal, timeline maintenance and retention replacement. Readers never flush.
func (s *Store) publishResident(b *branchStore) {
	if b == nil {
		return
	}
	if b.ingressPages == nil {
		b.ingressPages = map[int64]*ingressPage{}
	}
	groups := map[int64][]pending{}
	for _, record := range b.ingressTail {
		hour := eventTime(record.id[:]) / TimeHourMS
		groups[hour] = append(groups[hour], record)
	}
	b.ingressTail = nil
	for hour, records := range groups {
		sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i].id[:], records[j].id[:]) < 0 })
		b.ingressPages[hour] = &ingressPage{prior: b.ingressPages[hour], records: records, first: eventTime(records[0].id[:]), last: eventTime(records[len(records)-1].id[:])}
	}
	ingress := make(map[int64]*ingressPage, len(b.ingressPages))
	for hour, page := range b.ingressPages {
		ingress[hour] = page
	}
	sealed := min(b.time.sealed(), utcHour(b.live.firstPending()))
	if len(b.runs) > 0 {
		sealed = max(sealed, b.runs[len(b.runs)-1].PAC.Placement.To)
	}
	p := &residentSnapshot{branch: branchStore{branch: b.branch, layout: b.layout, generation: b.generation,
		runs: b.runs[:len(b.runs):len(b.runs)], baseRows: b.baseRows, packsRetired: b.packsRetired, packsEnd: b.packsEnd,
		sealedMemory: cloneArenas(b.sealedMemory), rows: b.rows[:len(b.rows):len(b.rows)], time: b.time, timeAnchor: b.timeAnchor, summaryDays: maps.Clone(b.summaryDays), summaries: b.summaries[:len(b.summaries):len(b.summaries)], blockBase: b.blockBase}, ingress: ingress, sequence: s.generationSequence(b.branch), sealed: sealed, retained: min(b.time.first(), sealed), file: b.readerFile, unpin: b.pinMemory()}
	if p.file != nil {
		p.file.refs.Add(1)
	}
	p.index = b.readerIndex
	if p.index != nil {
		p.index.refs.Add(1)
	}
	p.summaryFile = b.readerSummary
	if p.summaryFile != nil {
		p.summaryFile.refs.Add(1)
		p.branch.sum = p.summaryFile.file
	}
	p.refs.Store(1)
	// Ring ownership ends at successful seal. Each published value is immutable.
	p.liveSummaries = make(map[int64]*liveSummaryHour)
	prior := s.published[b.branch].Load()
	b.accumulators.each(func(hour int64, a *summaryAccumulator) {
		value := a.live
		if prior != nil {
			if saved := prior.liveSummaries[hour]; saved != nil && saved.Total.Revision == value.Total.Revision {
				p.liveSummaries[hour] = saved
				return
			}
		}
		value.Fields = value.Fields.clone()
		p.liveSummaries[hour] = &value
	})
	if old := s.published[b.branch].Swap(p); old != nil {
		old.release()
	}
}
func (p *residentSnapshot) live(from, to int64) *liveWindow {
	result := &liveWindow{generation: p.branch.generation, sequence: p.sequence, sealed: p.sealed, retained: p.retained}
	for hour, tail := range p.ingress {
		if hour*TimeHourMS >= to || (hour+1)*TimeHourMS <= from {
			continue
		}
		for page := tail; page != nil; page = page.prior {
			if page.last < from || page.first >= to {
				continue
			}
			a := sort.Search(len(page.records), func(i int) bool { return eventTime(page.records[i].id[:]) >= from })
			z := sort.Search(len(page.records), func(i int) bool { return eventTime(page.records[i].id[:]) >= to })
			result.events = append(result.events, page.records[a:z]...)
		}
	}
	sort.Slice(result.events, func(i, j int) bool { return bytes.Compare(result.events[i].id[:], result.events[j].id[:]) < 0 })
	return result
}
func (b *branchStore) openSummaryReader() error {
	if b.sum == nil {
		return nil
	}
	if b.readerSummary != nil && b.readerSummary.source == b.sum {
		return nil
	}
	f, err := os.Open(b.sum.Name())
	if err != nil {
		return err
	}
	b.readerSummary.release()
	b.readerSummary = &residentFile{file: f, source: b.sum}
	b.readerSummary.refs.Store(1)
	return nil
}
func (s *Store) publishStates() error {
	stat, err := s.shared.Stat()
	if err != nil {
		return err
	}
	if prior := s.publishedStates.Load(); prior != nil && len(*prior) == int(stat.Size()) {
		end := int64(len(*prior))
		if end >= headerSize+8+leafStatsTrailer && string((*prior)[end-8:]) == leafStatsMagic {
			end = int64(order.Uint64((*prior)[end-leafStatsTrailer:]))
		}
		if end == s.trie.fileEnd {
			return nil
		}
	}
	raw := make([]byte, stat.Size())
	if _, err = s.shared.ReadAt(raw, 0); err != nil {
		return err
	}
	s.publishedStates.Store(&raw)
	return nil
}
func (b *branchStore) openResidentFile() error {
	f, err := os.Open(b.packs.Name())
	if err != nil {
		return err
	}
	b.readerFile = &residentFile{file: f}
	b.readerFile.refs.Store(1)
	return b.openSummaryReader()
}

// Only complete UTC hours remain in the sealed map. The pending map is retired
// by sealing, never by age, so unprocessed acknowledged events cannot be lost.
func (s *Store) publishMaintained(b *branchStore) {
	b.pruneMemory(s.config.Now())
	s.publishResident(b)
}

// Branch creation is a writer operation, performed only before a branch has a
// published address space. Existing publications never acquire the writer lock.
func (s *Store) prepareResident(branch Branch) (*residentSnapshot, error) {
	if p := s.resident(branch); p != nil {
		return p, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	if _, err := s.openBranch(branch); err != nil {
		return nil, err
	}
	return s.resident(branch), nil
}
