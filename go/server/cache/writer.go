package cache

// Validate and durably stage ingress; sealing assigns canonical row positions.
import (
	"bytes"
	"errors"
	"fmt"
	"time"
	"trie-trunk/go/internal/seedtransform"
)

// Row IDs are assigned only after the three-cycle finalization boundary.
func (s *Store) Append(branch Branch, raw []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	duplicate, e := s.appendStaged(branch, raw, true)
	if e == nil {
		s.publishResident(s.branches[branch])
		s.queueSearch(branch)
	}
	return duplicate, e
}

// AppendBatch acknowledges a durable prefix with one swap sync. The returned
// count covers accepted receipts and stops before the first rejection.
func (s *Store) AppendBatch(branch Branch, frames [][]byte) (count int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, raw := range frames {
		if _, err = s.appendStaged(branch, raw, false); err != nil {
			break
		}
		count++
	}
	if b := s.branches[branch]; b != nil && b.swap != nil {
		if e := b.syncIngress(); e != nil {
			return 0, s.fail(e)
		}
	}
	if count > 0 {
		s.publishResident(s.branches[branch])
		s.queueSearch(branch)
	}
	return count, err
}
func (s *Store) appendStaged(branch Branch, raw []byte, sync bool) (bool, error) {
	return s.appendOwned(branch, raw, sync, nil)
}
func (s *Store) appendOwned(branch Branch, raw []byte, sync bool, input *nativeBuffer) (bool, error) {
	return s.appendProviderOwned(branch, raw, sync, input, nil)
}
func (s *Store) appendProviderOwned(branch Branch, raw []byte, sync bool, input *nativeBuffer, provider *seedtransform.Plan) (bool, error) {
	if e := s.check(); e != nil {
		return false, e
	}
	original := raw
	if !branch.Valid() {
		return false, fmt.Errorf("invalid branch")
	}
	sourceDigest := s.layout.Digest
	if provider != nil {
		sourceDigest = provider.Source.Digest
	}
	reject := func(reason string, e error) error {
		return s.rejectInputDefinition(branch, original, reason, e, sourceDigest)
	}
	if provider == nil {
		if e := s.layout.Scan(raw, &s.scratch); e != nil {
			return false, reject("invalid_pil", e)
		}
	} else {
		values, e := provider.Apply(raw, s.scratch.Values)
		if e != nil {
			return false, reject("provider_transform", e)
		}
		s.scratch.Values = values
	}
	if s.layout.Shared >= 0 && s.scratch.Values[s.layout.Shared].State != Absent {
		if e := s.expandShared(s.scratch.Values); e != nil {
			return false, reject("invalid_reference", e)
		}

	}
	// Supplied identity is validated before transforms and can never be overwritten.
	identity := s.scratch.Values[s.layout.Event]
	if identity.State != Absent && !(s.layout.logStrategy() && identity.State == Null) {
		if e := s.layout.validateIdentity(identity); e != nil {
			return false, reject("invalid_event_id", e)
		}
	}
	if s.transforms != nil && len(s.transforms.Steps) > 0 {
		transformed, e := s.transformValues(s.scratch.Values)
		if e != nil {
			return false, reject("transform_definition_mismatch", e)
		}
		s.scratch.Values = transformed
	}
	if s.layout.logStrategy() && identity.State != Absent && identity.State != Null {
		current := s.scratch.Values[s.layout.Event]
		if current.State != identity.State || !bytes.Equal(current.Bytes, identity.Bytes) {
			return false, reject("invalid_event_id", fmt.Errorf("transform cannot replace supplied @id"))
		}
	}
	if !s.layout.logStrategy() && identity.State == Absent {
		ms := s.config.Now().UnixMilli()
		if v := s.scratch.Values[s.layout.Time]; v.State == Direct {
			ms = int64(unsigned(v.Bytes))
		}
		generated, e := seedKey(ms, 0)
		if e != nil {
			return false, e
		}
		s.scratch.Values[s.layout.Event] = Value{Direct, generated[:]}

	}
	var id [16]byte
	var e error
	if s.layout.logStrategy() {
		e = s.layout.generateLogIdentity(s.scratch.Values, s.identityScratch[:s.ingress.entropyBytes()])
		if e == nil {
			id, e = seedKey(int64(unsigned(s.scratch.Values[s.layout.Time].Bytes)), 0)
		}
	} else {
		id, e = eventID(s.scratch.Values[s.layout.Event])
	}
	if e != nil {
		return false, reject("invalid_event_id", e)
	}
	b, e := s.openBranch(branch)
	if e != nil {
		return false, s.fail(e)
	}
	if s.capacityClosed {
		return false, ErrIndexCapacity
	}
	ms := eventTime(id[:])
	if ms < b.ingressBoundary() {
		return false, s.rejectLateDefinition(b, id, raw, sourceDigest, s.scratch.Values)
	}
	hour, _ := semanticBounds(time.UnixMilli(ms), 1, s.location)
	ordinal := b.provisionalRecords[hour.UnixMilli()]
	if ordinal >= maxHourRecords {
		return false, ErrIndexCapacity
	}
	id, e = seedKey(ms, ordinal)
	if e != nil {
		return false, e
	}
	if !s.layout.logStrategy() {
		s.scratch.Values[s.layout.Event] = Value{Direct, id[:]}
	}
	s.scratch.Values[s.layout.Record] = Uint(uint64(ordinal), 3)
	s.scratch.Values[s.layout.Time] = Uint(uint64(ms), 8)
	s.scratch.Values[s.layout.Leaf] = Value{}
	if e = s.stripBlindEventID(s.scratch.Values); e != nil {
		return false, reject("invalid_pil", e)
	}
	// Normalize field spans first, then encode exactly once into a leased native
	// chunk. This never overwrites input or any previously published prefix.
	output := nativeIngress{memory: s.memory}
	defer output.close()
	raw, e = s.layout.Layout.EncodeAllocated(s.scratch.Values, output.reserve)
	if e != nil {
		if s.transforms != nil && len(s.transforms.Steps) > 0 {
			return false, reject("transform_definition_mismatch", e)
		}
		return false, e
	}
	output.current.used += len(raw)
	output.partial = 0
	input = output.current.buffer
	// EncodeAllocated validated the authored schema. Rebind field spans to
	// the native output through the codec without validating that schema twice.
	if e = s.layout.ScanFields(raw, &s.scratch); e != nil {
		return false, e
	}
	if branch == Write && s.config.ObjectIndex {
		if _, e := s.checkCollectionWrite(b, s.scratch.Values); e != nil {
			var readError *collectionReadError
			if errors.As(e, &readError) {
				return false, s.fail(e)
			}
			return false, reject("invalid_collection_revision", e)
		}
	}
	if s.capacityClosed {
		return false, ErrIndexCapacity
	}
	if s.capacityClosed || b.tail()+uint64(b.live.count(0)+1)*(1+IndexHeaderSlots) > s.indexCapacity() {
		return false, ErrIndexCapacity
	}
	if e = s.stagingCapacity(len(raw)); e != nil {
		return false, e
	}
	if !b.live.room(s.config.Now()) {
		return false, fmt.Errorf("%w: native live index record limit", ErrStagingFull)
	}
	owned, e := b.own(s, id, raw, input)
	if e != nil {
		return false, e
	}
	// Receipt is acknowledged only after the complete frame is on stable storage.
	file, e := s.ingressFile(b, hour.UnixMilli())
	if e != nil {
		return false, s.fail(e)
	}
	b.ingressDirty[file] = true
	if e = appendRecord(file, raw); e == nil && sync {
		e = file.Sync()
		if e == nil {
			delete(b.ingressDirty, file)
		}
	}
	if e != nil {
		return false, s.fail(e)
	}
	if e = s.invalidateReceivedValues(branch, s.scratch.Values); e != nil {
		return false, s.fail(e)
	}
	copyFrame := owned
	if e = s.rememberPendingValues(b, pending{id, copyFrame}, s.scratch.Values); e != nil {
		return false, s.fail(e)
	}
	s.ingress.record()
	return false, nil
}

// Native ingress transfers immutable frames to resident hour arenas. The caller
// releases its buffer handles after this call; accepted frames remain native.
func (s *Store) appendNativeBatch(branch Branch, frames []nativeInput) (count int, err error) {
	return s.appendProviderBatch(branch, frames, nil)
}
func (s *Store) appendProviderBatch(branch Branch, frames []nativeInput, provider *seedtransform.Plan) (count int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, frame := range frames {
		if _, err = s.appendProviderOwned(branch, frame.raw, false, frame.buffer, provider); err != nil {
			break
		}
		count++
	}
	if b := s.branches[branch]; b != nil && b.swap != nil {
		if e := b.syncIngress(); e != nil {
			return 0, s.fail(e)
		}
	}
	if count > 0 {
		s.publishResident(s.branches[branch])
		s.queueSearch(branch)
	}
	return
}

// A durable PAC seals its whole hour against new arrivals, independently of
// the seed watermark that certifies completeness after cleanup.
func (b *branchStore) ingressBoundary() int64 {
	boundary := max(b.finalizedBefore, b.sealingBefore)
	if len(b.runs) > 0 {
		boundary = max(boundary, b.runs[len(b.runs)-1].PAC.Placement.To)
	}
	return boundary
}
