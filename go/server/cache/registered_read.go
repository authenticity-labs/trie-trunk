package cache

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"trie-trunk/go/internal/node"
)

// A read lease pins the published timeline and its open PACKS file. Rust owns
// timeline interpretation and PAC framing. Transport reuses the existing native
// buffer pool and contiguous-range coalescing.
type registeredReadSnapshot struct {
	generation   uint64
	storage      seedStorage
	refs         atomic.Int64
	timeline     *seedNativeTime
	packs        *residentFile
	index        *residentFile
	indexSize    uint64
	summaries    *residentFile
	summaryIndex *seedNativeSummaryIndex
	summarySize  uint64
	header       struct {
		First      uint64   `json:"first"`
		Sealed     uint64   `json:"sealed"`
		Generation [16]byte `json:"generation"`
	}
	size        uint64
	strings     *seedStrings
	publication seedPublication
}

func (s *registeredReadSnapshot) release() {
	if s.refs.Add(-1) == 0 {
		s.strings.close()
		s.timeline.close()
		s.summaryIndex.close()
		if s.summaries != nil {
			s.summaries.release()
		}
		s.packs.release()
		if s.index != nil {
			s.index.release()
		}
	}
}

// Capture and cache only published metadata. The returned node lease separately
// pins live bytes until response completion; live Packs never enter this cache.
func (p *Endpoint) registeredSnapshot(route registeredRoute, operation string, ranges []summaryRange, clock uint64) (*registeredReadSnapshot, *nativeBuffer, *node.Snapshot, error) {
	key := filepath.Join(route.Storage.Directory, route.Storage.Stem)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, nil, nil, fmt.Errorf("endpoint closed")
	}
	previous := p.registeredReads[key]
	if previous != nil {
		previous.refs.Add(1)
	}
	p.mu.Unlock()
	var known *uint64
	if previous != nil {
		known = &previous.generation
	}
	var windows [][2]uint64
	if operation == "packs" || operation == "summaries" || operation == "timelines" {
		for _, r := range ranges {
			windows = append(windows, [2]uint64{uint64(r.from), uint64(r.to)})
		}
	}
	kind := node.Packs
	switch operation {
	case "summaries":
		kind = node.Summaries
	case "timelines":
		kind = node.Timeline
	}
	lease, err := p.node.Snapshot(route.Branch, windows, kind, known, clock)
	if err != nil {
		if previous != nil {
			previous.release()
		}
		return nil, nil, nil, err
	}
	snapshot := previous
	if lease.Published {
		snapshot, err = p.publication(lease)
		if previous != nil {
			previous.release()
		}
		if err != nil {
			lease.Close()
			return nil, nil, nil, err
		}
		// A slower request may finish after a newer publication. It may use its
		// pinned snapshot, but must never replace the cache with an older one.
		p.mu.Lock()
		cached := p.registeredReads[key]
		if !p.closed && (cached == nil || cached.generation < snapshot.generation) {
			snapshot.refs.Add(1)
			p.registeredReads[key] = snapshot
			if cached != nil {
				cached.release()
			}
		}
		p.mu.Unlock()
	}
	if snapshot == nil {
		lease.Close()
		return nil, nil, nil, fmt.Errorf("missing publication snapshot")
	}
	var buffer *nativeBuffer
	if operation != "timelines" && operation != "strings" {
		buffer = p.readMemory.buffer()
	}
	return snapshot, buffer, lease, nil
}
func (p *Endpoint) publication(lease *node.Snapshot) (snapshot *registeredReadSnapshot, err error) {
	timeline, err := seedNativeOpenTime(lease.Bytes(1, 0))
	if err != nil {
		return nil, err
	}
	snapshot = &registeredReadSnapshot{timeline: timeline, storage: p.storage, generation: lease.Generation, size: lease.Length, indexSize: lease.IndexLength, summarySize: lease.SummaryLength}
	snapshot.refs.Store(1)
	defer func() {
		if err != nil {
			snapshot.release()
		}
	}()
	info, err := timeline.info()
	if err != nil {
		return snapshot, err
	}
	err = json.Unmarshal(info.bytes, &snapshot.header)
	info.close()
	if err != nil {
		return snapshot, err
	}
	if lease.Length > 0 {
		snapshot.strings, snapshot.publication, err = openSeedStrings(lease.Bytes(2, 0), lease.Length)
		if err != nil {
			return snapshot, err
		}
	}
	slots := []**residentFile{&snapshot.packs, &snapshot.index, &snapshot.summaries}
	for i, slot := range slots {
		file, e := lease.File(uint32(i))
		if e != nil {
			return snapshot, e
		}
		if file != nil {
			*slot = &residentFile{file: file}
			(*slot).refs.Store(1)
		}
	}
	if snapshot.summaries != nil {
		snapshot.summaryIndex, err = seedSummaryFooter(lease.Bytes(3, 0), lease.SummaryLength)
	}
	return snapshot, err
}

func (p *Endpoint) registeredReadResource(w http.ResponseWriter, r *http.Request) bool {
	if p.registeredReads == nil {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(parts) != 5 || (parts[4] != "roots" && parts[4] != "strings" && parts[4] != "seeds" && parts[4] != "packs" && parts[4] != "timelines" && parts[4] != "indexes" && parts[4] != "summaries") {
		return false
	}
	post := r.Method == http.MethodPost && parts[4] == "seeds"
	stream := parts[4] == "seeds" && r.Method == http.MethodGet && strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
	if (parts[4] == "seeds" && !post && !stream) || (parts[4] != "seeds" && r.Method != http.MethodGet) {
		allow := "GET"
		if parts[4] == "seeds" {
			allow = "POST, GET"
		}
		w.Header().Set("Allow", allow)
		http.Error(w, "method not supported", 405)
		return true
	}
	values := r.Header.Values("Trunk-Branch")
	if parts[4] == "roots" && r.URL.Query().Has("branch") {
		selected := r.URL.Query()["branch"]
		if len(selected) != 1 || (len(values) != 0 && (len(values) != 1 || selected[0] != values[0])) {
			http.Error(w, "conflicting subscription branch", 400)
			return true
		}
		values = selected
	}
	if len(values) != 1 {
		http.Error(w, "select one registered branch", 400)
		return true
	}
	id, err := strconv.ParseUint(values[0], 10, 16)
	if err != nil || id == 0 {
		http.Error(w, "invalid Trunk-Branch", 400)
		return true
	}
	route, err := taxonomyRoute(p.config.Root, uint16(id))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return true
	}
	if route.Cache != p.config.Cache || strings.Join(parts[:4], "/") != route.Source {
		http.Error(w, "Trunk-Branch does not match resource", 400)
		return true
	}
	if supplied := r.Header.Values("Definition-Version"); (post || stream || parts[4] == "roots") && (len(supplied) > 1 || (len(supplied) == 1 && supplied[0] != route.Digest)) {
		http.Error(w, "Trunk-Branch Definition mismatch", 400)
		return true
	}
	if d := r.URL.Query().Get("definition"); d != "" && d != route.Digest {
		http.Error(w, "Trunk-Branch Definition mismatch", 400)
		return true
	}
	reader, err := taxonomyReader(p.config.Root)
	if err == nil {
		var definition *seedNativeDefinition
		definition, err = reader.prescribe(p.config.Root, route.Definition)
		if definition != nil {
			definition.close()
		}
	}
	if err != nil {
		http.Error(w, err.Error(), 409)
		return true
	}
	if parts[4] == "roots" {
		p.subscribe(w, r, route)
		return true
	}
	if stream {
		p.streamSeeds(w, r, route)
		return true
	}
	if post {
		p.postRegisteredSeed(w, r, route)
		return true
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		http.Error(w, "endpoint closed", 503)
		return true
	}
	p.commitWork.Add(1)
	p.mu.Unlock()
	defer p.commitWork.Done()
	if parts[4] == "timelines" && r.URL.Query().Has("bounds") {
		query := r.URL.Query()
		if len(query["bounds"]) != 1 || query.Get("bounds") != "1" || len(query) != 1 {
			http.Error(w, "bounds selects the whole registered timeline", http.StatusBadRequest)
			return true
		}
		bounds, err := p.node.TimeBounds(route.Branch)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return true
		}
		defer bounds.Close()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Trunk-Branch", values[0])
		w.Write(bounds.Bytes(0, 0))
		return true
	}
	now := p.config.Now()
	var ids []uint32
	var ranges []summaryRange
	if parts[4] == "strings" {
		q, e := url.ParseQuery(r.URL.RawQuery)
		values := q["id"]
		if e != nil || len(values) == 0 || len(values) > 256 || len(q) > 1 && !(len(q) == 2 && len(q["definition"]) == 1) {
			http.Error(w, "request 1 to 256 string ordinals", 400)
			return true
		}
		seen := map[uint64]bool{}
		for _, v := range values {
			id, e := strconv.ParseUint(v, 10, 32)
			if e != nil || seen[id] {
				http.Error(w, "invalid or duplicate string ordinal", 400)
				return true
			}
			ids = append(ids, uint32(id))
			seen[id] = true
		}
	} else if parts[4] == "summaries" {
		ranges, err = summaryRanges(r.URL.Query())
	} else if parts[4] == "indexes" {
		_, err = parseRawParameters(r, "indexes")
	} else if parts[4] == "timelines" {
		ranges, err = registeredTimelineRanges(r, now)
	} else {
		ranges, err = registeredTimeRanges(r, now)
	}
	if err != nil {
		http.Error(w, err.Error(), 400)
		return true
	}
	snapshot, buffer, lease, err := p.registeredSnapshot(route, parts[4], ranges, uint64(now.UnixMilli()))
	var live []*seedNativeOutput
	var summaries map[uint64][]byte
	var registryTag string
	if lease != nil {
		defer lease.Close()
		for i := 0; i < lease.Live; i++ {
			live = append(live, &seedNativeOutput{bytes: lease.Bytes(4, i)})
		}
		if parts[4] == "summaries" {
			summaries = map[uint64][]byte{}
			for i := 0; i < lease.Summaries; i++ {
				summaries[lease.Number(i)] = lease.Bytes(5, i)
			}
			registryTag = fmt.Sprintf("\"%x\"", sha256.Sum256(lease.Bytes(0, 0)))
		}
	}
	if err != nil {
		if snapshot != nil {
			snapshot.release()
		}
		if buffer != nil {
			buffer.close()
		}
		http.Error(w, err.Error(), 503)
		return true
	}
	defer snapshot.release()
	if buffer != nil {
		defer buffer.close()
	}
	w.Header().Set("Trunk-Branch", values[0])
	w.Header().Set("Trunk-Source", strings.Join(parts[:4], "/"))
	w.Header().Set("Definition-Version", route.Digest)
	w.Header().Set("Trunk-Generation", fmt.Sprintf("%x", snapshot.header.Generation))
	w.Header().Set("Trunk-Generation-Sequence", "1")
	if snapshot.packs != nil {
		w.Header().Set("Trunk-Beds-Length", strconv.FormatUint(snapshot.publication.length, 10))
		w.Header().Set("Trunk-Beds-Packs", strconv.FormatUint(snapshot.publication.count, 10))
	}
	w.Header().Set("Cache-Control", "no-store")
	if parts[4] == "strings" {
		if want := r.Header.Get("Trunk-Generation"); want == "" || want != w.Header().Get("Trunk-Generation") {
			http.Error(w, "BOX generation mismatch", http.StatusPreconditionFailed)
			return true
		}
		data, e := snapshot.strings.entries(ids)
		if e != nil {
			http.Error(w, e.Error(), 404)
			return true
		}
		defer data.close()
		w.Header().Set("Content-Type", "application/json")
		w.Write(data.bytes)
		return true
	}
	if parts[4] == "summaries" {
		w.Header().Set("Trunk-Registry", registryTag)
		if expected := r.Header.Get("Trunk-Registry"); expected != "" && expected != registryTag {
			http.Error(w, "summary registry changed; refresh trunk.trie", http.StatusPreconditionFailed)
			return true
		}
		w.Header().Set("Trunk-Path", route.Storage.Directory+"/"+route.Storage.Stem+".summ")
		err = snapshot.writeSummaries(w, r, ranges, buffer, summaries)
	} else if parts[4] == "indexes" {
		if r.URL.Query().Get("generation") != fmt.Sprintf("%x", snapshot.header.Generation) {
			http.Error(w, "index generation changed", 409)
			return true
		}
		err = snapshot.writeIndexes(w, r, buffer)
	} else if parts[4] == "timelines" {
		w.Header().Set("Trunk-Path", route.Storage.Directory+"/"+route.Storage.Stem+".time")
		if counts := lease.Bytes(6, 0); len(counts) != 0 {
			w.Header().Set("Trunk-Open-Counts", string(counts))
			w.Header().Add("Access-Control-Expose-Headers", "Trunk-Open-Counts")
		}
		err = snapshot.writeTimeline(w, r, ranges)
	} else {
		err = snapshot.writeSeeds(w, r, ranges, now, buffer, live)
	}
	if err != nil {
		http.Error(w, err.Error(), 409)
	}
	return true
}

func (s *registeredReadSnapshot) writeIndexes(w http.ResponseWriter, r *http.Request, buffer *nativeBuffer) error {
	if s.index == nil {
		return fmt.Errorf("published index file unavailable")
	}
	result, err := s.timeline.indexSpans(r.URL.Query()["rows"], s.indexSize)
	if err != nil {
		return err
	}
	var spans [][2]uint64
	err = json.Unmarshal(result.bytes, &spans)
	result.close()
	if err != nil {
		return err
	}
	var length uint64
	for _, span := range spans {
		if span[1] > math.MaxInt64 {
			return fmt.Errorf("index span exceeds host range")
		}
		length += span[1] - span[0]
	}
	bytes, err := buffer.resize(32 << 10)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Trunk-Format", "index-segments/0")
	w.Header().Set("Content-Length", strconv.FormatUint(length, 10))
	out := summaryContextWriter{r.Context(), w}
	for _, span := range spans {
		if _, err = io.CopyBuffer(out, io.NewSectionReader(s.index.file, int64(span[0]), int64(span[1]-span[0])), bytes); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	return nil
}

// Preserve the existing timeline request contract, including its next-hour
// default and ordered, disjoint ranges. This is transport selection only.
func registeredTimelineRanges(r *http.Request, now time.Time) ([]summaryRange, error) {
	q, err := parseRawParameters(r, "timelines")
	if err != nil {
		return nil, err
	}
	values := r.URL.Query()["range"]
	if len(values) == 0 {
		end := q["endTime"]
		if end == "" {
			end = strconv.FormatInt(now.UTC().Truncate(time.Hour).Add(time.Hour).UnixMilli(), 10)
		}
		values = []string{q["beginTime"] + "," + end}
	}
	if len(values) > 128 {
		return nil, fmt.Errorf("too many timeline ranges")
	}
	ranges := make([]summaryRange, 0, len(values))
	var last, total int64
	for i, value := range values {
		parts := strings.Split(value, ",")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid timeline range")
		}
		a, firstErr := strconv.ParseInt(parts[0], 10, 64)
		z, lastErr := strconv.ParseInt(parts[1], 10, 64)
		if firstErr != nil || lastErr != nil || a < 0 || a >= z || a%TimeHourMS != 0 || z%TimeHourMS != 0 || i > 0 && a < last || z-a > 8*366*24*TimeHourMS {
			return nil, fmt.Errorf("invalid timeline range")
		}
		total += (z - a) / TimeHourMS
		if total > 8*366*24 {
			return nil, fmt.Errorf("timeline selection exceeds limit")
		}
		ranges = append(ranges, summaryRange{a, z})
		last = z
	}
	return ranges, nil
}

func registeredTimeRanges(r *http.Request, now time.Time) ([]summaryRange, error) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	for _, pair := range [][2]string{{"beginTime", "from"}, {"endTime", "to"}} {
		if q.Has(pair[1]) {
			return nil, fmt.Errorf("use beginTime and endTime")
		}
		if !q.Has(pair[0]) {
			continue
		}
		values := q[pair[0]]
		if len(values) != 1 {
			return nil, fmt.Errorf("duplicate time parameter")
		}
		ms, e := strconv.ParseInt(values[0], 10, 64)
		if e != nil || ms < 0 || ms > 1<<48-1 {
			return nil, fmt.Errorf("invalid time parameter")
		}
		q.Del(pair[0])
		q.Set(pair[1], time.UnixMilli(ms).UTC().Format(time.RFC3339Nano))
	}
	if q.Has("from") && !q.Has("to") {
		q.Set("to", "now")
	}
	return seedRanges(q, now)
}

func (s *registeredReadSnapshot) writeTimeline(w http.ResponseWriter, r *http.Request, ranges []summaryRange) error {
	type span struct{ Start, End int }
	spans := []span{{0, TimeTrieHeader}}
	length := TimeTrieHeader
	for _, window := range ranges {
		result, err := s.timeline.spans(uint64(window.from), uint64(window.to))
		if err != nil {
			return err
		}
		var selected []span
		err = json.Unmarshal(result.bytes, &selected)
		result.close()
		if err != nil {
			return err
		}
		for _, part := range selected {
			length += part.End - part.Start
		}
		spans = append(spans, selected...)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Trunk-Format", "time-trie/0")
	w.Header().Set("Content-Length", strconv.Itoa(length))
	out := summaryContextWriter{r.Context(), w}
	for _, part := range spans {
		if _, err := out.Write(s.timeline.bytes()[part.Start:part.End]); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	return nil
}

func (s *registeredReadSnapshot) writeSeeds(w http.ResponseWriter, r *http.Request, ranges []summaryRange, now time.Time, buffer *nativeBuffer, live []*seedNativeOutput) error {
	type span struct{ start, end uint64 }
	var spans []span
	var total uint64
	for _, window := range ranges {
		selection, err := s.timeline.selectRange(uint64(window.from), uint64(window.to))
		if err != nil {
			return err
		}
		for _, leaf := range selection.leaves {
			if leaf.Count == 0 {
				continue
			}
			if s.packs == nil {
				selection.close()
				return fmt.Errorf("published hour has no PACKS file")
			}
			address := leaf.Packs
			if !s.storage.Packed {
				address = leaf.Beds
			}
			start, end, e := storedSegmentRange(s.packs.file, 0, address)
			if e != nil {
				selection.close()
				return e
			}
			if end > s.publication.bodyEnd || end > math.MaxInt64 || total > math.MaxInt64-(end-start) {
				selection.close()
				return fmt.Errorf("Pack exceeds published file extent")
			}
			total += end - start
			if len(spans) > 0 && spans[len(spans)-1].end == start {
				spans[len(spans)-1].end = end
			} else {
				spans = append(spans, span{start, end})
			}
		}
		selection.close()
	}
	bytes, err := buffer.resize(32 << 10)
	if err != nil {
		return err
	}
	for _, pack := range live {
		total += uint64(len(pack.bytes))
	}
	w.Header().Set("Content-Type", "application/pil")
	w.Header().Set("Trunk-Format", "seed-packs/0")
	w.Header().Set("Trunk-Sealed-Through", time.UnixMilli(int64(s.header.Sealed)).UTC().Format(time.RFC3339Nano))
	w.Header().Set("Trunk-Retained-From", time.UnixMilli(int64(s.header.First)).UTC().Format(time.RFC3339Nano))
	w.Header().Set("Trunk-Window-To", strconv.FormatInt(now.UnixMilli(), 10))
	w.Header().Set("Content-Length", strconv.FormatUint(total, 10))
	out := summaryContextWriter{r.Context(), w}
	for _, part := range spans {
		if _, err = io.CopyBuffer(out, io.NewSectionReader(s.packs.file, int64(part.start), int64(part.end-part.start)), bytes); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	for _, pack := range live {
		if _, err = out.Write(pack.bytes); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	return nil
}

// The resident Rust footer resolves daily PAC extents. Copy the selected bytes
// using the same pinned file, pool, and adjacent-range coalescing as /packs.
func (s *registeredReadSnapshot) writeSummaries(w http.ResponseWriter, r *http.Request, ranges []summaryRange, buffer *nativeBuffer, live map[uint64][]byte) error {
	type part struct {
		start, end uint64
		raw        []byte
	}
	var parts []part
	var total uint64
	count := 0
	for _, window := range ranges {
		for day := uint64(window.from); day < uint64(window.to); day += summaryDayMilliseconds {
			count++
			if raw, ok := live[day]; ok {
				parts = append(parts, part{raw: raw})
				total += uint64(len(raw))
				continue
			}
			if s.summaries == nil || s.summaryIndex == nil {
				return fmt.Errorf("summary storage is not published")
			}
			selected, err := s.summaryIndex.spans(s.timeline, []summaryRange{{from: int64(day), to: int64(day + summaryDayMilliseconds)}})
			if err != nil {
				return err
			}
			for _, span := range selected {
				if span.Start > span.End || span.End > s.summarySize || span.End > math.MaxInt64 || total > math.MaxInt64-(span.End-span.Start) {
					return fmt.Errorf("summary Pack exceeds published extent")
				}
				total += span.End - span.Start
				if len(parts) > 0 && parts[len(parts)-1].raw == nil && parts[len(parts)-1].end == span.Start {
					parts[len(parts)-1].end = span.End
				} else {
					parts = append(parts, part{start: span.Start, end: span.End})
				}
			}
		}
	}
	bytes, err := buffer.resize(32 << 10)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/pil")
	w.Header().Set("Trunk-Format", "summary-packs/0")
	w.Header().Set("Trunk-Pack-Count", strconv.Itoa(count))
	w.Header().Set("Content-Length", strconv.FormatUint(total, 10))
	out := summaryContextWriter{r.Context(), w}
	for _, part := range parts {
		if part.raw != nil {
			_, err = out.Write(part.raw)
		} else {
			_, err = io.CopyBuffer(out, io.NewSectionReader(s.summaries.file, int64(part.start), int64(part.end-part.start)), bytes)
		}
		if err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	return nil
}
