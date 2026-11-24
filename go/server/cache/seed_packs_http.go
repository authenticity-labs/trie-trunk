package cache

import (
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// One exact branch per stream. Sparse closed ranges may accompany an open
// beginTime tail. Sealed hours remain whole; only live frames use exact bounds.
func seedRanges(q url.Values, now time.Time) ([]summaryRange, error) {
	for k, v := range q {
		if k != "tap" && k != "definition" && k != "from" && k != "to" && k != "range" && k != "states" {
			return nil, fmt.Errorf("unsupported seed parameter: %s", k)
		}
		if k != "range" && len(v) != 1 {
			return nil, fmt.Errorf("duplicate seed parameter")
		}
	}
	if len(q["range"]) > 128 {
		return nil, fmt.Errorf("too many sparse seed ranges")
	}
	var ranges []summaryRange
	for _, raw := range q["range"] {
		p := strings.Split(raw, ",")
		if len(p) != 2 {
			return nil, fmt.Errorf("range must be begin_ms,end_ms")
		}
		a, e := strconv.ParseInt(p[0], 10, 64)
		z, f := strconv.ParseInt(p[1], 10, 64)
		if e != nil || f != nil || a < 0 || z <= a || z > 1<<48-1 || a%3600000 != 0 || z%3600000 != 0 {
			return nil, fmt.Errorf("invalid UTC hour range")
		}
		ranges = append(ranges, summaryRange{a, z})
	}
	if q.Has("from") {
		a, e := time.Parse(time.RFC3339Nano, q.Get("from"))
		z := now
		var f error
		if q.Get("to") != "now" && q.Has("to") {
			z, f = time.Parse(time.RFC3339Nano, q.Get("to"))
		}
		if e != nil || f != nil || a.UnixMilli() < 0 || !a.Before(z) {
			return nil, fmt.Errorf("invalid seed window")
		}
		ranges = append(ranges, summaryRange{a.UnixMilli(), z.UnixMilli()})
	} else if len(ranges) == 0 || q.Get("to") != "now" && q.Has("to") {
		return nil, fmt.Errorf("missing seed window")
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].from < ranges[j].from })
	out := ranges[:0]
	for _, r := range ranges {
		if len(out) > 0 && r.from <= out[len(out)-1].to {
			out[len(out)-1].to = max(out[len(out)-1].to, r.to)
		} else {
			out = append(out, r)
		}
	}
	var hours int64
	for _, r := range out {
		hours += (r.to - r.from + 3599999) / 3600000
	}
	if hours > 65536 {
		return nil, fmt.Errorf("seed request exceeds 65536 hours")
	}
	return out, nil
}
func (h *HTTP) seedPacks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "GET required", 405)
		return
	}
	apis := h.sources
	if len(apis) == 0 {
		apis = []*HTTP{h}
	}
	q := r.URL.Query()
	var api *HTTP
	var branch Branch
	for _, a := range apis {
		if q.Get("definition") != "" && q.Get("definition") != fmtDigestNative(a.Store.layout.Digest) {
			continue
		}
		bs, e := a.branch(q.Get("tap"))
		if e != nil || len(bs) != 1 {
			continue
		}
		if api != nil {
			http.Error(w, "select a definition for this branch", 409)
			return
		}
		api, branch = a, bs[0]
	}
	if api == nil {
		http.Error(w, "unknown seed branch", 400)
		return
	}
	s := api.Store
	now := s.config.Now()
	ranges, err := seedRanges(q, now)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	type part struct {
		offset, length uint64
		raw            [][]byte
	}
	var parts []part
	var file *os.File
	snapshot, prepareError := s.prepareResident(branch)
	if prepareError != nil {
		http.Error(w, prepareError.Error(), 503)
		return
	}
	defer snapshot.release()
	b := &snapshot.branch

	live := snapshot.live(ranges[0].from, ranges[len(ranges)-1].to)

	last := -1
	for _, span := range ranges {
		i := sort.Search(len(b.runs), func(i int) bool { return b.runs[i].PAC.Placement.To > span.from })
		for ; i < len(b.runs) && b.runs[i].PAC.Placement.From < span.to; i++ {
			if i <= last {
				continue
			}
			run := b.runs[i]
			raw := b.residentPack(run, now)
			if raw == nil {
				file = snapshot.file.file
			}
			parts = append(parts, part{offset: run.PacksStart - b.packsRetired, length: run.PacksEnd - run.PacksStart, raw: raw})
			last = i
		}
	}

	// Retained immutable ingress PILs feed each uncompressed tail pack. No live
	// record is written to the sealed datastore or advertised as a complete hour.
	filtered := live.events[:0]
	for _, e := range live.events {
		ms := eventTime(e.id[:])
		if summaryContains(ranges, ms) {
			filtered = append(filtered, e)
		}
	}
	live.events = filtered
	for at := 0; at < len(live.events); {
		hour := utcHour(eventTime(live.events[at].id[:]))
		end := at + 1
		for end < len(live.events) && utcHour(eventTime(live.events[end].id[:])) == hour {
			end++
		}
		p, owner, e := s.livePack(branch, hour, live.events[at:end])
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		defer owner.close()
		parts = append(parts, part{length: uint64(len(p.Bytes)), raw: [][]byte{p.Bytes}})
		at = end
	}

	lengths := make([]uint32, len(parts))
	for i, p := range parts {
		if p.length > pil.PACMaximum {
			http.Error(w, "seed pack exceeds limit", 500)
			return
		}
		lengths[i] = uint32(p.length)
	}
	context := append([]byte(nil), live.generation[:]...)
	context = order.AppendUint64(context, live.sequence)
	context = order.AppendUint64(context, uint64(live.sealed))
	context = order.AppendUint64(context, uint64(live.retained))
	context = order.AppendUint64(context, uint64(now.UnixMilli()))
	prefix, err := pil.NestedPACHeader(lengths, map[string][]byte{"seed.window": context})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/pil")
	w.Header().Set("Trunk-Format", "seed-packs/0")
	w.Header().Set("Trunk-Source", api.tap(branch))
	w.Header().Set("Definition-Version", fmtDigestNative(s.layout.Digest))
	// Bind the response to the already-pinned snapshot. These use the same
	// headers as timeline/native reads and are available before any Pack arrives.
	w.Header().Set("Trunk-Generation", fmt.Sprintf("%x", live.generation))
	w.Header().Set("Trunk-Generation-Sequence", strconv.FormatUint(live.sequence, 10))
	w.Header().Set("Trunk-Sealed-Through", time.UnixMilli(live.sealed).UTC().Format(time.RFC3339Nano))
	w.Header().Set("Trunk-Retained-From", time.UnixMilli(live.retained).UTC().Format(time.RFC3339Nano))
	w.Header().Set("Trunk-Window-To", strconv.FormatInt(now.UnixMilli(), 10))
	w.Header().Set("Content-Length", strconv.FormatUint(uint64(order.Uint32(prefix)), 10))
	out := summaryContextWriter{r.Context(), w}
	if _, err = out.Write(prefix); err != nil {
		return
	}
	var buf []byte
	if file != nil {
		owner := s.memory.buffer()
		defer owner.close()
		buf, err = owner.resize(32 << 10)
		if err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	for i := 0; i < len(parts); {
		p := parts[i]
		if p.raw != nil {
			for _, raw := range p.raw {
				if _, err = out.Write(raw); err != nil {
					break
				}
			}
			i++
		} else {
			end := p.offset + p.length
			j := i + 1
			for j < len(parts) && parts[j].raw == nil && parts[j].offset == end {
				end += parts[j].length
				j++
			}
			_, err = io.CopyBuffer(out, io.NewSectionReader(file, int64(p.offset), int64(end-p.offset)), buf)
			i = j
		}
		if err != nil {
			panic(http.ErrAbortHandler)
		}
	}
}
