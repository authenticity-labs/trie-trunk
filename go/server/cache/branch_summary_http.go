package cache

import (
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"
)

// Copy closed-day PACs unchanged. Unfinished days use an in-memory snapshot of
// existing aggregate columns, without searching event indexes or decoding events.
func (h *HTTP) branchSummaries(w http.ResponseWriter, r *http.Request, file branchFile, from, to time.Time) {
	h.Store.ingress.headers(w.Header())
	if to.Sub(from) > 1024*24*time.Hour {
		http.Error(w, "summary range exceeds 1024 days", 400)
		return
	}
	ranges := []summaryRange{{from.UnixMilli(), to.UnixMilli()}}
	if r.URL.Query().Has("live") && !r.URL.Query().Has("range") {
		ranges = nil
	}
	if r.URL.Query().Has("range") {
		var e error
		ranges, e = summaryRanges(r.URL.Query())
		if e != nil {
			http.Error(w, e.Error(), 400)
			return
		}
	}
	s := h.Store
	for _, span := range ranges {
		if s.summaryDay(file.branch, span.from) != span.from || s.summaryDay(file.branch, span.to) != span.to {
			http.Error(w, "summary range does not match the configured UTC offset", 400)
			return
		}
	}
	snapshot, prepareError := s.prepareResident(file.branch)
	if prepareError != nil {
		http.Error(w, prepareError.Error(), 503)
		return
	}
	defer snapshot.release()
	b := &snapshot.branch
	var livePacks [][]byte
	if r.URL.Query().Has("live") {
		now := s.config.Now().UnixMilli()
		a, z, checkpoints, e := liveSummaryParameters(r.URL.Query(), now)
		if e != nil {
			http.Error(w, e.Error(), 400)
			return
		}
		// Only retained resident ingress evidence is advertised. Missing older
		// sealed summaries are not interpreted as empty hours.
		a = max(a, utcHour(now)-24*TimeHourMS)
		z = min(z, now)
		for hour := a; hour < z; hour += TimeHourMS {
			value, ok := snapshot.liveSummaries[hour]
			if !ok && hour < snapshot.sealed {
				continue
			}
			if value == nil {
				value = &liveSummaryHour{}
			}
			raw, e := encodeLiveSummary(hour, now, *value, checkpoints[hour], s.layout.aggregates)
			if e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			livePacks = append(livePacks, raw)
		}
	}

	transient := map[int64][]byte{}
	grouped := map[int64][]Summary{}
	for _, sum := range b.summaries {
		day := s.summaryDay(file.branch, sum.Columns.Period[0])
		if _, sealed := b.summaryDays[day]; !sealed && summaryContains(ranges, day) {
			grouped[day] = append(grouped[day], sum)
		}
	}
	for day, sums := range grouped {
		raw, e := encodeSummaryDay(day, day, day+summaryDayMS, sums, false)
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		transient[day] = raw
	}

	var days []int64
	for day := range b.summaryDays {
		if summaryContains(ranges, day) {
			days = append(days, day)
		}
	}
	for day := range transient {
		days = append(days, day)
	}
	sort.Slice(days, func(i, j int) bool { return days[i] < days[j] })
	var locs []summaryDayLocation
	for _, day := range days {
		if raw, ok := transient[day]; ok {
			locs = append(locs, summaryDayLocation{Length: uint64(len(raw))})
		} else {
			locs = append(locs, b.summaryDays[day])
		}
	}
	var f *os.File
	if len(locs) > 0 {

		f = b.sum
	}

	generation := fmt.Sprintf("%x", b.generation)
	sequence := snapshot.sequence
	w.Header().Set("Trunk-Format", "summary-packs/0")
	w.Header().Set("Trunk-Path", file.path)
	w.Header().Set("Trunk-Source", h.tap(file.branch))
	w.Header().Set("Definition-Version", fmtDigestNative(s.layout.Digest))
	w.Header().Set("Trunk-Generation", generation)
	w.Header().Set("Trunk-Generation-Sequence", strconv.FormatUint(sequence, 10))
	w.Header().Set("Trunk-Pack-Count", strconv.Itoa(len(locs)+len(livePacks)))
	w.Header().Set("Content-Type", "application/pil")
	w.Header().Set("Cache-Control", "private, no-store")
	lengths := make([]uint32, len(locs)+len(livePacks))
	for i, loc := range locs {
		if loc.Length > pil.PACMaximum {
			http.Error(w, "summary exceeds PAC limit", 500)
			return
		}
		lengths[i] = uint32(loc.Length)
	}
	for i, raw := range livePacks {
		lengths[len(locs)+i] = uint32(len(raw))
	}
	prefix, e := pil.NestedPACHeader(lengths, nil)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatUint(uint64(order.Uint32(prefix)), 10))
	out := summaryContextWriter{r.Context(), w}
	if _, err := out.Write(prefix); err != nil {
		return
	}
	buf := make([]byte, 32<<10)
	for i, loc := range locs {
		if raw, ok := transient[days[i]]; ok {
			if _, err := out.Write(raw); err != nil {
				panic(http.ErrAbortHandler)
			}
			continue
		}
		if _, err := io.CopyBuffer(out, io.NewSectionReader(f, int64(loc.Offset), int64(loc.Length)), buf); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
	for _, raw := range livePacks {
		if _, err := out.Write(raw); err != nil {
			panic(http.ErrAbortHandler)
		}
	}
}
