package cache

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

type summaryRange struct{ from, to int64 }

// Ranges are complete fixed-offset days, half open. Limit selected days, not the span
// between sparse gaps. Canonicalize duplicates/overlaps before any storage I/O.
func summaryRanges(q url.Values) ([]summaryRange, error) {
	if len(q["range"]) == 0 || len(q["range"]) > summaryBatchLimit {
		return nil, fmt.Errorf("expected 1..1024 summary ranges")
	}
	if q.Has("beginTime") || q.Has("endTime") || q.Has("begin") || q.Has("end") || q.Has("from") || q.Has("to") {
		return nil, fmt.Errorf("range cannot be combined with time bounds")
	}
	var ranges []summaryRange
	for _, raw := range q["range"] {
		p := strings.Split(raw, ",")
		if len(p) != 2 {
			return nil, fmt.Errorf("range must be begin_ms,end_ms")
		}
		a, e := strconv.ParseInt(p[0], 10, 64)
		z, f := strconv.ParseInt(p[1], 10, 64)
		if e != nil || f != nil || a < 0 || z <= a || z > 1<<48-1 || a%TimeHourMS != 0 || (z-a)%summaryDayMS != 0 {
			return nil, fmt.Errorf("invalid whole-day range")
		}
		ranges = append(ranges, summaryRange{a, z})
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
	var days int64
	for _, r := range out {
		days += (r.to - r.from) / summaryDayMS
	}
	if days > summaryBatchLimit {
		return nil, fmt.Errorf("summary request exceeds 1024 days")
	}
	return out, nil
}
func summaryContains(ranges []summaryRange, day int64) bool {
	i := sort.Search(len(ranges), func(i int) bool { return ranges[i].to > day })
	return i < len(ranges) && ranges[i].from <= day
}
