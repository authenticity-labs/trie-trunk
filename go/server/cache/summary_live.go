package cache

// Writer-maintained five-minute records. Publishing copies only these small
// values; request handling never visits PILs or calculates aggregate values.
import (
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

type liveSummaryBucket struct {
	Aggregate
	Errors, Revision uint64
}
type liveSummaryHour struct {
	Fields  fieldHour
	Buckets [12]liveSummaryBucket
	Total   liveSummaryBucket
}

func (s *Store) accumulateLiveSummary(h *liveSummaryHour, branch Branch, id [16]byte, values []Value) error {
	hour := utcHour(eventTime(id[:]))
	record := int((eventTime(id[:]) - hour) / sumBucketMS)
	var duration *uint64
	if s.layout.Duration >= 0 && values[s.layout.Duration].State == Direct {
		n := unsigned(values[s.layout.Duration].Bytes)
		duration = &n
	}
	if e := h.Total.add(duration); e != nil {
		return e
	}
	if e := h.Buckets[record].add(duration); e != nil {
		return e
	}
	h.Total.Revision++
	h.Buckets[record].Revision = h.Total.Revision
	if branch == Error {
		h.Total.Errors++
		h.Buckets[record].Errors++
	}
	return nil
}
func (h liveSummaryHour) prefixRevision(record int) uint64 {
	var n uint64
	for _, b := range h.Buckets[:record] {
		n = max(n, b.Revision)
	}
	return n
}

var liveSummaryOnce sync.Once
var liveSummaryLayout *pil.Layout
var liveSummaryError error

func summaryBucketLayout() (*pil.Layout, error) {
	liveSummaryOnce.Do(func() {
		d := pil.Definition{Identity: "trie-trunk/summary-bucket"}
		for _, name := range []string{"count", "duration_count", "sum", "min", "max", "errors"} {
			d.Fields = append(d.Fields, pil.Field{Name: name, Type: "U64"})
		}
		liveSummaryLayout, liveSummaryError = pil.Compile(d)
	})
	return liveSummaryLayout, liveSummaryError
}

type summaryCheckpoint struct {
	record   int
	revision uint64
}

func liveSummaryParameters(q url.Values, now int64) (from, to int64, checkpoints map[int64]summaryCheckpoint, err error) {
	parts := strings.Split(q.Get("live"), ",")
	if len(parts) < 1 || len(parts) > 2 {
		return 0, 0, nil, fmt.Errorf("live must be begin_ms[,end_ms]")
	}
	from, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return
	}
	to = now
	if len(parts) == 2 {
		to, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return
		}
	}
	if from < 0 || from%TimeHourMS != 0 || to <= from || to > 1<<48-1 {
		return 0, 0, nil, fmt.Errorf("invalid live summary window")
	}
	checkpoints = map[int64]summaryCheckpoint{}
	for _, raw := range q["checkpoint"] {
		p := strings.Split(raw, ",")
		if len(p) != 3 {
			return 0, 0, nil, fmt.Errorf("invalid summary checkpoint")
		}
		hour, e := strconv.ParseInt(p[0], 10, 64)
		record, f := strconv.Atoi(p[1])
		revision, g := strconv.ParseUint(p[2], 10, 64)
		if e != nil || f != nil || g != nil || hour%TimeHourMS != 0 || record < 0 || record > 12 {
			return 0, 0, nil, fmt.Errorf("invalid summary checkpoint")
		}
		if _, ok := checkpoints[hour]; ok {
			return 0, 0, nil, fmt.Errorf("duplicate summary checkpoint")
		}
		checkpoints[hour] = summaryCheckpoint{record, revision}
	}
	return
}
func encodeLiveSummary(hour, observed int64, h liveSummaryHour, checkpoint summaryCheckpoint, plans ...*aggregatePlan) ([]byte, error) {
	l, e := summaryBucketLayout()
	if e != nil {
		return nil, e
	}
	end := int(min(int64(12), max(int64(1), (observed-hour)/sumBucketMS+1)))
	complete := int(min(int64(12), max(int64(0), (observed-hour)/sumBucketMS)))
	first := 0
	if checkpoint.record <= complete && h.prefixRevision(checkpoint.record) == checkpoint.revision {
		first = checkpoint.record
	}
	records := make([][]byte, 0, end-first)
	for _, b := range h.Buckets[first:end] {
		v := []pil.Value{}
		for _, n := range []uint64{b.Count, b.DurationCount, b.Sum, b.Min, b.Max, b.Errors} {
			v = append(v, pil.Uint(n, 8))
		}
		raw, e := l.Encode(nil, v)
		if e != nil {
			return nil, e
		}
		records = append(records, raw)
	}
	pack, e := pil.FactorPAC(l, records, nil, false)
	if e != nil {
		return nil, e
	}
	// Exact hour rollups are maintained at ingress, including buckets omitted by
	// the checkpoint. The checkpoint validates a prefix, not an event timestamp.
	meta := make([]byte, 26)
	order.PutUint64(meta, uint64(hour))
	order.PutUint64(meta[8:], uint64(observed))
	meta[16] = byte(first)
	meta[17] = byte(complete)
	order.PutUint64(meta[18:], h.prefixRevision(complete))
	r := h.Total
	pack.Headers = map[string][]byte{"live-summary": meta, "rollup": summaryRollupHeader(CalendarRollup{Count: r.Count, DurationCount: r.DurationCount, Sum: r.Sum, Min: r.Min, Max: r.Max, Errors: r.Errors})}
	if len(plans) > 0 {
		raw, err := plans[0].pack(h.Fields, first, end)
		if err != nil {
			return nil, err
		}
		pack.Headers["aggregates"] = raw
	}
	pack.RecordWidth = 50
	return pack.Marshal()
}
