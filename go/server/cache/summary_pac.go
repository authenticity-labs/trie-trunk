package cache

import (
	"bytes"
	_ "embed"
	"fmt"
	"sync"

	pil "github.com/authenticity-labs/pill-packer/go/codec"
)

var summaryPACOnce sync.Once
var summaryPACLayout *pil.Layout
var summaryPACError error

//go:embed summary.rx
var summaryDefinition []byte

func summaryLayout() (*pil.Layout, error) {
	summaryPACOnce.Do(func() {
		summaryPACLayout, summaryPACError = pil.Parse(summaryDefinition, pil.IdentityProfile{Primary: -1})
	})
	return summaryPACLayout, summaryPACError
}

func encodeSummaryPAC(day, from, to int64, sums []Summary, compressed bool, level int) ([]byte, error) {
	return encodeSummaryPACCodec(day, from, to, sums, compressed, PACPolicy{Level: &level})
}
func encodeSummaryPACCodec(day, from, to int64, sums []Summary, compressed bool, policy PACPolicy) ([]byte, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	if day < 0 || day%TimeHourMS != 0 || from < day || to > day+summaryDayMS || from > to || from%TimeHourMS != 0 || to%TimeHourMS != 0 {
		return nil, fmt.Errorf("invalid summary day coverage")
	}
	if len(sums) == 0 || len(sums) > 24 {
		return nil, fmt.Errorf("invalid summary member count")
	}
	l, err := summaryLayout()
	if err != nil {
		return nil, err
	}
	encoding, err := chooseSummaryEncoding(sums, compressed)
	if err != nil {
		return nil, err
	}
	encodingBytes := make([]byte, summaryEncodingSize)
	encoding.put(encodingBytes)
	frames := make([][]byte, 0, len(sums))
	var previousHour int64
	for i, sum := range sums {
		if _, err = validateSummary(sum); err != nil {
			return nil, err
		}
		hour := utcHour(sum.Columns.Period[0])
		if hour < from || hour >= to || hour >= day+summaryDayMS {
			return nil, fmt.Errorf("summary outside day coverage")
		}
		if i > 0 && hour <= previousHour {
			return nil, fmt.Errorf("unordered or duplicate summary hour")
		}
		previousHour = hour
		columns, e := encodeSummaryColumns(sum, &encoding)
		if e != nil {
			return nil, e
		}
		if compressed {
			if e = shuffleSummary(columns, encoding, false); e != nil {
				return nil, e
			}
		}
		values := []pil.Value{
			pil.Uint(uint64(day), 8),
			pil.Uint(uint64(from), 8),
			pil.Uint(uint64(to), 8),
			pil.Uint(uint64(hour), 8),
			pil.Uint(boolUint(compressed), 1),
			{State: pil.Offset, Bytes: encodingBytes},
			{State: pil.Offset, Bytes: columns},
		}
		frame, e := l.Encode(nil, values)
		if e != nil {
			return nil, e
		}
		frames = append(frames, frame)
	}
	pack, err := pil.FactorPAC(l, frames, nil, false)
	if err != nil {
		return nil, err
	}
	rollup, err := dailyRollup(sums)
	if err != nil {
		return nil, err
	}
	pack.Headers = map[string][]byte{"rollup": summaryRollupHeader(rollup)}
	pack.AddressBytes = 2
	pack.Bytes, err = pack.Marshal()
	if err != nil {
		return nil, err
	}
	if len(pack.Bytes) > maxSummaryDay {
		return nil, fmt.Errorf("summary PAC exceeds limit")
	}
	if compressed {
		packed, e := pil.CompressPACAlgorithm(pack.Bytes, policy.algorithm(), policy.level(), false)
		if e != nil {
			return nil, e
		}
		if len(packed) > maxSummaryDay+16 {
			return nil, fmt.Errorf("compressed summary PAC exceeds limit")
		}
		return packed, nil
	}
	return pack.Bytes, nil
}

func decodeSummaryPAC(raw []byte) (day, from, to int64, sums []Summary, err error) {
	day, from, to, sums, _, err = decodeSummaryPACInfo(raw)
	return
}

func decodeSummaryPACInfo(raw []byte) (day, from, to int64, sums []Summary, sealed bool, err error) {
	bad := func() error { return fmt.Errorf("invalid daily summary PAC") }
	plain, err := pil.ExpandPAC(raw)
	if err != nil {
		return 0, 0, 0, nil, false, err
	}
	l, err := summaryLayout()
	if err != nil {
		return 0, 0, 0, nil, false, err
	}
	pack, err := pil.ParsePAC(l, plain, nil)
	if err != nil || len(pack.Common) != 0 || len(pack.Extensions) != 0 || pack.Nested || len(pack.Headers) != 1 || len(pack.Headers["rollup"]) != 49 || len(pack.Members) > 24 {
		return 0, 0, 0, nil, false, bad()
	}
	var frame pil.Frame
	for i, member := range pack.Members {
		if err = l.ScanPlain(member, &frame); err != nil {
			return 0, 0, 0, nil, false, err
		}
		values := frame.Values
		if len(values) != 7 || values[0].State != pil.Direct || values[1].State != pil.Direct || values[2].State != pil.Direct || values[3].State != pil.Direct || values[4].State != pil.Direct || values[5].State != pil.Offset || values[6].State != pil.Offset {
			return 0, 0, 0, nil, false, bad()
		}
		d, f, z, hour := int64(pil.Unsigned(values[0].Bytes)), int64(pil.Unsigned(values[1].Bytes)), int64(pil.Unsigned(values[2].Bytes)), int64(pil.Unsigned(values[3].Bytes))
		memberSealed := pil.Unsigned(values[4].Bytes) == 1
		if len(values[4].Bytes) != 1 || pil.Unsigned(values[4].Bytes) > 1 {
			return 0, 0, 0, nil, false, bad()
		}
		if i == 0 {
			day, from, to, sealed = d, f, z, memberSealed
		} else if d != day || f != from || z != to || memberSealed != sealed {
			return 0, 0, 0, nil, false, bad()
		}
		if d < 0 || d%TimeHourMS != 0 || f < d || z > d+summaryDayMS || f > z || f%TimeHourMS != 0 || z%TimeHourMS != 0 || hour < f || hour >= z || hour >= d+summaryDayMS {
			return 0, 0, 0, nil, false, bad()
		}
		if len(values[5].Bytes) != summaryEncodingSize {
			return 0, 0, 0, nil, false, bad()
		}
		encoding, e := readSummaryEncoding(values[5].Bytes)
		if e != nil {
			return 0, 0, 0, nil, false, bad()
		}
		columns := values[6].Bytes
		if sealed {
			if e = validateShuffledSummary(columns, encoding); e != nil {
				return 0, 0, 0, nil, false, bad()
			}
			columns = bytes.Clone(columns)
			if e = shuffleSummary(columns, encoding, true); e != nil {
				return 0, 0, 0, nil, false, bad()
			}
		}
		var sum Summary
		if err = decodeSummaryColumns(columns, &sum, &encoding); err != nil || utcHour(sum.Columns.Period[0]) != hour {
			return 0, 0, 0, nil, false, bad()
		}
		if i > 0 && utcHour(sums[i-1].Columns.Period[0]) >= hour {
			return 0, 0, 0, nil, false, bad()
		}
		sums = append(sums, sum)
	}
	if len(sums) == 0 {
		return 0, 0, 0, nil, false, bad()
	}
	rollup, e := dailyRollup(sums)
	if e != nil || !bytes.Equal(pack.Headers["rollup"], summaryRollupHeader(rollup)) {
		err = bad()
		return
	}
	return
}

// Six exact U64 totals plus a statistics-completeness byte. PAC application
// headers own the daily aggregate; no transport envelope duplicates it.
func summaryRollupHeader(r CalendarRollup) []byte {
	b := make([]byte, 49)
	for i, n := range []uint64{r.Count, r.DurationCount, r.Sum, r.Min, r.Max, r.Errors} {
		order.PutUint64(b[i*8:], n)
	}
	b[48] = byte(boolUint(r.StatisticsComplete))
	return b
}

func boolUint(v bool) uint64 {
	if v {
		return 1
	}
	return 0
}
