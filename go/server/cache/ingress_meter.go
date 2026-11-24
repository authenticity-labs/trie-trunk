package cache

import (
	"math"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// One constant-space arrival meter per trunk. It is deliberately not replayed,
// persisted, or used as an identity counter. Recovery is not new ingress.
type ingressMeter struct {
	started          time.Time
	ms, hour         atomic.Uint64
	peakMS, peakHour atomic.Uint64
}

var ingressMeters sync.Map

func trunkIngress(root string) *ingressMeter {
	key, err := filepath.Abs(root)
	if err != nil {
		key = filepath.Clean(root)
	}
	if existing, ok := ingressMeters.Load(key); ok {
		return existing.(*ingressMeter)
	}
	value, _ := ingressMeters.LoadOrStore(key, &ingressMeter{started: time.Now()})
	return value.(*ingressMeter)
}

// Pack bucket and count in one atomic word, avoiding locks and allocations.
// Delayed callers join the newer bucket: this is an arrival-rate estimate.
// Saturation keeps the maximum rather than wrapping to a smaller estimate.
func incrementIngress(word *atomic.Uint64, bucket uint64, countBits uint) uint64 {
	mask := uint64(1)<<countBits - 1
	for {
		old := word.Load()
		previous, count := old>>countBits, old&mask
		if bucket > previous {
			count = 1
		} else {
			bucket = previous
			if count < mask {
				count++
			}
		}
		if word.CompareAndSwap(old, bucket<<countBits|count) {
			return count
		}
	}
}

func raiseIngressPeak(peak *atomic.Uint64, count uint64) {
	for old := peak.Load(); count > old; old = peak.Load() {
		if peak.CompareAndSwap(old, count) {
			return
		}
	}
}

func (m *ingressMeter) record() { m.recordAt(time.Since(m.started)) }
func (m *ingressMeter) recordAt(elapsed time.Duration) {
	if elapsed < 0 {
		elapsed = 0
	}
	raiseIngressPeak(&m.peakMS, incrementIngress(&m.ms, uint64(elapsed/time.Millisecond), 24))
	raiseIngressPeak(&m.peakHour, incrementIngress(&m.hour, uint64(elapsed/time.Hour), 40))
}

// Birthday union bound: n*(n-1)/(2*2^bits) <= 1e-9 per millisecond.
// Calculate thresholds once; ingress uses integer comparisons only. Ten bytes
// carry 74 bits because the UUID version and variant occupy the other six.
var entropyThresholds = func() [6]uint64 {
	var limits [6]uint64
	for i := range limits {
		bits := min(40+i*8, 74)
		limits[i] = uint64((1 + math.Sqrt(1+8e-9*math.Ldexp(1, bits))) / 2)
	}
	return limits
}()

func ingressEntropyBytes(peak uint64) int {
	for i, limit := range entropyThresholds {
		if peak <= limit {
			return i + 5
		}
	}
	return 10 // Maximum supported entropy, not a uniqueness guarantee.
}

func (m *ingressMeter) entropyBytes() int {
	// Include the prospective arrival when crossing a width threshold.
	return ingressEntropyBytes(m.peakMS.Load() + 1)
}

func (m *ingressMeter) headers(h http.Header) {
	h.Set("Trunk-Ingress", "since="+strconv.FormatInt(m.started.UnixMilli(), 10)+
		", peak-ms="+strconv.FormatUint(m.peakMS.Load(), 10)+
		", peak-hour="+strconv.FormatUint(m.peakHour.Load(), 10)+
		", entropy-bytes="+strconv.Itoa(m.entropyBytes()))
	h.Add("Access-Control-Expose-Headers", "Trunk-Ingress")
}
