package cache

import (
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIngressEntropyThresholds(t *testing.T) {
	if ingressEntropyBytes(0) != 5 || ingressEntropyBytes(1) != 5 {
		t.Fatal("minimum")
	}
	for i, limit := range entropyThresholds {
		bits := min(40+8*i, 74)
		probability := func(n uint64) float64 { return float64(n) * float64(n-1) / (2 * math.Ldexp(1, bits)) }
		if probability(limit) > 1e-9 || probability(limit+1) <= 1e-9 {
			t.Fatalf("incorrect threshold %d: %d", bits, limit)
		}
		if ingressEntropyBytes(limit) != 5+i {
			t.Fatal("width at threshold", limit)
		}
		if i < 5 && ingressEntropyBytes(limit+1) != 6+i {
			t.Fatal("growth at threshold", limit)
		}
	}
	if ingressEntropyBytes(math.MaxUint64) != 10 {
		t.Fatal("maximum")
	}
}

func TestIngressMeterConcurrentAndBuckets(t *testing.T) {
	m := &ingressMeter{started: time.Now()}
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 1000; j++ {
				m.recordAt(time.Millisecond)
			}
		}()
	}
	workers.Wait()
	if m.peakMS.Load() != 8000 || m.peakHour.Load() != 8000 {
		t.Fatal("lost ingress", m.peakMS.Load(), m.peakHour.Load())
	}
	m.recordAt(2 * time.Millisecond)
	if m.peakMS.Load() != 8000 || m.peakHour.Load() != 8001 {
		t.Fatal("millisecond reset")
	}
	m.recordAt(time.Hour)
	if m.hour.Load()&((1<<40)-1) != 1 || m.peakHour.Load() != 8001 {
		t.Fatal("hour reset")
	}
	// A delayed writer cannot reset a newer bucket or lower the peak.
	m.recordAt(time.Millisecond)
	if m.hour.Load()&((1<<40)-1) != 2 || m.peakHour.Load() != 8001 {
		t.Fatal("delayed writer")
	}
	if n := testing.AllocsPerRun(100, func() { m.recordAt(2 * time.Hour) }); n != 0 {
		t.Fatal("meter allocated", n)
	}
}

func TestIngressCounterSaturation(t *testing.T) {
	var word atomic.Uint64
	word.Store(5<<24 | ((1 << 24) - 1))
	if incrementIngress(&word, 5, 24) != (1<<24)-1 {
		t.Fatal("count wrapped")
	}
	if incrementIngress(&word, 6, 24) != 1 {
		t.Fatal("next bucket")
	}
}

func TestIngressMeterTrunkScopeAndHeaders(t *testing.T) {
	root := t.TempDir()
	m := trunkIngress(root)
	if m != trunkIngress(root+"/.") || m == trunkIngress(t.TempDir()) {
		t.Fatal("incorrect trunk scope")
	}
	m.recordAt(0)
	h := http.Header{}
	m.headers(h)
	if !strings.Contains(h.Get("Trunk-Ingress"), "peak-ms=1, peak-hour=1, entropy-bytes=5") {
		t.Fatal(h)
	}
	if h.Get("Access-Control-Expose-Headers") != "Trunk-Ingress" {
		t.Fatal("header not exposed")
	}
}
