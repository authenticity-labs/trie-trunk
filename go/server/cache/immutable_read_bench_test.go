package cache

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Identical fixture and request for the saved pre-change implementation and the
// immutable implementation. Excludes fixture construction and network latency.
func BenchmarkImmutableSeedDelivery(b *testing.B) {
	l := testLayout(b, false)
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	now := at.Add(80 * time.Minute)
	s, err := Open(Config{Root: b.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	off := false
	s.policy.SearchIndex = &off
	frames := make([][]byte, 10000)
	for i := range frames {
		frames[i] = testFrame(b, l, at.Add(time.Duration(i)*time.Millisecond), uint32(i+1), i%3 != 0)
	}
	if _, err = s.AppendBatch(Read, frames); err != nil {
		b.Fatal(err)
	}
	if err = s.Flush(); err != nil {
		b.Fatal(err)
	}
	retained := s.NativeMemory().Used
	h := &HTTP{Store: s}
	request := httptest.NewRequest("GET", fmt.Sprintf("/demo/payment/transactions.v1/read/packs?beginTime=%d", at.UnixMilli()), nil)
	request.Header.Set("Accept", "application/pil")
	// Check the real response before timing. A fast empty response or an error
	// must not be reported as improved delivery throughput. Replication must
	// transfer the stored Pack unchanged, without decoding/re-encoding it.
	preflight := httptest.NewRecorder()
	h.ServeHTTP(preflight, request)
	if preflight.Code != http.StatusOK {
		b.Fatalf("delivery preflight: HTTP %d: %s", preflight.Code, preflight.Body.String())
	}
	branch := s.branches[Read]
	var delivered uint64
	for _, run := range branch.runs {
		stored, err := readBytes(branch.packs, run.PacksStart-branch.packsRetired, run.PacksEnd-branch.packsRetired)
		if err != nil {
			b.Fatal(err)
		}
		if len(stored) == 0 || !bytes.Contains(preflight.Body.Bytes(), stored) {
			b.Fatal("delivery did not preserve the stored Pack bytes")
		}
		delivered += run.End - run.Start
	}
	if delivered != uint64(len(frames)) {
		b.Fatalf("delivery fixture contains %d members, expected %d", delivered, len(frames))
	}
	expectedBytes := preflight.Body.Len()
	for _, parallel := range []bool{false, true} {
		name := "Serial"
		if parallel {
			name = "Parallel"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(expectedBytes))
			b.ResetTimer()
			run := func() {
				w := &countedResponse{header: make(http.Header)}
				h.ServeHTTP(w, request)
				if w.bytes != expectedBytes {
					panic(fmt.Sprintf("delivery changed: %d bytes, expected %d", w.bytes, expectedBytes))
				}
			}
			if parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						run()
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					run()
				}
			}
			b.ReportMetric(float64(retained), "resident-B")
		})
	}
}
