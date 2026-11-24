package cache

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Same prepared records, native input reader, validation, durable append and
// publication on each build. Store creation and shutdown are outside the timer.
// Each operation is 128 events; this includes fsync and is not a codec-only rate.
func BenchmarkIngressCodecBatch(b *testing.B) {
	const count = 128
	at := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	for _, strategy := range []string{"positional", "authored"} {
		for _, size := range []int{78, 4096} {
			b.Run(fmt.Sprintf("%s/payload=%d", strategy, size), func(b *testing.B) {
				var l *Layout
				if strategy == "authored" {
					l = testLogLayout(b)
				} else {
					l = testLayout(b, false)
				}
				defer l.Close()
				frames := make([][]byte, count)
				for i := range frames {
					var values []Value
					if strategy == "authored" {
						values = make([]Value, len(l.Widths))
						values[l.Time] = Uint(uint64(at.UnixMilli()+int64(i)), 8)
						values[l.Duration] = Uint(uint64(i), 8)
						values[fieldIndex(l, "SeverityNumber")] = Uint(9, 1)
						// A schema field with actual typed nested bytes, not a layout shortcut.
						body, e := encoder.Marshal(string(bytes.Repeat([]byte("x"), size)))
						if e != nil {
							b.Fatal(e)
						}
						values[fieldIndex(l, "Body")] = Value{Offset, body}
					} else {
						var f Frame
						if e := l.Scan(testFrame(b, l, at.Add(time.Duration(i)*time.Millisecond), uint32(i+1), false), &f); e != nil {
							b.Fatal(e)
						}
						values = f.Values
						values[5] = Value{Offset, bytes.Repeat([]byte("x"), size)}
					}
					if i%4 == 0 {
						values[l.Duration] = Value{}
					} else if i%4 == 1 && strategy == "positional" {
						values[l.Duration] = Value{State: Null}
					}
					raw, e := l.Encode(nil, values)
					if e != nil {
						b.Fatal(e)
					}
					frames[i] = raw
				}
				root := b.TempDir()
				b.ReportAllocs()
				b.SetBytes(int64(len(frames[0]) * count))
				b.ResetTimer()
				b.StopTimer()
				var peak uint64
				for i := 0; i < b.N; i++ {
					dir := filepath.Join(root, "iteration")
					s, e := Open(Config{Root: dir, Cache: "bench", Tenant: "local", Layout: l, Now: func() time.Time { return at.Add(30 * time.Minute) }})
					if e != nil {
						b.Fatal(e)
					}
					// Open branch before timing, avoiding one-time file creation in this sample.
					if _, e = s.openBranch(Read); e != nil {
						b.Fatal(e)
					}
					stream := nativeIngress{memory: s.memory}
					input := make([]nativeInput, count)
					reader := bytes.NewReader(nil)
					b.StartTimer()
					for j, raw := range frames {
						reader.Reset(raw)
						input[j], e = stream.read(l, reader)
						if e != nil {
							b.Fatal(e)
						}
					}
					n, e := s.appendNativeBatch(Read, input)
					if e != nil || n != count {
						b.Fatalf("accepted=%d err=%v", n, e)
					}
					stream.close()
					b.StopTimer()
					peak = max(peak, s.memory.stats().Peak)
					if e = s.Close(); e != nil {
						b.Fatal(e)
					}
					if e = os.RemoveAll(dir); e != nil {
						b.Fatal(e)
					}
				}
				b.ReportMetric(float64(peak), "native-peak-B")
			})
		}
	}
}

// Isolate the former Encode(nil) -> native allocation -> copy -> Scan sequence
// from disk sync. Both modes use identical decoded fields and must produce
// identical PIL bytes. This is the encoding/handoff stage, not total ingress.
func BenchmarkIngressEncodeHandoff(b *testing.B) {
	for _, size := range []int{78, 4096, 65536} {
		l, raw := ingressFixture(b, size)
		defer l.Close()
		var source Frame
		if err := l.Scan(raw, &source); err != nil {
			b.Fatal(err)
		}
		for _, mode := range []string{"before", "after"} {
			b.Run(fmt.Sprintf("payload=%d/%s", size, mode), func(b *testing.B) {
				memory := newNativeMemory(16 << 20)
				defer memory.close()
				var decoded Frame
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					stream := nativeIngress{memory: memory}
					var out []byte
					var err error
					if mode == "before" {
						out, err = l.Encode(nil, source.Values)
						if err == nil {
							var frame nativeInput
							frame, err = stream.allocate(len(out))
							if err == nil {
								copy(frame.raw, out)
								out = frame.raw
							}
						}
						if err == nil {
							err = l.Scan(out, &decoded)
						}
					} else {
						out, err = l.Layout.EncodeAllocated(source.Values, stream.reserve)
						if err == nil {
							stream.current.used += len(out)
							stream.partial = 0
							err = l.ScanFields(out, &decoded)
						}
					}
					if err != nil || !bytes.Equal(out, raw) {
						b.Fatal("handoff changed bytes", err)
					}
					stream.close()
				}
			})
		}
	}
}
