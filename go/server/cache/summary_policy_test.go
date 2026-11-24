package cache

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSummaryCompressionPolicy(t *testing.T) {
	if (Policy{}).Summary.level() != 1 || (Policy{}).Summary.algorithm() != "zstd" {
		t.Fatal("daily summaries must default to Zstd 1")
	}
	for _, level := range []int{0, 1, 6, 9, 22, 23} {
		c, e := ParseConfiguration([]byte(fmt.Sprintf("version=1\ntimezone='UTC'\n[defaults.summary]\nlevel=%d\n", level)))
		if level < 1 || level > 22 {
			if e == nil {
				t.Fatal("accepted invalid level", level)
			}
			continue
		}
		if e != nil || c.Resolve("payment/transactions.v1").Summary.level() != level {
			t.Fatal(level, e)
		}
	}
	c, e := ParseConfiguration([]byte("version=1\ntimezone='UTC'\n[defaults.summary]\nlevel=9\n[resources.'payment/transactions.v1'.summary]\nlevel=6\n"))
	if e != nil || c.Resolve("payment/transactions.v1").Summary.level() != 6 || c.Resolve("other/items.v1").Summary.level() != 9 {
		t.Fatal("inheritance", e)
	}
	for _, inherit := range []bool{true, false} {
		p := filepath.Join(t.TempDir(), "resource.conf")
		if e = os.WriteFile(p, []byte(fmt.Sprintf("version=1\ninherit=%t\n", inherit)), 0600); e != nil {
			t.Fatal(e)
		}
		policy, _, e := readScope(p, c.Defaults, true)
		want := 1
		if inherit {
			want = 9
		}
		if e != nil || policy.Summary.level() != want {
			t.Fatal("scope", e)
		}
	}
	sum := aggregateFixture()
	for _, level := range []int{1, 6, 9} {
		raw, e := encodeSummaryDayLevel(0, 0, summaryDayMS, []Summary{sum}, true, level)
		if e != nil {
			t.Fatal(e)
		}
		_, _, _, got, e := decodeSummaryDay(raw)
		if e != nil || !reflect.DeepEqual(got, []Summary{sum}) {
			t.Fatal("level roundtrip", level, e)
		}
		plain, e := expandPAC(raw)
		if e != nil {
			t.Fatal(e)
		}
		expected, e := compressPAC(plain, level)
		if e != nil || !bytes.Equal(expected, raw) || order.Uint32(raw[4:8]) != 514 {
			t.Fatal("summary did not use selected Zstd level", level, e)
		}
	}
}

// Uses persisted packets from the real 50k-event fixture, not invented text.
func BenchmarkSavedSummaryCompression(b *testing.B) {
	root := os.Getenv("TRUNK_SUMMARY_SAMPLES")
	if root == "" {
		b.Skip("set TRUNK_SUMMARY_SAMPLES")
	}
	for i := 1; i <= 3; i++ {
		raw, e := os.ReadFile(filepath.Join(root, fmt.Sprintf("sample-%d.sumday", i)))
		if e != nil {
			b.Fatal(e)
		}
		body, e := expandPAC(raw)
		if e != nil {
			b.Fatal(e)
		}
		for _, level := range []int{1, 6, 9} {
			b.Run(fmt.Sprintf("sample%d/gzip%d", i, level), func(b *testing.B) {
				b.ReportAllocs()
				for n := 0; n < b.N; n++ {
					var out bytes.Buffer
					w, _ := gzip.NewWriterLevel(&out, level)
					w.Write(body)
					w.Close()
					b.ReportMetric(float64(out.Len()+PACCompressedHeaderSize), "bytes/packet")
				}
			})
		}
	}
}
