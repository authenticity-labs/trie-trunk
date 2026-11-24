package cache

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"
)

func testLogLayout(t testing.TB) *Layout {
	t.Helper()
	raw, err := os.ReadFile("../../../schemas/http-log.rx")
	if err != nil {
		t.Fatal(err)
	}
	raw, err = BindAuthoredDefinition(raw, Binding{Format: Format, Strategy: "log", Service: "test", Resource: "logs", Major: 1, Timezone: "UTC", Roles: Roles{Event: "@id", Duration: "duration"}, Path: []string{"SeverityNumber"}})
	if err != nil {
		t.Fatal(err)
	}
	l, err := ParseDefinition(raw)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestLogStrategyDoesNotRequireDurationRole(t *testing.T) {
	raw, err := os.ReadFile("../../../schemas/http-log.rx")
	if err != nil {
		t.Fatal(err)
	}
	raw, err = BindAuthoredDefinition(raw, Binding{Format: Format, Strategy: "log", Service: "test", Resource: "logs", Major: 1, Timezone: "UTC", Roles: Roles{Event: "@id"}})
	if err != nil {
		t.Fatal(err)
	}
	l, err := ParseDefinition(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.Duration != -1 || fieldIndex(l, "duration") < 0 || fieldIndex(l, "@duration") >= 0 {
		t.Fatal("ordinary duration field was treated as a required log intrinsic")
	}
}
func TestLogEntropySurvivesIngressSealAndRecovery(t *testing.T) {
	l := testLogLayout(t)
	defer l.Close()
	start := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	now := start.Add(30 * time.Minute)
	c := Config{Root: t.TempDir(), Cache: "test", Tenant: "local", Layout: l, Now: func() time.Time { return now }}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	input := func(id Value) []byte {
		v := make([]Value, len(l.Widths))
		v[l.Event] = id
		v[l.Time] = Uint(uint64(start.UnixMilli()), 8)
		v[l.Duration] = Uint(11, 8)
		v[fieldIndex(l, "SeverityNumber")] = Uint(9, 1)
		b, e := l.Encode(nil, v)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	supplied := []byte{3, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, id := range []Value{{}, {State: Null}, {State: Offset, Bytes: supplied}} {
		if _, err = s.Append(Read, input(id)); err != nil {
			t.Fatal(err)
		}
	}
	identities := [][]byte{}
	check := func(first bool) {
		var rows [][]byte
		err := s.SearchLive(context.Background(), Query{Branch: Read, From: start, To: start.Add(time.Hour)}, func(b []byte) error { rows = append(rows, append([]byte(nil), b...)); return nil })
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 3 {
			t.Fatalf("rows: %d", len(rows))
		}
		for i, b := range rows {
			var f Frame
			if err := l.Scan(b, &f); err != nil {
				t.Fatal(err)
			}
			id := f.Values[l.Event].Bytes
			if (i < 2 && len(id) != 5) || (i == 2 && (len(id) != 10 || id[0] > 3)) {
				t.Fatalf("unexpected entropy width: %x", id)
			}
			if unsigned(f.Values[l.Time].Bytes) != uint64(start.UnixMilli()) {
				t.Fatal("time changed")
			}
			if first {
				identities = append(identities, append([]byte(nil), id...))
			} else if !bytes.Equal(identities[i], id) {
				t.Fatal("identity changed during seal/recovery")
			}
		}
	}
	check(true)
	if s.ingress.peakHour.Load() != 3 {
		t.Fatal("accepted ingress was not counted")
	}
	if bytes.Equal(identities[0], identities[1]) || !bytes.Equal(identities[2], supplied) {
		t.Fatal("generation or supplied identity preservation")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	check(false)
	now = start.Add(2 * time.Hour)
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	check(false)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	check(false)
	if s.ingress.peakHour.Load() != 3 {
		t.Fatal("recovery or sealing counted as new ingress")
	}
}
func TestLogIngressRejectsExcessEntropyAndMissingTime(t *testing.T) {
	l := testLogLayout(t)
	defer l.Close()
	v := make([]Value, len(l.Widths))
	if l.generateLogIdentity(v, make([]byte, 5)) == nil {
		t.Fatal("missing time accepted")
	}
	v[l.Time] = Uint(1, 8)
	v[l.Event] = Value{Offset, []byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0}}
	if l.generateLogIdentity(v, make([]byte, 5)) == nil {
		t.Fatal("more than 74 bits accepted")
	}
}
