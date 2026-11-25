//go:build trunk_prototype

package cache

import (
	"os"
	"testing"
)

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
