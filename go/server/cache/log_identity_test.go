package cache

import (
	"os"
	"testing"
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
