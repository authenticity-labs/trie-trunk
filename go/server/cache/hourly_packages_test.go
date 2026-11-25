package cache

import (
	"os"
	"testing"
	"time"
)

func TestHourlyDefaultAndCalendarDays(t *testing.T) {
	for _, tc := range []struct {
		zone       string
		month      time.Month
		day, count int
	}{
		{"America/New_York", time.October, 17, 24},
		{"America/New_York", time.March, 8, 23},
		{"America/New_York", time.November, 1, 25},
		{"Australia/Lord_Howe", time.April, 5, 25},
		{"Australia/Lord_Howe", time.October, 4, 24},
		{"Asia/Kathmandu", time.October, 17, 24},
	} {
		t.Run(tc.zone+tc.month.String(), func(t *testing.T) {
			def := testLayout(t, false).Definition
			def.Resource.Timezone = tc.zone
			l, err := Compile(def)
			if err != nil {
				t.Fatal(err)
			}
			s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "owner", Layout: l})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err := os.Stat(configurationPath(s.config.Root, []string{s.config.Cache})); !os.IsNotExist(err) {
				t.Fatal("implicit cache override", err)
			}
			raw, err := os.ReadFile(configurationPath(s.config.Root, nil))
			if err != nil {
				t.Fatal(err)
			}
			// Only the trunk file exists by default. Resolved calendar behavior
			// remains hourly without a generated cache override.
			config, err := ParseConfiguration(raw)
			if err != nil || config.Defaults.Replication.hours() != 1 || s.policy.Replication.hours() != 1 || newTimeTrie().hours != 1 {
				t.Fatal("default is not hourly")
			}
			start := time.Date(2026, tc.month, tc.day, 0, 0, 0, 0, s.location)
			end := start.AddDate(0, 0, 1)
			windows, err := s.PlanPackages(start.UTC(), end.UTC())
			if err != nil || len(windows) != 24 {
				t.Fatalf("%d windows, want %d: %v", len(windows), 24, err)
			}
			at := start
			for _, w := range windows {
				if !w.From.Equal(at) || !w.To.After(w.From) || w.IndexPeriod != "1h" {
					t.Fatalf("gap or overlap %+v", w)
				}
				// Every instant resolves to this precise occurrence, including repeated hours.
				for probe := w.From; probe.Before(w.To); probe = probe.Add(time.Minute) {
					a, b := semanticBounds(probe, 1, s.location)
					if !a.Equal(w.From) || !b.Equal(w.To) {
						t.Fatalf("inconsistent bounds at %v: %v %v", probe, a, b)
					}
				}
				at = w.To
			}
			if !at.Equal(end) {
				t.Fatal("day incomplete")
			}
			// An exclusive hour edge must not pull the following package.
			w := windows[4]
			exact, err := s.PlanPackages(w.From.Add(time.Minute), w.To)
			if err != nil || len(exact) != 1 || !exact[0].From.Equal(w.From) {
				t.Fatal("exclusive edge widened")
			}
		})
	}
	if (ReplicationPolicy{}).hours() != 1 {
		t.Fatal("omitted period not hourly")
	}
}
