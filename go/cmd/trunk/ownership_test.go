package main

import (
	"context"
	"strings"
	"testing"
)

func TestServeRequiresRegisteredNetworkConfiguration(t *testing.T) {
	clearTrunkEnvironment(t)
	err := runServe(context.Background(), []string{"--env", "", "--dir", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "--cache and --tenant") {
		t.Fatalf("unconfigured serving must not select the retired engine: %v", err)
	}
}

func TestServiceCommandBoundaries(t *testing.T) {
	for _, args := range [][]string{{"roots", "5410", "serve", "443"}, {"serve", "443", "roots", "5410"}, {"roots", "--dir", "serve", "serve", "443"}} {
		commands, err := splitServices(args)
		if err != nil || len(commands) != 2 {
			t.Fatal(commands, err)
		}
	}
	for _, args := range [][]string{{"roots", "5410", "roots"}, {"serve", "443", "serve"}, {"other"}} {
		if _, err := splitServices(args); err == nil {
			t.Fatal("accepted", args)
		}
	}
}
