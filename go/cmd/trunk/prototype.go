//go:build trunk_prototype

package main

// Retained object-index experiments are opt-in tooling, never a node fallback.
import (
	"context"
	"fmt"
	"os"
)

func runPrototype(ctx context.Context, args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "cache":
			return runCache(ctx, args[1:], os.Stdin, os.Stdout)
		case "tap":
			return runTap(args[1:])
		case "search":
			return runSearch(ctx, args[1:], os.Stdout)
		}
	}
	return fmt.Errorf("prototype tooling: cache, tap, search (isolated experimental data only)")
}
