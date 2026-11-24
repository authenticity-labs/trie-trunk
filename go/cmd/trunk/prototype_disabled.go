//go:build !trunk_prototype

package main

import (
	"context"
	"fmt"
)

func runPrototype(context.Context, []string) error {
	return fmt.Errorf("object-index prototype tooling is excluded from this build")
}
