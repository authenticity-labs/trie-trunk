package main

// Process transport only. Rust owns planning, storage, and result serialization.
import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"trie-trunk/go/internal/node"
)

func runNode(ctx context.Context, args []string) error {
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	args = append([]string{"--node-command"}, args...)
	command := exec.CommandContext(ctx, binary, args...)
	command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("native query: %w", err)
	}
	return nil
}

func nodeCommand(args []string) error { return node.Command(args, os.Stdin, os.Stdout, os.Stderr) }
