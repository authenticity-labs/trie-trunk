package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	if e := run(os.Args[1:]); e != nil {
		var child *exec.ExitError
		if errors.As(e, &child) {
			// The native command already wrote its diagnostic to stderr.
			code := child.ExitCode()
			if code < 1 {
				code = 1
			}
			os.Exit(code)
		}
		if errors.Is(e, context.Canceled) {
			os.Exit(130)
		}
		log.Print(e)
		os.Exit(1)
	}
}

func run(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(args) == 0 {
		return fmt.Errorf("%s", usage)
	}
	var err error
	switch args[0] {
	case "prototype":
		return runPrototype(ctx, args[1:])
	case "--node-command":
		return nodeCommand(args[1:])
	case "install", "inspect", "candidate", "ports", "beds", "time", "search", "definitions", "seeds":
		return runNode(ctx, args)
	case "roots", "serve":
		err = runServices(ctx, args)
	case "--help", "-h":
		fmt.Print(usage + "\n")
	default:
		err = fmt.Errorf("%s", usage)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}

type serviceCommand struct {
	name string
	args []string
}

func splitServices(args []string) ([]serviceCommand, error) {
	var commands []serviceCommand
	seen := map[string]bool{}
	for len(args) > 0 {
		name := args[0]
		if name != "roots" && name != "serve" || seen[name] {
			return nil, fmt.Errorf("expected roots and/or serve, each at most once")
		}
		seen[name] = true
		args = args[1:]
		i := 0
		for i < len(args) && args[i] != "roots" && args[i] != "serve" {
			arg := args[i]
			i++
			if strings.HasPrefix(arg, "-") && !strings.Contains(arg, "=") && arg != "--help" && arg != "-h" && i < len(args) {
				i++
			}
		}
		commands = append(commands, serviceCommand{name, args[:i]})
		args = args[i:]
	}
	return commands, nil
}

func runServices(ctx context.Context, args []string) error {
	commands, err := splitServices(args)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx = context.WithValue(ctx, nativePoolKey{}, &nativePool{entries: map[string]*nativeEntry{}})
	done := make(chan error, len(commands))
	for _, command := range commands {
		go func() {
			if command.name == "roots" {
				done <- runRootsContext(ctx, command.args)
			} else {
				done <- runServe(ctx, command.args)
			}
		}()
	}
	var result error
	for range commands {
		result = errors.Join(result, <-done)
		cancel()
	}
	return result
}

const usage = `usage:
  trunk install <directory> <name> [--storage pack|browse]
  trunk inspect <directory>
  trunk definitions register --dir PATH|--socket PATH --cache NAME --service NAME --resource NAME --type NAME --file RX|-
    Raw RX PIL input; placement and the RX lineage select the registration.
  trunk definitions draft|promote --help
  trunk seeds test --help
  trunk seeds load --dir PATH|--socket PATH --branch ID --file PIL|-
    Load one complete PIL; acknowledgement follows durable recovery storage.
  trunk beds|time query --dir PATH|--socket PATH --scope SOURCE --from TIME --to TIME
    Shared Rust queries. Tables on stdout; --json selects JSONL. --sql accepts SQL.
    --stats writes execution diagnostics to stderr. 'search' invokes beds query.
  trunk candidate <source-byte-length>
    Read sample input from stdin and emit a Definition candidate for review.
  trunk ports inspect <file|-> [--profile file.json]
    Detect or apply a source format and emit sampling/review JSON.
  trunk ports review <mapping.json|-> [--rx]
    Validate through DOC; --rx emits the authored RX bytes without registration.
  trunk serve --dir <directory> --control-socket <path> [--stdio]
    Socket-only configuration; --stdio accepts status/close and exits on EOF.
  trunk serve --dir PATH --postgres 127.0.0.1:55432
    Read-only PostgreSQL clients connect as trunk with TRUNK_PG_PASSWORD.
    For LAN access, supply --postgres-cert and --postgres-key and enable client SSL.
  trunk serve [443] --dir PATH --cache NAME --tenant OWNER
    HTTP/2 (TCP), HTTP/3 (UDP), and a private Unix socket share one Rust node.
    --cert and --key configure TLS; TRUNK_TOKEN or --client-ca authenticates access.
    --http-address 127.0.0.1:3020 adds an HTTP origin for a trusted local proxy.
    --web PATH serves the independent UI; --web '' disables it.
    --storage pack|browse selects .packs or .beds, defaulting to node configuration.
    Changing storage configuration does not convert existing files.
  trunk roots [5140] --dir PATH --cache NAME --tenant OWNER
    POST /definitions registers raw RX PIL with Resource-Service, Resource-Name,
    and Resource-Type headers. POST /CACHE/SERVICE/RESOURCE/TYPE/seeds ingests PIL.
    The /seeds WebSocket accepts PIL batches and acknowledges durable admission.
    Loopback HTTP is supported; remote listeners require TLS and authentication.
    'roots PORT serve PORT' shares the same owner and commit scheduler.
    Loads .env and .env.<ENVIRONMENT>; --env PATH selects one file, --env '' disables.
    Explicit arguments override process environment, profile, and .env defaults.`
