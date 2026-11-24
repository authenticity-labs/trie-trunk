package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"time"
	"trie-trunk/go/internal/node"
	"trie-trunk/go/server/cache"
	"trie-trunk/go/server/search"
	"trie-trunk/go/server/serve"
	"trie-trunk/go/server/telemetry"
)

func runServe(ctx context.Context, args []string) (result error) {
	portText := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		portText, args = args[0], args[1:]
	}
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.String("env", ".env", "environment file; empty disables file loading")
	cacheName := flags.String("cache", "", "serve a collection-native cache")
	tenant := flags.String("tenant", "", "cache tenant owner")
	root := flags.String("dir", "./taps", "store root")
	storage := flags.String("storage", "", "storage representation: pack or browse (defaults to node configuration)")
	postgres := flags.String("postgres", "", "read-only PostgreSQL listener (loopback IP:port)")
	control := flags.String("control-socket", "", "private Rust query/control socket")
	stdio := flags.Bool("stdio", false, "read lifecycle commands from stdin; exit when stdin closes")
	web := flags.String("web", "./web", "built web app directory; empty disables static files")
	host := flags.String("host", "127.0.0.1", "listen host")
	httpAddress := flags.String("http-address", "", "optional trusted reverse-proxy HTTP origin (loopback IP:port)")
	cert := flags.String("cert", "", "TLS server certificate")
	key := flags.String("key", "", "TLS server private key")
	ca := flags.String("client-ca", "", "required client certificate CA")
	socket := flags.String("socket", "", "Unix socket; default <dir>/.trunk/search.sock")
	queries := flags.Int("queries", 4, "maximum concurrent searches, including followers")
	serverLog := flags.Bool("server-log", true, "record resource observations through the shared Rust node")
	headers := flags.String("entry-headers", "", "default JSON field order")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected serve arguments")
	}
	env, err := applyEnvironment(flags, "dir", "storage", "web", "host", "http-address", "cert", "key", "client-ca", "socket", "queries", "entry-headers", "cache", "tenant")
	if err != nil {
		return err
	}
	startPostgres := func(owner *node.Node) (func(), error) {
		if *postgres == "" {
			return func() {}, nil
		}
		password, _ := env.lookup("TRUNK_PG_PASSWORD")
		listener, err := owner.Postgres(*postgres, password)
		if err != nil {
			return nil, err
		}
		return func() { listener.Close() }, nil
	}
	// A socket-only invocation is the same node with network listeners disabled.
	// Snoot uses this public command; it does not ship a second database process.
	if *stdio && *control == "" {
		return fmt.Errorf("--stdio requires --control-socket")
	}
	memory, _, err := configureNativeRuntime()
	if err != nil {
		return err
	}
	if (*control != "" || *postgres != "") && portText == "" && *cert == "" && *key == "" && *httpAddress == "" {
		owner, err := node.Open(*root, node.Config{Storage: *storage}, memory, uint64(time.Now().UnixMilli()))
		if err != nil {
			return err
		}
		defer owner.Close()
		closePG, err := startPostgres(owner)
		if err != nil {
			return err
		}
		defer closePG()
		if *control == "" {
			*control = filepath.Join(*root, ".trunk", "control.sock")
		}
		service, err := owner.Serve(*control)
		if err != nil {
			return err
		}
		defer service.Close()
		return nodeLifecycle(ctx, owner, *stdio)
	}
	if *cacheName == "" || *tenant == "" {
		return fmt.Errorf("network serving requires --cache and --tenant")
	}
	if portText == "" {
		portText, _ = env.lookup("TRUNK_SERVE_PORT")
		if portText == "" {
			portText = "443"
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid serve port; expected 1..65535")
	}
	if *cert == "" || *key == "" {
		return fmt.Errorf("HTTP/2 and HTTP/3 serving requires both --cert and --key")
	}
	token, _ := env.lookup("TRUNK_TOKEN")
	if token == "" && *ca == "" {
		return fmt.Errorf("serving requires TRUNK_TOKEN or --client-ca")
	}
	tlsConfig, err := serverTLS(*cert, *key, *ca)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(*root, 0700); err != nil {
		return err
	}
	owner, err := node.Open(*root, node.Config{Storage: *storage}, memory, uint64(time.Now().UnixMilli()))
	if err != nil {
		return err
	}
	defer owner.Close()
	closePG, err := startPostgres(owner)
	if err != nil {
		return err
	}
	defer closePG()
	*storage = owner.Configuration().Storage
	if *control != "" {
		service, err := owner.Serve(*control)
		if err != nil {
			return err
		}
		defer service.Close()
	}
	if *socket == "" {
		*socket = search.SocketPath(*root)
	}
	ctx, stopServer := context.WithCancel(ctx)
	defer stopServer()
	observer := &telemetry.Observer{OnError: func(err error) { log.Printf("server telemetry: %v", err) }}
	var native *cache.Endpoint
	var api http.Handler
	if *cacheName != "" {
		executable, e := os.Executable()
		if e != nil {
			return e
		}
		if e = cache.InstallDefinitionTools(*root, filepath.Dir(executable)); e != nil {
			return e
		}
		var release func() error
		native, release, err = openNative(ctx, *root, *cacheName, *tenant, *storage)
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, release()) }()
		api = native.Handler(token)
		if *serverLog {
			if err = observer.Open(owner, *cacheName); err != nil {
				return err
			}
			api = observer.Wrap(api)
		}
	}
	defer observer.Close()

	config := serve.Config{
		Root: *root, Web: *web, Address: net.JoinHostPort(*host, strconv.Itoa(port)), Socket: *socket,
		HTTPAddress: *httpAddress,
		TLS:         tlsConfig, Token: token, Headers: strings.Split(*headers, ","), MaxQueries: *queries,
		Ready: func(address string) {
			log.Printf("serve http2/http3 listening on https://%s; socket %s", address, *socket)
		},
		HTTPReady: func(address string) {
			log.Printf("serve HTTP origin listening on http://%s (trusted local proxy)", address)
		},
		OnError: func(err error) { log.Printf("search: %v", err) },
	}
	if api != nil {
		config.API = api
	}
	if native != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			e := native.Run(ctx, time.Second)
			if e != nil {
				cancel()
			}
			done <- e
		}()
		defer func() { cancel(); result = errors.Join(result, <-done) }()
	}
	err = serve.Run(ctx, config)
	if errors.Is(err, os.ErrPermission) && port < 1024 {
		return fmt.Errorf("serve port %d requires privileged-port binding permission; use 8443 or run with permission to bind port %d: %w", port, port, err)
	}
	return err

}

func nodeLifecycle(ctx context.Context, owner *node.Node, stdio bool) error {
	if !stdio {
		<-ctx.Done()
		return ctx.Err()
	}
	finished := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 4096), 4096)
		out := json.NewEncoder(os.Stdout)
		for scanner.Scan() {
			switch strings.TrimSpace(scanner.Text()) {
			case "close":
				finished <- nil
				return
			case "status":
				if err := out.Encode(map[string]any{"ok": true, "configuration": owner.Configuration()}); err != nil {
					finished <- err
					return
				}
			default:
				if err := out.Encode(map[string]any{"ok": false, "error": "Unknown lifecycle action"}); err != nil {
					finished <- err
					return
				}
			}
		}
		finished <- scanner.Err()
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-finished:
		return err
	}
}
