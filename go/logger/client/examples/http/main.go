// This example prints local requests or sends HTTP events to a registered Trunk branch.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"time"

	"trie-trunk/go/codec"
	logging "trie-trunk/go/logger/client"
	"trie-trunk/go/models"
)

func main() {
	endpoint := flag.String("trunk", "", "registered /seeds URL (omit for readable stdout)")
	branch := flag.Uint("branch", 0, "registered Trunk branch ID")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, *endpoint, *branch); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, endpoint string, branch uint) (err error) {
	var handler http.Handler
	if endpoint == "" {
		logger, err := localLogger()
		if err != nil {
			return err
		}
		defer logger.Close()
		handler = localHandler(logger)

	} else {
		if branch == 0 || branch > math.MaxUint16 {
			return fmt.Errorf("supply a registered branch ID in 1..65535")
		}
		logs, openErr := logging.HTTP()
		if openErr != nil {
			return openErr
		}
		defer logs.Close()
		delivery, openErr := logs.Connect(ctx, logging.Destination{URL: endpoint, Branch: uint16(branch), Token: os.Getenv("TRUNK_TOKEN")})
		if openErr != nil {
			return openErr
		}
		defer func() {
			flush, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = errors.Join(err, delivery.Close(flush))
		}()
		send := func(ctx context.Context, start time.Time, method, customer string, status int) error {
			event := logs.Event().Time(uint64(start.UnixMilli())).Method(method).Route("/customer").Status(uint16(status)).
				Field("customer_id", customer)
			if us := time.Since(start).Microseconds(); us <= math.MaxUint32 {
				event.Duration(uint32(us))
			}
			return delivery.Write(ctx, event)
		}
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			customer := r.URL.Query().Get("id")
			status := http.StatusOK
			if customer == "" {
				status = http.StatusBadRequest
				http.Error(w, "customer ID required", status)
			} else {
				fmt.Fprintln(w, "customer found")
			}
			if err := send(r.Context(), start, r.Method, customer, status); err != nil {
				log.Printf("log delivery: %v", err)
			}
		})
	}
	s := &http.Server{Addr: "127.0.0.1:8080", Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	stopped := make(chan error, 1)
	go func() { stopped <- s.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.Shutdown(shutdown)
	case err := <-stopped:
		return err
	}
}

const (
	CustomerService  logging.ServiceId    = 1
	CustomerResource logging.ResourceId   = 1
	LookupCustomer   logging.OperationId  = 1
	DatabaseLookup   logging.CheckpointId = 1
)

func localLogger() (*logging.Logger, error) {
	dictionary, err := codec.NewDictionary("customers/v1", map[string]map[uint16]string{
		"service": {1: "customers"}, "resource": {1: "customer"}, "operation": {1: "lookup"}, "checkpoint": {1: "database"},
	})
	if err != nil {
		return nil, err
	}
	config := logging.Config{
		OnError:    func(err error) { log.Printf("log/response write failed: %v", err) },
		Format:     codec.ReadableJSON,
		Dictionary: dictionary, Service: codec.Ref(uint16(CustomerService)), Version: "1.0.0",
	}
	return logging.New(os.Stdout, config)
}

func localHandler(logger *logging.Logger) http.Handler {
	return logger.Handle(func(w http.ResponseWriter, r *http.Request) error {
		entry := logging.FromContext(r.Context()).Resource(CustomerResource).Operation(LookupCustomer)
		id := r.URL.Query().Get("id")
		if id == "" {
			return models.New(models.MISSING_REQUEST_PARAMETER)
		}
		entry.Field("customer_id", codec.String(id))
		start := time.Now()
		// Perform the real lookup here; record its duration without a stop closure.
		entry.Checkpoint(DatabaseLookup, time.Since(start))
		w.Header().Set("Content-Type", "text/plain")
		_, err := fmt.Fprintln(w, "customer found")
		return err
	})
}
