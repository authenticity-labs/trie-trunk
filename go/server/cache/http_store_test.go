//go:build !trunk_prototype

package cache

import "net/http"

// Expose the retired Store as a handler only to prototype clients and tests.
func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.prototypeHTTP(w, r) }

func Open(c Config) (*Store, error) { return openStore(c) }
func (s *Store) Close() error       { return s.close() }

func (s *Store) Flush() error { return s.flushAll() }
