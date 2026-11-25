package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"trie-trunk/go/server/cache"
)

// All listeners share one Commit Scheduler admission limit. Environment changes take
// effect on process restart; resource definitions do not own scheduler limits.
var nativeRuntime struct {
	sync.Once
	memory      uint64
	liveRecords int
	commits     *cache.CommitScheduler
	err         error
}

func configureNativeRuntime() (uint64, *cache.CommitScheduler, error) {
	nativeRuntime.Do(func() {
		workers := 1
		if raw, ok := os.LookupEnv("TRUNK_NATIVE_MEMORY_BYTES"); ok {
			nativeRuntime.memory, nativeRuntime.err = strconv.ParseUint(raw, 10, 64)
			if nativeRuntime.err != nil || nativeRuntime.memory == 0 {
				nativeRuntime.err = fmt.Errorf("TRUNK_NATIVE_MEMORY_BYTES must be a positive byte count")
				return
			}
		}
		if raw, ok := os.LookupEnv("TRUNK_LIVE_INDEX_RECORDS"); ok {
			nativeRuntime.liveRecords, nativeRuntime.err = strconv.Atoi(raw)
			if nativeRuntime.err != nil || nativeRuntime.liveRecords < 1 {
				nativeRuntime.err = fmt.Errorf("TRUNK_LIVE_INDEX_RECORDS must be a positive record count")
				return
			}
		}
		if raw, ok := os.LookupEnv("TRUNK_SEAL_WORKERS"); ok {
			workers, nativeRuntime.err = strconv.Atoi(raw)
			if nativeRuntime.err != nil || workers < 1 || workers > 64 {
				nativeRuntime.err = fmt.Errorf("TRUNK_SEAL_WORKERS must be between 1 and 64")
				return
			}
		}
		nativeRuntime.commits = cache.NewCommitScheduler(workers)
	})
	return nativeRuntime.memory, nativeRuntime.commits, nativeRuntime.err
}

type nativePoolKey struct{}
type nativeEntry struct {
	store   *cache.Endpoint
	tenant  string
	storage string
	refs    int
}
type nativePool struct {
	sync.Mutex
	entries map[string]*nativeEntry
}

// Combined roots/serve listeners share one writer and staging queue. Separate
// processes still honor the on-disk writer lease.
func openNative(ctx context.Context, root, name, tenant, storage string) (*cache.Endpoint, func() error, error) {
	if name == "" || tenant == "" {
		return nil, nil, fmt.Errorf("ingress requires --cache and --tenant")
	}
	memory, commits, e := configureNativeRuntime()
	if e != nil {
		return nil, nil, e
	}
	config := cache.Config{Root: root, Cache: name, Tenant: tenant, Storage: storage, NativeMemoryBytes: memory, LiveIndexRecords: nativeRuntime.liveRecords, CommitScheduler: commits}
	open := func() (*cache.Endpoint, error) {
		endpoint, err := cache.OpenEndpoint(config)
		if err != nil {
			return nil, err
		}
		if err := endpoint.EnableQueryEvents(); err != nil {
			endpoint.Close()
			return nil, err
		}
		return endpoint, nil
	}
	pool, _ := ctx.Value(nativePoolKey{}).(*nativePool)
	if pool == nil {
		s, e := open()
		if e != nil {
			return nil, nil, e
		}
		return s, s.Close, nil
	}
	absolute, e := filepath.Abs(filepath.Join(root, name))
	if e != nil {
		return nil, nil, e
	}
	pool.Lock()
	defer pool.Unlock()
	entry := pool.entries[absolute]
	if entry != nil {
		if entry.tenant != tenant || entry.storage != storage {
			return nil, nil, fmt.Errorf("listeners sharing a cache require the same tenant and storage configuration")
		}
	} else {
		s, e := open()
		if e != nil {
			return nil, nil, e
		}
		entry = &nativeEntry{store: s, tenant: tenant, storage: storage}
		pool.entries[absolute] = entry
	}
	entry.refs++
	release := func() error {
		pool.Lock()
		defer pool.Unlock()
		entry.refs--
		if entry.refs == 0 {
			delete(pool.entries, absolute)
			return entry.store.Close()
		}
		return nil
	}
	return entry.store, release, nil
}
