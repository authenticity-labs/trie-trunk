package cache

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"trie-trunk/go/internal/node"
)

// Endpoint prepares registered RX prescriptions under one cache lease.
// Definition identity and resource bindings are supplied by trunk.trie.
type routingSnapshot struct{ sources []*HTTP }

type Endpoint struct {
	routing               atomic.Pointer[routingSnapshot]
	registrationSlots     chan struct{}
	mu                    sync.Mutex
	config                Config
	storage               seedStorage
	api                   *HTTP
	group                 *storeGroup
	closeGroup            func() error
	lease                 *os.File
	apis                  map[string]*HTTP
	preferred             map[string]*HTTP
	closed                bool
	streams               context.Context
	stopStreams           context.CancelFunc
	registeredReads       map[string]*registeredReadSnapshot
	node                  *node.Node
	registeredLeaves      map[string]*registeredLeafSnapshot
	registeredLeafWriters map[string]*registeredLeafWriter
	publishingLeaves      map[string]bool
	commitWork            sync.WaitGroup
	closeDone             chan struct{}
	readMemory            *nativeMemory
}

func collectionKey(l *Layout) string {
	return fmt.Sprintf("%s.v%d", l.Definition.Identity, l.Definition.Resource.Major)
}
func OpenEndpoint(c Config) (p *Endpoint, err error) {
	if !name(c.Cache) || c.Tenant == "" || c.Root == "" {
		return nil, fmt.Errorf("cache, tenant and root required")
	}
	if c.Layout != nil {
		return nil, fmt.Errorf("register a DOC-authored RX Definition in trunk.trie before opening the endpoint")
	}
	p = &Endpoint{config: c, registrationSlots: make(chan struct{}, 4), apis: map[string]*HTTP{}, preferred: map[string]*HTTP{}}
	p.streams, p.stopStreams = context.WithCancel(context.Background())
	if p.config.Storage == "" {
		p.config.Storage = "pack"
	}
	p.storage, err = storagePolicy(p.config.Storage)
	if err != nil {
		return nil, err
	}
	if p.config.Now == nil {
		p.config.Now = time.Now
	}
	p.registeredReads = make(map[string]*registeredReadSnapshot)
	p.registeredLeaves = make(map[string]*registeredLeafSnapshot)
	p.registeredLeafWriters = make(map[string]*registeredLeafWriter)
	p.publishingLeaves = make(map[string]bool)
	if p.config.CommitScheduler == nil {
		p.config.CommitScheduler = defaultCommitScheduler
	}
	budget := c.NativeMemoryBytes
	if budget == 0 {
		budget = 1 << 30
	}
	p.readMemory = newNativeMemory(budget)
	// Take the writer lease before reading preferences/catalogs. Otherwise a
	// second administrator could publish a new choice between read and open.
	p.lease, err = cacheLease(c)
	if err != nil {
		p.readMemory.close()
		return nil, err
	}
	opened := p
	defer func() {
		if err != nil {
			opened.Close()
		}
	}()
	if err = initializeTrunkConfig(c.Root); err != nil {
		return nil, err
	}
	p.node, err = node.Open(c.Root, node.Config{Storage: p.config.Storage}, budget, uint64(p.config.Now().UnixMilli()))
	if err != nil {
		return nil, err
	}
	p.readMemory.close()
	p.readMemory = &nativeMemory{ptr: p.node.Memory(), limit: budget}
	p.publishRouting()
	return p, nil
}

// Caller holds mu, or endpoint is not published yet. All minors stay open so
// recovery drains old staging and object history never silently skips old maps.
func (p *Endpoint) install(l *Layout) error {
	return p.installType(l, p.config.ResourceType)
}

func (p *Endpoint) installType(l *Layout, resourceType string) error {
	if p.closed {
		return fmt.Errorf("endpoint closed")
	}
	if p.node != nil {
		return fmt.Errorf("registered nodes require RX registration; retired Store layouts cannot be installed")
	}
	digest := fmt.Sprintf("%x", l.Digest)
	if existing := p.apis[digest]; existing != nil {
		if resourceType != "" && resourceType != existing.Store.config.ResourceType {
			return fmt.Errorf("immutable resource type conflict")
		}
		return nil
	}
	if p.group == nil {
		g, e := newStoreGroup(p.config)
		if e != nil {
			return e
		}
		p.group = g
		p.closeGroup = g.close
	}
	c := p.config
	c.Layout = l
	c.ResourceType = resourceType
	// A cache may mix replay-enabled and observation-only definitions. Policy
	// still decides per collection; an absent identity cannot enable postings.
	if l.Object < 0 {
		c.ObjectIndex = false
	}
	store, e := p.group.open(c)
	if e != nil {
		return e
	}
	api := &HTTP{Store: store, rollover: func() (*Layout, error) { return p.rollover(store) }}
	p.apis[digest] = api
	key := collectionKey(l)
	if current := p.preferred[key]; current == nil {
		p.preferred[key] = api
	}
	if p.api == nil || collectionKey(p.api.Store.layout) == key {
		p.api = p.preferred[key]
	}
	p.publishRouting()
	return nil
}

// Prefer is an administrative operation, never triggered by a producer's PUT.
// Existing sources remain readable; in-flight old-layout requests keep their
// immutable binding. New negotiation receives the selected compatible map.
func (p *Endpoint) Prefer(l *Layout) error {
	if l == nil {
		return fmt.Errorf("definition required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("endpoint closed")
	}
	previous := p.preferred[collectionKey(l)]
	if e := p.validatePreferred(l); e != nil {
		return e
	}
	if e := p.install(l); e != nil {
		return e
	}
	p.preferred[collectionKey(l)] = p.apis[fmt.Sprintf("%x", l.Digest)]
	if e := p.persistPreferences(); e != nil {
		if previous == nil {
			delete(p.preferred, collectionKey(l))
		} else {
			p.preferred[collectionKey(l)] = previous
		}
		p.selectDefault()
		return e
	}
	p.selectDefault()
	return nil
}
func (p *Endpoint) registrationAPI(r *http.Request, proposed *Layout) (*HTTP, error) {
	if proposed != nil {
		return p.preferred[collectionKey(proposed)], nil
	}
	identity := r.Header.Get("Definition-Identity")
	revision := r.Header.Get("Definition-Revision")
	if identity != "" && revision != "" {
		n, e := strconv.ParseUint(revision, 10, 64)
		if e != nil {
			return nil, fmt.Errorf("invalid definition revision")
		}
		return p.preferred[fmt.Sprintf("%s.v%d", identity, n>>20)], nil
	}
	if len(p.preferred) > 1 {
		return nil, fmt.Errorf("definition identity and revision required for multiple collections")
	}
	return p.api, nil
}
func (p *Endpoint) publishRouting() { p.routing.Store(&routingSnapshot{sources: p.sources()}) }

func (p *Endpoint) sources() []*HTTP {
	sources := make([]*HTTP, 0, len(p.apis))
	for _, api := range p.apis {
		sources = append(sources, api)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Store.binding < sources[j].Store.binding })
	return sources
}
func (p *Endpoint) Handler(token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		p.ServeHTTP(w, r)
	})
}
func (p *Endpoint) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("positive maintenance interval required")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if closed {
				return nil
			}
			if e := p.flushRegistered(uint64(p.config.Now().UnixMilli())); e != nil {
				return e
			}
		}
	}
}
func (p *Endpoint) Close() error {
	p.mu.Lock()
	if p.closed {
		done := p.closeDone
		p.mu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	p.closed = true
	if p.stopStreams != nil {
		p.stopStreams()
	}
	p.closeDone = make(chan struct{})
	defer close(p.closeDone)
	p.mu.Unlock()
	p.commitWork.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, writer := range p.registeredLeafWriters {
		writer.close()
	}
	clear(p.registeredLeafWriters)
	p.routing.Store(nil)
	clear(p.registeredLeaves)
	for key, snapshot := range p.registeredReads {
		snapshot.release()
		delete(p.registeredReads, key)
	}
	p.readMemory.close()
	p.node.Close()
	if p.lease != nil {
		return p.lease.Close()
	}
	if p.closeGroup != nil {
		return p.closeGroup()
	}
	return nil
}
