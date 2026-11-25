package cache

// PrototypeStore owns the deferred object Store used by the storage benchmarks.
type PrototypeStore struct{ *Store }

func OpenPrototype(c Config) (*PrototypeStore, error) {
	store, err := openStore(c)
	if err != nil {
		return nil, err
	}
	return &PrototypeStore{store}, nil
}

func (p *PrototypeStore) Close() error { return p.Store.close() }
