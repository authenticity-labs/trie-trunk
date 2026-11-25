package cache

import (
	"bytes"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"os"
	"path/filepath"
	meta "trie-trunk/go/internal/definitionmeta"
	"trie-trunk/go/internal/seedtransform"
)

// ProviderConfig binds an authored target to this resource. The bundle registry
// selects exact source RX identities; request field names never select a schema.
type ProviderConfig struct {
	Bundle     string `toml:"bundle"`
	Definition string `toml:"definition"`
}

func (s *Store) loadProviders(c *ProviderConfig) (err error) {
	if c == nil {
		return nil
	}
	dir := c.Bundle
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(s.dir, dir)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "registry.map"))
	if err != nil {
		return err
	}
	var registry Catalog
	if err = pil.UnmarshalObject(raw, &registry); err != nil {
		return err
	}
	var targetEntry meta.Resources
	var targetDigest string
	for digest, entry := range registry.DefinitionResources {
		if entry.Name == c.Definition {
			targetEntry = entry
			targetDigest = digest
			break
		}
	}
	if targetDigest == "" {
		return fmt.Errorf("provider target %s absent from registry", c.Definition)
	}
	read := func(name string) (*pil.Layout, error) {
		if filepath.Base(name) != name {
			return nil, fmt.Errorf("invalid Definition filename")
		}
		raw, e := os.ReadFile(filepath.Join(dir, name+".rx"))
		if e != nil {
			return nil, e
		}
		l, e := pil.Define(raw)
		if e != nil {
			return nil, e
		}
		if e = os.MkdirAll(filepath.Join(s.config.Root, "definitions"), 0700); e == nil {
			e = ensureMap(definitionPath(s.config.Root, l.Digest), raw)
		}
		if e != nil {
			l.Close()
			return nil, e
		}
		return l, nil
	}
	target, err := read(c.Definition)
	if err != nil {
		return err
	}
	defer target.Close()
	if fmtDigestNative(target.Digest) != targetDigest {
		return fmt.Errorf("target RX digest mismatch")
	}
	bound, err := BindAuthoredDefinition(target.Bytes, s.layout.Definition.Resource)
	if err != nil {
		return err
	}
	if !bytes.Equal(bound, s.layout.Bytes) {
		return fmt.Errorf("provider target does not match resource Definition")
	}
	readMetadata := func(name string) (*meta.Layout, error) {
		layout, err := read(name)
		if err != nil {
			return nil, err
		}
		defer layout.Close()
		return meta.Open(layout.Bytes)
	}
	targetMeta, err := meta.Open(target.Bytes)
	if err != nil {
		return err
	}
	defer targetMeta.Close()
	ms, err := readMetadata("field-meta")
	if err != nil {
		return err
	}
	defer ms.Close()
	ss, err := readMetadata("synonym")
	if err != nil {
		return err
	}
	defer ss.Close()
	metadata, err := os.Open(filepath.Join(dir, "definition.meta"))
	if err != nil {
		return err
	}
	defer metadata.Close()
	synonyms, err := os.Open(filepath.Join(dir, "synonym.map"))
	if err != nil {
		return err
	}
	defer synonyms.Close()
	pack, err := meta.ReadPack(metadata, targetEntry.Metadata, ms, targetMeta)
	if err != nil {
		return err
	}
	prepared, err := meta.Prepare(pack, ms, targetMeta)
	if err != nil {
		return err
	}
	plans := map[string]*seedtransform.Plan{}
	defer func() {
		if err != nil {
			for _, p := range plans {
				p.Close()
				p.Source.Close()
			}
		}
	}()
	for name, location := range targetEntry.Synonyms {
		source, e := read(name)
		if e != nil {
			return e
		}
		pack, e := meta.ReadPack(synonyms, location, ss, targetMeta)
		if e != nil {
			source.Close()
			return e
		}
		mapping, e := meta.PrepareSynonyms(pack, ss, targetMeta)
		if e != nil {
			source.Close()
			return e
		}
		// Storage-only fields are appended by Seed binding. Authored ordinals remain
		// stable, and full bound-RX equality above verifies all destination constraints.
		plan, e := seedtransform.Compile(source, s.layout.Layout, mapping, prepared)
		if e != nil {
			source.Close()
			return e
		}
		plans[fmtDigestNative(source.Digest)] = plan
	}
	s.providers = plans
	return nil
}
