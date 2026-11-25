package definitionmeta

import (
	"crypto/sha256"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go"
)

type field struct {
	Name string `json:"name"`
}

// Layout holds the registered codec and the field names used to bind presentation metadata.
type Layout struct {
	*pil.Definition
	Fields []field
	Digest [32]byte
}

func Open(raw []byte) (*Layout, error) {
	d, err := pil.FromPIL(raw)
	if err != nil {
		return nil, err
	}
	b, err := d.RuntimeLayout()
	var fields struct {
		Fields []field `json:"fields"`
		Hash   []byte  `json:"rx-hash"`
	}
	if err == nil {
		err = pil.UnmarshalObject(b, &fields)
	}
	if err != nil {
		d.Close()
		return nil, err
	}
	result := &Layout{Definition: d, Fields: fields.Fields, Digest: sha256.Sum256(raw)}
	if len(fields.Hash) > 0 {
		if len(fields.Hash) != len(result.Digest) {
			d.Close()
			return nil, fmt.Errorf("invalid RX digest length")
		}
		copy(result.Digest[:], fields.Hash)
	}
	return result, nil
}
