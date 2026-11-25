package cache

import (
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"io"
	"sync"
)

// TPIL control schemas and tags are Trie-Trunk protocol choices. Their wire
// encoding is the same definition-driven PIL codec used by data seeds.
type controlSchema struct {
	types  []string
	once   sync.Once
	layout *pil.Layout
}

func controlLayout(types ...string) *controlSchema { return &controlSchema{types: types} }
func (c *controlSchema) definition() *pil.Layout {
	c.once.Do(func() {
		d := pil.Definition{Identity: "trie-trunk/control"}
		for i, t := range c.types {
			d.Fields = append(d.Fields, pil.Field{Name: fmt.Sprintf("field_%d", i), Type: t})
		}
		l, err := pil.Compile(d)
		if err != nil {
			panic(err)
		}
		c.layout = l
	})
	return c.layout
}

var defineControl = controlLayout("BYTES", "BYTES", "BYTES")
var bindControl = controlLayout("U32", "BYTES", "STRING")
var manifestControl = controlLayout("U32", "OBJECT")
var chunkControl = controlLayout("U32", "U8", "U64", "BYTES", "BYTES")
var endControl = controlLayout("U32")
var viewControl = controlLayout("BYTES")
var trunkControl = controlLayout("BYTES")

func writeControl(w io.Writer, tag byte, c *controlSchema, values ...pil.Value) error {
	if n, err := w.Write([]byte{tag}); err != nil {
		return err
	} else if n != 1 {
		return io.ErrShortWrite
	}
	return c.definition().Write(w, values)
}
func bytesValue(b []byte) pil.Value { return pil.Value{State: pil.Offset, Bytes: b} }
