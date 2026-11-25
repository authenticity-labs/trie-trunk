// Package seedstream defines the registered /seeds WebSocket envelope.
// Message bodies are repeated big-endian uint32 lengths followed by complete PILs.
package seedstream

const (
	Live            = "trunk.roots.v1"
	Protocol        = "trunk.seeds.v1"
	MaxMessage      = 4 << 20
	MaxPill         = 1 << 20
	Durable    byte = 1
)
