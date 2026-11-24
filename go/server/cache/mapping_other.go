//go:build !darwin && !linux

package cache

import (
	"fmt"
	"sync/atomic"
)

func (b *branchStore) mapIndex() error   { return fmt.Errorf("index mapping requires macOS or Linux") }
func (b *branchStore) unmapIndex() error { return nil }
func (b *branchStore) snapshotIndex() ([]byte, func(), error) {
	return nil, nil, fmt.Errorf("index mapping requires macOS or Linux")
}

// These platforms reject mapping before a snapshot can be published.
type residentMapping struct{ refs atomic.Int64 }

func (m *residentMapping) release() {}
