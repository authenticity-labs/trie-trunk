package search

import (
	"context"
)

type cancelOutput struct {
	cancel context.CancelFunc
	calls  int
}

func (w *cancelOutput) Write(data []byte) (int, error) { w.calls++; w.cancel(); return len(data), nil }
