package cache

import (
	"context"
	"io"
)

const summaryBatchLimit = 1024

type summaryContextWriter struct {
	ctx context.Context
	out io.Writer
}

func (w summaryContextWriter) Write(b []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.out.Write(b)
}
