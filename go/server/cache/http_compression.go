package cache

import (
	"fmt"
	native "github.com/authenticity-labs/pill-packer/go"
	pilcodec "github.com/authenticity-labs/pill-packer/go/codec"
	"io"
	"net/http"
	"strings"
)

// HTTPCompressionFeature describes request-body compression, independently of
// the PIL layout and the older per-event compression binding.
const HTTPCompressionFeature = "http-native-compression"

func ingressReader(r *http.Request) (io.Reader, func(), error) {
	switch strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))) {
	case "", "identity":
		return r.Body, func() {}, nil
	case "gzip", "zstd", "br":
		flags, _ := pilcodec.CompressionFlags(strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))))
		z, err := native.NewDecoder(r.Body, flags)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid compressed request: %w", err)
		}
		return z, func() { _ = z.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unsupported Content-Encoding")
	}
}
