package cache

import (
	"fmt"
	native "github.com/authenticity-labs/pill-packer/go"
	"io"
	"net/http"
	"strings"
)

// HTTPCompressionFeature describes request-body compression, independently of
// the PIL layout and the older per-event compression binding.
const HTTPCompressionFeature = "http-native-compression"

func ingressReader(r *http.Request) (io.Reader, func(), error) {
	encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	switch encoding {
	case "", "identity":
		return r.Body, func() {}, nil
	case "gzip", "zstd", "br":
		flags, err := native.CompressionFlags(encoding, 1)
		if err != nil {
			return nil, nil, err
		}
		z, err := native.NewDecoder(r.Body, flags)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid compressed request: %w", err)
		}
		return z, func() { _ = z.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unsupported Content-Encoding")
	}
}
