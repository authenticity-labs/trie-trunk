package cache

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
)

func gzipMember(t *testing.T, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestHTTPClientDecodesConsecutiveGzipMembers(t *testing.T) {
	// A response is one coded representation, even when it is assembled from
	// independent stored members. No bytes may sit outside those members.
	a, b := []byte("first PAC"), []byte("second PAC")
	wire := append(gzipMember(t, a), gzipMember(t, b)...)
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			if _, err := http.ReadRequest(bufio.NewReader(server)); err != nil {
				return
			}
			response := &http.Response{StatusCode: 200, ProtoMajor: 1, ProtoMinor: 1, Header: http.Header{"Content-Encoding": {"gzip"}}, ContentLength: int64(len(wire)), Body: io.NopCloser(bytes.NewReader(wire))}
			_ = response.Write(server)
		}()
		return client, nil
	}}
	defer transport.CloseIdleConnections()
	r, err := (&http.Client{Transport: transport}).Get("http://local.test/")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	plain, err := io.ReadAll(r.Body)
	if err != nil || !r.Uncompressed || !bytes.Equal(plain, append(a, b...)) {
		t.Fatal("multi-member HTTP decoding", err)
	}
}
