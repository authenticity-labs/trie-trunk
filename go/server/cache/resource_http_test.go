package cache

import (
	"crypto/subtle"
	"net/http"
)

// Existing operation tests continue to exercise storage/codec behavior directly.
// Resource routing is tested separately below, through the public handler.
func (p *Endpoint) operationHandler(token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		p.serveHTTP(w, r)
	})
}
