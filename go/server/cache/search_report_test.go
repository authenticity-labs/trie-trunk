package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSearchDiagnosticsAbsent(t *testing.T) {
	for _, tc := range []struct {
		handler http.Handler
		status  int
	}{
		{&HTTP{Token: "secret"}, http.StatusNotFound},
		// The ingress endpoint falls through to its ordinary Definition requirement.
		{(&Endpoint{}).Handler("secret"), http.StatusPreconditionFailed},
	} {
		for _, authenticated := range []bool{false, true} {
			req := httptest.NewRequest("POST", "/diagnostics/searches", strings.NewReader(`{"id":"auth-test","ui":{}}`))
			req.Header.Set("Content-Type", "application/json")
			if authenticated {
				req.Header.Set("Authorization", "Bearer secret")
			}
			res := httptest.NewRecorder()
			tc.handler.ServeHTTP(res, req)
			want := 401
			if authenticated {
				want = tc.status
			}
			if res.Code != want {
				t.Fatalf("status %d, want %d: %s", res.Code, want, res.Body.String())
			}
		}
	}
}
