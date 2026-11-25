package cache

import (
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go/codec"
	"net/http/httptest"
	"net/url"
	"testing"
)

// Tests may aggregate separate branch requests for comparison with a view.
// Each actual HTTP stream always contains one branch only.
type summaryPackQuery struct {
	Source, Definition, Key string
	Day                     int64
}

func requestSummaryPacks(t *testing.T, h *HTTP, refs []summaryPackQuery) *httptest.ResponseRecorder {
	t.Helper()
	outer := pil.PAC{Nested: true, AddressBytes: 2}
	for _, ref := range refs {
		q := url.Values{"definition": {ref.Definition}, "range": {fmt.Sprintf("%d,%d", ref.Day, ref.Day+summaryDayMS)}}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/"+ref.Source+"/summaries?"+q.Encode(), nil))
		if w.Code != 200 {
			return w
		}
		child, e := pil.ParsePAC(nil, w.Body.Bytes(), nil)
		if e != nil {
			t.Fatal(e)
		}
		outer.Members = append(outer.Members, child.Members...)
	}
	w := httptest.NewRecorder()
	w.Header().Set("Content-Type", "application/pil")
	body, e := outer.Marshal()
	if e != nil {
		t.Fatal(e)
	}
	if _, e := w.Write(body); e != nil {
		t.Fatal(e)
	}
	return w
}

func decodeSummaryTest(raw []byte, out *[][]byte) error {
	p, e := pil.ParsePAC(nil, raw, nil)
	if e != nil {
		return e
	}
	*out = p.Members
	return nil
}
