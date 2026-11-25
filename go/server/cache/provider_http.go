package cache

import (
	"encoding/json"
	"fmt"
	pil "github.com/authenticity-labs/pill-packer/go"
	"io"
	"net/http"
	"strings"
	"time"
	"trie-trunk/go/internal/seedtransform"
)

// JSON/CBOR are boundary adapters: the source RX codec validates and binds each
// external record once. The ordinary PIL path never goes through this adapter.
// Its transformed destination is still encoded once into native resident memory.
func (h *HTTP) providerIngress(w http.ResponseWriter, r *http.Request, kind string, p *seedtransform.Plan) {
	branches, e := h.branch(r.URL.Query().Get("tap"))
	if e != nil || len(branches) != 1 {
		http.Error(w, "provider ingress requires one destination branch", 412)
		return
	}
	if kind != "application/json" && kind != "application/jsonl" && kind != "application/x-ndjson" && kind != "application/cbor" {
		http.Error(w, "provider requires PIL, JSON or CBOR", 415)
		return
	}
	h.ingressOnce.Do(func() { h.ingressSlots = make(chan struct{}, 64) })
	select {
	case h.ingressSlots <- struct{}{}:
		defer func() { <-h.ingressSlots }()
	default:
		http.Error(w, "ingestion busy", 503)
		return
	}
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(5 * time.Minute))
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Minute))
	defer controller.SetReadDeadline(time.Time{})
	defer controller.SetWriteDeadline(time.Time{})
	encoding := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding")))
	if encoding != "" && encoding != "identity" && encoding != "gzip" && encoding != "zstd" && encoding != "br" {
		http.Error(w, "unsupported Content-Encoding", 415)
		return
	}
	input, closeInput, e := ingressReader(r)
	if e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	defer closeInput()
	body := http.MaxBytesReader(w, io.NopCloser(input), MaxFrame)
	defer body.Close()
	w.Header().Set("PIL-Version", "2")
	w.Header().Set("Definition-Version", fmtDigestNative(h.Store.layout.Digest))
	count, size := 0, 0
	batch := make([]nativeInput, 0, 128)
	stage := func() error {
		n, err := h.Store.appendProviderBatch(branches[0], batch, p)
		count += n
		clear(batch)
		batch = batch[:0]
		size = 0
		return err
	}
	report := func(err error, status int) {
		w.Header().Set("Trunk-Received", fmt.Sprint(count))
		http.Error(w, err.Error(), status)
	}
	add := func(raw []byte) error {
		batch = append(batch, nativeInput{raw: raw})
		size += len(raw)
		if len(batch) >= 1024 || size >= 4<<20 {
			return stage()
		}
		return nil
	}
	if kind == "application/cbor" {
		raw, err := io.ReadAll(body)
		if err != nil {
			report(err, 400)
			return
		}
		var value any
		if err = ingressDecoder.Unmarshal(raw, &value); err != nil {
			report(err, 400)
			return
		}
		raw, err = pil.MarshalObject(value)
		if err == nil {
			raw, err = p.Source.EncodeObject(raw)
		}
		if err != nil {
			report(err, 422)
			return
		}
		e = add(raw)
	} else {
		d := json.NewDecoder(body)
		for {
			var raw json.RawMessage
			err := d.Decode(&raw)
			if err == io.EOF {
				break
			}
			if err != nil {
				e = err
				break
			}
			encoded, err := p.Source.EncodeJSON(raw)
			if err != nil {
				e = err
				break
			}
			if err = add(encoded); err != nil {
				report(err, 422)
				return
			}
			if kind == "application/json" {
				var extra json.RawMessage
				if err = d.Decode(&extra); err != io.EOF {
					e = fmt.Errorf("single provider record required")
				}
				break
			}
		}
	}
	if err := stage(); err != nil {
		report(err, 422)
		return
	}
	if e != nil {
		report(e, 422)
		return
	}
	if e = h.Store.flushAll(); e != nil {
		report(e, 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Trunk-Acknowledgement", "durable-staging")
	w.Header().Set("Trunk-Received", fmt.Sprint(count))
	_ = json.NewEncoder(w).Encode(map[string]int{"received": count})
}
