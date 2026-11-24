package cache

// HTTP framing only. The shared Rust Node owns durable admission, recovery,
// live Packs, summary accumulation and the commit scheduler.
import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"trie-trunk/go/internal/node"
)

const registeredPillMaximum = 1 << 20
const registeredIngressMaximum = 64 << 20
const registeredRecoveryMaximum = 256 << 20
const recoveryPrefixBytes = 10

func (p *Endpoint) postRegisteredSeed(w http.ResponseWriter, r *http.Request, route registeredRoute) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		http.Error(w, "endpoint closed", 503)
		return
	}
	p.commitWork.Add(1)
	p.mu.Unlock()
	defer p.commitWork.Done()
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/pil" {
		http.Error(w, "Content-Type: application/pil required", 415)
		return
	}
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		http.Error(w, "full uncompressed PIL required", 415)
		return
	}
	if r.ContentLength > registeredPillMaximum {
		http.Error(w, "Pill exceeds ingress limit", 413)
		return
	}
	buffer := p.readMemory.buffer()
	defer buffer.close()
	size := 0
	for {
		target, e := buffer.resizeLimit(min(registeredPillMaximum+1, size+64*1024), registeredPillMaximum+1)
		if e != nil {
			http.Error(w, e.Error(), 503)
			return
		}
		n, e := r.Body.Read(target[size:])
		size += n
		if size > registeredPillMaximum {
			http.Error(w, "Pill exceeds ingress limit", 413)
			return
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			http.Error(w, e.Error(), 400)
			return
		}
	}
	pill := buffer.bytes[:size]
	if err = p.node.Ingest(route.Branch, pill, uint64(p.config.Now().UnixMilli())); err != nil {
		status := http.StatusServiceUnavailable
		var failure *node.Error
		if errors.As(err, &failure) {
			status = failure.Status
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Trunk-Branch", strconv.Itoa(int(route.Branch)))
	w.Header().Set("Trunk-Accepted", "1")
	w.WriteHeader(http.StatusNoContent)
}

func (p *Endpoint) flushRegistered(clock uint64) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.commitWork.Add(1)
	p.mu.Unlock()
	defer p.commitWork.Done()
	return p.node.Commit(clock)
}
