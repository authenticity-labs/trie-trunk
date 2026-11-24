package cache

// RX documents are PIL payloads. Placement belongs to request metadata and is
// resolved by the same Rust registration transaction used by other adapters.
import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"trie-trunk/go/internal/node"
)

func definitionHeaders(h http.Header, l *Layout) {
	h.Set("PIL-Version", "2")
	h.Set("PIL-Features", "adaptive-integers,"+HTTPCompressionFeature)
	h.Set("Definition-Version", fmt.Sprintf("%x", l.Digest))
	h.Set("Definition-Identity", l.Definition.Identity)
	h.Set("Definition-Revision", fmt.Sprint(l.Definition.Version))
}
func (p *Endpoint) registration(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		w.Header().Set("Allow", "PUT, POST")
		http.Error(w, "PUT or POST required; retrieve RX through the separate Definition resource", 405)
		return
	}
	if r.URL.RawQuery != "" {
		http.Error(w, "registration placement belongs in headers", 400)
		return
	}
	kind, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || kind != "application/pil" {
		http.Error(w, "RX body requires Content-Type: application/pil", 415)
		return
	}
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		http.Error(w, "raw RX PIL required", 415)
		return
	}
	target := node.RegistrationTarget{Cache: p.config.Cache}
	for _, field := range []struct {
		name  string
		value *string
	}{
		{"Draft-Action", &target.Action}, {"Draft-Generation", &target.Generation}, {"Resource-Service", &target.Service}, {"Resource-Name", &target.Resource}, {"Resource-Type", &target.Type}, {"Resource-Split", &target.Split},
	} {
		values := r.Header.Values(field.name)
		if len(values) > 1 {
			http.Error(w, "one "+field.name+" required", 400)
			return
		}
		if len(values) == 1 {
			if !name(values[0]) {
				http.Error(w, "invalid "+field.name, 400)
				return
			}
			*field.value = values[0]
		}
	}
	if target.Action != "" && target.Action != "save" && target.Action != "promote" && target.Action != "inspect" && target.Action != "read" {
		http.Error(w, "Unknown Definition action", 400)
		return
	}
	target.Discard = r.Header.Get("Draft-Discard") == "true"
	if values := r.Header.Values("Trunk-Branch"); len(values) > 0 {
		if len(values) != 1 {
			http.Error(w, "one Trunk-Branch required", 400)
			return
		}
		id, e := strconv.ParseUint(values[0], 10, 16)
		if e != nil || id == 0 {
			http.Error(w, "invalid Trunk-Branch", 400)
			return
		}
		branch := uint16(id)
		target.Branch = &branch
	} else if target.Service == "" || target.Resource == "" || target.Type == "" {
		http.Error(w, "Resource-Service, Resource-Name and Resource-Type are required", 400)
		return
	}
	if p.node == nil {
		http.Error(w, "Trunk unavailable", 503)
		return
	}
	select {
	case p.registrationSlots <- struct{}{}:
		defer func() { <-p.registrationSlots }()
	default:
		http.Error(w, "registration busy", 503)
		return
	}
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(5 * time.Minute))
	defer controller.SetReadDeadline(time.Time{})
	const maximum = 16 << 20
	if r.ContentLength > maximum {
		http.Error(w, "RX body exceeds registration limit", 413)
		return
	}
	buffer := p.readMemory.buffer()
	defer buffer.close()
	size := 0
	for {
		space, e := buffer.resizeLimit(min(maximum+1, size+64*1024), maximum+1)
		if e != nil {
			http.Error(w, e.Error(), 503)
			return
		}
		n, e := r.Body.Read(space[size:])
		size += n
		if size > maximum {
			http.Error(w, "RX body exceeds registration limit", 413)
			return
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			http.Error(w, "unreadable RX body", 400)
			return
		}
	}
	registered, err := p.node.Registration(target, buffer.bytes[:size])
	if err != nil {
		status := http.StatusServiceUnavailable
		var failure *node.Error
		if errors.As(err, &failure) {
			status = failure.Status
		}
		http.Error(w, err.Error(), status)
		return
	}
	if registered.Generation != "" {
		w.Header().Set("Draft-Generation", registered.Generation)
		w.Header().Set("Cache-Control", "no-store")
		if target.Action == "read" {
			w.Header().Set("Content-Type", "application/pil")
			_, _ = w.Write(registered.Bytes)
			return
		}
		if len(registered.Model) > 0 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(registered.Model)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Trunk-Branch", strconv.Itoa(int(registered.Branch)))
	w.Header().Set("Location", "/"+url.PathEscape(p.config.Cache)+"/definitions?file="+url.QueryEscape(registered.File))
	w.WriteHeader(http.StatusNoContent)
}
func hasAdaptive(r *http.Request) bool {
	return strings.Contains(","+r.Header.Get("PIL-Features")+",", ",adaptive-integers,")
}
