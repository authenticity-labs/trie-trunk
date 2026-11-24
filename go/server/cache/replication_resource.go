package cache

// These responses contain only bytes from the selected resource. Range
// placement comes from request parameters; there are no transport records.
import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (h *HTTP) replicationResource(w http.ResponseWriter, r *http.Request) bool {
	p := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	kind := p[len(p)-1]
	if kind != "definitions" && kind != "registries" && kind != "leaves" && kind != "indexes" && kind != "timelines" {
		return false
	}
	// The diagnostic comparison remains an explicit query operation.
	if kind == "definitions" && r.URL.Query().Get("definition") == "" && r.Header.Get("Definition-Version") != "" {
		return false
	}
	if kind == "indexes" && r.URL.Query().Get("mode") == "search" {
		return false
	}
	if r.Method != http.MethodGet {
		if kind == "definitions" {
			return false
		}
		w.Header().Set("Allow", "GET")
		http.Error(w, "GET required", 405)
		return true
	}
	q, err := parseRawParameters(r, kind)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return true
	}
	var selected *HTTP
	for _, api := range h.sources {
		if api.Store.config.Cache != p[0] {
			continue
		}
		if q["definition"] != "" && q["definition"] != fmtDigestNative(api.Store.layout.Digest) {
			continue
		}
		if len(p) >= 4 && api.tap(Write) != strings.Join(p[:3], "/")+"/write" {
			continue
		}
		if selected != nil && kind != "registries" {
			http.Error(w, "select a Definition", 409)
			return true
		}
		selected = api
		if kind == "registries" {
			break
		}
	}
	if selected == nil {
		http.NotFound(w, r)
		return true
	}
	if kind == "registries" && q["kind"] == "inventory" {

		snapshot, e := h.trunkSnapshot(p[0])
		if e != nil {
			http.Error(w, e.Error(), 500)
			return true
		}

		body, e := encoder.Marshal(snapshot.catalog)
		if e != nil {
			http.Error(w, e.Error(), 500)
			return true
		}
		w.Header().Set("Content-Type", "application/pil")
		if metadataUnchanged(w, r, body) {
			return true
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
		return true
	}
	s := selected.Store
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/octet-stream")
	if kind == "definitions" || kind == "registries" {
		w.Header().Set("Content-Type", "application/pil")
	}
	var raw []byte
	var segments [][]byte
	var releaseIndex func()
	var spans [][2]int64
	var sequence uint64
	switch kind {
	case "definitions":
		raw = append([]byte(nil), s.layout.Bytes...)
	case "registries":
		if q["kind"] == "taxonomy" {
			w.Header().Set("Content-Type", "application/octet-stream")
			raw, err = taxonomyBytes(s.config.Root)
		} else {
			var catalog Catalog
			catalog, err = readCatalog(s.config.Root, s.config.Cache)
			if err == nil {
				raw, err = encoder.Marshal(catalog)
			}
		}

	case "leaves":
		if snapshot := s.publishedStates.Load(); snapshot != nil {
			raw = *snapshot
		} else {
			err = fmt.Errorf("state snapshot unavailable")
		}
	case "timelines":
		var branches []Branch
		branches, err = selected.branch(strings.Join(p[:4], "/"))
		if err == nil && len(branches) != 1 {
			err = fmt.Errorf("one exact branch required")
		}
		if err == nil {
			var b *branchStore

			snapshot, e := s.prepareResident(branches[0])
			if e != nil {
				err = e
			} else {
				defer snapshot.release()
				b = &snapshot.branch
				sequence = snapshot.sequence
			}
			if err == nil {

			}
			if err == nil {
				ranges := r.URL.Query()["range"]
				if len(ranges) == 0 {
					end := q["endTime"]
					if end == "" {
						end = fmt.Sprint(s.config.Now().UTC().Truncate(time.Hour).Add(time.Hour).UnixMilli())
					}
					ranges = []string{q["beginTime"] + "," + end}
				}
				if len(ranges) > 128 {
					err = fmt.Errorf("too many timeline ranges")
				}
				type span struct{ a, z int64 }
				var windows []span
				var last, totalHours int64
				for i, value := range ranges {
					p := strings.Split(value, ",")
					if len(p) != 2 {
						err = fmt.Errorf("invalid timeline range")
						break
					}
					a, e1 := strconv.ParseInt(p[0], 10, 64)
					z, e2 := strconv.ParseInt(p[1], 10, 64)
					if e1 != nil || e2 != nil || a < 0 || a >= z || a%TimeHourMS != 0 || z%TimeHourMS != 0 || i > 0 && a < last || z-a > 8*366*24*TimeHourMS {
						err = fmt.Errorf("invalid timeline range")
						break
					}
					last = z
					totalHours += (z - a) / TimeHourMS
					if totalHours > 8*366*24 {
						err = fmt.Errorf("timeline selection exceeds limit")
						break
					}
					a = max(a, b.time.first())
					z = min(z, b.time.sealed())
					if a < z {
						windows = append(windows, span{a, z})
					}
				}
				if err == nil {
					raw = append(raw, b.time.raw[:TimeTrieHeader]...)
					for _, window := range windows {
						for at := window.a; at < window.z; {
							next := min(window.z, time.Date(time.UnixMilli(at).Year()+1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
							_, _, offset, _ := b.time.positions(time.UnixMilli(at))
							length := (next - at) / TimeHourMS * int64(b.time.leafBytes())
							raw = append(raw, b.time.raw[offset:offset+int(length)]...)
							at = next
						}
					}
					w.Header().Set("Trunk-Generation", fmt.Sprintf("%x", b.generation))
					w.Header().Set("Trunk-Generation-Sequence", fmt.Sprint(sequence))
					w.Header().Set("Trunk-Source", selected.tap(branches[0]))
					w.Header().Set("Trunk-Format", "time-trie/0")
					w.Header().Set("Trunk-Path", s.config.Cache+"/"+s.binding+"/"+branches[0].Name()+".time")
				}
			}
		}
	case "indexes":
		var branches []Branch
		branches, err = selected.branch(strings.Join(p[:4], "/"))
		if err == nil && len(branches) != 1 {
			err = fmt.Errorf("one exact branch required")
		}
		if err == nil {
			var b *branchStore

			snapshot, e := s.prepareResident(branches[0])
			if e != nil {
				err = e
			} else {
				defer snapshot.release()
				b = &snapshot.branch
				sequence = snapshot.sequence
			}
			if err == nil {

			}
			if err == nil && q["generation"] != fmt.Sprintf("%x", b.generation) {
				err = fmt.Errorf("index generation changed")
			}
			if err == nil {
				wanted := r.URL.Query()["rows"]
				if len(wanted) == 0 || len(wanted) > 128 {
					err = fmt.Errorf("provide 1–128 complete row ranges")
				}
				var previous uint64
				for i, v := range wanted {
					ends := strings.Split(v, ",")
					if len(ends) != 2 {
						err = fmt.Errorf("invalid rows")
						break
					}
					a, e1 := strconv.ParseUint(ends[0], 10, 64)
					z, e2 := strconv.ParseUint(ends[1], 10, 64)
					if e1 != nil || e2 != nil || a >= z || i > 0 && a < previous {
						err = fmt.Errorf("invalid row order")
						break
					}
					first, last := false, false
					for _, run := range b.runs {
						if run.Start-IndexHeaderSlots == a {
							first = true
						}
						if run.End == z {
							last = true
						}
					}
					if !first || !last || a < b.baseRows || z > b.tail() {
						err = fmt.Errorf("rows must select complete sealed segments")
						break
					}
					spans = append(spans, [2]int64{headerSize + int64((a-b.baseRows)*IndexRowSize), int64((z - a) * IndexRowSize)})
					previous = z
				}
				if err == nil {

					var rows []byte
					rows, releaseIndex = b.rows, func() {}
					if err == nil {
						for _, span := range spans {
							at := span[0] - headerSize
							segments = append(segments, rows[at:at+span[1]])
						}
					}
					w.Header().Set("Trunk-Generation", fmt.Sprintf("%x", b.generation))
					w.Header().Set("Trunk-Source", selected.tap(branches[0]))
				}
			}
		}
	}
	w.Header().Set("Definition-Version", fmtDigestNative(s.layout.Digest))

	if err != nil {
		http.Error(w, err.Error(), 409)
		return true
	}
	if releaseIndex != nil {
		defer releaseIndex()
		var total int64
		for _, span := range spans {
			total += span[1]
		}
		w.Header().Set("Content-Length", strconv.FormatInt(total, 10))
		w.Header().Set("Trunk-Format", "index-segments/0")
		for _, segment := range segments {
			if _, err = w.Write(segment); err != nil {
				return true
			}
		}
	} else {

		if kind == "registries" && q["kind"] == "taxonomy" && metadataUnchanged(w, r, raw) {
			return true
		}
		sum := sha256.Sum256(raw)

		w.Header().Set("ETag", fmt.Sprintf("\"%x\"", sum))
		w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
		_, _ = w.Write(raw)
	}
	return true
}
func parseRawParameters(r *http.Request, kind string) (map[string]string, error) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	if kind == "timelines" && q.Has("range") && (q.Has("beginTime") || q.Has("endTime")) {
		return nil, fmt.Errorf("use ranges or beginTime/endTime, not both")
	}
	out := map[string]string{}
	for key, values := range q {
		if key == "rows" && kind == "indexes" || key == "range" && kind == "timelines" {
			continue
		}
		if key != "definition" && !(kind == "registries" && key == "kind" && (values[0] == "inventory" || values[0] == "taxonomy")) && !(kind == "indexes" && key == "generation") && !(kind == "timelines" && (key == "beginTime" || key == "endTime")) || len(values) != 1 {
			return nil, fmt.Errorf("unsupported %s parameter", key)
		}
		out[key] = values[0]
	}
	return out, err
}
