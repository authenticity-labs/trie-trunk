package cache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRegisteredHTTPIngressRecoveryAndPublication(t *testing.T) {
	for _, storage := range []string{"pack", "browse"} {
		t.Run(storage, func(t *testing.T) { registeredIngressLifecycle(t, storage) })
	}
}
func registeredIngressLifecycle(t *testing.T, storage string) {
	const hour = int64(1794830400000)
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	fixture := func(name string) []byte {
		t.Helper()
		b, e := os.ReadFile("../../../testdata/http-ingress/" + name)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	if e := RegisterSeedDefinition(root, fixture("query.rx"), SeedRegistration{Cache: "demo", Service: "trie-trunk", Resource: "queries", ResourceType: "query", File: "query.rx"}); e != nil {
		t.Fatal(e)
	}
	now := time.UnixMilli(hour + 3000)
	open := func() *Endpoint {
		t.Helper()
		p, e := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", Storage: storage, Now: func() time.Time { return now }})
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	p := open()
	defer func() { p.Close() }()
	legacy := httptest.NewRecorder()
	legacyRequest := httptest.NewRequest("POST", "/ingest", strings.NewReader("{}"))
	legacyRequest.Header.Set("Content-Type", "application/json")
	p.ServeHTTP(legacy, legacyRequest)
	if legacy.Code != 404 {
		t.Fatal("registered node exposed retired ingestion", legacy.Code)
	}
	for _, test := range []struct {
		method, operation, allow string
		status                   int
	}{
		{"GET", "seeds", "POST", 405},
		{"POST", "packs", "GET", 405},
		{"GET", "beds", "", 404},
	} {
		r := httptest.NewRequest(test.method, "/demo/trie-trunk/queries/query/"+test.operation, nil)
		r.Header.Set("Trunk-Branch", "1")
		w := httptest.NewRecorder()
		p.ServeHTTP(w, r)
		if w.Code != test.status || w.Header().Get("Allow") != test.allow {
			t.Fatalf("%s /%s: status=%d allow=%q", test.method, test.operation, w.Code, w.Header().Get("Allow"))
		}
	}
	request := func(method string, body []byte) *httptest.ResponseRecorder {
		t.Helper()
		q := ""
		operation := "seeds"
		if method == "GET" {
			operation = "packs"
			q = fmt.Sprintf("?beginTime=%d&endTime=%d", hour, hour+2*TimeHourMS)
		}
		r := httptest.NewRequest(method, "/demo/trie-trunk/queries/query/"+operation+q, bytes.NewReader(body))
		r.Header.Set("Trunk-Branch", "1")
		r.Header.Set("Content-Type", "application/pil")
		w := httptest.NewRecorder()
		p.ServeHTTP(w, r)
		return w
	}
	if w := request("POST", []byte{0xff}); w.Code != 400 {
		t.Fatalf("malformed PIL: %d %s", w.Code, w.Body.String())
	}
	for _, n := range []string{"0.pil", "1.pil"} {
		w := request("POST", fixture(n))
		if w.Code != 204 {
			t.Fatalf("POST %d %s", w.Code, w.Body.String())
		}
	}
	save := func(name string, raw []byte) {
		t.Helper()
		if out := os.Getenv("TRUNK_HTTP_FIXTURE"); out != "" {
			if e := os.MkdirAll(out, 0755); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(filepath.Join(out, name), raw, 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
	before := request("GET", nil)
	if before.Code != 200 {
		t.Fatalf("GET %d %s", before.Code, before.Body.String())
	}
	if n, e := seedNativeLength(before.Body.Bytes()); e != nil || n != uint64(before.Body.Len()) {
		t.Fatal("live framing", n, e)
	}
	route, e := taxonomyRoute(root, 1)
	if e != nil {
		t.Fatal(e)
	}
	prefix := filepath.Join(root, route.Storage.Directory, route.Storage.Stem)
	save("live.pac", before.Body.Bytes())
	headers, _ := json.Marshal(before.Header())
	save("live.headers.json", headers)
	for _, name := range []string{"trunk.trie", "definitions/query.rx", "demo/trie-trunk/queries/query.v1.time"} {
		raw, e := os.ReadFile(filepath.Join(root, name))
		if e != nil {
			t.Fatal(e)
		}
		save(strings.Replace(filepath.Base(name), ".v1.", ".", 1), raw)
	}
	// A restart replays the same complete PILs and discards only a torn tail.
	p.Close()
	file, e := os.OpenFile(prefix+".seeds", os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	file.Write([]byte{1, 2, 3})
	file.Close()
	p = open()
	recovered := request("GET", nil)
	if recovered.Code != 200 || !bytes.Equal(before.Body.Bytes(), recovered.Body.Bytes()) {
		t.Fatalf("recovery %d %s", recovered.Code, recovered.Body.String())
	}
	now = time.UnixMilli(hour + TimeHourMS + 2000)
	if w := request("POST", fixture("2.pil")); w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	now = time.UnixMilli(hour + TimeHourMS + 900000)
	if e = p.flushRegistered(uint64(now.UnixMilli())); e != nil {
		t.Fatal("commit", e)
	}
	sealed := request("GET", nil)
	if sealed.Code != 200 {
		t.Fatal(sealed.Code, sealed.Body.String())
	}
	save("sealed-live.pac", sealed.Body.Bytes())

	headers, _ = json.Marshal(sealed.Header())
	save("sealed-live.headers.json", headers)
	for _, suffix := range []string{"time", strings.TrimPrefix(p.storage.Suffix, "."), "indx"} {
		raw, e := os.ReadFile(prefix + "." + suffix)
		if e != nil {
			t.Fatal(e)
		}
		save("sealed."+suffix, raw)
	}
	n, e := seedNativeLength(sealed.Body.Bytes())
	if e != nil || n >= uint64(sealed.Body.Len()) {
		t.Fatal("expected sealed and live Pack", n, e)
	}
	if sealed.Header().Get("Trunk-Sealed-Through") != time.UnixMilli(hour+TimeHourMS).UTC().Format(time.RFC3339Nano) {
		t.Fatal("incorrect publication watermark")
	}
	if _, e = os.Stat(prefix + fmt.Sprintf(".%d.pack", hour)); !os.IsNotExist(e) {
		t.Fatal("published recovery file retained", e)
	}
	p.Close()
	p = open()
	after := request("GET", nil)
	if after.Code != 200 || !bytes.Equal(sealed.Body.Bytes(), after.Body.Bytes()) {
		t.Fatal("post-commit restart", after.Code, after.Body.String())
	}
	other := ".beds"
	if storage == "browse" {
		other = ".packs"
	}
	if _, err := os.Stat(prefix + other); !os.IsNotExist(err) {
		t.Fatalf("unconfigured representation %s exists: %v", other, err)
	}
	p.Close()
	beforeTime, err := os.ReadFile(prefix + ".time")
	if err != nil {
		t.Fatal(err)
	}
	if storage == "pack" {
		storage = "browse"
	} else {
		storage = "pack"
	}
	changed, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", Storage: storage, Now: func() time.Time { return now }})
	if changed != nil {
		changed.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "does not convert stored data") {
		t.Fatalf("configuration silently converted storage: %v", err)
	}
	afterTime, err := os.ReadFile(prefix + ".time")
	if err != nil || !bytes.Equal(beforeTime, afterTime) {
		t.Fatal("failed configuration changed publication", err)
	}
	if _, err := os.Stat(prefix + other); !os.IsNotExist(err) {
		t.Fatal("failed configuration created another datastore", err)
	}
}

func TestRegisteredCommitCheckpointRecoversEveryPublicationBoundary(t *testing.T) {
	for _, stage := range []string{"checkpoint", "partial-packs", "packs", "index", "time"} {
		t.Run(stage, func(t *testing.T) {
			const hour = int64(1794830400000)
			root := t.TempDir()
			t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
			read := func(n string) []byte {
				b, e := os.ReadFile("../../../testdata/http-ingress/" + n)
				if e != nil {
					t.Fatal(e)
				}
				return b
			}
			if e := RegisterSeedDefinition(root, read("query.rx"), SeedRegistration{Cache: "demo", Service: "trie-trunk", Resource: "queries", ResourceType: "query", File: "query.rx"}); e != nil {
				t.Fatal(e)
			}
			now := time.UnixMilli(hour + 3000)
			config := Config{Root: root, Cache: "demo", Tenant: "local", Now: func() time.Time { return now }}
			p, e := OpenEndpoint(config)
			if e != nil {
				t.Fatal(e)
			}
			r := httptest.NewRequest("POST", "/demo/trie-trunk/queries/query/seeds", bytes.NewReader(read("0.pil")))
			r.Header.Set("Trunk-Branch", "1")
			r.Header.Set("Content-Type", "application/pil")
			w := httptest.NewRecorder()
			p.ServeHTTP(w, r)
			if w.Code != 204 {
				t.Fatal(w.Code, w.Body.String())
			}
			route, e := taxonomyRoute(root, 1)
			if e != nil {
				t.Fatal(e)
			}
			p.Close()
			prefix := filepath.Join(root, route.Storage.Directory, route.Storage.Stem)
			registry, e := taxonomyReader(root)
			if e != nil {
				t.Fatal(e)
			}
			native, e := registry.trie.ingress(route.Branch, registeredIngressMaximum, registeredPillMaximum, uint64(hour))
			if e != nil {
				t.Fatal(e)
			}
			defer native.close()
			if _, e = native.load(route.Branch, read("0.pil"), 0, true); e != nil {
				t.Fatal(e)
			}
			now = time.UnixMilli(hour + TimeHourMS + 900000)
			sealed, e := native.seal(uint64(hour), uint64(now.UnixMilli()))
			if e != nil {
				t.Fatal(e)
			}
			defer sealed.close()
			timeline, e := os.ReadFile(prefix + ".time")
			if e != nil {
				t.Fatal(e)
			}
			file, e := os.Open(prefix + ".packs")
			if e != nil {
				t.Fatal(e)
			}
			stat, _ := file.Stat()
			memory := newNativeMemory(16 << 20)
			defer memory.close()
			metadata := memory.buffer()
			_, e = seedNativePublication(file, uint64(stat.Size()), metadata)
			file.Close()
			if e != nil {
				t.Fatal(e)
			}
			plan, e := sealed.prepare(timeline, metadata.bytes, uint64(stat.Size()), true)
			metadata.close()
			if e != nil {
				t.Fatal(e)
			}
			defer plan.close()
			checkpoint, e := plan.checkpoint()
			if e != nil {
				t.Fatal(e)
			}
			defer checkpoint.close()
			journal, e := os.OpenFile(prefix+".seeds", os.O_RDWR, 0600)
			if e != nil {
				t.Fatal(e)
			}
			defer journal.Close()
			journal.Seek(0, 2)
			journal.Write(checkpoint.bytes)
			journal.Sync()
			expected := append([]byte(nil), plan.bytes(0)...)
			if stage != "checkpoint" {
				f, _ := os.OpenFile(prefix+".packs", os.O_RDWR, 0600)
				body := plan.bytes(0)
				if stage == "partial-packs" {
					body = body[:len(body)/2]
				}
				f.WriteAt(body, int64(plan.offset))
				f.Sync()
				f.Close()
			}
			if stage == "index" || stage == "time" {
				if e = os.WriteFile(prefix+".indx", plan.bytes(1), 0600); e != nil {
					t.Fatal(e)
				}
			}
			if stage == "time" {
				if e = replace(prefix+".time", plan.bytes(2)); e != nil {
					t.Fatal(e)
				}
			}
			p.Close()
			p, e = OpenEndpoint(config)
			if e != nil {
				t.Fatal(e)
			}
			defer p.Close()
			get := httptest.NewRequest("GET", fmt.Sprintf("/demo/trie-trunk/queries/query/packs?beginTime=%d&endTime=%d", hour, hour+TimeHourMS), nil)
			get.Header.Set("Trunk-Branch", "1")
			result := httptest.NewRecorder()
			p.ServeHTTP(result, get)
			if result.Code != 200 {
				t.Fatal(result.Code, result.Body.String())
			}
			n, e := seedNativeLength(expected)
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Equal(result.Body.Bytes(), expected[:n]) {
				t.Fatal("checkpoint did not restore exact published Pack")
			}
			if _, e = os.Stat(journal.Name()); !os.IsNotExist(e) {
				t.Fatal("recovered checkpoint retained", e)
			}
		})
	}
}

func TestQueryEventRegistrationIsIdempotent(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	p, e := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local"})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if e = p.EnableQueryEvents(); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if e != nil {
		t.Fatal(e)
	}
	if e = p.EnableQueryEvents(); e != nil {
		t.Fatal(e)
	}
	after, e := os.ReadFile(filepath.Join(root, "trunk.trie"))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("registration changed", e)
	}
	route, e := taxonomyRoute(root, 1)
	if e != nil || route.Source != "demo/trunk-client/queries/query" {
		t.Fatal(route, e)
	}
}

func TestRegisteredHTTPIngestionSummaries(t *testing.T) {
	for _, storage := range []string{"pack", "browse"} {
		t.Run(storage, func(t *testing.T) { registeredSummaryLifecycle(t, storage) })
	}
}
func registeredSummaryLifecycle(t *testing.T, storage string) {
	const hour = int64(1794830400000)
	day := hour / 86400000 * 86400000
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	definition, err := seedQueryDefinition()
	if err != nil {
		t.Fatal(err)
	}
	defer definition.close()
	if err = RegisterSeedDefinition(root, definition.bytes, SeedRegistration{Cache: "demo", Service: "trie-trunk", Resource: "queries", ResourceType: "query", File: "query.rx"}); err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(hour + 3000)
	open := func() *Endpoint {
		p, e := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", Storage: storage, Now: func() time.Time { return now }})
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	p := open()
	defer func() { p.Close() }()
	request := func(method, operation string, body []byte) *httptest.ResponseRecorder {
		path := "/demo/trie-trunk/queries/query/" + operation
		if method == "GET" {
			if operation == "summaries" {
				path += fmt.Sprintf("?range=%d,%d", day, day+86400000)
			} else {
				path += fmt.Sprintf("?beginTime=%d&endTime=%d", hour, hour+2*TimeHourMS)
			}
		}
		r := httptest.NewRequest(method, path, bytes.NewReader(body))
		r.Header.Set("Trunk-Branch", "1")
		r.Header.Set("Content-Type", "application/pil")
		w := httptest.NewRecorder()
		p.ServeHTTP(w, r)
		return w
	}
	codec, e := seedNativeRegisterMember(definition.bytes, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer codec.close()
	post := func(n string) {
		ordinal := int(n[0] - '0')
		stamp := hour + int64(ordinal+1)*1000
		if ordinal == 2 {
			stamp = hour + TimeHourMS + 1000
		}
		raw, e := codec.ingest([]seedNativeValue{
			{ordinal: 0, kind: seedUnsigned, low: uint64(stamp)},
			{ordinal: 1, kind: seedBytes, bytes: []byte{byte(ordinal + 1), 0, 0, 0, 0, 0, 0, 0, 0, 0}},
			{ordinal: 11, kind: seedUnsigned, low: uint64(ordinal + 1)},       // rows
			{ordinal: 12, kind: seedFloat, real: float64((ordinal + 1) * 10)}, // duration
		}, false)
		if e != nil {
			t.Fatal(e)
		}
		defer raw.close()
		w := request("POST", "seeds", raw.bytes)
		if w.Code != 204 {
			t.Fatalf("POST: %d %s", w.Code, w.Body.String())
		}
	}

	save := func(name string, raw []byte) {
		if out := os.Getenv("TRUNK_HTTP_SUMMARY"); out != "" {
			if e := os.MkdirAll(out, 0700); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(filepath.Join(out, name), raw, 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
	read := func(name string) []byte {
		w := request("GET", "summaries", nil)
		if w.Code != 200 {
			t.Fatalf("summaries %s: %d %s", name, w.Code, w.Body.String())
		}
		if w.Header().Get("Trunk-Pack-Count") != "1" {
			t.Fatal(w.Header())
		}
		tag := w.Header().Get("Trunk-Registry")
		if tag == "" {
			t.Fatal("summary response omitted owning registry")
		}
		verify := func(expected string, status int) {
			r := httptest.NewRequest("GET", fmt.Sprintf("/demo/trie-trunk/queries/query/summaries?range=%d,%d", day, day+86400000), nil)
			r.Header.Set("Trunk-Branch", "1")
			r.Header.Set("Trunk-Registry", expected)
			result := httptest.NewRecorder()
			p.ServeHTTP(result, r)
			if result.Code != status || result.Header().Get("Trunk-Registry") != tag {
				t.Fatalf("registry handshake: %d %s", result.Code, result.Body.String())
			}
			if status == 200 && !bytes.Equal(result.Body.Bytes(), w.Body.Bytes()) {
				t.Fatal("registry check changed Pack bytes")
			}
			if status == 412 && result.Header().Get("Trunk-Format") == "summary-packs/0" {
				t.Fatal("stale registry received Pack stream")
			}
		}
		verify(tag, 200)
		verify("\"outdated\"", 412)
		save(name+".pac", w.Body.Bytes())
		return bytes.Clone(w.Body.Bytes())
	}
	post("0")
	post("1")
	live := read("live")
	p.Close()
	p = open()
	if got := read("recovered"); !bytes.Equal(live, got) {
		t.Fatal("open summary changed across recovery")
	}
	now = time.UnixMilli(hour + TimeHourMS + 2000)
	post("2")
	// A new event invalidates the resident PAC, but not its registered schema.
	// Refresh through the opaque-byte boundary without re-exporting RX/trie JSON.
	snapshot, snapshotErr := p.node.Snapshot(1, [][2]uint64{{uint64(day), uint64(day + 86400000)}}, true, nil, uint64(now.UnixMilli()))
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	refreshed := bytes.Clone(snapshot.Bytes(5, 0))
	snapshot.Close()
	if len(refreshed) == 0 || bytes.Equal(refreshed, live) {
		t.Fatal("ingestion did not refresh the published summary snapshot")
	}
	if got := read("refreshed"); !bytes.Equal(refreshed, got) {
		t.Fatal("HTTP summary differs from the refreshed opaque PAC")
	}
	if e := p.flushRegistered(uint64(hour + TimeHourMS + 900000)); e != nil {
		t.Fatal(e)
	}
	three := read("hour-committed")
	p.Close()
	p = open()
	if got := read("hour-recovered"); !bytes.Equal(three, got) {
		t.Fatal("committed-hour summary changed across recovery")
	}
	now = time.UnixMilli(day + 86400000 + 900000)
	if e := p.flushRegistered(uint64(now.UnixMilli())); e != nil {
		t.Fatal("daily publication", e)
	}
	sealed := read("sealed")
	if bytes.Equal(sealed, three) {
		t.Fatal("daily summary was not sealed")
	}
	p.Close()
	p = open()
	if got := read("sealed-recovered"); !bytes.Equal(sealed, got) {
		t.Fatal("sealed summary changed across restart")
	}
	prefix := filepath.Join(root, "demo/trie-trunk/queries/query.v1")
	// Stage a replacement using the actual Rust writer, damage the published
	// footer, then restart. Recovery must replay the tail before opening the Box.
	p.Close()
	registry, e := taxonomyReader(root)
	if e != nil {
		t.Fatal(e)
	}
	native, e := registry.trie.ingress(1, registeredIngressMaximum, registeredPillMaximum, uint64(day+86400000))
	if e != nil {
		t.Fatal(e)
	}
	defer native.close()
	timeline, e := os.ReadFile(prefix + ".time")
	if e != nil {
		t.Fatal(e)
	}
	rx, e := os.ReadFile(filepath.Join(root, "definitions/summary.rx"))
	if e != nil {
		t.Fatal(e)
	}
	fsum, e := os.Open(prefix + ".summ")
	if e != nil {
		t.Fatal(e)
	}
	sumstat, e := fsum.Stat()
	if e != nil {
		t.Fatal(e)
	}
	memory := newNativeMemory(16 << 20)
	defer memory.close()
	metadata := memory.buffer()
	defer metadata.close()
	idx, e := seedNativeSummaryFooter(fsum, uint64(sumstat.Size()), metadata)
	fsum.Close()
	if e != nil {
		t.Fatal(e)
	}
	idx.close()
	if _, e = native.summaryStart(registry.trie, timeline, rx, metadata.bytes, uint64(sumstat.Size())); e != nil {
		t.Fatal(e)
	}
	stagedSummary, checkpoint, e := native.summary(registry.image, uint64(day+86400000), uint64(day+2*86400000), metadata.bytes, uint64(sumstat.Size()), true)
	if e != nil {
		t.Fatal(e)
	}
	defer stagedSummary.close()
	if e = replace(prefix+".summ.pack", checkpoint); e != nil {
		t.Fatal(e)
	}
	p.Close()
	f, e := os.OpenFile(prefix+".summ", os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	stat, _ := f.Stat()
	_, e = f.WriteAt([]byte{0, 0}, stat.Size()-2)
	f.Close()
	if e != nil {
		t.Fatal(e)
	}
	p = open()
	if got := read("checkpoint-recovered"); !bytes.Equal(sealed, got) {
		t.Fatal("summary checkpoint changed historical Pack")
	}
	if _, e = os.Stat(prefix + ".summ.pack"); !os.IsNotExist(e) {
		t.Fatal("checkpoint not retired", e)
	}
	for _, name := range []string{"trunk.trie", "definitions/query.rx", "definitions/summary.rx", "demo/trie-trunk/queries/query.v1.summ", "demo/trie-trunk/queries/query.v1.time"} {
		raw, e := os.ReadFile(filepath.Join(root, name))
		if e != nil {
			t.Fatal(e)
		}
		save(strings.Replace(filepath.Base(name), ".v1.", ".", 1), raw)
	}
	events := request("GET", "packs", nil)
	if events.Code != 200 {
		t.Fatal(events.Code, events.Body.String())
	}
	save("events.pac", events.Body.Bytes())
}

func TestSummaryAnnotationRegistrationRefresh(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	register := func(annotation string) {
		doc, e := seedNativeDocument([]byte(fmt.Sprintf(`[{"header":{"rx-name":"sample"},"fields":[{"name":"time","type":[1,0],"size":6,"annotations":["@time"]},{"name":"duration","type":[1,0],"size":4,"annotations":%s}]}]`, annotation)))
		if e != nil {
			t.Fatal(e)
		}
		defer doc.close()
		if e = RegisterSeedDefinition(root, doc.bytes, SeedRegistration{Cache: "demo", Service: "lab", Resource: "events", ResourceType: "sample", File: "sample.rx"}); e != nil {
			t.Fatal(e)
		}
	}
	register("[]")
	p, e := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local"})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	route, e := taxonomyRoute(root, 1)
	if e != nil {
		t.Fatal(e)
	}
	if e = p.node.Prepare(route.Branch, uint64(time.Now().UnixMilli())); e != nil {
		t.Fatal(e)
	}
	register(`["#summ(numeric)"]`)
	day := time.Now().UTC().Truncate(24 * time.Hour).UnixMilli()
	r := httptest.NewRequest("GET", fmt.Sprintf("/demo/lab/events/sample/summaries?range=%d,%d", day, day+86400000), nil)
	r.Header.Set("Trunk-Branch", "1")
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("current summary: %d %s", w.Code, w.Body.String())
	}
}
