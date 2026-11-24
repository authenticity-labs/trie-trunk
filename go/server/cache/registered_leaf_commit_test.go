package cache

import (
	"bytes"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRegisteredLeafHandoffUsesCommitSchedulerAndPublishesSnapshots(t *testing.T) {
	root := t.TempDir()
	t.Cleanup(func() { taxonomyRegistrations.Delete(filepath.Join(root, "trunk.trie")) })
	fixture := func(name string) []byte {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join("../../../testdata/registered-trunk", name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	register := func(minor uint32) {
		t.Helper()
		if err := RegisterSeedDefinition(root, fixture("sample.rx"), SeedRegistration{
			Cache: "demo", Service: "payment", Resource: "transactions", ResourceType: "http", File: "sample.rx",
			Selection: &SeedDefinitionSelection{Major: 1, Namespace: "measurements", Name: "sample", Minor: minor},
		}); err != nil {
			t.Fatal(err)
		}
	}
	register(0)
	hour := time.UnixMilli(1_793_520_000_000)
	now := hour.Add(75 * time.Minute)
	scheduler := NewCommitScheduler(1)
	endpoint, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", CommitScheduler: scheduler, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	path := "demo/payment/transactions/http.v1.leaf"
	if err = endpoint.commitLeafBatch(1, hour.Add(time.Hour), 1, fixture("old-nodes.pil")); err == nil {
		t.Fatal("committed inside straggler window")
	}
	if err = endpoint.commitLeafBatch(1, hour, 1, fixture("old-nodes.pil")); err != nil {
		t.Fatal(err)
	}
	before, err := endpoint.registeredLeaf(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before.bytes, fixture("old.leaf")) {
		t.Fatal("first commit differs from Rust leaf fixture")
	}
	register(1)
	now = hour.Add(135 * time.Minute)
	// A failed file write must neither freeze the old root nor publish a new
	// snapshot. Retrying the exact node batch must still be possible.
	writer := endpoint.registeredLeafWriters[path]
	if err = writer.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err = endpoint.commitLeafBatch(2, hour.Add(time.Hour), 1, fixture("current-nodes.pil")); err == nil {
		t.Fatal("closed file write succeeded")
	}
	failed, e := endpoint.registeredLeaf(path)
	if e != nil || failed != before {
		t.Fatal("failed write replaced existing reader", e)
	}
	writer.file = nil

	release := scheduler.acquire()
	done := make(chan error, 1)
	nextNodes := fixture("current-nodes.pil")
	go func() { done <- endpoint.commitLeafBatch(2, hour.Add(time.Hour), 1, nextNodes) }()
	deadline := time.Now().Add(5 * time.Second)
	for scheduler.Stats().Waiting == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if scheduler.Stats().Waiting != 1 {
		release()
		t.Fatal("leaf handoff bypassed commit scheduler")
	}
	whileWaiting, e := endpoint.registeredLeaf(path)
	if e != nil || whileWaiting != before {
		release()
		t.Fatal("waiting commit blocked or replaced existing reader", e)
	}
	release()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	after, err := endpoint.registeredLeaf(path)
	if err != nil {
		t.Fatal(err)
	}
	if after == before || len(after.definitions) != 2 {
		t.Fatal("root handoff was not published")
	}
	if !bytes.Equal(before.bytes, fixture("old.leaf")) {
		t.Fatal("existing reader snapshot changed")
	}
	if !bytes.Equal(after.bytes[:80], before.bytes[:80]) {
		t.Fatal("existing root moved")
	}
	request := httptest.NewRequest("GET", "/demo/payment/transactions/leaves", nil)
	request.Header.Set("Trunk-Branch", "1")
	response := httptest.NewRecorder()
	endpoint.ServeHTTP(response, request)
	if response.Code != 200 || !bytes.Equal(response.Body.Bytes(), after.bytes) {
		t.Fatal("replication did not see committed roots", response.Code)
	}
	if err = endpoint.commitLeafBatch(1, hour, 1, fixture("old-nodes.pil")); err == nil {
		t.Fatal("retired version accepted another commit")
	}
	retained, e := endpoint.registeredLeaf(path)
	if e != nil || retained != after {
		t.Fatal("rejected commit invalidated reader", e)
	}
	// Reopen the actual file with a fresh endpoint: the Rust footer selects
	// the same versions without any host state or old Definition decoder.
	if err = endpoint.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenEndpoint(Config{Root: root, Cache: "demo", Tenant: "local", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snapshot, err := reopened.registeredLeaf(path)
	if err != nil || !bytes.Equal(snapshot.bytes, after.bytes) {
		t.Fatal("committed roots did not survive reopen", err)
	}
}
