package cache

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
)

func quotaBool(b bool) *bool       { return &b }
func quotaNumber(n uint64) *uint64 { return &n }
func TestQuotaAllocationValidation(t *testing.T) {
	read := []string{"demo", "svc", "res", "read"}
	write := []string{"demo", "svc", "res", "write"}
	policy := quotaPolicy{Pool: quotaNumber(1000), Scopes: []quotaScope{{Path: read, Limit: quotaNumber(200)}, {Path: write, Limit: quotaNumber(300)}, {Path: []string{"demo", "svc"}, Managed: quotaBool(true), Limit: quotaNumber(600)}}}
	if err := validateQuotas(policy, 2000); err != nil {
		t.Fatal(err)
	}
	policy.Scopes[0].Limit = quotaNumber(301)
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("accepted sibling over-allocation")
	}
	policy.Scopes[0].Limit = quotaNumber(200)
	policy.Pool = quotaNumber(500)
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("accepted child gate above root")
	}
	policy.Pool = quotaNumber(1000)
	if validateQuotas(policy, 999) == nil {
		t.Fatal("accepted capacity above disk")
	}
	policy.Scopes[2].Recovery = &quotaRecovery{Unit: "percent", Value: 25}
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("accepted intermediate recovery")
	}
	policy.Scopes[2].Recovery = nil
	policy.Scopes[0].Recovery = &quotaRecovery{Unit: "bytes", Value: 201}
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("accepted recovery above quota")
	}
}
func TestQuotaPersistenceAndPublicationConflict(t *testing.T) {
	root := t.TempDir()
	p := registeredConfigurationEndpoint(t, root)
	var err error
	call := func(method, token, revision string, body []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/storage/quotas", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("If-Match", `"`+revision+`"`)
		w := httptest.NewRecorder()
		p.Handler("secret").ServeHTTP(w, r)
		return w
	}
	if w := call("GET", "", "", nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
	w := call("GET", "secret", "", nil)
	var state quotaSettings
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &state) != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	if state.Revision != "unset" || state.Enforced || state.Disk.Maximum == 0 || state.Disk.Maximum > state.Disk.Total {
		t.Fatal(state)
	}
	policy := quotaPolicy{Pool: quotaNumber(min(1<<30, state.Disk.Maximum)), Scopes: []quotaScope{{Path: []string{"demo", "payment", "transactions", "http"}, Limit: quotaNumber(100), Recovery: &quotaRecovery{Unit: "percent", Value: 25}}}}
	policy.Groups = []quotaGroup{{Path: []string{"demo", "payment", "transactions"}, ResourceType: "Custom", Managed: true, Limit: 200, RecoveryPercent: 30}}
	raw, _ := json.Marshal(policy)
	w = call("PUT", "secret", state.Revision, raw)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if json.Unmarshal(w.Body.Bytes(), &state) != nil || state.Enforced || state.Policy.Pool == nil || len(state.Policy.Groups) != 1 || state.Policy.Groups[0].RecoveryPercent != 30 {
		t.Fatal(w.Body.String())
	}
	if w = call("PUT", "secret", "unset", raw); w.Code != 412 {
		t.Fatal(w.Code)
	}
	fake := quotaPolicy{Pool: policy.Pool, Scopes: []quotaScope{{Path: []string{"demo", "payment", "transactions", "error.read"}, Limit: quotaNumber(1)}}}
	fakeRaw, _ := json.Marshal(fake)
	if rejected := call("PUT", "secret", state.Revision, fakeRaw); rejected.Code != 400 {
		t.Fatal("unknown physical quota scope accepted", rejected.Code)
	}
	published, _ := os.ReadFile(configurationPath(root, nil))
	w = call("PUT", "secret", state.Revision, []byte(`{"poolBytes":1,"scopes":[]}`+`{}`))
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	after, _ := os.ReadFile(configurationPath(root, nil))
	if !bytes.Equal(published, after) {
		t.Fatal("invalid update changed publication")
	}
	diskPolicy, revision, err := readQuotaPolicy(root)
	if err != nil || revision != state.Revision || *diskPolicy.Pool != *policy.Pool || len(diskPolicy.Groups) != 1 || diskPolicy.Groups[0].Limit != 200 {
		t.Fatal(diskPolicy, revision, err)
	}
	if w = call("POST", "secret", "", nil); w.Code != 405 {
		t.Fatal(w.Code)
	}
}

func TestQuotaConcurrentAdministrators(t *testing.T) {
	root := t.TempDir()
	first, err := OpenEndpoint(Config{Root: root, Cache: "one", Tenant: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenEndpoint(Config{Root: root, Cache: "two", Tenant: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for i, p := range []*Endpoint{first, second} {
		wg.Add(1)
		go func(i int, p *Endpoint) {
			defer wg.Done()
			raw, _ := json.Marshal(quotaPolicy{Pool: quotaNumber(uint64(100 + i)), Scopes: []quotaScope{}})
			req := httptest.NewRequest("PUT", "/storage/quotas", bytes.NewReader(raw))
			req.Header.Set("If-Match", `"unset"`)
			w := httptest.NewRecorder()
			p.Handler("").ServeHTTP(w, req)
			codes <- w.Code
		}(i, p)
	}
	wg.Wait()
	close(codes)
	successes, conflicts := 0, 0
	for code := range codes {
		if code == 200 {
			successes++
		} else if code == 412 {
			conflicts++
		} else {
			t.Fatal(code)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatal(successes, conflicts)
	}
}

func TestQuotaManagedDefaultsAndInactiveLimits(t *testing.T) {
	for depth := 0; depth <= 4; depth++ {
		if quotaManaged(nil, depth) != true {
			t.Fatal(depth)
		}
	}
	leaf := []string{"demo", "svc", "res", "read"}
	policy := quotaPolicy{Pool: quotaNumber(1000), Scopes: []quotaScope{
		{Path: []string{"demo", "svc"}, Managed: quotaBool(false), Limit: quotaNumber(100)},
		{Path: leaf, Limit: quotaNumber(500)},
	}}
	if err := validateQuotas(policy, 2000); err != nil {
		t.Fatal(err)
	}
	policy.Scopes[0].Managed = quotaBool(true)
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("reenabled undersized gate accepted")
	}
	policy.Scopes[1].Managed = quotaBool(false)
	policy.Scopes[1].Recovery = &quotaRecovery{Unit: "bytes", Value: 450}
	if err := validateQuotas(policy, 2000); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(policy)
	var roundtrip quotaPolicy
	if err := json.Unmarshal(raw, &roundtrip); err != nil {
		t.Fatal(err)
	}
	if *roundtrip.Scopes[1].Managed || *roundtrip.Scopes[1].Limit != 500 || roundtrip.Scopes[1].Recovery.Value != 450 {
		t.Fatal(roundtrip)
	}
	policy.Managed = quotaBool(false)
	policy.Pool = quotaNumber(5000)
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("root flag bypassed physical budget")
	}
	policy.Pool = quotaNumber(1000)
	policy.Scopes[0].Limit = quotaNumber(2001)
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("unmanaged root allowed active child to exceed disk")
	}
}

func TestSharedResourceQuotaValidation(t *testing.T) {
	resource := []string{"demo", "svc", "res"}
	read := append(append([]string{}, resource...), "read")
	write := append(append([]string{}, resource...), "write")
	policy := quotaPolicy{Pool: quotaNumber(1000), Scopes: []quotaScope{
		{Path: read, Limit: quotaNumber(700)}, {Path: write, Limit: quotaNumber(600)},
	}, Groups: []quotaGroup{{Path: resource, ResourceType: "Logs", Managed: true, Limit: 800, RecoveryPercent: 25}}}
	if err := validateQuotas(policy, 2000); err != nil {
		t.Fatal("counted dormant leaf allocations", err)
	}
	policy.Groups[0].Managed = false
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("restored overallocated leaves")
	}
	policy.Groups[0].Managed = true
	policy.Scopes = append(policy.Scopes, quotaScope{Path: []string{"demo", "svc", "other", "read"}, Limit: quotaNumber(201)})
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("shared pool did not reserve capacity")
	}
	policy.Scopes = policy.Scopes[:2]
	policy.Groups[0].RecoveryPercent = 101
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("invalid recovery percentage")
	}
	policy.Groups[0].RecoveryPercent = 25
	policy.Groups = append(policy.Groups, policy.Groups[0])
	if validateQuotas(policy, 2000) == nil {
		t.Fatal("duplicate shared pool")
	}
}

func TestQuotaAncestorAllotmentsReserveOnce(t *testing.T) {
	p := quotaPolicy{Pool: quotaNumber(1000), Scopes: []quotaScope{
		{Path: []string{"a"}, Limit: quotaNumber(600)},
		{Path: []string{"b"}, Limit: quotaNumber(400)},
		{Path: []string{"a", "service", "resource", "info"}, Limit: quotaNumber(600)},
	}}
	if err := validateQuotas(p, 2000); err != nil {
		t.Fatal(err)
	}
	p.Scopes[1].Limit = quotaNumber(401)
	if validateQuotas(p, 2000) == nil {
		t.Fatal("unallocated ancestor capacity not reserved")
	}
	p.Scopes[1].Limit = quotaNumber(400)
	p.Scopes[2].Limit = quotaNumber(601)
	if validateQuotas(p, 2000) == nil {
		t.Fatal("grandchild exceeded nearest allotment")
	}
}
