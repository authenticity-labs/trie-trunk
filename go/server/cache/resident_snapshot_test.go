//go:build trunk_prototype

package cache

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestImmutablePublicationRetainsBothRepresentations(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(at.Add(time.Minute).UnixMilli())
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return time.UnixMilli(clock.Load()) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Append(Read, testFrame(t, l, at, 1, false)); err != nil {
		t.Fatal(err)
	}
	old := s.resident(Read)
	defer old.release()
	before := old.live(at.UnixMilli(), at.Add(time.Hour).UnixMilli())
	saved := bytes.Clone(before.events[0].frame)
	if _, err = s.Append(Read, testFrame(t, l, at.Add(time.Second), 2, true)); err != nil {
		t.Fatal(err)
	}
	if got := old.live(at.UnixMilli(), at.Add(time.Hour).UnixMilli()); len(got.events) != 1 {
		t.Fatal("published tail changed")
	}
	next := s.resident(Read)
	if got := next.live(at.UnixMilli(), at.Add(time.Hour).UnixMilli()); len(got.events) != 2 {
		t.Fatal("new tail not published")
	}
	next.release()
	clock.Store(at.Add(80 * time.Minute).UnixMilli())
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	sealed := s.resident(Read)
	defer sealed.release()
	if len(sealed.ingress) != 0 || len(sealed.branch.sealedMemory) != 1 {
		t.Fatal("seal did not replace ingress with one PAC")
	}
	if len(s.branches[Read].memory) != 0 {
		t.Fatal("sealed PIL payload retained in ingress map")
	}
	if !bytes.Equal(before.events[0].frame, saved) {
		t.Fatal("sealing changed a published PIL")
	}
	pack := bytes.Join(sealed.branch.residentPack(sealed.branch.runs[0], time.Time{}), nil)
	if len(pack) == 0 {
		t.Fatal("missing resident PAC")
	}
	clock.Store(at.Add(26 * time.Hour).UnixMilli())
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	expired := s.resident(Read)
	if len(expired.branch.sealedMemory) != 0 {
		t.Fatal("hour 25 retained")
	}
	expired.release()
	if !bytes.Equal(pack, bytes.Join(sealed.branch.residentPack(sealed.branch.runs[0], time.Time{}), nil)) {
		t.Fatal("eviction invalidated retained PAC")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before.events[0].frame, saved) {
		t.Fatal("close invalidated retained PIL")
	}
}

func TestResourceReadsProceedWhileWriterMutexesHeld(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	now := at.Add(80 * time.Minute)
	p, err := OpenEndpoint(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s := p.api.Store
	if _, err = s.Append(Read, testFrame(t, l, at, 1, false)); err != nil {
		t.Fatal(err)
	}
	if err = s.Flush(); err != nil {
		t.Fatal(err)
	}
	b := s.branches[Read]
	run := b.runs[0]
	paths := []string{
		fmt.Sprintf("/demo/payment/transactions.v1/read/packs?beginTime=%d", at.UnixMilli()),
		fmt.Sprintf("/demo/payment/transactions.v1/read/indexes?generation=%x&rows=%d,%d", b.generation, run.Start-IndexHeaderSlots, run.End),
		fmt.Sprintf("/demo/payment/transactions.v1/read/timelines?beginTime=%d", at.UnixMilli()),
		fmt.Sprintf("/demo/payment/transactions.v1/read/summaries?range=%d,%d", at.Truncate(24*time.Hour).UnixMilli(), at.Truncate(24*time.Hour).Add(24*time.Hour).UnixMilli()),
		"/demo/payment/transactions.v1/states",
		"/demo/definitions?definition=" + fmtDigestNative(l.Digest),
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			p.mu.Lock()
			s.mu.Lock()
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { w := httptest.NewRecorder(); p.ServeHTTP(w, httptest.NewRequest("GET", path, nil)); done <- w }()
			var result *httptest.ResponseRecorder
			select {
			case result = <-done:
			case <-time.After(time.Second):
			}
			s.mu.Unlock()
			p.mu.Unlock()
			if result == nil {
				<-done
				t.Fatal("resource read waited on a writer mutex")
			}
			if result.Code != 200 {
				t.Fatal(result.Code, result.Body.String())
			}
		})
	}
}

// Exercise actual response writers while ingress and sealing replace publications.
// Every snapshot must contain each acknowledged record in exactly one form.
func TestConcurrentIngressSealAndResourceReads(t *testing.T) {
	l := testLayout(t, false)
	at := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(at.Add(time.Minute).UnixMilli())
	s, err := Open(Config{Root: t.TempDir(), Cache: "demo", Tenant: "test", Layout: l, Now: func() time.Time { return time.UnixMilli(clock.Load()) }})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	off := false
	s.policy.SearchIndex = &off
	if _, err = s.Append(Read, testFrame(t, l, at, 1, false)); err != nil {
		t.Fatal(err)
	}
	h := &HTTP{Store: s}
	stop := make(chan struct{})
	failures := make(chan error, 2)
	var readers sync.WaitGroup
	for i := 0; i < 2; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var previous uint64
			for {
				select {
				case <-stop:
					return
				default:
				}
				p := s.resident(Read)
				live := p.live(at.UnixMilli(), at.Add(12*time.Hour).UnixMilli())
				count := uint64(len(live.events))
				for _, run := range p.branch.runs {
					count += run.End - run.Start
				}
				p.release()
				if count < previous {
					failures <- fmt.Errorf("publication lost records: %d -> %d", previous, count)
					return
				}
				previous = count
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest("GET", fmt.Sprintf("/demo/payment/transactions.v1/read/packs?beginTime=%d", at.UnixMilli()), nil))
				if w.Code != 200 {
					failures <- fmt.Errorf("resource response %d: %s", w.Code, w.Body.String())
					return
				}
			}
		}()
	}
	defer func() {
		close(stop)
		readers.Wait()
		close(failures)
		for err := range failures {
			t.Error(err)
		}
	}()
	for hour := 0; hour < 3; hour++ {
		for n := 0; n < 8; n++ {
			eventTime := at.Add(time.Duration(hour)*time.Hour + time.Duration(n+1)*time.Second)
			clock.Store(eventTime.Add(time.Minute).UnixMilli())
			if _, err = s.Append(Read, testFrame(t, l, eventTime, uint32(hour*8+n+2), n%2 == 0)); err != nil {
				t.Fatal(err)
			}
		}
		clock.Store(at.Add(time.Duration(hour)*time.Hour + 80*time.Minute).UnixMilli())
		if err = s.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	p := s.resident(Read)
	defer p.release()
	count := 0
	for _, run := range p.branch.runs {
		count += int(run.End - run.Start)
	}
	if count != 25 || len(p.ingress) != 0 {
		t.Fatalf("final publication: %d sealed events, %d ingress hours", count, len(p.ingress))
	}
}
