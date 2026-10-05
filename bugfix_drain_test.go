package gateway

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Scenario (drain duplicate registration): while a deregistered backend
// is still draining an in-flight request, registering the same name or
// the same address is rejected with a stable 409 and never creates a
// duplicate; deregistering by name does not report drained while the old
// in-flight request is still running. Only after it finishes can the
// name/address be registered again.
func TestDrainRejectsDuplicateUntilInFlightFinished(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var relOnce sync.Once
	rel := func() { relOnce.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		fmt.Fprintln(w, "done")
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(rel)

	lb, pool, admin := newAdmin(t, testConfig())
	pool.Register(mustBackend(t, "dup", srv.URL))

	respCh := make(chan int, 1)
	go func() {
		respCh <- doRequest(t, lb, http.MethodGet, "http://gw/").Code
	}()
	<-entered

	drainCh := make(chan int, 1)
	go func() {
		code, _ := adminDo(t, admin, http.MethodDelete, "/-/admin/backends/dup", "")
		drainCh <- code
	}()
	eventually(t, "dup draining", func() bool { return pool.GetByName("dup").Draining() })

	// Repeated re-registration attempts (same name+url, same name with a
	// different url, same url under a different name) are all refused.
	for i := 0; i < 5; i++ {
		code, body := adminDo(t, admin, http.MethodPost, "/-/admin/backends",
			fmt.Sprintf(`{"name":"dup","url":%q}`, srv.URL))
		if code != http.StatusConflict || body["error"] != "duplicate_backend" {
			t.Fatalf("same name+url during drain: got %d %v, want 409 duplicate_backend", code, body)
		}
	}
	code, _ := adminDo(t, admin, http.MethodPost, "/-/admin/backends",
		fmt.Sprintf(`{"name":"dup","url":"http://127.0.0.1:1"}`))
	if code != http.StatusConflict {
		t.Fatalf("same name during drain: got %d, want 409", code)
	}
	code, _ = adminDo(t, admin, http.MethodPost, "/-/admin/backends",
		fmt.Sprintf(`{"name":"other","url":%q}`, srv.URL))
	if code != http.StatusConflict {
		t.Fatalf("same url during drain: got %d, want 409", code)
	}
	t.Log("same name or same address rejected with 409 duplicate_backend while draining: OK")

	// No duplicate backend exists: exactly one instance with the name and
	// one with the address, and it is the draining one.
	all := pool.allInstances()
	var nameCount, urlCount int
	for _, b := range all {
		if b.Name == "dup" {
			nameCount++
		}
		if b.URL.String() == srv.URL {
			urlCount++
		}
	}
	if nameCount != 1 || urlCount != 1 || len(pool.Snapshot()) != 0 {
		t.Fatalf("duplicate instance during drain: name=%d url=%d routable=%d",
			nameCount, urlCount, len(pool.Snapshot()))
	}

	// A second DELETE by name joins the real drain and must not return
	// before the old in-flight request finishes.
	secondDelete := make(chan int, 1)
	go func() {
		c, _ := adminDo(t, admin, http.MethodDelete, "/-/admin/backends/dup", "")
		secondDelete <- c
	}()
	select {
	case c := <-drainCh:
		t.Fatalf("first DELETE returned %d while in-flight request still running", c)
	case c := <-secondDelete:
		t.Fatalf("second DELETE by name returned %d (reported drained) while old request still running", c)
	case <-time.After(100 * time.Millisecond):
		t.Log("both DELETE calls stay blocked until the in-flight request winds down: OK")
	}

	rel()
	if c := <-respCh; c != http.StatusOK {
		t.Fatalf("in-flight request lost: got %d", c)
	}
	select {
	case c := <-drainCh:
		if c != http.StatusOK {
			t.Fatalf("first DELETE got %d, want 200", c)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first DELETE never returned after drain")
	}
	select {
	case c := <-secondDelete:
		if c != http.StatusOK {
			t.Fatalf("joined DELETE got %d, want 200", c)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("joined DELETE never returned after drain")
	}

	// Fully drained: name/address can be registered again, exactly once.
	code, _ = adminDo(t, admin, http.MethodPost, "/-/admin/backends",
		fmt.Sprintf(`{"name":"dup","url":%q}`, srv.URL))
	if code != http.StatusCreated {
		t.Fatalf("re-register after drain: got %d, want 201", code)
	}
	code, _ = adminDo(t, admin, http.MethodPost, "/-/admin/backends",
		fmt.Sprintf(`{"name":"dup","url":%q}`, srv.URL))
	if code != http.StatusConflict {
		t.Fatalf("second re-register: got %d, want 409", code)
	}
	t.Log("re-registration allowed exactly once only after the drain completed: OK")
}

// Scenario: drain-timeout re-registration is also refused while the
// background drain is still running, and is allowed once it completes.
func TestDrainTimeoutStillRejectsDuplicateRegistration(t *testing.T) {
	release := make(chan struct{})
	var relOnce sync.Once
	rel := func() { relOnce.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(rel)

	cfg := testConfig()
	cfg.DrainTimeout = 100 * time.Millisecond
	lb, pool, admin := newAdmin(t, cfg)
	pool.Register(mustBackend(t, "slow", srv.URL))

	respCh := make(chan int, 1)
	go func() {
		respCh <- doRequest(t, lb, http.MethodGet, "http://gw/").Code
	}()
	eventually(t, "request in flight", func() bool {
		return pool.GetByName("slow").InFlight() == 1
	})

	code, body := adminDo(t, admin, http.MethodDelete, "/-/admin/backends/slow", "")
	if code != http.StatusGatewayTimeout || body["error"] != "drain_timeout" {
		t.Fatalf("got %d %v, want 504 drain_timeout", code, body)
	}

	// Background drain still in flight: duplicate registration refused.
	code, _ = adminDo(t, admin, http.MethodPost, "/-/admin/backends",
		fmt.Sprintf(`{"name":"slow","url":%q}`, srv.URL))
	if code != http.StatusConflict {
		t.Fatalf("re-register during background drain: got %d, want 409", code)
	}

	// A direct pool registration with the draining identity fails too.
	if err := pool.Register(mustBackend(t, "slow", srv.URL)); err != ErrBackendExists {
		t.Fatalf("pool.Register during background drain: %v, want ErrBackendExists", err)
	}

	rel()
	if c := <-respCh; c != http.StatusOK {
		t.Fatalf("in-flight request lost after drain timeout: got %d", c)
	}
	eventually(t, "background drain finished", func() bool {
		return pool.GetByName("slow") == nil || !pool.GetByName("slow").Draining()
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodPost, "/-/admin/backends",
			strings.NewReader(fmt.Sprintf(`{"name":"slow","url":%q}`, srv.URL)))
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, req)
		if rec.Code == http.StatusCreated {
			t.Log("re-registration accepted after the background drain finished: OK")
			return
		}
		if rec.Code != http.StatusConflict {
			t.Fatalf("re-register after background drain: got %d", rec.Code)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("re-registration stayed rejected after the background drain completed")
}
