package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func adminSetup(t *testing.T, cfg Config, withLB bool) (string, *Pool, *LoadBalancer) {
	t.Helper()
	pool := NewPool(RoundRobin, testLogger(t))
	mux := http.NewServeMux()
	MountManagement(mux, "/-/admin/", "/-/status", pool, cfg, testLogger(t))
	var lb *LoadBalancer
	if withLB {
		var err error
		lb, err = NewLoadBalancer(pool, cfg, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle("/", lb)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL, pool, lb
}

func adminDo(t *testing.T, method, url string, body string) (int, map[string]any) {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	req, _ := http.NewRequest(method, url, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// Scenario: runtime registration is immediately routable; duplicate and
// illegal registrations return stable 4xx codes and never corrupt the pool.
func TestAdminRegister(t *testing.T) {
	base, pool, _ := adminSetup(t, testConfig(), false)

	status, body := adminDo(t, http.MethodPost, base+"/-/admin/backends",
		`{"name":"b1","url":"http://127.0.0.1:19101"}`)
	if status != http.StatusCreated || body["result"] != "registered" {
		t.Fatalf("valid register: got %d %v, want 201 registered", status, body)
	}
	if got := pool.Snapshot(); len(got) != 1 || got[0].Name != "b1" {
		t.Fatalf("pool after register = %v", got)
	}
	t.Log("POST /backends valid -> 201 registered: OK")

	status, body = adminDo(t, http.MethodPost, base+"/-/admin/backends",
		`{"name":"b1","url":"http://127.0.0.1:19102"}`)
	if status != http.StatusConflict || body["error"] != "backend_exists" {
		t.Fatalf("duplicate name: got %d %v, want 409 backend_exists", status, body)
	}
	status, _ = adminDo(t, http.MethodPost, base+"/-/admin/backends",
		`{"name":"other","url":"http://127.0.0.1:19101"}`)
	if status != http.StatusConflict {
		t.Fatalf("duplicate url: got %d, want 409", status)
	}
	if len(pool.Snapshot()) != 1 {
		t.Fatal("duplicate register mutated the pool")
	}
	t.Log("duplicate name/url -> 409 backend_exists, pool intact: OK")

	for _, tc := range []struct{ label, body string }{
		{"bad scheme", `{"name":"x","url":"ftp://127.0.0.1:1"}`},
		{"missing host", `{"name":"x","url":"http:///x"}`},
		{"missing url", `{"name":"x"}`},
		{"malformed json", `{not json`},
	} {
		status, body = adminDo(t, http.MethodPost, base+"/-/admin/backends", tc.body)
		if status != http.StatusBadRequest {
			t.Fatalf("%s: got %d %v, want 400", tc.label, status, body)
		}
		t.Logf("%s -> 400 %s: OK", tc.label, body["error"])
	}
	if len(pool.Snapshot()) != 1 {
		t.Fatal("invalid registrations mutated the pool")
	}

	status, _ = adminDo(t, http.MethodPost, base+"/-/admin/backends",
		`{"url":"http://127.0.0.1:19200"}`)
	if status != http.StatusCreated {
		t.Fatalf("default-name register: got %d, want 201", status)
	}
	if _, err := pool.Lookup("127.0.0.1-19200"); err != nil {
		t.Fatalf("auto-derived name not found: %v", err)
	}
	t.Log("omitted name auto-derived from host: OK")
}

// Scenario: DELETE drains in-flight work, then returns; concurrent deletes
// share one drain and all observe the same result.
func TestAdminDeregisterDrains(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		fmt.Fprintln(w, "done")
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	base, pool, lb := adminSetup(t, cfg, true)
	b := mustBackend(t, "slow", srv.URL)
	if err := pool.RegisterBackend(b); err != nil {
		t.Fatal(err)
	}

	respCh := make(chan int, 1)
	go func() {
		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		respCh <- rec.Code
	}()
	<-entered

	const n = 4
	results := make(chan int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _ := adminDo(t, http.MethodDelete, base+"/-/admin/backends/slow", "")
			results <- status
		}()
	}

	eventually(t, "backend draining", b.Draining)
	if got := pool.Next(nil); got != nil {
		t.Fatalf("draining backend still routable: %s", got.Name)
	}

	close(release)
	if code := <-respCh; code != http.StatusOK {
		t.Fatalf("in-flight request lost: got %d", code)
	}
	wg.Wait()
	close(results)
	for status := range results {
		if status != http.StatusOK {
			t.Fatalf("concurrent DELETE got %d, want 200", status)
		}
	}
	eventually(t, "entry removed after drain", func() bool {
		_, err := pool.Lookup("slow")
		return err == ErrBackendNotFound
	})
	t.Log("4 concurrent DELETEs: single drain, in-flight preserved, all 200: OK")
}

// Scenario: deleting an unknown or already-removed backend is a stable 404.
func TestAdminDeregisterNotFound(t *testing.T) {
	base, _, _ := adminSetup(t, testConfig(), false)
	status, body := adminDo(t, http.MethodDelete, base+"/-/admin/backends/ghost", "")
	if status != http.StatusNotFound || body["error"] != "backend_not_found" {
		t.Fatalf("got %d %v, want 404 backend_not_found", status, body)
	}
	status, _ = adminDo(t, http.MethodGet, base+"/-/admin/backends/ghost", "")
	if status != http.StatusNotFound {
		t.Fatalf("inspect unknown: got %d, want 404", status)
	}
	t.Log("unknown backend DELETE/GET -> 404 backend_not_found: OK")
}

// Scenario: a drain exceeding the request timeout answers 504 promptly,
// stops routing immediately, and continues draining server-side.
func TestAdminDrainTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	t.Cleanup(srv.Close)

	base, pool, _ := adminSetup(t, testConfig(), false)
	b := mustBackend(t, "stuck", srv.URL)
	if err := pool.RegisterBackend(b); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	go func() { _, _ = http.DefaultClient.Do(req) }()
	if !b.Acquire() {
		t.Fatal("acquire")
	}
	defer func() {
		cancel()
		b.Release()
	}()

	start := time.Now()
	status, body := adminDo(t, http.MethodDelete, base+"/-/admin/backends/stuck?timeout=100ms", "")
	elapsed := time.Since(start)
	if status != http.StatusGatewayTimeout || body["error"] != "drain_timeout" {
		t.Fatalf("got %d %v, want 504 drain_timeout", status, body)
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("management request hung for %s", elapsed)
	}
	if got := pool.Next(nil); got != nil {
		t.Fatal("timed-out drain still routes new traffic")
	}
	if _, err := pool.Lookup("stuck"); err != nil {
		t.Fatal("draining entry must remain visible until drain completes")
	}
	t.Logf("drain timeout -> 504 after %s, routing halted, drain continues: OK",
		elapsed.Round(time.Millisecond))
}

// Scenario: admin churn overlapping business traffic: once a DELETE has
// returned (drain complete), that backend receives zero further requests
// until it is re-registered.
func TestAdminChurnUnderTraffic(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRetries = 0
	cfg.DrainTimeout = 3 * time.Second
	base, _, _ := adminSetup(t, cfg, true)

	const n = 4
	type instance struct {
		name string
		srv  *httptest.Server
		hits atomic.Int64
	}
	instances := make([]*instance, n)
	for i := range instances {
		ins := &instance{name: fmt.Sprintf("churn-%d", i)}
		ins.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ins.hits.Add(1)
		}))
		instances[i] = ins
		status, _ := adminDo(t, http.MethodPost, base+"/-/admin/backends",
			fmt.Sprintf(`{"name":%q,"url":%q}`, ins.name, ins.srv.URL))
		if status != http.StatusCreated {
			t.Fatalf("register %s: %d", ins.name, status)
		}
	}
	t.Cleanup(func() {
		for _, ins := range instances {
			ins.srv.Close()
		}
	})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := http.Get(base + "/")
				if err == nil {
					_ = resp.Body.Close()
				}
			}
		}()
	}

	// For every instance: deregister (full drain), watch for a quiet window,
	// then re-register. Traffic generators run the whole time.
	for _, ins := range instances {
		done := make(chan struct{})
		go func() {
			req, _ := http.NewRequest(http.MethodDelete,
				base+"/-/admin/backends/"+ins.name+"?timeout=3s", nil)
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
			close(done)
		}()
		<-done

		// Drain completed: a quiet window must show zero hits.
		before := ins.hits.Load()
		time.Sleep(120 * time.Millisecond)
		if got := ins.hits.Load() - before; got != 0 {
			t.Fatalf("%s received %d requests after drain completed", ins.name, got)
		}
		t.Logf("%s: zero traffic in %s window after drain: OK",
			ins.name, 120*time.Millisecond)

		status, _ := adminDo(t, http.MethodPost, base+"/-/admin/backends",
			fmt.Sprintf(`{"name":%q,"url":%q}`, ins.name, ins.srv.URL))
		if status != http.StatusCreated {
			t.Fatalf("re-register %s: %d", ins.name, status)
		}
	}
	close(stop)
	wg.Wait()

	resp, err := http.Get(base + "/-/status")
	if err != nil {
		t.Fatal(err)
	}
	var doc StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if doc.Total.Requests == 0 {
		t.Fatal("global requests counter stayed zero under traffic")
	}
	t.Logf("churn survived; global requests=%d", doc.Total.Requests)
}
