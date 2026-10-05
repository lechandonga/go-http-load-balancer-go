package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newAdmin builds a gateway + admin API (mounted like cmd/gateway does).
func newAdmin(t *testing.T, cfg Config) (*LoadBalancer, *Pool, http.Handler) {
	t.Helper()
	lb, pool := newGateway(t, cfg)
	admin := NewAdminHandler(pool, cfg, testLogger(t))
	mux := http.NewServeMux()
	mux.Handle("/-/admin/", http.StripPrefix("/-/admin", admin))
	mux.Handle("/", lb)
	return lb, pool, mux
}

func adminDo(t *testing.T, h http.Handler, method, path, body string) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	resp := rec.Result()
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("admin response not JSON: %q", raw)
		}
	}
	return resp.StatusCode, decoded
}

// Scenario: register via admin API, list, and reject invalid/duplicate input.
func TestAdminRegisterListAndValidation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(upstream.Close)

	_, _, admin := newAdmin(t, testConfig())

	code, body := adminDo(t, admin, http.MethodPost, "/-/admin/backends",
		fmt.Sprintf(`{"name":"a1","url":%q}`, upstream.URL))
	if code != http.StatusCreated {
		t.Fatalf("register: got %d (%v), want 201", code, body)
	}
	if body["name"] != "a1" || body["healthy"] != true {
		t.Fatalf("register response malformed: %v", body)
	}
	t.Log("registered a1 via admin API: 201")

	code, _ = adminDo(t, admin, http.MethodPost, "/-/admin/backends",
		fmt.Sprintf(`{"url":%q}`, upstream.URL))
	if code != http.StatusConflict {
		t.Fatalf("duplicate url: got %d, want 409", code)
	}
	code, _ = adminDo(t, admin, http.MethodPost, "/-/admin/backends",
		`{"name":"a1","url":"http://127.0.0.1:9"}`)
	if code != http.StatusConflict {
		t.Fatalf("duplicate name: got %d, want 409", code)
	}
	t.Log("duplicate name/url rejected: 409")

	for _, bad := range []string{
		`{"url":"not-a-url"}`,
		`{"url":"ftp://x"}`,
		`{"url":"http://"}`,
		`{"name":"x"}`,
		`{`,
	} {
		code, body = adminDo(t, admin, http.MethodPost, "/-/admin/backends", bad)
		if code != http.StatusBadRequest {
			t.Fatalf("invalid input %s: got %d (%v), want 400", bad, code, body)
		}
	}
	t.Log("invalid URLs and malformed bodies rejected: 400")

	req := httptest.NewRequest(http.MethodGet, "/-/admin/backends", nil)
	rec := httptest.NewRecorder()
	admin.(http.Handler).ServeHTTP(rec, req)
	var list []BackendStatus
	raw, _ := io.ReadAll(rec.Result().Body)
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("list not JSON: %v", err)
	}
	if len(list) != 1 || list[0].Name != "a1" {
		t.Fatalf("list = %+v, want exactly a1", list)
	}
	t.Log("list shows the registered backend: OK")
}

// Scenario: deregistering an unknown backend is a stable, readable 404.
func TestAdminDeregisterNotFound(t *testing.T) {
	_, _, admin := newAdmin(t, testConfig())
	code, body := adminDo(t, admin, http.MethodDelete, "/-/admin/backends/ghost", "")
	if code != http.StatusNotFound || body["error"] != "backend_not_found" {
		t.Fatalf("got %d (%v), want 404 backend_not_found", code, body)
	}
	t.Log("deregister of unknown backend: 404 backend_not_found")
}

// Scenario: admin deregistration drains in-flight requests before returning.
func TestAdminDeregisterDrainsInFlight(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		fmt.Fprintln(w, "done")
	}))
	t.Cleanup(srv.Close)

	lb, pool, admin := newAdmin(t, testConfig())
	pool.Register(mustBackend(t, "slow", srv.URL))

	respCh := make(chan int, 1)
	go func() {
		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		respCh <- rec.Code
	}()
	<-entered

	drainCh := make(chan int, 1)
	go func() {
		code, _ := adminDo(t, admin, http.MethodDelete, "/-/admin/backends/slow", "")
		drainCh <- code
	}()
	eventually(t, "draining flag set", func() bool {
		return pool.GetByName("slow").Draining()
	})
	select {
	case code := <-drainCh:
		t.Fatalf("DELETE returned %d while request still in flight", code)
	case <-time.After(50 * time.Millisecond):
		t.Log("DELETE blocked while in-flight request runs: OK")
	}
	close(release)
	if code := <-respCh; code != http.StatusOK {
		t.Fatalf("in-flight request lost: got %d", code)
	}
	if code := <-drainCh; code != http.StatusOK {
		t.Fatalf("DELETE got %d, want 200 after drain", code)
	}
	if pool.GetByName("slow") != nil {
		t.Fatal("backend still present after drain")
	}
	t.Log("in-flight request completed, DELETE returned 200, backend gone: OK")
}

// Scenario: drain timeout is distinguishable (504 drain_timeout) and the
// in-flight request is still not lost.
func TestAdminDeregisterDrainTimeout(t *testing.T) {
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
	pool.Register(mustBackend(t, "stuck", srv.URL))

	respCh := make(chan int, 1)
	go func() {
		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		respCh <- rec.Code
	}()
	eventually(t, "request in flight", func() bool {
		return pool.GetByName("stuck").InFlight() == 1
	})

	code, body := adminDo(t, admin, http.MethodDelete, "/-/admin/backends/stuck", "")
	if code != http.StatusGatewayTimeout || body["error"] != "drain_timeout" {
		t.Fatalf("got %d (%v), want 504 drain_timeout", code, body)
	}
	t.Log("drain timeout reported distinctly: 504 drain_timeout")

	if got := pool.Next(nil); got != nil {
		t.Fatalf("timed-out backend still routable: %v", got.Name)
	}
	rel()
	if code := <-respCh; code != http.StatusOK {
		t.Fatalf("in-flight request lost after drain timeout: got %d", code)
	}
	t.Log("routing stopped at timeout; in-flight request still completed: OK")
}

// Scenario: concurrent deregistration of the same backend joins one drain;
// every caller observes the same successful result.
func TestConcurrentDeregisterSameBackend(t *testing.T) {
	release := make(chan struct{})
	var relOnce sync.Once
	rel := func() { relOnce.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(rel)

	_, pool := newGateway(t, testConfig())
	b := mustBackend(t, "shared", srv.URL)
	pool.Register(b)
	if !b.Acquire() {
		t.Fatal("acquire")
	}

	const callers = 5
	var started sync.WaitGroup
	started.Add(callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			started.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			errs[i] = pool.Deregister(ctx, b)
		}(i)
	}
	started.Wait()
	time.Sleep(50 * time.Millisecond) // let all callers join the drain
	b.Release()                       // finish the in-flight request
	rel()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d got %v, want nil (joined drain)", i, err)
		}
	}
	t.Log("5 concurrent Deregister calls joined one drain, all succeeded: OK")

	if err := pool.Deregister(context.Background(), b); err != ErrBackendNotFound {
		t.Fatalf("repeat deregister got %v, want ErrBackendNotFound", err)
	}
	t.Log("repeat deregister after completion: stable not-found")
}

// Scenario: admin deregistration under live traffic — after the drain
// returns, no request ever reaches the removed backend again.
func TestAdminDeregisterUnderTraffic(t *testing.T) {
	var hitsA, hitsB atomic.Int64
	mk := func(c *atomic.Int64) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c.Add(1)
		}))
	}
	s1, s2 := mk(&hitsA), mk(&hitsB)
	t.Cleanup(s1.Close)
	t.Cleanup(s2.Close)

	lb, pool, admin := newAdmin(t, testConfig())
	pool.Register(mustBackend(t, "victim", s1.URL))
	pool.Register(mustBackend(t, "survivor", s2.URL))

	stop := make(chan struct{})
	var failed atomic.Int64
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
				rec := doRequest(t, lb, http.MethodGet, "http://gw/")
				if rec.Code != http.StatusOK {
					failed.Add(1)
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	code, _ := adminDo(t, admin, http.MethodDelete, "/-/admin/backends/victim", "")
	if code != http.StatusOK {
		t.Fatalf("deregister got %d, want 200", code)
	}
	after := hitsA.Load()
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	if hitsA.Load() != after {
		t.Fatalf("removed backend received %d more requests after drain", hitsA.Load()-after)
	}
	if failed.Load() != 0 {
		t.Fatalf("%d client requests failed during deregistration", failed.Load())
	}
	if hitsB.Load() == 0 {
		t.Fatal("surviving backend received no traffic")
	}
	t.Logf("traffic continued on survivor (%d hits), removed backend untouched after drain: OK",
		hitsB.Load())
}

// Scenario: admin traffic itself is never proxied and never counted.
func TestAdminTrafficNotCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)

	_, pool, admin := newAdmin(t, testConfig())
	pool.Register(mustBackend(t, "b1", srv.URL))

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/-/admin/backends", nil)
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("admin list got %d", rec.Code)
		}
	}
	if got := pool.Metrics().Snapshot().Requests; got != 0 {
		t.Fatalf("admin requests leaked into metrics: requests=%d", got)
	}
	if c := pool.GetByName("b1").Counters(); c.Requests != 0 {
		t.Fatalf("admin requests reached backend counters: %+v", c)
	}
	t.Log("admin requests never touch forwarding metrics: OK")
}

// Scenario: re-registering the same name/URL while a drain is still in
// progress is rejected with a consistent 409, no duplicate instance ever
// appears, and a by-name deregister joins the ongoing drain instead of
// reporting "drained" while the old in-flight request is still running.
func TestAdminReregisterDuringDrainRejected(t *testing.T) {
	release := make(chan struct{})
	var relOnce sync.Once
	rel := func() { relOnce.Do(func() { close(release) }) }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		fmt.Fprintln(w, "done")
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(rel)

	lb, pool, admin := newAdmin(t, testConfig())
	pool.Register(mustBackend(t, "slow", srv.URL))

	respCh := make(chan int, 1)
	go func() {
		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		respCh <- rec.Code
	}()
	eventually(t, "slow request in flight", func() bool {
		return pool.GetByName("slow").InFlight() == 1
	})

	t.Log("deregister with short timeout: drain starts but cannot finish yet")
	code, body := adminDo(t, admin, http.MethodDelete,
		"/-/admin/backends/slow?timeout=50ms", "")
	if code != http.StatusGatewayTimeout || body["error"] != "drain_timeout" {
		t.Fatalf("got %d (%v), want 504 drain_timeout", code, body)
	}

	t.Log("while draining: same name/URL registrations must be rejected")
	for _, dup := range []string{
		fmt.Sprintf(`{"name":"slow","url":%q}`, srv.URL),  // same name + url
		`{"name":"slow","url":"http://127.0.0.1:9"}`,      // same name
		fmt.Sprintf(`{"name":"slow2","url":%q}`, srv.URL), // same url
	} {
		code, body = adminDo(t, admin, http.MethodPost, "/-/admin/backends", dup)
		if code != http.StatusConflict || body["error"] != "duplicate_backend" {
			t.Fatalf("register %s during drain: got %d (%v), want 409 duplicate_backend",
				dup, code, body)
		}
	}
	if n := len(pool.Snapshot()); n != 0 {
		t.Fatalf("duplicate instance appeared in pool during drain: %d backends", n)
	}
	t.Log("all duplicate registrations rejected with 409, pool has no duplicates: OK")

	t.Log("second DELETE by name joins the ongoing drain and must not report drained early")
	drainCh := make(chan int, 1)
	go func() {
		code, _ := adminDo(t, admin, http.MethodDelete, "/-/admin/backends/slow", "")
		drainCh <- code
	}()
	select {
	case code := <-drainCh:
		t.Fatalf("second DELETE returned %d while the old in-flight request is still running", code)
	case <-time.After(100 * time.Millisecond):
		t.Log("second DELETE still waiting for the old in-flight request: OK")
	}

	rel()
	if code := <-respCh; code != http.StatusOK {
		t.Fatalf("in-flight request lost: got %d", code)
	}
	if code := <-drainCh; code != http.StatusOK {
		t.Fatalf("second DELETE got %d, want 200 after the old request finished", code)
	}
	t.Log("old in-flight request finished; only then did DELETE report drained: OK")

	code, _ = adminDo(t, admin, http.MethodPost, "/-/admin/backends",
		fmt.Sprintf(`{"name":"slow","url":%q}`, srv.URL))
	if code != http.StatusCreated {
		t.Fatalf("re-register after drain completed: got %d, want 201", code)
	}
	if n := len(pool.Snapshot()); n != 1 {
		t.Fatalf("pool size = %d, want exactly 1 after re-register", n)
	}
	t.Log("after drain completed, same name/URL registers cleanly: 201")
}
