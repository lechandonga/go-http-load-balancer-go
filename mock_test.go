package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// drillControl posts to a mock's control endpoint.
func drillControl(t *testing.T, m *MockBackend, path string) {
	t.Helper()
	resp, err := http.Post(m.URL()+path, "application/json", nil)
	if err != nil {
		t.Fatalf("drill control %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("drill control %s: got %d", path, resp.StatusCode)
	}
}

// newDrillGateway starts n mock backends behind a fresh gateway and
// returns everything a drill scenario needs. Active probing is slowed
// down so scenarios are driven by passive (real request) signals only.
func newDrillGateway(t *testing.T, n int) (*LoadBalancer, *Pool, http.Handler, []*MockBackend) {
	t.Helper()
	cfg := testConfig()
	cfg.HealthInterval = time.Hour // passive-only scenarios
	lb, pool := newGateway(t, cfg)
	admin := NewAdminHandler(pool, cfg, testLogger(t))
	mux := http.NewServeMux()
	mux.Handle("/-/admin/", http.StripPrefix("/-/admin", admin))
	mux.Handle("/", lb)

	var mocks []*MockBackend
	for i := 0; i < n; i++ {
		m, err := NewMockBackend(fmt.Sprintf("drill-%d", i+1), "127.0.0.1:0")
		if err != nil {
			t.Fatalf("mock %d: %v", i, err)
		}
		t.Cleanup(m.Close)
		mocks = append(mocks, m)
		pool.Register(mustBackend(t, m.Name, m.URL()))
	}
	return lb, pool, mux, mocks
}

func getBody(t *testing.T, lb *LoadBalancer) (int, string) {
	t.Helper()
	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	return rec.Code, bodyOf(t, rec)
}

// Scenario: mock backend runtime control — error injection, hang,
// down/up — all take effect immediately over HTTP.
func TestMockBackendControl(t *testing.T) {
	m, err := NewMockBackend("m1", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)

	get := func(path string) (int, string) {
		t.Helper()
		resp, err := http.Get(m.URL() + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}

	code, body := get("/")
	if code != http.StatusOK || !strings.Contains(body, "m1") {
		t.Fatalf("normal: got %d %q", code, body)
	}

	drillControl(t, m, "/-/drill/error")
	if code, _ := get("/"); code != http.StatusInternalServerError {
		t.Fatalf("error injection: got %d, want 500", code)
	}
	drillControl(t, m, "/-/drill/error?enabled=false")

	drillControl(t, m, "/-/drill/hang?d=200ms")
	start := time.Now()
	if code, _ := get("/"); code != http.StatusOK {
		t.Fatalf("hang: got %d, want 200 after delay", code)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("hang took only %s", elapsed)
	}
	drillControl(t, m, "/-/drill/hang?d=0s")

	drillControl(t, m, "/-/drill/down")
	if code, _ := get("/healthz"); code != http.StatusServiceUnavailable {
		t.Fatalf("down healthz: got %d, want 503", code)
	}
	if code, _ := get("/"); code != http.StatusServiceUnavailable {
		t.Fatalf("down traffic: got %d, want 503", code)
	}
	drillControl(t, m, "/-/drill/up")
	if code, _ := get("/"); code != http.StatusOK {
		t.Fatalf("up: got %d, want 200", code)
	}

	resp, err := http.Get(m.URL() + "/-/drill")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var state map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		t.Fatalf("state not JSON: %v", err)
	}
	if state["up"] != true || state["error_injection"] != false {
		t.Fatalf("unexpected state: %v", state)
	}
	t.Log("error injection / hang / down / up all take effect immediately: OK")
}

// Scenario: drill chain — round robin distributes across mocks.
func TestDrillRoundRobinDistribution(t *testing.T) {
	lb, _, _, mocks := newDrillGateway(t, 2)
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		code, body := getBody(t, lb)
		if code != http.StatusOK {
			t.Fatalf("got %d", code)
		}
		for _, m := range mocks {
			if strings.Contains(body, m.Name) {
				seen[m.Name]++
			}
		}
	}
	if seen["drill-1"] != 2 || seen["drill-2"] != 2 {
		t.Fatalf("distribution = %v, want 2/2", seen)
	}
	t.Log("round robin across mocks: 2/2")
}

// Scenario: drill chain — injected 5xx crosses the failure threshold, the
// instance is removed from rotation and traffic flows around it.
func TestDrillErrorInjectionEvictsBackend(t *testing.T) {
	cfg := testConfig()
	lb, pool, _, mocks := newDrillGateway(t, 2)
	_ = cfg

	drillControl(t, mocks[0], "/-/drill/error")
	// Round robin alternates, so drill-1 sees every other request; 5
	// requests give it the 3 consecutive failures needed to trip.
	for i := 0; i < 5; i++ {
		getBody(t, lb)
	}
	bad := pool.GetByName("drill-1")
	eventually(t, "drill-1 unhealthy", func() bool { return !bad.Healthy() })

	for i := 0; i < 4; i++ {
		code, body := getBody(t, lb)
		if code != http.StatusOK || !strings.Contains(body, "drill-2") {
			t.Fatalf("traffic not rerouted: got %d %q", code, body)
		}
	}
	t.Log("injected 5xx evicted drill-1 after threshold; all traffic on drill-2: OK")

	drillControl(t, mocks[0], "/-/drill/error?enabled=false")
}

// Scenario: drill chain — while 5xx injection is active the instance stays
// evicted across many active-probe cycles (no flapping), and only comes
// back via the recovery threshold after the injection is cancelled.
func TestDrillInjectedErrorStaysEvictedAcrossProbeCycles(t *testing.T) {
	cfg := testConfig() // active probing every 20ms
	lb, pool := newGateway(t, cfg)
	var mocks []*MockBackend
	for i := 1; i <= 2; i++ {
		m, err := NewMockBackend(fmt.Sprintf("drill-%d", i), "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Close)
		mocks = append(mocks, m)
		pool.Register(mustBackend(t, m.Name, m.URL()))
	}
	checker := NewHealthChecker(pool, cfg, testLogger(t))
	checker.Start()
	t.Cleanup(checker.Stop)

	drillControl(t, mocks[0], "/-/drill/error")
	t.Log("5xx injected into drill-1; driving traffic to cross the failure threshold")
	for i := 0; i < 5; i++ {
		getBody(t, lb)
	}
	bad := pool.GetByName("drill-1")
	eventually(t, "drill-1 unhealthy", func() bool { return !bad.Healthy() })

	t.Log("waiting across many active-probe cycles: drill-1 must stay evicted")
	time.Sleep(30 * cfg.HealthInterval)
	if bad.Healthy() {
		t.Fatal("drill-1 flapped back to healthy while 5xx injection still active")
	}
	for i := 0; i < 6; i++ {
		code, body := getBody(t, lb)
		if code != http.StatusOK || !strings.Contains(body, "drill-2") {
			t.Fatalf("traffic not rerouted after probe cycles: got %d %q", code, body)
		}
	}
	if bad.Healthy() {
		t.Fatal("drill-1 recovered while still returning 500")
	}
	t.Log("injection active: drill-1 stayed unhealthy, all traffic on drill-2: OK")

	drillControl(t, mocks[0], "/-/drill/error?enabled=false")
	t.Log("injection cancelled; waiting for recovery threshold via active probes")
	eventually(t, "drill-1 healthy again", bad.Healthy)
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		_, body := getBody(t, lb)
		seen[strings.TrimSpace(strings.Split(body, " ")[2])] = true
	}
	if !seen["drill-1"] {
		t.Fatalf("recovered drill-1 not back in rotation: %v", seen)
	}
	t.Log("after cancelling injection, drill-1 recovered and rejoined rotation: OK")
}

// Scenario: drill chain — hang triggers the distinguishable timeout error.
func TestDrillHangTriggersTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.BackendTimeout = 100 * time.Millisecond
	cfg.MaxRetries = 0
	lb, pool := newGateway(t, cfg)
	m, err := NewMockBackend("hang", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	pool.Register(mustBackend(t, "hang", m.URL()))

	drillControl(t, m, "/-/drill/hang?d=5s")
	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("got %d, want 504", rec.Code)
	}
	if got := rec.Header().Get("X-Lb-Error"); got != "backend_timeout" {
		t.Fatalf("X-Lb-Error = %q, want backend_timeout", got)
	}
	if got := pool.Metrics().Snapshot().FailTimeout; got != 1 {
		t.Fatalf("fail_timeout = %d, want 1", got)
	}
	t.Log("hang produced distinguishable 504 backend_timeout, counted in metrics: OK")
}

// Scenario: drill chain — all backends down fails fast and distinctly.
func TestDrillAllDownFastFail(t *testing.T) {
	lb, _, _, mocks := newDrillGateway(t, 2)
	for _, m := range mocks {
		drillControl(t, m, "/-/drill/down")
	}
	// Drive passive failures until both are unhealthy.
	for i := 0; i < 6; i++ {
		getBody(t, lb)
	}
	start := time.Now()
	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("X-Lb-Error"); got != "no_healthy_backends" {
		t.Fatalf("X-Lb-Error = %q, want no_healthy_backends", got)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("not a fast fail: %s", elapsed)
	}
	t.Log("all mocks down: fast, distinguishable 503 no_healthy_backends: OK")
}

// Scenario: drill chain — deregistering a mock mid slow request lets the
// slow request finish, and the mock never sees new traffic afterwards.
func TestDrillDeregisterDuringSlowRequest(t *testing.T) {
	lb, pool, admin, mocks := newDrillGateway(t, 2)

	drillControl(t, mocks[0], "/-/drill/hang?d=300ms")

	respCh := make(chan int, 1)
	go func() {
		rec := doRequest(t, lb, http.MethodGet, "http://gw/?slow")
		respCh <- rec.Code
	}()
	eventually(t, "slow request in flight on drill-1", func() bool {
		return pool.GetByName("drill-1").InFlight() == 1
	})

	// Deregister via the admin API while the slow request is running.
	req := httptest.NewRequest(http.MethodDelete, "/-/admin/backends/drill-1", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin deregister got %d, want 200 (drain completed)", rec.Code)
	}
	if code := <-respCh; code != http.StatusOK {
		t.Fatalf("slow request lost during drain: got %d", code)
	}
	t.Log("slow request finished cleanly while drill-1 was deregistered: OK")

	drillControl(t, mocks[0], "/-/drill/hang?d=0s")
	for i := 0; i < 4; i++ {
		code, body := getBody(t, lb)
		if code != http.StatusOK || !strings.Contains(body, "drill-2") {
			t.Fatalf("drill-1 still receiving traffic: %d %q", code, body)
		}
	}
	t.Log("after drain, drill-1 receives no new traffic: OK")
}
