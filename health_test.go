package gateway

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// Scenario: health flapping. Single outliers must not flip the state;
// only consecutive threshold-reaching observations do.
func TestHealthStateMachineThresholds(t *testing.T) {
	b := mustBackend(t, "sm", "http://127.0.0.1:1")
	failThresh, recThresh := 3, 2

	step := func(ok bool, wantHealthy bool, wantChanged bool) {
		t.Helper()
		var failErr error
		if !ok {
			failErr = ErrBadResponse
		}
		changed, healthy := b.recordOutcome(ok, failErr, failThresh, recThresh)
		if changed != wantChanged || healthy != wantHealthy {
			t.Fatalf("recordOutcome(%v) = changed=%v healthy=%v, want changed=%v healthy=%v",
				ok, changed, healthy, wantChanged, wantHealthy)
		}
	}

	t.Log("single failures below threshold keep the backend healthy")
	step(false, true, false)
	step(false, true, false)
	step(true, true, false)
	step(false, true, false)
	step(false, true, false)
	t.Log("third consecutive failure flips to unhealthy")
	step(false, false, true)

	t.Log("single success below recovery threshold keeps it unhealthy")
	step(true, false, false)
	step(false, false, false)
	step(true, false, false)
	t.Log("second consecutive success flips back to healthy")
	step(true, true, true)
}

// Scenario: active probing drives the same state machine; a backend that
// starts failing probes is removed from rotation and later recovers.
func TestActiveHealthChecker(t *testing.T) {
	var up atomic.Bool
	up.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	_, pool := newGateway(t, cfg)
	b := mustBackend(t, "probed", srv.URL)
	pool.Register(b)

	checker := NewHealthChecker(pool, cfg, testLogger(t))
	checker.Start()
	t.Cleanup(checker.Stop)

	t.Log("backend starts healthy and stays healthy while probes pass")
	eventually(t, "initial healthy state", b.Healthy)

	up.Store(false)
	t.Log("probes now fail; waiting for consecutive-failure threshold")
	eventually(t, "backend marked unhealthy", func() bool { return !b.Healthy() })
	if got := pool.Next(nil); got != nil {
		t.Fatalf("unhealthy backend still routable: %v", got.Name)
	}

	up.Store(true)
	t.Log("probes recover; waiting for consecutive-success threshold")
	eventually(t, "backend marked healthy again", b.Healthy)
	if got := pool.Next(nil); got == nil {
		t.Fatal("recovered backend not routable")
	} else {
		got.Release()
	}
}

// Scenario: passive results from real requests participate in health.
// A backend returning 500s is marked unhealthy after the threshold and
// the gateway then fast-fails with 503.
func TestPassiveHealthFeedback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	lb, pool := newGateway(t, cfg)
	b := mustBackend(t, "flaky", srv.URL)
	pool.Register(b)

	for i := 0; i < cfg.FailThreshold; i++ {
		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("attempt %d: got %d, want 500 forwarded", i, rec.Code)
		}
	}
	if b.Healthy() {
		t.Fatal("backend still healthy after threshold 500s")
	}
	t.Log("backend unhealthy after consecutive 500s; gateway must fast-fail")

	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503 no_healthy_backends", rec.Code)
	}
	if got := rec.Header().Get("X-Lb-Error"); got != "no_healthy_backends" {
		t.Fatalf("X-Lb-Error = %q, want no_healthy_backends", got)
	}
}
