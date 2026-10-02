package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// deadBackendURL returns a URL where nothing listens (connection refused).
func deadBackendURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	u := srv.URL
	srv.Close()
	return u
}

// Scenario: the first backend is unreachable; the retry must go to a
// different healthy backend and succeed.
func TestRetryFailsOverToHealthyBackend(t *testing.T) {
	var hits atomic.Int64
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprintln(w, "from good")
	}))
	t.Cleanup(good.Close)

	cfg := testConfig()
	lb, pool := newGateway(t, cfg)
	pool.Register(mustBackend(t, "dead", deadBackendURL(t)))
	pool.Register(mustBackend(t, "good", good.URL))

	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d (%s), want 200 via failover", rec.Code, bodyOf(t, rec))
	}
	if hits.Load() != 1 {
		t.Fatalf("good backend hit %d times, want 1", hits.Load())
	}
	t.Log("unreachable attempt classified, retried on healthy backend: OK")
}

// Scenario: non-idempotent requests are never blindly retried.
func TestNonIdempotentNotRetried(t *testing.T) {
	var hits atomic.Int64
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	t.Cleanup(good.Close)

	cfg := testConfig()
	lb, pool := newGateway(t, cfg)
	pool.Register(mustBackend(t, "dead", deadBackendURL(t)))
	pool.Register(mustBackend(t, "good", good.URL))

	req := httptest.NewRequest(http.MethodPost, "http://gw/submit", strings.NewReader("payload"))
	rec := httptest.NewRecorder()
	lb.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("got %d, want 502 backend_unreachable", rec.Code)
	}
	if got := rec.Header().Get("X-Lb-Error"); got != "backend_unreachable" {
		t.Fatalf("X-Lb-Error = %q, want backend_unreachable", got)
	}
	if hits.Load() != 0 {
		t.Fatalf("POST was retried: good backend hit %d times, want 0", hits.Load())
	}
	t.Log("POST failed once, classified, and was not retried: OK")
}

// Scenario: client cancellation propagates to the downstream backend and
// stops the retry loop immediately.
func TestClientCancelPropagates(t *testing.T) {
	sawCancel := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		close(sawCancel)
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	lb, pool := newGateway(t, cfg)
	pool.Register(mustBackend(t, "slow", srv.URL))
	gw := httptest.NewServer(lb)
	t.Cleanup(gw.Close)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, gw.URL+"/", nil)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := http.DefaultClient.Do(req)
	if err == nil {
		t.Fatal("expected client error after cancel")
	}
	select {
	case <-sawCancel:
		t.Log("backend observed client cancellation: OK")
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not propagate to backend")
	}
}

// Scenario: no healthy backends -> fast, distinguishable failure.
func TestNoHealthyBackendsFastFail(t *testing.T) {
	cfg := testConfig()
	lb, _ := newGateway(t, cfg)

	start := time.Now()
	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("X-Lb-Error"); got != "no_healthy_backends" {
		t.Fatalf("X-Lb-Error = %q, want no_healthy_backends", got)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("fast fail took %s", elapsed)
	}
}

// Scenario: a backend exceeding the per-attempt timeout yields a
// distinguishable 504, not a leaked transport error.
func TestBackendTimeoutClassified(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second)
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	cfg.BackendTimeout = 100 * time.Millisecond
	cfg.MaxRetries = 0
	lb, pool := newGateway(t, cfg)
	pool.Register(mustBackend(t, "slow", srv.URL))

	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("got %d, want 504", rec.Code)
	}
	if got := rec.Header().Get("X-Lb-Error"); got != "backend_timeout" {
		t.Fatalf("X-Lb-Error = %q, want backend_timeout", got)
	}
	if strings.Contains(bodyOf(t, rec), "context deadline") {
		t.Fatal("underlying error leaked to client")
	}
}

// Scenario: the retry budget bounds total request time across attempts.
func TestRetryBudgetBoundsTotalTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	cfg.BackendTimeout = 400 * time.Millisecond
	cfg.RetryBudget = 200 * time.Millisecond
	cfg.MaxRetries = 5
	lb, pool := newGateway(t, cfg)
	pool.Register(mustBackend(t, "s1", srv.URL))
	pool.Register(mustBackend(t, "s2", srv.URL))

	start := time.Now()
	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	elapsed := time.Since(start)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("got %d, want 504 after budget exhaustion", rec.Code)
	}
	if elapsed > 450*time.Millisecond {
		t.Fatalf("retry budget not enforced: took %s", elapsed)
	}
	t.Logf("budget enforced, gave up after %s: OK", elapsed.Round(time.Millisecond))
}
