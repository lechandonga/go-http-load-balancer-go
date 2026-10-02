package gateway

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testWriter routes gateway logs into the test log so `go test -v` shows
// state transitions next to the scenario that caused them.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("[gateway] %s", string(p))
	return len(p), nil
}

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.FailThreshold = 3
	cfg.RecoveryThreshold = 2
	cfg.HealthInterval = 20 * time.Millisecond
	cfg.HealthTimeout = 50 * time.Millisecond
	cfg.BackendTimeout = 300 * time.Millisecond
	cfg.MaxRetries = 2
	cfg.RetryBudget = 2 * time.Second
	cfg.DrainTimeout = 2 * time.Second
	return cfg
}

func mustBackend(t *testing.T, name, rawURL string) *Backend {
	t.Helper()
	b, err := NewBackend(name, rawURL)
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}
	return b
}

func newGateway(t *testing.T, cfg Config) (*LoadBalancer, *Pool) {
	t.Helper()
	pool := NewPool(cfg.Strategy, testLogger(t))
	lb, err := NewLoadBalancer(pool, cfg, testLogger(t))
	if err != nil {
		t.Fatalf("NewLoadBalancer: %v", err)
	}
	t.Cleanup(lb.CloseIdleConnections)
	return lb, pool
}

// eventually polls cond until it holds or the deadline passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within timeout: %s", what)
}

func doRequest(t *testing.T, lb *LoadBalancer, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	lb.ServeHTTP(rec, req)
	return rec
}

func bodyOf(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	b, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}
