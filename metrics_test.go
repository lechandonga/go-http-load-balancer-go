package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// statusJSON fetches the /-/status document from the pool handler.
func statusJSON(t *testing.T, pool *Pool) PoolStatus {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/-/status", nil)
	rec := httptest.NewRecorder()
	pool.StatusHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status got %d", rec.Code)
	}
	var st PoolStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("status not JSON: %v", err)
	}
	return st
}

func findBackend(t *testing.T, st PoolStatus, name string) BackendStatus {
	t.Helper()
	for _, b := range st.Backends {
		if b.Name == name {
			return b
		}
	}
	t.Fatalf("backend %s missing from status: %+v", name, st)
	return BackendStatus{}
}

// Scenario: each forwarding failure class lands in its own counter,
// per backend and in the global total.
func TestMetricsFailureClassification(t *testing.T) {
	// unreachable: nothing listening
	dead := deadBackendURL(t)

	// timeout: hangs beyond BackendTimeout
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second)
	}))
	t.Cleanup(slow.Close)

	// bad response: a complete status line followed by a malformed header
	// line. The transport deterministically rejects the response while
	// reading headers (no response is forwarded), which must classify as
	// bad_response rather than unreachable.
	truncatedLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { truncatedLn.Close() })
	go func() {
		for {
			conn, err := truncatedLn.Accept()
			if err != nil {
				return
			}
			go func() {
				io.WriteString(conn, "HTTP/1.1 200 OK\r\nBADHEADER-NO-COLON\r\n\r\n")
				conn.Close()
			}()
		}
	}()
	truncatedURL := "http://" + truncatedLn.Addr().String()

	// One fresh gateway per case so each backend fails in isolation.
	shot := func(name, url string, wantCode int) *Pool {
		cfg := testConfig()
		cfg.BackendTimeout = 100 * time.Millisecond
		cfg.MaxRetries = 0
		lb, pool := newGateway(t, cfg)
		pool.Register(mustBackend(t, name, url))
		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		if rec.Code != wantCode {
			t.Fatalf("%s: got %d, want %d", name, rec.Code, wantCode)
		}
		return pool
	}

	p1 := shot("dead", dead, http.StatusBadGateway)
	p2 := shot("slow", slow.URL, http.StatusGatewayTimeout)
	p3 := shot("truncated", truncatedURL, http.StatusBadGateway)

	if got := p1.Metrics().Snapshot().FailUnreachable; got != 1 {
		t.Fatalf("unreachable count = %d, want 1", got)
	}
	if got := p2.Metrics().Snapshot().FailTimeout; got != 1 {
		t.Fatalf("timeout count = %d, want 1", got)
	}
	if got := p3.Metrics().Snapshot().FailBadResponse; got != 1 {
		t.Fatalf("bad_response count = %d, want 1", got)
	}

	// All three classes in one gateway for the global-total assertion.
	cfg := testConfig()
	cfg.BackendTimeout = 100 * time.Millisecond
	cfg.MaxRetries = 0
	lb, pool := newGateway(t, cfg)
	pool.Register(mustBackend(t, "dead", dead))
	pool.Register(mustBackend(t, "slow", slow.URL))
	pool.Register(mustBackend(t, "truncated", truncatedURL))
	for i := 0; i < 3; i++ {
		doRequest(t, lb, http.MethodGet, "http://gw/")
	}

	total := pool.Metrics().Snapshot()
	if total.FailUnreachable != 1 || total.FailTimeout != 1 || total.FailBadResponse != 1 {
		t.Fatalf("global classified counters = %+v, want exactly 1 of each", total)
	}
	if total.Failures != 3 {
		t.Fatalf("global failures = %d, want 3", total.Failures)
	}
	t.Logf("unreachable/timeout/bad_response each counted once, globally and per backend: OK")
}

// Scenario: retries are counted as extra attempts only, on the backend
// that received the retry and in the global total.
func TestMetricsRetryCounting(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	}))
	t.Cleanup(good.Close)

	cfg := testConfig()
	lb, pool := newGateway(t, cfg)
	dead := mustBackend(t, "dead", deadBackendURL(t))
	goodB := mustBackend(t, "good", good.URL)
	pool.Register(dead)
	pool.Register(goodB)

	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200 via failover", rec.Code)
	}
	if got := goodB.Counters().Retries; got != 1 {
		t.Fatalf("good backend retries = %d, want 1", got)
	}
	if got := dead.Counters().Retries; got != 0 {
		t.Fatalf("dead backend retries = %d, want 0 (first attempt is not a retry)", got)
	}
	total := pool.Metrics().Snapshot()
	if total.Retries != 1 || total.Requests != 2 {
		t.Fatalf("global retries/requests = %d/%d, want 1/2", total.Retries, total.Requests)
	}
	t.Log("one extra attempt counted as exactly one retry: OK")
}

// Scenario: client cancellation is not a backend failure and not a retry.
func TestMetricsClientCancelNotCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	lb, pool := newGateway(t, cfg)
	b := mustBackend(t, "slow", srv.URL)
	pool.Register(b)
	gw := httptest.NewServer(lb)
	t.Cleanup(gw.Close)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, gw.URL+"/", nil)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, _ = http.DefaultClient.Do(req)
	eventually(t, "request observed", func() bool { return b.Counters().Requests == 1 })

	c := b.Counters()
	if c.Failures != 0 || c.FailUnreachable != 0 || c.FailTimeout != 0 || c.FailBadResponse != 0 {
		t.Fatalf("client cancel counted as backend failure: %+v", c)
	}
	total := pool.Metrics().Snapshot()
	if total.Failures != 0 || total.Retries != 0 {
		t.Fatalf("client cancel leaked into global metrics: %+v", total)
	}
	t.Log("client cancellation left all failure/retry counters at zero: OK")
}

// Scenario: health flips are counted per backend and globally.
func TestMetricsHealthFlips(t *testing.T) {
	var up atomic.Bool
	up.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(srv.Close)

	cfg := testConfig()
	lb, pool := newGateway(t, cfg)
	b := mustBackend(t, "flaky", srv.URL)
	pool.Register(b)
	checker := NewHealthChecker(pool, cfg, testLogger(t))
	checker.Start()
	t.Cleanup(checker.Stop)

	up.Store(false)
	for i := 0; i < cfg.FailThreshold; i++ {
		doRequest(t, lb, http.MethodGet, "http://gw/")
	}
	if b.Healthy() {
		t.Fatal("backend should be unhealthy")
	}
	up.Store(true)
	// Active probes (20ms interval in testConfig) drive recovery.
	eventually(t, "backend recovered", b.Healthy)

	if got := b.Counters().HealthFlips; got != 2 {
		t.Fatalf("health flips = %d, want 2 (down+up)", got)
	}
	if got := pool.Metrics().Snapshot().HealthFlips; got != 2 {
		t.Fatalf("global health flips = %d, want 2", got)
	}
	t.Log("healthy->unhealthy->healthy counted as two flips: OK")
}

// Scenario: the /-/status document is structured, stable, and the global
// total matches the sum of what the gateway actually did under concurrency.
func TestMetricsConsistencyUnderConcurrency(t *testing.T) {
	var okHits atomic.Int64
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		okHits.Add(1)
	}))
	t.Cleanup(good.Close)

	cfg := testConfig()
	cfg.MaxRetries = 2
	lb, pool := newGateway(t, cfg)
	dead := mustBackend(t, "dead", deadBackendURL(t))
	goodB := mustBackend(t, "good", good.URL)
	pool.Register(dead)
	pool.Register(goodB)

	const workers, perWorker = 16, 25
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				rec := doRequest(t, lb, http.MethodGet, "http://gw/")
				if rec.Code != http.StatusOK {
					t.Errorf("got %d, want 200 (failover to good)", rec.Code)
				}
			}
		}()
	}
	wg.Wait()

	st := statusJSON(t, pool)
	deadSt := findBackend(t, st, "dead")
	goodSt := findBackend(t, st, "good")

	const totalRequests = int64(workers * perWorker)
	// Every request: 1 attempt on dead (counted once it becomes unhealthy
	// only healthy ones are picked... dead may go unhealthy) — instead of
	// assuming distribution, assert internal consistency.
	sumRequests := deadSt.Requests + goodSt.Requests
	if st.Total.Requests != sumRequests {
		t.Fatalf("total requests %d != per-backend sum %d", st.Total.Requests, sumRequests)
	}
	if st.Total.Retries != deadSt.Retries+goodSt.Retries {
		t.Fatalf("total retries %d != per-backend sum", st.Total.Retries)
	}
	if st.Total.FailUnreachable != deadSt.FailUnreachable+goodSt.FailUnreachable {
		t.Fatalf("total unreachable %d != per-backend sum", st.Total.FailUnreachable)
	}
	if st.Total.Requests < totalRequests {
		t.Fatalf("lost attempts under concurrency: total=%d, clients sent=%d",
			st.Total.Requests, totalRequests)
	}
	if goodSt.Requests != okHits.Load() {
		t.Fatalf("good backend counter %d != actual hits %d", goodSt.Requests, okHits.Load())
	}
	t.Logf("after %d concurrent requests: totals match per-backend sums and actual hits: OK",
		totalRequests)
}
