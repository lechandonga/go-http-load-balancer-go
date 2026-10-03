package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func backendMetrics(t *testing.T, b *Backend) BackendCounters {
	t.Helper()
	return b.Counters()
}

// Scenario: each failure class lands in its own per-backend and global
// counter; client cancellation counts nowhere.
func TestMetricsFailureClasses(t *testing.T) {
	t.Run("unreachable counted on backend and total", func(t *testing.T) {
		cfg := testConfig()
		cfg.MaxRetries = 0
		lb, pool := newGateway(t, cfg)
		b := mustBackend(t, "dead", deadBackendURL(t))
		pool.Register(b)

		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("got %d, want 502", rec.Code)
		}
		c := backendMetrics(t, b)
		if c.Unreachable != 1 || c.Failures != 1 || c.Requests != 1 {
			t.Fatalf("backend counters = %+v, want 1 unreachable/1 request", c)
		}
		g := pool.GlobalMetrics()
		if g.Unreachable != 1 || g.Failures != 1 || g.Requests != 1 {
			t.Fatalf("global counters = %+v", g)
		}
		t.Log("unreachable: per-backend unreachable=1, total failures=1: OK")
	})

	t.Run("timeout counted distinctly", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(time.Second)
		}))
		t.Cleanup(srv.Close)
		cfg := testConfig()
		cfg.BackendTimeout = 80 * time.Millisecond
		cfg.MaxRetries = 0
		lb, pool := newGateway(t, cfg)
		b := mustBackend(t, "slow", srv.URL)
		pool.Register(b)

		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		if rec.Code != http.StatusGatewayTimeout {
			t.Fatalf("got %d, want 504", rec.Code)
		}
		c := backendMetrics(t, b)
		if c.Timeout != 1 || c.Unreachable != 0 || c.Failures != 1 {
			t.Fatalf("backend counters = %+v, want timeout=1", c)
		}
		t.Log("timeout: per-backend timeout=1, unreachable=0: OK")
	})

	t.Run("5xx response counted separately and still forwarded", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
		}))
		t.Cleanup(srv.Close)
		cfg := testConfig()
		cfg.FailThreshold = 100 // keep it routable across the two requests
		lb, pool := newGateway(t, cfg)
		b := mustBackend(t, "five", srv.URL)
		pool.Register(b)

		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("5xx not forwarded: got %d", rec.Code)
		}
		c := backendMetrics(t, b)
		if c.Response5xx != 1 || c.Unreachable != 0 || c.Timeout != 0 || c.Failures != 1 {
			t.Fatalf("backend counters = %+v, want response_5xx=1", c)
		}
		if pool.GlobalMetrics().Response5xx != 1 {
			t.Fatal("global response_5xx != 1")
		}
		t.Log("5xx: forwarded 502 and counted response_5xx=1: OK")
	})
}

// Scenario: retries are counted as extra attempts only, on the backend that
// received each extra attempt; the first attempt is not a retry.
func TestMetricsRetries(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(good.Close)

	cfg := testConfig()
	cfg.MaxRetries = 2
	lb, pool := newGateway(t, cfg)
	dead := mustBackend(t, "dead", deadBackendURL(t))
	alive := mustBackend(t, "alive", good.URL)
	pool.Register(dead)
	pool.Register(alive)

	rec := doRequest(t, lb, http.MethodGet, "http://gw/")
	if rec.Code != http.StatusOK {
		t.Fatalf("failover request: got %d", rec.Code)
	}

	deadC := backendMetrics(t, dead)
	aliveC := backendMetrics(t, alive)
	if deadC.Requests != 1 || deadC.Retries != 0 {
		t.Fatalf("dead counters = %+v, want requests=1 retries=0", deadC)
	}
	if aliveC.Requests != 1 || aliveC.Retries != 1 {
		t.Fatalf("alive counters = %+v, want requests=1 retries=1", aliveC)
	}
	g := pool.GlobalMetrics()
	if g.Requests != 2 || g.Retries != 1 || g.Failures != 1 {
		t.Fatalf("global = %+v, want requests=2 retries=1 failures=1", g)
	}
	t.Log("retry: first attempt requests=1/retries=0, failover attempt retries=1: OK")
}

// Scenario: client cancellation is not a backend failure and not a request.
func TestMetricsClientCancelExcluded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	cfg := testConfig()
	cfg.MaxRetries = 0
	lb, pool := newGateway(t, cfg)
	b := mustBackend(t, "slow", srv.URL)
	pool.Register(b)
	gw := httptest.NewServer(lb)
	t.Cleanup(gw.Close)

	client := &http.Client{Timeout: 100 * time.Millisecond}
	_, _ = client.Get(gw.URL + "/")

	eventually(t, "client request finished", func() bool { return true })
	time.Sleep(50 * time.Millisecond)
	c := backendMetrics(t, b)
	if c.Requests != 1 {
		t.Fatalf("requests = %d, want 1 (attempt happened)", c.Requests)
	}
	if c.Failures != 0 || c.Timeout != 0 || c.Unreachable != 0 {
		t.Fatalf("client cancel counted as backend failure: %+v", c)
	}
	if g := pool.GlobalMetrics(); g.Failures != 0 {
		t.Fatalf("global failures = %d, want 0 after client cancel", g.Failures)
	}
	t.Log("client cancel: requests=1, all failure classes 0: OK")
}

// Scenario: health flips are counted per backend and globally.
func TestMetricsHealthFlips(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	cfg := testConfig()
	lb, pool := newGateway(t, cfg)
	b := mustBackend(t, "flap", srv.URL)
	pool.Register(b)

	for i := 0; i < cfg.FailThreshold; i++ {
		doRequest(t, lb, http.MethodGet, "http://gw/")
	}
	c := backendMetrics(t, b)
	if c.FlipsToUnhealthy != 1 || c.HealthFlips != 1 || c.FlipsToHealthy != 0 {
		t.Fatalf("flip counters = %+v", c)
	}
	if g := pool.GlobalMetrics(); g.FlipsToUnhealthy != 1 || g.HealthFlips != 1 {
		t.Fatalf("global flips = %+v", g)
	}
	t.Log("health flip to unhealthy counted once per backend and total: OK")
}

// Scenario: management requests are absent from business metrics, and the
// status document exposes the stable field set as JSON.
func TestStatusDocumentAndAdminExcluded(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(good.Close)
	cfg := testConfig()
	pool := NewPool(RoundRobin, testLogger(t))
	lb, err := NewLoadBalancer(pool, cfg, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	b := mustBackend(t, "b", good.URL)
	if err := pool.RegisterBackend(b); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	MountManagement(mux, "/-/admin/", "/-/status", pool, cfg, testLogger(t))
	mux.Handle("/", lb)
	gw := httptest.NewServer(mux)
	t.Cleanup(gw.Close)

	doRequest(t, lb, http.MethodGet, "http://gw/") // one business request

	resp, err := http.Get(gw.URL + "/-/status")
	if err != nil {
		t.Fatal(err)
	}
	var doc StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if len(doc.Backends) != 1 {
		t.Fatalf("backends = %d, want 1", len(doc.Backends))
	}
	bs := doc.Backends[0]
	if bs.Name != "b" || bs.Metrics.Requests != 1 || bs.Metrics.Failures != 0 {
		t.Fatalf("backend status = %+v", bs)
	}
	for _, want := range []string{"requests", "failures", "unreachable", "timeout",
		"bad_response", "response_5xx", "retries", "health_flips",
		"flips_to_healthy", "flips_to_unhealthy"} {
		var raw map[string]json.RawMessage
		blob, _ := json.Marshal(bs.Metrics)
		_ = json.Unmarshal(blob, &raw)
		if _, ok := raw[want]; !ok {
			t.Fatalf("metric field %q missing from JSON", want)
		}
	}

	// Pound the management endpoints; business counters must not move.
	for i := 0; i < 10; i++ {
		resp, err := http.Get(gw.URL + "/-/status")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		req, _ := http.NewRequest(http.MethodGet, gw.URL+"/-/admin/backends", nil)
		r2, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = r2.Body.Close()
	}
	if g := pool.GlobalMetrics(); g.Requests != 1 {
		t.Fatalf("management traffic leaked into business metrics: %+v", g)
	}
	t.Log("status JSON fields stable; 20 management requests excluded: OK")
}

// Scenario: concurrent business pressure never loses or corrupts counters.
func TestMetricsConcurrentConsistency(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(good.Close)
	cfg := testConfig()
	lb, pool := newGateway(t, cfg)
	pool.Register(mustBackend(t, "a", good.URL))
	pool.Register(mustBackend(t, "b", good.URL))

	const workers = 8
	const each = 100
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				doRequest(t, lb, http.MethodGet, "http://gw/")
			}
		}()
	}
	wg.Wait()

	var sum int64
	for _, b := range pool.Snapshot() {
		c := b.Counters()
		sum += c.Requests
	}
	if got := pool.GlobalMetrics().Requests; got != workers*each {
		t.Fatalf("global requests = %d, want %d (per-backend sum %d)",
			got, workers*each, sum)
	}
	if sum != workers*each {
		t.Fatalf("per-backend sum = %d, want %d", sum, workers*each)
	}
	t.Logf("concurrent counters exact: per-backend sum %d == total %d: OK",
		sum, pool.GlobalMetrics().Requests)
}
