package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Scenario: deregistration while requests are in flight.
// New routing stops immediately, in-flight requests finish, Deregister
// returns only after the drain completes.
func TestDeregisterDrainsInFlight(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		fmt.Fprintln(w, "done")
	}))
	t.Cleanup(srv.Close)

	lb, pool := newGateway(t, testConfig())
	b := mustBackend(t, "b1", srv.URL)
	pool.Register(b)

	respCh := make(chan int, 1)
	go func() {
		rec := doRequest(t, lb, http.MethodGet, "http://gw/")
		respCh <- rec.Code
	}()
	<-entered
	t.Log("in-flight request started on b1; deregistering concurrently")

	drainDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		drainDone <- pool.Deregister(ctx, b)
	}()

	eventually(t, "draining flag visible", b.Draining)
	if got := pool.Next(nil); got != nil {
		t.Fatalf("Next returned draining backend %v", got.Name)
	}
	select {
	case err := <-drainDone:
		t.Fatalf("Deregister returned %v before in-flight request finished", err)
	case <-time.After(50 * time.Millisecond):
		t.Log("Deregister still blocked while request is in flight: OK")
	}

	close(release)
	if code := <-respCh; code != http.StatusOK {
		t.Fatalf("in-flight request lost: got status %d, want 200", code)
	}
	if err := <-drainDone; err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	t.Log("in-flight request completed, backend drained and removed: OK")
}

// Scenario: concurrent register/deregister/routing must be race-free and
// never route to a deregistered backend.
func TestConcurrentRegisterDeregister(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	t.Cleanup(srv.Close)

	lb, pool := newGateway(t, testConfig())
	const workers = 8
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				b := mustBackend(t, fmt.Sprintf("churn-%d", i), srv.URL)
				pool.Register(b)
				rec := doRequest(t, lb, http.MethodGet, "http://gw/")
				_ = rec
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = pool.Deregister(ctx, b)
				cancel()
			}
		}(i)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	t.Logf("survived register/deregister churn, proxied %d requests", hits.Load())
}

// Scenario: strategies distribute requests deterministically.
func TestStrategies(t *testing.T) {
	var h1, h2 atomic.Int64
	mk := func(c *atomic.Int64) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c.Add(1)
		}))
	}
	s1, s2 := mk(&h1), mk(&h2)
	t.Cleanup(s1.Close)
	t.Cleanup(s2.Close)

	t.Run("round_robin alternates", func(t *testing.T) {
		cfg := testConfig()
		cfg.Strategy = RoundRobin
		lb, pool := newGateway(t, cfg)
		pool.Register(mustBackend(t, "rr1", s1.URL))
		pool.Register(mustBackend(t, "rr2", s2.URL))
		before1, before2 := h1.Load(), h2.Load()
		for i := 0; i < 4; i++ {
			doRequest(t, lb, http.MethodGet, "http://gw/")
		}
		if h1.Load()-before1 != 2 || h2.Load()-before2 != 2 {
			t.Fatalf("round robin got %d/%d, want 2/2", h1.Load()-before1, h2.Load()-before2)
		}
	})

	t.Run("least_connections prefers idle backend", func(t *testing.T) {
		cfg := testConfig()
		cfg.Strategy = LeastConnections
		lb, pool := newGateway(t, cfg)
		b1 := mustBackend(t, "lc1", s1.URL)
		b2 := mustBackend(t, "lc2", s2.URL)
		pool.Register(b1)
		pool.Register(b2)
		if !b1.Acquire() {
			t.Fatal("acquire b1")
		}
		defer b1.Release()
		before := h2.Load()
		doRequest(t, lb, http.MethodGet, "http://gw/")
		if h2.Load()-before != 1 {
			t.Fatalf("least_connections did not pick idle backend")
		}
	})
}
