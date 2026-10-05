package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Scenario (5xx injection): while a backend keeps returning 500 it must
// stay unhealthy and receive no traffic across many active health check
// rounds — probes must not resurrect it. After the injection is canceled
// it comes back via the normal recovery threshold.
func TestInjected5xxStaysEvictedAcrossProbeRounds(t *testing.T) {
	cfg := testConfig()
	cfg.HealthInterval = 20 * time.Millisecond
	lb, pool := newGateway(t, cfg)

	bad, err := NewMockBackend("bad", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bad.Close)
	good, err := NewMockBackend("good", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(good.Close)
	pool.Register(mustBackend(t, "bad", bad.URL()))
	pool.Register(mustBackend(t, "good", good.URL()))

	checker := NewHealthChecker(pool, cfg, testLogger(t))
	checker.Start()
	t.Cleanup(checker.Stop)

	drillControl(t, bad, "/-/drill/error")
	for i := 0; i < 6; i++ {
		getBody(t, lb)
	}
	badB := pool.GetByName("bad")
	eventually(t, "bad marked unhealthy", func() bool { return !badB.Healthy() })
	flipsAtEviction := badB.Counters().HealthFlips
	if flipsAtEviction != 1 {
		t.Fatalf("health flips at eviction = %d, want 1", flipsAtEviction)
	}

	probeRounds := 20
	time.Sleep(time.Duration(probeRounds) * cfg.HealthInterval)
	if badB.Healthy() {
		t.Fatal("5xx-injected backend was resurrected by active probes")
	}
	if got := badB.Counters().HealthFlips; got != flipsAtEviction {
		t.Fatalf("health flipped %d->%d while injection still active", flipsAtEviction, got)
	}
	for i := 0; i < 12; i++ {
		code, body := getBody(t, lb)
		if code != http.StatusOK || !strings.Contains(body, "good") {
			t.Fatalf("request %d reached the still-5xx backend: %d %q", i, code, body)
		}
	}
	t.Logf("across %d+ active probe rounds the 5xx backend stayed evicted and took zero traffic: OK", probeRounds)

	drillControl(t, bad, "/-/drill/error?enabled=false")
	eventually(t, "bad recovered after injection canceled", badB.Healthy)
	saw := map[string]bool{}
	for i := 0; i < 6; i++ {
		_, body := getBody(t, lb)
		saw[strings.TrimSpace(body)] = true
	}
	if !containsName(saw, "bad") {
		t.Fatalf("backend never took traffic after recovery: %v", saw)
	}
	t.Log("after canceling the injection the backend recovered via the success threshold: OK")
}

func containsName(m map[string]bool, name string) bool {
	for k := range m {
		if strings.Contains(k, name) {
			return true
		}
	}
	return false
}

// Scenario (metrics reconciliation): under concurrent pressure, with
// client cancellations and admin traffic interleaved, the global total
// equals the sum over every backend for requests, failures, every
// failure class, retries and flips; per-backend failures equal their own
// category sums; and counters match the attempts that actually happened.
func TestMetricsReconciliationUnderConcurrency(t *testing.T) {
	var goodHits, flakyHits atomic.Int64
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(good.Close)
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flakyHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(flaky.Close)
	cancelOnly := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(cancelOnly.Close)

	cfg := testConfig()
	cfg.MaxRetries = 1
	lb, pool := newGateway(t, cfg)
	admin := NewAdminHandler(pool, cfg, testLogger(t))
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/-/admin/") {
			http.StripPrefix("/-/admin", admin).ServeHTTP(w, r)
			return
		}
		lb.ServeHTTP(w, r)
	}))
	t.Cleanup(gw.Close)

	pool.Register(mustBackend(t, "good", good.URL))
	pool.Register(mustBackend(t, "flaky", flaky.URL))
	// A dedicated single-backend gateway serves canceled requests so a
	// canceled attempt can never be routed at the business backends.
	cancelLb, cancelPool := newGateway(t, cfg)
	cancelPool.Register(mustBackend(t, "slow-only", cancelOnly.URL))
	cancelGw := httptest.NewServer(cancelLb)
	t.Cleanup(cancelGw.Close)

	const workers, perWorker = 12, 30
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				resp, err := http.Get(gw.URL + "/")
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}()
	}
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				ctx, cancel := context.WithCancel(context.Background())
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, cancelGw.URL+"/", nil)
				go func() {
					time.Sleep(5 * time.Millisecond)
					cancel()
				}()
				if resp, err := http.DefaultClient.Do(req); err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 20; j++ {
			resp, err := http.Get(gw.URL + "/-/admin/backends")
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	}()
	wg.Wait()

	st := statusJSON(t, pool)
	var sum BackendCounters
	for _, b := range st.Backends {
		sum.Requests += b.Requests
		sum.Failures += b.Failures
		sum.FailUnreachable += b.FailUnreachable
		sum.FailTimeout += b.FailTimeout
		sum.FailBadResponse += b.FailBadResponse
		sum.Retries += b.Retries
		sum.HealthFlips += b.HealthFlips
		if b.Failures != b.FailUnreachable+b.FailTimeout+b.FailBadResponse {
			t.Fatalf("backend %s failures %d != category sum (%d+%d+%d)",
				b.Name, b.Failures, b.FailUnreachable, b.FailTimeout, b.FailBadResponse)
		}
	}
	tot := st.Total
	if tot != sum {
		t.Fatalf("global total %+v != per-backend sum %+v", tot, sum)
	}

	goodSt := findBackend(t, st, "good")
	flakySt := findBackend(t, st, "flaky")
	if goodSt.Requests != goodHits.Load() {
		t.Fatalf("good requests %d != actual hits %d", goodSt.Requests, goodHits.Load())
	}
	if flakySt.Requests != flakyHits.Load() {
		t.Fatalf("flaky requests %d != actual hits %d", flakySt.Requests, flakyHits.Load())
	}
	if flakySt.FailBadResponse != flakyHits.Load() || flakySt.Failures != flakyHits.Load() {
		t.Fatalf("flaky failure accounting %+v != hits %d", flakySt, flakyHits.Load())
	}

	cancelSt := statusJSON(t, cancelPool)
	slowSt := findBackend(t, cancelSt, "slow-only")
	if slowSt.Failures != 0 || slowSt.FailUnreachable+slowSt.FailTimeout+slowSt.FailBadResponse != 0 {
		t.Fatalf("client cancel leaked into slow backend counters: %+v", slowSt)
	}
	if cancelSt.Total.Failures != 0 || cancelSt.Total.Requests != slowSt.Requests {
		t.Fatalf("cancel gateway totals inconsistent: total=%+v backend=%+v", cancelSt.Total, slowSt)
	}
	if tot.FailUnreachable != 0 {
		t.Fatalf("unexpected unreachable failures: %d", tot.FailUnreachable)
	}
	if tot.Requests < int64(workers*perWorker) {
		t.Fatalf("lost attempts: total=%d clients=%d", tot.Requests, workers*perWorker)
	}
	t.Logf("reconciled under concurrency: requests=%d failures=%d (bad=%d) retries=%d flips=%d; cancels/admins untracked: OK",
		tot.Requests, tot.Failures, tot.FailBadResponse, tot.Retries, tot.HealthFlips)
}

// Scenario (metrics reconciliation): with only the active checker
// running against an offline backend and zero business traffic, probe
// failures still land in a stable category and global totals reconcile.
func TestActiveProbeFailuresReconcileWithoutTraffic(t *testing.T) {
	cfg := testConfig()
	cfg.HealthInterval = 20 * time.Millisecond
	_, pool := newGateway(t, cfg)
	m, err := NewMockBackend("down", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	pool.Register(mustBackend(t, "down", m.URL()))

	checker := NewHealthChecker(pool, cfg, testLogger(t))
	checker.Start()
	t.Cleanup(checker.Stop)

	drillControl(t, m, "/-/drill/down")
	b := pool.GetByName("down")
	eventually(t, "down marked unhealthy by probes", func() bool { return !b.Healthy() })
	time.Sleep(5 * cfg.HealthInterval)

	st := statusJSON(t, pool)
	down := findBackend(t, st, "down")
	if down.Requests != 0 {
		t.Fatalf("no business traffic expected, requests=%d", down.Requests)
	}
	if down.Failures == 0 {
		t.Fatal("probe failures were not counted")
	}
	if down.Failures != down.FailUnreachable+down.FailTimeout+down.FailBadResponse {
		t.Fatalf("probe failures %d have no stable category sum %+v", down.Failures, down)
	}
	if down.FailBadResponse != down.Failures {
		t.Fatalf("offline probes expected all bad_response, got %+v", down)
	}
	if st.Total.Failures != down.Failures || st.Total.FailBadResponse != down.FailBadResponse {
		t.Fatalf("global total %+v != probe backend %+v", st.Total, down)
	}
	t.Logf("probe-only failures reconcile: %d failures all in fail_bad_response, zero business requests: OK",
		down.Failures)
}
