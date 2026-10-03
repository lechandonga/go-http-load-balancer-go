package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gateway "github.com/lechandonga/go-http-load-balancer-go"
	"github.com/lechandonga/go-http-load-balancer-go/internal/drill"
)

// startDrillGateway launches the mock cluster plus a gateway wired exactly
// like cmd/gateway does in -drill mode.
func startDrillGateway(t *testing.T, count int, cfg gateway.Config) (string, *drill.Cluster, func()) {
	t.Helper()
	cluster, err := drill.StartCluster(count, nil, "mock-")
	if err != nil {
		t.Fatal(err)
	}
	pool := gateway.NewPool(cfg.Strategy, nil)
	for _, target := range cluster.Targets() {
		b, err := gateway.NewBackend(target[0], target[1])
		if err != nil {
			t.Fatal(err)
		}
		pool.Register(b)
	}
	lb, err := gateway.NewLoadBalancer(pool, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	checker := gateway.NewHealthChecker(pool, cfg, nil)
	checker.Start()
	mux := http.NewServeMux()
	gateway.MountManagement(mux, "/-/admin/", "/-/status", pool, cfg, nil)
	mux.Handle("/", lb)
	gw := httptest.NewServer(mux)

	cleanup := func() {
		gw.Close()
		checker.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = cluster.Shutdown(ctx)
	}
	return gw.URL, cluster, cleanup
}

func drillCfg() gateway.Config {
	cfg := gateway.DefaultConfig()
	cfg.FailThreshold = 2
	cfg.RecoveryThreshold = 2
	// Probe cadence is deliberately slower than request-driven observation so
	// the 5xx passive-ejection scenario reaches its threshold first.
	cfg.HealthInterval = 500 * time.Millisecond
	cfg.HealthTimeout = 200 * time.Millisecond
	cfg.BackendTimeout = 300 * time.Millisecond
	cfg.MaxRetries = 0 // deterministic per-instance assertions
	cfg.DrainTimeout = 3 * time.Second
	return cfg
}

func control(t *testing.T, b *drill.Backend, action string) {
	t.Helper()
	resp, err := http.Post(b.URL+action, "application/json", nil)
	if err != nil {
		t.Fatalf("control %s: %v", action, err)
	}
	_ = resp.Body.Close()
}

func gwGet(t *testing.T, base string) (int, string) {
	t.Helper()
	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatalf("gateway get: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func statusTotal(t *testing.T, base string) gateway.GlobalCounters {
	t.Helper()
	resp, err := http.Get(base + "/-/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var doc struct {
		Total gateway.GlobalCounters `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	return doc.Total
}

// Full drill chain through one running gateway.
func TestDrillEndToEnd(t *testing.T) {
	cfg := drillCfg()
	base, cluster, cleanup := startDrillGateway(t, 3, cfg)
	defer cleanup()

	// 1. Round robin reaches every instance.
	seen := map[string]bool{}
	for i := 0; i < 6; i++ {
		status, body := gwGet(t, base)
		if status != http.StatusOK {
			t.Fatalf("baseline request %d: %d %s", i, status, body)
		}
		for _, b := range cluster.Backends {
			if body == fmt.Sprintf("hello from %s (path=/, n=", b.Name) ||
				strings.Contains(body, b.Name) {
				seen[b.Name] = true
			}
		}
	}
	if len(seen) != 3 {
		t.Fatalf("round robin only reached %v", seen)
	}
	t.Logf("round robin distributed to %d instances: OK", len(seen))

	// 2. Inject 5xx on instance 1; passive health removes it and traffic
	// bypasses it.
	victim := cluster.Backends[0]
	control(t, victim, "/control/fail5xx?on=1")
	// Drive traffic until the victim has accumulated the passive threshold
	// and been ejected; a single lucky 200 before that must not count.
	waitFor(t, 4*time.Second, func() bool {
		status, body := gwGet(t, base)
		tot := statusTotal(t, base)
		return tot.Response5xx >= int64(cfg.FailThreshold) &&
			status == http.StatusOK && !strings.Contains(body, victim.Name)
	})
	for i := 0; i < 6; i++ {
		status, body := gwGet(t, base)
		if status != http.StatusOK || strings.Contains(body, victim.Name) {
			t.Fatalf("traffic not bypassed after 5xx threshold: %d %s", status, body)
		}
	}
	tot := statusTotal(t, base)
	if tot.Response5xx < 2 {
		t.Fatalf("5xx injections not counted: %+v", tot)
	}
	t.Logf("5xx threshold ejected %s, traffic bypasses it, response_5xx=%d: OK",
		victim.Name, tot.Response5xx)

	// 3. A hanging instance produces a distinct 504.
	control(t, victim, "/control/fail5xx?on=0")
	hanger := cluster.Backends[1]
	control(t, hanger, "/control/hang?d=3s")
	waitFor(t, 5*time.Second, func() bool {
		status, _ := gwGet(t, base)
		return status == http.StatusGatewayTimeout
	})
	tot = statusTotal(t, base)
	if tot.Timeout < 1 {
		t.Fatalf("timeout not counted: %+v", tot)
	}
	control(t, hanger, "/control/hang?d=0")
	waitFor(t, 3*time.Second, func() bool {
		status, _ := gwGet(t, base)
		return status == http.StatusOK
	})
	t.Log("hang -> 504 backend_timeout, timeout counter incremented: OK")

	// 4. All instances offline -> fast, distinguishable 503.
	for _, b := range cluster.Backends {
		control(t, b, "/control/down")
	}
	waitFor(t, 3*time.Second, func() bool {
		resp, err := http.Get(base + "/")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusServiceUnavailable &&
			resp.Header.Get("X-Lb-Error") == "no_healthy_backends"
	})
	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable ||
		resp.Header.Get("X-Lb-Error") != "no_healthy_backends" {
		t.Fatalf("all down: got %d (%s), want 503 no_healthy_backends",
			resp.StatusCode, resp.Header.Get("X-Lb-Error"))
	}
	t.Log("all down -> 503 no_healthy_backends: OK")

	// 5. Deregister while a slow request is in flight: it finishes, then the
	// instance never gets new traffic.
	for _, b := range cluster.Backends {
		control(t, b, "/control/up")
		control(t, b, "/control/hang?d=0")
		control(t, b, "/control/fail5xx?on=0")
	}
	target := cluster.Backends[2]
	// Wait for every instance to recover health first (no injections set).
	waitFor(t, 4*time.Second, func() bool {
		st, _ := gwGet(t, base)
		return st == http.StatusOK
	})

	// Now make only the target slow: its business requests take 250ms while
	// health probes answer immediately, so it stays healthy and routable.
	control(t, target, "/control/hang?d=250ms")

	// Fire requests until one is actually in flight on the target.
	waitFor(t, 3*time.Second, func() bool {
		go func() { _, _ = http.Get(base + "/") }()
		time.Sleep(30 * time.Millisecond)
		return target.InFlight() > 0
	})

	inflight := make(chan int, 4)
	fire := func() {
		go func() {
			r, err := http.Get(base + "/")
			if err != nil {
				inflight <- 0
				return
			}
			_ = r.Body.Close()
			inflight <- r.StatusCode
		}()
	}
	fire()
	fire()

	// Target is confirmed in flight; DELETE must wait for it and return 200.
	drainStart := time.Now()
	drReq, _ := http.NewRequest(http.MethodDelete,
		base+"/-/admin/backends/"+target.Name+"?timeout=3s", nil)
	dr, err := http.DefaultClient.Do(drReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = dr.Body.Close()
	if dr.StatusCode != http.StatusOK {
		t.Fatalf("deregister during slow request: got %d, want 200", dr.StatusCode)
	}
	if elapsed := time.Since(drainStart); elapsed < 200*time.Millisecond {
		t.Fatalf("drain returned in %s, before the 250ms slow request finished", elapsed)
	}
	t.Logf("drain blocked %s for the in-flight slow request then returned 200: OK",
		time.Since(drainStart).Round(10*time.Millisecond))
	// At least one in-flight request must have completed successfully.
	okSeen := false
	for i := 0; i < 2; i++ {
		if code := <-inflight; code == http.StatusOK {
			okSeen = true
		}
	}
	if !okSeen {
		t.Fatal("in-flight slow request was lost (no 200 observed)")
	}
	control(t, target, "/control/hang?d=0")
	// Bring other two back; target stays deregistered.
	names := map[string]bool{}
	for _, b := range cluster.Backends {
		if b != target {
			names[b.Name] = true
		}
	}
	waitFor(t, 3*time.Second, func() bool {
		st, body := gwGet(t, base)
		if st == http.StatusOK {
			for name := range names {
				if strings.Contains(body, name) {
					delete(names, name)
				}
			}
		}
		return len(names) == 0
	})
	for i := 0; i < 6; i++ {
		st, body := gwGet(t, base)
		if st != http.StatusOK || strings.Contains(body, target.Name) {
			t.Fatalf("deregistered instance still served: %d %s", st, body)
		}
	}
	if len(names) != 0 {
		t.Fatalf("surviving instances never served again: %v", names)
	}
	t.Log("deregister during slow request: request finished, instance never routed again: OK")
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}
