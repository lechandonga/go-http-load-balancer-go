package lb

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// 场景 3：重试与失败分类
func TestScenario_RetryAndFailureKinds(t *testing.T) {
	const scenario = "RETRY"

	t.Run("idempotent_request_switches_to_other_backend", func(t *testing.T) {
		caseStart(t, scenario, "首个实例返回 500 时，GET 换到其他健康实例")
		cfg := testConfig()
		var deadHits, liveHits atomic.Int64
		dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				deadHits.Add(1)
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "boom")
		}))
		live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				liveHits.Add(1)
			}
			_, _ = io.WriteString(w, `{"backend":"live"}`)
		}))
		defer dead.Close()
		defer live.Close()

		base, gw := newTestGateway(t, cfg,
			BackendSpec{ID: "a-500", URL: dead.URL},
			BackendSpec{ID: "b-live", URL: live.URL},
		)
		defer closeGateway(t, gw)

		resp, body := doGet(t, base, "/r")
		if resp.StatusCode != 200 || !strings.Contains(body, "live") {
			t.Fatalf("not retried onto live backend: status=%d body=%s", resp.StatusCode, body)
		}
		if deadHits.Load() == 0 || liveHits.Load() == 0 {
			t.Fatalf("want hits on both backends, dead=%d live=%d", deadHits.Load(), liveHits.Load())
		}
		caseOK(t, scenario, "GET 重试换到存活实例", "dead_hits=%d live_hits=%d",
			deadHits.Load(), liveHits.Load())
	})

	t.Run("post_is_never_retried", func(t *testing.T) {
		caseStart(t, scenario, "非幂等 POST 即使 500 也只打一次，不重放")
		cfg := testConfig()
		var hits atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				hits.Add(1)
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "boom")
		}))
		defer srv.Close()
		base, gw := newTestGateway(t, cfg, BackendSpec{ID: "b1", URL: srv.URL})
		defer closeGateway(t, gw)

		resp, err := http.Post(base+"/pay", "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError || string(data) != "boom" {
			t.Fatalf("POST response not passed through: status=%d body=%q", resp.StatusCode, data)
		}
		if hits.Load() != 1 {
			t.Fatalf("non-idempotent request delivered %d times", hits.Load())
		}
		caseOK(t, scenario, "POST 原样透传、仅一次", "hits=%d status=500", hits.Load())
	})

	t.Run("unreachable_and_timeout_are_distinguishable", func(t *testing.T) {
		caseStart(t, scenario, "连接拒绝=502；超时=504，且错误不透出底层原文")

		// 不可达：监听一个端口后立刻关闭，得到稳定的 ECONNREFUSED。
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		deadURL := "http://" + ln.Addr().String()
		_ = ln.Close()

		cfg := testConfig()
		cfg.MaxAttempts = 1
		base, gw := newTestGateway(t, cfg, BackendSpec{ID: "dead", URL: deadURL})
		defer closeGateway(t, gw)

		resp, body := doGet(t, base, "/x")
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("unreachable: want 502, got %d", resp.StatusCode)
		}
		if strings.Contains(strings.ToLower(body), "connect") ||
			strings.Contains(strings.ToLower(body), "refused") {
			t.Fatalf("underlying error leaked to client: %s", body)
		}
		caseOK(t, scenario, "不可达映射为 502", "body=%s", body)
		closeGateway(t, gw)

		// 超时：后端挂住，网关单次尝试超时短。
		hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done() // 网关取消后立即退出，同时验证取消向下游传播
		}))
		defer hang.Close()
		cfg2 := testConfig()
		cfg2.MaxAttempts = 1
		cfg2.RequestTimeout = 100 * time.Millisecond
		cfg2.TotalBudget = 2 * time.Second
		base2, gw2 := newTestGateway(t, cfg2, BackendSpec{ID: "hang", URL: hang.URL})
		defer closeGateway(t, gw2)

		start := time.Now()
		resp2, body2 := doGet(t, base2, "/slow")
		elapsed := time.Since(start)
		if resp2.StatusCode != http.StatusGatewayTimeout {
			t.Fatalf("timeout: want 504, got %d body=%s", resp2.StatusCode, body2)
		}
		if elapsed > time.Second {
			t.Fatalf("timeout request hung for %v", elapsed)
		}
		caseOK(t, scenario, "超时映射为 504 且快速返回", "elapsed=%v body=%s",
			elapsed.Round(10*time.Millisecond), body2)
	})

	t.Run("all_unhealthy_fast_fail", func(t *testing.T) {
		caseStart(t, scenario, "所有后端不健康时立即 503，不挂起")
		cfg := testConfig()
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		deadURL := "http://" + ln.Addr().String()
		_ = ln.Close()
		base, gw := newTestGateway(t, cfg,
			BackendSpec{ID: "d1", URL: deadURL},
			BackendSpec{ID: "d2", URL: deadURL},
		)
		defer closeGateway(t, gw)

		// 等健康探测把两个实例都摘掉。
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			snaps := gw.Registry().Snapshots()
			alive := 0
			for _, s := range snaps {
				if s.Alive {
					alive++
				}
			}
			if alive == 0 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		start := time.Now()
		resp, body := doGet(t, base, "/")
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("want 503, got %d", resp.StatusCode)
		}
		if time.Since(start) > 200*time.Millisecond {
			t.Fatal("fast fail took too long")
		}
		caseOK(t, scenario, "全不可用 503 快速失败", "body=%s", body)
	})
}
