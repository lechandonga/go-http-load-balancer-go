package lb

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 场景 2：健康抖动 —— 单次抖动不得反复摘除/恢复。
func TestScenario_HealthDebounce(t *testing.T) {
	const scenario = "HEALTH_DEBOUNCE"
	cfg := testConfig().Health

	t.Run("single_failures_do_not_flip_until_threshold", func(t *testing.T) {
		caseStart(t, scenario, "真实请求失败：未达阈值 3 不摘除")
		// 直接构造 Backend，避免运行中的健康监控器并发观测污染连击计数。
		b := newBackend("b1", "http://127.0.0.1:1")

		b.observe(false, false, cfg, time.Now())
		b.observe(false, false, cfg, time.Now())
		if !b.Alive() {
			t.Fatal("flipped unhealthy before threshold")
		}
		caseOK(t, scenario, "前两次失败保持健康", "alive=true")

		b.observe(false, false, cfg, time.Now())
		if b.Alive() {
			t.Fatal("should be unhealthy at threshold")
		}
		caseOK(t, scenario, "连续第三次失败摘除", "alive=false")
	})

	t.Run("single_probe_success_does_not_recover_and_jitter_resets", func(t *testing.T) {
		caseStart(t, scenario, "探测恢复：单次成功不恢复，中间失败清零连击（阈值 2）")
		b := newBackend("b1", "http://127.0.0.1:1")
		// 先置为不健康（模拟已被摘除）。
		for i := 0; i < 3; i++ {
			b.observe(false, true, cfg, time.Now())
		}

		b.observe(true, true, cfg, time.Now())
		if b.Alive() {
			t.Fatal("recovered after 1 probe success")
		}
		caseOK(t, scenario, "首次探测成功仍不健康", "alive=false")

		// 恢复需要连续 2 次成功：先来 1 次成功（仍不健康），
		// 紧接着 1 次失败抖动必须把恢复连击清零。
		b.observe(false, true, cfg, time.Now())
		b.observe(true, true, cfg, time.Now())
		if b.Alive() {
			t.Fatal("recovered despite interleaved failure jitter")
		}
		caseOK(t, scenario, "失败把恢复连击清零，单次成功不恢复", "alive=false")

		// 清零后再来一次成功（连同上面那次）构成连续两次，必须恢复。
		b.observe(true, true, cfg, time.Now())
		if !b.Alive() {
			t.Fatal("should recover after 2 consecutive successes")
		}
		caseOK(t, scenario, "清零后连续两次探测成功才恢复", "alive=true，抖动不横跳")
	})

	t.Run("active_probes_take_dead_backend_down_e2e", func(t *testing.T) {
		caseStart(t, scenario, "端到端：主动探测连续失败摘除死实例，全部不可用时 503")
		gwCfg := testConfig()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "ok")
		}))
		base, gw := newTestGateway(t, gwCfg, BackendSpec{ID: "b1", URL: srv.URL})
		defer closeGateway(t, gw)

		if resp, _ := doGet(t, base, "/"); resp.StatusCode != 200 {
			t.Fatal("initial request failed")
		}
		srv.Close()

		var snap Snapshot
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			snap = gw.Registry().Snapshots()[0]
			if !snap.Alive {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if snap.Alive {
			t.Fatal("dead backend stayed healthy")
		}
		if snap.UnhealthyStreak < gwCfg.Health.FailureThreshold {
			t.Fatalf("streak %d < threshold", snap.UnhealthyStreak)
		}
		caseOK(t, scenario, "探测连续失败达阈值后摘除",
			"unhealthy_streak=%d", snap.UnhealthyStreak)

		resp, body := doGet(t, base, "/x")
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("want 503, got %d", resp.StatusCode)
		}
		if !strings.Contains(body, "no healthy backend") {
			t.Fatalf("unexpected body: %s", body)
		}
		caseOK(t, scenario, "摘除后无健康实例快速失败", "503 + 可区分错误体")
	})
}
