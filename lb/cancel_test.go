package lb

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 场景 4：客户端取消必须向下游传播；场景 5：管理 API。
func TestScenario_ClientCancelPropagates(t *testing.T) {
	const scenario = "CLIENT_CANCEL"
	caseStart(t, scenario, "客户端取消后，下游请求 context 被取消，网关不写错误响应")

	downstreamGotCancel := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		downstreamGotCancel <- struct{}{}
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.RequestTimeout = 5 * time.Second
	cfg.TotalBudget = 10 * time.Second
	base, gw := newTestGateway(t, cfg, BackendSpec{ID: "b1", URL: srv.URL})
	defer closeGateway(t, gw)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/slow", nil)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		errCh <- err
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-downstreamGotCancel:
	case <-time.After(2 * time.Second):
		t.Fatal("client cancellation did not propagate to downstream")
	}
	caseOK(t, scenario, "下游收到 context.Canceled", "取消已传播")

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected client-side request error after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client request did not return after cancel")
	}
	caseOK(t, scenario, "客户端侧请求立即结束", "err 非挂起")
}

func TestScenario_AdminAPI(t *testing.T) {
	const scenario = "ADMIN_API"
	caseStart(t, scenario, "运行期经 HTTP 注册、列举、摘除后端")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	base, gw := newTestGateway(t, testConfig())
	defer closeGateway(t, gw)

	// 初始为空：业务请求 503，管理列表为空数组。
	if resp, _ := doGet(t, base, "/"); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("want 503 with empty registry, got %d", resp.StatusCode)
	}

	postResp, err := http.Post(base+"/admin/backends", "application/json",
		strings.NewReader(`{"id":"b1","url":"`+srv.URL+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	postResp.Body.Close()
	if postResp.StatusCode != http.StatusCreated {
		t.Fatalf("register status=%d", postResp.StatusCode)
	}

	listResp, listBody := doGet(t, base, "/admin/backends")
	if listResp.StatusCode != http.StatusOK || !strings.Contains(listBody, `"id":"b1"`) {
		t.Fatalf("list backends failed: %d %s", listResp.StatusCode, listBody)
	}
	caseOK(t, scenario, "注册并列举成功", "backends=%s", listBody)

	if resp, body := doGet(t, base, "/app"); resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("routing after register failed: %d %q", resp.StatusCode, body)
	}
	caseOK(t, scenario, "注册后流量可达", "status=200")

	req, _ := http.NewRequest(http.MethodDelete, base+"/admin/backends/b1", nil)
	delResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("deregister status=%d", delResp.StatusCode)
	}
	if resp, _ := doGet(t, base, "/app"); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("want 503 after deregister, got %d", resp.StatusCode)
	}
	caseOK(t, scenario, "摘除后流量快速失败", "status=503")

	metricsResp, metricsBody := doGet(t, base, "/admin/metrics")
	if metricsResp.StatusCode != 200 || !strings.Contains(metricsBody, "lb_requests_total") {
		t.Fatalf("metrics endpoint bad: %d %s", metricsResp.StatusCode, metricsBody)
	}
	caseOK(t, scenario, "指标可见", "含 lb_requests_total / 失败分类计数")
}
