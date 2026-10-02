package lb

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 本文件按“场景 -> 用例”组织，配合 `go test -v` 即可从日志看出
// 覆盖了哪些场景、每个场景有哪些用例及其 PASS/FAIL 结果。

func caseStart(t *testing.T, scenario, name string) {
	t.Helper()
	t.Logf("[SCENARIO %s] CASE ==> %s", scenario, name)
}

func caseOK(t *testing.T, scenario, name, format string, args ...any) {
	t.Helper()
	t.Logf("[SCENARIO %s] CASE <== PASS %s: %s", scenario, name, fmt.Sprintf(format, args...))
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Health.ProbeInterval = 50 * time.Millisecond
	cfg.Health.ProbeTimeout = 200 * time.Millisecond
	cfg.Health.FailureThreshold = 3
	cfg.Health.SuccessThreshold = 2
	cfg.RequestTimeout = time.Second
	cfg.TotalBudget = 5 * time.Second
	cfg.MaxAttempts = 3
	cfg.DrainTimeout = 5 * time.Second
	return cfg
}

// newTestGateway 在随机端口启动完整网关，返回基础地址与实例。
func newTestGateway(t *testing.T, cfg Config, specs ...BackendSpec) (string, *Gateway) {
	t.Helper()
	cfg.Backends = specs
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listen = ln.Addr().String()
	gw := New(cfg, quietLogger())
	go func() { _ = gw.server.Serve(ln) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if conn, derr := net.Dial("tcp", cfg.Listen); derr == nil {
			conn.Close()
			return "http://" + cfg.Listen, gw
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("gateway did not start")
	return "", nil
}

func closeGateway(t *testing.T, gw *Gateway) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = gw.Shutdown(ctx)
}

func doGet(t *testing.T, base, path string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(data)
}

// slowBackend 记录命中数，首个请求阻塞直到 release 关闭。
func slowBackend(t *testing.T, id string) (*httptest.Server, *atomic.Int64, chan struct{}) {
	t.Helper()
	var hits atomic.Int64
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" { // "/" 是主动健康探测，不计入业务请求
			hits.Add(1)
		}
		// 只有业务请求 /slow 阻塞；健康探测打在 ProbePath "/" 上，必须立即返回。
		if r.URL.Path == "/slow" {
			<-release
		}
		w.Header().Set("X-Backend", id)
		_, _ = fmt.Fprintf(w, `{"backend":%q}`, id)
	}))
	return srv, &hits, release
}

// 场景 1：运行期摘除时的在途请求
func TestScenario_DeregisterDrainInflight(t *testing.T) {
	const scenario = "DEREGISTER_DRAIN"
	cfg := testConfig()
	srv, hits, release := slowBackend(t, "b1")
	defer srv.Close()
	base, gw := newTestGateway(t, cfg, BackendSpec{ID: "b1", URL: srv.URL})
	defer closeGateway(t, gw)

	caseStart(t, scenario, "摘除期间在途请求正常收尾，不丢请求")

	type result struct {
		status int
		body   string
	}
	inflightDone := make(chan result, 1)
	go func() {
		resp, body := doGet(t, base, "/slow")
		inflightDone <- result{resp.StatusCode, body}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for hits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if hits.Load() == 0 {
		t.Fatal("request never reached backend")
	}

	deregDone := make(chan error, 1)
	go func() { deregDone <- gw.Registry().Deregister("b1") }()

	// 摘除必须阻塞到在途收尾为止。
	select {
	case err := <-deregDone:
		t.Fatalf("Deregister returned before inflight drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	caseOK(t, scenario, "摘除阻塞等待在途", "Deregister 未提前返回")

	// 摘除进行中：新请求快速失败，且不能到达正在排空的实例。
	resp2, body2 := doGet(t, base, "/new")
	if resp2.StatusCode != http.StatusServiceUnavailable {
		close(release)
		t.Fatalf("want 503 during drain, got %d body=%s", resp2.StatusCode, body2)
	}
	if hits.Load() != 1 {
		close(release)
		t.Fatalf("draining backend received %d requests, want 1", hits.Load())
	}
	caseOK(t, scenario, "摘除中不接收新请求", "503 fast-fail, backend hits=1")

	close(release)
	select {
	case err := <-deregDone:
		if err != nil {
			t.Fatalf("Deregister: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Deregister did not return after inflight finished")
	}
	select {
	case res := <-inflightDone:
		if res.status != http.StatusOK || !strings.Contains(res.body, `"b1"`) {
			t.Fatalf("inflight response lost: status=%d body=%s", res.status, res.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inflight client never got response")
	}
	caseOK(t, scenario, "在途请求收尾成功", "status=200, body 含 b1")

	caseStart(t, scenario, "彻底移除后请求绝不到达旧实例")
	_, body := doGet(t, base, "/after")
	if !strings.Contains(body, "no healthy backend") {
		t.Fatalf("want no-healthy error, got %s", body)
	}
	if hits.Load() != 1 {
		t.Fatalf("removed backend got extra hits: %d", hits.Load())
	}
	caseOK(t, scenario, "移除后零路由", "hits 仍为 1")
}

// 并发摘除 + 并发请求：验证没有数据竞争（配合 -race）且不丢已接受请求之外的保证。
func TestScenario_ConcurrentDeregisterRace(t *testing.T) {
	const scenario = "CONCURRENT_DEREGISTER_RACE"
	caseStart(t, scenario, "并发注册/摘除/请求下不 panic、无竞态、无请求落到已摘除实例")

	r := NewRegistry(RoundRobin, time.Second)
	srvs := make([]*httptest.Server, 4)
	for i := range srvs {
		id := fmt.Sprintf("b%d", i)
		srvs[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("X-Backend", id)
			_, _ = io.WriteString(w, id)
		}))
		r.Register(id, srvs[i].URL)
	}
	defer func() {
		for _, s := range srvs {
			s.Close()
		}
	}()

	var churnWG, pickerWG sync.WaitGroup
	stop := make(chan struct{})

	// 不断摘/挂两个实例。
	for _, idx := range []int{0, 1} {
		churnWG.Add(1)
		go func(idx int) {
			defer churnWG.Done()
			id := fmt.Sprintf("b%d", idx)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = r.Deregister(id)
				r.Register(id, srvs[idx].URL)
				time.Sleep(time.Millisecond)
			}
		}(idx)
	}

	// 不断 pick/acquire/release，统计被派发实例是否当前在册且健康。
	var acquired atomic.Int64
	for w := 0; w < 4; w++ {
		pickerWG.Add(1)
		go func() {
			defer pickerWG.Done()
			for i := 0; i < 500; i++ {
				b, draining, alive := r.pickHealthyChecked(nil)
				if b == nil {
					continue
				}
				acquired.Add(1)
				// 占位瞬间（注册表锁内）必然健康且未摘除。
				if draining || !alive {
					t.Errorf("request acquired on draining=%v alive=%v backend %s",
						draining, alive, b.id)
				}
				b.release()
			}
		}()
	}

	// picker 是有界循环，先跑完；再停止 churn 并等待收尾。
	pickerWG.Wait()
	close(stop)
	churnWG.Wait()
	if acquired.Load() == 0 {
		t.Fatal("no acquire succeeded during concurrent churn")
	}
	caseOK(t, scenario, "全部派发目标在派发时刻均健康且未摘除",
		"acquired=%d", acquired.Load())
}
