// Command lbgateway 是仅依赖 Go 标准库的 HTTP 反向代理 / 负载均衡网关。
//
// 直接启动（默认 :8080，无初始后端，经管理 API 动态注册）：
//
//	go run ./cmd/lbgateway
//
// 使用配置文件：
//
//	go run ./cmd/lbgateway -config configs/example.json
//
// 一键本地故障演练（自动拉起 3 个内存后端）：
//
//	go run ./cmd/lbgateway -demo
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lechandonga/go-http-load-balancer-go/lb"
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file (defaults are used when omitted)")
	listen := flag.String("listen", "", "override listen address, e.g. :8080")
	demo := flag.Bool("demo", false, "start 3 in-process demo backends for local fault drills")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := lb.DefaultConfig()
	if *configPath != "" {
		var err error
		cfg, err = lb.LoadConfig(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load config: %v\n", err)
			os.Exit(1)
		}
	}

	var demoServers []*httptest.Server
	if *demo {
		cfg.Backends, demoServers = startDemoBackends(log)
		printDemoGuide(cfg.Listen)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	defer func() {
		for _, s := range demoServers {
			s.Close()
		}
	}()

	gw := lb.New(cfg, log)
	errCh := make(chan error, 1)
	go func() {
		log.Info("gateway listening", "addr", cfg.Listen, "backends", len(cfg.Backends))
		errCh <- gw.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "server: %v\n", err)
			os.Exit(1)
		}
	case sig := <-sigCh:
		log.Info("shutting down", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := gw.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "shutdown: %v\n", err)
			os.Exit(1)
		}
	}
}

// startDemoBackends 拉起 3 个进程内 HTTP 后端。
// 每个后端支持演练开关：
//
//	GET  /                 -> 200 并回显实例 ID
//	POST /control/fail     -> 后续请求返回 500
//	POST /control/ok       -> 恢复正常
//	POST /control/hang     -> 后续请求挂起 10s（演练超时 / 客户端取消）
//	POST /control/reset    -> 清除演练状态
func startDemoBackends(log *slog.Logger) ([]lb.BackendSpec, []*httptest.Server) {
	specs := make([]lb.BackendSpec, 0, 3)
	servers := make([]*httptest.Server, 0, 3)
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("demo-%d", i)
		srv := httptest.NewServer(newDemoBackend(id, log))
		servers = append(servers, srv)
		specs = append(specs, lb.BackendSpec{ID: id, URL: srv.URL})
		log.Info("demo backend started", "id", id, "url", srv.URL)
	}
	return specs, servers
}

func newDemoBackend(id string, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	var mu struct {
		sync.Mutex
		fail bool
		hang bool
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Info("demo backend hit", "backend", id, "path", r.URL.Path,
			"method", r.Method, "remote", r.Header.Get("X-Forwarded-For"))
		mu.Lock()
		fail, hang := mu.fail, mu.hang
		mu.Unlock()
		if hang {
			select {
			case <-r.Context().Done():
				log.Info("demo backend request canceled by gateway", "backend", id)
			case <-time.After(10 * time.Second):
			}
			return
		}
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprintf(w, `{"backend":%q,"error":"injected"}`, id)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"backend":%q,"path":%q}`, id, r.URL.Path)
	})
	mux.HandleFunc("/control/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		switch r.URL.Path {
		case "/control/fail":
			mu.Lock()
			mu.fail = true
			mu.Unlock()
		case "/control/ok":
			mu.Lock()
			mu.fail = false
			mu.hang = false
			mu.Unlock()
		case "/control/hang":
			mu.Lock()
			mu.hang = true
			mu.Unlock()
		case "/control/reset":
			mu.Lock()
			mu.fail = false
			mu.hang = false
			mu.Unlock()
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		log.Info("demo backend control", "backend", id, "state", r.URL.Path)
		_, _ = fmt.Fprintf(w, `{"backend":%q,"state":%q}`, id, r.URL.Path)
	})
	return mux
}

func printDemoGuide(addr string) {
	base := "http://localhost" + addr
	fmt.Fprintf(os.Stderr, `
=== demo fault drill guide ===
# 观察轮询分发（X-Forwarded-For 已被网关注入）
curl -s %[1]s/foo; curl -s %[1]s/bar; curl -s %[1]s/baz

# 查看实例健康状态与指标
curl -s %[1]s/admin/backends | python3 -m json.tool
curl -s %[1]s/admin/metrics

# 让 demo-2 连续返回 500（经真实请求达到失败阈值 3 次后被摘除）
# demo-2 的端口在启动日志 "demo backend started id=demo-2 url=..." 中查看：
for i in 1 2 3; do curl -s -X POST http://127.0.0.1:PORT/control/fail; done

# 让某实例挂起（演练网关超时 / 客户端取消）：
curl -s -X POST http://127.0.0.1:PORT/control/hang

# 优雅摘除 demo-3（先停新请求，等待在途请求收尾）
curl -s -X DELETE %[1]s/admin/backends/demo-3

# 重新注册
curl -s -X POST %[1]s/admin/backends -d '{"id":"demo-3","url":"http://127.0.0.1:PORT"}'
=============================
`, base)
}
