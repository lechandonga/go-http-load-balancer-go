package lb

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// Gateway 组合注册表、健康探测、代理与管理接口，并统一启停。
type Gateway struct {
	cfg      Config
	log      *slog.Logger
	registry *Registry
	metrics  *Metrics
	monitor  *healthMonitor
	server   *http.Server
}

// New 按 cfg 构造网关（含初始后端注册，但不启动监听）。
func New(cfg Config, log *slog.Logger) *Gateway {
	cfg.Health = cfg.Health.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	registry := NewRegistry(cfg.Strategy, cfg.DrainTimeout)
	for _, spec := range cfg.Backends {
		registry.Register(spec.ID, spec.URL)
	}
	metrics := newMetrics()

	mux := http.NewServeMux()
	proxy := NewProxy(registry, cfg, metrics, log)
	admin := &adminHandler{registry: registry, metrics: metrics, prefix: cfg.AdminPathPrefix}
	admin.register(mux)
	mux.HandleFunc("/", proxy.ServeHTTP)

	monitor := newHealthMonitor(registry, cfg.Health, log)
	registry.setTransitionHook(func(id string, healthy bool) {
		if healthy {
			log.Info("health transition", "backend", id, "to", "healthy")
		} else {
			log.Warn("health transition", "backend", id, "to", "unhealthy")
		}
	})

	g := &Gateway{
		cfg:      cfg,
		log:      log,
		registry: registry,
		metrics:  metrics,
		monitor:  monitor,
		server: &http.Server{
			Addr:              cfg.Listen,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
	monitor.start()
	return g
}

// ListenAndServe 启动健康探测与 HTTP 监听，阻塞直到服务器停止。
func (g *Gateway) ListenAndServe() error {
	err := g.server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Registry 暴露注册表，供测试与嵌入场景直接操作。
func (g *Gateway) Registry() *Registry { return g.registry }

// Shutdown 优雅停止：先停健康探测与监听（拒绝新连接），
// 由在途请求的 Server 关闭流程自行收尾。
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.monitor.stop()
	return g.server.Shutdown(ctx)
}
