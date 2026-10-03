// Command gateway runs the HTTP load balancer locally.
//
// Example:
//
//	go run ./cmd/gateway -addr :8080 -backends http://127.0.0.1:9001,http://127.0.0.1:9002
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gateway "github.com/lechandonga/go-http-load-balancer-go"
)

func main() {
	var (
		addr         = flag.String("addr", ":8080", "listen address")
		backends     = flag.String("backends", "", "comma-separated backend URLs, e.g. http://127.0.0.1:9001,http://127.0.0.1:9002")
		strategy     = flag.String("strategy", "round_robin", "round_robin | least_connections")
		failThresh   = flag.Int("fail-threshold", 3, "consecutive failures before marking unhealthy")
		recThresh    = flag.Int("recovery-threshold", 2, "consecutive successes before marking healthy")
		healthPath   = flag.String("health-path", "/healthz", "active health check path")
		interval     = flag.Duration("health-interval", 2*time.Second, "active health check interval")
		maxRetries   = flag.Int("max-retries", 2, "retries per request (idempotent only)")
		budget       = flag.Duration("retry-budget", 15*time.Second, "total time budget per client request")
		drainTimeout = flag.Duration("drain-timeout", 30*time.Second, "max wait for in-flight requests during deregistration")
		adminOn      = flag.Bool("admin", true, "enable the runtime admin API (never proxied to backends)")
		adminPath    = flag.String("admin-path", "/-/admin", "mount path of the admin API")
		drill        = flag.Bool("drill", false, "drill mode: also start local mock backends and register them")
		drillN       = flag.Int("drill-backends", 3, "number of mock backends in drill mode")
		drillPort    = flag.Int("drill-port-base", 19001, "first mock backend port in drill mode")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	strat, err := gateway.ParseStrategy(*strategy)
	if err != nil {
		logger.Error("invalid strategy", "error", err)
		os.Exit(2)
	}
	cfg := gateway.DefaultConfig()
	cfg.Strategy = strat
	cfg.FailThreshold = *failThresh
	cfg.RecoveryThreshold = *recThresh
	cfg.HealthPath = *healthPath
	cfg.HealthInterval = *interval
	cfg.MaxRetries = *maxRetries
	cfg.RetryBudget = *budget
	cfg.DrainTimeout = *drainTimeout

	pool := gateway.NewPool(cfg.Strategy, logger)
	register := func(name, raw string) {
		b, err := gateway.NewBackend(name, raw)
		if err != nil {
			logger.Error("invalid backend URL", "url", raw, "error", err)
			os.Exit(2)
		}
		if err := pool.Register(b); err != nil {
			logger.Error("backend registration failed", "url", raw, "error", err)
			os.Exit(2)
		}
	}
	for i, raw := range strings.Split(*backends, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		register(fmt.Sprintf("backend-%d", i+1), raw)
	}

	var mocks []*gateway.MockBackend
	if *drill {
		for i := 0; i < *drillN; i++ {
			name := fmt.Sprintf("drill-%d", i+1)
			m, err := gateway.NewMockBackend(name, fmt.Sprintf("127.0.0.1:%d", *drillPort+i))
			if err != nil {
				logger.Error("mock backend failed", "name", name, "error", err)
				os.Exit(2)
			}
			mocks = append(mocks, m)
			register(name, m.URL())
			logger.Info("drill mock backend up",
				"name", name, "url", m.URL(),
				"control", m.URL()+"/-/drill")
		}
	}

	lb, err := gateway.NewLoadBalancer(pool, cfg, logger)
	if err != nil {
		logger.Error("invalid config", "error", err)
		os.Exit(2)
	}
	checker := gateway.NewHealthChecker(pool, cfg, logger)
	checker.Start()
	defer checker.Stop()

	mux := http.NewServeMux()
	mux.Handle("/-/status", pool.StatusHandler())
	if *adminOn {
		prefix := strings.TrimSuffix(*adminPath, "/")
		admin := gateway.NewAdminHandler(pool, cfg, logger)
		mux.Handle(prefix+"/", http.StripPrefix(prefix, admin))
		logger.Info("admin API enabled", "path", prefix)
	}
	mux.Handle("/", lb)

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		logger.Info("gateway listening", "addr", *addr, "strategy", strat.String())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	for _, m := range mocks {
		m.Close()
	}
}
