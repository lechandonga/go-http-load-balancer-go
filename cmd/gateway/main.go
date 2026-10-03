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
	"strconv"
	"strings"
	"syscall"
	"time"

	gateway "github.com/lechandonga/go-http-load-balancer-go"
	"github.com/lechandonga/go-http-load-balancer-go/internal/drill"
)

func main() {
	var (
		addr       = flag.String("addr", ":8080", "listen address")
		backends   = flag.String("backends", "", "comma-separated backend URLs, e.g. http://127.0.0.1:9001,http://127.0.0.1:9002")
		strategy   = flag.String("strategy", "round_robin", "round_robin | least_connections")
		failThresh = flag.Int("fail-threshold", 3, "consecutive failures before marking unhealthy")
		recThresh  = flag.Int("recovery-threshold", 2, "consecutive successes before marking healthy")
		healthPath = flag.String("health-path", "/healthz", "active health check path")
		interval   = flag.Duration("health-interval", 2*time.Second, "active health check interval")
		healthTO   = flag.Duration("health-timeout", 1*time.Second, "per-probe timeout")
		backendTO  = flag.Duration("backend-timeout", 5*time.Second, "per-attempt timeout for a single proxied request")
		maxRetries = flag.Int("max-retries", 2, "retries per request (idempotent only)")
		budget     = flag.Duration("retry-budget", 15*time.Second, "total time budget per client request")
		drainTime  = flag.Duration("drain-timeout", 30*time.Second, "default wait when draining a backend via the admin API")
		adminPath  = flag.String("admin-path", gateway.DefaultAdminPath, "runtime management path prefix; set to empty to disable")
		statusPath = flag.String("status-path", gateway.StatusPath, "JSON status/metrics path; set to empty to disable")
		drillOn    = flag.Bool("drill", false, "drill mode: also start local mock backends")
		drillN     = flag.Int("drill-backends", 3, "drill mode: number of mock backends to start")
		drillPorts = flag.String("drill-ports", "", "drill mode: comma-separated fixed ports for the mock backends, e.g. 19001,19002,19003 (default: random free ports)")
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
	cfg.HealthTimeout = *healthTO
	cfg.BackendTimeout = *backendTO
	cfg.MaxRetries = *maxRetries
	cfg.RetryBudget = *budget
	cfg.DrainTimeout = *drainTime
	if err := gateway.ValidateManagementPath(*adminPath); err != nil {
		logger.Error("invalid admin-path", "error", err)
		os.Exit(2)
	}
	if err := gateway.ValidateManagementPath(*statusPath); err != nil {
		logger.Error("invalid status-path", "error", err)
		os.Exit(2)
	}

	pool := gateway.NewPool(cfg.Strategy, logger)
	for i, raw := range strings.Split(*backends, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		b, err := gateway.NewBackend(fmt.Sprintf("backend-%d", i+1), raw)
		if err != nil {
			logger.Error("invalid backend URL", "url", raw, "error", err)
			os.Exit(2)
		}
		pool.Register(b)
	}

	var cluster *drill.Cluster
	if *drillOn {
		ports, err := parsePorts(*drillPorts)
		if err != nil {
			logger.Error("invalid drill-ports", "error", err)
			os.Exit(2)
		}
		cluster, err = drill.StartCluster(*drillN, ports, "mock-")
		if err != nil {
			logger.Error("drill startup failed", "error", err)
			os.Exit(1)
		}
		for _, target := range cluster.Targets() {
			b, err := gateway.NewBackend(target[0], target[1])
			if err != nil {
				logger.Error("invalid mock backend url", "url", target[1], "error", err)
				os.Exit(2)
			}
			pool.Register(b)
		}
		for _, b := range cluster.Backends {
			logger.Info("mock backend ready",
				"name", b.Name, "url", b.URL,
				"control", b.URL+"/control/state",
				"up", b.URL+"/control/up",
				"down", b.URL+"/control/down",
				"fail5xx", b.URL+"/control/fail5xx?on=1",
				"hang", b.URL+"/control/hang?d=3s")
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
	gateway.MountManagement(mux, *adminPath, *statusPath, pool, cfg, logger)
	mux.Handle("/", lb)

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		logger.Info("gateway listening",
			"addr", *addr, "strategy", strat.String(),
			"admin_path", valueOrDisabled(*adminPath),
			"status_path", valueOrDisabled(*statusPath))
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
	if cluster != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		if err := cluster.Shutdown(shutCtx); err != nil {
			logger.Warn("mock backends shutdown", "error", err)
		}
	}
}

func parsePorts(raw string) ([]int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	ports := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > 65535 {
			return nil, fmt.Errorf("invalid port %q", p)
		}
		ports = append(ports, n)
	}
	return ports, nil
}

func valueOrDisabled(p string) string {
	if p == "" {
		return "disabled"
	}
	return p
}
