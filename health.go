package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// recordOutcome feeds one observation into the backend state machine.
// Failed observations (ok == false) must carry failErr, the classified
// failure sentinel. The per-backend aggregate and category counters are
// bumped atomically inside the backend's state-machine critical section;
// the global totals are bumped here at the same call, so per-backend and
// global counters can never drift apart.
func recordOutcome(logger *slog.Logger, metrics *Metrics, source string, b *Backend, ok bool, failErr error, cfg Config) {
	if !ok && failErr != nil {
		if metrics != nil {
			metrics.noteFailure(failErr)
		}
	}
	changed, healthy := b.recordOutcome(ok, failErr, cfg.FailThreshold, cfg.RecoveryThreshold)
	if changed {
		if metrics != nil {
			metrics.noteHealthFlip()
		}
		logger.Info("backend health state changed",
			"backend", b.Name,
			"healthy", healthy,
			"source", source,
		)
	}
}

// HealthChecker actively probes all registered backends on an interval.
// Probe results share the same consecutive-threshold state machine as
// passive (real request) results, so concurrent probing and deregistration
// cannot corrupt state: every mutation goes through Backend.mu.
type HealthChecker struct {
	pool   *Pool
	cfg    Config
	logger *slog.Logger
	client *http.Client

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewHealthChecker builds a checker over the pool.
func NewHealthChecker(pool *Pool, cfg Config, logger *slog.Logger) *HealthChecker {
	if logger == nil {
		logger = slog.Default()
	}
	return &HealthChecker{
		pool:   pool,
		cfg:    cfg,
		logger: logger,
		client: &http.Client{Timeout: cfg.HealthTimeout},
		stopCh: make(chan struct{}),
	}
}

// Start launches the probing loop in the background.
func (hc *HealthChecker) Start() {
	hc.wg.Add(1)
	go hc.loop()
}

// Stop terminates the probing loop and waits for in-flight probes.
func (hc *HealthChecker) Stop() {
	close(hc.stopCh)
	hc.wg.Wait()
}

func (hc *HealthChecker) loop() {
	defer hc.wg.Done()
	ticker := time.NewTicker(hc.cfg.HealthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-hc.stopCh:
			return
		case <-ticker.C:
			hc.probeAll()
		}
	}
}

// probeAll probes a snapshot of the pool concurrently. Backends removed
// mid-round simply get one last harmless probe.
func (hc *HealthChecker) probeAll() {
	backends := hc.pool.Snapshot()
	var wg sync.WaitGroup
	for _, b := range backends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hc.probe(b)
		}()
	}
	wg.Wait()
}

// probe performs a single active health check against one backend.
func (hc *HealthChecker) probe(b *Backend) {
	u := *b.URL
	u.Path = hc.cfg.HealthPath
	ctx, cancel := context.WithTimeout(context.Background(), hc.cfg.HealthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		recordOutcome(hc.logger, hc.pool.metrics, "active", b, false, ErrBackendUnreachable, hc.cfg)
		return
	}
	resp, err := hc.client.Do(req)
	if err != nil {
		recordOutcome(hc.logger, hc.pool.metrics, "active", b, false, classifyProbeError(err), hc.cfg)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 500 {
		recordOutcome(hc.logger, hc.pool.metrics, "active", b, true, nil, hc.cfg)
	} else {
		recordOutcome(hc.logger, hc.pool.metrics, "active", b, false, ErrBadResponse, hc.cfg)
	}
}

// classifyProbeError maps a probe transport error to the shared failure
// taxonomy: a failed probe is a real failure observation and must land in
// a stable, scriptable category (unreachable / timeout), never in limbo.
func classifyProbeError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ErrBackendTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrBackendTimeout
	}
	return ErrBackendUnreachable
}
