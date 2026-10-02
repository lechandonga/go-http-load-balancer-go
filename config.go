package gateway

import (
	"fmt"
	"time"
)

// Strategy selects how the pool picks the next backend for a request.
type Strategy int

const (
	// RoundRobin rotates through healthy backends in registration order.
	RoundRobin Strategy = iota
	// LeastConnections picks the healthy backend with the fewest in-flight requests.
	LeastConnections
)

func (s Strategy) String() string {
	if s == LeastConnections {
		return "least_connections"
	}
	return "round_robin"
}

// ParseStrategy converts a CLI/config string into a Strategy.
func ParseStrategy(s string) (Strategy, error) {
	switch s {
	case "round_robin", "roundrobin", "rr":
		return RoundRobin, nil
	case "least_connections", "leastconn", "lc":
		return LeastConnections, nil
	default:
		return RoundRobin, fmt.Errorf("unknown strategy %q", s)
	}
}

// Config tunes the load balancer behavior. All durations are in Go time units.
type Config struct {
	// Strategy is the balancing strategy used by the pool.
	Strategy Strategy
	// FailThreshold is the number of consecutive failures (active probes or
	// real requests) required to mark a backend unhealthy.
	FailThreshold int
	// RecoveryThreshold is the number of consecutive successes required to
	// mark an unhealthy backend healthy again.
	RecoveryThreshold int
	// HealthPath is the path probed by the active health checker.
	HealthPath string
	// HealthInterval is the delay between active probe rounds.
	HealthInterval time.Duration
	// HealthTimeout is the per-probe timeout.
	HealthTimeout time.Duration
	// BackendTimeout is the per-attempt timeout for a single proxied request.
	BackendTimeout time.Duration
	// MaxRetries is the maximum number of retry attempts after the first try.
	// Retries always go to a different healthy backend and only apply to
	// idempotent requests.
	MaxRetries int
	// RetryBudget is the total time budget for one client request including
	// all retries. Zero means no extra budget beyond the client context.
	RetryBudget time.Duration
	// DrainTimeout bounds how long Deregister waits for in-flight requests.
	DrainTimeout time.Duration
}

// DefaultConfig returns a Config with sane local-development defaults.
func DefaultConfig() Config {
	return Config{
		Strategy:          RoundRobin,
		FailThreshold:     3,
		RecoveryThreshold: 2,
		HealthPath:        "/healthz",
		HealthInterval:    2 * time.Second,
		HealthTimeout:     1 * time.Second,
		BackendTimeout:    5 * time.Second,
		MaxRetries:        2,
		RetryBudget:       15 * time.Second,
		DrainTimeout:      30 * time.Second,
	}
}

// Validate reports invalid configuration values.
func (c Config) Validate() error {
	if c.FailThreshold < 1 {
		return fmt.Errorf("FailThreshold must be >= 1, got %d", c.FailThreshold)
	}
	if c.RecoveryThreshold < 1 {
		return fmt.Errorf("RecoveryThreshold must be >= 1, got %d", c.RecoveryThreshold)
	}
	if c.HealthInterval <= 0 {
		return fmt.Errorf("HealthInterval must be > 0, got %s", c.HealthInterval)
	}
	if c.HealthTimeout <= 0 {
		return fmt.Errorf("HealthTimeout must be > 0, got %s", c.HealthTimeout)
	}
	if c.BackendTimeout <= 0 {
		return fmt.Errorf("BackendTimeout must be > 0, got %s", c.BackendTimeout)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("MaxRetries must be >= 0, got %d", c.MaxRetries)
	}
	if c.RetryBudget < 0 {
		return fmt.Errorf("RetryBudget must be >= 0, got %s", c.RetryBudget)
	}
	if c.DrainTimeout <= 0 {
		return fmt.Errorf("DrainTimeout must be > 0, got %s", c.DrainTimeout)
	}
	return nil
}
