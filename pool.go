package gateway

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Pool is the set of registered backends. Registration and deregistration
// are safe to call concurrently with request routing.
type Pool struct {
	mu       sync.RWMutex
	backends []*Backend
	rr       atomic.Uint64
	strategy Strategy
	logger   *slog.Logger
}

// NewPool creates a Pool using the given strategy.
func NewPool(strategy Strategy, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{strategy: strategy, logger: logger}
}

// Register adds a backend so it becomes routable immediately.
func (p *Pool) Register(b *Backend) {
	p.mu.Lock()
	p.backends = append(p.backends, b)
	p.mu.Unlock()
	p.logger.Info("backend registered", "backend", b.Name, "url", b.URL.String())
}

// Deregister stops routing new requests to b, then waits for in-flight
// requests to finish (bounded by ctx). The backend never receives new
// traffic once Deregister has been entered.
func (p *Pool) Deregister(ctx context.Context, b *Backend) error {
	b.startDrain()

	p.mu.Lock()
	for i, cur := range p.backends {
		if cur == b {
			p.backends = append(p.backends[:i], p.backends[i+1:]...)
			break
		}
	}
	p.mu.Unlock()
	p.logger.Info("backend deregistering, draining in-flight requests",
		"backend", b.Name, "inflight", b.InFlight())

	done := make(chan struct{})
	go func() {
		b.waitDrain()
		close(done)
	}()
	select {
	case <-done:
		p.logger.Info("backend drained and removed", "backend", b.Name)
		return nil
	case <-ctx.Done():
		return ErrDrainTimeout
	}
}

// Snapshot returns a copy of the current backend list.
func (p *Pool) Snapshot() []*Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Backend, len(p.backends))
	copy(out, p.backends)
	return out
}

// Next picks a healthy, non-draining backend not in excluded and reserves an
// in-flight slot on it. The caller must call Release when done. Returns nil
// when nothing is routable.
func (p *Pool) Next(excluded map[*Backend]struct{}) *Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := len(p.backends)
	if n == 0 {
		return nil
	}
	switch p.strategy {
	case LeastConnections:
		return p.nextLeastConn(excluded)
	default:
		return p.nextRoundRobin(excluded, n)
	}
}

func (p *Pool) nextRoundRobin(excluded map[*Backend]struct{}, n int) *Backend {
	start := p.rr.Add(1) - 1
	for i := 0; i < n; i++ {
		b := p.backends[(int(start)+i)%n]
		if _, skip := excluded[b]; skip {
			continue
		}
		if b.Acquire() {
			return b
		}
	}
	return nil
}

func (p *Pool) nextLeastConn(excluded map[*Backend]struct{}) *Backend {
	var best *Backend
	var bestConns int64
	for _, b := range p.backends {
		if _, skip := excluded[b]; skip {
			continue
		}
		if !b.Healthy() || b.Draining() {
			continue
		}
		c := b.InFlight()
		if best == nil || c < bestConns {
			best, bestConns = b, c
		}
	}
	if best != nil && best.Acquire() {
		return best
	}
	return nil
}
