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
	draining map[*Backend]*drainHandle
	removed  BackendCounters
	rr       atomic.Uint64
	strategy Strategy
	logger   *slog.Logger
	metrics  *Metrics
}

// drainHandle tracks one in-progress drain so concurrent Deregister calls
// for the same backend join the same wait instead of draining twice.
type drainHandle struct {
	done chan struct{}
	err  error
}

// NewPool creates a Pool using the given strategy.
func NewPool(strategy Strategy, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{
		strategy: strategy,
		logger:   logger,
		draining: make(map[*Backend]*drainHandle),
		metrics:  &Metrics{},
	}
}

// Metrics returns the gateway-wide cumulative counters for this pool.
func (p *Pool) Metrics() *Metrics {
	return p.metrics
}

// Register adds a backend so it becomes routable immediately. It returns
// ErrBackendExists if a backend with the same name or URL is registered
// or still draining: until a drain fully completes, the name and URL stay
// reserved so no duplicate instance can appear in the pool.
func (p *Pool) Register(b *Backend) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, cur := range p.backends {
		if cur.Name == b.Name || cur.URL.String() == b.URL.String() {
			return ErrBackendExists
		}
	}
	for cur := range p.draining {
		if cur.Name == b.Name || cur.URL.String() == b.URL.String() {
			return ErrBackendExists
		}
	}
	p.backends = append(p.backends, b)
	p.logger.Info("backend registered", "backend", b.Name, "url", b.URL.String())
	return nil
}

// GetByName returns the registered (or currently draining) backend with
// the given name, or nil.
func (p *Pool) GetByName(name string) *Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, b := range p.backends {
		if b.Name == name {
			return b
		}
	}
	for b := range p.draining {
		if b.Name == name {
			return b
		}
	}
	return nil
}

// Deregister stops routing new requests to b, then waits for in-flight
// requests to finish (bounded by ctx). The backend never receives new
// traffic once Deregister has been entered.
//
// Concurrent Deregister calls for the same backend join the same drain and
// observe the same result. Deregistering a backend that is not registered
// returns ErrBackendNotFound. If the wait times out, the backend stays
// removed from routing and the drain completes in the background; a later
// Deregister joins that wait.
func (p *Pool) Deregister(ctx context.Context, b *Backend) error {
	p.mu.Lock()
	if h, ok := p.draining[b]; ok {
		// A drain is already in progress: join it for a consistent result.
		p.mu.Unlock()
		select {
		case <-h.done:
			return h.err
		case <-ctx.Done():
			return ErrDrainTimeout
		}
	}
	found := false
	for i, cur := range p.backends {
		if cur == b {
			p.backends = append(p.backends[:i], p.backends[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		p.mu.Unlock()
		return ErrBackendNotFound
	}
	h := &drainHandle{done: make(chan struct{})}
	p.draining[b] = h
	p.mu.Unlock()

	b.startDrain()
	p.logger.Info("backend deregistering, draining in-flight requests",
		"backend", b.Name, "inflight", b.InFlight())

	done := make(chan struct{})
	go func() {
		b.waitDrain()
		close(done)
	}()
	select {
	case <-done:
		p.finishDrain(b, h, nil)
		p.logger.Info("backend drained and removed", "backend", b.Name)
		return nil
	case <-ctx.Done():
		// Routing has already stopped; the drain finishes in the
		// background so late joiners still observe the final result.
		go func() {
			<-done
			p.finishDrain(b, h, nil)
			p.logger.Info("backend drained and removed", "backend", b.Name)
		}()
		return ErrDrainTimeout
	}
}

// finishDrain publishes the drain result to any joined waiters and folds
// the backend's final counters into the removed aggregate, so the global
// total always equals the sum of registered backends plus removed ones.
func (p *Pool) finishDrain(b *Backend, h *drainHandle, err error) {
	p.mu.Lock()
	delete(p.draining, b)
	p.removed.add(b.Counters())
	h.err = err
	close(h.done)
	p.mu.Unlock()
}

// removedCounters returns the cumulative counters of fully drained
// backends.
func (p *Pool) removedCounters() BackendCounters {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.removed
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
