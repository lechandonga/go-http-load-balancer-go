package gateway

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Sentinel errors returned by the management operations.
var (
	// ErrBackendNotFound means no registered backend uses that name.
	ErrBackendNotFound = errors.New("backend not found")
	// ErrBackendAlreadyExists means a backend with that name/URL is registered.
	ErrBackendAlreadyExists = errors.New("backend already exists")
)

// Pool is the set of registered backends. Registration and deregistration
// are safe to call concurrently with request routing.
type Pool struct {
	mu       sync.RWMutex
	byName   map[string]*poolEntry
	order    []string
	rr       atomic.Uint64
	strategy Strategy
	logger   *slog.Logger
	metrics  Metrics
}

// poolEntry tracks one backend and, while it is draining, lets concurrent
// deregister calls share a single drain instead of repeating it.
type poolEntry struct {
	backend  *Backend
	draining bool
	doneCh   chan struct{} // closed once the in-flight count reaches zero
}

// NewPool creates a Pool using the given strategy.
func NewPool(strategy Strategy, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{strategy: strategy, logger: logger, byName: make(map[string]*poolEntry)}
}

// Register adds a backend so it becomes routable immediately.
func (p *Pool) Register(b *Backend) {
	p.mu.Lock()
	b.pool = p
	p.byName[b.Name] = &poolEntry{backend: b}
	p.order = append(p.order, b.Name)
	p.mu.Unlock()
	p.logger.Info("backend registered", "backend", b.Name, "url", b.URL.String())
}

// Deregister stops routing new requests to b, then waits for in-flight
// requests to finish (bounded by ctx). The backend never receives new
// traffic once Deregister has been entered.
func (p *Pool) Deregister(ctx context.Context, b *Backend) error {
	return p.DeregisterByName(ctx, b.Name)
}

// RegisterBackend validates name/URL uniqueness, then registers the backend.
// It returns ErrBackendAlreadyExists when the name or URL is already in use,
// including a backend that is still draining; pool state is unchanged.
func (p *Pool) RegisterBackend(b *Backend) error {
	if b == nil || b.Name == "" {
		return errors.New("backend name is required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.byName[b.Name]; exists {
		return ErrBackendAlreadyExists
	}
	for _, e := range p.byName {
		if e.backend.URL.String() == b.URL.String() {
			return ErrBackendAlreadyExists
		}
	}
	b.pool = p
	p.byName[b.Name] = &poolEntry{backend: b}
	p.order = append(p.order, b.Name)
	p.logger.Info("backend registered", "backend", b.Name, "url", b.URL.String())
	return nil
}

// Lookup returns the registered backend with the given name, or
// ErrBackendNotFound (draining backends are still found until removed).
func (p *Pool) Lookup(name string) (*Backend, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	e, ok := p.byName[name]
	if !ok {
		return nil, ErrBackendNotFound
	}
	return e.backend, nil
}

// DeregisterByName starts draining name (once) and blocks until the in-flight
// requests finish or ctx expires. Concurrent calls for the same name share
// the same drain: they never start a second drain or lose in-flight work, and
// each caller waits independently. The entry is removed from routing
// immediately; it disappears from the map only after the drain finishes.
// Returns ErrBackendNotFound for unknown/already-removed names and
// ErrDrainTimeout when this caller's wait expires.
func (p *Pool) DeregisterByName(ctx context.Context, name string) error {
	p.mu.Lock()
	e, ok := p.byName[name]
	if !ok {
		p.mu.Unlock()
		return ErrBackendNotFound
	}
	if !e.draining {
		e.draining = true
		e.doneCh = make(chan struct{})
		e.backend.startDrain()
		p.removeFromOrderLocked(name)
		p.logger.Info("backend deregistering, draining in-flight requests",
			"backend", name, "inflight", e.backend.InFlight())
		go p.finishDrain(e)
	}
	done := e.doneCh
	p.mu.Unlock()

	select {
	case <-done:
		p.logger.Info("backend drained and removed", "backend", name)
		return nil
	case <-ctx.Done():
		return ErrDrainTimeout
	}
}

// finishDrain waits for in-flight work, then drops the entry from the map.
// Waiting callers are released by closing doneCh even if they had timed out
// earlier; the backend never receives new traffic meanwhile.
func (p *Pool) finishDrain(e *poolEntry) {
	e.backend.waitDrain()
	name := e.backend.Name
	p.mu.Lock()
	if cur := p.byName[name]; cur == e {
		delete(p.byName, name)
	}
	p.mu.Unlock()
	close(e.doneCh)
}

func (p *Pool) removeFromOrderLocked(name string) {
	for i, n := range p.order {
		if n == name {
			p.order = append(p.order[:i], p.order[i+1:]...)
			return
		}
	}
}

// Snapshot returns a copy of the current backend list.
func (p *Pool) Snapshot() []*Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Backend, 0, len(p.order))
	for _, name := range p.order {
		if e, ok := p.byName[name]; ok {
			out = append(out, e.backend)
		}
	}
	return out
}

// SnapshotAll also includes backends that are draining but not yet removed,
// so status output can show their drain progress.
func (p *Pool) SnapshotAll() []*Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Backend, 0, len(p.byName))
	for _, e := range p.byName {
		out = append(out, e.backend)
	}
	return out
}

// Next picks a healthy, non-draining backend not in excluded and reserves an
// in-flight slot on it. The caller must call Release when done. Returns nil
// when nothing is routable.
func (p *Pool) Next(excluded map[*Backend]struct{}) *Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := len(p.order)
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
		b := p.byName[p.order[(int(start)+i)%n]].backend
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
	for _, name := range p.order {
		b := p.byName[name].backend
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
