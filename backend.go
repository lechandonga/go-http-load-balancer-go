package gateway

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
)

// Backend is a single upstream instance. Its health state, draining flag and
// in-flight accounting are all safe for concurrent use.
//
// Lifecycle: healthy/unhealthy flips only after consecutive thresholds are
// reached (see recordOutcome). Deregistration sets draining, which makes
// Acquire fail immediately; the in-flight WaitGroup then drains naturally.
type Backend struct {
	// Name is a human-readable identifier used in logs and status output.
	Name string
	// URL is the base URL of the instance (scheme + host required).
	URL *url.URL

	mu         sync.Mutex
	healthy    bool
	draining   bool
	consecOK   int
	consecFail int

	inflight sync.WaitGroup
	conns    atomic.Int64

	totalReqs  atomic.Int64
	totalFails atomic.Int64

	failUnreachable atomic.Int64
	failTimeout     atomic.Int64
	failBadResponse atomic.Int64
	retryCount      atomic.Int64
	healthFlips     atomic.Int64
}

// NewBackend builds a Backend that starts out healthy.
func NewBackend(name string, rawURL string) (*Backend, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL %q: %w", rawURL, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid backend URL %q: scheme must be http/https and host is required", rawURL)
	}
	return &Backend{Name: name, URL: u, healthy: true}, nil
}

// Acquire reserves an in-flight slot. It fails if the backend is draining or
// unhealthy, guaranteeing no new traffic after deregistration starts.
func (b *Backend) Acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.draining || !b.healthy {
		return false
	}
	b.inflight.Add(1)
	b.conns.Add(1)
	b.totalReqs.Add(1)
	return true
}

// Release frees an in-flight slot reserved by Acquire.
func (b *Backend) Release() {
	b.conns.Add(-1)
	b.inflight.Done()
}

// startDrain flips the draining flag. After this returns, no goroutine can
// Acquire the backend, so waitDrain observes a quiescent WaitGroup.
func (b *Backend) startDrain() {
	b.mu.Lock()
	b.draining = true
	b.mu.Unlock()
}

// waitDrain blocks until all in-flight requests complete.
func (b *Backend) waitDrain() {
	b.inflight.Wait()
}

// finishDrain clears the draining flag after the drain has fully
// completed and the backend has been retired.
func (b *Backend) finishDrain() {
	b.mu.Lock()
	b.draining = false
	b.mu.Unlock()
}

// Draining reports whether deregistration has begun.
func (b *Backend) Draining() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.draining
}

// Healthy reports the current health state.
func (b *Backend) Healthy() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.healthy
}

// InFlight returns the number of requests currently being proxied.
func (b *Backend) InFlight() int64 {
	return b.conns.Load()
}

// Stats returns cumulative counters for observability.
func (b *Backend) Stats() (requests, failures int64) {
	return b.totalReqs.Load(), b.totalFails.Load()
}

// noteRetry records one extra attempt (beyond the first) routed to b.
func (b *Backend) noteRetry() {
	b.retryCount.Add(1)
}

// Counters returns a consistent snapshot of all per-backend counters.
func (b *Backend) Counters() BackendCounters {
	return BackendCounters{
		Requests:        b.totalReqs.Load(),
		Failures:        b.totalFails.Load(),
		FailUnreachable: b.failUnreachable.Load(),
		FailTimeout:     b.failTimeout.Load(),
		FailBadResponse: b.failBadResponse.Load(),
		Retries:         b.retryCount.Load(),
		HealthFlips:     b.healthFlips.Load(),
	}
}

// recordOutcome feeds one observation (active probe or real request) into
// the health state machine. A failed observation must carry failErr, the
// classified failure sentinel: its aggregate failure counter and exactly
// one classification counter are bumped inside the same critical section
// as the state-machine update, so concurrent observations can never make
// the counters disagree with what entered the state machine. It returns
// true only when the state flipped.
//
// The consecutive counters are the anti-flapping mechanism: a single
// outlier resets the opposite counter but cannot switch the state.
func (b *Backend) recordOutcome(ok bool, failErr error, failThreshold, recoveryThreshold int) (changed bool, healthy bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ok {
		b.consecOK++
		b.consecFail = 0
		if !b.healthy && b.consecOK >= recoveryThreshold {
			b.healthy = true
			b.healthFlips.Add(1)
			return true, true
		}
		return false, b.healthy
	}
	b.consecFail++
	b.consecOK = 0
	b.totalFails.Add(1)
	switch {
	case errors.Is(failErr, ErrBackendTimeout):
		b.failTimeout.Add(1)
	case errors.Is(failErr, ErrBadResponse):
		b.failBadResponse.Add(1)
	default:
		b.failUnreachable.Add(1)
	}
	if b.healthy && b.consecFail >= failThreshold {
		b.healthy = false
		b.healthFlips.Add(1)
		return true, false
	}
	return false, b.healthy
}
