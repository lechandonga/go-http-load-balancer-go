package gateway

import (
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

	// pool is the owning pool, used to propagate health flips into the
	// global counters. Set by Pool.RegisterBackend; nil for standalone use.
	pool *Pool

	mu         sync.Mutex
	healthy    bool
	draining   bool
	consecOK   int
	consecFail int

	inflight sync.WaitGroup
	conns    atomic.Int64

	totalReqs       atomic.Int64
	failUnreachable atomic.Int64
	failTimeout     atomic.Int64
	failBadResponse atomic.Int64
	failResponse5xx atomic.Int64
	retries         atomic.Int64
	flipsHealthy    atomic.Int64
	flipsUnhealthy  atomic.Int64
}

// NewBackend builds a Backend that starts out healthy.
func NewBackend(name string, rawURL string) (*Backend, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid backend URL %q: scheme must be http or https", rawURL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid backend URL %q: host is required", rawURL)
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

// recordOutcome feeds one observation (active probe or real request) into
// the health state machine. It returns true only when the state flipped.
//
// The consecutive counters are the anti-flapping mechanism: a single
// outlier resets the opposite counter but cannot switch the state.
func (b *Backend) recordOutcome(ok bool, failThreshold, recoveryThreshold int) (changed bool, healthy bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ok {
		b.consecOK++
		b.consecFail = 0
		if !b.healthy && b.consecOK >= recoveryThreshold {
			b.healthy = true
			b.flipsHealthy.Add(1)
			if b.pool != nil {
				b.pool.metrics.addFlip(true)
			}
			return true, true
		}
		return false, b.healthy
	}
	b.consecFail++
	b.consecOK = 0
	if b.healthy && b.consecFail >= failThreshold {
		b.healthy = false
		b.flipsUnhealthy.Add(1)
		if b.pool != nil {
			b.pool.metrics.addFlip(false)
		}
		return true, false
	}
	return false, b.healthy
}

// recordFailure adds one business-traffic failure of the given class.
// Active health probes and client cancellations are not counted here.
func (b *Backend) recordFailure(class FailureClass) {
	switch class {
	case FailUnreachable:
		b.failUnreachable.Add(1)
	case FailTimeout:
		b.failTimeout.Add(1)
	case FailBadResponse:
		b.failBadResponse.Add(1)
	case FailResponse5xx:
		b.failResponse5xx.Add(1)
	}
}

// recordRetry counts one additional (non-initial) attempt sent here.
func (b *Backend) recordRetry() { b.retries.Add(1) }

// Counters returns the cumulative business metrics for this backend.
func (b *Backend) Counters() BackendCounters {
	return BackendCounters{
		Requests:         b.totalReqs.Load(),
		Unreachable:      b.failUnreachable.Load(),
		Timeout:          b.failTimeout.Load(),
		BadResponse:      b.failBadResponse.Load(),
		Response5xx:      b.failResponse5xx.Load(),
		Retries:          b.retries.Load(),
		FlipsToHealthy:   b.flipsHealthy.Load(),
		FlipsToUnhealthy: b.flipsUnhealthy.Load(),
	}.finalized()
}

// BackendCounters is the stable per-backend metric snapshot. Failures is the
// sum of the four failure classes and HealthFlips is the sum of the two
// flip counters.
type BackendCounters struct {
	Requests         int64 `json:"requests"`
	Failures         int64 `json:"failures"`
	Unreachable      int64 `json:"unreachable"`
	Timeout          int64 `json:"timeout"`
	BadResponse      int64 `json:"bad_response"`
	Response5xx      int64 `json:"response_5xx"`
	Retries          int64 `json:"retries"`
	FlipsToHealthy   int64 `json:"flips_to_healthy"`
	FlipsToUnhealthy int64 `json:"flips_to_unhealthy"`
	HealthFlips      int64 `json:"health_flips"`
}

func (c BackendCounters) finalized() BackendCounters {
	c.Failures = c.Unreachable + c.Timeout + c.BadResponse + c.Response5xx
	c.HealthFlips = c.FlipsToHealthy + c.FlipsToUnhealthy
	return c
}

// FailureClass identifies why one proxied attempt failed.
type FailureClass int

const (
	// FailUnreachable: connection/dial failure.
	FailUnreachable FailureClass = iota
	// FailTimeout: attempt or budget timeout.
	FailTimeout
	// FailBadResponse: malformed/truncated upstream response.
	FailBadResponse
	// FailResponse5xx: upstream answered with a 5xx status.
	FailResponse5xx
)
