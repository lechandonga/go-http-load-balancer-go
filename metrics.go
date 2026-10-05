package gateway

import (
	"errors"
	"sync/atomic"
)

// BackendCounters is a consistent snapshot of one backend's counters.
//
// Requests counts proxy attempts routed to the backend. Failures counts
// failed forwarding attempts (transport errors and 5xx responses) and
// always equals FailUnreachable+FailTimeout+FailBadResponse.
// HealthFailures counts failed active health-check observations — the
// stable, explicit home for background-check failures. Retries counts
// extra attempts (beyond the first) routed here. HealthFlips counts
// healthy<->unhealthy transitions.
type BackendCounters struct {
	Requests        int64 `json:"requests"`
	Failures        int64 `json:"failures"`
	FailUnreachable int64 `json:"fail_unreachable"`
	FailTimeout     int64 `json:"fail_timeout"`
	FailBadResponse int64 `json:"fail_bad_response"`
	HealthFailures  int64 `json:"health_failures"`
	Retries         int64 `json:"retries"`
	HealthFlips     int64 `json:"health_flips"`
}

// add accumulates another snapshot into c (used for the removed-backends
// aggregate so global totals always reconcile).
func (c *BackendCounters) add(o BackendCounters) {
	c.Requests += o.Requests
	c.Failures += o.Failures
	c.FailUnreachable += o.FailUnreachable
	c.FailTimeout += o.FailTimeout
	c.FailBadResponse += o.FailBadResponse
	c.HealthFailures += o.HealthFailures
	c.Retries += o.Retries
	c.HealthFlips += o.HealthFlips
}

// Metrics holds gateway-wide cumulative counters. They are monotonic and
// include backends that have since been deregistered, so totals always
// match what the gateway actually did.
type Metrics struct {
	requests        atomic.Int64
	failures        atomic.Int64
	failUnreachable atomic.Int64
	failTimeout     atomic.Int64
	failBadResponse atomic.Int64
	healthFailures  atomic.Int64
	retries         atomic.Int64
	healthFlips     atomic.Int64
}

// noteAttempt records one forwarding attempt routed to some backend.
func (m *Metrics) noteAttempt() {
	m.requests.Add(1)
}

// noteFailure records one classified forwarding failure.
func (m *Metrics) noteFailure(err error) {
	m.failures.Add(1)
	switch {
	case errors.Is(err, ErrBackendTimeout):
		m.failTimeout.Add(1)
	case errors.Is(err, ErrBadResponse):
		m.failBadResponse.Add(1)
	default:
		m.failUnreachable.Add(1)
	}
}

// noteHealthFailure records one failed active health-check observation.
func (m *Metrics) noteHealthFailure() {
	m.healthFailures.Add(1)
}

// noteRetry records one extra attempt beyond the first.
func (m *Metrics) noteRetry() {
	m.retries.Add(1)
}

// noteHealthFlip records one healthy<->unhealthy transition.
func (m *Metrics) noteHealthFlip() {
	m.healthFlips.Add(1)
}

// Snapshot returns a consistent copy of the global counters.
func (m *Metrics) Snapshot() BackendCounters {
	return BackendCounters{
		Requests:        m.requests.Load(),
		Failures:        m.failures.Load(),
		FailUnreachable: m.failUnreachable.Load(),
		FailTimeout:     m.failTimeout.Load(),
		FailBadResponse: m.failBadResponse.Load(),
		HealthFailures:  m.healthFailures.Load(),
		Retries:         m.retries.Load(),
		HealthFlips:     m.healthFlips.Load(),
	}
}
