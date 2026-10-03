package gateway

import (
	"errors"
	"sync/atomic"
)

// Metrics holds the process-wide aggregate of business-traffic counters.
// Only proxied traffic counts here: management requests and active health
// probes never touch these values. All operations are lock-free.
type Metrics struct {
	requests        atomic.Int64
	failUnreachable atomic.Int64
	failTimeout     atomic.Int64
	failBadResponse atomic.Int64
	failResponse5xx atomic.Int64
	retries         atomic.Int64
	flipsHealthy    atomic.Int64
	flipsUnhealthy  atomic.Int64
}

func (m *Metrics) addRequest() { m.requests.Add(1) }
func (m *Metrics) addRetry()   { m.retries.Add(1) }
func (m *Metrics) addFlip(toHealthy bool) {
	if toHealthy {
		m.flipsHealthy.Add(1)
	} else {
		m.flipsUnhealthy.Add(1)
	}
}

func (m *Metrics) addFailure(class FailureClass) {
	switch class {
	case FailUnreachable:
		m.failUnreachable.Add(1)
	case FailTimeout:
		m.failTimeout.Add(1)
	case FailBadResponse:
		m.failBadResponse.Add(1)
	case FailResponse5xx:
		m.failResponse5xx.Add(1)
	}
}

// GlobalCounters is the stable JSON snapshot of aggregated metrics.
type GlobalCounters struct {
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

// Snapshot returns a consistent-enough aggregate read of all counters.
func (m *Metrics) Snapshot() GlobalCounters {
	c := GlobalCounters{
		Requests:         m.requests.Load(),
		Unreachable:      m.failUnreachable.Load(),
		Timeout:          m.failTimeout.Load(),
		BadResponse:      m.failBadResponse.Load(),
		Response5xx:      m.failResponse5xx.Load(),
		Retries:          m.retries.Load(),
		FlipsToHealthy:   m.flipsHealthy.Load(),
		FlipsToUnhealthy: m.flipsUnhealthy.Load(),
	}
	c.Failures = c.Unreachable + c.Timeout + c.BadResponse + c.Response5xx
	c.HealthFlips = c.FlipsToHealthy + c.FlipsToUnhealthy
	return c
}

// GlobalMetrics exposes the process-wide aggregate for the status endpoint.
func (p *Pool) GlobalMetrics() GlobalCounters { return p.metrics.Snapshot() }

// failureClassForError maps a classified proxy error to a failure class.
// Returns -1 for client cancellation, which is never a backend failure.
func failureClassForError(err error) FailureClass {
	switch {
	case errors.Is(err, ErrBackendTimeout):
		return FailTimeout
	case errors.Is(err, ErrBadResponse):
		return FailBadResponse
	case errors.Is(err, ErrBackendUnreachable):
		return FailUnreachable
	default:
		return FailUnreachable
	}
}
