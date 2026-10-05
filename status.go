package gateway

import (
	"encoding/json"
	"net/http"
)

// BackendStatus is the observability snapshot of one backend.
type BackendStatus struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	Healthy  bool   `json:"healthy"`
	Draining bool   `json:"draining"`
	// Retired is true for backends whose drain has fully completed. They
	// stay in the document with their final counters so the total always
	// equals the sum of every backend listed here; they never route.
	Retired         bool  `json:"retired"`
	InFlight        int64 `json:"in_flight"`
	Requests        int64 `json:"requests"`
	Failures        int64 `json:"failures"`
	FailUnreachable int64 `json:"fail_unreachable"`
	FailTimeout     int64 `json:"fail_timeout"`
	FailBadResponse int64 `json:"fail_bad_response"`
	Retries         int64 `json:"retries"`
	HealthFlips     int64 `json:"health_flips"`
}

// PoolStatus is the top-level status document: per-backend detail plus a
// gateway-wide cumulative total. The total is exactly the sum of the
// per-backend counters in this document — routable, draining and retired
// instances are all listed.
type PoolStatus struct {
	Backends []BackendStatus `json:"backends"`
	Total    BackendCounters `json:"total"`
}

func statusOf(b *Backend) BackendStatus { return statusEx(b, false) }

func statusEx(b *Backend, retired bool) BackendStatus {
	c := b.Counters()
	return BackendStatus{
		Name:            b.Name,
		URL:             b.URL.String(),
		Healthy:         b.Healthy(),
		Draining:        b.Draining(),
		Retired:         retired,
		InFlight:        b.InFlight(),
		Requests:        c.Requests,
		Failures:        c.Failures,
		FailUnreachable: c.FailUnreachable,
		FailTimeout:     c.FailTimeout,
		FailBadResponse: c.FailBadResponse,
		Retries:         c.Retries,
		HealthFlips:     c.HealthFlips,
	}
}

// StatusHandler exposes the pool state as JSON for manual verification and
// scripting (e.g. GET /-/status on the gateway). The output shape is
// stable: {"backends": [...], "total": {...}}.
func (p *Pool) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backends := p.allInstances()
		out := make([]BackendStatus, 0, len(backends))
		for _, b := range backends {
			out = append(out, statusEx(b, p.isRetired(b)))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PoolStatus{
			Backends: out,
			Total:    p.metrics.Snapshot(),
		})
	})
}
