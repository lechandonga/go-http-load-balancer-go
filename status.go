package gateway

import (
	"encoding/json"
	"net/http"
)

// BackendStatus is the observability snapshot of one backend.
type BackendStatus struct {
	Name            string `json:"name"`
	URL             string `json:"url"`
	Healthy         bool   `json:"healthy"`
	Draining        bool   `json:"draining"`
	InFlight        int64  `json:"in_flight"`
	Requests        int64  `json:"requests"`
	Failures        int64  `json:"failures"`
	FailUnreachable int64  `json:"fail_unreachable"`
	FailTimeout     int64  `json:"fail_timeout"`
	FailBadResponse int64  `json:"fail_bad_response"`
	Retries         int64  `json:"retries"`
	HealthFlips     int64  `json:"health_flips"`
}

// PoolStatus is the top-level status document: per-backend detail plus a
// gateway-wide cumulative total (including deregistered backends).
type PoolStatus struct {
	Backends []BackendStatus `json:"backends"`
	Total    BackendCounters `json:"total"`
}

func statusOf(b *Backend) BackendStatus {
	c := b.Counters()
	return BackendStatus{
		Name:            b.Name,
		URL:             b.URL.String(),
		Healthy:         b.Healthy(),
		Draining:        b.Draining(),
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
		backends := p.Snapshot()
		out := make([]BackendStatus, 0, len(backends))
		for _, b := range backends {
			out = append(out, statusOf(b))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PoolStatus{
			Backends: out,
			Total:    p.metrics.Snapshot(),
		})
	})
}
