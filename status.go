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
	InFlight int64  `json:"in_flight"`
	Requests int64  `json:"requests"`
	Failures int64  `json:"failures"`
}

// StatusHandler exposes the pool state as JSON for manual verification and
// scripting (e.g. GET /-/status on the gateway).
func (p *Pool) StatusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backends := p.Snapshot()
		out := make([]BackendStatus, 0, len(backends))
		for _, b := range backends {
			reqs, fails := b.Stats()
			out = append(out, BackendStatus{
				Name:     b.Name,
				URL:      b.URL.String(),
				Healthy:  b.Healthy(),
				Draining: b.Draining(),
				InFlight: b.InFlight(),
				Requests: reqs,
				Failures: fails,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
}
