package gateway

import (
	"encoding/json"
	"net/http"
)

// BackendStatus is the observability snapshot of one backend.
type BackendStatus struct {
	Name     string          `json:"name"`
	URL      string          `json:"url"`
	Healthy  bool            `json:"healthy"`
	Draining bool            `json:"draining"`
	InFlight int64           `json:"in_flight"`
	Metrics  BackendCounters `json:"metrics"`
}

// StatusResponse is the stable top-level status document. Field names are
// part of the script-facing contract.
type StatusResponse struct {
	Backends []BackendStatus `json:"backends"`
	Total    GlobalCounters  `json:"total"`
}

// statusHandler exposes the pool state and metrics as JSON for manual
// verification and scripting (e.g. GET /-/status on the gateway). It is
// served on the management mux, so these requests never hit the backends
// and are never counted as business traffic.
func (p *Pool) statusHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		backends := p.SnapshotAll()
		out := make([]BackendStatus, 0, len(backends))
		for _, b := range backends {
			out = append(out, BackendStatus{
				Name:     b.Name,
				URL:      b.URL.String(),
				Healthy:  b.Healthy(),
				Draining: b.Draining(),
				InFlight: b.InFlight(),
				Metrics:  b.Counters(),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(StatusResponse{Backends: out, Total: p.metrics.Snapshot()})
	})
}
