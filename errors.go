package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Sentinel errors classifying the failure chain. They are wrapped, never
// exposed raw to clients, and map to distinct HTTP statuses.
var (
	// ErrNoHealthyBackends means no backend is currently routable.
	ErrNoHealthyBackends = errors.New("no healthy backends available")
	// ErrBackendUnreachable means the backend could not be dialed/connected.
	ErrBackendUnreachable = errors.New("backend unreachable")
	// ErrBackendTimeout means the attempt exceeded its timeout or budget.
	ErrBackendTimeout = errors.New("backend timeout")
	// ErrBadResponse means the backend replied with an invalid response.
	ErrBadResponse = errors.New("bad response from backend")
	// ErrClientCanceled means the downstream client canceled the request.
	ErrClientCanceled = errors.New("client canceled request")
	// ErrDrainTimeout means Deregister did not finish draining in time.
	ErrDrainTimeout = errors.New("timed out waiting for backend to drain")
	// ErrBackendNotFound means the named backend is not registered.
	ErrBackendNotFound = errors.New("backend not found")
	// ErrBackendExists means a backend with the same name or URL exists.
	ErrBackendExists = errors.New("backend already registered")
)

// statusForError maps classified errors to client-visible HTTP statuses.
func statusForError(err error) int {
	switch {
	case errors.Is(err, ErrNoHealthyBackends):
		return http.StatusServiceUnavailable
	case errors.Is(err, ErrBackendTimeout):
		return http.StatusGatewayTimeout
	case errors.Is(err, ErrBackendUnreachable),
		errors.Is(err, ErrBadResponse):
		return http.StatusBadGateway
	default:
		return http.StatusBadGateway
	}
}

// codeForError is a stable machine-readable string for the error body.
func codeForError(err error) string {
	switch {
	case errors.Is(err, ErrNoHealthyBackends):
		return "no_healthy_backends"
	case errors.Is(err, ErrBackendTimeout):
		return "backend_timeout"
	case errors.Is(err, ErrBackendUnreachable):
		return "backend_unreachable"
	case errors.Is(err, ErrBadResponse):
		return "bad_response"
	default:
		return "proxy_error"
	}
}

// writeError renders a sanitized, classified error. Underlying causes
// (dial errors, etc.) are logged, never leaked to the client.
func writeError(w http.ResponseWriter, err error) {
	status := statusForError(err)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Lb-Error", codeForError(err))
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": codeForError(err),
	})
}
