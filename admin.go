package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// AdminHandler is the gateway's runtime management API. It is mounted on
// the gateway listener under a configurable prefix (default /-/admin) and
// is never proxied to any backend, nor counted in forwarding metrics.
//
// Routes (relative to the mount prefix):
//
//	POST   /backends            register a backend  {"name": "...", "url": "..."}
//	GET    /backends            list registered backends
//	DELETE /backends/{name}     gracefully deregister (?timeout= overrides DrainTimeout)
type AdminHandler struct {
	pool   *Pool
	cfg    Config
	logger *slog.Logger
}

// NewAdminHandler builds the management API over the pool.
func NewAdminHandler(pool *Pool, cfg Config, logger *slog.Logger) *AdminHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminHandler{pool: pool, cfg: cfg, logger: logger}
}

type registerRequest struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeAdminError(w http.ResponseWriter, status int, code, detail string) {
	writeJSON(w, status, map[string]string{"error": code, "detail": detail})
}

// ServeHTTP routes admin requests. It expects to be mounted with the
// prefix stripped (see cmd/gateway).
func (a *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	switch {
	case path == "backends" && r.Method == http.MethodPost:
		a.register(w, r)
	case path == "backends" && r.Method == http.MethodGet:
		a.list(w, r)
	case strings.HasPrefix(path, "backends/") && r.Method == http.MethodDelete:
		a.deregister(w, r, strings.TrimPrefix(path, "backends/"))
	case path == "backends" || strings.HasPrefix(path, "backends/"):
		writeAdminError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST/GET on /backends, DELETE on /backends/{name}")
	default:
		writeAdminError(w, http.StatusNotFound, "not_found", "unknown admin endpoint")
	}
}

func (a *AdminHandler) register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", "body must be JSON: {\"name\": \"...\", \"url\": \"...\"}")
		return
	}
	if req.URL == "" {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", "url is required")
		return
	}
	name := req.Name
	if name == "" {
		name = a.nextName()
	}
	b, err := NewBackend(name, req.URL)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_backend_url", err.Error())
		return
	}
	if err := a.pool.Register(b); err != nil {
		if errors.Is(err, ErrBackendExists) {
			writeAdminError(w, http.StatusConflict, "duplicate_backend",
				fmt.Sprintf("name or url already registered or still draining: %s", req.URL))
			return
		}
		writeAdminError(w, http.StatusInternalServerError, "register_failed", err.Error())
		return
	}
	a.logger.Info("admin: backend registered", "backend", b.Name, "url", b.URL.String())
	writeJSON(w, http.StatusCreated, statusOf(b))
}

// nextName picks backend-N with the smallest unused N.
func (a *AdminHandler) nextName() string {
	for i := 1; ; i++ {
		name := fmt.Sprintf("backend-%d", i)
		if a.pool.GetByName(name) == nil {
			return name
		}
	}
}

func (a *AdminHandler) list(w http.ResponseWriter, r *http.Request) {
	backends := a.pool.Snapshot()
	out := make([]BackendStatus, 0, len(backends))
	for _, b := range backends {
		out = append(out, statusOf(b))
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *AdminHandler) deregister(w http.ResponseWriter, r *http.Request, name string) {
	b := a.pool.GetByName(name)
	if b == nil {
		writeAdminError(w, http.StatusNotFound, "backend_not_found", "no registered backend named "+name)
		return
	}
	timeout := a.cfg.DrainTimeout
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			writeAdminError(w, http.StatusBadRequest, "invalid_request", "timeout must be a positive Go duration, e.g. 5s")
			return
		}
		timeout = d
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	err := a.pool.Deregister(ctx, b)
	switch {
	case err == nil:
		a.logger.Info("admin: backend deregistered", "backend", name)
		writeJSON(w, http.StatusOK, map[string]string{"status": "drained", "name": name})
	case errors.Is(err, ErrDrainTimeout):
		a.logger.Warn("admin: drain timed out", "backend", name, "timeout", timeout)
		writeAdminError(w, http.StatusGatewayTimeout, "drain_timeout",
			fmt.Sprintf("backend %s removed from routing but still has in-flight requests after %s", name, timeout))
	case errors.Is(err, ErrBackendNotFound):
		writeAdminError(w, http.StatusNotFound, "backend_not_found", "no registered backend named "+name)
	default:
		writeAdminError(w, http.StatusInternalServerError, "deregister_failed", err.Error())
	}
}
