package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// AdminHandler serves the runtime management surface. It is mounted on its
// own prefix on the same listener as business traffic but is handled
// entirely inside the gateway: requests here are never proxied and never
// counted in business metrics.
//
// Endpoints (prefix defaults to /-/admin):
//
//	GET    prefix/backends             list backends with state and metrics
//	POST   prefix/backends             register {"name":..,"url":..}
//	DELETE prefix/backends/{name}      deregister and drain (?timeout=30s)
//	GET    prefix/backends/{name}      inspect one backend
//
// All responses are JSON: {"result": "...", ...} on success and
// {"error": "<code>", "message": "..."} on failure.
type AdminHandler struct {
	pool   *Pool
	cfg    Config
	logger *slog.Logger
}

// NewAdminHandler builds the management handler using cfg.DrainTimeout as
// the default drain wait.
func NewAdminHandler(pool *Pool, cfg Config, logger *slog.Logger) *AdminHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AdminHandler{pool: pool, cfg: cfg, logger: logger}
}

type adminResponse struct {
	Result  string         `json:"result"`
	Backend *BackendStatus `json:"backend,omitempty"`
}

type adminError struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type registerRequest struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

// MountManagement attaches the runtime management surface to mux.
// adminPrefix (e.g. "/-/admin/") sets the management entry; an empty
// adminPrefix disables it entirely. statusPath (typically "/-/status") sets
// the metrics document; an empty statusPath disables it. The handlers own
// these paths completely: the business handler never sees these requests.
func MountManagement(mux *http.ServeMux, adminPrefix, statusPath string, pool *Pool, cfg Config, logger *slog.Logger) {
	if adminPrefix != "" {
		admin := NewAdminHandler(pool, cfg, logger)
		stripped := strings.TrimSuffix(adminPrefix, "/")
		mux.Handle(stripped+"/backends", http.StripPrefix(stripped, admin))
		mux.Handle(stripped+"/backends/", http.StripPrefix(stripped, admin))
	}
	if statusPath != "" {
		mux.Handle(statusPath, pool.statusHandler())
	}
}

// StatusPath is the default path of the status document.
const StatusPath = "/-/status"

// DefaultAdminPath is the default management entry prefix.
const DefaultAdminPath = "/-/admin/"

// ValidateManagementPath checks a configured management path. It must be
// absolute (start with "/") and not be the bare root, which would swallow
// all business traffic.
func ValidateManagementPath(p string) error {
	if p == "" {
		return nil
	}
	if !strings.HasPrefix(p, "/") {
		return fmt.Errorf("management path must start with '/', got %q", p)
	}
	if p == "/" {
		return errors.New("management path must not be '/'")
	}
	return nil
}

func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path
	switch {
	case path == "/backends" || path == "/backends/":
		switch r.Method {
		case http.MethodGet:
			h.list(w)
		case http.MethodPost:
			h.register(w, r)
		default:
			writeAdminError(w, http.StatusMethodNotAllowed, "method_not_allowed",
				fmt.Sprintf("method %s not allowed", r.Method))
		}
	default:
		// /backends/{name}[/...]
		name := path[len("/backends/"):]
		if name == "" {
			writeAdminError(w, http.StatusBadRequest, "invalid_request", "backend name is required")
			return
		}
		if slash := strings.IndexByte(name, '/'); slash >= 0 {
			name = name[:slash]
		}
		switch r.Method {
		case http.MethodGet:
			h.inspect(w, name)
		case http.MethodDelete:
			h.deregister(w, r, name)
		default:
			writeAdminError(w, http.StatusMethodNotAllowed, "method_not_allowed",
				fmt.Sprintf("method %s not allowed", r.Method))
		}
	}
}

func (h *AdminHandler) list(w http.ResponseWriter) {
	backends := h.pool.SnapshotAll()
	out := make([]BackendStatus, 0, len(backends))
	for _, b := range backends {
		out = append(out, statusOf(b))
	}
	writeJSON(w, http.StatusOK, map[string]any{"result": "ok", "backends": out})
}

func (h *AdminHandler) inspect(w http.ResponseWriter, name string) {
	b, err := h.pool.Lookup(name)
	if err != nil {
		writeAdminError(w, http.StatusNotFound, "backend_not_found",
			fmt.Sprintf("no backend named %q", name))
		return
	}
	status := statusOf(b)
	writeJSON(w, http.StatusOK, adminResponse{Result: "ok", Backend: &status})
}

func (h *AdminHandler) register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeAdminError(w, http.StatusBadRequest, "invalid_json",
				"request body must be a JSON object with name and url")
			return
		}
	}
	if req.URL == "" {
		writeAdminError(w, http.StatusBadRequest, "invalid_request", "url is required")
		return
	}
	if req.Name == "" {
		req.Name = defaultNameForURL(req.URL)
	}
	b, err := NewBackend(req.Name, req.URL)
	if err != nil {
		writeAdminError(w, http.StatusBadRequest, "invalid_backend", err.Error())
		return
	}
	if err := h.pool.RegisterBackend(b); err != nil {
		if errors.Is(err, ErrBackendAlreadyExists) {
			writeAdminError(w, http.StatusConflict, "backend_exists",
				"backend with same name or url already registered")
			return
		}
		writeAdminError(w, http.StatusBadRequest, "invalid_backend", err.Error())
		return
	}
	status := statusOf(b)
	h.logger.Info("admin: backend registered", "backend", b.Name, "url", b.URL.String())
	writeJSON(w, http.StatusCreated, adminResponse{Result: "registered", Backend: &status})
}

func (h *AdminHandler) deregister(w http.ResponseWriter, r *http.Request, name string) {
	timeout := h.cfg.DrainTimeout
	if v := r.URL.Query().Get("timeout"); v != "" {
		parsed, err := parseDuration(v)
		if err != nil || parsed <= 0 {
			writeAdminError(w, http.StatusBadRequest, "invalid_request",
				"timeout must be a positive Go duration, e.g. 5s")
			return
		}
		timeout = parsed
	}
	ctx, cancel := contextWithTimeout(r.Context(), timeout)
	defer cancel()

	err := h.pool.DeregisterByName(ctx, name)
	switch {
	case err == nil:
		h.logger.Info("admin: backend deregistered", "backend", name)
		writeJSON(w, http.StatusOK, map[string]string{
			"result": "deregistered", "name": name,
		})
	case errors.Is(err, ErrBackendNotFound):
		writeAdminError(w, http.StatusNotFound, "backend_not_found",
			fmt.Sprintf("no backend named %q (already removed?)", name))
	case errors.Is(err, ErrDrainTimeout):
		writeAdminError(w, http.StatusGatewayTimeout, "drain_timeout",
			fmt.Sprintf("backend %q removed from routing but in-flight requests did not finish within %s; draining continues server-side", name, timeout))
	default:
		writeAdminError(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}

func statusOf(b *Backend) BackendStatus {
	return BackendStatus{
		Name:     b.Name,
		URL:      b.URL.String(),
		Healthy:  b.Healthy(),
		Draining: b.Draining(),
		InFlight: b.InFlight(),
		Metrics:  b.Counters(),
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeAdminError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, adminError{Error: code, Message: message})
}

func defaultNameForURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "backend"
	}
	return strings.NewReplacer(":", "-", "/", "-").Replace(u.Host)
}

func parseDuration(s string) (time.Duration, error) { return time.ParseDuration(s) }

func contextWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}
