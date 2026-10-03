package gateway

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// MockBackend is a controllable fake upstream used by drill mode and
// tests. Its behavior can be switched at runtime over HTTP:
//
//	POST /-/drill/error?enabled=true|false   always answer 500
//	POST /-/drill/hang?d=300ms               hang every request for d (0 clears)
//	POST /-/drill/down                       go offline (health + traffic fail)
//	POST /-/drill/up                         come back online
//	GET  /-/drill                            current drill state as JSON
//
// Regular traffic: GET /healthz reports liveness; any other path answers
// 200 with the instance name, after applying the injected behavior.
type MockBackend struct {
	// Name is echoed in responses and used as the gateway backend name.
	Name string

	srv *http.Server
	ln  net.Listener

	up        atomic.Bool
	injectErr atomic.Bool
	hangNanos atomic.Int64
}

// NewMockBackend starts a mock upstream listening on addr (e.g.
// "127.0.0.1:19001"; ":0" picks a free port).
func NewMockBackend(name, addr string) (*MockBackend, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mock backend %s: %w", name, err)
	}
	m := &MockBackend{Name: name, ln: ln}
	m.up.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", m.handleHealth)
	mux.HandleFunc("/-/drill", m.handleState)
	mux.HandleFunc("/-/drill/error", m.handleErrorToggle)
	mux.HandleFunc("/-/drill/hang", m.handleHang)
	mux.HandleFunc("/-/drill/down", m.handleDown)
	mux.HandleFunc("/-/drill/up", m.handleUp)
	mux.HandleFunc("/", m.handleTraffic)

	m.srv = &http.Server{Handler: mux}
	go m.srv.Serve(ln)
	return m, nil
}

// URL returns the base URL to register with the gateway.
func (m *MockBackend) URL() string {
	return "http://" + m.ln.Addr().String()
}

// Addr returns the listen address.
func (m *MockBackend) Addr() string {
	return m.ln.Addr().String()
}

// Close shuts the mock down.
func (m *MockBackend) Close() {
	_ = m.srv.Close()
}

func (m *MockBackend) state() map[string]any {
	return map[string]any{
		"name":            m.Name,
		"up":              m.up.Load(),
		"error_injection": m.injectErr.Load(),
		"hang":            time.Duration(m.hangNanos.Load()).String(),
	}
}

func (m *MockBackend) writeState(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m.state())
}

func (m *MockBackend) handleState(w http.ResponseWriter, r *http.Request) {
	m.writeState(w)
}

func (m *MockBackend) handleErrorToggle(w http.ResponseWriter, r *http.Request) {
	enabled := r.URL.Query().Get("enabled")
	m.injectErr.Store(enabled != "false")
	m.writeState(w)
}

func (m *MockBackend) handleHang(w http.ResponseWriter, r *http.Request) {
	d, err := time.ParseDuration(r.URL.Query().Get("d"))
	if err != nil || d < 0 {
		http.Error(w, `{"error":"invalid_duration"}`, http.StatusBadRequest)
		return
	}
	m.hangNanos.Store(int64(d))
	m.writeState(w)
}

func (m *MockBackend) handleDown(w http.ResponseWriter, r *http.Request) {
	m.up.Store(false)
	m.writeState(w)
}

func (m *MockBackend) handleUp(w http.ResponseWriter, r *http.Request) {
	m.up.Store(true)
	m.writeState(w)
}

func (m *MockBackend) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !m.up.Load() {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprintln(w, "ok")
}

func (m *MockBackend) handleTraffic(w http.ResponseWriter, r *http.Request) {
	if !m.up.Load() {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	if d := time.Duration(m.hangNanos.Load()); d > 0 {
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
	}
	if m.injectErr.Load() {
		http.Error(w, "injected error", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "hello from %s (path=%s)\n", m.Name, r.URL.Path)
}
