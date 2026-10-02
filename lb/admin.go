package lb

import (
	"encoding/json"
	"net/http"
	"strings"
)

// adminHandler 提供运行期注册/摘除、状态查看与指标，全部本地 HTTP，
// 无外部依赖、无凭据。
type adminHandler struct {
	registry *Registry
	metrics  *Metrics
	prefix   string
}

func (m *adminHandler) register(mux *http.ServeMux) {
	p := strings.TrimRight(m.prefix, "/")
	mux.HandleFunc(p+"/backends", m.handleBackends)
	mux.HandleFunc(p+"/backends/", m.handleBackend)
	mux.HandleFunc(p+"/metrics", m.handleMetrics)
	mux.HandleFunc(p+"/healthz", m.handleHealthz)
}

// GET    /admin/backends          列出实例及健康状态
// POST   /admin/backends          {"id":"b1","url":"http://127.0.0.1:9001"} 注册/替换
// DELETE /admin/backends/{id}     优雅摘除（先停新请求，等待在途收尾）
func (m *adminHandler) handleBackends(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		m.writeJSON(w, http.StatusOK, m.registry.Snapshots())
	case http.MethodPost:
		var spec BackendSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil || spec.ID == "" || spec.URL == "" {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": `invalid body, want {"id":"...","url":"http://..."}`,
			})
			return
		}
		m.registry.Register(spec.ID, spec.URL)
		m.writeJSON(w, http.StatusCreated, map[string]string{
			"id": spec.ID, "url": spec.URL, "status": "registered",
		})
	default:
		w.Header().Set("Allow", "GET, POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (m *adminHandler) handleBackend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.Header().Set("Allow", "DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, strings.TrimRight(m.prefix, "/")+"/backends/")
	if id == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err := m.registry.Deregister(id); err != nil {
		m.writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	m.writeJSON(w, http.StatusOK, map[string]string{
		"id": id, "status": "drained_and_removed",
	})
}

func (m *adminHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(m.metrics.Render()))
}

// healthz 只在至少存在一个健康实例时返回 200，便于自身被上游/脚本探测。
func (m *adminHandler) handleHealthz(w http.ResponseWriter, r *http.Request) {
	for _, s := range m.registry.Snapshots() {
		if s.Alive && !s.Draining {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("no healthy backends"))
}

func (m *adminHandler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
