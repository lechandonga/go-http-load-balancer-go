package lb

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// Metrics 是进程内、仅用标准库实现的计数器集合，
// 通过 /admin/metrics 以 Prometheus 文本风格暴露，无需任何外部服务。
type Metrics struct {
	requestsTotal      atomic.Int64
	requestsFailed     atomic.Int64
	requestsRetried    atomic.Int64
	noHealthyFastFails atomic.Int64

	mu              sync.Mutex
	failuresByKind  map[FailureKind]int64
	retriesByReason map[string]int64
}

func newMetrics() *Metrics {
	return &Metrics{
		failuresByKind:  make(map[FailureKind]int64),
		retriesByReason: make(map[string]int64),
	}
}

func (m *Metrics) incRequest()   { m.requestsTotal.Add(1) }
func (m *Metrics) incFailed()    { m.requestsFailed.Add(1) }
func (m *Metrics) incRetried()   { m.requestsRetried.Add(1) }
func (m *Metrics) incNoHealthy() { m.noHealthyFastFails.Add(1) }

func (m *Metrics) incFailureKind(k FailureKind) {
	m.mu.Lock()
	m.failuresByKind[k]++
	m.mu.Unlock()
}

func (m *Metrics) incRetryReason(reason string) {
	m.mu.Lock()
	m.retriesByReason[reason]++
	m.mu.Unlock()
}

// Render 输出文本格式指标快照。
func (m *Metrics) Render() string {
	m.mu.Lock()
	kinds := make([]FailureKind, 0, len(m.failuresByKind))
	for k := range m.failuresByKind {
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	reasons := make([]string, 0, len(m.retriesByReason))
	for r := range m.retriesByReason {
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	m.mu.Unlock()

	s := "# HELP lb_requests_total Total client requests accepted by the gateway.\n"
	s += "# TYPE lb_requests_total counter\n"
	s += fmt.Sprintf("lb_requests_total %d\n", m.requestsTotal.Load())
	s += "# HELP lb_requests_failed_total Requests that ended in an error response.\n"
	s += "# TYPE lb_requests_failed_total counter\n"
	s += fmt.Sprintf("lb_requests_failed_total %d\n", m.requestsFailed.Load())
	s += "# HELP lb_requests_retried_total Retry attempts dispatched to another backend.\n"
	s += "# TYPE lb_requests_retried_total counter\n"
	s += fmt.Sprintf("lb_requests_retried_total %d\n", m.requestsRetried.Load())
	s += "# HELP lb_no_healthy_fastfail_total Requests failed fast because no backend was available.\n"
	s += "# TYPE lb_no_healthy_fastfail_total counter\n"
	s += fmt.Sprintf("lb_no_healthy_fastfail_total %d\n", m.noHealthyFastFails.Load())

	s += "# TYPE lb_upstream_failures_total counter\n"
	for _, k := range kinds {
		m.mu.Lock()
		v := m.failuresByKind[k]
		m.mu.Unlock()
		s += fmt.Sprintf("lb_upstream_failures_total{kind=%q} %d\n", k, v)
	}
	s += "# TYPE lb_retry_attempts_total counter\n"
	for _, r := range reasons {
		m.mu.Lock()
		v := m.retriesByReason[r]
		m.mu.Unlock()
		s += fmt.Sprintf("lb_retry_attempts_total{reason=%q} %d\n", r, v)
	}
	return s
}
