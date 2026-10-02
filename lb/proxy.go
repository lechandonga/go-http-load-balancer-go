package lb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Proxy 是反向代理 / 负载均衡的 HTTP Handler。
type Proxy struct {
	registry *Registry
	cfg      Config
	client   *http.Client
	metrics  *Metrics
	log      *slog.Logger
}

// NewProxy 构造代理。client 的传输层支持连接复用，超时在每次尝试上单独施加。
func NewProxy(r *Registry, cfg Config, m *Metrics, log *slog.Logger) *Proxy {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 64
	return &Proxy{
		registry: r,
		cfg:      cfg,
		client:   &http.Client{Transport: transport},
		metrics:  m,
		log:      log,
	}
}

// isIdempotent 判断方法语义上是否幂等。只有幂等方法才允许自动重试；
// POST/PATCH 等非幂等请求即使失败也绝不盲目重放。
func isIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut,
		http.MethodDelete, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.metrics.incRequest()

	totalCtx, cancelTotal := context.WithTimeout(r.Context(), p.cfg.TotalBudget)
	defer cancelTotal()

	idempotent := isIdempotent(r.Method)

	// 幂等请求且带 body 时，缓冲 body 以便在其他实例上重放；
	// 超过缓冲上限则放弃重试资格，避免无界内存占用。
	var bodyBytes []byte
	canRetryBody := true
	if idempotent && r.Body != nil && r.Body != http.NoBody {
		bodyBytes, canRetryBody = p.bufferBody(r)
	}
	canRetry := idempotent && canRetryBody

	tried := map[string]bool{}
	var lastErr *UpstreamError
	attempts := 0

	for attempts = 0; attempts < p.cfg.MaxAttempts; attempts++ {
		// 客户端取消：立即向下游传播并返回，不写错误体（连接已撤销）。
		if err := totalCtx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				p.log.Debug("client canceled", "path", r.URL.Path)
				return
			}
			p.writeError(w, http.StatusGatewayTimeout, "gateway: request time budget exhausted")
			p.metrics.incFailed()
			return
		}

		b := p.registry.pickHealthy(tried)
		if b == nil {
			break // 所有健康实例都已尝试过或根本没有健康实例
		}

		resp, uerr := p.attempt(totalCtx, b, r, bodyBytes, attempts > 0)
		if uerr == nil {
			retriableStatus := resp.StatusCode >= p.cfg.RetryStatusAbove

			// 状态码属于可重试区间且仍有资格 / 预算，则排空响应体后换实例，
			// 同时把这次真实失败计入后端健康连击。
			if retriableStatus && canRetry &&
				attempts+1 < p.cfg.MaxAttempts && totalCtx.Err() == nil {
				b.mu.Lock()
				b.totalRequests++
				b.totalFailures++
				b.mu.Unlock()
				p.recordObservation(b, false)
				b.release()
				tried[b.id] = true
				drainBody(resp)
				p.metrics.incRetried()
				p.metrics.incRetryReason(fmt.Sprintf("status_%d", resp.StatusCode))
				p.log.Warn("retrying on another backend due to status",
					"backend", b.id, "status", resp.StatusCode, "attempt", attempts+1)
				resp = nil
				continue
			}

			// 正常响应（或虽为 5xx 但不再重试：非幂等 / 次数耗尽 / 预算到期）。
			b.mu.Lock()
			b.totalRequests++
			if retriableStatus {
				// 最终要向客户端交付这个失败状态：计入健康连击。
				b.totalFailures++
			}
			b.mu.Unlock()
			p.recordObservation(b, !retriableStatus)
			b.release()
			p.writeResponse(w, resp)
			return
		}

		// 该次尝试失败：真实请求结果参与健康判定。
		b.mu.Lock()
		b.totalRequests++
		b.totalFailures++
		b.mu.Unlock()
		p.recordObservation(b, false)
		b.release()

		if uerr.Kind == KindCanceled {
			p.log.Debug("client canceled mid-flight", "backend", b.id)
			return
		}
		p.metrics.incFailureKind(uerr.Kind)
		lastErr = uerr
		tried[b.id] = true
		p.log.Warn("upstream attempt failed",
			"backend", b.id, "kind", uerr.Kind, "attempt", attempts+1, "op", uerr.Op)

		if !canRetry || !IsRetryable(uerr.Kind) || attempts+1 >= p.cfg.MaxAttempts {
			break
		}
		if totalCtx.Err() != nil {
			break
		}
		p.metrics.incRetried()
		p.metrics.incRetryReason(string(uerr.Kind))
	}

	// 快速失败路径：没有任何健康实例时不等待、不挂起。
	if lastErr == nil {
		p.metrics.incNoHealthy()
		p.metrics.incFailed()
		p.writeError(w, http.StatusServiceUnavailable,
			"gateway: no healthy backend available")
		return
	}
	p.metrics.incFailed()
	status, msg := mapFailure(lastErr)
	p.writeError(w, status, msg)
}

func mapFailure(e *UpstreamError) (int, string) {
	switch e.Kind {
	case KindTimeout:
		return http.StatusGatewayTimeout, "gateway: upstream request timed out"
	case KindBadResponse:
		return http.StatusBadGateway, "gateway: upstream returned an invalid response"
	default:
		return http.StatusBadGateway, "gateway: upstream is unreachable"
	}
}

// attempt 向单个实例发起一次尝试。调用方根据状态码与重试资格
// 决定排空重试或透传响应。
func (p *Proxy) attempt(parent context.Context, b *Backend, r *http.Request,
	bodyBytes []byte, isRetry bool) (*http.Response, *UpstreamError) {

	attemptCtx, cancel := context.WithTimeout(parent, p.cfg.RequestTimeout)
	defer cancel()

	target, err := url.Parse(b.url)
	if err != nil {
		return nil, classify(b.id, "parse", err)
	}
	target.Path = joinPath(target.Path, r.URL.Path)
	target.RawPath = r.URL.RawPath
	target.RawQuery = r.URL.RawQuery
	target.Fragment = r.URL.Fragment

	var body io.Reader
	if bodyBytes != nil {
		body = bytes.NewReader(bodyBytes)
	} else if !isRetry {
		body = r.Body
	}
	req, err := http.NewRequestWithContext(attemptCtx, r.Method, target.String(), body)
	if err != nil {
		return nil, classify(b.id, "build", err)
	}
	copyHeaders(req.Header, r.Header)
	removeHopHeaders(req.Header)

	if r.RemoteAddr != "" {
		if host, _, ok := strings.Cut(r.RemoteAddr, ":"); ok {
			addForwarded(req.Header, "X-Forwarded-For", host)
		}
	}
	if r.TLS != nil {
		addForwarded(req.Header, "X-Forwarded-Proto", "https")
	} else {
		addForwarded(req.Header, "X-Forwarded-Proto", "http")
	}
	if req.Header.Get("Host") == "" {
		req.Host = r.Host
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, classify(b.id, "roundtrip", err)
	}
	return resp, nil
}

func (p *Proxy) recordObservation(b *Backend, ok bool) {
	transitioned, alive := b.observe(ok, false, p.cfg.Health.withDefaults(), time.Now())
	if transitioned {
		if alive {
			p.log.Info("backend recovered via real traffic", "backend", b.id)
		} else {
			p.log.Warn("backend marked unhealthy via real traffic",
				"backend", b.id, "failure_threshold", p.cfg.Health.FailureThreshold)
		}
		p.registry.noteTransition(b.id, alive)
	}
}

func (p *Proxy) bufferBody(r *http.Request) ([]byte, bool) {
	limited := io.LimitReader(r.Body, p.cfg.MaxBufferedBody+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, false
	}
	if int64(len(data)) > p.cfg.MaxBufferedBody {
		return nil, false
	}
	return data, true
}

func (p *Proxy) writeResponse(w http.ResponseWriter, resp *http.Response) {
	defer resp.Body.Close()
	removeHopHeaders(resp.Header)
	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	// 流式拷贝；头已提交后若中途失败无法改状态码，只能结束响应体。
	if _, err := io.Copy(w, resp.Body); err != nil {
		p.log.Debug("response stream interrupted", "error", err)
	}
}

func (p *Proxy) writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":%q}`, msg)
}

func drainBody(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
}

func joinPath(a, b string) string {
	switch {
	case a == "" || a == "/":
		if b == "" {
			return "/"
		}
		return b
	case b == "" || b == "/":
		return a
	default:
		return strings.TrimRight(a, "/") + "/" + strings.TrimLeft(b, "/")
	}
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func addForwarded(h http.Header, key, value string) {
	if existing := h.Get(key); existing != "" {
		h.Set(key, existing+", "+value)
	} else {
		h.Set(key, value)
	}
}

// hop-by-hop 头按 RFC 7230 6.1 剥离，代理不端到端转发。
var hopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

func removeHopHeaders(h http.Header) {
	for _, name := range hopHeaders {
		h.Del(name)
	}
}
