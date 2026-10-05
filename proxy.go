package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// maxBufferedBody caps how much of a request body is buffered for retries.
const maxBufferedBody = 10 << 20

// LoadBalancer is an http.Handler that reverse-proxies requests to the
// healthy backends of a Pool, with classified errors, bounded retries and
// client-cancellation propagation.
type LoadBalancer struct {
	pool      *Pool
	cfg       Config
	transport *http.Transport
	logger    *slog.Logger
}

// NewLoadBalancer builds the gateway handler. A nil logger uses slog.Default.
func NewLoadBalancer(pool *Pool, cfg Config, logger *slog.Logger) (*LoadBalancer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &LoadBalancer{
		pool: pool,
		cfg:  cfg,
		transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConns:        256,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 5 * time.Second,
		},
		logger: logger,
	}, nil
}

// CloseIdleConnections releases pooled upstream connections.
func (lb *LoadBalancer) CloseIdleConnections() {
	lb.transport.CloseIdleConnections()
}

// isIdempotent reports whether replaying the method is safe by semantics.
func isIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions,
		http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

// ServeHTTP routes one client request, retrying transport-level failures on
// other healthy backends within the configured retry budget.
func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if lb.cfg.RetryBudget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, lb.cfg.RetryBudget)
		defer cancel()
	}

	// Non-idempotent requests are never retried: replaying them could
	// duplicate side effects. Idempotent bodies are buffered once so the
	// request can be rebuilt for each attempt.
	retryable := isIdempotent(r.Method)
	var body []byte
	if retryable && r.Body != nil && r.ContentLength != 0 {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, maxBufferedBody+1))
		r.Body.Close()
		if err != nil || len(body) > maxBufferedBody {
			retryable = false
			body = nil
		}
	}

	excluded := make(map[*Backend]struct{})
	var lastErr error
	for attempt := 0; attempt <= lb.cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil && r.Context().Err() == nil {
			// Retry budget exhausted.
			writeError(w, ErrBackendTimeout)
			return
		}
		b := lb.pool.Next(excluded)
		if b == nil {
			if lastErr != nil {
				writeError(w, lastErr)
			} else {
				writeError(w, ErrNoHealthyBackends)
			}
			return
		}
		lb.pool.metrics.noteAttempt()
		if attempt > 0 {
			// Every attempt beyond the first is one retry.
			b.noteRetry()
			lb.pool.metrics.noteRetry()
		}
		err := lb.proxyOnce(ctx, w, b, r, body)
		b.Release()
		if err == nil {
			return
		}
		lastErr = err
		if errors.Is(err, ErrClientCanceled) {
			// Client is gone; nothing to write, nothing to retry.
			return
		}
		lb.logger.Warn("attempt failed",
			"backend", b.Name, "attempt", attempt+1,
			"error", codeForError(err), "detail", err)
		excluded[b] = struct{}{} // retries must go elsewhere
		if !retryable {
			break
		}
	}
	writeError(w, lastErr)
}

// proxyOnce forwards the request to one backend and streams the response.
// It returns nil once the response has been fully handed to the client.
func (lb *LoadBalancer) proxyOnce(ctx context.Context, w http.ResponseWriter, b *Backend, r *http.Request, body []byte) error {
	attemptCtx, cancel := context.WithTimeout(ctx, lb.cfg.BackendTimeout)
	defer cancel()

	outReq, err := lb.buildOutbound(attemptCtx, b, r, body)
	if err != nil {
		return err
	}

	resp, err := lb.transport.RoundTrip(outReq)
	if err != nil {
		cerr := lb.classify(err, r)
		if !errors.Is(cerr, ErrClientCanceled) {
			// Client cancellation is not a backend failure signal and
			// reaches neither the state machine nor any failure counter.
			recordOutcome(lb.logger, lb.pool.metrics, "passive", b, false, cerr, lb.cfg)
		}
		return cerr
	}

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, copyErr := io.Copy(w, resp.Body)
	// A truncation announced via Content-Length surfaces only when the
	// body is closed, so the close error must be inspected explicitly.
	closeErr := resp.Body.Close()

	// Exactly one observation is recorded for the whole exchange: the
	// response is forwarded as-is even on 5xx, but a 5xx status or a
	// truncated body is a bad-response failure feeding the state machine
	// and the counters once. A 5xx answer never lands in
	// unreachable/timeout since the connection itself worked.
	switch {
	case copyErr != nil || closeErr != nil:
		if r.Context().Err() != nil {
			// The downstream client went away mid-body: never counted as
			// a backend failure.
			return ErrClientCanceled
		}
		recordOutcome(lb.logger, lb.pool.metrics, "passive", b, false, ErrBadResponse, lb.cfg)
		return ErrBadResponse
	case resp.StatusCode >= 500:
		recordOutcome(lb.logger, lb.pool.metrics, "passive", b, false, ErrBadResponse, lb.cfg)
	default:
		recordOutcome(lb.logger, lb.pool.metrics, "passive", b, true, nil, lb.cfg)
	}
	return nil
}

// buildOutbound rewrites the inbound request into an upstream request.
func (lb *LoadBalancer) buildOutbound(ctx context.Context, b *Backend, r *http.Request, body []byte) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	} else if r.Body != nil && r.ContentLength != 0 {
		reader = r.Body // non-retryable path: single attempt, stream as-is
	}
	u := *b.URL
	u.Path = joinPath(b.URL.Path, r.URL.Path)
	u.RawQuery = r.URL.RawQuery
	outReq, err := http.NewRequestWithContext(ctx, r.Method, u.String(), reader)
	if err != nil {
		return nil, err
	}
	outReq.Header = r.Header.Clone()
	outReq.Host = r.Host
	outReq.Header.Set("X-Forwarded-For", clientIP(r))
	return outReq, nil
}

// classify maps transport errors onto the sentinel error chain without
// leaking the underlying error to clients.
func (lb *LoadBalancer) classify(err error, r *http.Request) error {
	if r.Context().Err() != nil {
		return ErrClientCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrBackendTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrBackendTimeout
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrBadResponse
	}
	if isMalformedResponse(err) {
		// The peer answered with an HTTP response that cannot be parsed:
		// a bad response, not a reachability problem.
		return ErrBadResponse
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "read" {
		// The connection was established and a response was being read
		// when the peer dropped/reset it: the backend answered badly
		// rather than being unreachable.
		return ErrBadResponse
	}
	return ErrBackendUnreachable
}

// isMalformedResponse reports whether err is net/http's text error for
// an unparseable response. net/http returns these as plain errors (not a
// sentinel or a typed error), so the stable message prefixes are matched
// explicitly instead of errors.Is.
func isMalformedResponse(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "malformed HTTP response") ||
		strings.Contains(msg, "malformed MIME header")
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func joinPath(base, p string) string {
	if base == "" || base == "/" {
		return p
	}
	if p == "" {
		return base
	}
	return base + p
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
