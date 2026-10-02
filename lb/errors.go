package lb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
)

// FailureKind 是可区分的下游失败类别，网关据此决定重试与对外状态码，
// 不把底层错误原文直接透出给客户端。
type FailureKind string

const (
	// KindUnreachable：连接建立失败（拒绝、无路由、DNS 失败等）。
	KindUnreachable FailureKind = "unreachable"
	// KindTimeout：建立连接或等待响应超过时间预算。
	KindTimeout FailureKind = "timeout"
	// KindBadResponse：下游返回畸形响应或传输中途异常中断。
	KindBadResponse FailureKind = "bad_response"
	// KindCanceled：客户端主动取消，向下游传播后不产生对外错误响应。
	KindCanceled FailureKind = "canceled"
)

// UpstreamError 包装一次下游调用失败。
type UpstreamError struct {
	Kind    FailureKind
	Backend string
	Op      string
	err     error
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("lb: upstream %s on backend %s during %s: %v", e.Kind, e.Backend, e.Op, e.err)
}

func (e *UpstreamError) Unwrap() error { return e.err }

// classify 把底层错误归入可区分类别。取消优先于超时判定
// （客户端取消会同时让请求 context 与 deadline 结束）。
func classify(backend, op string, err error) *UpstreamError {
	switch {
	case errors.Is(err, context.Canceled):
		return &UpstreamError{Kind: KindCanceled, Backend: backend, Op: op, err: err}
	}

	var nerr net.Error
	if errors.As(err, &nerr) {
		if nerr.Timeout() || errors.Is(err, context.DeadlineExceeded) {
			return &UpstreamError{Kind: KindTimeout, Backend: backend, Op: op, err: err}
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &UpstreamError{Kind: KindTimeout, Backend: backend, Op: op, err: err}
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		switch {
		case errors.Is(opErr.Err, syscall.ECONNREFUSED),
			errors.Is(opErr.Err, syscall.ECONNRESET),
			errors.Is(opErr.Err, syscall.EHOSTUNREACH),
			errors.Is(opErr.Err, syscall.ENETUNREACH),
			errors.Is(opErr.Err, syscall.EPIPE):
			return &UpstreamError{Kind: KindUnreachable, Backend: backend, Op: op, err: err}
		}
	}

	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return &UpstreamError{Kind: KindBadResponse, Backend: backend, Op: op, err: err}
	}
	return &UpstreamError{Kind: KindUnreachable, Backend: backend, Op: op, err: err}
}

// IsRetryable 报告该失败是否允许在另一个健康实例上重试。
func IsRetryable(kind FailureKind) bool {
	return kind == KindUnreachable || kind == KindTimeout || kind == KindBadResponse
}
