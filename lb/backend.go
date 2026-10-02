package lb

import (
	"sync"
	"sync/atomic"
	"time"
)

// Backend 表示一个被网关代理的后端实例及其全部运行期状态。
//
// 并发约定：
//   - mu 保护 drain / alive / 健康连击计数 / inflight；
//   - inflight 同时提供原子读取，供 least-conn 策略无锁比较；
//   - cond 在 inflight 归零时广播，摘除流程借此等待在途请求收尾。
type Backend struct {
	id  string
	url string

	mu              sync.Mutex
	cond            *sync.Cond
	drain           bool
	alive           bool
	inflight        atomic.Int64
	healthyStreak   int
	unhealthyStreak int
	totalProbes     uint64
	totalRequests   uint64
	totalFailures   uint64
	bornAt          time.Time
	lastChangeAt    time.Time
	lastHealthyAt   time.Time
	lastUnhealthyAt time.Time
}

func newBackend(id, u string) *Backend {
	b := &Backend{
		id:           id,
		url:          u,
		alive:        true, // 新注册实例乐观视为健康，主动探测会立即校验
		bornAt:       time.Now(),
		lastChangeAt: time.Now(),
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// ID 返回后端唯一标识。
func (b *Backend) ID() string { return b.id }

// URL 返回后端基础地址。
func (b *Backend) URL() string { return b.url }

// Alive 返回当前健康状态。
func (b *Backend) Alive() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.alive
}

// Draining 返回是否处于摘除排空阶段。
func (b *Backend) Draining() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.drain
}

// Inflight 返回当前在途请求数。
func (b *Backend) Inflight() int64 { return b.inflight.Load() }

// acquireLocked 是 acquire 的锁内实现。
func (b *Backend) acquireLocked() bool {
	if b.drain || !b.alive {
		return false
	}
	b.inflight.Add(1)
	return true
}

// acquire 尝试为一个新请求占位。摘除中或不健康的实例会被拒绝，
// 因此请求永远不会被派发到已摘除 / 不健康的实例。
func (b *Backend) acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.acquireLocked()
}

// release 在请求结束时归还占位；归零时唤醒等待排空的摘除流程。
func (b *Backend) release() {
	if b.inflight.Add(-1) == 0 {
		b.mu.Lock()
		b.cond.Broadcast()
		b.mu.Unlock()
	}
}

// beginDrain 将实例标记为摘除中：此后 acquire 一律失败（停止新请求），
// 但已有在途请求继续执行。
func (b *Backend) beginDrain() {
	b.mu.Lock()
	b.drain = true
	b.mu.Unlock()
}

// waitDrained 阻塞直到在途请求全部收尾；超时返回 false。
func (b *Backend) waitDrained(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.inflight.Load() > 0 {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		timer := time.AfterFunc(remaining, b.cond.Broadcast)
		b.cond.Wait()
		timer.Stop()
	}
	return true
}

// Snapshot 是后端状态的只读视图，供管理接口与测试使用。
type Snapshot struct {
	ID              string    `json:"id"`
	URL             string    `json:"url"`
	Alive           bool      `json:"alive"`
	Draining        bool      `json:"draining"`
	Inflight        int64     `json:"inflight"`
	HealthyStreak   int       `json:"healthy_streak"`
	UnhealthyStreak int       `json:"unhealthy_streak"`
	TotalRequests   uint64    `json:"total_requests"`
	TotalProbes     uint64    `json:"total_probes"`
	TotalFailures   uint64    `json:"total_failures"`
	BornAt          time.Time `json:"born_at"`
	LastChangeAt    time.Time `json:"last_change_at"`
	LastHealthyAt   time.Time `json:"last_healthy_at,omitempty"`
	LastUnhealthyAt time.Time `json:"last_unhealthy_at,omitempty"`
}

func (b *Backend) snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Snapshot{
		ID:              b.id,
		URL:             b.url,
		Alive:           b.alive,
		Draining:        b.drain,
		Inflight:        b.inflight.Load(),
		HealthyStreak:   b.healthyStreak,
		UnhealthyStreak: b.unhealthyStreak,
		TotalRequests:   b.totalRequests,
		TotalProbes:     b.totalProbes,
		TotalFailures:   b.totalFailures,
		BornAt:          b.bornAt,
		LastChangeAt:    b.lastChangeAt,
		LastHealthyAt:   b.lastHealthyAt,
		LastUnhealthyAt: b.lastUnhealthyAt,
	}
}
