package lb

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// Strategy 是负载均衡策略名。
type Strategy string

const (
	// RoundRobin 在健康实例间轮询。
	RoundRobin Strategy = "round-robin"
	// LeastConn 选择当前在途请求最少的健康实例。
	LeastConn Strategy = "least-conn"
)

// ErrBackendNotFound 在摘除不存在的实例时返回。
var ErrBackendNotFound = errors.New("lb: backend not found")

// Registry 维护全部在册后端，并负责摘除排空与实例选择。
type Registry struct {
	drainTimeout time.Duration

	mu        sync.Mutex
	backends  map[string]*Backend
	draining  map[*Backend]struct{}
	rrCounter uint64

	strategy Strategy

	transitionHook func(id string, healthy bool)
}

// NewRegistry 创建注册表。strategy 为空时使用 round-robin。
func NewRegistry(strategy Strategy, drainTimeout time.Duration) *Registry {
	if strategy == "" {
		strategy = RoundRobin
	}
	if drainTimeout <= 0 {
		drainTimeout = 30 * time.Second
	}
	return &Registry{
		backends:     make(map[string]*Backend),
		draining:     make(map[*Backend]struct{}),
		strategy:     strategy,
		drainTimeout: drainTimeout,
	}
}

// Register 注册一个实例。重复 ID 覆盖旧实例前会先把旧实例摘除排空，
// 保证不会有两条指向同一 ID 的流量并行。排空在锁外进行，
// 不阻塞其他实例的请求选择。
func (r *Registry) Register(id, u string) *Backend {
	r.mu.Lock()
	old := r.backends[id]
	b := newBackend(id, u)
	r.backends[id] = b
	if old != nil {
		old.beginDrain() // 与 Deregister 同理，临界区内封死竞态窗口
		r.draining[old] = struct{}{}
	}
	r.mu.Unlock()

	if old != nil {
		r.drainBackend(old)
	}
	return b
}

// Deregister 摘除实例：立即停止派发新请求，等待在途请求收尾后彻底移除。
// 等待有统一超时上限；超时后强制移除（在途请求自身的 ResponseWriter
// 仍由请求 goroutine 正常收尾，不会丢请求数据）。
func (r *Registry) Deregister(id string) error {
	r.mu.Lock()
	b, ok := r.backends[id]
	if !ok {
		r.mu.Unlock()
		return ErrBackendNotFound
	}
	delete(r.backends, id)
	b.beginDrain() // 必须在 registry 临界区内标记：与 pickHealthy 的占位互斥，
	// 杜绝“已从 map 删除但 drain 标志尚未置位”的窗口。
	r.draining[b] = struct{}{}
	r.mu.Unlock()

	r.drainBackend(b)
	return nil
}

func (r *Registry) drainBackend(b *Backend) {
	if !b.waitDrained(r.drainTimeout) {
		// 排空超时：在途请求仍会正常 release，不影响计数正确性。
		b.cond.Broadcast()
	}
	r.mu.Lock()
	delete(r.draining, b)
	r.mu.Unlock()
}

// pickHealthy 原子地完成“选候选 + 占位”，保证派发目标在派发时刻
// 健康、未摘除且未被本轮尝试过。选不中（占位与并发摘除竞争失败）时
// 在同一次加锁内换候选重试，绝不把请求路由到已摘除实例。
// 调用方拿到实例后必须配对调用 release。
func (r *Registry) pickHealthy(tried map[string]bool) *Backend {
	b, _, _ := r.pickHealthyChecked(tried)
	return b
}

// pickHealthyChecked 额外返回占位瞬间（registry 临界区内）的
// draining/alive，供测试验证“派发目标在派发时刻必然健康且未摘除”。
func (r *Registry) pickHealthyChecked(tried map[string]bool) (b *Backend, draining bool, alive bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for {
		candidates := make([]*Backend, 0, len(r.backends))
		for _, b := range r.backends {
			if tried != nil && tried[b.id] {
				continue
			}
			if _, draining := r.draining[b]; draining {
				continue
			}
			candidates = append(candidates, b)
		}
		// 按 ID 稳定排序：轮询起点与最少连接的平局选择都是确定的。
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].id < candidates[j].id })

		var chosen *Backend
		switch r.strategy {
		case LeastConn:
			chosen = candidates[0]
			for _, cand := range candidates[1:] {
				if cand.Inflight() < chosen.Inflight() {
					chosen = cand
				}
			}
		default:
			if len(candidates) > 0 {
				r.rrCounter++
				chosen = candidates[(r.rrCounter-1)%uint64(len(candidates))]
			}
		}
		if chosen == nil {
			return nil, false, false
		}
		if chosen.acquireLocked() {
			return chosen, chosen.drain, chosen.alive
		}
		// 占位与并发摘除竞争失败：本轮屏蔽该实例后重选。
		if tried == nil {
			tried = map[string]bool{}
		}
		tried[chosen.id] = true
	}
}

// listForProbe 返回当前在册实例快照，摘除中的实例也包含在内
// （排空期间仍可探测，便于重新上线时快速恢复；已删除的自然消失）。
func (r *Registry) listForProbe() []*Backend {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Backend, 0, len(r.backends))
	for _, b := range r.backends {
		out = append(out, b)
	}
	return out
}

func (r *Registry) setTransitionHook(h func(id string, healthy bool)) {
	r.mu.Lock()
	r.transitionHook = h
	r.mu.Unlock()
}

func (r *Registry) noteTransition(id string, healthy bool) {
	r.mu.Lock()
	h := r.transitionHook
	r.mu.Unlock()
	if h != nil {
		h(id, healthy)
	}
}

// Snapshots 返回全部在册实例的状态快照。
func (r *Registry) Snapshots() []Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Snapshot, 0, len(r.backends))
	for _, b := range r.backends {
		out = append(out, b.snapshot())
	}
	return out
}

func (r *Registry) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.backends)
}
