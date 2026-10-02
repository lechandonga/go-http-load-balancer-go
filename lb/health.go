package lb

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// HealthConfig 是健康判定的去抖阈值与探测参数。
//
// 状态切换规则（核心去抖语义）：
//   - 健康   -> 不健康：连续 FailureThreshold 次失败（探测或真实请求）；
//   - 不健康 -> 健康：  连续 SuccessThreshold 次成功（仅主动探测，因为不健康
//     的实例不会收到真实请求）；
//   - 单次抖动只改变连击计数，不触发摘除/恢复，避免反复横跳。
type HealthConfig struct {
	FailureThreshold int           `json:"failure_threshold"`
	SuccessThreshold int           `json:"success_threshold"`
	ProbeInterval    time.Duration `json:"-"`
	ProbeTimeout     time.Duration `json:"-"`
	ProbePath        string        `json:"probe_path"`
}

func (c HealthConfig) withDefaults() HealthConfig {
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 3
	}
	if c.SuccessThreshold <= 0 {
		c.SuccessThreshold = 2
	}
	if c.ProbeInterval <= 0 {
		c.ProbeInterval = 2 * time.Second
	}
	if c.ProbeTimeout <= 0 {
		c.ProbeTimeout = time.Second
	}
	if c.ProbePath == "" {
		c.ProbePath = "/"
	}
	return c
}

// observe 记录一次健康观测结果（ok=true 成功，false 失败）。
// fromProbe 标明来源：真实请求失败与主动探测失败等价计数；
// 成功观测在实例已不健康时只有主动探测才能带来恢复计数。
// 返回值表示健康状态是否发生了切换。
func (b *Backend) observe(ok, fromProbe bool, cfg HealthConfig, now time.Time) (transitioned bool, alive bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ok {
		if !b.alive && !fromProbe {
			return false, b.alive // 不健康实例收不到真实流量；防御性忽略
		}
		b.healthyStreak++
		b.unhealthyStreak = 0
		if !b.alive && b.healthyStreak >= cfg.SuccessThreshold {
			b.alive = true
			b.lastHealthyAt = now
			b.lastChangeAt = now
			return true, true
		}
		return false, b.alive
	}

	b.unhealthyStreak++
	b.healthyStreak = 0
	// 只有当前健康的实例才可能因连续失败被摘除；已经不健康时，
	// 失败只维持/重置连击，不产生重复状态变化。
	if b.alive && b.unhealthyStreak >= cfg.FailureThreshold {
		b.alive = false
		b.lastUnhealthyAt = now
		b.lastChangeAt = now
		return true, false
	}
	return false, b.alive
}

// healthMonitor 周期性并发探测所有在册实例。
type healthMonitor struct {
	registry *Registry
	cfg      HealthConfig
	client   *http.Client
	log      *slog.Logger

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func newHealthMonitor(r *Registry, cfg HealthConfig, log *slog.Logger) *healthMonitor {
	return &healthMonitor{
		registry: r,
		cfg:      cfg.withDefaults(),
		client: &http.Client{
			Timeout: cfg.withDefaults().ProbeTimeout,
		},
		log: log,
	}
}

func (m *healthMonitor) start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.wg.Add(1)
	go m.loop(ctx)
}

func (m *healthMonitor) stop() {
	m.cancel()
	m.wg.Wait()
}

func (m *healthMonitor) loop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.ProbeInterval)
	defer ticker.Stop()
	m.probeAll(ctx) // 启动后立即探测一轮，尽快纠正乐观健康
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.probeAll(ctx)
		}
	}
}

// probeAll 对当前在册实例并发发起一轮探测；每轮独立 WaitGroup，
// 摘除/注册与探测并发进行也不会让计数错位（observe 在 Backend 锁内串行化）。
func (m *healthMonitor) probeAll(ctx context.Context) {
	backends := m.registry.listForProbe()
	var wg sync.WaitGroup
	for _, b := range backends {
		wg.Add(1)
		go func(b *Backend) {
			defer wg.Done()
			m.probeOne(ctx, b)
		}(b)
	}
	wg.Wait()
}

func (m *healthMonitor) probeOne(ctx context.Context, b *Backend) {
	reqCtx, cancel := context.WithTimeout(ctx, m.cfg.ProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, b.url+m.cfg.ProbePath, nil)
	if err != nil {
		m.record(b, false)
		return
	}
	resp, err := m.client.Do(req)
	if err != nil {
		m.record(b, false)
		return
	}
	defer resp.Body.Close()
	resp.Body.Read(make([]byte, 1024)) // 触发连接复用所需的最小读取
	m.record(b, resp.StatusCode >= 200 && resp.StatusCode < 400)
}

func (m *healthMonitor) record(b *Backend, ok bool) {
	b.mu.Lock()
	b.totalProbes++
	b.mu.Unlock()

	transitioned, alive := b.observe(ok, true, m.cfg, time.Now())
	if transitioned {
		if alive {
			m.log.Info("backend recovered",
				"backend", b.id, "url", b.url,
				"success_threshold", m.cfg.SuccessThreshold)
		} else {
			m.log.Warn("backend marked unhealthy",
				"backend", b.id, "url", b.url,
				"failure_threshold", m.cfg.FailureThreshold)
		}
		m.registry.noteTransition(b.id, alive)
	}
}
