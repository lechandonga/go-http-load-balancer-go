package lb

import (
	"encoding/json"
	"os"
	"time"
)

// BackendSpec 描述一个初始后端。
type BackendSpec struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

// Config 是网关的全部可配置项。零值经 LoadDefaults 后可用。
type Config struct {
	Listen          string   `json:"listen"`
	AdminPathPrefix string   `json:"admin_path_prefix"`
	Strategy        Strategy `json:"strategy"`

	// 请求级失败与重试语义
	MaxAttempts      int           `json:"max_attempts"`       // 含首次，最多尝试的后端数（1 = 不重试）
	RequestTimeout   time.Duration `json:"-"`                  // 单次下游尝试超时
	TotalBudget      time.Duration `json:"-"`                  // 整次请求（含重试）总时间预算
	RetryStatusAbove int           `json:"retry_status_above"` // 下游状态码 >= 该值且幂等时重试；0 表示 500
	MaxBufferedBody  int64         `json:"max_buffered_body"`  // 幂等请求体重试所需缓冲上限（字节）

	// 健康判定
	Health HealthConfig `json:"health"`

	// 摘除
	DrainTimeout time.Duration `json:"-"`

	// 初始后端（也可全部留空，运行期经管理 API 注册）
	Backends []BackendSpec `json:"backends"`
}

// Duration 用于 JSON 中时间字段的字符串形式（如 "2s"）。
type durations struct {
	ProbeInterval string `json:"probe_interval"`
	ProbeTimeout  string `json:"probe_timeout"`
}

type configFile struct {
	Listen           string        `json:"listen"`
	AdminPathPrefix  string        `json:"admin_path_prefix"`
	Strategy         Strategy      `json:"strategy"`
	MaxAttempts      int           `json:"max_attempts"`
	RequestTimeout   string        `json:"request_timeout"`
	TotalBudget      string        `json:"total_budget"`
	RetryStatusAbove int           `json:"retry_status_above"`
	MaxBufferedBody  int64         `json:"max_buffered_body"`
	Health           HealthConfig  `json:"health"`
	HealthTiming     durations     `json:"health_timing"`
	DrainTimeout     string        `json:"drain_timeout"`
	Backends         []BackendSpec `json:"backends"`
}

// DefaultConfig 返回本地开发可用的默认配置。
func DefaultConfig() Config {
	return Config{
		Listen:           ":8080",
		AdminPathPrefix:  "/admin",
		Strategy:         RoundRobin,
		MaxAttempts:      3,
		RequestTimeout:   3 * time.Second,
		TotalBudget:      10 * time.Second,
		RetryStatusAbove: 500,
		MaxBufferedBody:  1 << 20,
		Health: HealthConfig{
			FailureThreshold: 3,
			SuccessThreshold: 2,
			ProbeInterval:    2 * time.Second,
			ProbeTimeout:     time.Second,
			ProbePath:        "/",
		},
		DrainTimeout: 30 * time.Second,
	}
}

// LoadConfig 从 JSON 文件加载配置，缺省字段取默认值。
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	var f configFile
	if err := json.Unmarshal(data, &f); err != nil {
		return cfg, err
	}
	applyFile(&cfg, f)
	return cfg, nil
}

func applyFile(cfg *Config, f configFile) {
	if f.Listen != "" {
		cfg.Listen = f.Listen
	}
	if f.AdminPathPrefix != "" {
		cfg.AdminPathPrefix = f.AdminPathPrefix
	}
	if f.Strategy != "" {
		cfg.Strategy = f.Strategy
	}
	if f.MaxAttempts > 0 {
		cfg.MaxAttempts = f.MaxAttempts
	}
	if d := parseDur(f.RequestTimeout); d > 0 {
		cfg.RequestTimeout = d
	}
	if d := parseDur(f.TotalBudget); d > 0 {
		cfg.TotalBudget = d
	}
	if f.RetryStatusAbove > 0 {
		cfg.RetryStatusAbove = f.RetryStatusAbove
	}
	if f.MaxBufferedBody > 0 {
		cfg.MaxBufferedBody = f.MaxBufferedBody
	}
	if f.Health.FailureThreshold > 0 {
		cfg.Health.FailureThreshold = f.Health.FailureThreshold
	}
	if f.Health.SuccessThreshold > 0 {
		cfg.Health.SuccessThreshold = f.Health.SuccessThreshold
	}
	if f.Health.ProbePath != "" {
		cfg.Health.ProbePath = f.Health.ProbePath
	}
	if d := parseDur(f.HealthTiming.ProbeInterval); d > 0 {
		cfg.Health.ProbeInterval = d
	}
	if d := parseDur(f.HealthTiming.ProbeTimeout); d > 0 {
		cfg.Health.ProbeTimeout = d
	}
	if d := parseDur(f.DrainTimeout); d > 0 {
		cfg.DrainTimeout = d
	}
	if len(f.Backends) > 0 {
		cfg.Backends = f.Backends
	}
}

func parseDur(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

// MarshalJSON 输出含可读时长的配置，便于示例文档与调试。
func (c Config) MarshalJSON() ([]byte, error) {
	f := configFile{
		Listen:           c.Listen,
		AdminPathPrefix:  c.AdminPathPrefix,
		Strategy:         c.Strategy,
		MaxAttempts:      c.MaxAttempts,
		RequestTimeout:   c.RequestTimeout.String(),
		TotalBudget:      c.TotalBudget.String(),
		RetryStatusAbove: c.RetryStatusAbove,
		MaxBufferedBody:  c.MaxBufferedBody,
		Health:           c.Health,
		HealthTiming: durations{
			ProbeInterval: c.Health.ProbeInterval.String(),
			ProbeTimeout:  c.Health.ProbeTimeout.String(),
		},
		DrainTimeout: c.DrainTimeout.String(),
		Backends:     c.Backends,
	}
	return json.MarshalIndent(f, "", "  ")
}
