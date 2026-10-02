# Go HTTP 反向代理 / 负载均衡网关（仅标准库）

一个零第三方依赖、可在本地直接运行的 HTTP 反向代理 / 负载均衡网关，
面向开发与故障演练场景：后端实例可以频繁上下线，网关在并发下的
摘除、健康切换、重试行为是**确定且可解释**的。

- 仅使用 Go 标准库（`net/http`、`net/http/httputil` 未使用，转发自行实现以精确控制重试）
- 不需要任何外部服务或凭据
- 未修改 `go.mod` 的 module 声明与 Go 版本

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `lb/backend.go` | 后端实例状态：在途计数、摘除排空（`sync.Cond`）、状态快照 |
| `lb/registry.go` | 注册表：运行期注册 / 摘除、`round-robin` 与 `least-conn` 选实例 |
| `lb/health.go` | 健康状态机（阈值去抖）+ 并发主动探测 |
| `lb/proxy.go` | 反向代理：转发、幂等判定、预算内换实例重试、取消传播、hop-by-hop 头剥离 |
| `lb/errors.go` | 下游失败分类（不可达 / 超时 / 响应异常 / 取消）与状态码映射 |
| `lb/admin.go` | 管理 API：注册、摘除、状态、指标、健康探针 |
| `lb/metrics.go` | 进程内计数器，文本指标输出 |
| `lb/config.go` | 配置加载与默认值 |
| `lb/gateway.go` | 组装与统一启停 |
| `cmd/lbgateway/main.go` | 本地启动入口，`-demo` 一键故障演练 |
| `configs/example.json` | 配置示例 |

## 健康状态机

每个后端只有两个对外状态：`healthy`（接收流量）与 `unhealthy`（不接收新请求）。
状态切换由两个独立的“连击计数”决定，**主动探测和真实请求结果都参与**：

- `healthy -> unhealthy`：连续失败达到 `failure_threshold`（默认 3）。
- `unhealthy -> healthy`：连续成功达到 `success_threshold`（默认 2）。
  不健康实例收不到真实流量，因此恢复只能由主动探测贡献。
- 任意一次相反结果都会把连击计数清零；**单次抖动只改变计数，不切换状态**，
  因此不会出现一次超时就摘除、一次成功就恢复的反复横跳。
- 主动探测对在册实例**并发**发起，每轮独立 `WaitGroup`；状态更新在
  `Backend` 自身的锁内串行化，并发探测与并发摘除不会把状态搞乱。
- 新注册实例乐观视为健康，监控器启动后立即探测一轮尽快纠正。
- 状态切换通过结构化日志（`health transition` / `backend marked unhealthy` /
  `backend recovered`）和 `/admin/metrics`、`/admin/backends` 可见。

## 摘除与在途请求

摘除（`DELETE /admin/backends/{id}` 或配置替换）是**优雅摘除**：

1. 在注册表临界区内把实例移出候选集并标记 `draining`，与“选实例 + 占位”互斥，
   因此摘除生效后不可能再有新请求派发到该实例，也不存在“已删除但未标记”的竞态窗口。
2. 阻塞等待该实例的在途请求全部收尾（`inflight` 归零，`sync.Cond` 广播），
   受 `drain_timeout`（默认 30s）约束；在途请求自身继续正常完成，**不丢请求**。
3. 排空期间若所有健康实例都不可用，新请求立即得到 `503 no healthy backend`，不挂起。

## 失败分类与重试语义

下游失败被归为四类，且**不向客户端透出底层错误原文**：

| 类别 | 触发情形 | 对外状态码 |
| --- | --- | --- |
| `unreachable` | 连接被拒 / 连接被重置 / 主机或网络不可达 | 502 |
| `timeout` | 建连或等响应超过单次尝试超时 / 总预算 | 504 |
| `bad_response` | 下游响应畸形、传输中途异常中断 | 502 |
| `canceled` | 客户端主动取消 | 不写错误响应（连接已撤销） |

重试规则：

- 仅**幂等方法**自动重试：`GET`、`HEAD`、`PUT`、`DELETE`、`OPTIONS`、`TRACE`。
  `POST`、`PATCH` 等**非幂等请求绝不盲目重放**，即使收到 5xx 也只透传一次。
- 下游状态码 `>= retry_status_above`（默认 500）的幂等请求也会重试。
- 每次重试都换到**另一个健康实例**（本轮已尝试实例进入 `tried` 集合，不会重复）。
- 同时受 `max_attempts`（默认 3，含首次）与总时间预算 `total_budget`（默认 10s）、
  单次尝试超时 `request_timeout`（默认 3s）约束。
- 幂等请求体最多缓冲 `max_buffered_body`（默认 1 MiB）以便重放；超限则放弃重试资格。
- 所有后端都不健康 / 都已尝试：**快速失败**，503 + 固定 JSON 错误体，不挂起。
- 客户端取消通过 `http.NewRequestWithContext` 直接传播到下游请求 context；
  总预算到期返回 504。

## 可配置项

见 `configs/example.json`：

| 配置 | 说明 | 默认 |
| --- | --- | --- |
| `listen` | 监听地址 | `:8080` |
| `admin_path_prefix` | 管理 API 前缀 | `/admin` |
| `strategy` | `round-robin` 或 `least-conn` | `round-robin` |
| `max_attempts` | 最大尝试后端数（含首次，1 = 不重试） | 3 |
| `request_timeout` | 单次下游尝试超时 | 3s |
| `total_budget` | 整请求总时间预算 | 10s |
| `retry_status_above` | 触发重试的最低下游状态码（幂等时） | 500 |
| `max_buffered_body` | 可缓冲的幂等请求体上限（字节） | 1048576 |
| `health.failure_threshold` | 连续失败摘除阈值 | 3 |
| `health.success_threshold` | 连续成功恢复阈值 | 2 |
| `health.probe_path` | 主动探测路径 | `/` |
| `health_timing.probe_interval` | 探测间隔 | 2s |
| `health_timing.probe_timeout` | 单次探测超时 | 1s |
| `drain_timeout` | 摘除排空等待上限 | 30s |
| `backends` | 启动时的初始后端（也可全留空，运行期注册） | 空 |

## 本地启动与手工验证

需要 Go 1.25+（`go.mod` 未改动）。

一键故障演练（自动在进程内拉起 3 个后端，无需任何外部服务）：

```bash
go run ./cmd/lbgateway -demo
```

启动后网关监听 `:8080`，演练指南会打印到 stderr。常用命令：

```bash
# 观察轮询分发，响应里的 backend 字段在 demo-1/2/3 间轮换
curl -s http://localhost:8080/foo
curl -s http://localhost:8080/bar

# 查看实例健康状态与指标
curl -s http://localhost:8080/admin/backends | python3 -m json.tool
curl -s http://localhost:8080/admin/metrics

# 优雅摘除 demo-3：先停新请求，等待在途收尾
curl -s -X DELETE http://localhost:8080/admin/backends/demo-3

# 重新注册（PORT 换成启动日志里 demo-3 的实际端口）
curl -s -X POST http://localhost:8080/admin/backends \
  -d '{"id":"demo-3","url":"http://127.0.0.1:PORT"}'
```

每个 demo 后端还提供故障注入开关（端口见启动日志）：

```bash
curl -s -X POST http://127.0.0.1:PORT/control/fail   # 之后返回 500
curl -s -X POST http://127.0.0.1:PORT/control/hang   # 之后挂起（演练超时 / 取消）
curl -s -X POST http://127.0.0.1:PORT/control/ok     # 恢复
```

使用自己的后端 / 配置文件：

```bash
go run ./cmd/lbgateway -config configs/example.json
go run ./cmd/lbgateway -listen :9090   # 命令行覆盖监听地址
```

### 管理 API

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| `GET` | `/admin/backends` | 列出实例及健康 / 在途状态 |
| `POST` | `/admin/backends` | 注册或替换，body：`{"id":"b1","url":"http://..."}` |
| `DELETE` | `/admin/backends/{id}` | 优雅摘除（停新请求 + 等待在途收尾） |
| `GET` | `/admin/metrics` | 文本指标（请求数、失败分类、重试原因、快速失败数） |
| `GET` | `/admin/healthz` | 至少一个健康实例时返回 200，否则 503 |

## 测试

测试按“场景 -> 用例”组织，`-v` 日志中每个用例都有
`[SCENARIO 名称] CASE ==> ...` 与 `CASE <== PASS ...` 记录，
可以直接看出覆盖了哪些场景、各用例及结果：

- 运行期摘除时的在途请求（阻塞排空、不丢请求、摘除中零新路由、移除后零路由）
- 并发摘除 / 注册 / 选实例竞态（派发瞬间健康且未摘除）
- 健康抖动（未达阈值不摘除 / 不恢复、失败清零恢复连击、探测端到端摘除）
- 重试与失败分类（幂等换实例、POST 不重试、502/504 区分、错误不透出、全不可用快速失败）
- 客户端取消向下游传播
- 管理 API 注册 / 列举 / 摘除 / 指标

开启竞态检测运行：

```bash
CGO_ENABLED=1 go test -race -count=1 -v ./...
```
