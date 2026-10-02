# go-http-load-balancer-go

一套只用 Go 标准库实现的本地 HTTP 反向代理 / 负载均衡网关，用于开发联调与故障演练时的流量承接。支持后端运行期注册/摘除、主动+被动健康判定、有预算的重试和可区分的失败语义。

## 整体结构

```
config.go    Config / Strategy：全部可调参数与校验
backend.go   Backend：单实例状态机（健康、摘除、在途计数），并发安全
pool.go      Pool：注册/摘除/选路（round_robin、least_connections）
health.go    HealthChecker：主动探测；与被动结果共用同一状态机
proxy.go     LoadBalancer：反向代理、错误分类、重试、取消传播
errors.go    哨兵错误 → HTTP 状态码 / 错误码映射
status.go    /-/status：池状态 JSON 输出（指标观测）
cmd/gateway/       本地启动入口
cmd/demo-backend/  演示后端（可切换健康、可注入延迟）
```

并发模型：每个 `Backend` 的健康状态、连续成功/失败计数、draining 标志都由同一把互斥锁保护；在途请求用 `WaitGroup` 计数。所有状态变更（主动探测、被动请求结果、摘除）都收敛到这一个临界区，因此并发探测与摘除不会把状态搞乱，`-race` 下验证通过。

## 注册与摘除（优雅排空）

- `Pool.Register(b)`：立即可路由。
- `Pool.Deregister(ctx, b)`：
  1. 先置 draining 标志——从这一刻起 `Acquire` 必然失败，**不会有新请求**再派发到该实例；
  2. 从路由表移除；
  3. 等待在途请求收尾（受 ctx / `DrainTimeout` 约束），在途请求**不会丢**；
  4. 排空完成后返回，超时返回 `ErrDrainTimeout`。

## 健康状态机

每个后端只有 `healthy / unhealthy` 两态，主动探测（`HealthChecker` 定时 GET `HealthPath`）与真实请求结果（被动）都作为观测输入同一个状态机：

- 连续失败达到 `FailThreshold` → 置为 unhealthy，不再接收新请求；
- 连续成功达到 `RecoveryThreshold` → 恢复 healthy；
- 反向观测会重置当前连续计数，**单次抖动不会造成反复摘除/恢复**；
- 每次状态翻转都会打日志（`backend health state changed`, `source=active|passive`），也可通过 `GET /-/status` 查看实时状态与累计计数。

被动判定规则：传输层错误（不可达/超时/异常响应）记失败；收到响应时状态码 < 500 记成功，≥ 500 记失败（响应本身仍原样转发给客户端）。客户端取消不计为后端失败。

## 失败与重试语义

失败分类（客户端收到 `X-Lb-Error` 头 + JSON 错误体，底层错误只进日志、不外泄）：

| 场景 | 错误码 | HTTP 状态 |
| --- | --- | --- |
| 无健康后端（快速失败，不挂起） | `no_healthy_backends` | 503 |
| 后端不可达（连接失败等） | `backend_unreachable` | 502 |
| 后端/预算超时 | `backend_timeout` | 504 |
| 后端响应异常（截断等） | `bad_response` | 502 |

重试规则：

- 仅传输层失败会重试；已收到响应（含 5xx）视为有效应答，原样转发；
- 仅幂等方法（GET/HEAD/OPTIONS/PUT/DELETE）重试；**POST 等非幂等请求绝不盲目重试**；
- 每次重试**必须换到其他健康实例**（本轮已失败的实例被排除）；
- 受 `MaxRetries`（次数）与 `RetryBudget`（总时间预算）双重约束，预算耗尽返回 504；
- 单次尝试受 `BackendTimeout` 约束；客户端取消会沿 `context` 传播到下游并立即终止重试。

## 可配置项

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `Strategy` | `round_robin` | `round_robin` / `least_connections` |
| `FailThreshold` | 3 | 连续失败多少次摘除 |
| `RecoveryThreshold` | 2 | 连续成功多少次恢复 |
| `HealthPath` | `/healthz` | 主动探测路径 |
| `HealthInterval` | 2s | 探测间隔 |
| `HealthTimeout` | 1s | 单次探测超时 |
| `BackendTimeout` | 5s | 单次转发超时 |
| `MaxRetries` | 2 | 最大重试次数 |
| `RetryBudget` | 15s | 单请求总时间预算 |
| `DrainTimeout` | 30s | 摘除排空上限 |

## 本地启动与验证

```bash
# 终端 1/2：起两个演示后端
go run ./cmd/demo-backend -addr :9001 -name a
go run ./cmd/demo-backend -addr :9002 -name b

# 终端 3：起网关
go run ./cmd/gateway -addr :8080 -backends http://127.0.0.1:9001,http://127.0.0.1:9002

# 验证
curl localhost:8080/            # 轮询分发到 a / b
curl localhost:9001/toggle      # 把 a 置为不健康
curl localhost:8080/-/status    # 观察 a 被摘除（healthy=false）
curl localhost:8080/            # 只走 b；全部下线时返回 503 no_healthy_backends
curl 'localhost:9001/?sleep=10' # 配合 -backend-timeout 观察 504 backend_timeout
```

## 测试

```bash
CGO_ENABLED=1 go test -race -v ./...
```

覆盖场景（`go test -v` 日志中可见各场景用例与结果）：

- `TestDeregisterDrainsInFlight`：运行期摘除时在途请求收尾、不丢请求、不再派新请求；
- `TestConcurrentRegisterDeregister`：并发注册/摘除/路由的竞态压力（`-race`）；
- `TestHealthStateMachineThresholds` / `TestActiveHealthChecker` / `TestPassiveHealthFeedback`：健康抖动不翻转、连续阈值才切换、主动+被动共同参与；
- `TestRetryFailsOverToHealthyBackend` / `TestNonIdempotentNotRetried` / `TestRetryBudgetBoundsTotalTime`：重试换实例、非幂等不重试、预算约束；
- `TestClientCancelPropagates` / `TestNoHealthyBackendsFastFail` / `TestBackendTimeoutClassified`：取消传播、快速失败、错误分类不外泄。
