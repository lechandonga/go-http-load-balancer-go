# go-http-load-balancer-go

一套只用 Go 标准库实现的本地 HTTP 反向代理 / 负载均衡网关，用于开发联调与故障演练时的流量承接。支持后端运行期注册/摘除、主动+被动健康判定、有预算的重试、可区分的失败语义、分类指标和一键本地演练。

本轮新增：

- **运行期管理面**：网关自身提供 HTTP 管理入口，不重启即可挂新实例、摘已有实例，摘除带优雅排空与独立超时；
- **分类指标**：按实例输出不可达/超时/响应异常/5xx、重试次数、健康翻转次数和全局汇总，结构化 JSON；
- **本地演练模式**：一条命令同时拉起网关和多个可现场注入故障的模拟后端，Ctrl-C 干净退出。

## 整体结构

```
config.go    Config / Strategy：全部可调参数与校验
backend.go   Backend：单实例状态机（健康、摘除、在途计数），并发安全
pool.go      Pool：按名注册/摘除/选路（round_robin、least_connections），摘除共享排空
health.go    HealthChecker：主动探测；与被动结果共用同一状态机
proxy.go     LoadBalancer：反向代理、错误分类、重试、取消传播
errors.go    哨兵错误 → HTTP 状态码 / 错误码映射
metrics.go   全局指标聚合（仅业务流量；管理面与探针不计入）
status.go    /-/status：池状态 + 分类指标 JSON
admin.go     /-/admin：运行期注册/摘除/查看管理面
cmd/gateway/       本地启动入口
cmd/demo-backend/  演示后端（可切换健康、可注入延迟）
internal/drill/    演练模式的可控模拟后端
```

并发模型：每个 `Backend` 的健康状态、连续成功/失败计数、draining 标志都由同一把互斥锁保护；在途请求用 `WaitGroup` 计数。所有状态变更（主动探测、被动请求结果、摘除）都收敛到这一个临界区，因此并发探测与摘除不会把状态搞乱，`-race` 下验证通过。指标全部用原子计数器，高并发下不丢计数，读取不需要锁。

## 运行期管理面

管理面默认挂在业务监听地址的 `/-/admin/` 下，**无需额外配置**，curl 直接可用。它由网关自己处理，请求绝不转发到后端，也不进入任何业务指标。默认同时开启 `/-/status` 指标文档。

| 操作 | 方法与路径 | 说明 |
| --- | --- | --- |
| 列出实例 | `GET /-/admin/backends` | 含健康/排空/在途状态与每实例指标 |
| 查看单个 | `GET /-/admin/backends/{name}` | 不存在返回 404 |
| 注册实例 | `POST /-/admin/backends` | JSON 体：`{"name":"b1","url":"http://127.0.0.1:9001"}`；`name` 可省略，按 host 自动取名 |
| 摘除实例 | `DELETE /-/admin/backends/{name}?timeout=30s` | 立即停新流量并优雅排空；`timeout` 可选，默认取 `-drain-timeout` |

返回结果（均为 JSON，状态码稳定、可脚本断言）：

| HTTP | `result` / `error` | 含义 |
| --- | --- | --- |
| 200 | `result: "ok"` / `"deregistered"` | 查询成功 / 已摘除且排空完成 |
| 201 | `result: "registered"` | 新实例已注册并立即参与路由 |
| 400 | `invalid_json` / `invalid_request` / `invalid_backend` | 体不是 JSON、缺 url、地址 scheme 非 http(s)、缺 host 等 |
| 404 | `backend_not_found` | 摘除/查看的名字不存在，或已排空移除（重复摘除得到同样结果） |
| 405 | `method_not_allowed` | 方法不支持 |
| 409 | `backend_exists` | 同名或同 URL 已注册（含正在排空的实例）；池子状态不变 |
| 504 | `drain_timeout` | `timeout` 内未排空；实例已彻底移出路由，排空在后台继续，管理请求不会被挂住 |

摘除语义：

- DELETE 一开始就置 draining 标志，从此该实例**不会再收到任何新请求**，然后等待在途请求全部收尾；
- 在途请求正常处理完，**不会被掐断或丢失**；
- 对**同一个实例并发摘除只会发生一次排空**：所有调用方共享同一个排空过程、各自独立等待，结果一致；超时的调用方拿到 504，之后排空完成时新的调用方拿到 200（名字已彻底移除则拿到 404）；
- 排空完成后实例才从名单消失；排空期间 `GET` 仍能看到它 `draining=true` 与 `in_flight` 的变化。

管理面路径可改、可关，避免与业务路由冲突：

- `-admin-path /ops/` 改路径；必须以 `/` 开头且不能是 `/`，启动时校验；
- `-status-path /metrics` 改指标文档路径；
- `-admin-path ''`、`-status-path ''` 分别关闭对应入口；关闭后这些路径作为普通业务路径处理（会被转发）。

## 注册与摘除（优雅排空）

- `Pool.Register(b)`：启动参数里的后端注册，立即可路由。
- `Pool.RegisterBackend(b)`：管理面使用的注册，校验名字与 URL 不重复；冲突返回 `ErrBackendAlreadyExists`，非法输入返回错误且池子不变。
- `Pool.Deregister(ctx, b)`：
  1. 先置 draining 标志——从这一刻起 `Acquire` 必然失败，**不会有新请求**再派发到该实例；
  2. 从路由表移除；
  3. 等待在途请求收尾（受 ctx / `DrainTimeout` 约束），在途请求**不会丢**；
	4. 排空完成后返回，超时返回 `ErrDrainTimeout`。
- `Pool.DeregisterByName(ctx, name)`：管理面实际调用的入口。重复摘除共享同一次排空；名字不存在（或已排空移除）返回 `ErrBackendNotFound`。

## 健康状态机

每个后端只有 `healthy / unhealthy` 两态，主动探测（`HealthChecker` 定时 GET `HealthPath`）与真实请求结果（被动）都作为观测输入同一个状态机：

- 连续失败达到 `FailThreshold` → 置为 unhealthy，不再接收新请求；
- 连续成功达到 `RecoveryThreshold` → 恢复 healthy；
- 反向观测会重置当前连续计数，**单次抖动不会造成反复摘除/恢复**；
- 每次状态翻转都会打日志（`backend health state changed`, `source=active|passive`），也可通过 `GET /-/status` 查看实时状态与累计计数。

被动判定规则：传输层错误（不可达/超时/异常响应）记失败；收到响应时状态码 < 500 记成功，≥ 500 记失败（响应本身仍原样转发给客户端）。客户端取消不计为后端失败。

演练模拟后端在注入 5xx 时，其 `/healthz` 也会返回失败，因此被动请求和主动探针都会推动摘除，不会互相“续命”。

## 指标（分类可观测）

`GET /-/status`（可用 `-status-path` 改路径或关闭）返回稳定的结构化 JSON：

```json
{
  "backends": [
    {
      "name": "mock-1",
      "url": "http://127.0.0.1:19001",
      "healthy": true,
      "draining": false,
      "in_flight": 0,
      "metrics": {
        "requests": 12,
        "failures": 3,
        "unreachable": 1,
        "timeout": 1,
        "bad_response": 0,
        "response_5xx": 1,
        "retries": 2,
        "flips_to_healthy": 1,
        "flips_to_unhealthy": 1,
        "health_flips": 2
      }
    }
  ],
  "total": { "requests": 0, "failures": 0, "unreachable": 0, "timeout": 0,
             "bad_response": 0, "response_5xx": 0, "retries": 0,
             "flips_to_healthy": 0, "flips_to_unhealthy": 0, "health_flips": 0 }
}
```

字段口径（字段名稳定，脚本可直接断言）：

- `requests`：真正派发到该实例的尝试次数（每次重试落到的实例各计一次；主动探针、管理面请求不计）；
- `unreachable` / `timeout` / `bad_response` / `response_5xx`：四类业务失败各自的次数，`failures` 恒等于四者之和；
- `retries`：**额外尝试**次数（第一次尝试不计重试）；
- `flips_to_healthy` / `flips_to_unhealthy`：健康状态两个方向的翻转次数，`health_flips` 为两者之和；
- `total`：所有实例的同口径全局汇总。

计数与实际转发严格对齐：客户端自己取消只记 `requests`、不计任何失败；管理面与指标请求完全不进这些计数；5xx 响应照常转发的同时计入 `response_5xx`。所有计数均为原子操作，并发压力下精确一致（单测验证多 worker 压测后每实例之和等于全局总数）。

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

启动参数（旧参数与默认值全部保留，行为不变）：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-admin-path` | `/-/admin/` | 管理面前缀，置空关闭 |
| `-status-path` | `/-/status` | 指标文档路径，置空关闭 |
| `-drain-timeout` | `30s` | 管理面摘除默认等待上限 |
| `-health-timeout` | `1s` | 单次探测超时（对应 `HealthTimeout`） |
| `-backend-timeout` | `5s` | 单次转发超时（对应 `BackendTimeout`） |
| `-drill` | 关 | 演练模式：同进程拉起模拟后端 |
| `-drill-backends` | `3` | 模拟后端数量 |
| `-drill-ports` | 随机端口 | 模拟后端固定端口，逗号分隔，数量须与 `-drill-backends` 一致 |

既有参数 `-addr`、`-backends`、`-strategy`、`-fail-threshold`、`-recovery-threshold`、`-health-path`、`-health-interval`、`-max-retries`、`-retry-budget` 的含义与默认值均未改变；不加任何新参数的旧启动方式照旧可用。

## 本地演练模式

一条命令同时启动网关和多个可现场控制的模拟后端，不用再开多个终端：

```bash
go run ./cmd/gateway -addr :8080 -drill -drill-backends 3
# 或使用固定端口：
go run ./cmd/gateway -addr :8080 -drill -drill-backends 3 -drill-ports 19001,19002,19003
```

启动日志会打印每个模拟实例的地址和全部控制 URL。控制接口均为 `POST`：

| 动作 | 示例 |
| --- | --- |
| 查看状态与计数 | `curl -X POST http://127.0.0.1:19001/control/state` |
| 立即返回 5xx | `curl -X POST 'http://127.0.0.1:19001/control/fail5xx?on=1'`（`?on=0` 取消；注入期间 `/healthz` 也失败） |
| 挂起指定时长 | `curl -X POST 'http://127.0.0.1:19001/control/hang?d=3s'`（`?d=0` 取消；健康探针路径不受影响） |
| 整体下线 | `curl -X POST http://127.0.0.1:19001/control/down`（`/healthz` 返回 503） |
| 重新上线 | `curl -X POST http://127.0.0.1:19001/control/up` |

照着敲即可复现的完整验证流程（用较短超时让现象确定）：

```bash
# 1) 启动
go run ./cmd/gateway -addr :8080 -drill -drill-backends 3 -drill-ports 19001,19002,19003 \
  -backend-timeout 1s -max-retries 0 -health-interval 30s

# 2) 正常轮询：多敲几次，响应在 mock-1 / mock-2 / mock-3 间轮转
for i in 1 2 3 4 5 6; do curl -s localhost:8080/; done

# 3) 注入 5xx：达到 fail-threshold 后该实例被摘掉，之后流量全部绕开
curl -s -X POST 'http://127.0.0.1:19001/control/fail5xx?on=1'
for i in $(seq 1 10); do curl -s -o /dev/null -w '%{http_code} ' localhost:8080/; done; echo
curl -s localhost:8080/-/status    # mock-1 healthy=false，metrics.response_5xx 增加

# 4) 挂起触发可区分的超时错误
curl -s -X POST 'http://127.0.0.1:19002/control/hang?d=3s'
curl -s -D - -o /dev/null localhost:8080/   # 504，X-Lb-Error: backend_timeout
curl -s -X POST 'http://127.0.0.1:19002/control/hang?d=0'

# 5) 全部下线：快速失败且错误可区分
for p in 19001 19002 19003; do curl -s -X POST http://127.0.0.1:$p/control/down; done
curl -s -D - -o /dev/null localhost:8080/   # 503，X-Lb-Error: no_healthy_backends

# 6) 对正在处理慢请求的实例执行摘除：慢请求正常收尾，之后该实例收不到新流量
for p in 19001 19002 19003; do curl -s -X POST http://127.0.0.1:$p/control/up; done
curl -s -X POST 'http://127.0.0.1:19003/control/hang?d=1s'
( sleep 0.2; curl -s -X DELETE 'localhost:8080/-/admin/backends/mock-3?timeout=5s' ) &
for i in 1 2 3 4; do curl -s localhost:8080/; done; wait
# DELETE 等到慢请求收尾后返回 {"result":"deregistered"}；之后多敲几次只有 mock-1 / mock-2

# 7) 现场挂新实例 / 摘已有实例（不用重启网关）
curl -s -X POST localhost:8080/-/admin/backends -d '{"name":"extra","url":"http://127.0.0.1:19003"}'
curl -s -X DELETE localhost:8080/-/admin/backends/extra
```

`Ctrl-C`（SIGINT/SIGTERM）时网关先停止接收新连接、等待在途请求，然后关闭全部模拟后端，干净退出。

## 本地启动与验证

```bash
# 方式一：演练模式（推荐，一条命令）
go run ./cmd/gateway -addr :8080 -drill -drill-backends 3

# 方式二：手动起演示后端（旧方式，照旧可用）
go run ./cmd/demo-backend -addr :9001 -name a
go run ./cmd/demo-backend -addr :9002 -name b

# 终端 3：起网关
go run ./cmd/gateway -addr :8080 -backends http://127.0.0.1:9001,http://127.0.0.1:9002

# 验证
curl localhost:8080/                          # 轮询分发到 a / b
curl localhost:9001/toggle                   # 把 a 置为不健康
curl localhost:8080/-/status                 # 观察 a 被摘除（healthy=false）与分类指标
curl localhost:8080/-/admin/backends         # 管理面列出实例
curl localhost:8080/                         # 只走 b；全部下线时返回 503 no_healthy_backends
curl 'localhost:9001/?sleep=10'             # 配合 -backend-timeout 观察 504 backend_timeout
```

## 测试

```bash
CGO_ENABLED=1 go test -race -v ./...
```

覆盖场景（`go test -v` 日志中可见各场景用例与结果）：

- `TestAdminRegister`：管理面注册（201）、重复名/URL（409）、非法地址/坏 JSON（400）、缺名自动取名，且任何失败都不污染池子；
- `TestAdminDeregisterDrains`：4 个并发 DELETE 共享一次排空，在途请求正常 200 收尾、全部得到一致结果；
- `TestAdminDeregisterNotFound`：摘除/查看不存在实例稳定 404，重复摘除结果一致；
- `TestAdminDrainTimeout`：排空超限时管理请求按 `?timeout` 快速返回 504，不再派新流量，后台继续排空；
- `TestAdminChurnUnderTraffic`：管理面持续增删叠加 8 路业务流量，排空完成后的实例在静默窗口内收到 0 个请求；
- `TestDeregisterDrainsInFlight`：运行期摘除时在途请求收尾、不丢请求、不再派新请求；
- `TestConcurrentRegisterDeregister`：并发注册/摘除/路由的竞态压力（`-race`）；
- `TestHealthStateMachineThresholds` / `TestActiveHealthChecker` / `TestPassiveHealthFeedback`：健康抖动不翻转、连续阈值才切换、主动+被动共同参与；
- `TestRetryFailsOverToHealthyBackend` / `TestNonIdempotentNotRetried` / `TestRetryBudgetBoundsTotalTime`：重试换实例、非幂等不重试、预算约束；
- `TestClientCancelPropagates` / `TestNoHealthyBackendsFastFail` / `TestBackendTimeoutClassified`：取消传播、快速失败、错误分类不外泄。
- `TestMetricsFailureClasses`：不可达/超时/5xx 各自进独立计数并与全局汇总一致；
- `TestMetricsRetries`：重试只算额外尝试，首试不计；
- `TestMetricsClientCancelExcluded`：客户端取消不计后端失败；
- `TestMetricsHealthFlips`：健康翻转每实例与全局各计一次；
- `TestStatusDocumentAndAdminExcluded`：状态 JSON 字段集合稳定，管理面/指标请求不进业务计数；
- `TestMetricsConcurrentConsistency`：8×100 并发压测后每实例之和精确等于全局总数；
- `TestDrillEndToEnd`：演练全链路——轮询分发、5xx 阈值摘除绕流、挂起 504、全下线 503、慢请求中摘除且之后零流量、管理面恢复实例。
