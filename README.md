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
metrics.go   Metrics / BackendCounters：分类失败、重试、健康翻转计数
status.go    /-/status：实例明细 + 全局汇总的 JSON 输出
admin.go     AdminHandler：运行期注册/摘除的管理 API（默认 /-/admin）
mock.go      MockBackend：演练模式的可控模拟后端
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
- 并发摘除同一实例会**汇入同一次排空**，所有调用者观察到一致结果；实例未注册时返回 `ErrBackendNotFound`。
- 排空超时后实例仍已从路由表移除，后台继续等待在途收尾，后续摘除调用可汇入等待。
- **排空完成前，同名或同地址的注册会被稳定拒绝**（`ErrBackendExists` / 管理面 409）：name 和 url 在排空期间保持占用，池中绝不会出现同名或同址的重复实例；排空完成后即可正常重新注册。

## 健康状态机

每个后端只有 `healthy / unhealthy` 两态，主动探测（`HealthChecker` 定时 GET `HealthPath`）与真实请求结果（被动）都作为观测输入同一个状态机：

- 连续失败达到 `FailThreshold` → 置为 unhealthy，不再接收新请求；
- 连续成功达到 `RecoveryThreshold` → 恢复 healthy；
- 反向观测会重置当前连续计数，**单次抖动不会造成反复摘除/恢复**；
- 每次状态翻转都会打日志（`backend health state changed`, `source=active|passive`），也可通过 `GET /-/status` 查看实时状态与累计计数。

被动判定规则：传输层错误（不可达/超时/异常响应）记失败；收到响应时状态码 < 500 记成功，≥ 500 记失败（响应本身仍原样转发给客户端）。客户端取消不计为后端失败。

演练模式下注入 5xx（`/-/drill/error`）时，模拟实例的 `/healthz` 同样返回 500——即"注入期间实例整体故障"。因此主动探测与被动观测一致：**注入不取消，实例一直被判不健康、流量一直绕开，不会反复翻转**；取消注入后，主动探测连续成功达到 `RecoveryThreshold` 才重新接入。挂起（`hang`）只影响业务请求、触发超时分类；整体下线/上线（`down`/`up`）同时影响健康检查与流量，三种注入行为保持可区分。

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

## 运行期管理 API（Admin API）

网关默认在**同一监听地址**上提供管理入口，挂载在 `/-/admin`（可用 `-admin-path` 修改，`-admin=false` 关闭）。管理流量由网关自己处理，**绝不转发到任何后端，也不计入业务指标**。关闭后该路径不再特殊，按普通业务流量转发。

| 请求 | 说明 |
| --- | --- |
| `POST /-/admin/backends` | 注册实例，body：`{"name":"b3","url":"http://127.0.0.1:9003"}`（name 可省略，自动生成） |
| `GET /-/admin/backends` | 列出当前注册的实例及各自状态 |
| `DELETE /-/admin/backends/{name}` | 优雅摘除：立即停止派新请求，等在途收尾后返回；`?timeout=5s` 可覆盖本次等待上限 |

返回结果（均为 JSON，脚本可直接断言 `error` 字段）：

| 场景 | HTTP 状态 | body |
| --- | --- | --- |
| 注册成功 | 201 | 实例状态 JSON |
| URL 非法 / body 非法 | 400 | `{"error":"invalid_backend_url" / "invalid_request"}` |
| name 或 url 重复（含同名/同址实例仍在排空中） | 409 | `{"error":"duplicate_backend"}` |
| 摘除成功（排空完成） | 200 | `{"status":"drained","name":"..."}` |
| 实例不存在（含排空完成后重复摘除） | 404 | `{"error":"backend_not_found"}` |
| 排空等待超时（实例已停止接新流量，在途请求不丢，后台继续排空） | 504 | `{"error":"drain_timeout"}` |

并发摘除同一实例会汇入同一次排空，结果一致，不会重复排空或丢在途请求。排空未完成时按名字再次摘除会**汇入同一次等待**——旧在途请求未收尾前不会返回"已排空"；此期间同名或同址的注册一律 409 拒绝，不会产生重复实例。

## 指标与状态输出

`GET /-/status` 返回结构化 JSON，字段稳定：

```json
{
  "backends": [
    {
      "name": "drill-1", "url": "http://127.0.0.1:19001",
      "healthy": true, "draining": false, "in_flight": 0,
      "requests": 4, "failures": 0,
      "fail_unreachable": 0, "fail_timeout": 0, "fail_bad_response": 0,
      "health_failures": 0, "retries": 0, "health_flips": 1
    }
  ],
  "total": {
    "requests": 9, "failures": 0,
    "fail_unreachable": 0, "fail_timeout": 0, "fail_bad_response": 0,
    "health_failures": 0, "retries": 0, "health_flips": 1
  },
  "removed": {
    "requests": 0, "failures": 0,
    "fail_unreachable": 0, "fail_timeout": 0, "fail_bad_response": 0,
    "health_failures": 0, "retries": 0, "health_flips": 0
  }
}
```

字段口径：

- `requests`：路由到该实例的转发尝试次数（含重试；客户端取消的尝试已路由但未到达上游，也计在这里）；
- `failures`：**转发失败**次数（传输层错误 + 收到的 5xx 响应），恒等于 `fail_unreachable + fail_timeout + fail_bad_response`；
- `fail_unreachable` / `fail_timeout` / `fail_bad_response`：转发失败的分类计数（连不上 / 超时 / 响应异常，含上游返回 5xx），与实际失败一一对应；
- `health_failures`：**主动健康检查失败**次数——后台探测产生的失败观测的固定去处，与业务转发失败严格分开；
- `retries`：额外尝试次数（首次尝试不算重试，只按额外尝试计）；
- `health_flips`：healthy↔unhealthy 翻转次数；
- `total`：网关级累计汇总，单调不减，**包含已摘除实例**；
- `removed`：已排空完成的实例的计数合计；
- 客户端主动取消不算后端失败（不进入任何 failure 计数），管理面请求不进入任何计数。

对账方法（脚本可直接断言）：

- 每个实例：`failures == fail_unreachable + fail_timeout + fail_bad_response`；
- 全局：`total.<字段> == Σ backends.<字段> + removed.<字段>`，对每个字段都成立；
- 业务对账：`requests` 总数 == 上游实际收到的业务请求数 + 已路由但被客户端取消的尝试数。

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

## 本地演练模式（一条命令）

```bash
go run ./cmd/gateway -addr :8080 -drill -drill-backends 3 -drill-port-base 19001
```

`-drill` 会同时拉起网关和 3 个模拟后端（`127.0.0.1:19001~19003`，名称 `drill-1/2/3`）并自动注册。每个模拟实例可通过 HTTP 现场控制：

| 请求（直接打模拟实例端口） | 效果 |
| --- | --- |
| `curl -XPOST localhost:19001/-/drill/error` | 所有请求（含 `/healthz`）立即返回 500（`?enabled=false` 关闭） |
| `curl -XPOST 'localhost:19001/-/drill/hang?d=5s'` | 每个请求挂起 5s（`d=0s` 取消） |
| `curl -XPOST localhost:19001/-/drill/down` / `up` | 整体下线 / 上线（健康检查与流量都失败/恢复） |
| `curl localhost:19001/-/drill` | 查看当前演练状态 |

照着敲的完整验证流程：

```bash
# 1. 正常轮询分发
for i in 1 2 3 4; do curl -s localhost:8080/; done        # drill-1/2/3 轮换

# 2. 注入 5xx：达到阈值后 drill-1 被摘除；注入不取消就一直绕开
curl -XPOST localhost:19001/-/drill/error
for i in $(seq 1 9); do curl -s -o /dev/null localhost:8080/; done
curl -s localhost:8080/-/status                            # drill-1 healthy=false，fail_bad_response 在涨
sleep 10                                                   # 跑过多个健康检查周期
curl -s localhost:8080/-/status                            # drill-1 仍 healthy=false（不翻转）
for i in 1 2 3 4; do curl -s localhost:8080/; done         # 仍只返回 drill-2 / drill-3
curl -XPOST 'localhost:19001/-/drill/error?enabled=false'  # 取消注入
sleep 6                                                    # 主动探测连续成功达到恢复阈值
curl -s localhost:8080/-/status                            # drill-1 恢复 healthy=true，重新接入

# 2b. 指标对账：total == 各实例之和 + removed；failures == 三类失败之和
curl -s localhost:8080/-/status | python3 -c '
import json,sys
st=json.load(sys.stdin)
fields=["requests","failures","fail_unreachable","fail_timeout","fail_bad_response","health_failures","retries","health_flips"]
for b in st["backends"]:
    assert b["failures"]==b["fail_unreachable"]+b["fail_timeout"]+b["fail_bad_response"], b
for f in fields:
    assert st["total"][f]==sum(b[f] for b in st["backends"])+st["removed"][f], f
print("reconciled OK")'

# 3. 运行期摘除 drill-1 / drill-3，只留 drill-2
curl -XDELETE localhost:8080/-/admin/backends/drill-1      # {"status":"drained",...}
curl -XDELETE localhost:8080/-/admin/backends/drill-3
curl -XDELETE localhost:8080/-/admin/backends/drill-1      # 404 {"error":"backend_not_found"}

# 4. 挂起触发可区分超时（504 backend_timeout）
curl -XPOST 'localhost:19002/-/drill/hang?d=10s'
curl -s -D- -o /dev/null localhost:8080/                   # 504, X-Lb-Error: backend_timeout

# 5. 慢请求在途时摘除：等待超时单独区分，在途请求不丢
curl -s -o /dev/null localhost:8080/ &                     # 慢请求在途（挂起 10s）
curl -XDELETE 'localhost:8080/-/admin/backends/drill-2?timeout=1s'
#   → 504 {"error":"drain_timeout"}：已停止派新请求，在途请求后台继续收尾

# 5b. 排空未完成时，同名/同址重新注册被稳定拒绝，不会产生重复实例
curl -XPOST localhost:8080/-/admin/backends \
  -d '{"name":"drill-2","url":"http://127.0.0.1:19002"}'   # 409 {"error":"duplicate_backend"}
curl -s localhost:8080/-/admin/backends                    # 列表中无 drill-2，无重复实例
curl -XDELETE localhost:8080/-/admin/backends/drill-2      # 汇入同一次排空，在途收尾后才返回 drained
curl -XPOST 'localhost:19002/-/drill/hang?d=0s'

# 6. 全部下线：快速失败且错误可区分
curl -XPOST localhost:19002/-/drill/down
curl -XPOST localhost:8080/-/admin/backends \
  -d '{"name":"drill-2","url":"http://127.0.0.1:19002"}'   # 重新挂回（down 状态）
for i in $(seq 1 5); do curl -s -o /dev/null localhost:8080/; done
time curl -s -D- -o /dev/null localhost:8080/              # 立即 503, X-Lb-Error: no_healthy_backends

# Ctrl-C 干净退出（网关与所有模拟后端一起关闭）
```

## 启动参数一览

原有参数（`-addr` `-backends` `-strategy` `-fail-threshold` `-recovery-threshold` `-health-path` `-health-interval` `-max-retries` `-retry-budget`）行为不变，新增：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-drain-timeout` | `30s` | 摘除时等待在途请求的上限 |
| `-admin` | `true` | 是否开启管理 API |
| `-admin-path` | `/-/admin` | 管理 API 挂载路径 |
| `-drill` | `false` | 演练模式：同时拉起模拟后端 |
| `-drill-backends` | `3` | 演练模式模拟后端数量 |
| `-drill-port-base` | `19001` | 演练模式首个模拟后端端口 |

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
- `TestAdminRegisterListAndValidation` / `TestAdminDeregisterNotFound` / `TestAdminDeregisterDrainsInFlight` / `TestAdminDeregisterDrainTimeout`：管理面注册校验、重复/非法拒绝、在途排空、排空超时可区分；
- `TestConcurrentDeregisterSameBackend` / `TestAdminDeregisterUnderTraffic` / `TestAdminTrafficNotCounted`：并发摘除结果一致、业务流量叠加管理操作不丢请求、管理流量不进指标；
- `TestMetricsFailureClassification` / `TestMetricsRetryCounting` / `TestMetricsClientCancelNotCounted` / `TestMetricsHealthFlips` / `TestMetricsConsistencyUnderConcurrency`：失败分类、重试计数、取消不计失败、翻转计数、并发下全局与实例计数一致；
- `TestMetricsHealthCheckFailuresReconciled` / `TestMetricsReconciliationUnderConcurrency`：纯后台探测失败进入 `health_failures` 固定去处；并发压力 + 客户端取消 + 管理面流量叠加下全字段对账（`total == Σ backends + removed`、`failures == 三类之和`）；
- `TestAdminReregisterDuringDrainRejected`：排空期间同名/同址注册一律 409、不产生重复实例，按名摘除汇入排空、在途未收尾不误报已排空；
- `TestMockBackendControl` / `TestDrill*`：演练后端的现场控制与完整链路（轮询、5xx 摘除、注入期间跨多个探测周期持续绕开且不翻转、挂起超时、全下线快速失败、慢请求中摘除）。
