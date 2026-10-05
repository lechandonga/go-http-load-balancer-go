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

## 健康状态机

每个后端只有 `healthy / unhealthy` 两态，主动探测（`HealthChecker` 定时 GET `HealthPath`）与真实请求结果（被动）都作为观测输入同一个状态机：

- 连续失败达到 `FailThreshold` → 置为 unhealthy，不再接收新请求；
- 连续成功达到 `RecoveryThreshold` → 恢复 healthy；
- 反向观测会重置当前连续计数，**单次抖动不会造成反复摘除/恢复**；
- 每次状态翻转都会打日志（`backend health state changed`, `source=active|passive`），也可通过 `GET /-/status` 查看实时状态与累计计数。

被动判定规则：传输层错误（不可达/超时/异常响应）记失败；收到响应时状态码 < 500 记成功，≥ 500 记失败（响应本身仍原样转发给客户端）。客户端取消不计为后端失败。

### 5xx 演练注入期间的语义（重要）

演练模式对模拟实例 `POST /-/drill/error` 表示**整个实例**处于故障态：注入期间它不仅对业务请求返回 500，其主动健康检查端点 `/healthz` 同样返回 500。因此：

- 注入未取消、实例持续返回 5xx 期间，主动探测也一直失败，实例会**一直保持 unhealthy、流量持续绕开**，不会出现"摘除—几秒后又被判回健康"的翻转；
- 只有 `?enabled=false` 取消注入后，`/healthz` 才恢复正常，实例再按 `RecoveryThreshold`（连续成功次数）正常重新接入；
- 状态翻转只可能由"连续失败达阈值 / 连续成功达阈值"引起，注入期间 `health_flips` 保持为摘除时的那一次；
- 挂起（`/-/drill/hang`，仅业务请求挂起、健康检查正常）与整体下线/上线（`/-/drill/down|up`）的可区分行为不变：挂起仍只触发可区分的超时分类，下线则健康检查与业务流量同时失败、上线后按恢复阈值回来。

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
| name 或 url 与在册实例重复 | 409 | `{"error":"duplicate_backend"}` |
| 同名或同址实例仍在排空中（在途请求尚未收尾） | 409 | `{"error":"duplicate_backend","detail":"... already registered or still draining ..."}` |
| 摘除成功（排空完成） | 200 | `{"status":"drained","name":"..."}` |
| 实例不存在（含排空完成后重复摘除） | 404 | `{"error":"backend_not_found"}` |
| 排空等待超时（实例已停止接新流量，在途请求不丢，后台继续排空） | 504 | `{"error":"drain_timeout"}` |

并发摘除同一实例会汇入同一次排空，结果一致，不会重复排空或丢在途请求。

排空期间重复注册的语义（防止"旧实例还在收尾、同名新实例已进池"）：

- 摘除一旦开始，该实例的 **name 与 url 都被占用**，直到在途请求全部收尾、排空真正完成；期间任何同名或同址的 `POST /backends` 一律稳定返回 **409 `duplicate_backend`**（同名不同址、同址不同名都拒绝），池子里不会出现第二个同名或同址实例；
- 排空超时（504）只是"等待"超时，路由早已停止、后台仍在收尾；这段时间同样拒绝重复注册，等后台排空完成后才允许重新注册；
- 排空中再次按名字 `DELETE` 不会摘到任何新实例，而是**汇入旧实例那次排空**，必须等旧的在途请求真正收尾后才返回 200，不会在慢请求仍在跑时误报已排空；
- 只有排空彻底完成后，同名/同址才能重新注册（返回 201），注册成功后立即再注册一次仍会因重复被 409 拒绝。

## 指标与状态输出

`GET /-/status` 返回结构化 JSON，字段稳定：

```json
{
  "backends": [
    {
      "name": "drill-1", "url": "http://127.0.0.1:19001",
      "healthy": true, "draining": false, "retired": false, "in_flight": 0,
      "requests": 4, "failures": 0,
      "fail_unreachable": 0, "fail_timeout": 0, "fail_bad_response": 0,
      "retries": 0, "health_flips": 1
    }
  ],
  "total": {
    "requests": 9, "failures": 0,
    "fail_unreachable": 0, "fail_timeout": 0, "fail_bad_response": 0,
    "retries": 0, "health_flips": 1
  }
}
```

字段口径：

- `requests`：转发到该实例的尝试次数（含重试，不含主动健康检查）；`total.requests` 为全局尝试总数；
- `failures`：进入健康状态机的失败观测总数，**既包括业务转发失败也包括主动探测失败**；
- `fail_unreachable` / `fail_timeout` / `fail_bad_response`：失败观测的三类计数，每个失败观测恰好落入其中一类——连不上（dial/connection 失败）/ 超时（deadline、读超时、主动探测超时）/ 响应异常（收到 5xx、响应头或响应体非法/截断）。主动探测失败也有稳定去处：探测 5xx/异常归入 `fail_bad_response`，探测超时归入 `fail_timeout`，探测连不上归入 `fail_unreachable`；
- 每个实例恒有 `failures == fail_unreachable + fail_timeout + fail_bad_response`，全局亦然；
- `retries`：额外尝试次数（首次尝试不算重试）；
- `health_flips`：healthy↔unhealthy 翻转次数；
- `draining`：正在摘除排空中（已不接新流量、在途请求收尾中）；`retired`：排空已彻底完成、永不再路由，但其最终计数仍保留在文档里；
- 状态文档列出**在路由、排空中、已摘除（retired）**三类实例；`total` 是网关级累计汇总，单调不减，且**恒等于文档中所有实例（含 retired）各字段之和**——脚本可直接用 `backends` 求和与 `total` 对账；
- 业务转发次数与失败次数与实际发生一一对应：实例 `requests` 等于真正派发到它的尝试数；5xx/截断等失败响应即使原样转发（客户端可能已先收到 200 状态行），也仍是一条失败观测，计入 `failures` 与 `fail_bad_response`；
- 客户端主动取消不算后端失败，管理面请求不进入任何计数。

对账方法（照着敲，用 `jq`）：

```bash
s=$(curl -s localhost:8080/-/status)
echo "$s" | jq '.backends | map(.failures) | add == (.total.failures)'          # true
echo "$s" | jq '.backends | all(.failures == .fail_unreachable + .fail_timeout + .fail_bad_response)'  # true
echo "$s" | jq -e '.total == (.backends | map({requests: .requests, failures: .failures, fail_unreachable: .fail_unreachable, fail_timeout: .fail_timeout, fail_bad_response: .fail_bad_response, retries: .retries, health_flips: .health_flips}) | add // {requests:0,failures:0,fail_unreachable:0,fail_timeout:0,fail_bad_response:0,retries:0,health_flips:0})' >/dev/null && echo totals-match
```

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
| `curl -XPOST localhost:19001/-/drill/error` | 所有请求立即返回 500（`?enabled=false` 关闭） |
| `curl -XPOST 'localhost:19001/-/drill/hang?d=5s'` | 每个请求挂起 5s（`d=0s` 取消） |
| `curl -XPOST localhost:19001/-/drill/down` / `up` | 整体下线 / 上线（健康检查与流量都失败/恢复） |
| `curl localhost:19001/-/drill` | 查看当前演练状态 |

照着敲的完整验证流程：

```bash
# 1. 正常轮询分发
for i in 1 2 3 4; do curl -s localhost:8080/; done        # drill-1/2/3 轮换

# 2. 注入 5xx：达到阈值后 drill-1 被摘除，流量绕开
curl -XPOST localhost:19001/-/drill/error
for i in $(seq 1 9); do curl -s -o /dev/null localhost:8080/; done
curl -s localhost:8080/-/status                            # drill-1 healthy=false, health_flips=1
curl -s localhost:8080/                                    # 只返回 drill-2 / drill-3
# 等若干个健康检查周期（默认 2s，可等 10s 以上），drill-1 仍然 healthy=false，
# 业务请求持续只落 drill-2/drill-3；/healthz 在注入期间同样返回 500，不会被探测复活
curl -s localhost:8080/-/status | jq '.backends[] | select(.name=="drill-1") | {healthy,health_flips,fail_bad_response}'
curl -XPOST localhost:19001/-/drill/error?enabled=false    # 取消注入
sleep 5                                                    # 连续探测成功达到恢复阈值后重新接入

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
curl -XPOST 'localhost:19002/-/drill/hang?d=0s'

# 6. 全部下线：快速失败且错误可区分
curl -XPOST localhost:19002/-/drill/down
curl -XPOST localhost:8080/-/admin/backends \
  -d '{"name":"drill-2","url":"http://127.0.0.1:19002"}'   # 重新挂回（down 状态）
for i in $(seq 1 5); do curl -s -o /dev/null localhost:8080/; done
time curl -s -D- -o /dev/null localhost:8080/              # 立即 503, X-Lb-Error: no_healthy_backends

# Ctrl-C 干净退出（网关与所有模拟后端一起关闭）
```

## 三块修复的复现验证流程

先起演练网关：

```bash
go run ./cmd/gateway -addr :8080 -drill -drill-backends 3 -drill-port-base 19001
```

### 1. 5xx 注入后持续摘除、流量持续绕开

```bash
curl -XPOST localhost:19001/-/drill/error                     # 对 drill-1 注入 500
for i in $(seq 1 9); do curl -s -o /dev/null localhost:8080/; done   # 触发失败阈值
curl -s localhost:8080/-/status | jq '.backends[] | select(.name=="drill-1") | .healthy'  # false
# 跑过 5 个以上健康检查周期（默认 2s/次），期间持续打业务请求：
sleep 10
for i in $(seq 1 12); do curl -s localhost:8080/; done         # 全部来自 drill-2/drill-3，无 drill-1
curl -s localhost:8080/-/status | jq '.backends[] | select(.name=="drill-1") | {healthy,health_flips}'
#   → {"healthy":false,"health_flips":1}：注入期间不翻转、不复活
curl -XPOST 'localhost:19001/-/drill/error?enabled=false'      # 取消注入
sleep 5
curl -s localhost:8080/-/status | jq '.backends[] | select(.name=="drill-1") | .healthy'  # true：按恢复阈值回来
```

### 2. 指标对账（业务、后台探测、并发、取消与管理面隔离）

```bash
# 2.1 业务 + 5xx 注入后，全局汇总等于各实例之和、实例失败数等于分类之和
curl -XPOST localhost:19001/-/drill/error
for i in $(seq 1 20); do curl -s -o /dev/null localhost:8080/; done
s=$(curl -s localhost:8080/-/status)
echo "$s" | jq '.backends | all(.failures == .fail_unreachable + .fail_timeout + .fail_bad_response)'  # true
echo "$s" | jq '.total.failures == ([.backends[].failures] | add)'                                     # true

# 2.2 只让后台健康检查跑、无任何业务请求：把一个实例整体下线
curl -XPOST 'localhost:19001/-/drill/error?enabled=false'      # 先清掉 5xx 注入
curl -XPOST localhost:19002/-/drill/down
sleep 3                                                        # 只有主动探测在跑
s=$(curl -s localhost:8080/-/status)
echo "$s" | jq '.backends[] | select(.name=="drill-2") | {requests,failures,fail_bad_response}'
#   → requests=0，failures>0 且全部落在 fail_bad_response（探测 503）；total 与实例数相等
echo "$s" | jq '.total.failures == ([.backends[].failures] | add)'                                    # true
curl -XPOST localhost:19002/-/drill/up

# 2.3 管理面流量不混入业务指标
before=$(curl -s localhost:8080/-/status | jq -c .total)
for i in $(seq 1 10); do curl -s -o /dev/null localhost:8080/-/admin/backends; done
after=$(curl -s localhost:8080/-/status | jq -c .total)
[ "$before" = "$after" ] && echo admin-not-counted

# 2.4 全字段对账（含已摘除实例 retired）
curl -s -XDELETE localhost:8080/-/admin/backends/drill-3
curl -s localhost:8080/-/status | jq -e '
  .total == (.backends | map({requests,failures,fail_unreachable,fail_timeout,fail_bad_response,retries,health_flips})
             | add // {requests:0,failures:0,fail_unreachable:0,fail_timeout:0,fail_bad_response:0,retries:0,health_flips:0})' \
  >/dev/null && echo totals-match
```

（客户端取消不计失败由自动化用例 `TestMetricsClientCancelNotCounted` 与 `TestMetricsReconciliationUnderConcurrency` 覆盖；并发压测下的不丢计数、对账一致由后者保证。）

### 3. 排空期间重复注册被拒、按名摘除不误报排空

为让慢请求在网关侧稳定保持在途（不被重试/超时干扰），用一个独立的**单后端**演练网关（端口与第 1、2 节的网关不冲突）。需要两个终端。终端 A：

```bash
go run ./cmd/gateway -addr :8081 -drill -drill-backends 1 -drill-port-base 19101 -max-retries 0 -drain-timeout 30s
```

终端 B（挂起 3s——小于默认 5s 后端超时，慢请求会真实在途；发出后立即摘除）：

```bash
curl -XPOST 'localhost:19101/-/drill/hang?d=3s'
curl -s localhost:8081/ -o /tmp/slow.out &
sleep 0.5
curl -s localhost:8081/-/status | jq '.backends[0].in_flight'           # 1，慢请求在途

# 摘除 drill-1：只等 1s，在途还没收尾，返回 504 drain_timeout
curl -s -w '\nHTTP %{http_code}\n' -XDELETE 'localhost:8081/-/admin/backends/drill-1?timeout=1s'

# 旧实例还在收尾：同名/同址注册一律 409，不产生重复实例
curl -s -o /dev/null -w '%{http_code}\n' -XPOST localhost:8081/-/admin/backends \
  -d '{"name":"drill-1","url":"http://127.0.0.1:19101"}'                 # 409
curl -s -o /dev/null -w '%{http_code}\n' -XPOST localhost:8081/-/admin/backends \
  -d '{"name":"drill-1","url":"http://127.0.0.1:1"}'                     # 409（同名不同址）
curl -s -o /dev/null -w '%{http_code}\n' -XPOST localhost:8081/-/admin/backends \
  -d '{"name":"other","url":"http://127.0.0.1:19101"}'                   # 409（同址不同名）
curl -s localhost:8081/-/admin/backends | jq '[.[].name]'                # []，在路由表无重复
curl -s localhost:8081/-/status | jq '[.backends[] | select(.name=="drill-1")] | length'  # 1（仅排空中那一个）

# 再按名摘除：汇入旧排空，必须等在途请求收尾（约再等 2s）才回 200，不会立刻误报 drained
time curl -s -XDELETE localhost:8081/-/admin/backends/drill-1

# 排空彻底完成后才能重新注册
curl -s -o /dev/null -w '%{http_code}\n' -XPOST localhost:8081/-/admin/backends \
  -d '{"name":"drill-1","url":"http://127.0.0.1:19101"}'                 # 201
curl -s -o /dev/null -w '%{http_code}\n' -XPOST localhost:8081/-/admin/backends \
  -d '{"name":"drill-1","url":"http://127.0.0.1:19101"}'                 # 409（重复）
```

终端 A 收尾：

```bash
curl -XPOST 'localhost:19101/-/drill/hang?d=0s'
# Ctrl-C 停掉 :8081 网关
```

---

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
- `TestMockBackendControl` / `TestDrill*`：演练后端的现场控制与完整链路（轮询、5xx 摘除、挂起超时、全下线快速失败、慢请求中摘除）。

本轮修复新增的覆盖：

- `TestInjected5xxStaysEvictedAcrossProbeRounds`：5xx 注入后跨 20+ 个主动探测周期仍保持摘除、业务流量持续绕开，取消注入后才按恢复阈值重新接入；
- `TestMetricsReconciliationUnderConcurrency`：12 路并发业务压力 + 客户端取消 + 管理面流量交织下，全局 total 与所有实例（含已摘除）逐字段求和相等、实例失败数等于自身三类之和、计数与真实命中一一对应、取消与管理面不入账；
- `TestActiveProbeFailuresReconcileWithoutTraffic`：实例整体下线、只有后台健康检查在跑、零业务请求时，探测失败有稳定分类（`fail_bad_response`）且全局与实例对账相等；
- `TestDrainRejectsDuplicateUntilInFlightFinished`：慢请求在途时摘除，同名/同址/同名不同址/同址不同名注册全部 409 且不产生重复实例，第二次按名摘除汇入旧排空、在途未收尾时不误报已排空，排空完成后才允许重新注册；
- `TestDrainTimeoutStillRejectsDuplicateRegistration`：排空 504 超时后的后台收尾期同样拒绝重复注册，收尾完成后才接受。
