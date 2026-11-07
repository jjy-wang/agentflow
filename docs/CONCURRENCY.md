# CONCURRENCY —— 高并发设计

## 并发点清单

| # | 并发点 | 难点 | 实现要点 |
|---|---|---|---|
| C1 | 万级 SSE 长连接 | 慢客户端拖垮写缓冲 | WebFlux 异步 + 心跳 + 每租户配额 + 有界缓冲 + 超时踢除 |
| C2 | RPM/TPM 双维度限流 | token 要等响应才知道 | 固定窗口 + 令牌桶 + 预扣结算 |
| C3 | 模型舱壁 | 单个慢模型拖垮全局 | 每模型 Semaphore + 超时预算 + 熔断 |
| C4 | 背压 | 上游吐 token 快于下游消费 | Reactor 背压 + 有界缓冲 + 分级丢弃策略 |
| C5 | 慢请求不占线程 | MVC 一连接一线程 | WebFlux 事件循环，线程与连接解耦 |
| C6 | 长任务不占连接 | Agent 跑 30 秒 | 异步 run + 事件订阅分离 + 状态机 |
| C7 | 多实例无状态 | 会话可能落任意实例 | 状态全落 PG/Redis |
| C8 | 突发流量 | 瞬间打满配额 | 本地预检 + Redis 精确计数 |
| C9 | 缓存击穿 | 同会话并发首包 | 单飞 + 逻辑过期 |
| C10 | 队列积压 | 模型限流后任务堆积 | 有界队列 + 优先级 + 水位告警 |
| C11 | 同轮工具调用 | 多工具并行与顺序语义冲突 | `flatMap(maxConcurrency)` 有界并行 + 下标排序回灌 + 失败降级 |

## 连接层

- **线程与连接解耦**：WebFlux + Netty 事件循环，1 万 SSE 连接由少量事件循环线程承载，连接数 ≠ 线程数。压测见 `loadtest/sse_10k.js`。
- **应用层心跳**：每 15 秒一条 `ping` 事件（id 固定为 0，不进入 `Last-Event-ID` 单调序列），防止中间网关静默断连；Netty 连接级 `idle-timeout=0` 关闭，空闲治理由应用层掌控。
- **每租户连接配额**：`SseConnectionRegistry` 在订阅时 `open(tenantId)`、结束时 `close(tenantId)`，单租户上限 200（`agentflow.stream.max-connections-per-tenant`），超限拒绝，防单一租户挤爆实例。
- **慢客户端治理**：每条连接独立有界写缓冲（`agentflow.stream.write-buffer-size=256`），溢出时显式 `DROP_OLDEST`——只丢这条连接最旧的未消费事件，保住新事件时效，不影响其他连接；连续丢弃超过 `agentflow.stream.max-lag-events`（默认 200）主动断连，丢弃数计 `sse.events.dropped.total`、断连数计 `sse.connections.lagged_closed.total`。无界缓冲在 1 万连接下必然 OOM，这是硬性红线。
- **兜底超时**：SSE 流整体 `timeout(3min)`（墙钟 120s + 余量），异常路径不允许悬挂连接。

## 限流

两级结构，可用性优先：

1. **本地预检**（`LocalRateLimiter`）：每个实例一个本地令牌桶，容量 = 全局额度 / 实例数 × 1.2（`local-factor`）。Redis 不可用时自动降级为纯本地近似限流——宁可近似、不可全挂。
2. **Redis 精确限流**（`QuotaService` + Lua）：
   - **RPM**：固定窗口计数；
   - **TPM**：令牌桶，"读-判断-扣减"在 `quota_reserve.lua` / `quota_settle.lua` 中原子完成，时间取 Redis `TIME`，避免多实例时钟漂移把补充速率算错；
   - **预扣-结算**：请求前按 `prompt 估算 + maxTokens` 预扣，响应结束在 `doFinally` 按实际用量结算退回多扣部分。预扣估多只是短时保守，估少会被 429，所以按 `maxTokens` 上限预扣；漏结算会导致配额永久减少，因此结算必须挂在 `doFinally` 而非正常路径。
   - 超限返回错误码 `-32005`（HTTP 429），计 `quota.rejected.total{model, reason}`。

### 本地预检与分布式限流的分工

`LocalRateLimiter` 是**单机内存结构**，定位是"预检"而非"最终权威"：

| 维度 | 本地预检（LocalRateLimiter） | 分布式限流（QuotaService + Redis Lua） |
|---|---|---|
| 作用 | 挡住明显超量、降低 Redis 压力、Redis 故障时降级近似限流 | 全局精确配额，多实例共享同一份额度 |
| 数据 | 单实例内存令牌桶，进程重启即丢失 | Redis 键，多实例一致 |
| 结论强度 | 通过 ≠ 一定放行；拒绝 ≠ 全局额度耗尽 | 通过才真正扣减；拒绝即全局额度不足 |
| 错误码 | `RATE_LIMITED`（HTTP 429 / -32005） | `QUOTA_EXCEEDED`（HTTP 429 / -32005） |

因此线上排查必须看错误码区分：`RATE_LIMITED` 是单实例预检拒绝（可能是本地桶被抽干或该实例流量偏斜），`QUOTA_EXCEEDED` 才是全局额度真的不足，`MODEL_BUSY` 是舱壁满/模型冷却，`NO_MODEL_AVAILABLE` 只表示路由确实找不到已注册模型。

### 令牌申请-归还语义

本地令牌桶遵循"申请-归还"契约，与 Redis 的预扣-结算同构：

- `tryAcquire` 返回 `true` 即视为一次成功申请，调用方必须在**任何未完成真实调用的终止路径**归还：下游 5xx/超时、上下文超限、舱壁拒绝、客户端取消（`ON_CANCEL`）、预扣后流尚未订阅就被取消。
- `tryAcquire` 返回 `false` 时没有扣减，**严禁退款**；退款只能由预扣成功的那次申请发起。
- 成功调用按实际用量结算：`reserved - actual` 的预扣余量同样归还本地桶（与 Redis `settle` 同构），避免成功请求长期按 `maxTokens` 占用本地额度、慢性抽干令牌桶。
- `LlmInvoker` 用 `try/finally` 语义的 `doFinally`（成功/失败/取消都触发）加 `onErrorResume` 双保险归还，`LocalRefund` 的原子标记保证同一次预扣**只退一次**，重复回调（超时 + 取消 + doFinally）不会把桶灌爆。
- 退款以桶容量为硬上限截断：即使重复退款，可用令牌也不会超过容量。未扣减过的 key 不会凭空建桶。
- 压测中 81% 请求报 `NO_MODEL_AVAILABLE` 的根因就是"只扣不退"：高并发失败路径把本地桶抽干后，后续请求全部被预检拒绝，而错误码又误归为"没有可用模型"。修复后失败请求会归还令牌，桶具备自愈能力。

可观测指标（标签只用固定枚举，模型名来自配置、租户/runId 禁入）：

| 指标 | 含义 |
|---|---|
| `llm.local.limiter.available.tokens{model}` | 当前本地桶可用令牌数（预检余量，不是全局配额） |
| `llm.local.limiter.rejected.total{model,reason=INSUFFICIENT_TOKENS}` | 本地预检拒绝次数 |
| `llm.quota.refunded.total{model}` | 预扣归还次数，用于核对扣减/退款是否对称 |
| `llm.quota.rejected.total{model,scope=local 或 redis}` | 配额拒绝，scope 区分本地预检与 Redis 精确限流 |

## 背压

- Reactor 操作符链天然背压：下游 `Flux` 的 request 量沿链路上溯到模型流式读取。
- 写向 SSE 客户端前每条连接各有有界缓冲（256）；缓冲满时按 `DROP_OLDEST` 丢最旧事件并补发滞后提示，详见下节。
- `loadtest/backpressure.js` 以 500 tps 的上游快吐验证：200 个慢读连接下实例不 OOM、丢帧计数可见。

### 慢消费者隔离（有界缓冲 + 丢弃最旧 + 回放兜底）

`RunEventBus.subscribe` 为每个订阅建立独立的 `onBackpressureBuffer(write-buffer-size, DROP_OLDEST)`：慢连接只在自己这一份缓冲里丢最旧事件，共享 sink 与其他连接不受影响；sink 侧的共享缓冲仅服务内部落库订阅，客户端在 sink 处即声明无界需求，因此不会再被慢客户端打满。

丢弃发生后，该连接在下一条事件前会收到一条 `stream.lagged` 提示（`{"lagged":true,"lastEventId":<已投递到的id>}`），客户端据此带 `Last-Event-ID` 重连即可从 PG 回放补齐缺口——**丢事件是可恢复的**，因为事件发布时已双写 `t_run_event` 且回放窗口覆盖 run 全量事件。连续丢弃超过 `max-lag-events`（默认 200）则主动断开该连接并记日志，避免长期占用内存与 CPU；正常客户端消费速度跟得上，不经过额外缓冲、事件顺序与内容完全不变。

| 方案 | 慢客户端影响 | 内存 | 可恢复性 |
|---|---|---|---|
| 无界缓冲 | 慢连接无限堆积，拖垮实例 | 随连接数/时长无界增长，1 万连接下必然 OOM | 不丢事件，但实例先挂 |
| 直接断流 | 一有积压就断开，抖动即断连 | 恒定 | 依赖客户端识别断流并重连，体验差、易误伤 |
| 有界缓冲 + DROP_OLDEST + 回放（当前） | 只丢该连接最旧事件，新事件时效保留；超阈值才断连 | 每连接上限为 `write-buffer-size` | 事件已落 PG，`stream.lagged` + `Last-Event-ID` 回放补齐，可恢复降级 |

## 舱壁

`ModelBulkhead` 为每个模型一把 `Semaphore`（`mock-large=64`、`mock-small=128`，见 `application.yml`）：某模型变慢/抖动时最多占满自己的信号量，不会拖垮其他模型与全局事件循环。配合每模型超时预算（`timeout-ms`、`ttft-timeout-ms`）与 Resilience4j 熔断，慢模型被快速失败隔离。舱壁拒绝直接计指标并返回可重试错误，不在信号量上无界排队。

### 自适应舱壁（AIMD）

写死并发的痛点：上限按"上游最好的那一天"配置，上游退化时 64 个并发全部压在慢模型上，TTFT 与错误率螺旋上升；按"最差的那一天"配置又常年浪费吞吐。`agentflow.llm.models.<name>.adaptive-concurrency: true` 开启 AIMD 模式（默认关闭，保持写死并发原行为）：

- **加性增**：每集满一个成功窗口（窗口大小 = 当前动态上限）上限 +1，封顶 `max-concurrency`；
- **乘性减**：出现超时/5xx，或 TTFT 超过 `adaptive-ttft-threshold-ms`（默认 3000ms），上限立即 ×0.7，下保底 1；
- **信号来源**：`LlmInvoker.doFinally` 按终止信号回传——`ON_ERROR → failure()`、`ON_COMPLETE → success(ttftMs)`、取消只释放不调整（客户端断开不算上游退化）；
- **线程安全**：动态上限、`inFlight`、窗口计数均为 `AtomicInteger`，准入用 CAS 闸门（`inFlight < limit` 才放行），配置上限的信号量做硬兜底，`Permit` 释放幂等；
- **初始值**：`max(1, max/4)` 的保守起点，仿 TCP 拥塞窗口的慢启动爬坡思路。

| 对比项 | 写死并发 | AIMD 自适应 |
|---|---|---|
| 上游健康 | 跑满配置上限 | 爬坡后同样封顶配置上限 |
| 上游退化 | 满并发继续压，雪崩放大 | 单次失败 ×0.7 快速收缩，给上游恢复空间 |
| 退化恢复后 | 无需动作 | 按窗口 +1 缓慢恢复，避免回弹过猛 |
| 抖动误判 | 无 | 偶发失败会误降一档，但窗口爬坡廉价，可接受 |

**验证方法**：
```bash
mvn -q test -Dtest=ModelBulkheadAimdTest   # 加性增/乘性减边界、TTFT 超阈值退化、拒绝不排队
# 运行期观测：对开启 adaptive-concurrency 的模型用 fault-injection 打 5xx，
# 观察 llm.error.total 上升后新请求快速被拒绝（MODEL_BUSY），恢复后吞吐缓慢回升
```

## 金丝雀灰度模型路由

`agentflow.llm.canary`（`enabled/from/to/weight-percent/sticky-by-tenant`）把首选为 `from` 的流量按权重切到 `to`：粘性模式（默认）按 `tenantId` 哈希模 100 分桶，同一租户多轮对话始终落同一模型；无租户或关闭粘性时每请求随机分桶。命中计 `llm.canary.hit.total{from,to}`（标签只放模型名，租户 ID 禁入标签）。灰度只替换首选，兜底候选逻辑不变。

## 工具级并发（有界并行）

模型一轮返回多个工具调用时，`ReActLoop` 不再用 `Flux.concat` 串行跑，而是：

- **有界策略**：`Flux.fromIterable(monos).flatMap(m -> m, maxConcurrency)`，`maxConcurrency` 来自 `agentflow.tool.parallel.max-concurrency`（默认 4），且强制 ≥1。上限的意义是隔离下游——工具通常打 PG/HTTP，无界并行会把连接池与三方接口一起打穿；`agentflow.tool.parallel.enabled=false` 时退回 `Flux.concat`，保留旧行为可回退。
- **顺序语义**：`tool.call` 事件按模型原始顺序先全部发出；`tool.result` 按完成顺序发（前端靠 `callId` 关联）；每个结果携带原始下标，整轮结束后排序写回上下文，保证模型侧消息顺序稳定、可复现。
- **失败隔离**：单个工具错误 `onErrorResume` 成失败结果回灌，只有全部失败才抛 `TOOL_ALL_FAILED`，避免一个慢/坏工具拖垮整轮。
- **共享状态**：并行前顺序完成重复调用检测与 `incToolCall()`；`RunContext` 的计数用 `AtomicInteger`、指纹表用 `ConcurrentHashMap.merge`、`emit` 加锁，事件 `seq` 保持单调。
- **线程模型**：工具 handler 仍在 `ToolExecutor` 的 `boundedElastic` 上执行，事件循环线程不做阻塞调用。

**压测方法**：

```bash
# 1) 单测验证并发上限：5 个可控延迟的假工具、maxConcurrency=2，断言在途峰值 == 2
mvn -q test -Dtest=ReActLoopParallelToolsTest
# 2) 集成压测：复用 loadtest/baseline.js 的请求模板，让 mock 模型每轮返回 4 个 tool_call，工具固定 200ms 延迟
#    串行基线：一轮 ≈ 800ms；并行后应接近 200~400ms，观察 tool.call.duration P95 与 PG 连接池 active
k6 run loadtest/baseline.js
curl -s localhost:8080/actuator/prometheus | grep -E 'tool_call_duration|hikaricp_connections_active'
# 3) 观察在途峰值不越界：apply max-concurrency 2/4/8 三档，确认 connections_active 不超过对应上限
```

## 线程模型

- **事件循环线程**（Netty）：只跑非阻塞逻辑。禁止在事件循环上做 JDBC、`Thread.sleep`、`.block()`。
- **boundedElastic**：MyBatis/JDBC 等阻塞调用统一调度到 boundedElastic，并带租户上下文的 Reactor Context 传播（禁止 ThreadLocal 传租户）。
- **恢复任务**：`RunRecoveryJob` 定时轮询（5s），批量 50 条 `SKIP LOCKED` 抢占，执行在独立调度器上，与请求链路隔离。
- **RabbitMQ 消费者**：手动 ACK、`prefetch=1`、并发 2~4，重试由业务代码控制，禁止 `basicNack(requeue=true)` 死循环重投。

## 容量

容量公式与实测代入见 `docs/CAPACITY.md`，核心结论：

- 单实例 SSE 连接上限受内存与文件句柄约束，1 万连接场景需 `ulimit -n 65535`；
- 单实例 run 吞吐受模型舱壁信号量（64）与 TPM 配额（200k tokens/min）双重约束，先到先限；
- PG 连接池 rw/ro 各 20，两实例合计 80 < PG `max_connections=300`，留有管理余量；
- Redis Lettuce 连接池 max-active 32，令牌桶 Lua 单次 RTT < 1ms，不是瓶颈。
