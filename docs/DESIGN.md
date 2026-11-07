# DESIGN —— AgentFlow 总体设计

## 架构图

```
                          ┌──────────────────────────────────────────────┐
  客户端 ──SSE/HTTP──────►│              AgentFlow 应用实例 ×2            │
                          │                                              │
                          │  api/        AgentController / Approval /    │
                          │              RunStatus （WebFlux, 鉴权 Filter)│
                          │  run/        RunService · 状态机 · 租约 ·    │
                          │              RunRecoveryJob (SKIP LOCKED)    │
                          │  agent/      ReActLoop / PlanExecuteLoop ·   │
                          │              ContextBuilder · LoopGuard      │
                          │  tool/       ToolRegistry · ToolExecutor ·   │
                          │              JSON Schema 校验 · 幂等          │
                          │  llm/        ModelRouter · ModelRegistry ·   │
                          │              OpenAiCompatibleClient          │
                          │  quota/      本地预检 + Redis Lua 令牌桶      │
                          │              （预扣-结算）+ 模型信号量舱壁     │
                          │  stream/     RunEventBus · EventReplayStore ·│
                          │              SseConnectionRegistry           │
                          │  memory/     SessionMemory · VectorMemory    │
                          │  audit/      审计哈希链 · 敏感字段脱敏         │
                          └──────┬───────────┬────────────┬─────────────┘
                                 │           │            │
                        ┌────────▼───┐ ┌─────▼─────┐ ┌────▼─────────┐
                        │ PostgreSQL │ │  Redis 7  │ │  mock-llm    │
                        │ + pgvector │ │ 令牌桶/   │ │ (压测专用    │
                        │ 状态/记忆/ │ │ 事件回放/ │ │  流式模型)   │
                        │ 审计/租约  │ │ 缓存      │ └──────────────┘
                        └────────────┘ └───────────┘
                                 │
                        ┌────────▼───┐
                        │ RabbitMQ   │  报表等长耗时任务异步化
                        └────────────┘
```

## 模块职责

| 包 | 职责 | 关键类 |
|---|---|---|
| `api` | 对外 REST/SSE 入口，参数校验，统一响应 | `AgentController`、`ApprovalController`、`RunStatusController` |
| `auth` | API Key 鉴权、租户上下文（Reactor Context，非 ThreadLocal） | `ApiKeyAuthWebFilter`、`TenantContext` |
| `run` | run 生命周期：状态机、租约、恢复、审批、取消 | `RunService`、`RunRecoveryJob`、`RunLeaseService`、`ApprovalService` |
| `agent` | Agent 执行引擎：ReAct / Plan-Execute 双模式、上下文预算、防死循环 | `ReActLoop`、`PlanExecuteLoop`、`ContextBuilder`、`ContextCompressor`、`LoopGuard` |
| `tool` | 工具协议：注册、Schema 生成、参数校验、超时、幂等 | `ToolRegistry`、`ToolExecutor`、`SchemaGenerator` |
| `llm` | 自研 LLM 网关：模型注册、路由、OpenAI 兼容流式客户端 | `ModelRouter`、`ModelRegistry`、`OpenAiCompatibleClient`、`LlmInvoker` |
| `quota` | RPM/TPM 双维限流：本地预检 + Redis Lua 预扣结算 | `QuotaService`、`LocalRateLimiter`、`TokenBucketScript`、`ModelBulkhead` |
| `stream` | SSE 事件总线（双写 PG 持久化）、断线回放（PG 为主、Redis 为辅）、连接注册与慢客户端治理 | `RunEventBus`、`EventReplayStore`、`SseConnectionRegistry` |
| `memory` | 会话记忆与 pgvector 长期记忆召回、摘要 | `SessionMemory`、`VectorMemory`、`Summarizer` |
| `cache` | 前缀缓存、Embedding 缓存、单飞防击穿 | `PrefixCache`、`EmbeddingCache`、`SingleFlight` |
| `guard` | Prompt 注入防护、输出脱敏 | `PromptInjectionGuard`、`OutputSanitizer` |
| `audit` | 运行审计哈希链、敏感字段遮蔽 | `RunAuditService`、`SensitiveMasker` |
| `metric` | 业务指标出口与标签基数守卫 | `AgentMetrics`、`MetricGuard` |

## 模型路由与灰度

`ModelRouter.candidates` 返回"首选 + 兜底"的候选链，`LlmInvoker` 依次故障转移。灰度发布走金丝雀配置 `agentflow.llm.canary`：仅当首选解析为 `from` 模型时，按 `weight-percent`（0~100）把流量切到 `to` 模型；`sticky-by-tenant: true`（默认）用 `tenantId` 哈希模 100 分桶，保证同一租户多轮对话模型一致（租户随 Reactor Context 传递，禁止 ThreadLocal），无租户上下文时退化为每请求随机分桶。命中计 `llm.canary.hit.total{from,to}` 低基数标签。金丝雀只替换首选，兜底候选不变，`to` 未注册或权重为 0 时自动失效为原路由。配合 `adaptive-concurrency` 的 AIMD 舱壁，新模型接入后并发上限随真实成功率自动爬坡，无需人工调参。

## 模型 fallback 与熔断半开

### 三层决策不冲突

同一次 `LlmInvoker.invoke` 的模型选择按固定顺序叠加，职责互不覆盖：

1. **金丝雀决定"优先选谁"**：`ModelRouter.applyCanary` 只替换首选，不改变候选链顺序，也不感知模型健康度；
2. **fallback 链决定"首选挂了选谁"**：首选出现可重试错误（5xx / 超时 / 连接失败 / 429 / 上下文超长）后，按 `fallback-chain` 有序链切到下一个；金丝雀选出的新首选同样受本链兜底；
3. **熔断冷却决定"谁暂时别选"**：失败模型进入冷却期，`ModelRouter.allowRequest` 直接返回 false，`LlmInvoker` 跳过它继续沿 fallback 链前进。

因此三者不会互相破坏：金丝雀改的是入口，fallback 改的是失败后的退路，熔断只是把不健康的候选从退路里临时剔除。

### fallback 链配置

```yaml
agentflow:
  llm:
    fallback-chain: [mock-large, mock-small]   # 全局默认链
    models:
      mock-large:
        fallback-chain: [mock-small]           # 模型级链，优先级最高
        cooldown-ms: 30000                     # 模型级冷却，覆盖全局 open-ms
```

优先级：模型级 `fallback-chain` > 旧字段 `fallbacks` > 全局 `llm.fallback-chain`。链内自动去重、跳过未注册与超预算模型；单次请求最多尝试 `max-attempts`（默认 3）个模型，避免兜底链过长放大故障。业务错误（4xx 参数非法/内容违规）不触发 fallback——换模型也没用，只会放大成本。

### 熔断三态

每个模型一份实例内状态机，与 litellm Router 的 cooldown 对齐：

```
            连续失败 >= failure-threshold
   CLOSED ───────────────────────────────► OPEN
     ▲                                      │
     │  半开探测成功                          │ now >= openUntilMs
     │                                      ▼
     └──────────────────────────────── HALF_OPEN
                                            │
                 半开探测失败（onFailure）     │ 只放行 half-open-probes 个请求
                 重新计时冷却 ◄────────────────┘
```

- **CLOSED**：正常放行；失败累计到 `failure-threshold`（默认 3）打开熔断。
- **OPEN**：冷却期内 `allowRequest` 直接返回 false，请求走 fallback 链；冷却时长优先取模型级 `cooldown-ms`，否则用全局 `llm.cooldown.open-ms`（默认 30000）。
- **HALF_OPEN**：冷却到期后自动进入，只放行 `half-open-probes`（默认 1）个探测请求；探测成功 `onSuccess` 闭合回 CLOSED，失败 `onFailure` 立即重新 OPEN 并重新计时。取消、配额拒绝、舱壁拒绝等"非模型故障"走 `onAbort`，只归还探测名额、不累计失败次数。
- 状态为实例内 `synchronized` 原子状态：多实例各自熔断，成本最低；若需跨实例强一致可替换为 Redis 实现，接口不变。

### 计费口径（不会重复计费）

每个候选模型各自预扣，但**只有最终成功的那次调用按实际 token 结算**：失败/取消的中间尝试在 `doFinally` 里按 `settled=0` 结算，预扣全额退回；本地令牌桶同样只对成功调用保留消耗。因此 `mock-large` 失败切到 `mock-small` 成功时，账面上只发生一次真实计费。若后续需要把失败尝试的上游成本也计入（供应商可能已计费），应新增独立的"上游成本"指标而不是修改用户配额口径。

### 指标与错误码

| 指标 | 含义 |
|---|---|
| `llm.model.fallback.total{from,to}` | 真正发生模型切换的次数（金丝雀替换不计） |
| `llm.model.cooldown.total{model}` | 熔断打开/重新打开次数 |
| `llm.canary.hit.total{from,to}` | 金丝雀命中次数 |

标签只放模型名（配置内的固定枚举），租户/runId 禁入。错误码语义：本地预检拒绝 `RATE_LIMITED`，Redis 额度不足 `QUOTA_EXCEEDED`，舱壁满或候选全在冷却 `MODEL_BUSY`，路由确实找不到模型 `NO_MODEL_AVAILABLE`，候选都调用过但全部失败 `UPSTREAM_ERROR`（HTTP 502）。

## 数据流

一次流式 run 的完整链路：

1. `POST /v1/agent/runs/stream` 经 `ApiKeyAuthWebFilter` 解析 API Key，租户 ID 写入 Reactor Context。
2. `RunService.create` 落库 run（状态 `PENDING`），写入 `state_json` 初始快照，签发 60 秒租约。
3. `AgentRuntime` 按 mode 选择 `ReActLoop` 或 `PlanExecuteLoop`；每步经 `QuotaService.reserve`（本地预检 → Redis Lua 预扣）→ `ModelBulkhead` 信号量 → `LlmInvoker` 流式调用。
4. 模型产出 `llm.delta` 逐 token 推入 `RunEventBus`；总线把每条业务事件双写 PG `t_run_event`（`(run_id, seq)` 唯一约束幂等，失败仅降级记日志），同时 `EventReplayStore` 追加到 Redis List（最近 500 条，TTL 30 分钟）作为热备。回放以 PG 为主：实例重启后窗口不丢失；Redis 短窗口仅在 PG 查询失败或无数据时兜底。保留期策略：`t_run_event` 随 run 全量保留，由运维定时任务按 `created_at` 清理（建议保留 7 天，与审计链留存周期对齐）；Redis 侧仍受 500 条 / 30 分钟限制。
5. 模型返回工具调用时：`ToolExecutor` 做 Schema 校验 → 白名单 → 幂等检查（`(run_id, tool_call_id)` 唯一约束）→ 执行，高风险工具（如 `asset_delete`）挂起转 `WAITING_APPROVAL`。
6. 工具结果以 `role=tool` 回灌进入下一步；`LoopGuard` 四道闸（最大步数 12、工具调用 8 次、墙钟 120s、重复指纹）任一触发即终止。
7. 响应结束 `QuotaService.settle` 按实际 token 结算退回多扣部分；状态机转 `SUCCEEDED` / `FAILED` / `CANCELED`，审计链追加一条带前序哈希的记录。
8. 客户端断开触发 `doOnCancel` 级联取消：释放模型连接、信号量、配额预扣，run 转 `CANCELED`。

## 超时预算与失败语义

单次 LLM 调用有 TTFT/整体超时，但 8 步各慢一次仍能把一个 run 拖到分钟级，期间一直占用 SSE 连接、模型舱壁信号量与配额。因此再加两层**墙钟预算**，与 `LoopGuard` 的协作式检查形成互补：

| 层级 | 配置 | 默认 | 覆盖范围 | 实现位置 |
|---|---|---|---|---|
| run 级 | `agentflow.agent.max-duration-ms` | 300000（5 分钟） | 整条 run 链（含提示词加载、记忆召回、所有步） | `AgentRuntime.execute` 的 `.timeout(...)` |
| step 级 | `agentflow.agent.step-timeout-ms` | 60000 | 一次 LLM 调用 + 该步工具执行，不含下一步递归 | `ReActLoop.loop` 对单步 `.timeout(...)` |

`step-timeout-ms` 必须严格小于 `max-duration-ms`。启动期由 `AgentFlowProperties.normalizeTimeoutBudget()` 校验：不满足时按 `max-duration-ms` 收敛并打 WARN，不抛异常阻断启动（配置错误不应该让进程起不来）。

**失败语义（单步超时可重试，连续超时才终结 run）**

1. 单步超时先发 `step.started{kind:"STEP_TIMEOUT_RETRY"}`，按可重试失败处理，最多重试 1 次；
2. 重试走 `ReActLoop.loop()` 入口，`nextStep()` 正常计数，因此不会绕过 `LoopGuard` 的最大步数与工具调用上限；
3. 任一步正常完成即清零「连续超时」计数；连续两次单步超时 → `STEP_TIMEOUT`，run 总预算耗尽 → `RUN_TIMEOUT`；
4. run 级 `.timeout` 到点产生 `TimeoutException`，入口统一映射为 `AgentException(RUN_TIMEOUT)`，message 带已用时长与预算。

**代价**：单步重试会重放该步的 LLM 调用与工具调用，因此工具必须幂等（`ToolExecutor` 已按 `(run_id, tool_call_id)` 去重）；换来的是偶发抖动不至于直接把整个 run 判死。

**超时后的资源回收（复用既有收尾路径，不另起一套）**

- 超时错误沿同一条失败路径进入 `RunService.finish`：状态落 `TIMEOUT`，`error_code` 为 `RUN_TIMEOUT` / `STEP_TIMEOUT`；`RunMapper.finish` 同时把 `lease_until` 置为 `now()` 释放租约，`eventBus.complete` 关闭 SSE sink 并让客户端流结束。
- 上游被取消触发 `LlmInvoker` 的 `doFinally`：归还 `ModelBulkhead` 信号量、`QuotaService.settle` 按实际 token 结算（`refund = reserved - actual`），配额不会因超时被永久多扣。
- 终态事件复用现有 `run.failed`（不新增 `EventType`），payload 增加错误码与耗时：`{"state":"TIMEOUT","tokens":n,"code":"RUN_TIMEOUT","elapsedMs":300012}`。

**边界说明**：`LoopGuard` 的 `WALL_CLOCK_TIMEOUT`（`max-wall-clock-ms`，120s）是步与步之间的协作式检查，只能挡住"下一步开始前"已超时的 run；run 级硬截止才能兜住单步内部（流式调用、工具执行）卡死。两者都落 `TIMEOUT` 状态。断点续跑会新建 `RunContext`，超时预算按每次执行尝试独立计时。

## HITL 挂起与恢复

高风险工具（`riskLevel >= 3`）不能与普通失败混为一谈：失败意味着 run 终止，挂起只意味着"暂停等待人工裁决"。因此 `ToolResult` 使用三态 `SUCCESS / FAILURE / WAITING_APPROVAL`，`ReActLoop` 在每轮执行前**整批预判**，命中待审批工具时本轮一个都不执行，返回 `StepOutcome.suspended()` 并以 `Mono.empty()` 正常结束本步——不走 `TOOL_ALL_FAILED`，不会把 `WAITING_APPROVAL` 覆盖成 `FAILED`。

```
                        ┌──── 普通失败/超时 ────► FAILED / TIMEOUT
                        │
PENDING ─► RUNNING ─────┤
             │          ├─ 高风险工具预判命中 ─► WAITING_APPROVAL
             │          │        │ approve               │ reject / 超时
             │          │        ▼                       ▼
             │          │      PENDING ─► RUNNING ─►   CANCELED / TIMEOUT
             │          │   （补执行挂起批次）  SUCCEEDED
             └──────────┴─ 正常完成 ─► SUCCEEDED
```

**挂起时做什么**

1. `ReActLoop.executeTools` 预判命中 → `ctx.suspend(calls)` 记住整批调用 → `ToolExecutor.awaitApproval` 写 `t_approval`。
2. `ApprovalService.suspend`：发 `run.waiting_approval`（带 tool / args / callId / pending[]），把 `AgentState`（含 `pendingToolCalls`、步数、工具计数、token、事件序号）写入 `state_json`，run 置 `WAITING_APPROVAL`，`lease_until=now()` 释放租约。
3. `RunService` 的 `onComplete` 发现 `ctx.isSuspended()`：保留 SSE sink 与最后事件序号，**不落终态、不发 run.failed**，客户端连接保持等待。

**审批通过后如何真正续跑**

`ApprovalService.decide` 先以 `t_approval.status='PENDING'` CAS 裁决，再以 `state='WAITING_APPROVAL'` CAS 把 run 置 `PENDING`；随后 `RunService.resumeAfterApproval` 用 `PENDING→RUNNING` CAS 抢占（抢不到说明恢复任务/其他实例已接管，直接放弃，避免双跑），从 `state_json` 恢复消息、进度计数、事件序号与待执行批次，调用 `AgentRuntime.executeResume` → `ReActLoop.resume`：只把 `pendingToolCalls` 标记为已获批并补执行一次，把结果以 `role=tool` 回灌，然后继续下一步 ReAct 循环。之前已成功的工具只存在于历史消息中，不会被再次调用；同一 `toolCallId` 仍受 `(run_id, tool_call_id)` 唯一约束兜底，重复恢复也不会产生双份副作用。

**驳回与超时**

- 驳回：`t_approval` 与 run 双重 CAS → `CANCELED`（`error_code=APPROVAL_REJECTED`），`lease_until=now()` 释放租约，关闭 SSE；挂起期间无在途模型调用，配额已按实际用量结算。
- 超时：`agentflow.approval.timeout`（默认 30 分钟）。`RunRecoveryJob` 的既有 5 秒调度周期顺带做对账扫描（不新建调度器），审批入口 `decideLatest` 也做惰性判定；命中后审批单与 run 置 `TIMEOUT`（`APPROVAL_TIMEOUT`）并释放资源。

**并发与越权**：`decideLatest` 先校验 `run_id + tenant_id` 归属（跨租户按 `RUN_NOT_FOUND` 拒绝），裁决与状态迁移全部是 `UPDATE ... WHERE status/state = 期望值` 的 CAS，同一 run 重复审批只有第一次生效。

## 幂等与请求去重

客户端超时重试、网关重放、用户双击都会让 `POST /v1/agent/runs` 与 `POST /v1/agent/runs/stream` 重复建 run、重复预扣模型配额、重复烧 token。`idempotency/IdempotencyWebFilter` 在鉴权之后、业务之前用 Redis 做分布式状态机，把重复提交挡在进入业务之前。

**作用域与顺序**

- 仅拦截 `POST /v1/agent/runs`（异步创建）与 `POST /v1/agent/runs/stream`（SSE），其余方法/路径原样放行。
- 顺序：`ApiKeyAuthWebFilter`（`LOWEST_PRECEDENCE - 10`）→ `IdempotencyWebFilter`（`LOWEST_PRECEDENCE - 5`）。租户从 Reactor Context 的 `tenantId` 读取，不使用 ThreadLocal；拿不到租户时按 fail-open 放行（鉴权层正常不会走到这里）。
- 开关 `agentflow.idempotency.enabled=true`。`require-key=false`（默认）时缺失 `Idempotency-Key` 直接放行，保持老客户端向后兼容；开启后缺失返回 400 `-32003`。Key 超过 `key-max-length`（默认 128）同样 400，且不打 Redis。

**状态机**

Redis key 为 `idem:{tenantId}:{idempotencyKey}`，value 同时记录 owner 随机令牌，所有收尾操作走 Lua CAS，避免 TTL 过期/淘汰后误删其他请求刚抢到的锁。

```
                 SETNX idem:{tenant}:{key} = IN_PROGRESS|owner  (PX ttl)
首次请求 ───────────────────────────────► 放行业务
                                            │
        ┌───────────────────────────────────┼───────────────────────────────────┐
        ▼                                   ▼                                   ▼
  2xx + 有响应体                      2xx 无响应体 / SSE                   4xx / 5xx / 异常
  CACHED|owner|status|base64          COMPLETED|owner                    CAS DEL（释放锁）
        │                                   │                                   │
        ▼                                   ▼                                   ▼
  重复请求：原样重放首次响应          重复请求：409（流不可重放）          重试可重新进入业务
  （Idempotency-Replayed: true）      TTL 内不再重复执行

重复请求命中 IN_PROGRESS ──► 409 {"code":-32009,"message":"重复请求正在处理中"}，不进入业务
```

- `ttl` 默认 10m，统一覆盖“处理中”锁与“已完成”状态；状态过期后同 key 可重新作为首次请求处理。
- `POST /v1/agent/runs` 的 2xx JSON 响应会缓存（单响应上限 64KB，Base64 存入 value）供重放；`/runs/stream` 的 SSE 响应不可缓存，只保留 `COMPLETED` 完成标记，TTL 内同 key 再提交返回 409，避免重复跑一遍完整 Agent。
- 释放锁挂在响应完成链路上（`chain.filter(...).then(...)` 与 `onErrorResume`），不是 filter 的同步 `finally`：业务 error 与 4xx/5xx 会 CAS 删除 key，客户端修正后可以重试；客户端主动断开（cancel）不删锁，让超时重试在 TTL 内仍被 409 挡住，而不是再创建一条 run。
- 缓存响应只重放给同一类接口：同一个 key 混用到 `/runs` 与 `/runs/stream` 一律按 409 冲突处理。

**fail-open 取舍**

Redis 不可用（连接失败、超时、脚本错误）时选择放行并打 WARN，而不是拒绝请求。理由：幂等是防重复扣费的优化，不是正确性底线；把幂等存储当强依赖会让一次 Redis 抖动放大成全站不可用。代价是降级窗口内可能重复创建，需要靠 run 状态机、配额与审计链兜底。指标 `idempotency.filter.total{result=miss|hit|conflict|released|fail_open}` 只打结果枚举，键与租户不进标签，避免高基数。

## 配额 Saga 与对账补偿

配额预扣与结算是两次独立的 Redis 调用，中间没有任何分布式事务：

```
LlmInvoker                     Redis 桶                         t_quota_reservation（PG）
   │
   ├─① begin ─────────────────────────────────────────────────► INIT      （已登记、未预扣）
   ├─② QuotaService.reserve ──► 预扣 reserved_tokens
   ├─③ markReserved ──────────────────────────────────────────► RESERVED  （记录实际预扣量）
   ├─④ 模型流式调用（成功/失败/取消）
   ├─⑤ QuotaService.settle ───► 退回 reserved - actual
   └─⑥ markSettled ───────────────────────────────────────────► SETTLED   （终态）
```

**为什么不是强一致**

预扣走 Redis Lua（毫秒级、每请求必经），结算跨 Redis + PostgreSQL；若要求强一致，要么引入 2PC/分布式事务（跨存储、成本高、可用性差），要么把结算放进预扣的同一事务（Redis 快路径被 DB 拖慢）。token 配额是**软限额**：多扣（泄漏）是可接受的保守偏差，多退（宽容）也只是让租户多拿一点额度。因此选择 Saga：主链路只做预扣/结算，账本异步 best-effort 记录，失败由对账任务补偿。

**泄漏窗口有多大**

- 预扣成功、结算未执行（JVM 崩溃、Redis 故障、进程被 kill）：泄漏上限 = 整笔 `reserved_tokens`；对账任务在 `stale-after`（默认 5m）+ 一个扫描间隔（≤60s）内把账本推进到 `REFUNDED` 并退回。结算失败时 `LlmInvoker` 不会把账本推进到 `SETTLED`，账本停在 `RESERVED` 等待补偿。
- 预扣成功但 `markReserved` 也没写成功（账本停留在 `INIT`）：按约定 `INIT` 不退款，这笔泄漏不会被自动补偿，只会出现在账本明细里；属于「账本自身写失败」的窄窗口，概率远低于 Redis 调用失败。
- 结算成功但 `markSettled` 写失败（账本停在 `RESERVED`）：对账会按整笔 `reserved_tokens` 退款，即多退「本次实际消耗」那部分；方向偏向租户（宽容），不会吃掉租户额度，金额上限为单次 `reserved`。
- CAS 置 `REFUNDED` 之后 Redis 退款失败（Redis 仍不可用）：不会重复退款，但这一笔会少退一次，任务打 `ERROR` 日志并计 `quota.reconcile.failed.total{stage=REFUND}`，由 RUNBOOK 的人工补退步骤兜底。

**幂等与多实例安全**

- 扫描条件：`status IN ('INIT','RESERVED') AND created_at/updated_at < now() - stale_after`，走 `(status, created_at)` 索引，按 `batch-size`（默认 200）分批。
- 抢占：`UPDATE t_quota_reservation SET status='REFUNDED' WHERE id=? AND status=?`，返回 1 的实例才执行退款，返回 0 直接跳过；不使用 `SELECT FOR UPDATE` 长时间持锁。
- 重复执行/多实例天然幂等：终态不再被扫描，CAS 保证同一账本最多一次退款。
- `INIT` 与 `RESERVED` 必须区分：`INIT` 尚未真正扣减，只改状态不退款；`RESERVED` 才按账本上的 `reserved_tokens` 退，桶容量 `tpm_limit` 也随账本落库，不受后续配置变更影响。
- 账本写入全部切 `boundedElastic`，且 **best-effort**：写失败只打 WARN，不阻断 LLM 调用。取舍是「账本丢一行 → 少一次自动退款」而不是「DB 抖动 → 全站模型调用失败」。
- 指标只用固定名称/固定枚举标签（`quota.reconcile.scanned.total`、`refunded.total`、`skipped.total`、`failed.total{stage=SCAN|REFUND}`），runId/model 只进日志，避免高基数。

配置：`agentflow.quota.reconcile.enabled / interval-ms / stale-after / batch-size`，见 `QuotaReconciliationJob`、`QuotaReservationLedger`、`mapper/QuotaReservationMapper`、`db/migration/V7__quota_reservation.sql`。

## 异步报告任务

`report_generate` 工具不阻塞 Agent 主链路：先把任务写入 `report_task`（V5 迁移，表名无 `t_` 前缀，状态字段 `PENDING/RUNNING/SUCCEEDED/FAILED`），再投递 RabbitMQ，由 `consumer/ReportTaskConsumer` 异步执行。

完整链路（拓扑名与 `config/RabbitConfig` 一致）：

```
ReportTools.generate
  │ ① INSERT report_task(status=PENDING) —— 先落库，消息丢失可补发
  ▼
report.exchange ──report.generate──► report.queue
                                        │ ② @RabbitListener 手动 ACK
                                        ▼
                              CAS: PENDING → RUNNING
                                 │ 成功                 │ 0 行
                                 ▼                      ▼
                        ReportGenerator.generate   查状态：
                                 │                  SUCCEEDED/FAILED/RUNNING → ACK 跳过
                                 │                  记录不存在 → ACK 丢弃
                                 ▼
                        CAS: RUNNING → SUCCEEDED
                                 │
                                 └─ 异常 ─► 可重试？ ─否─► FAILED + ACK（毒消息不回头）
                                              │是
                                              ▼
                                    同步重试 ≤ max-retry(3)
                                          │耗尽
                                          ▼
                                 FAILED + basicNack(requeue=false)
                                          │
                                          ▼
                  report.exchange.dlx ──report.generate.dlq──► report.queue.dlq
```

**幂等要点**

1. 唯一键是 `report_task.task_id`；消费入口用 `UPDATE ... SET status='RUNNING' WHERE task_id=? AND status='PENDING'` 做 CAS，多实例/重复投递只有一个消费者能认领。
2. 认领失败后再查状态：`SUCCEEDED`（已生成）、`FAILED`（终态）、`RUNNING`（其他消费线程处理中）一律 ACK 跳过，不重复生成；记录不存在属于不可重试错误，ACK 丢弃避免毒消息循环。
3. 报表产物按 `taskId.json` 先写临时文件再覆盖替换，同一任务重复执行只覆盖同一路径，不产生多份文件。
4. 手动 ACK：成功与不可重试失败 `basicAck`，重试耗尽或无法解析/参数缺失的消息 `basicNack(requeue=false)`，由主队列的 DLX 参数转投 `report.queue.dlq`。

**重试与线程模型**

- `agentflow.report.max-retry` 默认 3，表示首次失败后最多再试 3 次（总尝试 4 次）；重试在消费线程内同步完成，不重新入队，避免 requeue 热循环。
- 可重试：`DataAccessException`（临时 DB 异常）、`ReportRetryPolicy.RetryableException`（下游 5xx）、`IOException`（文件/网络 IO）；其余（参数非法、记录不存在）快速失败。
- `@RabbitListener` 是阻塞消费模型，消费者直接同步调用 `JdbcTemplate`，不包 `Mono`/`boundedElastic`：那只是把阻塞换到另一个线程池，既无收益又会掩盖真实线程模型。
- `spring.rabbitmq.listener.simple`：`prefetch=1`、`concurrency=2`、`max-concurrency=4`、`acknowledge-mode=manual`；业务重试由代码控制，因此关闭容器级 retry。

**升级注意**：`report.queue` 只在 `RabbitConfig` 声明一次且带 DLX 参数。若目标环境已存在同名但参数不同的旧队列，RabbitMQ 会抛 `PRECONDITION_FAILED`，需先删除旧队列（确认无积压）再发布。

## 可观测性与 GenAI 语义约定

指标只能回答"整体 P99 是否变差"，回答不了"这一次 run 为什么慢"。本项目在保留现有 12 个低基数指标的同时，按 OpenTelemetry GenAI 语义约定补一条单次 run 的因果链，二者分工明确：

- **metrics**：聚合、告警、容量与 SLO，标签只放模型/工具/结果等低基数维度，禁止 `runId`、`tenantId`、`sessionId`。
- **traces**：单次 run 的 span 树，定位"哪一步、哪个工具、哪次模型调用"贡献了延迟或错误；单次关联靠 traceId，而不是把高基数 ID 塞进标签。

### span 树

```
invoke_agent {model}                 run 根 span：一次 run 一条 trace
├── chat {model}                     每步一次 LLM 调用
│   └── execute_tool {tool}          该轮工具调用（并行时兄弟 span 并行）
└── chat {model}
```

| span | `gen_ai.operation.name` | 关键属性 |
|---|---|---|
| run 根 | `invoke_agent` | `gen_ai.system`、`gen_ai.request.model`、`agentflow.run.mode`、`agentflow.run.state` |
| 每步 | `chat` | 上述 + `gen_ai.usage.input_tokens`、`gen_ai.usage.output_tokens`、`gen_ai.response.finish_reasons`、`gen_ai.prompt.length`、`gen_ai.completion.length` |
| 工具 | `execute_tool` | `gen_ai.tool.name`、`agentflow.tool.success` |

### PII 与成本

- `agentflow.telemetry.capture-content=false`（默认）时，prompt / completion **不写入 span**，只记字符长度与 token 数；满足"能算成本、能看上下文规模，但不泄露内容"。
- 显式开启后，内容先经 `audit/SensitiveMasker` 脱敏（手机号、身份证、邮箱、`sk-*`、`password=` 等），再按 `max-content-chars` 截断，避免大 prompt 撑爆 span。
- `gen_ai.usage.*` 直接来自上游 usage 块，与成本指标同源，避免两套口径。

### 采样与异步安全

- `agentflow.telemetry.sample-rate`（默认 0.1）由 `config/TracingConfig#agentFlowSampler` 注册为全局 `parentBased(traceIdRatioBased(...))` Sampler。根 span 按比例采样，子 span 继承父决策：一条 run 要么整棵树保留，要么整体丢弃，不会出现"根在、子丢"的断链。该 Bean 覆盖了 Boot 的 `management.tracing.sampling.probability`，避免"业务采样 × SDK 采样"双重衰减。
- WebFlux 下禁止 ThreadLocal：`ReActLoop` 用 `contextWrite` 把根 `GenAiSpan` 放进 Reactor Context，`step()`/`executeOne()` 通过 `deferContextual` 读取；子 span 创建时显式 `setParent(context)`。工具并行、`boundedElastic` 切换线程都不依赖"当前 span"，取消/超时路径由 `doFinally` 幂等结束 span。
- 未采样时 `GenAiSpan` 以空操作短路，不创建子 span、不做内容脱敏，几乎零开销。

配置：`agentflow.telemetry.capture-content`、`sample-rate`、`max-content-chars`；代码入口 `telemetry/GenAiTelemetry`、`telemetry/GenAiSpan`、`telemetry/GenAiAttributes`。

## prompt 前缀稳定性与缓存命中

provider 侧前缀缓存（vLLM prefix caching、OpenAI prompt caching）按 token 前缀命中：前缀前几个 token 一变，后面全部失效，直接推高 TTFT 与输入 token 计费。因此 system prompt 的拼装顺序必须是"**最稳定 → 最易变**"：

1. 固定角色说明（`prompts/system_zh.txt`，静态资源）；
2. 工具 schema，按 `name` 字典序稳定排序；
3. 长期记忆召回、会话摘要、执行计划等动态段；
4. 若未来必须注入当前时间，只能放最后一段。

实现上：

- `prompts/PromptAssembler#sortTools` 统一排序；`agent/ContextBuilder` 在裁剪前先排序，保证"注册顺序改变 / ConcurrentHashMap 遍历顺序变化"不影响输出字节。
- 记忆召回、计划续跑沿用"追加在 system prompt 末尾"的方式（`ReActLoop.withMemory`、`PlanExecuteLoop`），因此稳定前缀永远在前面；动态段标记收敛在 `PromptAssembler.DYNAMIC_MARKERS`。
- `PromptAssembler#fingerprint` 计算 `sha256(稳定前缀 + 排序后的工具 schema)` 的前 16 位十六进制。指纹只覆盖稳定部分，记忆/摘要/计划变化不会让指纹漂移；同一工具集、同一角色说明的两次构建必须得到同一指纹。
- 请求前缀不写入随机数、时间戳、sessionId；这些信息若业务需要，只能出现在动态尾巴或消息体。
- `prompts/PromptPrefixMeter` 记录无标签计数：`agent.prompt.prefix.build.total` 与 `agent.prompt.prefix.reused.total`。同一指纹第二次出现即计一次复用，直接反映可缓存前缀的比例；指纹本身只进 DEBUG 日志，不当 metric tag，避免高基数。

**TTFT 与成本为什么受影响**：前缀命中时 provider 可跳过已缓存 token 的 prefill，首 token 延迟近似只受新增动态段与输出长度影响；未命中则每次都要重新 prefill 整段 system prompt，长系统提示下 TTFT 与输入计费会随轮次线性放大。前缀指纹复用率因此是比"缓存命中率"更早可观测的代理指标。

测试覆盖（`ContextBuilderTest`）：同一工具集两次构建指纹一致且复用计数 +1；记忆注入变化不影响稳定前缀；打乱工具注册顺序后输出顺序与指纹一致。

## 补短板表

| 实习常见短板 | 本项目对应设计 |
|---|---|
| 没有真实并发场景 | 1 万 SSE 连接压测 + 模型并发舱壁 |
| 只会同步阻塞写法 | WebFlux 全链路非阻塞 + 背压 |
| 没做过限流 | RPM/TPM 双维度 + 预扣结算 + 分布式 |
| 没做过长任务可靠性 | 状态机 + 租约 + `SKIP LOCKED` + 断点续跑 |
| 不会做缓存治理 | 前缀缓存、embedding 缓存、单飞、击穿保护 |
| 没做过流式协议 | SSE 事件模型 + Last-Event-ID 回放 |
| 没有可观测体系 | TTFT/TPS/token/步数 12 指标 + SLO + 告警 |
| 不会做容量规划 | 压测拐点 + 容量公式 + 压测专用 mock 模型 |
| 不懂取舍 | 6 篇 ADR，每篇必写"代价" |

## 部署拓扑

本地 docker compose 一键起全栈：`postgres`（pgvector/pg16，`max_connections=300`）+ `redis`（512MB，allkeys-lru）+ `rabbitmq`（vhost `/agentflow`）+ `mock-llm`（:9000）+ `app1`（:8080）+ `app2`（:8081）。两个应用实例共享同一 PG/Redis，验证多实例抢占与租约恢复。

**读写分离说明**：本地用同一 PG 实例 + 只读账号（`agentflow_ro`，由 `docker/init-ro-user.sql` 创建，`V1__init_schema.sql` 末尾补 `GRANT SELECT`）模拟读写分离；`DataSourceConfig` 同时装配 `rw-pool`（Hikari 20 连接）与 `ro-pool`（20 连接，readOnly=true），查询走 ro，写入走 rw。生产环境把 `spring.datasource.ro.url` 指向流复制从库即可，代码零改动。

**优雅停机**：`server.shutdown=graceful`，关闭阶段超时 30s；Netty 不再接受新连接，存量 SSE 由应用层心跳驱动自然完结或被兜底超时（墙钟 + 30s）关闭，run 状态因租约过期被另一实例接管，不存在硬断丢任务。
