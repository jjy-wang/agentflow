# RESUME —— 简历亮点（每条附代码位置与验证命令）

> AgentFlow —— 企业级 Agent 运行时平台 | Java 后端开发
> 技术栈：Java 17、Spring Boot 3、WebFlux、PostgreSQL(pgvector)、Redis、RabbitMQ、Resilience4j、Micrometer、Testcontainers、k6

## 一、简历粘贴版（直接复制，4 条）

**标题行**

```
AgentFlow | 企业级 Agent 运行时平台
Java 17 / Spring Boot 3.2 / WebFlux / PostgreSQL + pgvector / Redis / RabbitMQ
```

**一句话定位**：面向长任务的可恢复 Agent 运行时，解决流式输出、幂等、审批、限流在高并发下的失控问题。

**核心条目（按优先级取 4 条）**

1. 解决 Agent 长任务"断线即丢、超时失控"：设计 run/step 两级超时预算并将 deadline 沿调用链下传，配合 SSE 的 `Last-Event-ID` 断点回放实现断线续传；同轮多工具调用改为有界并行（上限 4，顺序可复现）。单实例 20 并发 × 10 轮共 200 请求全部成功，P50/P95/P99 = 3045/3076/3091ms，P99 与 P50 仅差 46ms，证明应用侧无排队放大。

2. 定位并修复模型限流"只扣不退"导致的雪崩：压测发现 20 并发下 81% 请求失败（`NO_MODEL_AVAILABLE`），定位为本地令牌桶按 `prompt+maxTokens` 预扣、结算只退 Redis 不退本地桶；补齐 `refund` 归还路径并引入配额 Saga（INIT→RESERVED→SETTLED/REFUNDED）做对账补偿。同场景复测 200/200 成功、失败率 81%→0%，成功吞吐 2.9→5.59 runs/s，全程堆上限 512MB，零 OOM。

3. 解决重复提交导致的重复建单与重复扣费：实现 `Idempotency-Key + Redis 状态机 + Lua CAS` 的分布式幂等，重复请求返回 409 且不重复执行；工具层再以 `(runId, toolCallId)` 做二级幂等，保证崩溃重放时工具恰好执行一次。

4. 实现高风险工具的人机审批（HITL）挂起/恢复：按工具风险等级整轮预判，挂起时释放 run 租约、不占线程；审批通过后从 `state_json` 快照续跑。实测挂起期间副作用为 0，approve 后状态 `SUCCEEDED` 且工具恰好执行一次（`deleted` 计数 0→1）。

**备选条目（按 JD 再挑 1–2 条）**

5. 慢消费者隔离：SSE 采用 `DROP_OLDEST` + `stream.lagged` 信号 + 回放兜底，慢连接被主动断开而不阻塞生产端；模型侧实现 AIMD 自适应舱壁、fallback 链与熔断半开（CLOSED/OPEN/HALF_OPEN）。

6. 可观测性：按 OpenTelemetry GenAI 语义约定落地 `invoke_agent → chat → execute_tool` 三级 span，异步链路用显式 parent context 而非 ThreadLocal，跨线程仍能正确成链。

7. 工程质量：208 个用例全绿，Testcontainers 拉起真实 PG/Redis/RabbitMQ 做集成测试；补 `ApplicationContextRunner` 装配测试，修复了"207 个单测全绿但应用起不来"的双构造器注入问题。

## 二、数字口径（面试必问）

| 数字 | 诚实口径 |
|---|---|
| 5.59 runs/s | 单实例、本地 Docker、mock 模型，不是线上集群 |
| P99 3.09s | 其中约 2.8s 是 mock 模型固定输出耗时，应用侧增量只有几十毫秒 |
| 81%→0% | 同一场景、同一机器、同一并发的前后对比，可复现 |
| 208 用例 | `mvn test` 实测，不是"覆盖 90%"这类虚数 |

**不要写**："支撑百万 QPS""微服务集群""高可用多活"——没有实测支撑，一问就露。

## 三、逐条证据（代码位置 + 验证命令）

以下每条都能在仓库里点开对应文件、跑对应命令，用于面试前自检与现场追问。

## Bullet 1：Agent 运行时

**简历表述**：实现 ReAct / Plan-Execute 双模式执行引擎，支持工具调用、token 预算裁剪、超长对话摘要压缩、pgvector 长期记忆召回、SSE 逐 token 流式输出与 `Last-Event-ID` 断线回放；用"最大步数 + 重复调用指纹 + 墙钟超时 + token 预算"四道闸防死循环。

**代码位置**：
- 双模式引擎：`src/main/java/cn/com/agentflow/agent/ReActLoop.java`、`PlanExecuteLoop.java`
- 四道闸：`agent/LoopGuard.java`（`RunLimits` 定义阈值：max-steps 12、max-tool-calls 8、墙钟 120s）
- 上下文预算与裁剪：`agent/ContextBuilder.java`、`ContextCompressor.java`（摘要限 500 token）
- 记忆召回：`memory/VectorMemory.java`（pgvector HNSW、阈值 0.75、Top-K 5）、`memory/SessionMemory.java`
- 断线回放：`stream/EventReplayStore.java`（PG 持久化窗口为主，Redis 500 条 / 30 分钟热备为辅）
- 事件流持久化与断点续传回放：`stream/RunEventBus.java` 发布事件双写 `t_run_event`（`db/migration/V6__run_event.sql`，`(run_id, seq)` 唯一幂等，失败仅降级记日志），实例重启后按 `Last-Event-ID` 继续回放不丢事件

**验证命令**：
```bash
mvn -q test -Dtest=LoopGuardTest,ContextBuilderTest
curl -N -X POST localhost:8080/v1/agent/runs/stream -H 'X-Api-Key: demo-key-123456' \
  -H 'Content-Type: application/json' -d '{"query":"查询 10.0.0.1 主机","model":"mock-large"}'  # 观察 llm.delta 逐 token
# 断线后带 Last-Event-ID 重连 GET /v1/agent/runs/{runId}/events，确认事件不丢
```

## Bullet 1b：LLM 输出协议自愈与审计链完整性

**简历表述**：设计结构化输出协议守卫——模型吐出非法 JSON（截断的 tool_call arguments）时不再直接判 run 失败，而是把"格式错误 + 协议说明"作为系统反馈回灌上下文让模型自我纠正，单 run 限 2 次且计入步数预算防死循环；审计日志采用 prev_hash 链式哈希落库，配套 AuditChainVerifier 逐条重算校验，可定位首个被篡改行。

**代码位置**：
- 协议守卫与回灌：`agent/OutputProtocolGuard.java`、`agent/ReActLoop.java`（`PROTOCOL_RETRY` 事件 / `PROTOCOL_INVALID` 错误码）
- 链式审计：`audit/RunAuditService.java`（UTC+微秒规范化哈希）、`audit/AuditChainVerifier.java`（纯工具类，boundedElastic）

**验证命令**：
```bash
mvn -q test -Dtest=ReActLoopProtocolRetryTest,AuditChainVerifierTest
```

## Bullet 1c：工具并行与 RAG 闭环

**简历表述**：
- 同轮工具调用有界并行（顺序可复现 + 单点失败降级）：`flatMap(maxConcurrency=4)` 并发执行同一轮多个工具，`tool.call` 按原始顺序先发、`tool.result` 按完成顺序回推，结果按原始下标排序回灌上下文；单工具异常转失败结果让模型自我纠错，仅全部失败才终止整轮。
- pgvector 长期记忆闭环（工具自主召回 + 预检索注入）：`memory_recall`/`memory_write` 内置工具经 `ToolRegistry` 授权调用 pgvector，每轮 run 开跑前用最后一条用户消息做 Top-K 召回并注入 system prompt 独立小节；向量库异常降级为"无记忆"继续执行，不阻断 run。

**代码位置**：
- 并行执行与顺序组装：`agent/ReActLoop.java`（`executeTools`、`IndexedResult`）
- 并发安全：`agent/RunContext.java`（`AtomicInteger` 计数、`ConcurrentHashMap.merge` 指纹、串行化 `emit`）、`agent/LoopGuard.java`
- 记忆工具：`tools/MemoryTools.java`（`memory_recall`、`memory_write`）
- 预检索注入：`memory/MemoryRetrievalService.java`、`agent/ReActLoop.java`（`withMemory`）
- 配置：`agentflow.tool.parallel.enabled/max-concurrency`、`agentflow.agent.memory-pre-retrieval.enabled/top-k`

**验证命令**：
```bash
mvn -q test -Dtest=ReActLoopParallelToolsTest,LoopGuardTest,MemoryToolsTest,ReActLoopMemoryTest
# 压测：mock 模型每轮返回 4 个 200ms 工具，对比串行/并行一轮耗时与 PG connections_active
```

## Bullet 1d：run/step 两级超时预算与超时资源回收

**简历表述**：为 Agent 执行链补齐墙钟预算——run 级 `max-duration-ms`（默认 5 分钟，`AgentRuntime.execute` 上 `.timeout` 硬截止）兜住"每步都慢但单步都没超时"的累积挂起，step 级 `step-timeout-ms`（默认 60 秒，覆盖一次 LLM 调用 + 该步工具）做单步隔离；单步超时按可重试失败处理、最多重试 1 次且重试计入步数预算，连续两次或总预算耗尽才以 `STEP_TIMEOUT` / `RUN_TIMEOUT` 终结 run。超时复用统一收尾路径：落库 `TIMEOUT` 并释放租约、关闭 SSE、取消上游触发舱壁信号量归还与配额按实际用量结算，终态复用 `run.failed` 事件并在 payload 带 `code` 与 `elapsedMs`，不新增事件类型。

**代码位置**：
- run 级硬截止与错误码映射：`agent/AgentRuntime.java`（`RUN_TIMEOUT`）
- step 级超时、重试与 `STEP_TIMEOUT_RETRY` 事件：`agent/ReActLoop.java`（`loop`、`step`、`onStepTimeout`）
- 连续超时计数：`agent/RunContext.java`
- 配置校验（step 必须小于 total，非法按 total 收敛并 WARN）：`config/AgentFlowProperties.java`（`normalizeTimeoutBudget`）
- 统一收尾与事件 payload：`run/RunService.java`（`finish`，复用 `RUN_FAILED`）
- 配置：`agentflow.agent.max-duration-ms`、`agentflow.agent.step-timeout-ms`

**验证命令**：
```bash
mvn -q test -Dtest=RunTimeoutTest,AgentTimeoutConfigTest,RunServiceTimeoutTest
```

## Bullet 1e：可恢复的人工审批（挂起态持久化 + 幂等补执行）

**简历表述**：把高风险工具的人工审批做成可恢复的挂起态，而不是"返回失败"——`ToolResult` 引入 `SUCCESS / FAILURE / WAITING_APPROVAL` 三态，`ReActLoop` 每轮执行前整批预判、命中即 `Mono.empty()` 正常收束本轮，run 落 `WAITING_APPROVAL` 且不覆盖为 `FAILED`；挂起时把待执行工具批次与步数/token/事件序号一并写入 `state_json` 并释放租约，`run.waiting_approval` 事件携带 tool/args/callId/pending[] 供审批卡片渲染。审批通过后用 `t_approval.status` 与 run `state` 双重 CAS 保证只生效一次，`resumeAfterApproval` 从快照恢复并只补执行挂起批次（历史已成功工具不重跑，`(run_id, tool_call_id)` 唯一约束兜底），随后继续 ReAct 循环直至 `SUCCEEDED`；驳回转 `CANCELED` 并释放资源，超过 `agentflow.approval.timeout`（默认 30 分钟）由既有恢复任务对账收敛为 `TIMEOUT`，不新建调度器。

**代码位置**：
- 挂起语义：`tool/ToolResult.java`（三态）、`agent/ReActLoop.java`（`executeTools` 预判、`StepOutcome.suspended`、`resume`）
- 挂起落库与事件：`run/ApprovalService.java`（`suspend` / `decide` / `expireStale`）、`agent/AgentState.java`（`pendingToolCalls`、`eventSeq`）
- 恢复执行与 SSE 续推：`run/RunService.java`（`resumeAfterApproval` / `rejectAfterApproval` / `expireAfterApproval`）、`agent/AgentRuntime.executeResume`
- 配置：`agentflow.approval.timeout`（`AgentFlowProperties.Approval`）

**验证命令**：
```bash
mvn -q test -Dtest=ReActLoopApprovalTest,ApprovalServiceTest,RunServiceApprovalTest,ToolExecutorTest
# 挂起为 WAITING_APPROVAL / approve 后续跑且工具只执行一次 / reject 转 CANCELED / 重复与跨租户审批被拒 / 超时收敛
```

## Bullet 1f：GenAI 语义约定链路追踪（span 树 + PII 默认关闭）

**简历表述**：在已有"12 个低基数指标"之上补一条单次 run 的因果链，按 OpenTelemetry GenAI 语义约定产出 `invoke_agent {model}` → `chat {model}` → `execute_tool {tool}` 的 span 树，填充 `gen_ai.system`、`gen_ai.request.model`、`gen_ai.usage.input_tokens/output_tokens`、`gen_ai.response.finish_reasons`、`gen_ai.tool.name`；prompt/completion 默认不落 span，只记字符长度与 token 数，开启 `agentflow.telemetry.capture-content` 后先过 `SensitiveMasker` 脱敏再截断，避免可观测性变成 PII 泄漏面。采样用 `agentflow.telemetry.sample-rate` 驱动 `parentBased` 全局 Sampler，一条 run 要么整棵树保留要么整体丢弃，避免"根在、子丢"；WebFlux 下根 span 经 `contextWrite` 放入 Reactor Context，子 span 显式 `setParent(context)`，并行工具与线程切换不依赖 ThreadLocal。

**可被追问的点**：
- **span 树与 metrics 的分工**：metrics 用模型/工具/结果等低基数标签做聚合、告警与容量，不承载 `runId/tenantId`；trace 用 traceId 关联单次 run，回答"慢在哪一步、哪个工具、哪次模型调用"，两者不互相替代。
- **为什么默认不采内容**：prompt 体积大且含业务数据，写入 span 会同时放大存储成本与合规风险；长度 + token 已足够做成本归因，需要排障时才按配置临时开启并强制脱敏。
- **采样为什么不叠加**：自定义 `Sampler` Bean 覆盖 Boot 的 `management.tracing.sampling.probability`，避免业务采样与 SDK 采样相乘导致链路大面积丢失。

**代码位置**：
- span 工厂与属性：`telemetry/GenAiTelemetry.java`、`telemetry/GenAiSpan.java`、`telemetry/GenAiAttributes.java`
- 埋点边界（仅观测，不改控制流）：`agent/ReActLoop.java`（`withRunSpan`、`step`、`executeOne`）
- 采样：`config/TracingConfig.java`（`agentFlowSampler`）
- 配置：`agentflow.telemetry.capture-content/sample-rate/max-content-chars`
- 设计说明：`docs/DESIGN.md`「可观测性与 GenAI 语义约定」

**验证命令**：
```bash
mvn -q test -Dtest=GenAiTelemetryTest   # run/chat/tool 父子关系、usage/finish_reasons、内容默认不写、脱敏、未采样短路
# 接入 OTLP collector 后按 traceId 查询，确认一次 run 只有一棵 span 树且无 runId/tenantId 标签
```

## Bullet 1g：prompt 前缀稳定性与 provider 缓存复用

**简历表述**：把 system prompt 的拼装顺序重排为"最稳定 → 最易变"——固定角色说明与按 `name` 字典序排序的工具 schema 放最前，长期记忆召回、会话摘要、执行计划等动态段追加在末尾，时间戳类内容移除或后移到最后；同一工具集下即使用户记忆变化，稳定前缀仍字节级一致。用 `sha256(稳定前缀 + 排序后的工具 schema)` 前 16 位作为"前缀指纹"，只覆盖稳定部分，动态内容变化不会让指纹漂移；`agent.prompt.prefix.build.total` 与 `agent.prompt.prefix.reused.total` 两个无标签计数直接反映同一指纹的重复出现，指纹本身只进 DEBUG 日志，避免高基数标签。

**可被追问的点**：
- **前缀稳定如何影响 TTFT 与成本**：provider 按 token 前缀命中缓存；前缀一变，后续全部重新 prefill。稳定前缀让第 2 次及以后的请求跳过 system prompt 的 prefill，TTFT 近似只受新增动态段与输出长度影响，输入 token 计费也显著下降；前缀越长、轮次越多，收益越明显。
- **为什么用指纹而不是直接统计缓存命中**：provider 命中率对应用不可见；指纹复用率是应用侧可自证的代理指标，能区分"前缀设计退化"与"provider 侧缓存未生效"。
- **工具顺序为什么会影响**：工具 schema 是请求前缀的一部分，注册容器（并发 Map）遍历顺序不稳定会让相同工具集序列化出不同字节；先排序再做预算裁剪，保证输出可复现。

**代码位置**：
- 排序、稳定前缀与指纹：`prompts/PromptAssembler.java`
- 复用计数：`prompts/PromptPrefixMeter.java`
- 构建入口：`agent/ContextBuilder.java`（先排序再裁剪，指纹写入日志/指标）
- 动态段追加：`agent/ReActLoop.java#withMemory`、`agent/PlanExecuteLoop.java`
- 配置：`agentflow.prompt.log-fingerprint`
- 设计说明：`docs/DESIGN.md`「prompt 前缀稳定性与缓存命中」

**验证命令**：
```bash
mvn -q test -Dtest=ContextBuilderTest   # 同工具集指纹一致、记忆变化不影响前缀、打乱注册顺序输出一致
curl -s localhost:8080/actuator/prometheus | grep -E 'agent_prompt_prefix_(build|reused)_total'
```

## Bullet 2：模型网关限流

**简历表述**：针对供应商 RPM / TPM 双维度限制，设计"预扣-结算"令牌桶（Redis Lua 原子执行、时间取 Redis `TIME` 避免多实例时钟漂移），配合本地预检两级限流与每模型信号量舱壁；Redis 不可用时降级为本地近似限流而非整体失败。流量治理再加四道：**AIMD 自适应并发舱壁**——并发上限不写死，成功窗口 +1 爬坡、超时/5xx/TTFT 超阈值 ×0.7 收缩（上限封顶、下限保 1），上游退化时自动减负、恢复后缓慢回弹；**租户粘性金丝雀模型路由**——按权重把流量切到新模型，tenantId 哈希分桶保证同租户多轮对话模型一致，命中计低基数指标，灰度比例即配即生效；**跨模型 fallback + 熔断半开**——主模型 5xx/超时/上下文超长时按 `fallback-chain` 切到下一模型，失败模型进入 30s 冷却期，到期只放一个探测请求，成功闭合、失败重新冷却（CLOSED/OPEN/HALF_OPEN 三态），与金丝雀、舱壁按"金丝雀选首选 → fallback 选退路 → 熔断剔除不健康候选"顺序叠加且互不冲突；**限流令牌申请-归还语义**——本地预检在任何失败/超时/取消路径全额归还令牌，并用原子标记保证一次预扣只退一次、退款以桶容量截断，修复高并发下"只扣不退"抽干令牌桶、81% 请求被误报 `NO_MODEL_AVAILABLE` 的 P0。

**代码位置**：
- 预扣/结算：`quota/QuotaService.java`、`src/main/resources/lua/quota_reserve.lua`、`quota_settle.lua`
- 本地预检与令牌归还：`quota/LocalRateLimiter.java`（容量 = 全局/实例数 × 1.2；`tryAcquire`/`refund` 申请-归还契约、`llm.local.limiter.available.tokens{model}` / `rejected.total{model,reason}` 指标）
- 舱壁：`quota/ModelBulkhead.java`（写死并发 + AIMD 自适应双模式，`acquireAdaptive` 内 CAS 准入闸门 + 窗口统计）
- AIMD 信号回传：`llm/LlmInvoker.java`（`doFinally` 按 ON_ERROR/ON_COMPLETE/取消分别回传 failure/success/仅释放）
- fallback 与熔断：`llm/ModelRouter.java`（`fallback-chain` / `cooldown-ms` / CLOSED-OPEN-HALF_OPEN 三态；`llm.model.fallback.total{from,to}`、`llm.model.cooldown.total{model}`）、`llm/LlmInvoker.java`（跨模型切换、失败尝试按 0 结算避免重复计费）
- 金丝雀路由：`llm/ModelRouter.java`（`agentflow.llm.canary` 配置，tenantId 哈希模 100 分桶）、`metric/AgentMetrics.java`（`llm.canary.hit.total{from,to}`）
- 自研网关：`llm/OpenAiCompatibleClient.java`

**验证命令**：
```bash
mvn -q test -Dtest=ModelBulkheadAimdTest,ModelRouterCanaryTest   # AIMD 加性增/乘性减边界 + 粘性一致性与权重分布
mvn -q test -Dtest=LocalRateLimiterTest,LlmInvokerFallbackTest,ModelRouterCooldownTest   # 准入不扣/失败归还一次/限流码/fallback/半开恢复
k6 run loadtest/quota_burst.js   # 400/s 突发，观察 429 比例与 quota_rejected 计数
curl -s localhost:8080/actuator/prometheus | grep -E 'quota_rejected_total|llm_canary_hit_total|llm_model_fallback_total|llm_model_cooldown_total|llm_local_limiter'
docker exec af-redis redis-cli TIME   # Lua 用同一时间源
docker compose stop redis && curl -X POST localhost:8080/v1/agent/runs ... # 验证降级而非全挂
```

## Bullet 2b：配额预扣 Saga 对账补偿（最终一致性 + 幂等退款）

**简历表述**：预扣与结算跨 Redis/PostgreSQL 两次独立调用，JVM 崩溃或结算失败会让 token 永久泄漏。设计 `t_quota_reservation` 三段式 Saga 账本（`INIT → RESERVED → SETTLED/REFUNDED`）：预扣前登记、预扣成功 CAS 记实扣量、结算完成落终态；`QuotaReconciliationJob` 定时扫描超过 `stale-after` 的非终态账本，用 `UPDATE ... WHERE id=? AND status=?` CAS 抢占退款权，抢到才用与 settle 相同的 Lua 脚本退回预扣 token，`INIT` 只改状态不退款；账本写入全部 `boundedElastic` 且 best-effort（失败只 WARN，不阻断主链路），重复执行/多实例不会重复退款，指标仅用固定枚举标签。

**代码位置**：
- 状态机与账本：`quota/QuotaReservationLedger.java`、`quota/QuotaReservationStatus.java`、`mapper/QuotaReservationMapper.java`、`db/migration/V7__quota_reservation.sql`
- 对账任务：`quota/QuotaReconciliationJob.java`（`agentflow.quota.reconcile.*`，默认 60s 扫描 / 5m 陈旧 / 200 一批）
- 主链路钩子：`llm/LlmInvoker.java`（预扣前 INIT、预扣后 RESERVED、结算后 SETTLED）
- 退款脚本复用：`quota/QuotaService.java#refundReserved` + `lua/quota_settle.lua`
- 设计与运维：`docs/DESIGN.md`「配额 Saga 与对账补偿」、`docs/RUNBOOK.md`「配额异常排查 / 手动触发对账」

**验证命令**：
```bash
mvn -q test -Dtest=QuotaReconciliationJobTest,QuotaReservationLedgerTest,LlmInvokerQuotaLedgerTest,QuotaServiceTest
# INIT 超时不退款 / RESERVED 退一次 / 重复执行幂等 / CAS 抢占失败跳过 / 账本写失败不影响模型调用
```

## Bullet 3：长任务可靠性

**简历表述**：run 状态机持久化 + 60 秒租约（`SELECT ... FOR UPDATE SKIP LOCKED`）实现多实例抢占与断点续跑，工具调用以 `(run_id, tool_call_id)` 唯一约束保证幂等；客户端断开触发级联取消，回收模型连接、信号量与配额预扣。

**代码位置**：
- 状态机/租约：`run/RunState.java`（7 态）、`run/RunService.java`、`run/RunLeaseService.java`
- 抢占恢复：`run/RunRecoveryJob.java`（5s 扫描、批量 50、`SKIP LOCKED`）
- 幂等：`tool/ToolInvocationStore.java` + `V1__init_schema.sql` 中 `(run_id, tool_call_id)` 唯一约束
- 级联取消：`api/AgentController.java` 的 `doOnCancel` → `runService.cancel`

**验证命令**：
```bash
# 发起 run 后 kill app1，60 秒内 app2 接管（看 RunRecoveryJob 日志）
docker compose stop app1 && docker logs af-app2 2>&1 | grep -i recovery
# 同一 tool_call_id 重复执行只跑一次：查 tool_invocation 表无重复行
docker exec af-postgres psql -U agentflow -d agentflow -c \
  "SELECT run_id, tool_call_id, count(*) FROM tool_invocation GROUP BY 1,2 HAVING count(*)>1;"
```

## Bullet 3b：分布式幂等（Redis 状态机 + fail-open 取舍）

**简历表述**：为 `POST /v1/agent/runs`、`/runs/stream` 增加 `Idempotency-Key` 幂等层，基于 Redis SETNX + TTL 的 `IN_PROGRESS / COMPLETED / CACHED` 状态机（`idem:{tenantId}:{key}`）把超时重试、网关重放与双击挡在业务之前，避免重复建 run、重复预扣配额与重复消耗 token；非流式响应缓存首次结果供原样重放，SSE 只保留完成标记；失败按响应完成链路 CAS 释放锁，Redis 不可用时 fail-open（WARN）保证主流程不被幂等存储拖死；指标仅打结果枚举，避免高基数。

**代码位置**：
- 过滤器：`idempotency/IdempotencyWebFilter.java`（排在 `ApiKeyAuthWebFilter` 之后，租户取自 Reactor Context）
- 状态机：`idempotency/RedisIdempotencyStore.java`（Lua CAS 完成/释放）、`idempotency/IdempotencyResponseCapture.java`（响应旁路捕获）
- 配置：`AgentFlowProperties.Idempotency`、`application.yml` 的 `agentflow.idempotency.*`

**验证命令**：
```bash
mvn -q test -Dtest=IdempotencyWebFilterTest   # 并发去重/缺失与超长 key/失败释放/fail-open/重放
# 同一 Idempotency-Key 连发两次：首次 200，第二次 409 或重放首次响应（看 Idempotency-Replayed）
```

## Bullet 4：万级长连接

**简历表述**：WebFlux 全链路非阻塞，SSE 连接数与应用线程数解耦；实现每租户连接配额、15 秒心跳、有界写缓冲与慢客户端超时关闭，避免无界缓冲导致 OOM；慢消费者背压隔离（每连接独立有界缓冲 + DROP_OLDEST 可恢复丢弃 + `stream.lagged` 提示 + Last-Event-ID 回放兜底），连续丢弃超阈值主动断连，不让单个慢连接拖垮共享资源。

**代码位置**：
- 连接治理：`stream/SseConnectionRegistry.java`（每租户上限 200；`LagGuard` 连接级连续丢弃计数，超 `agentflow.stream.max-lag-events` 断连）
- 心跳与有界缓冲：`api/AgentController.java`（15s `ping`，id=0）、`agentflow.stream.write-buffer-size=256`、`write-timeout-seconds=5`
- 慢消费者隔离：`stream/RunEventBus.java`（每连接 `onBackpressureBuffer(write-buffer-size, DROP_OLDEST)` + `stream.lagged` 提示）
- 慢客户端指标：`metric/AgentMetrics.java`（`sse.slow_client.closed.total`）、`sse.events.dropped.total`、`sse.connections.lagged_closed.total`

**验证命令**：
```bash
ulimit -n 65535 && k6 run loadtest/sse_10k.js   # 1 万连接 5 分钟，观察无 OOM
curl -s localhost:8080/actuator/prometheus | grep -E 'sse_slow_client|backpressure_drop|sse_events_dropped|sse_connections_lagged_closed'
docker stats af-app1   # 压测期堆内存稳定
```

## Bullet 5：可观测体系

**简历表述**：定义 12 个核心指标（TTFT、token 消耗、步数分布、舱壁拒绝、背压丢帧等），启动期校验指标标签基数、禁止租户/会话 ID 入标签；配套 4 条 SLO 与 6 条告警 Runbook，压测报告给出容量拐点与实例数推导。

**代码位置**：
- 指标出口：`metric/AgentMetrics.java`（`agent.ttft`、`agent.run.duration`、`llm.tokens.total`、`quota.rejected.total`、`tool.call.duration`、`backpressure.drop.total` 等）
- 标签守卫：`metric/MetricGuard.java`（启动期校验，runId/tenantId 入标签直接拒绝）
- SLO/告警：`docs/SLO.md`（4 条 SLO + 错误预算）、`docs/RUNBOOK.md`（6 条告警含定位命令）
- 容量推导：`docs/BENCHMARK.md`（6 场景 + 拐点）、`docs/CAPACITY.md`（容量公式代入实测）

**验证命令**：
```bash
curl -s localhost:8080/actuator/prometheus | grep -c '^agent_\|^llm_\|^quota_\|^tool_\|^sse_\|^backpressure_'
# 标签检查：确认无 runId/tenantId 标签
curl -s localhost:8080/actuator/prometheus | grep -E 'tenantId=|runId=' && echo FAIL || echo PASS
k6 run loadtest/baseline.js   # ttft_ms P95 < 1500ms 阈值门槛
```

## Bullet 6：RabbitMQ 异步任务消费（幂等 + 有限重试 + 死信兜底）

**简历表述**：把报表生成从 Agent 主链路剥离为 RabbitMQ 异步任务：生产端先落库 `report_task` 再投递，消费端用 `PENDING→RUNNING→SUCCEEDED/FAILED` 条件更新 CAS，保证多实例与重复投递下同一任务只生成一次；可重试错误（临时 DB 异常、下游 5xx、IO）同步重试 3 次，耗尽后标记 FAILED 并 nack 进死信；不可重试错误直接标记 FAILED 并 ACK，避免毒消息阻塞主队列。消费者运行在 AMQP 阻塞线程，直接同步调用 JdbcTemplate，不用 `boundedElastic` 伪装非阻塞。

**代码位置**：
- 拓扑与死信：`config/RabbitConfig.java`（`report.exchange` / `report.queue` / `report.exchange.dlx` / `report.queue.dlq`）
- 生产端：`tools/ReportTools.java`（先落库后投递）
- 消费与状态机：`consumer/ReportTaskConsumer.java`、`consumer/ReportTaskStore.java`
- 重试决策：`consumer/ReportRetryPolicy.java`（`agentflow.report.max-retry`，默认 3）
- 产物生成：`consumer/JsonReportGenerator.java`
- 测试：`src/test/java/cn/com/agentflow/consumer/ReportTaskConsumerTest.java`

**验证命令**：
```bash
mvn -q test -Dtest=ReportTaskConsumerTest   # 幂等 / 重试耗尽进死信 / 不可重试快速失败 / CAS 并发
docker exec af-rabbit rabbitmqctl list_queues -p /agentflow name messages consumers
# 重复投递同一 task_id：report_task 只有一条且最终 SUCCEEDED，output-dir 下只有一份 {taskId}.json
```

## 数字填写红线

| 可以写 | 必须实测后填写 |
|---|---|
| "四道闸"、"12 个指标"、"6 个压测场景"、"5 个工具"、"6 条告警" | "单实例 P95 = ___ ms"、"压测到 ___ 并发连接" |

不写"百万 QPS"、"提升 300%"（无 baseline）；连接数写"压测到 1 万连接"，不写"支持百万连接"。
