# AGENT —— Agent 执行引擎设计

## ReAct 流程

`ReActLoop` 实现 Thought → Action → Observation 循环，单 run 上限 12 步（`agentflow.agent.max-steps`）：

1. **组装上下文**：`ContextBuilder` 按 token 预算缝出 prompt（见"上下文预算"）。
2. **调用模型**：`LlmInvoker` 走配额预扣 → 舱壁信号量 → 流式调用，`llm.delta` 事件实时外推。
3. **解析输出**：模型输出为纯文本则作为候选最终答案；为工具调用（`tool_call`）则进入 Action。
4. **执行工具**：`ToolExecutor` 依次做 JSON Schema 参数校验 → 白名单校验 → 幂等检查 → 超时执行。参数不合法、工具不存在、执行异常都把截断后的错误信息作为 Observation 以 `role=tool` 回灌，让模型自我纠正；同一工具错误最多重试 2 次。
5. **循环守卫**：每步结束过 `LoopGuard` 四道闸（见"断点续跑"前小节），通过则进入下一步，否则终止并给出终止原因。
6. **收敛**：模型连续一轮无工具调用即输出最终答案，状态机转 `SUCCEEDED`。

`PlanExecuteLoop` 面向复杂任务：第一步让模型输出结构化计划（JSON mode，校验失败带错误信息回灌重试），随后按计划逐步执行，避免每步重新全量推理；计划落 `state_json`，续跑时从计划断点继续。

## 结构化输出回灌重试

模型偶发吐出不符合输出协议的内容（典型是 tool_calls 的 `function.arguments` 为截断/未闭合的半截 JSON），旧行为是 `ToolExecutor` 解析抛错直接判 run 失败。现改为协议自愈：

1. **校验**：每轮输出聚合后先过 `OutputProtocolGuard`。协议约定：assistant 输出要么纯文本最终答复，要么一个或多个 tool_calls 且每个 `arguments` 必须是合法 JSON 对象。
2. **回灌**：校验失败不判死。违规输出仅以纯文本留档（不落 tool_calls，避免上下文配对污染），把"格式错误 + 协议说明"作为一条 `role=system` 反馈消息回灌上下文，递归进入下一步让模型重新输出。
3. **上限**：单 run 最多回灌 2 次，第 3 次违规以 `PROTOCOL_INVALID` 判失败。重试走 `loop()` 入口，`nextStep()` 正常计数，回灌天然计入 LoopGuard 步数预算，不会绕过四道闸死循环。
4. **可观测**：每次回灌发一条 `step.started` 事件（`kind=PROTOCOL_RETRY`，附 retry 序号）；次数以日志记录（`结构化输出协议违规，回灌重试 runId=... retry=...`）。

## 工具协议

- **声明**：工具用 `@Tool` / `@ToolParam` 注解声明在 Spring Bean 方法上（见 `tools/AssetTools`、`VulnTools`、`ReportTools`、`AssetAdminTools`），`ToolRegistry` 启动期扫描注册。
- **Schema**：`SchemaGenerator` 从方法签名与注解生成 JSON Schema，随请求下发给模型，使模型按契约出参。
- **校验**：入参先过 networknt json-schema-validator，幻觉参数（类型错、缺必填、多字段）在进业务前被拦掉。
- **白名单**：只允许已注册工具；模型幻觉出不存在的工具名，返回"工具不存在"作为 Observation 回灌。
- **幂等**：每次工具执行以 `(run_id, tool_call_id)` 唯一约束写入 `tool_invocation`，重复调用 `ON CONFLICT DO NOTHING` 直接返回首次结果，断点续跑不会重复执行。
- **审批**：`riskLevel=HIGH` 的工具（如 `asset_delete`）执行前挂起，run 转 `WAITING_APPROVAL`，审批通过/拒绝后经 `ApprovalService` 恢复或终止。
- **超时**：每个工具带超时预算，超时按失败回灌并计入 `tool.call.duration{code=timeout}`。

内置 7 个工具：`asset_search`、`asset_detail`、`vuln_search`、`report_generate`、`asset_delete`（高风险）、`memory_recall`、`memory_write`。

## 同轮工具调用有界并行

模型一轮可能给出多个相互独立的工具调用（如同时查资产与漏洞）。旧实现 `Flux.concat` 串行执行，一轮 3 个 1 秒的工具就要 3 秒，与"高并发 Agent 运行时"的定位不符。现改为有界并行：

- **为什么能并行**：工具是远程/DB 查询，彼此无共享可变状态；真正需要串行的是"事件顺序"和"上下文写入顺序"，而不是执行本身。
- **有界**：`agentflow.tool.parallel.enabled`（默认 true）+ `agentflow.tool.parallel.max-concurrency`（默认 4），实现为 `Flux.fromIterable(monos).flatMap(m -> m, maxConcurrency)`。在途工具数不超过上限，避免一轮 20 个工具把连接池与下游打满。关闭开关后退回 `Flux.concat`，保留旧串行语义可回退。
- **顺序保证**：
  1. 所有 `tool.call` 事件在开跑前按模型给出的原始顺序先发完，前端可以先渲染全部调用占位；
  2. `tool.result` 按实际完成顺序发送，前端用 `callId` 关联，允许乱序；
  3. 每个执行结果与原始下标绑定，整轮结束后按原始顺序排序再写入上下文，模型看到的 `role=tool` 消息顺序始终稳定，复现性不因调度抖动而改变。
- **失败降级**：单个工具异常转为一条失败结果（`error` 文本 + 错误码）回灌，模型据此自我纠正；只有本轮全部调用失败才以 `TOOL_ALL_FAILED` 向上抛错，避免把"部分可用"误判为整轮失败。
- **并发安全**：重复调用检测在并行前顺序预检；`RunContext.incToolCall()` 改 `AtomicInteger`，指纹表改 `ConcurrentHashMap.merge`，`emit` 加锁串行化，保证并发下计数不丢、重复检测不漏、SSE seq 单调。

对应测试：`ReActLoopParallelToolsTest`（乱序完成/顺序组装/单点失败/全部失败/并发上限/关闭并行）、`LoopGuardTest`（并发重复检测与计数）。

## 长期记忆闭环（RAG）

长期记忆不再只是"写好了没人调用"的基础设施，而是两条真实入口：

1. **工具自主召回/写入**：`memory_recall(query, topK=5)` 转向量后走 `VectorMemory` 召回并按相似度返回；`memory_write(content, tags)` 经 `EmbeddingService`（先查 `EmbeddingCache`）落 pgvector 并做哈希去重。工具通过 `@Tool` 由 `ToolRegistry` 统一注册，授权仍走既有 `t_tool_grant` 机制，不写死。租户来自 `RunContext` 框架注入，不作为模型可见参数，避免模型伪造租户读写他人记忆。
2. **预检索注入**：每轮 run 开跑前用最后一条 user 消息召回 Top-K（`agentflow.agent.memory-pre-retrieval.enabled/top-k`），拼成 system prompt 的独立小节。召回异常一律降级为"无记忆"继续执行，向量库故障不影响 run 成功。
3. **线程模型**：`VectorMemory` 是阻塞 JDBC，内部已 `subscribeOn(boundedElastic)`；工具 handler 本身也由 `ToolExecutor` 调度到 boundedElastic，事件循环不被阻塞。

对应测试：`MemoryToolsTest`（空记忆/命中）、`ReActLoopMemoryTest`（注入命中/召回失败降级）。

## 上下文预算

上下文不是越大越好，而是按预算缝出来的。`agentflow.agent.default-token-budget=32000`，分配比例：

| 区段 | 比例 | 说明 |
|---|---|---|
| system | 0.10 | 系统提示，顺序固定以命中供应商前缀缓存 |
| tools | 0.15 | 工具 Schema 定义 |
| history | 0.45 | 历史消息 + 记忆召回结果 |
| output | 0.30 | 预留给本轮输出（`maxTokens`） |

裁剪策略按 token 不按条数：一条工具返回可能 5000 token，按条数裁会爆窗。流程：整轮滑窗裁剪最老轮次 → 仍超预算 90%（`compressor-trigger-ratio`）触发 `ContextCompressor` 摘要压缩（摘要上限 500 token）→ 关键事实（计划、工具结果）落结构化字段，不依赖自然语言摘要保真。单步另有 `maxTokens` 与全局 `token_budget` 双层防 token 爆炸。

## 记忆

- **短期记忆**：`SessionMemory` 维护 session / message / run 三层模型，会话是逻辑概念，run 是一次执行；历史按轮次入库供滑窗取用。
- **长期记忆**：`VectorMemory` 基于 pgvector（HNSW 索引）做召回，余弦相似度阈值 0.75（`recall-threshold`），Top-K 5 条（`recall-top-k`），并按时效衰减（`weight-lambda=0.01`）加权；写入侧做哈希去重，embedding 经 `EmbeddingCache` 缓存，同会话并发首包用 `SingleFlight` 防击穿。
- **摘要**：`Summarizer` 负责超长历史压缩，摘要本身限 500 token，避免摘要反向撑爆预算。

## 断点续跑

run 全量状态（当前步骤、计划、已执行工具、累计 token）持久化在 `state_json`；执行实例持有 60 秒租约（`agentflow.run.lease-seconds`）并周期性续期。实例宕机后租约过期，`RunRecoveryJob`（每 5 秒一批，批量 50）用 `SELECT ... FOR UPDATE SKIP LOCKED` 抢占到期 run，从 `state_json` 恢复现场继续执行。LLM 调用本身不能续传，只能整步重发，因此单步成本受控、工具必须幂等——这正是 `(run_id, tool_call_id)` 唯一约束存在的原因。

**LoopGuard 四道闸**（防死循环）：

| 闸 | 阈值 | 触发行为 |
|---|---|---|
| 最大步数 | 12 | 终止，输出"已达最大步数" |
| 工具调用总数 | 8 | 终止 |
| 墙钟超时 | 120s | 终止，run 转 `TIMEOUT` |
| 重复调用指纹 | 连续 2 次相同 | 对 `(工具, 参数)` 做哈希，连续相同即终止 |

## 能力对照表

| # | 能力 | 实现要点 |
|---|---|---|
| A1 | ReAct 循环 | Thought→Action→Observation，上限 12 步；工具报错回灌自我纠正 |
| A2 | Plan-Execute | 先出结构化计划再逐步执行，计划落 `state_json` |
| A3 | 工具调用 | JSON Schema 校验 + 白名单 + 超时 + `(run_id, tool_call_id)` 幂等 |
| A4 | 上下文工程 | token 预算分配 + 整轮裁剪 + 超阈值摘要（500 token 上限） |
| A5 | 长期记忆 | pgvector HNSW 召回 + 时效衰减 + 哈希去重；`memory_recall`/`memory_write` 工具自主读写 + 开跑前预检索注入 |
| A6 | 流式输出 | SSE 逐 token，TTFT 单独埋点 |
| A7 | 断线续传 | `Last-Event-ID` + Redis 事件回放（500 条 / 30 分钟） |
| A8 | 中断恢复 | 状态机 + 60 秒租约 + `SKIP LOCKED` 抢占 + 断点续跑 |
| A9 | 人工审批 | 高风险工具挂起，审批后恢复/终止 |
| A10 | 结构化输出 | JSON mode + 校验失败带错误信息回灌重试 |
| A11 | 多轮会话 | session / message / run 三层模型 |
| A12 | 取消传播 | 客户端断开 → 级联取消，回收模型连接、信号量、配额预扣 |
| A13 | 工具级并行 | 同轮工具调用有界并行（默认 4），事件乱序、上下文顺序可复现，单点失败降级 |
