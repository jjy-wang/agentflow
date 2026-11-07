# BENCHMARK —— 实测报告

> 本文所有数字均为 2026-11-07 在本机真实跑出的结果，未跑通的项明确标注「未测」。
> 历史版本中的 k6 场景数据不适用于本次环境，已整体替换。

## 1. 测试环境

| 项 | 值 |
|---|---|
| 机器 | Windows 11 专业版；AMD Ryzen 7 7840H（8C/16T）；15.2 GB 内存 |
| Docker | Engine 29.9.2（API 1.56），Docker Compose v5.6.1 |
| JDK | Eclipse Temurin 17.0.16 |
| 应用 | 单个本地 JVM：`java -Xms256m -Xmx512m -XX:+UseG1GC -jar target/agentflow-1.0.0.jar`（默认 profile，未起 app2） |
| 中间件 | PostgreSQL 16 + pgvector 0.8.7、Redis 7.2-alpine、RabbitMQ 3.13-management-alpine（均为 compose 容器） |
| mock-llm | 容器 `af-mock-llm`（compose 构建，9000 端口），默认 200ms 首包延迟、128 token @ 50 tps |
| 压测工具 | **非 k6**：本机无 k6 且镜像拉取受网络限制，改用 PowerShell 调度 20 个 `curl` 进程并发（每请求读完整 SSE 流，`-w` 取 `http_code` 与 `time_total`） |

`docker compose ps`（实测）：

```
af-mock-llm   Up (healthy)
af-postgres   Up (healthy)   0.0.0.0:5432->5432
af-rabbit     Up (healthy)   0.0.0.0:5672->5672, 0.0.0.0:15672->15672
af-redis      Up (healthy)   0.0.0.0:6379->6379
```

基础设施连通性：`psql` 可连，`vector 0.8.7` 扩展已由迁移创建；`redis-cli ping` = PONG；`rabbitmq-diagnostics ping` = ok，管理端口 15672 返回 200；mock-llm `/health` 返回 200。

## 2. 集成测试（真实 Testcontainers）

命令（关键环境变量）：

```powershell
$env:DOCKER_HOST='npipe:////./pipe/dockerDesktopLinuxEngine'
$env:PATH='<JDK17>\bin;C:\Windows\system32;...'   # 需剔除含 '?' 的异常 PATH 项
mvn -B -Djacoco.skip=true -Dapi.version=1.44 test
```

结果：

| 指标 | 数值 |
|---|---|
| 总用例 | 208 |
| 失败 | 0 |
| 错误 | 0 |
| 跳过 | 0 |
| AgentEndToEndTest | 3 个通过，17.70s（真实拉起 rabbitmq:3.13-alpine / redis:7.2.4-alpine / pgvector/pgvector:pg16） |
| RunRecoveryJobTest | 1 个通过，2.445s（真实 PG） |

## 3. 全链路验证（真实 HTTP）

| 步骤 | 请求 | HTTP | 耗时 | 结果 |
|---|---|---|---|---|
| 健康检查 | `GET /actuator/health` | 200 | — | `{"status":"UP"}` |
| 控制台 | `GET /` | 200 | — | 返回内置 HTML |
| 主链路（README 示例，不带 sessionId） | `POST /v1/agent/runs/stream` | 200 | 5.236 s | 134 条 SSE；`status=SUCCEEDED`（200，29 ms） |
| 幂等主链路 | 同上 + `Idempotency-Key` | 200 | 4.499 s | 134 条 SSE；`status=SUCCEEDED` |
| 重复幂等键 | 同 key 再发 | **409** | 11.8 ms | `{"code":-32009,"message":"重复请求已完成，请勿重复提交"}` |
| 断点回放 | `GET /v1/agent/runs/{id}/events` + `Last-Event-ID: 10` | 200 | 19.6 ms | 124 条事件，首条 `id: 11` |
| 审批挂起 | `POST /v1/agent/runs`，query=删除资产 1 | 200 | 1 s 内 | 状态 `WAITING_APPROVAL`；`t_approval` 新增 `asset_delete/PENDING` |
| 审批前副作用 | `select count(*) from t_asset where deleted=1` | — | — | **0**（未审批不得执行，符合预期） |
| 同意审批 | `POST .../approve`，`{"approver":"e2e"}` | 200 | — | `true`；`t_approval` 变为 `APPROVED` |
| 审批后状态 | `GET .../status` | 200 | 1–2 s | `SUCCEEDED` |
| 审批后副作用 | 同上 SQL | — | — | **1**（审批通过后工具才真正执行） |

主链路 134 条事件构成：`run.started`×1、`step.started`×2、`tool.call`×1、`tool.result`×1、`llm.delta`×128、`run.succeeded`×1。

启动自检：本轮首次启动暴露 `GenAiTelemetry` 双构造器未标注 `@Autowired`，Spring 退化为查找无参构造器，抛 `No default constructor found` 导致**应用完全无法启动**，而当时 207 个单测全绿。已修复，并补 1 个 `ApplicationContextRunner` 装配测试（总用例 208）作为回归保护——这类问题只有容器真的装配一次才会暴露。

## 4. 压测结果（简化压测，非 k6）

方法：每个请求读取完整 SSE 流到结束，统计端到端耗时；预热 2 次后开始计时。

### 4.1 基线：无工具文本链路

- 并发：3；每并发 20 次；共 60 次
- 请求：`POST /v1/agent/runs/stream`（query="你好"，1 次 LLM 调用）

| 指标 | 实测 |
|---|---|
| 总耗时 | 84.84 s |
| 吞吐 | 0.71 req/s |
| P50 / P95 / P99 | 4244 ms / 4258 ms / 4259 ms |
| 最小 / 平均 / 最大 | 4211 ms / 4241 ms / 4259 ms |
| HTTP 非 200 | 0（错误率 0%） |
| 应用 ERROR 日志 | 0 条 |

### 4.2 业务链路：含 1 次工具调用（令牌桶修复后复测）

- 并发：20；每并发 10 次；共 200 次（分 10 轮，每轮 20 个 curl 进程并发，读完整 SSE 流）
- 请求：query="检索内网资产并汇总暴露面风险"（2 次 LLM 调用 + `asset_search`）
- 预热 2 次后开始计时

| 指标 | 实测 |
|---|---|
| 总耗时 | 35.78 s |
| 吞吐 | 5.59 req/s |
| 成功吞吐 | 5.59 runs/s（200 成功 / 35.78 s） |
| P50 / P95 / P99 | 3045 ms / 3076 ms / 3091 ms |
| 最小 / 最大 | 3031 ms / 3110 ms |
| HTTP 200 | 200 / 200 |
| 业务成功 | 200（100%），状态 `SUCCEEDED` |
| 业务失败 | 0（0%） |
| 应用 OOM / ERROR | 0（堆上限 512 MB，压测期间主机可用内存始终 > 5 GB） |

对比修复前同场景：162/200 失败（81%），错误码 `NO_MODEL_AVAILABLE`。修复要点是补齐本地令牌桶的**退还**路径：`LocalRateLimiter` 新增 `refund()`，`QuotaService` 在正常结算与异常/取消路径归还预扣配额，不再只扣不退。

另一个可读信号：P99 − P50 仅 46 ms，最小 3031 ms 与 mock-llm 的固定延迟下界（200 ms 首包 + 128 token @ 50 tps ≈ 2.8 s）基本重合，说明 20 并发下没有出现排队放大，系统是在"等模型"而不是"等自己"。

### 4.3 未测项

k6 场景脚本（baseline/multi_turn/sse_10k/backpressure/soak）、1 万 SSE 长连接、30 分钟浸泡、慢客户端背压、GC/堆内存曲线、多实例（app1/app2）水平扩展：**未测**。

## 5. 瓶颈观察

1. ~~本地令牌桶只扣不退~~（**已修复**）：修复前 `LocalRateLimiter` 容量 = `tpm-limit × local-factor` = 200000 × 1.2 = 240000，每次调用按 `prompt + maxTokens` 预扣，而 `QuotaService.settle()` 只把差额退回 Redis，本地桶没有任何退款入口，持续并下发被抽干后抛 `NO_MODEL_AVAILABLE`（-32004）。现已在 `LocalRateLimiter` 增加 `refund()`，并在正常结算、异常、取消三条路径归还预扣；4.2 复测 200/200 成功即为验证。
2. **拒绝语义仍可改进**：配额拒绝在 SSE 里表现为 `run.failed`，HTTP 仍为 200（流式协议不便改状态码）；调用方目前只能读事件里的 `error_code` 区分限流与模型故障，拿不到 429 做退避。这是已知且未改的项。
3. **PG / Redis / RabbitMQ 不是瓶颈**：修复后 200 请求全部成功，P99 3.09 s 与单请求下界 3.03 s 只差 60 ms，未出现排队放大；该判断来自延迟分布而非采样，比修复前那组压测后的瞬时 CPU 采样更可信。模型舱壁上限 64，20 并发远未打满。
4. **当前真实约束是 mock-llm 的固定输出速率**（128 token @ 50 tps ≈ 2.8 s/次），不是应用侧排队。想验证应用在高并发下的真实上限，需要把 mock-llm 的 token 数与 tps 调高，或换真实模型。

## 6. 结论

- 单实例、堆上限 512 MB 下，20 并发 × 10 轮（200 请求）**全部成功**，吞吐 **5.59 runs/s**，P50/P95/P99 = 3045/3076/3091 ms。
- 修复前同场景 81% `NO_MODEL_AVAILABLE`；根因是本地令牌桶预扣不退还，已修复并经复测验证，不是靠扩容绕过。
- 当前吞吐上限由 mock-llm 的固定输出速率决定（约 3 s/请求），应用侧未观察到排队；要测应用真实上限需提高 mock 的 tps。
- HTTP 层在流式接口上始终 200，压测必须结合 SSE 终态事件（`run.succeeded` / `run.failed`）判定成功率，不能只看状态码。
- 审批链路可用（见第 3 节）：挂起期间不产生副作用，approve 后续跑到 `SUCCEEDED` 且工具恰好执行一次。
- 尚未验证：1 万 SSE 长连接、30 分钟浸泡、慢客户端背压、多实例水平扩展（见 4.3）。
