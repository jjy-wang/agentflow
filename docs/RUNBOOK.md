# RUNBOOK —— 告警处置手册

6 条核心告警，每条给出定位命令、处置步骤与升级路径。告警规则基于 Prometheus 指标，全部指标见 `AgentMetrics`（标签只含低基数枚举值，禁止 runId/tenantId 入标签，由 `MetricGuard` 启动期校验）。

## 告警 1：TTFT P95 超阈值

**规则**：`histogram_quantile(0.95, rate(agent_ttft_bucket[5m])) > 1500`（ms），持续 5 分钟。

**定位命令**：

```bash
# 1. 看是否限流排队
curl -s localhost:8080/actuator/prometheus | grep quota_rejected_total
# 2. 看模型错误与舱壁
curl -s localhost:8080/actuator/prometheus | grep -E 'llm_error_total|agent_ttft'
# 3. 看 GC 与线程
curl -s localhost:8080/actuator/threaddump | head -100
docker exec af-app1 jstat -gc 1 1000 5
```

**处置步骤**：
1. `quota.rejected.total` 增长 → 是配额排队，按告警 3 处理；
2. `llm.error.total{type=timeout}` 增长 → 上游模型抖动，确认 mock/供应商状态，必要时 `ModelRouter` 切到健康模型；
3. GC 频繁（Full GC > 1 次/分钟）→ 降载或扩容实例。

**升级路径**：值班后端 → 模型平台负责人（上游抖动）→ 架构组（需调舱壁/配额参数）。

## 告警 2：配额拒绝率突增

**规则**：`rate(quota_rejected_total[5m]) / rate(agent_run_total[5m]) > 0.05`，持续 5 分钟。

**定位命令**：

```bash
# 看是哪个维度在拒绝（rpm 还是 tpm，哪个模型）
curl -s localhost:8080/actuator/prometheus | grep quota_rejected_total
# 看 Redis 桶余量
docker exec af-redis redis-cli --scan --pattern 'quota:*'
docker exec af-redis redis-cli TIME
```

**处置步骤**：
1. 确认是真实流量高峰还是异常调用方（按 API Key 维度查访问日志）；
2. `reason=rpm`：短期可接受，引导调用方退避；持续超限申请提额；
3. `reason=tpm`：检查是否有超大 `maxTokens` 请求（预扣放大），必要时对该调用方单独限额；
4. 确认 Redis 可用：Redis 故障时限流已降级为本地近似（`LocalRateLimiter`），修 Redis 优先。

**升级路径**：值班后端 → 容量负责人（提额/降配）→ 商务（供应商配额）。

## 告警 3：模型错误率升高

**规则**：`rate(llm_error_total[5m]) > 0.5`（次/秒），或错误率 > 2%，持续 3 分钟。

**定位命令**：

```bash
curl -s localhost:8080/actuator/prometheus | grep llm_error_total
# 按 type 区分：timeout / 5xx / parse
docker logs af-mock-llm --tail 200        # 压测环境
curl -s http://localhost:9000/health      # mock-llm 健康
```

**处置步骤**：
1. `type=timeout` 主导 → 上游慢，检查模型实例与网络；超 `ttft-timeout-ms` 的调用会被快速失败保护；
2. `type=5xx` 主导 → 上游故障，`ModelRouter` 摘除该模型，流量切到备用模型；
3. `type=parse` 主导 → 供应商协议变更，按 ADR 0002 跟进 `OpenAiCompatibleClient` 解析逻辑。

**升级路径**：值班后端 → 模型平台负责人 → 供应商工单。

## 告警 4：慢客户端踢除率过高 / SSE 连接异常

**规则**：`rate(sse_slow_client_closed_total[10m]) > 1`（次/秒），或单实例 SSE 连接数 > 8000。

**定位命令**：

```bash
curl -s localhost:8080/actuator/prometheus | grep -E 'sse_slow_client|backpressure_drop'
# 实例当前连接数
ss -s | head -5
docker exec af-app1 sh -c "netstat -an | grep :8080 | grep ESTABLISHED | wc -l"
```

**处置步骤**：
1. 踢除率高 = 下游网络差或客户端不消费：确认客户端实现按 `docs/PROTOCOL.md` 持续读取；
2. `backpressure.drop.total` 同步上升 → 上游 TPS 过高，对该模型降 `X-Mock-Tps`/调低输出速率；
3. 连接数接近 1 万 → 按 `docs/CAPACITY.md` 扩容实例，并确认 `ulimit -n 65535` 已生效。

**升级路径**：值班后端 → 客户端团队（消费逻辑）→ 容量负责人（扩容）。

## 告警 5：恢复任务积压（实例接管不及时）

**规则**：PG 中 `state='RUNNING' AND lease_expires_at < now() - interval '120 seconds'` 的 run 数 > 0。

**定位命令**：

```bash
# 查到期的 run
docker exec af-postgres psql -U agentflow -d agentflow -c \
  "SELECT run_id, state, lease_owner, lease_expires_at FROM agent_run WHERE state='RUNNING' AND lease_expires_at < now();"
# 看恢复任务日志
docker logs af-app1 2>&1 | grep RunRecoveryJob | tail -50
```

**处置步骤**：
1. 有到期 run 但无接管 → 检查存活实例：`docker compose ps`，至少一个实例在线才能接管；
2. 恢复任务报错 → 看日志堆栈，常见为 PG 连接耗尽（查 `pg_stat_activity`）；
3. 接管后 run 仍失败 → 查该 run 的 `state_json` 是否损坏，必要时人工置 `FAILED` 并通知调用方重试；
4. 双实例同时占有同一 run 属严重事故（`SKIP LOCKED` 失效），立刻下闸停恢复任务并上报。

**升级路径**：值班后端 → DBA（PG 锁/连接）→ 架构组（租约机制评审）。

## run 超时排查（RUN_TIMEOUT / STEP_TIMEOUT）

**规则**：`t_agent_run` 中 `state='TIMEOUT'` 的 run 数在 10 分钟内突增，或同一模型 `run_timeout` 日志持续出现。

**定位命令**：

```bash
# 1. 看最近超时的 run：错误码区分总预算（RUN_TIMEOUT）与单步连续超时（STEP_TIMEOUT）
docker exec af-postgres psql -U agentflow -d agentflow -c \
  "SELECT run_id, state, error_code, error_msg, step_index, token_used, started_at, finished_at
   FROM t_agent_run WHERE state='TIMEOUT' AND started_at > now() - interval '1 hour'
   ORDER BY started_at DESC LIMIT 20;"

# 2. 看超时前后的分步事件（是否有 STEP_TIMEOUT_RETRY，卡在第几步）
docker exec af-postgres psql -U agentflow -d agentflow -c \
  "SELECT seq, type, data FROM t_run_event WHERE run_id='<runId>' ORDER BY seq DESC LIMIT 30;"

# 3. 看应用日志：总预算 / 单步超时 / 模型首包
docker logs af-app1 2>&1 | grep -E "run 总时长超过预算|单步超时|单步连续超时|TTFT" | tail -100
```

**处置步骤**：
1. `error_code=RUN_TIMEOUT` → 看 `error_msg` 里的已用时长；若接近 `max-duration-ms`（默认 300s），确认是模型整体变慢还是步数过多卡在循环，先查 `step_index` 与工具耗时；
2. `error_code=STEP_TIMEOUT` → 单步连续两次超过 `step-timeout-ms`（默认 60s），优先查该步的 LLM TTFT 与工具下游（工具本身有独立超时）；重试事件 `STEP_TIMEOUT_RETRY` 说明第一次已按可重试处理；
3. 确认下游健康后才考虑调大 `agentflow.agent.max-duration-ms` / `step-timeout-ms`，且必须保持 step < max-duration（启动期会自动收敛并 WARN）；
4. 超时 run 已由统一收尾路径释放租约并关闭 SSE，配额按实际用量结算；无需手工补偿，若发现配额未回退，按 `QuotaService` 的"配额结算失败，需对账补偿"日志处理；
5. 大量 STEP_TIMEOUT 且集中在同一模型 → 按"告警 1：TTFT P95 超阈值"继续排查上游或舱壁排队。

**升级路径**：值班后端 → 模型网关负责人（上游延迟）→ 架构组（预算阈值评审）。

## 报告任务积压 / 死信排查

**规则**：`report.queue` 的 `messages_ready` 持续 > 100 或 `report.queue.dlq` 的 `messages` > 0，持续 10 分钟。

**拓扑**：`report.exchange --report.generate--> report.queue`，主队列挂 `x-dead-letter-exchange=report.exchange.dlx`，死信落到 `report.queue.dlq`；任务状态在 PG 表 `report_task`（无 `t_` 前缀）。

**定位命令**：

```bash
# 1. 看主队列与死信队列深度、消费者数
docker exec af-rabbit rabbitmqctl list_queues -p /agentflow \
  name messages messages_ready messages_unacknowledged consumers

# 2. 看任务状态分布与最老的积压任务
docker exec af-postgres psql -U agentflow -d agentflow -c \
  "SELECT status, count(*), min(created_at), max(created_at) FROM report_task GROUP BY status ORDER BY status;"
docker exec af-postgres psql -U agentflow -d agentflow -c \
  "SELECT task_id, status, created_at FROM report_task WHERE status IN ('PENDING','RUNNING') ORDER BY created_at LIMIT 20;"

# 3. 看消费者日志（重试、死信、参数非法、DB 异常）
docker logs af-app1 2>&1 | grep ReportTaskConsumer | tail -100
```

**处置步骤**：

1. `messages_ready` 上涨但 `consumers=0`：消费线程未启动或全部被阻塞。先确认实例在线（`docker compose ps`），再查 `ReportTaskConsumer` 日志与 PG 连接池（`pg_stat_activity`）；必要时重启实例或临时调大 `spring.rabbitmq.listener.simple.max-concurrency`。
2. `PENDING` 堆积且日志持续重试：按 `reason` 区分——`DataAccessException` 查 DB；`RetryableException`/`IOException` 查下游与产物目录 `agentflow.report.output-dir` 的磁盘空间。
3. 长期 `RUNNING`：说明消费线程执行中崩溃或实例宕机。确认该任务确实无消费者后，执行
   `UPDATE report_task SET status='PENDING' WHERE task_id='<taskId>' AND status='RUNNING';`
   再从 RabbitMQ 管理台按原 payload 重投到 `report.exchange`（routing key `report.generate`）。
4. `report.queue.dlq` 有消息：先不要 purge。管理台 `http://localhost:15672`（本地账号 `agentflow/agentflow`）→ Queues → `report.queue.dlq` → Get messages（requeue=true）查看 `x-death` 与 payload；修复根因（参数非法则修 payload，DB/下游故障则恢复依赖）后，把对应任务重置为 `PENDING`，再经 Exchanges → `report.exchange` 以 routing key `report.generate` 重投。
5. 重投后确认：主队列与死信队列深度归零，`report_task` 中该 `task_id` 变为 `SUCCEEDED`，且 `agentflow.report.output-dir` 下存在 `{taskId}.json`。

**升级路径**：值班后端 → DBA（DB 锁/连接）→ 下游报表渲染负责人（5xx/产物存储）→ 架构组（队列容量与重试参数评审）。

## 审计链校验

`t_run_audit` 是 prev_hash 链式结构：每行 `row_hash = sha256(trace_id|tenant_id|run_id|action|detail|prev_hash|created_at)`，篡改任一历史行都会断链。`audit/AuditChainVerifier` 是内部工具类（无 REST 端点），用于离线/运维场景验证某次 run 的审计完整性。

**操作步骤**：

```bash
# 1. 先看目标 run 的审计行数与时间范围
docker exec af-postgres psql -U agentflow -d agentflow -c \
  "SELECT id, action, prev_hash, row_hash, created_at FROM t_run_audit WHERE run_id='<runId>' ORDER BY id;"

# 2. 在应用实例内执行校验（jshell  attach 或运维一次性 Bean 调用）：
#    new AuditChainVerifier(jdbcTemplate).verify("<runId>").block()
#    返回 AuditChainReport：total / firstMismatchIndex / firstMismatchId / passed / message
```

**报告解读与处置**：
1. `passed=true`：链完整，无需处置；
2. `passed=false` 且 `message` 含"链式引用断裂"：`firstMismatchId` 行的 `prev_hash` 与上一行 `row_hash` 不一致 → 该行被插入或前序行被删除/替换，按安全事件处理，冻结该 run 并排查写入路径；
3. `passed=false` 且含"内容哈希不匹配"：行字段（detail/action/created_at 等）被原地篡改 → 同上按安全事件处理；
4. 校验只读不改库；定位到篡改位置后，用 `created_at` 前后时间窗对照应用日志与 DB 审计账号登录记录定人。

**升级路径**：值班后端 → 安全负责人（篡改定性）→ DBA（binlog/WAL 溯源）。

## 配额异常排查 / 手动触发对账

**适用场景**：租户反馈"额度掉得比实际用量快"、`quota.reconcile.failed.total` 增长、Redis 重启/抖动后怀疑预扣泄漏。

**先看指标与日志**：

```bash
# 扫描量、退款量、CAS 跳过量（均为固定名称，无高基数标签）
curl -s localhost:8080/actuator/prometheus | grep -E 'quota_reconcile_(scanned|refunded|skipped|failed)_total'

# 退款失败会带 reservation id / runId / model / tokens，便于人工补退
docker logs af-app1 2>&1 | grep -E '配额对账(退款失败|完成)' | tail -50
```

**看账本分布与陈旧明细**：

```bash
docker exec af-postgres psql -U agentflow -d agentflow -c \
  "SELECT status, count(*), min(created_at), max(created_at) FROM t_quota_reservation GROUP BY status ORDER BY status;"

# 非终态且超过 5 分钟：下一轮对账（≤ interval-ms）会自动处理
docker exec af-postgres psql -U agentflow -d agentflow -c \
  "SELECT id, run_id, model, reserved_tokens, status, created_at FROM t_quota_reservation \
   WHERE status IN ('INIT','RESERVED') AND created_at < now() - interval '5 minutes' ORDER BY created_at LIMIT 50;"
```

**手动触发对账**（无 REST 端点，二选一）：

1. 把待处理行的时钟回拨到 `stale-after` 之前，等下一轮扫描（最简单，无需重启）：
   `UPDATE t_quota_reservation SET created_at = now() - interval '1 hour', updated_at = now() - interval '1 hour' WHERE id = <id> AND status IN ('INIT','RESERVED');`
2. 临时调小 `agentflow.quota.reconcile.stale-after`（如 `0s`）并重启实例；确认处理完后改回 `5m`。`enabled: false` 可整体停掉自动退款。

**退款失败的人工补退**（日志出现"账本已置 REFUNDED，需人工补退"时）：账本状态已经是 `REFUNDED`，不会重复退款；Redis 恢复后按日志里的 `model` / `tokens` / `tpm_limit` 直接执行与 `quota_settle.lua` 等价的退回：

```bash
# KEYS[1]=quota:tpm:<model>，ARGV[1]=需要退回的 token，ARGV[2]=桶容量
docker exec af-redis redis-cli EVAL "\
local r=tonumber(ARGV[1]) local c=tonumber(ARGV[2]) \
if r>0 then local t=redis.call('TIME') local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000) \
local b=redis.call('HMGET',KEYS[1],'tokens','ts') local tokens=tonumber(b[1]) or 0 local ts=tonumber(b[2]) or now \
redis.call('HMSET',KEYS[1],'tokens',math.min(c,tokens+r),'ts',ts) end return 1" \
1 quota:tpm:mock-large 300 200000
```

补退后核对：`HGETALL quota:tpm:<model>` 的 `tokens` 已回升，且日志不再出现新的退款失败。若 `INIT` 行长期堆积，优先查账本写入 WARN（"配额账本 INIT 写入失败"）与 PG 连接池，而不是盲目退款——`INIT` 按设计不退款。

**升级路径**：值班后端 → DBA（账本表/连接池）→ 架构组（对账参数与退款策略评审）。
