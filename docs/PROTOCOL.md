# PROTOCOL —— 流式协议与接口约定

## 事件类型

SSE 事件通过 `event:` 字段区分类型，`id:` 为单调递增事件序号（心跳除外）：

| 事件 | 含义 | 载荷要点 |
|---|---|---|
| `run.started` | run 创建并开始执行 | runId、mode、model |
| `step.started` | 进入新一轮 ReAct 步骤 | step 序号 |
| `llm.delta` | 模型逐 token 增量输出 | `{"delta": "..."}` |
| `tool.call` | 模型发起工具调用 | 工具名、toolCallId、参数（脱敏） |
| `tool.result` | 工具执行完成 | toolCallId、结果摘要（截断） |
| `run.waiting_approval` | 高风险工具挂起等审批 | approvalId、step、tool、callId、args、pending[] |
| `run.succeeded` | run 成功结束 | 最终答案、token 用量 |
| `run.failed` | run 失败 | 错误码与消息 |
| `run.canceled` | run 被取消（客户端断开/主动取消） | 取消原因 |
| `ping` | 应用层心跳，15 秒一条 | `{}`，id 固定为 0 |
| `stream.lagged` | 慢消费者滞后提示：该连接已丢弃最旧事件 | `{"lagged":true,"lastEventId":1,"dropped":5}`，id 等于建议的 `lastEventId` |

事件流在 `run.succeeded` / `run.failed` / `run.canceled` 后自然完结；`run.waiting_approval` 不终止流，审批通过后继续推送。

### 慢消费者滞后提示（`stream.lagged`）

服务端为每条 SSE 连接维护独立有界缓冲（`agentflow.stream.write-buffer-size`，默认 256 条）。消费速度跟不上时按 DROP_OLDEST 丢弃最旧事件、优先保住新事件时效，并在下一条事件前补发一条 `stream.lagged`：

- `lagged: true` 表示该连接已发生丢弃，后续事件 id 可能出现不连续；
- `lastEventId` 是服务端建议的重连起点（该连接已成功消费到的最大事件 id），也是本条提示事件的 `id:` 字段；
- 客户端应以 `Last-Event-ID: <lastEventId>` 重连 `GET /v1/agent/runs/{runId}/events`，服务端从 PG 回放缺口，事件本身不会真正丢失；
- 连续丢弃超过 `agentflow.stream.max-lag-events`（默认 200）时服务端主动断开该连接，客户端按同样方式重连即可；
- 提示事件不写入 `t_run_event`、不进入业务事件序列，回放侧会按类型过滤，正常消费者永远不会收到。

## 错误码

统一错误以 HTTP 状态码 + JSON body 返回，业务错误码为负数：

| 错误码 | HTTP | 含义 | 处置建议 |
|---|---|---|---|
| `-32001` | 401 | API Key 缺失或无效 | 检查 `X-Api-Key` 头 |
| `-32002` | 403 | 租户无权限访问该资源 | 确认 runId 归属 |
| `-32003` | 400 | 请求参数校验失败 | 按 message 修正 body |
| `-32004` | 503 | 模型舱壁已满（信号量拒绝） | 退避重试或换模型 |
| `-32005` | 429 | RPM/TPM 配额超限 | 降速重试；检查配额用量 |
| `-32006` | 409 | run 状态不允许该操作（重复取消等） | 查询 run 状态 |
| `-32007` | 504 | 模型调用超时 | 重试；持续超时查模型侧 |
| `-32009` | 409 | 同一 `Idempotency-Key` 的重复请求（处理中或已完成且不可重放） | 不要重试；`/runs` 可直接使用首次响应，`/runs/stream` 请查询 run 状态 |
| `-32500` | 500 | 服务内部错误 | 带 traceId 查日志 |

## 幂等请求头

`POST /v1/agent/runs` 与 `POST /v1/agent/runs/stream` 支持 `Idempotency-Key` 请求头（单次请求唯一，建议 UUID；最长 128 字符）。服务端以 `idem:{tenantId}:{key}` 在 Redis 建状态机，TTL 默认 10 分钟：

- 首次请求：写入 `IN_PROGRESS` 后正常执行业务。
- 重复请求且首次仍在处理中：`409`，body 为 `{"code":-32009,"message":"重复请求正在处理中"}`，不会重复创建 run、重复预扣配额。
- 重复请求且首次已完成：`/runs` 返回首次的 HTTP 状态与响应体，并带 `Idempotency-Replayed: true`；`/runs/stream` 的 SSE 响应不可重放，返回 `409 -32009`。
- 首次请求业务失败（异常 / 4xx / 5xx）会释放 key，客户端可用同一 key 安全重试。
- 服务端默认不强制该头（`agentflow.idempotency.require-key=false`）；开启后缺失返回 `400 -32003`。
- Redis 不可用时按 fail-open 放行并记录 WARN，此时幂等不生效但主流程不受影响。

```bash
curl -X POST http://localhost:8080/v1/agent/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Api-Key: demo-key-123456' \
  -H 'Idempotency-Key: 6f1c2b8e-4f6a-4c1e-9d0b-2a7f3c5e8d11' \
  -d '{"query":"汇总最近的高危漏洞","model":"mock-large"}'
```

## 重连语义

- 每个业务事件带单调递增 `id`；事件发布时由 `RunEventBus` 双写 PG `t_run_event`（`(run_id, seq)` 唯一幂等），并保留最近 500 条在 Redis List（TTL 30 分钟，见 `EventReplayStore`）作为热备。
- 客户端重连时在请求头带 `Last-Event-ID: <最后收到的 id>`，服务端先回放 `id > Last-Event-ID` 的事件（PG 按 `run_id + seq` 查询为主，PG 失败或无数据时降级 Redis 短窗口），再无缝接上实时事件流（`Flux.concat(replay, live)`）。
- 心跳 `ping` 的 id 为 0，不参与单调序列、不落库，客户端不要把 0 当作 `Last-Event-ID` 回传。
- PG 持久化回放窗口覆盖 run 全量事件：实例重启后仍可断点续传回放；保留期策略为随 run 保留、由运维定时任务按 `created_at` 清理（建议 7 天）。仅在 PG 与 Redis 同时不可用的极端场景下回放缺失，客户端应改用 `GET /v1/agent/runs/{runId}` 拉取最终状态（见 `docs/adr/0005-状态机与租约.md`）。
- 断线默认触发级联取消（run 转 `CANCELED`）；需要"断开继续跑"的长任务，用 `POST /v1/agent/runs` 异步创建 + 轮询状态 / 重新订阅事件。

## 请求示例

### 流式执行（边跑边推）

```bash
curl -N -X POST http://localhost:8080/v1/agent/runs/stream \
  -H 'Content-Type: application/json' \
  -H 'X-Api-Key: demo-key-123456' \
  -d '{"query":"查询 10.0.0.1 这台主机的情况","model":"mock-large"}'
```

响应（节选）：

```
id: 1
event: run.started
data: {"runId":"r-8f2c...","mode":"react","model":"mock-large"}

id: 2
event: step.started
data: {"step":1}

id: 3
event: llm.delta
data: {"delta":"正在"}

id: 4
event: llm.delta
data: {"delta":"查询主机"}

id: 9
event: tool.call
data: {"tool":"asset_search","toolCallId":"tc-1","args":{"ip":"10.0.0.1"}}

id: 10
event: tool.result
data: {"toolCallId":"tc-1","summary":"命中 1 台主机"}

id: 15
event: run.succeeded
data: {"answer":"...","tokens":{"input":812,"output":156}}
```

### 异步创建 + 断线重连回放

```bash
# 1) 异步创建，立即拿到 runId
curl -X POST http://localhost:8080/v1/agent/runs \
  -H 'Content-Type: application/json' -H 'X-Api-Key: demo-key-123456' \
  -d '{"query":"汇总最近的高危漏洞","model":"mock-large"}'
# => {"code":0,"message":"OK","data":"r-8f2c..."}

# 2) 订阅事件；断线后带 Last-Event-ID 重连
curl -N http://localhost:8080/v1/agent/runs/r-8f2c.../events \
  -H 'X-Api-Key: demo-key-123456' -H 'Last-Event-ID: 42'

# 3) 轮询状态 / 主动取消
curl http://localhost:8080/v1/agent/runs/r-8f2c... -H 'X-Api-Key: demo-key-123456'
curl -X POST http://localhost:8080/v1/agent/runs/r-8f2c.../cancel -H 'X-Api-Key: demo-key-123456'
```

### 审批（HITL）

高风险工具（`riskLevel >= 3`，如 `asset_delete`）命中时不执行，run 从 `RUNNING` 转 `WAITING_APPROVAL` 挂起，当前 SSE 流保留并推送：

```
id: 21
event: run.waiting_approval
data: {"runId":"r-8f2c...","state":"WAITING_APPROVAL","step":3,"approvalId":88,
       "tool":"asset_delete","callId":"tc-9","args":{"assetId":42},
       "pending":[{"callId":"tc-9","name":"asset_delete","args":{"assetId":42}}]}
```

- `pending[]` 是本轮被整体挂起的工具调用批次（审批前不会执行其中任何一个，避免半执行）。
- `waiting_approval` 不结束当前 SSE 流；审批通过后同一连接继续收到后续 `tool.result` / `run.succeeded`。
- 审批接口按 run 维度操作，租户取自 API Key，只能审批自己租户的 run：

```bash
# 通过：立即从挂起步骤续跑（只补执行 pending 批次，历史已成功工具不重跑）
curl -X POST http://localhost:8080/v1/agent/runs/{runId}/approve \
  -H 'Content-Type: application/json' -H 'X-Api-Key: demo-key-123456' \
  -d '{"approver":"alice","comment":"确认删除"}'

# 驳回：run 转 CANCELED，挂起工具不执行，释放租约并关闭 SSE
curl -X POST http://localhost:8080/v1/agent/runs/{runId}/reject \
  -H 'Content-Type: application/json' -H 'X-Api-Key: demo-key-123456' \
  -d '{"approver":"alice","comment":"资产仍在使用"}'
```

语义约定：

- 同一 run 同时最多一条 `PENDING` 审批单；裁决用 `t_approval.status='PENDING'` 与 run `state='WAITING_APPROVAL'` 双重 CAS，重复 approve/reject 第二次返回审批已失效（`APPROVAL_NOT_FOUND` / `APPROVAL_STATE_CONFLICT`），不会二次执行。
- 跨租户审批按 `RUN_NOT_FOUND` 拒绝。
- 超过 `agentflow.approval.timeout`（默认 30 分钟）未裁决：由恢复任务的对账扫描，或审批入口的惰性判定，把审批单与 run 收敛为 `TIMEOUT`（`error_code=APPROVAL_TIMEOUT`），释放租约并关闭 SSE。
- 审批通过后由 `RunService.resumeAfterApproval` 抢占 `PENDING→RUNNING`；若已被其他实例接管则放弃，保证同一 run 只有一个执行体。
- 挂起态与待执行工具批次持久化在 `state_json`，进程重启后仍可恢复；同一 `toolCallId` 受 `(run_id, tool_call_id)` 唯一约束保护，补执行不会重复产生副作用。
