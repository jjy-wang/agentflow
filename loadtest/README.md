# AgentFlow 压测手册（k6）

本目录包含 6 个 k6 压测场景，全部打向 **mock-llm**（`mock-large` / `mock-small`），用于产出 `docs/BENCHMARK.md` 的实测数据与容量拐点。

## 场景清单

| 脚本 | 场景 | 并发模型 | 关注指标 |
|---|---|---|---|
| `baseline.js` | 基线：创建 run | constant-vus 100，2m | TTFT P95 < 1500ms，失败率 < 1% |
| `agent_multi_turn.js` | 多轮 ReAct（含一次工具调用） | constant-vus 50，3m | 端到端 P95 < 8000ms |
| `sse_10k.js` | 1 万 SSE 长连接 | constant-vus 10000，5m | 连接成功率、内存、无 OOM |
| `quota_burst.js` | 配额突发（400/s 超发） | constant-arrival-rate，30s | 429 比例、预扣结算正确性 |
| `backpressure.js` | 背压：上游 500 tps 快吐 | constant-vus 200，2m | 有界缓冲、无 OOM、丢帧计数 |
| `soak.js` | 浸泡：30 分钟稳定性 | constant-vus 30，30m | 内存曲线、GC、连接泄漏 |
| `fault_injection.js` | 故障注入：高延迟/429/5xx 突发/慢 TTFT/token 截断/连接重置 | 6 scenario 错峰，各 20-30s | 降级、重试、舱壁、快速失败是否符合阈值 |

## 压测铁律（必须遵守）

1. 只打 mock-llm，不打真实模型。
2. 每次测量前预热 60 秒。
3. 同场景跑 3 次取中位数。
4. 记录 P50 / P95 / P99，不写平均值。
5. 必须同时记录服务端指标（TTFT、舱壁拒绝、GC、连接数）。
6. 1 万连接场景先调高 `ulimit -n 65535`。

## 运行方式

前置：全栈已启动（`docker compose up -d`），演示 API Key 为 `demo-key-123456`。

```bash
# 基线
k6 run --env BASE_URL=http://localhost:8080 loadtest/baseline.js

# 多轮 Agent（走完整 ReAct 链路）
k6 run --env BASE_URL=http://localhost:8080 loadtest/agent_multi_turn.js

# 1 万 SSE 长连接（先 ulimit -n 65535，客户端机器需足够）
k6 run --env BASE_URL=http://localhost:8080 loadtest/sse_10k.js

# 配额突发
k6 run --env BASE_URL=http://localhost:8080 loadtest/quota_burst.js

# 背压
k6 run --env BASE_URL=http://localhost:8080 loadtest/backpressure.js

# 浸泡 30 分钟
k6 run --env BASE_URL=http://localhost:8080 loadtest/soak.js
```

## 结果记录规范

每个场景跑完后，把以下数据填入 `docs/BENCHMARK.md` 对应小节：

- 客户端侧：k6 输出的 `http_req_duration` / 自定义 Trend 的 P50 / P95 / P99，错误率；
- 服务端侧：`/actuator/prometheus` 抓取的 `agent_ttft`、`agent_run_duration`、`quota_rejected_total`、`sse_slow_client_closed_total`、`backpressure_drop_total`，以及 GC 次数 / 堆内存曲线（`jstat -gc` 或容器 stats）；
- 容量结论：吞吐拐点出现在多少 VU，拐点处的瓶颈（CPU / 连接数 / 舱壁信号量 / 配额）。
