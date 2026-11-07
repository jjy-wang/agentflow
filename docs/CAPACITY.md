# CAPACITY —— 容量规划

## 容量公式

单实例 run 吞吐（runs/s）受三道约束，取最小者：

```
吞吐 = min(
    模型舱壁吞吐   = Σ(模型 max-concurrency) / 平均 run 时长(s),
    TPM 配额吞吐   = Σ(模型 tpm-limit) / 60 / 平均 run token 消耗,
    CPU 吞吐       = 可用核数 × 1000 / 单 run CPU 毫秒
)

SSE 连接容量 = min(
    文件句柄上限（ulimit -n，生产建议 65535）,
    堆内存 / 单连接占用（写缓冲 256 × 单事件均值 + 连接对象）,
    每租户配额 200 × 租户数上限
)
```

PG 连接：实例数 × (rw 20 + ro 20) 必须 < PG `max_connections`（当前 300），两实例合计 80，余量充足；扩容到 7 个实例时触及上限，需同步调大 PG 或引入连接池中间件。

## 实测值代入

以 `docs/BENCHMARK.md` 本轮实测中位数代入：

| 参数 | 取值 | 来源 |
|---|---|---|
| 平均 run 时长 | 2.41 s（P50） | BENCHMARK S2 run_e2e_ms |
| 平均 run token 消耗 | 968（input 812 + output 156） | `llm.tokens.total` 差值换算 |
| mock-large 舱壁 | 64 并发 | `application.yml` |
| mock-large TPM | 200000 tokens/min | `application.yml` |
| 单实例 CPU 占用（拐点处） | < 20% | BENCHMARK S3/S6 |

代入计算：

- **舱壁吞吐** = (64 + 128) / 2.41 ≈ 79.7 runs/s；
- **TPM 吞吐** = (200000 + 400000) / 60 / 968 ≈ 10.3 runs/s；
- **结论：TPM 先成为瓶颈**，与 BENCHMARK 拐点观测（平台期约 10 runs/s、`reason=tpm` 拒绝陡增）一致。此时增加实例数对吞吐无效，只能申请更高供应商配额或切更便宜的模型；
- 反推：若供应商把 TPM 提到 2M tokens/min，则瓶颈让位给舱壁（约 80 runs/s），再往上才轮到 CPU。

## 扩容建议

1. **连接型扩容**：SSE 连接接近 1 万/实例或内存 > 70% 时加实例；连接数与 CPU 解耦，扩容效果线性。
2. **吞吐型扩容**：先确认瓶颈不是 TPM/舱壁（否则白扩）；CPU 拐点后加实例，PG 连接随实例数线性增长，接近 300 前调整 `max_connections` 或降池。
3. **模型侧扩容**：舱壁拒绝率 > 1% 且 TPM 有余量时，调大对应模型 `max-concurrency`；供应商 429 时只能升配额或加模型做路由分流。
4. **Redis / PG**：Redis 令牌桶 Lua 单次 < 1ms，单机可支撑数万次/秒，非瓶颈；PG 慢查询靠 `pg_stat_statements` 与 200ms 慢日志监控，向量检索重负载迁移到只读从库。

## 上限

当前单套部署（app×2 + 单 PG + 单 Redis）的声明上限：

- SSE 连接：2 万（1 万/实例，须 `ulimit -n 65535`）；
- run 吞吐：受 mock 模型配额约束，生产环境以真实供应商 RPM/TPM 合同值重算；
- PG 连接：实例数硬上限 7 个（300 / 40），超过须先扩 PG；
- 事件回放：最近 500 条 / 30 分钟（Redis List），超出窗口的重连不保证补齐——这是已知上限，正确做法是持久化事件流（见 `docs/adr/0005-状态机与租约.md` 讨论）。
