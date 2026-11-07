# MIGRATION —— 数据库迁移规范

## expand-only 规范

所有迁移只允许"扩张式"变更，禁止一切破坏旧版本代码兼容性的操作。Flyway 管理版本（`V1` ~ `V5` 已落盘），`validate-on-migrate=true`、`out-of-order=false`。

**允许**：

- `CREATE TABLE` / `CREATE INDEX`（并发方式，见下节）；
- `ALTER TABLE ... ADD COLUMN`，新列必须可空或有默认值；
- `GRANT`（每次建表后必须补 `GRANT SELECT ON ALL TABLES IN SCHEMA public TO agentflow_ro;`，保持只读账号可用）；
- 新增枚举值（应用层向前兼容读取）。

**禁止**：

- `DROP COLUMN` / `DROP TABLE` / `RENAME`：旧代码仍在读这些对象，必须走"双写-迁移-清理"多版本流程；
- `ALTER COLUMN` 改类型/收紧约束（如 `varchar(64)→varchar(32)`、`ADD NOT NULL` 无默认值）；
- 大表 `UPDATE` 全量回填：拆批 + 限速，避免锁表与复制延迟；
- 在迁移里写业务逻辑：迁移只做结构变更。

## 并发索引

索引创建必须在线、不阻塞读写：

```sql
-- 正确：并发建索引（Flyway 中需关闭该迁移的事务包装：@Transactional(false) 或单独脚本）
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_agent_run_tenant_state
  ON agent_run (tenant_id, state);

-- 错误：默认 CREATE INDEX 在 PG 会持有 SHARE 锁，阻塞写入
CREATE INDEX idx_x ON t (c);
```

要求：

- 大表（`agent_run`、`agent_step`、`message`、`run_audit`）索引一律 `CONCURRENTLY`；
- `CONCURRENTLY` 不能在事务块内执行，对应 Flyway 迁移脚本需单独成文件并在类路径配置中标记非事务；
- 建索引前用 `pg_stat_user_tables` 评估表规模，超千万行先在低峰窗口执行并通知 DBA；
- pgvector 的 HNSW 索引（`V2__pgvector.sql`）同理会锁资源，需在低峰执行并监控 `pg_stat_progress_create_index`。

## 回滚方式

expand-only 意味着**没有结构回滚，只有代码回滚 + 前向修复**：

1. **代码回滚**：结构是扩张的，旧代码可以直接跑在新结构上——这是 expand-only 存在的意义；
2. **前向修复**：结构写错了，用新的 `V(n+1)` 迁移修正（加正确列、废弃错误列），禁止改已执行的 `V(n)` 脚本（`validate-on-migrate` 会校验 checksum）；
3. **数据回滚**：迁移产生的问题数据用显式修复脚本处理，脚本必须幂等（可重复执行）且带 WHERE 边界，禁止全表无界 UPDATE；
4. **备份兜底**：高风险迁移（大表加列回填）执行前 `pg_dump` 目标表；RPO 由 PG 定期备份与 WAL 归档保证；
5. **演练**：每次发版的迁移先在预发库执行并验证旧版本应用可用，通过后才允许进生产流程（见 `docs/RELEASE.md`）。
