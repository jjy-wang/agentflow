-- 建索引：索引列与查询模式匹配。
-- 注意：不使用 CREATE INDEX CONCURRENTLY —— Flyway 迁移期间存在 idle-in-transaction 连接时，
-- 并发建索引会等待其 virtualxid 而永久阻塞，导致首次部署迁移卡死。空库/首次部署直接建索引即可。
CREATE INDEX IF NOT EXISTS idx_call_tenant_time ON t_agent_step(run_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_usage_day ON t_quota_usage(stat_day);
