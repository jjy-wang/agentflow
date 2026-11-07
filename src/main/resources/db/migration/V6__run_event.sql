-- 事件流持久化：SSE 业务事件双写落库，实例重启后仍可按 (run_id, seq) 断点回放
CREATE TABLE t_run_event (
    id         BIGSERIAL    PRIMARY KEY,
    run_id     VARCHAR(64)  NOT NULL,
    seq        BIGINT       NOT NULL,
    type       VARCHAR(32)  NOT NULL,
    data       TEXT,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (run_id, seq)
);

-- 回放按 run_id + seq 范围查询，run_id 为前导列
CREATE INDEX idx_run_event_run ON t_run_event(run_id);

-- 只读账号授权（与 V1/V3/V5 保持一致；集成测试库无该角色时跳过）
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentflow_ro') THEN
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO agentflow_ro;
    END IF;
END $$;
