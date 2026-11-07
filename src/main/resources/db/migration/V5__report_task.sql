-- 报表异步任务表：report_generate 工具提交的任务落库，供异步执行与状态查询
CREATE TABLE report_task (
    task_id    UUID         PRIMARY KEY,
    payload    JSONB        NOT NULL,
    status     VARCHAR(32)  NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 只读账号授权（与 V1/V3 保持一致；集成测试库无该角色时跳过）
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentflow_ro') THEN
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO agentflow_ro;
    END IF;
END $$;
