-- pgvector 扩展必须先于 t_memory.embedding 的 vector 类型创建
CREATE EXTENSION IF NOT EXISTS vector;

-- ===== 租户与鉴权 =====
CREATE TABLE t_tenant (
    id         BIGSERIAL PRIMARY KEY,
    code       VARCHAR(64)  NOT NULL UNIQUE,
    name       VARCHAR(128) NOT NULL,
    tier       SMALLINT     NOT NULL DEFAULT 1,
    status     SMALLINT     NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE t_api_key (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  BIGINT       NOT NULL,
    key_prefix VARCHAR(12)  NOT NULL,
    key_hash   VARCHAR(128) NOT NULL UNIQUE,
    status     SMALLINT     NOT NULL DEFAULT 1,
    expire_at  TIMESTAMPTZ,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX idx_api_key_tenant ON t_api_key(tenant_id);

-- ===== 会话 =====
CREATE TABLE t_session (
    id                BIGSERIAL PRIMARY KEY,
    tenant_id         BIGINT       NOT NULL,
    session_key       VARCHAR(64)  NOT NULL,
    title             VARCHAR(256),
    summary           TEXT,
    summary_upto_step INT          NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, session_key)
);

CREATE TABLE t_message (
    id           BIGSERIAL PRIMARY KEY,
    session_id   BIGINT      NOT NULL,
    tenant_id    BIGINT      NOT NULL,
    role         VARCHAR(16) NOT NULL,
    content      TEXT,
    tool_calls   JSONB,
    tool_call_id VARCHAR(64),
    token_in     INT         NOT NULL DEFAULT 0,
    token_out    INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_message_session ON t_message(session_id, id);

-- ===== Agent 执行 =====
CREATE TABLE t_agent_run (
    id           BIGSERIAL PRIMARY KEY,
    run_id       VARCHAR(32) NOT NULL UNIQUE,
    tenant_id    BIGINT      NOT NULL,
    session_id   BIGINT      NOT NULL,
    mode         VARCHAR(16) NOT NULL DEFAULT 'REACT',
    state        VARCHAR(24) NOT NULL,
    query        TEXT        NOT NULL,
    step_index   INT         NOT NULL DEFAULT 0,
    max_steps    INT         NOT NULL DEFAULT 12,
    token_budget INT         NOT NULL DEFAULT 32000,
    token_used   INT         NOT NULL DEFAULT 0,
    state_json   JSONB,
    version      INT         NOT NULL DEFAULT 0,
    lease_owner  VARCHAR(64),
    lease_until  TIMESTAMPTZ,
    error_code   VARCHAR(32),
    error_msg    VARCHAR(512),
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);
CREATE INDEX idx_run_state_lease ON t_agent_run(state, lease_until);
CREATE INDEX idx_run_tenant_time ON t_agent_run(tenant_id, started_at DESC);
CREATE INDEX idx_run_session     ON t_agent_run(session_id, started_at DESC);

CREATE TABLE t_agent_step (
    id          BIGSERIAL PRIMARY KEY,
    run_id      VARCHAR(32) NOT NULL,
    step_index  INT         NOT NULL,
    kind        VARCHAR(16) NOT NULL,
    tool_name   VARCHAR(64),
    input_json  JSONB,
    output_json JSONB,
    cost_ms     INT         NOT NULL DEFAULT 0,
    code        INT         NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, step_index, kind)
);

-- ===== 工具 =====
CREATE TABLE t_tool (
    id           BIGSERIAL PRIMARY KEY,
    name         VARCHAR(64)  NOT NULL UNIQUE,
    description  VARCHAR(512) NOT NULL,
    input_schema JSONB        NOT NULL,
    risk_level   SMALLINT     NOT NULL DEFAULT 1,
    timeout_ms   INT          NOT NULL DEFAULT 5000,
    enabled      SMALLINT     NOT NULL DEFAULT 1
);

CREATE TABLE t_tool_grant (
    id        BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT      NOT NULL,
    tool_name VARCHAR(64) NOT NULL,
    rpm_limit INT         NOT NULL DEFAULT 60,
    enabled   SMALLINT    NOT NULL DEFAULT 1,
    UNIQUE (tenant_id, tool_name)
);

CREATE TABLE t_tool_invocation (
    id           BIGSERIAL PRIMARY KEY,
    run_id       VARCHAR(32) NOT NULL,
    tool_call_id VARCHAR(64) NOT NULL,
    tool_name    VARCHAR(64) NOT NULL,
    status       VARCHAR(16) NOT NULL,
    result_json  JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, tool_call_id)
);

-- ===== 配额 =====
CREATE TABLE t_quota_usage (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   BIGINT NOT NULL,
    stat_day    DATE   NOT NULL,
    used_runs   INT    NOT NULL DEFAULT 0,
    used_tokens BIGINT NOT NULL DEFAULT 0,
    UNIQUE (tenant_id, stat_day)
);

-- ===== 长期记忆 =====
CREATE TABLE t_memory (
    id           BIGSERIAL PRIMARY KEY,
    tenant_id    BIGINT      NOT NULL,
    session_id   BIGINT,
    content      TEXT        NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    embedding    vector(1024),
    weight       REAL        NOT NULL DEFAULT 1.0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, content_hash)
);

-- ===== 审批 =====
CREATE TABLE t_approval (
    id         BIGSERIAL PRIMARY KEY,
    run_id     VARCHAR(32) NOT NULL,
    step_index INT         NOT NULL,
    tool_name  VARCHAR(64) NOT NULL,
    args_json  JSONB       NOT NULL,
    status     VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    approver   VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at TIMESTAMPTZ,
    UNIQUE (run_id, step_index)
);

-- ===== 审计（链式哈希）=====
CREATE TABLE t_run_audit (
    id         BIGSERIAL PRIMARY KEY,
    trace_id   VARCHAR(32) NOT NULL,
    tenant_id  BIGINT      NOT NULL,
    run_id     VARCHAR(32) NOT NULL,
    action     VARCHAR(32) NOT NULL,
    detail     VARCHAR(512),
    prev_hash  VARCHAR(64),
    row_hash   VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_audit_tenant_time ON t_run_audit(tenant_id, created_at DESC);

-- 只读账号授权（init-ro-user.sql 在表创建前执行，这里补授权）
-- 集成测试的临时库没有 init 脚本，故角色不存在时跳过授权
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentflow_ro') THEN
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO agentflow_ro;
    END IF;
END $$;
