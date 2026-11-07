-- 配额预扣账本：预扣（reserve）与结算（settle）不是原子的，中间崩溃会让 token 永久泄漏。
-- Saga 补偿状态机：INIT（已登记未预扣）→ RESERVED（预扣成功，记录实际预扣量）
--                 → SETTLED（正常结算完成）/ REFUNDED（对账退款）
-- 账本写入是 best-effort：写失败只影响补偿，不阻断主链路（见 QuotaReservationLedger）。
CREATE TABLE t_quota_reservation (
    id              BIGSERIAL    PRIMARY KEY,
    run_id          VARCHAR(64)  NOT NULL,
    model           VARCHAR(64)  NOT NULL,
    tpm_limit       INT          NOT NULL DEFAULT 0,
    reserved_tokens INT          NOT NULL DEFAULT 0,
    actual_tokens   INT,
    status          VARCHAR(16)  NOT NULL DEFAULT 'INIT',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 对账按「状态 + 陈旧时钟」扫描：updated_at 在每次状态迁移时刷新，created_at 兜住未被 CAS 更新过的 INIT
CREATE INDEX idx_quota_reservation_status_time ON t_quota_reservation(status, created_at);
CREATE INDEX idx_quota_reservation_run         ON t_quota_reservation(run_id);

-- 只读账号授权（与 V1/V3/V5/V6 保持一致；集成测试库无该角色时跳过）
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentflow_ro') THEN
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO agentflow_ro;
    END IF;
END $$;
