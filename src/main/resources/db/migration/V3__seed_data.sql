CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO t_tenant(code, name, tier) VALUES
  ('demo',    '演示租户',   3),
  ('standard','标准租户',   2),
  ('free',    '免费租户',   1)
ON CONFLICT (code) DO NOTHING;

-- 演示用 API Key：明文 demo-key-123456，库里只存 SHA-256
INSERT INTO t_api_key(tenant_id, key_prefix, key_hash)
SELECT t.id, 'demo-key-12', encode(digest('demo-key-123456','sha256'),'hex')
FROM t_tenant t WHERE t.code = 'demo'
ON CONFLICT (key_hash) DO NOTHING;

INSERT INTO t_tool(name, description, input_schema, risk_level, timeout_ms) VALUES
('asset_search',
 '按关键字查询资产，最多返回 20 条。适用于"有哪些主机""查找某 IP"这类问题。',
 '{"type":"object","properties":{"keyword":{"type":"string","description":"主机名或 IP 关键字"},"limit":{"type":"integer","description":"返回条数，1-20","minimum":1,"maximum":20}},"required":["keyword"]}'::jsonb,
 1, 3000),
('asset_detail',
 '查询单个资产的详情，包含各危险等级漏洞数量统计。需要资产 ID。',
 '{"type":"object","properties":{"assetId":{"type":"integer","description":"资产 ID"}},"required":["assetId"]}'::jsonb,
 1, 3000),
('vuln_search',
 '按关键字与危险等级查询漏洞，游标分页。',
 '{"type":"object","properties":{"keyword":{"type":"string","description":"漏洞名称关键字"},"level":{"type":"integer","description":"危险等级 1-4"},"cursor":{"type":"integer","description":"上一页最后一条 ID，首页不传"},"limit":{"type":"integer","description":"返回条数，1-50"}},"required":["keyword"]}'::jsonb,
 1, 5000),
('report_generate',
 '为指定资产范围生成安全报表，异步执行，返回任务 ID。',
 '{"type":"object","properties":{"assetIds":{"type":"array","items":{"type":"integer"},"description":"资产 ID 列表"},"format":{"type":"string","enum":["pdf","docx","xlsx"],"description":"报表格式"}},"required":["assetIds","format"]}'::jsonb,
 2, 8000),
('asset_delete',
 '删除资产（逻辑删除）。属于高风险操作，必须人工审批。',
 '{"type":"object","properties":{"assetId":{"type":"integer","description":"资产 ID"}},"required":["assetId"]}'::jsonb,
 3, 3000)
ON CONFLICT (name) DO NOTHING;

INSERT INTO t_tool_grant(tenant_id, tool_name, rpm_limit)
SELECT t.id, x.name, 60 FROM t_tenant t CROSS JOIN (VALUES
  ('asset_search'),('asset_detail'),('vuln_search'),('report_generate'),('asset_delete')
) AS x(name)
WHERE t.code = 'demo'
ON CONFLICT (tenant_id, tool_name) DO NOTHING;

-- 演示资产与漏洞数据（供工具查询）
CREATE TABLE IF NOT EXISTS t_asset (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    ip VARCHAR(64) NOT NULL,
    hostname VARCHAR(128),
    os VARCHAR(64),
    deleted SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS t_vuln (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    asset_id BIGINT NOT NULL,
    name VARCHAR(256) NOT NULL,
    level SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_vuln_asset ON t_vuln(asset_id, level);

INSERT INTO t_asset(tenant_id, ip, hostname, os)
SELECT t.id, '10.0.0.' || g, 'host-' || g, 'CentOS 7'
FROM t_tenant t CROSS JOIN generate_series(1, 50) g
WHERE t.code = 'demo'
  AND NOT EXISTS (SELECT 1 FROM t_asset a WHERE a.tenant_id = t.id);

INSERT INTO t_vuln(tenant_id, asset_id, name, level)
SELECT a.tenant_id, a.id, 'CVE-2024-' || (1000 + (a.id * 7 + g) % 900), 1 + (g % 4)
FROM t_asset a CROSS JOIN generate_series(1, 6) g
WHERE NOT EXISTS (SELECT 1 FROM t_vuln v WHERE v.asset_id = a.id);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agentflow_ro') THEN
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO agentflow_ro;
    END IF;
END $$;
