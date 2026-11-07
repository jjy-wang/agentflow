-- 只读账号：仅授予 SELECT，模拟生产从库的读权限边界
CREATE ROLE agentflow_ro LOGIN PASSWORD 'agentflow_ro';
GRANT CONNECT ON DATABASE agentflow TO agentflow_ro;
GRANT USAGE ON SCHEMA public TO agentflow_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO agentflow_ro;
-- 对已存在的表（迁移脚本创建后需再执行一次 GRANT）
GRANT SELECT ON ALL TABLES IN SCHEMA public TO agentflow_ro;
