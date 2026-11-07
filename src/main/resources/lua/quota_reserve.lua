-- KEYS[1] = rpm 前缀（会自动拼 ":分钟号"）
-- KEYS[2] = tpm 令牌桶 hash key
-- ARGV[1] = rpm_limit
-- ARGV[2] = tpm_limit
-- ARGV[3] = 本次预扣 token 数
-- 返回 {是否通过(1/0), 原因, 剩余额度}

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local rpm_limit = tonumber(ARGV[1])
local tpm_limit = tonumber(ARGV[2])
local reserve   = tonumber(ARGV[3])

-- ① RPM：按分钟固定窗口
local minute  = math.floor(now / 60000)
local rpm_key = KEYS[1] .. ':' .. minute
local used    = tonumber(redis.call('GET', rpm_key) or '0')
if used + 1 > rpm_limit then
    return {0, 'RPM_EXCEEDED', used}
end

-- ② TPM：令牌桶，容量 = 每分钟额度，按毫秒匀速补充
local b      = redis.call('HMGET', KEYS[2], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts     = tonumber(b[2])
if tokens == nil then
    tokens = tpm_limit
    ts = now
end
local delta = math.max(0, now - ts)
tokens = math.min(tpm_limit, tokens + delta * tpm_limit / 60000)

if tokens < reserve then
    return {0, 'TPM_EXCEEDED', math.floor(tokens)}
end

redis.call('HMSET', KEYS[2], 'tokens', tokens - reserve, 'ts', now)
redis.call('INCR', rpm_key)
redis.call('PEXPIRE', rpm_key, 120000)

return {1, 'OK', math.floor(tokens - reserve)}
