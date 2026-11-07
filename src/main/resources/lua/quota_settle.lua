-- KEYS[1] = tpm 令牌桶 hash key
-- ARGV[1] = 需要退回的 token 数
-- ARGV[2] = 桶容量（tpm_limit）

local refund   = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])

if refund > 0 then
    local t = redis.call('TIME')
    local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
    local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
    local tokens = tonumber(b[1]) or 0
    local ts = tonumber(b[2]) or now
    redis.call('HMSET', KEYS[1], 'tokens', math.min(capacity, tokens + refund), 'ts', ts)
end
return 1
