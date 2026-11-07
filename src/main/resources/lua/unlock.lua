-- KEYS[1] = 锁 key, ARGV[1] = 持有者 token
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
