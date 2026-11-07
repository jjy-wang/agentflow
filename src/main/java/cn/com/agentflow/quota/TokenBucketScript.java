package cn.com.agentflow.quota;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

/**
 * Redis 令牌桶脚本封装：读-判断-扣减必须在 Lua 内原子完成，时间取 Redis TIME，
 * 避免多实例时钟漂移导致补充速率算错。
 * 返回值：1 = 成功取到令牌，0 = 桶内令牌不足。
 */
@Component
public class TokenBucketScript {

    /** 令牌桶 Lua：KEYS[1]=桶键，ARGV[1]=容量，ARGV[2]=每秒补充速率，ARGV[3]=本次获取数 */
    private static final String LUA = """
            local key = KEYS[1]
            local capacity = tonumber(ARGV[1])
            local refillPerSec = tonumber(ARGV[2])
            local permits = tonumber(ARGV[3])

            local time = redis.call('TIME')
            local nowMs = time[1] * 1000 + math.floor(time[2] / 1000)

            local bucket = redis.call('HMGET', key, 'tokens', 'lastRefillMs')
            local tokens = tonumber(bucket[1])
            local lastRefillMs = tonumber(bucket[2])
            if tokens == nil then
                tokens = capacity
                lastRefillMs = nowMs
            end

            local delta = (nowMs - lastRefillMs) * refillPerSec / 1000.0
            tokens = math.min(capacity, tokens + delta)
            lastRefillMs = nowMs

            if tokens < permits then
                redis.call('HMSET', key, 'tokens', tokens, 'lastRefillMs', lastRefillMs)
                redis.call('PEXPIRE', key, 60000)
                return 0
            end

            tokens = tokens - permits
            redis.call('HMSET', key, 'tokens', tokens, 'lastRefillMs', lastRefillMs)
            redis.call('PEXPIRE', key, 60000)
            return 1
            """;

    private static final RedisScript<Long> SCRIPT = new DefaultRedisScript<>(LUA, Long.class);

    private final StringRedisTemplate redis;

    public TokenBucketScript(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * 尝试获取一个令牌。Redis 阻塞调用切到 boundedElastic，不占事件循环线程。
     *
     * @return 1 = 获取成功，0 = 令牌不足
     */
    public Mono<Long> tryAcquire(String key, long capacity, long refillPerSec) {
        return Mono.fromCallable(() -> {
                    Long r = redis.execute(SCRIPT, List.of(key),
                            String.valueOf(capacity), String.valueOf(refillPerSec), "1");
                    return r == null ? 0L : r;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}
