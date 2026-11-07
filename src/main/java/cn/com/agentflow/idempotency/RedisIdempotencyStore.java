package cn.com.agentflow.idempotency;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

/**
 * 基于 Redis 的分布式幂等状态机。
 * 值格式：
 * - IN_PROGRESS|owner
 * - COMPLETED|owner
 * - CACHED|owner|httpStatus|base64(body)
 *
 * 完成与释放都走 Lua CAS：只有值仍是自己写入的内容才允许改写，
 * 避免 TTL 过期（或 Redis LRU 淘汰）后误伤其他请求刚抢到的锁。
 */
@Slf4j
@Component
public class RedisIdempotencyStore implements IdempotencyStore {

    private static final String IN_PROGRESS_PREFIX = "IN_PROGRESS|";
    private static final String COMPLETED_PREFIX = "COMPLETED|";
    private static final String CACHED_PREFIX = "CACHED|";

    /** CAS 删除：值等于 owner 对应的 IN_PROGRESS 才删 */
    private static final RedisScript<Long> CAS_DELETE = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('DEL', KEYS[1]) else return 0 end",
            Long.class);

    /** CAS 覆盖：值匹配才写入新状态并续 TTL（毫秒） */
    private static final RedisScript<Long> CAS_SET = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                    + "redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3]) return 1 "
                    + "else return 0 end",
            Long.class);

    private final ReactiveStringRedisTemplate redis;

    public RedisIdempotencyStore(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public Mono<AcquireOutcome> tryAcquire(String key, String owner, Duration ttl) {
        return redis.opsForValue()
                .setIfAbsent(key, IN_PROGRESS_PREFIX + owner, ttl)
                .map(Boolean.TRUE::equals)
                .map(ok -> ok ? AcquireOutcome.ACQUIRED : AcquireOutcome.OCCUPIED)
                .defaultIfEmpty(AcquireOutcome.OCCUPIED)
                // 幂等是防重优化而非正确性底线：Redis 抖动时放行主流程，只留 WARN 供排查
                .onErrorResume(e -> {
                    log.warn("幂等状态写入失败，降级放行(fail-open): key={}, err={}", key, e.toString());
                    return Mono.just(AcquireOutcome.UNAVAILABLE);
                });
    }

    @Override
    public Mono<IdempotencyState> read(String key) {
        return redis.opsForValue()
                .get(key)
                .map(RedisIdempotencyStore::parse)
                .onErrorResume(e -> {
                    log.warn("幂等状态读取失败，按不存在处理: key={}, err={}", key, e.toString());
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> markCompleted(String key, String owner, Duration ttl) {
        return casSet(key, owner, COMPLETED_PREFIX + owner, ttl);
    }

    @Override
    public Mono<Void> storeResponse(String key, String owner, int httpStatus, String body, Duration ttl) {
        String encoded = Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));
        return casSet(key, owner, CACHED_PREFIX + owner + "|" + httpStatus + "|" + encoded, ttl);
    }

    @Override
    public Mono<Void> release(String key, String owner) {
        return redis.execute(CAS_DELETE, List.of(key), List.of(IN_PROGRESS_PREFIX + owner))
                .next()
                .then()
                .onErrorResume(e -> {
                    log.warn("幂等锁释放失败（等 TTL 自然过期）: key={}, err={}", key, e.toString());
                    return Mono.empty();
                });
    }

    private Mono<Void> casSet(String key, String owner, String newValue, Duration ttl) {
        return redis.execute(CAS_SET,
                        List.of(key),
                        List.of(IN_PROGRESS_PREFIX + owner, newValue, String.valueOf(ttl.toMillis())))
                .next()
                .then()
                .onErrorResume(e -> {
                    log.warn("幂等状态收尾失败（不影响已返回响应）: key={}, err={}", key, e.toString());
                    return Mono.empty();
                });
    }

    private static IdempotencyState parse(String raw) {
        if (raw == null) {
            return IdempotencyState.unknown();
        }
        if (raw.startsWith(IN_PROGRESS_PREFIX)) {
            return IdempotencyState.inProgress();
        }
        if (raw.startsWith(COMPLETED_PREFIX)) {
            return IdempotencyState.completed();
        }
        if (raw.startsWith(CACHED_PREFIX)) {
            String[] parts = raw.split("\\|", 4);
            if (parts.length == 4) {
                try {
                    String body = new String(Base64.getDecoder().decode(parts[3]), StandardCharsets.UTF_8);
                    return IdempotencyState.cached(Integer.parseInt(parts[2]), body);
                } catch (IllegalArgumentException e) {
                    log.warn("幂等缓存响应解析失败，按未知状态处理: err={}", e.toString());
                }
            }
        }
        return IdempotencyState.unknown();
    }
}
