package cn.com.agentflow.run;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

/**
 * run 的 Redis 租约（SET NX PX）：DB 租约（t_agent_run.lease_*）之外的轻量互斥层，
 * 用于"同一 run 只允许一个执行体"的快速判重，抢锁失败直接返回 false 不阻塞。
 * 释放走 lua/unlock.lua（GET 比对持有者再 DEL），防止误删他人锁。
 */
@Slf4j
@Component
public class RunLeaseService {

    private static final String KEY_PREFIX = "lease:run:";

    /** 释放锁脚本：只有持有者可删 */
    private static final RedisScript<Long> UNLOCK = RedisScript.of(
            new ClassPathResource("lua/unlock.lua"), Long.class);

    /** 续期脚本：持有者可延长租约 */
    private static final DefaultRedisScript<Long> RENEW = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('PEXPIRE', KEYS[1], ARGV[2]) "
                    + "else return 0 end",
            Long.class);

    private final ReactiveStringRedisTemplate redis;

    public RunLeaseService(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    /** 尝试加锁：底层即 SET key owner PX ttl NX，原子完成 */
    public Mono<Boolean> tryAcquire(String runId, String owner, Duration ttl) {
        return redis.opsForValue()
                .setIfAbsent(KEY_PREFIX + runId, owner, ttl)
                .defaultIfEmpty(false)
                .onErrorResume(e -> {
                    // Redis 挂了就放弃轻量互斥，退化到 DB 租约（SKIP LOCKED 兜底）
                    log.warn("run 租约加锁失败，降级放行: runId={}, err={}", runId, e.toString());
                    return Mono.just(true);
                });
    }

    /** 续期：持有者匹配才延长，返回是否仍持有 */
    public Mono<Boolean> renew(String runId, String owner, Duration ttl) {
        return redis.execute(RENEW, List.of(KEY_PREFIX + runId), List.of(owner, String.valueOf(ttl.toMillis())))
                .singleOrEmpty()
                .map(r -> r != null && r == 1L)
                .defaultIfEmpty(false)
                .onErrorResume(e -> {
                    log.warn("run 租约续期失败: runId={}, err={}", runId, e.toString());
                    return Mono.just(false);
                });
    }

    /** 释放：仅持有者能删（防误删他人锁） */
    public Mono<Boolean> release(String runId, String owner) {
        return redis.execute(UNLOCK, List.of(KEY_PREFIX + runId), List.of(owner))
                .singleOrEmpty()
                .map(r -> r != null && r == 1L)
                .defaultIfEmpty(false)
                .onErrorResume(e -> {
                    log.warn("run 租约释放失败（等 TTL 自然过期）: runId={}, err={}", runId, e.toString());
                    return Mono.just(false);
                });
    }
}
