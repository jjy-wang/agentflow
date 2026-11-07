package cn.com.agentflow.idempotency;

import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * 幂等状态机存储抽象。
 * owner 是首次请求生成的随机令牌，后续完成/释放都以"值仍等于自己写入的状态"为前提（CAS），
 * 防止 TTL 过期或被淘汰后，迟到的收尾动作误删/误改另一个请求的锁。
 */
public interface IdempotencyStore {

    enum AcquireOutcome {
        /** 抢锁成功，请求可以进入业务 */
        ACQUIRED,
        /** 锁已被其他请求持有 */
        OCCUPIED,
        /** 存储不可用，调用方应按 fail-open 放行 */
        UNAVAILABLE
    }

    Mono<AcquireOutcome> tryAcquire(String key, String owner, Duration ttl);

    /** 不存在返回 empty；读取失败也返回 empty，由后续重新抢锁触发 fail-open */
    Mono<IdempotencyState> read(String key);

    /** 流式响应不可缓存，只标记"已完成"，防止 TTL 内被重复提交 */
    Mono<Void> markCompleted(String key, String owner, Duration ttl);

    /** 非流式响应：缓存首次结果供重放 */
    Mono<Void> storeResponse(String key, String owner, int httpStatus, String body, Duration ttl);

    /** 业务失败时释放锁，让客户端的重试能够重新进入 */
    Mono<Void> release(String key, String owner);
}
