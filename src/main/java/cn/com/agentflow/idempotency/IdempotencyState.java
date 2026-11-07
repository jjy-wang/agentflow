package cn.com.agentflow.idempotency;

/**
 * 幂等状态快照。
 * IN_PROGRESS：首个请求仍持有锁；
 * COMPLETED：已完成但响应不可缓存（SSE 流式响应）；
 * CACHED：已完成且缓存了首次响应，可原样重放；
 * UNKNOWN：Redis 中的值格式无法识别，按"处理中"处理——宁可拒绝也不能重复执行。
 */
public record IdempotencyState(Kind kind, int httpStatus, String body) {

    public enum Kind {
        IN_PROGRESS, COMPLETED, CACHED, UNKNOWN
    }

    public static IdempotencyState inProgress() {
        return new IdempotencyState(Kind.IN_PROGRESS, 0, null);
    }

    public static IdempotencyState completed() {
        return new IdempotencyState(Kind.COMPLETED, 0, null);
    }

    public static IdempotencyState cached(int httpStatus, String body) {
        return new IdempotencyState(Kind.CACHED, httpStatus, body);
    }

    public static IdempotencyState unknown() {
        return new IdempotencyState(Kind.UNKNOWN, 0, null);
    }
}
