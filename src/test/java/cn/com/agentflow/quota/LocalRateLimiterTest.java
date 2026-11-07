package cn.com.agentflow.quota;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LocalRateLimiter 令牌桶：容量封顶、按 key 隔离、准入失败不扣、退款幂等且不超容量 */
class LocalRateLimiterTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private final LocalRateLimiter limiter = new LocalRateLimiter(registry);

    @Test
    void 桶满时可取到容量内任意数量() {
        assertTrue(limiter.tryAcquire("m1", 100, 100));
    }

    @Test
    void 取空后立即再取被拒绝() {
        assertTrue(limiter.tryAcquire("m1", 100, 100));
        assertFalse(limiter.tryAcquire("m1", 100, 1), "令牌耗尽后必须拒绝");
    }

    @Test
    void 申请数超过容量直接拒绝且不扣令牌() {
        assertFalse(limiter.tryAcquire("m2", 10, 11));
        // 准入失败没有扣减：容量内申请仍可通过，这正是“失败不扣”语义
        assertTrue(limiter.tryAcquire("m2", 10, 10));
    }

    @Test
    void 不同key的桶互相隔离() {
        assertTrue(limiter.tryAcquire("model-a", 50, 50));
        assertTrue(limiter.tryAcquire("model-b", 50, 50), "model-b 是独立桶，不受 model-a 影响");
        assertFalse(limiter.tryAcquire("model-a", 50, 1));
    }

    @Test
    void 部分消耗后余量按差值可用() {
        assertTrue(limiter.tryAcquire("m3", 100, 60));
        assertTrue(limiter.tryAcquire("m3", 100, 40));
        assertFalse(limiter.tryAcquire("m3", 100, 1));
    }

    @Test
    void 下游失败退款后令牌恢复可用() {
        assertTrue(limiter.tryAcquire("m4", 100, 100));
        assertFalse(limiter.tryAcquire("m4", 100, 1), "先确认桶已空");

        limiter.refund("m4", 100);

        assertTrue(limiter.tryAcquire("m4", 100, 100), "归还后必须能再次准入，否则桶会被抽干");
    }

    @Test
    void 重复退款被桶容量截断不会灌爆() {
        assertTrue(limiter.tryAcquire("m5", 100, 100));
        limiter.refund("m5", 100);
        limiter.refund("m5", 100);
        limiter.refund("m5", 100);

        long available = limiter.availableTokens("m5");
        assertTrue(available <= 100, "可用令牌不得超过容量，实际=" + available);
        assertTrue(available >= 100 - 1, "多次退款后应恢复到容量上限，实际=" + available);
    }

    @Test
    void 未扣减过的桶退款不凭空造桶() {
        limiter.refund("never-used", 100);
        assertEquals(-1, limiter.availableTokens("never-used"));
    }

    @Test
    void 拒绝次数按固定枚举标签计数() {
        assertFalse(limiter.tryAcquire("m6", 10, 11));
        assertFalse(limiter.tryAcquire("m6", 10, 11));

        double rejected = registry.get("llm.local.limiter.rejected.total")
                .tag("model", "m6")
                .tag("reason", "INSUFFICIENT_TOKENS")
                .counter()
                .count();
        assertEquals(2.0, rejected);
    }

    @Test
    void 可用令牌指标随扣减下降() {
        assertTrue(limiter.tryAcquire("m7", 100, 100));
        double available = registry.get("llm.local.limiter.available.tokens")
                .tag("model", "m7")
                .gauge()
                .value();
        assertTrue(available <= 1, "刚取空时可用令牌应接近 0，实际=" + available);
    }
}
