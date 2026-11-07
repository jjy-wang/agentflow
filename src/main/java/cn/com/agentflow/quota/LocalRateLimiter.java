package cn.com.agentflow.quota;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地令牌桶预检：容量 = 全局额度 / 实例数 × 1.2，用于 Redis 之前的快速失败与降级。
 *
 * <p><b>它是预检，不是最终权威。</b>本类是单机内存结构，只负责挡住明显超量、降低 Redis
 * 压力；权威配额在 Redis 分布式令牌桶（{@link QuotaService} + Lua），本地预检通过不代表
 * 全局一定放行，本地预检拒绝也不代表全局额度耗尽。Redis 不可用时它承担降级近似限流——
 * 宁可近似、不可全挂。</p>
 *
 * <p><b>申请-归还语义：</b>{@link #tryAcquire} 返回 true 后，调用方必须对本次预扣负责：
 * 调用失败、超时、取消或任何未真正完成结算的路径都必须调用 {@link #refund}；返回 false
 * 时没有扣减，严禁退款。退款以桶容量封顶，重复退款会被截断为容量上限，不会把桶灌爆。
 * 压测中 81% 请求报“无可用模型”的根因就是只扣不退：高并发下桶被抽干后，后续请求全部
 * 被本地预检拒绝，而错误码又被错误地归为 NO_MODEL_AVAILABLE。</p>
 */
@Component
public class LocalRateLimiter {

    /** 拒绝原因固定枚举，保证指标标签低基数 */
    private static final String REJECT_REASON = "INSUFFICIENT_TOKENS";

    private static final class Bucket {
        /** 当前令牌数；volatile 保证指标 Gauge 无锁读取时的可见性 */
        volatile double tokens;
        long lastRefillMs;
        /** 最近一次 tryAcquire 使用的容量，退款以此为上限，防止把桶灌爆 */
        int capacity;

        double available() {
            return Math.max(0, tokens);
        }
    }

    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    private final MeterRegistry registry;

    public LocalRateLimiter(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * 扣减令牌。同一实例内用同一把锁串行化，保证“判断余量 + 扣减”原子，
     * 并发下不会出现负余量。指标的 model 标签直接取 key（测试/单桶场景）。
     */
    public boolean tryAcquire(String key, int capacityPerMinute, int permits) {
        return tryAcquire(key, key, capacityPerMinute, permits);
    }

    /**
     * @param key          桶标识（调用方用前缀区分限流维度，如 {@code tpm:model}）
     * @param metricModel  指标标签使用的模型名，必须是配置中的固定枚举，禁用租户/runId
     */
    public synchronized boolean tryAcquire(String key, String metricModel,
                                           int capacityPerMinute, int permits) {
        long now = System.currentTimeMillis();
        Bucket b = buckets.computeIfAbsent(key, k -> {
            Bucket nb = new Bucket();
            nb.tokens = capacityPerMinute;
            nb.lastRefillMs = now;
            nb.capacity = capacityPerMinute;
            registerAvailableGauge(metricModel, nb);
            return nb;
        });
        double delta = (now - b.lastRefillMs) * capacityPerMinute / 60000.0;
        b.capacity = capacityPerMinute;
        b.tokens = Math.min(capacityPerMinute, b.tokens + delta);
        b.lastRefillMs = now;
        if (b.tokens < permits) {
            Counter.builder("llm.local.limiter.rejected.total")
                    .tag("model", metricModel)
                    .tag("reason", REJECT_REASON)
                    .register(registry)
                    .increment();
            return false;
        }
        b.tokens -= permits;
        return true;
    }

    /**
     * 退还本地预检扣掉的令牌：与 {@link #tryAcquire} 共用同一把锁，保证并发退款
     * 不会出现负值；退款以桶容量为硬上限，超额退款直接截断（不能把桶灌爆）。
     *
     * <p>桶不存在时不创建：从未扣减过就没有可退额度，凭空造桶会破坏限流精度。</p>
     */
    public synchronized void refund(String key, long permits) {
        if (permits <= 0) {
            return;
        }
        Bucket b = buckets.get(key);
        if (b == null) {
            return;
        }
        double cap = Math.max(0, b.capacity);
        b.tokens = Math.min(cap, b.tokens + (double) permits);
    }

    /** 当前可用令牌数（向下取整），仅供排障与测试；桶不存在返回 -1 */
    public synchronized long availableTokens(String key) {
        Bucket b = buckets.get(key);
        return b == null ? -1 : (long) Math.floor(b.available());
    }

    /**
     * 可用令牌数指标。标签只放模型名（配置中的固定枚举集合），容量随配置收敛，
     * 不会像租户 ID 那样随请求无限增长。
     */
    private void registerAvailableGauge(String model, Bucket bucket) {
        Gauge.builder("llm.local.limiter.available.tokens", bucket, Bucket::available)
                .tag("model", model)
                .description("本地预检令牌桶当前可用令牌数；它只是预检余量，不是全局配额")
                .register(registry);
    }
}
