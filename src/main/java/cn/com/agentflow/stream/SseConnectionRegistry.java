package cn.com.agentflow.stream;

import cn.com.agentflow.config.AgentFlowProperties;
import cn.com.agentflow.exception.AgentException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地连接计数（快路径）。跨实例近似值由 Redis 每 30 秒同步，
 * 这里刻意不每连接 INCR/DECR：1 万连接下会把 Redis 打满。
 * 同时承载慢消费者滞后统计：每个订阅一份 {@link LagGuard}，随订阅取消被 GC 回收。
 */
@Component
public class SseConnectionRegistry {

    private final Map<String, AtomicInteger> perTenant = new ConcurrentHashMap<>();
    private final AtomicInteger total = new AtomicInteger();
    private final AgentFlowProperties props;
    private final Counter droppedEvents;
    private final Counter laggedClosures;

    public SseConnectionRegistry(MeterRegistry registry, AgentFlowProperties props) {
        this.props = props;
        Gauge.builder("sse.connections.active", total, AtomicInteger::get)
                .description("当前活跃 SSE 连接数")
                .register(registry);
        // 标签只用固定指标名：runId/tenantId 属于高基数维度，禁止进标签
        this.droppedEvents = Counter.builder("sse.events.dropped.total")
                .description("慢消费者被丢弃的事件数（单连接有界缓冲 DROP_OLDEST）")
                .register(registry);
        this.laggedClosures = Counter.builder("sse.connections.lagged_closed.total")
                .description("连续丢弃超过阈值被主动关闭的 SSE 连接数")
                .register(registry);
    }

    public void open(String tenantId) {
        int limit = props.getStream().getMaxConnectionsPerTenant();
        AtomicInteger c = perTenant.computeIfAbsent(tenantId, k -> new AtomicInteger());
        if (c.incrementAndGet() > limit) {
            c.decrementAndGet();
            throw new AgentException("SSE_CONN_LIMIT", "连接数超过租户上限 " + limit);
        }
        total.incrementAndGet();
    }

    public void close(String tenantId) {
        AtomicInteger c = perTenant.get(tenantId);
        if (c != null && c.decrementAndGet() < 0) {
            c.set(0);
        }
        total.updateAndGet(v -> Math.max(0, v - 1));
    }

    public int active() {
        return total.get();
    }

    /** 每个订阅一份滞后账本：状态跟随连接，不跨连接共享 */
    public LagGuard newLagGuard() {
        return new LagGuard(Math.max(1, props.getStream().getMaxLagEvents()));
    }

    /** 单连接滞后账本：连续丢弃计数 + 已投递到的 Last-Event-ID，全部为无锁原子操作 */
    public final class LagGuard {

        private final int maxLagEvents;
        private final AtomicInteger consecutiveDrops = new AtomicInteger();
        private volatile long lastDeliveredId;
        private volatile boolean closed;

        private LagGuard(int maxLagEvents) {
            this.maxLagEvents = maxLagEvents;
        }

        /** 缓冲丢弃一条事件：连续计数 +1，并计入全局丢弃指标 */
        public void onDropped() {
            consecutiveDrops.incrementAndGet();
            droppedEvents.increment();
        }

        /** 连续丢弃是否超过阈值：超过时应主动断开该连接，等客户端带 Last-Event-ID 重连回放 */
        public boolean overThreshold() {
            return consecutiveDrops.get() > maxLagEvents;
        }

        /** 正常投递：说明该连接重新跟上生产速度，连续丢弃计数清零 */
        public void recordDelivered(long eventId) {
            if (eventId > 0) {
                lastDeliveredId = eventId;
            }
            consecutiveDrops.set(0);
        }

        /** 滞后后投递：只推进 Last-Event-ID，不清零连续丢弃计数 */
        public void recordDeliveredAfterLag(long eventId) {
            if (eventId > 0) {
                lastDeliveredId = eventId;
            }
        }

        public long lastDeliveredId() {
            return lastDeliveredId;
        }

        public int consecutiveDrops() {
            return consecutiveDrops.get();
        }

        /** 因滞后主动关闭连接：只计一次，避免多路终止信号重复计数 */
        public void markClosed() {
            if (!closed) {
                closed = true;
                laggedClosures.increment();
            }
        }
    }
}
