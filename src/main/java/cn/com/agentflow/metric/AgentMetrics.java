package cn.com.agentflow.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 业务指标统一出口。标签只允许低基数值（mode/state/model/tool 等枚举值），
 * runId/tenantId/sessionId 这类高基数标识严禁作 tag（见 MetricGuard 启动校验）。
 */
@Component
@RequiredArgsConstructor
public class AgentMetrics {

    private final MeterRegistry registry;

    public void runFinished(String mode, String state, long costMs) {
        Counter.builder("agent.run.total").tag("mode", mode).tag("state", state).register(registry).increment();
        Timer.builder("agent.run.duration").tag("mode", mode)
                .publishPercentiles(0.5, 0.95, 0.99).register(registry)
                .record(costMs, TimeUnit.MILLISECONDS);
    }

    public void ttft(String model, long startNanos) {
        long ms = (System.nanoTime() - startNanos) / 1_000_000;
        Timer.builder("agent.ttft").tag("model", model)
                .publishPercentiles(0.5, 0.95, 0.99).register(registry)
                .record(ms, TimeUnit.MILLISECONDS);
    }

    public void tokens(String model, int in, int out) {
        Counter.builder("llm.tokens.total").tag("model", model).tag("type", "input")
                .register(registry).increment(in);
        Counter.builder("llm.tokens.total").tag("model", model).tag("type", "output")
                .register(registry).increment(out);
    }

    /**
     * 配额拒绝：scope 只取 local / redis 两个固定枚举值，用于区分“本地预检拒绝”
     * 与“Redis 精确限流拒绝”，排查时不再把配额问题误判成模型不可用。
     */
    public void quotaRejected(String model, String scope) {
        Counter.builder("llm.quota.rejected.total").tag("model", model).tag("scope", scope)
                .register(registry).increment();
    }

    /** 本地预检额度退还（失败/取消路径），用于核对扣减与退款是否对称 */
    public void quotaRefunded(String model) {
        Counter.builder("llm.quota.refunded.total").tag("model", model)
                .register(registry).increment();
    }

    public void llmError(String model, String type) {
        Counter.builder("llm.error.total").tag("model", model).tag("type", type)
                .register(registry).increment();
    }

    /** 金丝雀命中：标签只放 from/to 模型名，租户高基数标识严禁入标签 */
    public void canaryHit(String from, String to) {
        Counter.builder("llm.canary.hit.total").tag("from", from).tag("to", to)
                .register(registry).increment();
    }

    public void toolCall(String tool, String code, long costMs) {
        Timer.builder("tool.call.duration").tag("tool", tool).tag("code", code)
                .register(registry).record(costMs, TimeUnit.MILLISECONDS);
    }

    public void backpressureDrop() {
        Counter.builder("backpressure.drop.total").register(registry).increment();
    }

    public void slowClientClosed() {
        Counter.builder("sse.slow_client.closed.total").register(registry).increment();
    }
}
