package cn.com.agentflow.metric;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Set;

/** 启动期校验指标基数：禁用高基数标签，避免 Prometheus 被打爆 */
@Configuration
@RequiredArgsConstructor
public class MetricGuard {

    private static final Set<String> FORBIDDEN_TAGS =
            Set.of("runId", "run_id", "tenantId", "tenant_id", "sessionId", "session_id", "query", "traceId");

    private final MeterRegistry registry;

    /**
     * 双保险：注册一个 MeterFilter，任何带禁用标签的 meter 在注册时直接拒绝，
     * 防止后续新增代码绕过 PostConstruct 校验（后者只能兜启动期已有的 meter）。
     */
    @Bean
    public MeterFilter forbiddenTagFilter() {
        return new MeterFilter() {
            @Override
            public MeterFilterReply accept(Meter.Id id) {
                for (var tag : id.getTags()) {
                    if (FORBIDDEN_TAGS.contains(tag.getKey())) {
                        throw new IllegalStateException(
                                "指标 " + id.getName() + " 使用了高基数标签: " + tag.getKey());
                    }
                }
                return MeterFilterReply.NEUTRAL;
            }
        };
    }

    @PostConstruct
    public void verify() {
        for (Meter meter : registry.getMeters()) {
            meter.getId().getTags().forEach(tag -> {
                if (FORBIDDEN_TAGS.contains(tag.getKey())) {
                    throw new IllegalStateException(
                            "指标 " + meter.getId().getName() + " 使用了高基数标签: " + tag.getKey());
                }
            });
        }
    }
}
