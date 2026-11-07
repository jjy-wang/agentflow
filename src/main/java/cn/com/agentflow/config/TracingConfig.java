package cn.com.agentflow.config;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.aop.ObservedAspect;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 链路追踪：pom 已引入 micrometer-tracing-bridge-otel + OTLP exporter，
 * Boot 自动装配 Tracer。这里补 @Observed 注解支持，并接管全局 Sampler。
 *
 * <p>采样只保留一个入口：{@code agentflow.telemetry.sample-rate}。
 * Boot 的 {@code management.tracing.sampling.probability} 对应 Sampler Bean 带
 * {@code @ConditionalOnMissingBean}，被本 Bean 覆盖后不再生效；两者默认值同为 0.1，
 * 因此升级前后采样行为一致，同时避免"业务采样 × SDK 采样"的双重衰减。</p>
 */
@Configuration
public class TracingConfig {

    @Bean
    public ObservedAspect observedAspect(ObservationRegistry registry) {
        return new ObservedAspect(registry);
    }

    /**
     * 根 span 按比例采样，子 span 继承父决策（parentBased），
     * 保证一条 run 的 chat / execute_tool 链路要么完整保留、要么整体丢弃。
     */
    @Bean
    public Sampler agentFlowSampler(AgentFlowProperties properties) {
        return Sampler.parentBased(
                Sampler.traceIdRatioBased(properties.getTelemetry().effectiveSampleRate()));
    }
}
