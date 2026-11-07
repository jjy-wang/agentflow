package cn.com.agentflow.config;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClientRequestException;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Resilience4j 兜底组件。
 * 模型调用的并发舱壁在 quota/ModelBulkhead 按模型精细控制（与 llm.models.*.max-concurrency 对齐），
 * 这里提供的是面向"非模型类外部调用"（embedding、工具 HTTP 出站）的通用默认值。
 */
@Configuration
public class ResilienceConfig {

    /** 通用出站舱壁：上限对齐默认模型并发档位，防止外部慢调用拖垮业务线程 */
    @Bean
    public Bulkhead outboundBulkhead(AgentFlowProperties props) {
        BulkheadConfig config = BulkheadConfig.custom()
                .maxConcurrentCalls(props.getLlm().getModels().values().stream()
                        .mapToInt(AgentFlowProperties.Model::getMaxConcurrency)
                        .max()
                        .orElse(64))
                .maxWaitDuration(Duration.ZERO)
                .build();
        return Bulkhead.of("outbound", config);
    }

    /** 通用出站重试：仅对网络层错误重试一次，业务错误码不重试 */
    @Bean
    public Retry outboundRetry() {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(2)
                .waitDuration(Duration.ofMillis(300))
                .retryExceptions(IOException.class, TimeoutException.class, WebClientRequestException.class)
                .build();
        return Retry.of("outbound", config);
    }
}
