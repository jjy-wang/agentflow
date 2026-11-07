package cn.com.agentflow.prompts;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 前缀指纹复用指标：同一指纹第二次出现即视为可复用前缀（provider 侧缓存命中候选）。
 *
 * <p>指标只做无标签计数，避免把指纹本身当 tag 造成高基数；
 * 指纹通过 DEBUG 日志输出，排障时按 traceId 关联单次 run。
 * 去重集合有界（{@value #MAX_TRACKED_PREFIXES} 条），过期后自动淘汰，防止内存无界增长。</p>
 */
@Slf4j
@Component
public class PromptPrefixMeter {

    public static final String BUILD_METRIC = "agent.prompt.prefix.build.total";
    public static final String REUSED_METRIC = "agent.prompt.prefix.reused.total";

    private static final long MAX_TRACKED_PREFIXES = 10_000;

    private final Cache<String, Boolean> seenPrefixes;
    private final Counter buildCounter;
    private final Counter reusedCounter;

    public PromptPrefixMeter(MeterRegistry meterRegistry) {
        this.seenPrefixes = Caffeine.newBuilder()
                .maximumSize(MAX_TRACKED_PREFIXES)
                .expireAfterAccess(Duration.ofHours(1))
                .build();
        this.buildCounter = Counter.builder(BUILD_METRIC)
                .description("system prompt 前缀构建次数")
                .register(meterRegistry);
        this.reusedCounter = Counter.builder(REUSED_METRIC)
                .description("system prompt 前缀指纹重复出现次数（可复用缓存）")
                .register(meterRegistry);
    }

    /** 记录一次构建；返回是否会命中此前出现过的同一前缀 */
    public boolean record(String fingerprint) {
        buildCounter.increment();
        if (fingerprint == null || fingerprint.isBlank()) {
            return false;
        }
        boolean reused = seenPrefixes.asMap().putIfAbsent(fingerprint, Boolean.TRUE) != null;
        if (reused) {
            reusedCounter.increment();
            log.debug("prompt 前缀指纹命中 fingerprint={} reused={}", fingerprint,
                    reusedCounter.count());
        } else {
            log.debug("prompt 前缀指纹首次出现 fingerprint={}", fingerprint);
        }
        return reused;
    }
}
