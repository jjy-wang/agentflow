package cn.com.agentflow.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

/**
 * 嵌入向量二级缓存：本地 Caffeine（单机快路径）+ Redis（跨实例共享）。
 * Redis 值存逗号分隔的浮点字符串，key 是文本 SHA-256，不落原文。
 * 缓存只加速不兜底正确性之外的语义：Redis 故障时退化为纯本地缓存。
 */
@Slf4j
@Component
public class EmbeddingCache {

    private static final String KEY_PREFIX = "cache:emb:";
    private static final Duration REDIS_TTL = Duration.ofDays(7);

    private final ReactiveStringRedisTemplate redis;
    private final Cache<String, float[]> local;

    public EmbeddingCache(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
        this.local = Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofDays(1))
                .build();
    }

    /** 先查本地再查 Redis；Redis 命中回填本地 */
    public Mono<float[]> get(String text) {
        String key = KEY_PREFIX + sha256Hex(text);
        float[] hit = local.getIfPresent(key);
        if (hit != null) {
            return Mono.just(hit);
        }
        return redis.opsForValue().get(key)
                .map(EmbeddingCache::parse)
                .doOnNext(v -> local.put(key, v))
                .onErrorResume(e -> {
                    log.warn("向量缓存读取失败，按 miss 处理: err={}", e.toString());
                    return Mono.empty();
                });
    }

    /** 空向量（embedding 降级产物）不写入，避免污染缓存 */
    public Mono<Void> put(String text, float[] vector) {
        if (vector == null || vector.length == 0) {
            return Mono.empty();
        }
        String key = KEY_PREFIX + sha256Hex(text);
        local.put(key, vector);
        return redis.opsForValue().set(key, join(vector), REDIS_TTL)
                .then()
                .onErrorResume(e -> {
                    log.warn("向量缓存写入失败，仅保留本地缓存: err={}", e.toString());
                    return Mono.empty();
                });
    }

    /** float[] → 逗号分隔字符串 */
    private static String join(float[] vector) {
        StringBuilder sb = new StringBuilder(vector.length * 8);
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(vector[i]);
        }
        return sb.toString();
    }

    private static float[] parse(String value) {
        String[] parts = value.split(",");
        float[] vector = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            vector[i] = Float.parseFloat(parts[i]);
        }
        return vector;
    }

    private static String sha256Hex(String raw) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(raw.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK 缺少 SHA-256 实现", e);
        }
    }
}
